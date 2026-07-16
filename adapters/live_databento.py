"""
Live Databento adapter.

Connects to Databento's live gateway, subscribes to a single symbol
(continuous front-month by default), and yields TickEvent objects
as trades arrive in real time.

Architecture notes:
- The Databento Python SDK provides synchronous and async iteration
  over the incoming record stream. We use sync iteration in a background
  thread (the same thread that runs the asyncio event loop for the
  rest of the adapter contract), bridging via asyncio.to_thread when
  blocking on the network read.

- TradeMsg records from the trades schema have the same field semantics
  as the historical CSV: ts_event (nanoseconds since epoch), price
  (scaled int64), size (int), side ('A'/'B'/'N'), and an instrument_id
  that we map back to the display symbol via SymbolMappingMsg records.

- Price is scaled: every 1 unit = 1e-9 dollars. So a raw price of
  25224500000000 represents $25224.500. The SDK's pretty_px property
  handles this for us, returning a Python float.

- We track instrument_id -> symbol mappings as SymbolMappingMsg records
  arrive (sent by the gateway at session start and on contract rolls
  for continuous symbols). The instrument_symbol field on each TickEvent
  is the resolved human-readable symbol, not the numeric instrument_id.

- Health states:
    green:  receiving data normally
    yellow: connected, no data for a while (could be a quiet market;
            heartbeats keep us connected)
    red:    disconnected / errored
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import AsyncIterator, Optional

import databento as db
from databento.common.symbology import InstrumentMap

from .base import TickAdapter, TickEvent, HealthStatus
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


# Databento side codes -> our normalized aggressor_side, same as CSV adapter.
SIDE_MAP = {
    "A": "buy",   # aggressor lifted the ask
    "B": "sell",  # aggressor hit the bid
}


class DatabentoLiveAdapter(TickAdapter):
    """Streams live trades from Databento for a single symbol.

    Args:
        symbol: instrument to subscribe to (e.g. "MNQ.c.0" for continuous
            front-month MNQ, or "MNQM6" for raw June 2026 contract)
        stype_in: symbology type. "continuous" for auto-rolling front-month,
            "raw_symbol" for explicit contract codes
        dataset: Databento dataset ID. Default GLBX.MDP3 (CME).
        api_key: Databento API key. If None, read from DATABENTO_API_KEY
            environment variable.
    """

    def __init__(
        self,
        symbol: str,
        stype_in: str = "continuous",
        dataset: str = "GLBX.MDP3",
        api_key: Optional[str] = None,
    ):
        self.symbol = symbol
        self.stype_in = stype_in
        self.dataset = dataset
        self._api_key = api_key or os.environ.get("DATABENTO_API_KEY")
        if not self._api_key:
            raise ValueError(
                "No API key provided. Set DATABENTO_API_KEY environment "
                "variable or pass api_key to the constructor."
            )

        self._client: Optional[db.Live] = None
        self._instrument_map = InstrumentMap()
        self._health: HealthStatus = "red"
        self._last_data_recv_ts: Optional[float] = None

        # Diagnostics, parallel to CSVAdapter for consistency.
        self.records_seen = 0
        self.ticks_yielded = 0
        self.dropped_no_aggressor = 0  # side == 'N'
        self.dropped_non_trade = 0     # non-Trades schema records
        self.dropped_unmapped_id = 0   # instrument_id not yet in our map
        self.parse_errors = 0

    async def connect(self) -> None:
        # Construct the client. The SDK picks up the env var on its own
        # if we don't pass key, but we resolved it explicitly above to
        # fail loudly when missing.
        self._client = db.Live(key=self._api_key)
        self._client.subscribe(
            dataset=self.dataset,
            schema="trades",
            stype_in=self.stype_in,
            symbols=[self.symbol],
        )
        # Note: we deliberately do NOT call client.start() here. The
        # Databento SDK rejects iteration after an explicit .start() —
        # iteration must be the thing that initiates streaming. The
        # first `async for` in stream() will do it.
        self._health = "yellow"  # subscribed but no data yet
        logger.info(
            "DatabentoLiveAdapter connected: dataset=%s symbol=%s stype=%s",
            self.dataset, self.symbol, self.stype_in,
        )

    async def stream(self) -> AsyncIterator[TickEvent]:
        if self._client is None:
            raise RuntimeError("DatabentoLiveAdapter.stream() called before connect()")

        # The SDK supports async iteration directly.
        async for record in self._client:
            self.records_seen += 1
            self._last_data_recv_ts = asyncio.get_event_loop().time()

            # SymbolMappingMsg: cache the id -> symbol mapping and continue.
            # These arrive at session start and on continuous-contract rolls.
            if isinstance(record, db.SymbolMappingMsg):
                self._instrument_map.insert_symbol_mapping_msg(record)
                logger.info(
                    "Symbol mapping: instrument_id=%d -> %s (valid %s..%s)",
                    record.instrument_id,
                    record.stype_out_symbol,
                    record.start_ts,
                    record.end_ts,
                )
                continue

            # SystemMsg: heartbeats, subscription acks, replay completed.
            # Useful for diagnostics but no data to yield.
            if isinstance(record, db.SystemMsg):
                if record.is_heartbeat():
                    logger.debug("Heartbeat received")
                else:
                    logger.info("System message: code=%s msg=%s",
                                getattr(record, "code", "?"),
                                getattr(record, "msg", ""))
                continue

            # ErrorMsg: log loudly. The gateway will close the connection
            # after sending one; the iteration will end naturally.
            if isinstance(record, db.ErrorMsg):
                logger.error(
                    "Live gateway error: code=%s msg=%s",
                    getattr(record, "code", "?"),
                    getattr(record, "err", ""),
                )
                self._health = "red"
                continue

            # TradeMsg: this is what we actually want.
            if not isinstance(record, db.TradeMsg):
                # Other record types (definitions, statistics, etc.) — we
                # didn't subscribe to these, but be defensive.
                self.dropped_non_trade += 1
                continue

            # Translate side code to our internal convention.
            # The Databento SDK exposes record.side as a Side enum
            # (Side.ASK / Side.BID / Side.NONE). The enum's .value is
            # the underlying single-character string ('A', 'B', 'N').
            # Fall back to str() conversion if the type is something else.
            side_obj = record.side
            if hasattr(side_obj, "value"):
                raw_side = side_obj.value
            elif isinstance(side_obj, int):
                raw_side = chr(side_obj)
            else:
                raw_side = str(side_obj)
            aggressor = SIDE_MAP.get(raw_side)
            if aggressor is None:
                self.dropped_no_aggressor += 1
                continue

            # Resolve instrument_id -> human symbol.
            ts_event = datetime.fromtimestamp(
                record.ts_event / 1_000_000_000, tz=timezone.utc
            )
            symbol = self._instrument_map.resolve(record.instrument_id, ts_event.date())
            if symbol is None:
                # This shouldn't happen for live trades on subscribed
                # symbols — the gateway sends SymbolMappingMsgs before the
                # first trade. But during the first few milliseconds of a
                # session, a race is theoretically possible. Drop and log.
                self.dropped_unmapped_id += 1
                if self.dropped_unmapped_id <= 5:
                    logger.warning(
                        "No symbol mapping for instrument_id=%d at %s; dropping",
                        record.instrument_id, ts_event,
                    )
                continue

            try:
                tick = TickEvent(
                    timestamp=ts_event,
                    price=float(record.pretty_price),
                    size=int(record.size),
                    aggressor_side=aggressor,
                    instrument_symbol=symbol,
                    # Stash the flags bitmask the same way the CSV adapter
                    # does, so trade_conditions is consistent across adapters.
                    trade_conditions=(str(record.flags),) if record.flags else (),
                )
            except (ValueError, AttributeError) as exc:
                self.parse_errors += 1
                logger.warning("Skipping bad record %d: %s",
                               self.records_seen, exc)
                continue

            self._health = "green"
            self.ticks_yielded += 1
            yield tick

        # Stream ended (gateway disconnected, client stopped, etc.).
        self._health = "red"
        logger.info(
            "DatabentoLiveAdapter stream ended: records=%d yielded=%d "
            "dropped_N=%d dropped_non_trade=%d dropped_unmapped=%d "
            "parse_errors=%d",
            self.records_seen, self.ticks_yielded,
            self.dropped_no_aggressor, self.dropped_non_trade,
            self.dropped_unmapped_id, self.parse_errors,
        )

    async def disconnect(self) -> None:
        if self._client is not None:
            try:
                self._client.stop()
            except Exception as exc:
                logger.warning("Error stopping live client: %s", exc)
            self._client = None
        self._health = "red"

    @property
    def health(self) -> HealthStatus:
        return self._health

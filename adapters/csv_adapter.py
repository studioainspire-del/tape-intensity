"""
CSV adapter for Databento trades schema.

Reads a Databento trades CSV file and yields TickEvent objects as fast
as the loop runs. No artificial timing — that's what the replay adapter
(Phase 2) is for.

Databento trades schema (the columns we care about):
    ts_event       : nanosecond UTC timestamp, integer
    rtype          : record type, always present
    publisher_id   : ignored
    instrument_id  : numeric ID, ignored in favor of symbol
    action         : 'T' for trade (we filter to these)
    side           : 'A' = aggressor hit ask (buy aggressor)
                     'B' = aggressor hit bid (sell aggressor)
                     'N' = no aggressor / auction / unknown -> dropped
    depth          : ignored
    price          : float (already in dollar units in CSV export)
    size           : integer contracts
    flags          : ignored for now
    ts_recv        : ignored
    ts_in_delta    : ignored
    sequence       : ignored
    symbol         : e.g. 'NQZ5', the human-readable instrument

Other columns may be present; we ignore them. If the schema header
shifts, validate_header will raise so the bug is loud.
"""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Optional

from .base import TickAdapter, TickEvent, HealthStatus


logger = logging.getLogger(__name__)


# Columns we require to be present in the CSV header.
# Extra columns are fine; missing required columns is a fatal error.
REQUIRED_COLUMNS = frozenset({"ts_event", "side", "price", "size", "symbol"})

# Databento side -> our normalized aggressor_side.
# 'N' is intentionally absent: those rows are dropped.
SIDE_MAP = {
    "A": "buy",   # aggressor lifted the ask
    "B": "sell",  # aggressor hit the bid
}


class CSVAdapter(TickAdapter):
    """Reads a Databento trades CSV and yields TickEvent objects.

    Args:
        path: path to the CSV file
        symbol_filter: if set, only yield ticks for this symbol. Useful
            when a CSV contains multiple instruments.
    """

    def __init__(self, path: str | Path, symbol_filter: Optional[str] = None):
        self.path = Path(path)
        self.symbol_filter = symbol_filter

        self._file = None
        self._reader: Optional[csv.DictReader] = None
        self._health: HealthStatus = "red"

        # Counters useful for diagnostics. Exposed as attributes so
        # main.py / tests can read them after streaming completes.
        self.rows_read = 0
        self.ticks_yielded = 0
        self.dropped_no_aggressor = 0   # side == 'N'
        self.dropped_non_trade = 0      # action != 'T' (if column present)
        self.dropped_other_symbol = 0   # filtered out by symbol_filter
        self.parse_errors = 0

    async def connect(self) -> None:
        if not self.path.exists():
            self._health = "red"
            raise FileNotFoundError(f"CSV not found: {self.path}")

        # We use the standard library csv module rather than pandas because
        # we want to stream row-by-row without loading the whole file into
        # memory. Real Databento exports can be large.
        self._file = open(self.path, "r", newline="", encoding="utf-8")
        self._reader = csv.DictReader(self._file)

        self._validate_header()
        self._health = "green"
        logger.info("CSVAdapter connected to %s", self.path)

    def _validate_header(self) -> None:
        if self._reader is None or self._reader.fieldnames is None:
            raise ValueError(f"CSV {self.path} appears empty or unreadable")

        present = set(self._reader.fieldnames)
        missing = REQUIRED_COLUMNS - present
        if missing:
            raise ValueError(
                f"CSV {self.path} missing required columns: {sorted(missing)}. "
                f"Found: {sorted(present)}"
            )

    async def stream(self) -> AsyncIterator[TickEvent]:
        if self._reader is None:
            raise RuntimeError("CSVAdapter.stream() called before connect()")

        for row in self._reader:
            self.rows_read += 1

            # Filter to actual trade actions if the column is present.
            # Some Databento schemas include book-update rows alongside trades.
            action = row.get("action")
            if action is not None and action != "T":
                self.dropped_non_trade += 1
                continue

            # Symbol filter (when CSV contains multiple instruments).
            symbol = row.get("symbol", "")
            if self.symbol_filter and symbol != self.symbol_filter:
                self.dropped_other_symbol += 1
                continue

            # Aggressor side translation. Drop 'N' silently but counted.
            raw_side = row.get("side", "")
            aggressor = SIDE_MAP.get(raw_side)
            if aggressor is None:
                self.dropped_no_aggressor += 1
                continue

            # Parse the rest. Any failure here is a bad row, not a bad
            # adapter — log it, count it, keep going.
            try:
                tick = TickEvent(
                    timestamp=_parse_databento_timestamp(row["ts_event"]),
                    price=float(row["price"]),
                    size=int(row["size"]),
                    aggressor_side=aggressor,
                    instrument_symbol=symbol,
                    trade_conditions=_parse_flags(row.get("flags", "")),
                )
            except (ValueError, KeyError) as exc:
                self.parse_errors += 1
                logger.warning("Skipping bad CSV row %d: %s", self.rows_read, exc)
                continue

            self.ticks_yielded += 1
            yield tick

        # Stream exhausted — file is done.
        self._health = "yellow"
        logger.info(
            "CSVAdapter exhausted: read=%d yielded=%d dropped_N=%d "
            "dropped_non_trade=%d dropped_symbol=%d parse_errors=%d",
            self.rows_read, self.ticks_yielded, self.dropped_no_aggressor,
            self.dropped_non_trade, self.dropped_other_symbol, self.parse_errors,
        )

    async def disconnect(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
            self._reader = None
        self._health = "red"

    @property
    def health(self) -> HealthStatus:
        return self._health


def _parse_databento_timestamp(raw: str) -> datetime:
    """Parse a Databento ts_event value.

    Databento exports ts_event as either:
      - a nanosecond integer since the Unix epoch (default for raw exports), or
      - an ISO 8601 string (when --pretty-ts is enabled at export time).

    Try integer first since it's the default. Fall back to ISO parsing.
    Always return a timezone-aware UTC datetime.
    """
    raw = raw.strip()
    if not raw:
        raise ValueError("empty ts_event")

    # Integer nanoseconds path.
    try:
        ns = int(raw)
        seconds = ns / 1_000_000_000
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    except ValueError:
        pass

    # ISO 8601 path. fromisoformat accepts trailing 'Z' as of Python 3.11.
    return datetime.fromisoformat(raw)


def _parse_flags(raw: str) -> tuple[str, ...]:
    """Convert a Databento flags field into trade_conditions.

    Databento flags are typically a small integer bitmask. We don't
    interpret them yet — just stash the raw value as a single-element
    tuple so it survives into TickEvent. A future change can decode
    specific bits if a strategy needs them.
    """
    raw = raw.strip()
    if not raw:
        return ()
    return (raw,)

"""
Processor: turns a stream of TickEvents into smoothed buy/sell rates.

Design summary:
- Push model. A background task (`consume`) runs forever, ingesting
  ticks as they arrive from any TickAdapter. Separately, `get_state()`
  returns a snapshot whenever called (typically by the dashboard).
  Ingestion rate and query rate are decoupled.

- Rolling buffer is a deque of (timestamp, size, signed_size) tuples.
  signed_size = +size for buys, -size for sells. We store both because
  buy_rate, sell_rate, and net_rate all become single-pass sums.

- Smoothing is windowed averaging: every get_state() walks the deque,
  prunes anything older than `window_seconds`, sums the sizes by side,
  divides by window length. O(n) per call but n stays small (a few
  thousand at most for MNQ during heavy flow).

- No locks on the tick buffer. Python's GIL serializes deque
  mutations, and we never yield mid-operation in a way that would
  split a logical update. This assumption holds for single-process
  asyncio. If we ever move to threads or multiprocessing, revisit.

- Server-side history (Phase 7a). A second deque holds sampled
  ProcessorStates so the dashboard survives backgrounded tabs and
  page reloads. record_snapshot() is called at a fixed cadence by a
  background task on the pipeline's asyncio loop; the history query
  methods are called from the Dash callback on the Flask thread, so
  the history deque (unlike the tick buffer) IS guarded by a lock —
  iterating a deque while another thread appends raises RuntimeError.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from adapters.base import TickAdapter, TickEvent


logger = logging.getLogger(__name__)


# Server-side history capacity: 30 minutes at the 5 Hz sampler cadence.
# Sized deliberately for Phase 7b's scrollback range — don't grow or
# shrink without revisiting that plan.
HISTORY_MAXLEN = 9000


@dataclass(frozen=True)
class ProcessorState:
    """A snapshot of the processor at a moment in time.

    All rates are contracts per second, smoothed over the configured window.
    """
    timestamp: datetime              # 'as of' time = most recent tick's timestamp
    buy_rate: float                  # aggressive buys, contracts/sec
    sell_rate: float                 # aggressive sells, contracts/sec
    net_rate: float                  # buy_rate - sell_rate
    last_price: float                # most recent tick's price
    last_tick_age_seconds: float     # wall-clock seconds since last tick (for health)
    tick_count_in_window: int        # diagnostic: ticks currently in rolling buffer


class IntensityProcessor:
    """Consumes TickEvents, exposes smoothed aggression rates.

    Args:
        window_seconds: smoothing window length. 10 for NQ/MNQ, 15-20 for GC/MGC.
    """

    def __init__(self, window_seconds: float = 10.0):
        if window_seconds <= 0:
            raise ValueError(f"window_seconds must be positive, got {window_seconds}")

        self.window_seconds = window_seconds

        # Buffer entries: (timestamp, size, signed_size)
        #   timestamp: tick's event timestamp (UTC, from the adapter)
        #   size: positive int, contracts traded
        #   signed_size: +size for buys, -size for sells
        self._buffer: deque[tuple[datetime, int, int]] = deque()

        # Most recent tick info, cached for get_state() without re-walking.
        self._last_tick_ts: Optional[datetime] = None
        self._last_price: float = 0.0

        # Server-side history of sampled states (Phase 7a). Appended by
        # record_snapshot() from the pipeline's asyncio loop, read by the
        # dashboard callback from the Flask thread — hence the lock (see
        # module docstring). Oldest snapshots roll off automatically.
        self._history: deque[ProcessorState] = deque(maxlen=HISTORY_MAXLEN)
        self._history_lock = threading.Lock()

        # Diagnostics.
        self.ticks_consumed = 0

    async def consume(self, adapter: TickAdapter) -> None:
        """Drain the adapter's stream into the rolling buffer forever.

        Intended to run as an asyncio Task. Returns when the adapter's
        stream is exhausted (CSV/replay) or never (live).
        """
        async for tick in adapter.stream():
            self._ingest(tick)
        logger.info(
            "Processor consume() finished: %d ticks consumed",
            self.ticks_consumed,
        )

    def _ingest(self, tick: TickEvent) -> None:
        """Add one tick to the buffer. Called from consume()."""
        signed = tick.size if tick.aggressor_side == "buy" else -tick.size
        self._buffer.append((tick.timestamp, tick.size, signed))
        self._last_tick_ts = tick.timestamp
        self._last_price = tick.price
        self.ticks_consumed += 1

    def get_state(self, now: Optional[datetime] = None) -> ProcessorState:
        """Compute and return the current smoothed state.

        Args:
            now: optional 'as of' time for age calculation. Defaults to
                wall-clock UTC now. Pass an explicit time during replay
                tests if you want age computed against tick time rather
                than wall time.

        Returns:
            ProcessorState. If the buffer is empty, returns a zero-state
            with timestamp = now (or epoch if no `now` provided).
        """
        if now is None:
            now = datetime.now(tz=timezone.utc)

        # Empty buffer: return zero-state.
        if not self._buffer or self._last_tick_ts is None:
            return ProcessorState(
                timestamp=now,
                buy_rate=0.0,
                sell_rate=0.0,
                net_rate=0.0,
                last_price=0.0,
                last_tick_age_seconds=float("inf"),
                tick_count_in_window=0,
            )

        # Prune anything older than (last_tick_ts - window_seconds).
        # We anchor pruning to the LATEST TICK, not wall-clock now,
        # because:
        #   (a) during replay, tick time and wall time can diverge;
        #   (b) during a quiet period live, anchoring to wall-clock
        #       would correctly show rates decaying to zero, which is
        #       desirable — BUT we want the buffer contents to remain
        #       interpretable as "the last N seconds of activity",
        #       which means pruning by tick time.
        # The rate calculation below uses window_seconds as the divisor
        # regardless, so a quiet period naturally produces low rates
        # because the numerator is small.
        cutoff = self._last_tick_ts.timestamp() - self.window_seconds
        while self._buffer and self._buffer[0][0].timestamp() < cutoff:
            self._buffer.popleft()

        # Sum sizes by side. Single pass over the (now-pruned) deque.
        buy_sum = 0
        sell_sum = 0
        for _ts, size, signed in self._buffer:
            if signed > 0:
                buy_sum += size
            else:
                sell_sum += size

        buy_rate = buy_sum / self.window_seconds
        sell_rate = sell_sum / self.window_seconds
        net_rate = buy_rate - sell_rate

        age = (now - self._last_tick_ts).total_seconds()

        return ProcessorState(
            timestamp=self._last_tick_ts,
            buy_rate=buy_rate,
            sell_rate=sell_rate,
            net_rate=net_rate,
            last_price=self._last_price,
            last_tick_age_seconds=age,
            tick_count_in_window=len(self._buffer),
        )

    # ------------------------------------------------------------------
    # Server-side history (Phase 7a)
    # ------------------------------------------------------------------

    def record_snapshot(self, now: Optional[datetime] = None) -> None:
        """Sample the current state into the server-side history.

        Intended to be called at a fixed 5 Hz cadence by a background
        task on the same loop as consume(). Warmup zero-states (nothing
        ingested yet) are skipped so their last_price=0.0 can't pollute
        the price auto-scale downstream — the same rule the browser-side
        history followed before Phase 7a. No backfill: history begins at
        the moment the sampler starts running.
        """
        state = self.get_state(now)
        if state.tick_count_in_window == 0:
            return
        with self._history_lock:
            self._history.append(state)

    def get_history_window(
        self, start_ts: datetime, end_ts: datetime
    ) -> list[ProcessorState]:
        """Return snapshots whose timestamp falls within [start_ts, end_ts].

        Linear scan over the history deque. Capacity caps at
        HISTORY_MAXLEN (9,000) entries, so this stays cheap at the
        dashboard's 5 Hz read cadence.
        """
        with self._history_lock:
            return [s for s in self._history if start_ts <= s.timestamp <= end_ts]

    def get_recent_history(self, seconds: float) -> list[ProcessorState]:
        """Return snapshots from the trailing `seconds` of history.

        Anchored to the NEWEST SNAPSHOT's timestamp, not wall-clock now,
        for the same reason get_state() prunes by tick time: during
        replay, tick time and wall time diverge, and the visible window
        should track the data, not the clock.
        """
        with self._history_lock:
            if not self._history:
                return []
            end_ts = self._history[-1].timestamp
        return self.get_history_window(end_ts - timedelta(seconds=seconds), end_ts)

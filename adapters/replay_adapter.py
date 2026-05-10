"""
Replay adapter: wraps another TickAdapter and adds realistic timing.

Purpose:
    A bare CSV adapter yields ticks as fast as the disk can read them.
    For visual testing and chart development, we want ticks to emerge
    at the same wall-clock pace they did historically. That's this.

Design:
    Composition, not inheritance. ReplayAdapter takes any TickAdapter
    in its constructor and yields the same TickEvent objects, just
    spaced out in real time.

    A future live adapter (DatabentoLiveAdapter, IQFeedAdapter, etc.)
    does NOT use ReplayAdapter, because the network already provides
    the timing for free.

Session filtering:
    Optionally, the adapter skips ticks outside a configured session
    window (e.g. RTH 8:30 AM - 3:00 PM CST). Pre-window ticks are
    consumed and discarded without sleeping (fast skip-ahead).
    Post-window ticks terminate the stream.

Speed multiplier:
    speed=1.0 means real-time; speed=10.0 means 10x faster (an hour
    in 6 minutes); speed=0 or negative is rejected.

Catch-up policy:
    If we fall behind real-time (the consumer is slow, or we paused
    in a debugger), we yield without sleeping until we're caught up.
    We never drop ticks. The processor's view of activity stays
    truthful even if pacing slips.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time as dt_time, timedelta
from typing import AsyncIterator, Optional
from zoneinfo import ZoneInfo

from .base import TickAdapter, TickEvent, HealthStatus


logger = logging.getLogger(__name__)


class ReplayAdapter(TickAdapter):
    """Wraps another TickAdapter, replays its ticks with realistic timing.

    Args:
        inner: the wrapped adapter (typically a CSVAdapter)
        speed: speed multiplier; 1.0 = real-time, 10.0 = 10x faster
        session_start: optional wall-clock start time (e.g. time(8, 30))
        session_end: optional wall-clock end time (e.g. time(15, 0))
        session_timezone: IANA timezone name for interpreting the times
    """

    def __init__(
        self,
        inner: TickAdapter,
        speed: float = 1.0,
        session_start: Optional[dt_time] = None,
        session_end: Optional[dt_time] = None,
        session_timezone: str = "America/Chicago",
    ):
        if speed <= 0:
            raise ValueError(f"speed must be positive, got {speed}")

        self.inner = inner
        self.speed = speed
        self.session_start = session_start
        self.session_end = session_end
        self.session_tz = ZoneInfo(session_timezone)

        # Counters parallel CSVAdapter's for diagnostics.
        self.ticks_yielded = 0
        self.skipped_pre_session = 0
        self.skipped_post_session = 0
        self.behind_count = 0  # times we yielded without sleeping (catch-up)

    async def connect(self) -> None:
        await self.inner.connect()
        logger.info(
            "ReplayAdapter connected (speed=%sx, session=%s-%s %s)",
            self.speed,
            self.session_start, self.session_end,
            self.session_tz.key,
        )

    async def stream(self) -> AsyncIterator[TickEvent]:
        # Track wall-clock pacing relative to tick timestamps.
        # The "anchor" is set on the first in-window tick: it pairs that
        # tick's timestamp with the wall-clock moment we received it.
        # Every subsequent tick computes its scheduled wall-clock
        # delivery time relative to that anchor.
        anchor_tick_ts: Optional[datetime] = None
        anchor_wall: Optional[float] = None

        async for tick in self.inner.stream():
            # Session filtering. Convert the tick's UTC timestamp into
            # the configured session timezone, then compare time-of-day.
            #
            # Subtlety: CSV files often span multiple days (e.g. CME's
            # Globex session opens the previous evening). A tick at
            # 19:00 CST on Wednesday is BEFORE 8:30am CST Thursday —
            # we should skip-and-continue, not terminate. So we only
            # enforce session_end after we've yielded at least one
            # in-window tick (i.e. once we've actually entered the
            # session).
            if self.session_start is not None or self.session_end is not None:
                local = tick.timestamp.astimezone(self.session_tz).time()
                in_window = True
                if self.session_start is not None and local < self.session_start:
                    in_window = False
                if self.session_end is not None and local >= self.session_end:
                    in_window = False

                if not in_window:
                    if self.ticks_yielded == 0:
                        # Haven't entered the session yet — keep scanning.
                        self.skipped_pre_session += 1
                        continue
                    else:
                        # We've been in-session and now we're out. Done.
                        self.skipped_post_session += 1
                        break

            # Pacing. First in-window tick sets the anchor and yields
            # immediately — no point sleeping before the first tick.
            if anchor_tick_ts is None:
                anchor_tick_ts = tick.timestamp
                anchor_wall = asyncio.get_event_loop().time()
                self.ticks_yielded += 1
                yield tick
                continue

            # Compute when this tick should be delivered, in wall-clock
            # seconds since the anchor.
            tick_offset = (tick.timestamp - anchor_tick_ts).total_seconds()
            scheduled_wall = anchor_wall + (tick_offset / self.speed)
            now_wall = asyncio.get_event_loop().time()
            sleep_for = scheduled_wall - now_wall

            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            else:
                # We're behind schedule. Yield immediately, never drop.
                self.behind_count += 1

            self.ticks_yielded += 1
            yield tick

        logger.info(
            "ReplayAdapter exhausted: yielded=%d pre_skip=%d post_skip=%d behind=%d",
            self.ticks_yielded,
            self.skipped_pre_session,
            self.skipped_post_session,
            self.behind_count,
        )

    async def disconnect(self) -> None:
        await self.inner.disconnect()

    @property
    def health(self) -> HealthStatus:
        return self.inner.health

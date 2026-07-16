"""
Per-instrument configuration.

Each instrument key maps to a dict of defaults used by the dashboard
and processor at startup. The dashboard's --instrument arg picks one
of these keys; everything else (smoothing, tick size, session window,
price decimals, default symbol filter) is read from here.

The 'symbol' field is the Databento contract code used to filter the
CSV. This is a starting point — for live data you'll resolve the
front-month contract dynamically (a Phase 6 concern). For now, edit
this file when you roll to a new contract month.
"""

from __future__ import annotations

from datetime import time as dt_time
from typing import TypedDict


class InstrumentConfig(TypedDict):
    display_name: str          # shown in the dashboard title
    symbol: str                # Databento contract code, e.g. "MNQM6"
    live_symbol: str           # continuous form for live, e.g. "MNQ.c.0"
    tick_size: float           # smallest price increment
    price_decimals: int        # number of decimal places to display
    smoothing_seconds: float   # rolling window for the intensity rates
    session_start: dt_time     # RTH start in the session timezone
    session_end: dt_time       # RTH end in the session timezone
    session_timezone: str      # IANA tz name


INSTRUMENTS: dict[str, InstrumentConfig] = {
    "MNQ": {
        "display_name": "Micro Nasdaq",
        "symbol": "MNQM6",
        "live_symbol": "MNQ.c.0",
        "tick_size": 0.25,
        "price_decimals": 2,
        "smoothing_seconds": 10.0,
        "session_start": dt_time(8, 30),
        "session_end": dt_time(15, 0),
        "session_timezone": "America/Chicago",
    },
    "NQ": {
        "display_name": "E-mini Nasdaq",
        "symbol": "NQM6",
        "live_symbol": "NQ.c.0",
        "tick_size": 0.25,
        "price_decimals": 2,
        "smoothing_seconds": 10.0,
        "session_start": dt_time(8, 30),
        "session_end": dt_time(15, 0),
        "session_timezone": "America/Chicago",
    },
    "MGC": {
        "display_name": "Micro Gold",
        "symbol": "MGCM6",
        "live_symbol": "MGC.c.0",
        "tick_size": 0.10,
        "price_decimals": 2,
        # GC is slower than NQ; longer smoothing window per the spec.
        "smoothing_seconds": 17.0,
        # Gold's most active window is roughly 7:20 AM - 12:30 PM CST
        # (the "core" pit-equivalent hours). Adjust if you find a
        # different window works better for your read.
        "session_start": dt_time(7, 20),
        "session_end": dt_time(12, 30),
        "session_timezone": "America/Chicago",
    },
    "GC": {
        "display_name": "Gold",
        "symbol": "GCM6",
        "live_symbol": "GC.c.0",
        "tick_size": 0.10,
        "price_decimals": 2,
        "smoothing_seconds": 17.0,
        "session_start": dt_time(7, 20),
        "session_end": dt_time(12, 30),
        "session_timezone": "America/Chicago",
    },
    "ES": {
        "display_name": "E-mini S&P 500",
        "symbol": "ESM6",
        "live_symbol": "ES.c.0",
        "tick_size": 0.25,
        "price_decimals": 2,
        "smoothing_seconds": 10.0,
        "session_start": dt_time(8, 30),
        "session_end": dt_time(15, 0),
        "session_timezone": "America/Chicago",
    },
    "MES": {
        "display_name": "Micro E-mini S&P 500",
        "symbol": "MESM6",
        "live_symbol": "MES.c.0",
        "tick_size": 0.25,
        "price_decimals": 2,
        "smoothing_seconds": 10.0,
        "session_start": dt_time(8, 30),
        "session_end": dt_time(15, 0),
        "session_timezone": "America/Chicago",
    },
}


def get(instrument: str) -> InstrumentConfig:
    """Look up an instrument config by key, case-insensitively."""
    key = instrument.upper()
    if key not in INSTRUMENTS:
        available = ", ".join(sorted(INSTRUMENTS.keys()))
        raise KeyError(f"Unknown instrument {instrument!r}. Available: {available}")
    return INSTRUMENTS[key]

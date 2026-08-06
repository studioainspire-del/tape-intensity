"""
Phase 5: Dash dashboard for the tape intensity indicator.

Now driven by per-instrument config (config/instruments.py) and
command-line arguments. Run with --help to see options.

Examples:
    python -m app.dashboard
    python -m app.dashboard MNQ
    python -m app.dashboard MNQ --csv sample_data/glbx-mdp3-20260410.trades.csv
    python -m app.dashboard MGC --csv data/gold_april10.csv --speed 5.0

Architecture:
    Main thread: Dash/Flask server.
    Background thread: asyncio loop, owns the adapter + consume task,
        plus (Phase 7a) the 5 Hz sampler task that records history
        snapshots server-side.
    Shared state: the IntensityProcessor singleton, which now also owns
        the history buffer. The callback reads slices of it; it no
        longer accumulates history browser-side, so backgrounded tabs
        and page reloads no longer lose or sparsify history.
    Scrollback (Phase 7b): dragging the chart pans back through the
        server-side history; a LIVE badge (top-right, shown only while
        panned) snaps back to the live edge. View state lives in module
        globals — see the scrollback section below.
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import logging
import os
import threading
import time as _time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import plotly.graph_objects as go
from dash import Dash, ctx, dcc, html, no_update, Input, Output, callback
from plotly.subplots import make_subplots

from adapters.csv_adapter import CSVAdapter
from adapters.live_databento import DatabentoLiveAdapter
from adapters.replay_adapter import ReplayAdapter
from config import instruments as instrument_config
from processor.intensity import HISTORY_MAXLEN, IntensityProcessor


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Display configuration
# ---------------------------------------------------------------------------

VISIBLE_WINDOW_SECONDS = 180   # 3-minute rolling view
POLL_INTERVAL_MS = 200         # 5 Hz refresh; also the sampler cadence

# Scrollback (Phase 7b): pan whose right edge lands within this many
# seconds of the newest snapshot counts as "returned to live".
LIVE_SNAP_SECONDS = 2.0
# Full buffer span in seconds, derived from the processor's capacity and
# the 5 Hz sampler cadence (= 30 minutes). Used to fetch the buffer
# extent when interpreting a pan.
HISTORY_SPAN_SECONDS = HISTORY_MAXLEN * (POLL_INTERVAL_MS / 1000.0)

# RVOL (Phase 8): current pace vs. recent baseline, as a multiple.
# "Is this minute busier than the last five minutes?" Both windows are
# trailing and span-normalized, so partial windows early in the session
# degrade gracefully instead of spiking.
RVOL_FAST_SECONDS = 60.0          # numerator window ("this minute")
RVOL_SLOW_SECONDS = 300.0         # baseline window ("last five minutes")
RVOL_MIN_BASELINE_SECONDS = 120.0  # below this much history, show no RVOL

# Live uirevision stability bucket. The live-mode uirevision advances
# with the live edge quantized to this many seconds. It must change
# often enough that the x-axis keeps scrolling and both y-axes keep
# auto-scaling (a constant key is the gotcha #4 lockout), but stay
# constant across short windows so Plotly does NOT re-autorange
# mid-gesture and clobber a pan-back before it registers as scrollback.
# ~1s balances a live-feeling scroll against solid pan entry.
LIVE_UIREV_BUCKET_SECONDS = 1.0

COLOR_BUY = "#00d68f"
COLOR_SELL = "#ff5b5b"
COLOR_PRICE = "#ffffff"
COLOR_NET = "#ffcc00"
COLOR_CVD = "#5ab4ff"
COLOR_RVOL = "#c58cff"
COLOR_BG = "#111111"
COLOR_FG = "#cccccc"
# Neutral-reference lines: zero on the intensity and CVD panels, 1.0x on
# RVOL (a ratio's "zero"). White so the sign flip reads at a glance —
# these are the lines the eye checks against, so they outrank gridlines.
COLOR_ZEROLINE = "#ffffff"

# LIVE badge styling (Phase 7b): subtle corner-mounted button, only
# visible while panned back. Absolute positioning relies on the graph's
# wrapper div being position:relative.
_LIVE_BTN_BASE = {
    "position": "absolute",
    "top": "10px",
    "right": "16px",
    "zIndex": 10,
    "fontFamily": "monospace",
    "fontSize": "12px",
    "letterSpacing": "1px",
    "color": COLOR_FG,
    "backgroundColor": "rgba(255, 255, 255, 0.08)",
    "border": "1px solid #444444",
    "borderRadius": "3px",
    "padding": "4px 10px",
    "cursor": "pointer",
}
_LIVE_BTN_HIDDEN = {**_LIVE_BTN_BASE, "display": "none"}
_LIVE_BTN_VISIBLE = {**_LIVE_BTN_BASE, "display": "block"}

# Clear-anchor button (Phase 8): sits in the toggles row, only visible
# while a CVD anchor is dropped.
_CLEAR_BTN_BASE = {
    "fontFamily": "monospace",
    "fontSize": "11px",
    "color": COLOR_FG,
    "backgroundColor": "rgba(255, 255, 255, 0.08)",
    "border": "1px solid #444444",
    "borderRadius": "3px",
    "padding": "2px 8px",
    "cursor": "pointer",
    "marginLeft": "24px",
}
_CLEAR_BTN_HIDDEN = {**_CLEAR_BTN_BASE, "display": "none"}
_CLEAR_BTN_VISIBLE = {**_CLEAR_BTN_BASE, "display": "inline-block"}


# ---------------------------------------------------------------------------
# Runtime state (populated by main() before the server starts)
# ---------------------------------------------------------------------------

# These are set in main() once we have parsed args + loaded the instrument
# config. They're module-level so the Dash callback can see them.
processor: IntensityProcessor
display_tz: ZoneInfo
display_name: str
symbol: str
price_decimals: int

# Staleness tracking for the health light.
_last_seen_tick_ts: Optional[datetime] = None
_last_seen_wall: float = 0.0

# Scrollback view state (Phase 7b). Module globals rather than a
# dcc.Store: this is a single-user dashboard with one server process,
# and the staleness trackers above already follow this pattern.
# _panned_end_ts anchors the frozen slice absolutely — the offset alone
# would drift forward as the live edge advances between refreshes.
_view_mode: str = "live"                   # "live" | "panned"
_view_offset_seconds: float = 0.0          # seconds behind the live edge
_panned_end_ts: Optional[datetime] = None  # right edge of the frozen slice
# Bumped on every panned -> live transition and folded into the live
# uirevision key. Plotly stores the user's pan state keyed to the
# uirevision value active when they dragged; if the post-scrollback
# live figure reused the pre-scrollback key, Plotly would RESTORE the
# stale drag position instead of snapping to the live edge (and the
# relayout echo of that restore would flip the server back to panned).
_live_epoch: int = 0


def _set_live() -> None:
    """Reset scrollback state to the live edge."""
    global _view_mode, _view_offset_seconds, _panned_end_ts, _live_epoch
    if _view_mode != "live":
        # Invalidate drag state stored under the previous live key.
        # Only on a real transition — bumping while already live would
        # reset the user's in-live zoom on every near-edge pan.
        _live_epoch += 1
    _view_mode = "live"
    _view_offset_seconds = 0.0
    _panned_end_ts = None


# CVD anchor state (Phase 8). The anchor's CVD value is captured as a
# scalar at drop time — NOT looked up in history later — so the Δ
# readout keeps working all session, even after the anchor's timestamp
# rolls off the 30-minute history buffer.
_cvd_anchor_ts: Optional[datetime] = None
_cvd_anchor_value: Optional[int] = None

# Last tick timestamp the live figure was built from. Lets tick
# refreshes skip rebuilding when no new tick arrived: the figure would
# be pixel-identical, and each needless Plotly.react clears the
# browser's hover state, which makes anchor clicks unreliable during
# quiet stretches.
_last_live_build_ts: Optional[datetime] = None

# Last-seen n_clicks per button. Button presses are detected by VALUE
# CHANGE, not by ctx trigger attribution: with the 5 Hz interval, a
# press that lands while a callback request is in flight gets coalesced
# into the next tick-triggered request — changedPropIds then says
# "tick" even though the click counter advanced, and trigger-based
# routing silently drops the press (observed ~1-in-6 under load).
# A value smaller than the stored one means the page reloaded
# (n_clicks reset); re-sync without treating it as a press.
_btn_seen = {"live": 0, "drop": 0, "clear": 0}

# Toggle set the current figure was built with. Same coalescing story
# as _btn_seen: a checklist change folded into a tick request arrives
# attributed to "tick", so the tick-skip branches must compare values,
# not trust the trigger, or a coalesced toggle change never renders.
_last_build_toggles: Optional[frozenset] = None

# Last-seen relayoutData / clickData, for the same value-diff reason as
# _btn_seen: every request carries the current values of all Inputs, so
# comparing values catches pans and chart clicks whose trigger was
# coalesced into an in-flight tick request. Single-deep on purpose: a
# rare late-arriving stale request briefly re-applies an old value, and
# the next tick (carrying the current value) corrects it — whereas
# deeper matching would permanently swallow a legitimate repeat.
_relayout_seen: Optional[dict] = None
_click_seen: Optional[dict] = None

# Serializes update_chart. Flask's threaded server otherwise interleaves
# concurrent requests mid-body, corrupting the seen-state bookkeeping
# above in arbitrary orders. Single user + 5 Hz: contention is nil.
_callback_lock = threading.Lock()


def _button_pressed(name: str, value: Optional[int]) -> bool:
    """True when this button's n_clicks advanced since last callback.

    Monotonic: concurrent requests race, and a tick request sent before
    a press but processed after it carries the OLD count — regressing
    the stored value on that would make the next request a phantom
    press. Only a true zero (page reload) resets.
    """
    value = value or 0
    if value == 0:
        _btn_seen[name] = 0
        return False
    pressed = value > _btn_seen[name]
    _btn_seen[name] = max(_btn_seen[name], value)
    return pressed


def _clear_anchor() -> None:
    """Remove the CVD anchor."""
    global _cvd_anchor_ts, _cvd_anchor_value
    _cvd_anchor_ts = None
    _cvd_anchor_value = None


def _parse_axis_ts(raw) -> Optional[datetime]:
    """Parse a Plotly date-axis value (naive string in the display tz)."""
    try:
        return datetime.fromisoformat(str(raw)).replace(tzinfo=display_tz)
    except ValueError:
        return None


def _drop_anchor_at(target: Optional[datetime]) -> bool:
    """Drop the CVD anchor at (or just before) the target time.

    target=None anchors at the newest snapshot. Returns True when the
    anchor changed. The anchor snaps to the history sample at/before
    the target and captures that sample's CVD value, so anchors can be
    dropped on a panned (historical) slice and the Δ is measured from
    that moment.
    """
    global _cvd_anchor_ts, _cvd_anchor_value

    hist = processor.get_recent_history(HISTORY_SPAN_SECONDS)
    if not hist:
        return False
    if target is None:
        sample = hist[-1]
    else:
        ts_list = [s.timestamp.timestamp() for s in hist]
        idx = max(bisect.bisect_right(ts_list, target.timestamp()) - 1, 0)
        sample = hist[idx]
    changed = sample.timestamp != _cvd_anchor_ts
    _cvd_anchor_ts = sample.timestamp
    _cvd_anchor_value = sample.cvd
    return changed


def _handle_click(click_data: Optional[dict]) -> bool:
    """Drop the CVD anchor at the clicked chart time (secondary path).

    Chart clicks only reach the server while Plotly's hover pipeline
    has points under the cursor, which is not fully reliable on a
    frequently-updating figure — the drop-anchor button is the primary,
    always-reliable path. This stays wired for when it works: clicking
    a spot is more precise than the button.
    """
    if not click_data or not click_data.get("points"):
        return False
    clicked = _parse_axis_ts(click_data["points"][0].get("x"))
    if clicked is None:
        return False
    return _drop_anchor_at(clicked)


def _rvol_at(ts_list: list[float], cum_list: list[int],
             t: float, cum_now: int) -> Optional[float]:
    """RVOL at epoch-seconds t, given parallel timestamp/volume arrays.

    Rate over the trailing fast window divided by rate over the trailing
    slow window, each normalized by the span actually found in history.
    None (a gap) when the baseline is too short or empty.
    """
    i_fast = bisect.bisect_right(ts_list, t - RVOL_FAST_SECONDS) - 1
    if i_fast < 0:
        return None
    i_slow = max(bisect.bisect_right(ts_list, t - RVOL_SLOW_SECONDS) - 1, 0)
    span_slow = t - ts_list[i_slow]
    span_fast = t - ts_list[i_fast]
    if span_slow < RVOL_MIN_BASELINE_SECONDS or span_fast <= 0:
        return None
    rate_fast = (cum_now - cum_list[i_fast]) / span_fast
    rate_slow = (cum_now - cum_list[i_slow]) / span_slow
    if rate_slow <= 0:
        return None
    return rate_fast / rate_slow


def _rvol_series(recent: list, ext: list) -> list[Optional[float]]:
    """RVOL for each visible sample, using the extended (lookback) slice."""
    ts_list = [s.timestamp.timestamp() for s in ext]
    cum_list = [s.cum_volume for s in ext]
    return [
        _rvol_at(ts_list, cum_list, s.timestamp.timestamp(), s.cum_volume)
        for s in recent
    ]


def _current_rvol() -> Optional[float]:
    """Latest RVOL value, for the (always-live) status bar."""
    ext = processor.get_recent_history(RVOL_SLOW_SECONDS + 2.0)
    if not ext:
        return None
    ts_list = [s.timestamp.timestamp() for s in ext]
    cum_list = [s.cum_volume for s in ext]
    return _rvol_at(ts_list, cum_list, ts_list[-1], ext[-1].cum_volume)


def _handle_relayout(relayout_data: Optional[dict]) -> bool:
    """Update scrollback state from a chart relayout event.

    Returns True when the view state actually changed (mode or frozen
    window), False otherwise. Ignoring no-op events matters: our own
    axis-range updates can echo back as relayout events, and rebuilding
    on those would fight the user's drag.
    """
    global _view_mode, _view_offset_seconds, _panned_end_ts

    if not relayout_data:
        return False
    prev = (_view_mode, _panned_end_ts)

    def _changed() -> bool:
        return (_view_mode, _panned_end_ts) != prev

    # Double-click autosize: snap back to live. (We never emit autorange
    # figures while panned — panned figures carry an explicit x-range —
    # so an autorange event here is always a user action.)
    if relayout_data.get("xaxis.autorange"):
        logger.debug("relayout: autorange -> live (was %s)", _view_mode)
        _set_live()
        return _changed()

    right_raw = relayout_data.get("xaxis.range[1]")
    if right_raw is None:
        # Y-only drag, dragmode change, etc. — not a horizontal pan.
        return False

    # Buffer extent: oldest and newest snapshot timestamps.
    hist = processor.get_recent_history(HISTORY_SPAN_SECONDS)
    if not hist:
        _set_live()
        return _changed()

    right_edge = _parse_axis_ts(right_raw)
    if right_edge is None:
        return False
    logger.debug("relayout: right_edge=%s mode=%s", right_edge, _view_mode)

    earliest, latest = hist[0].timestamp, hist[-1].timestamp

    if (latest - right_edge).total_seconds() <= LIVE_SNAP_SECONDS:
        # Panned (or zoomed) with the right edge at the live edge.
        _set_live()
        return _changed()

    # Hard stop at the buffer edge: the window's left side never slides
    # past the earliest snapshot. Until the buffer holds one full window
    # there is nothing to pan into, so stay live.
    min_end = earliest + timedelta(seconds=VISIBLE_WINDOW_SECONDS)
    if min_end >= latest:
        _set_live()
        return _changed()

    end = min(max(right_edge, min_end), latest)
    _view_mode = "panned"
    _view_offset_seconds = (latest - end).total_seconds()
    _panned_end_ts = end
    logger.debug("relayout -> panned: offset=%.1fs end=%s (changed=%s)",
                 _view_offset_seconds, end, _changed())
    return _changed()


# ---------------------------------------------------------------------------
# Background pipeline thread
# ---------------------------------------------------------------------------

def run_data_pipeline(
    *,
    mode: str,
    # CSV/replay mode args:
    csv_path: Optional[Path],
    symbol_filter: Optional[str],
    speed: float,
    session_start,
    session_end,
    session_timezone: str,
    # Live mode args:
    live_symbol: Optional[str],
) -> None:
    """Run the adapter -> processor pipeline forever in this thread.

    mode='replay': construct CSV + Replay adapters as before.
    mode='live':   construct the Databento live adapter.
    """
    async def _sampler():
        # Phase 7a: record a history snapshot every 200ms, matching the
        # dashboard poll cadence. Runs on the same loop as consume(), so
        # snapshots interleave with ingestion without cross-thread races
        # on the tick buffer. No backfill — history starts here.
        while True:
            processor.record_snapshot()
            await asyncio.sleep(POLL_INTERVAL_MS / 1000.0)

    async def _pipeline():
        if mode == "live":
            adapter = DatabentoLiveAdapter(
                symbol=live_symbol,
                stype_in="continuous",
            )
        else:
            inner = CSVAdapter(csv_path, symbol_filter=symbol_filter)
            adapter = ReplayAdapter(
                inner,
                speed=speed,
                session_start=session_start,
                session_end=session_end,
                session_timezone=session_timezone,
            )

        await adapter.connect()
        try:
            consume_task = asyncio.create_task(processor.consume(adapter))
            sampler_task = asyncio.create_task(_sampler())
            # consume() finishes when the stream is exhausted (replay) or
            # dies (live disconnect); the sampler alone never finishes.
            # Whichever ends first, cancel the other so the pipeline
            # winds down instead of hanging on the survivor.
            done, pending = await asyncio.wait(
                {consume_task, sampler_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                task.result()  # surface any pipeline error
        finally:
            await adapter.disconnect()

    asyncio.run(_pipeline())


# ---------------------------------------------------------------------------
# Dash app — layout built once, after main() configures globals
# ---------------------------------------------------------------------------

app = Dash(__name__, update_title=None)


def _build_layout() -> html.Div:
    """Construct the page layout. Called from main() after globals are set."""
    return html.Div(
        style={
            "backgroundColor": COLOR_BG,
            "color": COLOR_FG,
            "fontFamily": "monospace",
            "padding": "12px",
            "minHeight": "100vh",
        },
        children=[
            html.Div(
                style={"display": "flex", "justifyContent": "space-between",
                       "alignItems": "center", "marginBottom": "8px"},
                children=[
                    html.H2(f"Tape Intensity ({display_name} {symbol})",
                            style={"margin": 0}),
                    html.Div(id="status-bar", style={"fontSize": "14px"}),
                ],
            ),
            html.Div(
                style={"marginBottom": "4px", "display": "flex",
                       "alignItems": "center"},
                children=[
                    dcc.Checklist(
                        id="line-toggles",
                        options=[
                            {"label": " buy", "value": "buy"},
                            {"label": " sell", "value": "sell"},
                            {"label": " net delta", "value": "net"},
                            {"label": " price", "value": "price"},
                            {"label": " cvd", "value": "cvd"},
                            {"label": " rvol", "value": "rvol"},
                        ],
                        value=["buy", "sell", "net", "price", "cvd", "rvol"],
                        style={"color": COLOR_FG, "fontSize": "13px",
                               "display": "flex", "gap": "16px"},
                        inputStyle={"marginRight": "6px"},
                    ),
                    html.Button(
                        # Primary anchor path: always reliable (plain
                        # Dash callback). Anchors at the live edge, or
                        # at the frozen moment while panned back.
                        "drop Δ anchor",
                        id="drop-anchor-button",
                        n_clicks=0,
                        style=_CLEAR_BTN_VISIBLE,
                    ),
                    html.Button(
                        "clear Δ anchor",
                        id="clear-anchor-button",
                        n_clicks=0,
                        style=_CLEAR_BTN_HIDDEN,
                    ),
                ],
            ),
            html.Div(
                # Relative wrapper so the LIVE badge can corner-mount
                # over the chart with absolute positioning. The id is
                # also the anchor assets/crosshair.js attaches to.
                id="chart-wrapper",
                style={"position": "relative"},
                children=[
                    dcc.Graph(
                        id="intensity-chart",
                        style={"height": "70vh"},
                        config={
                            # Drag to pan, scroll wheel to zoom, double-click to autoscale.
                            # Hides the modebar entirely since we don't need its other tools.
                            "displayModeBar": False,
                            "scrollZoom": True,
                            "doubleClick": "autosize",
                        },
                    ),
                    html.Button(
                        "LIVE",
                        id="live-button",
                        n_clicks=0,
                        style=_LIVE_BTN_HIDDEN,
                    ),
                ],
            ),
            dcc.Interval(id="tick", interval=POLL_INTERVAL_MS, n_intervals=0),
        ],
    )


def _health_from_age(age_seconds: float) -> tuple[str, str]:
    if age_seconds < 1.0:
        return ("#00d68f", "GREEN")
    if age_seconds < 5.0:
        return ("#ffcc00", "YELLOW")
    return ("#ff5b5b", "RED")


@callback(
    Output("intensity-chart", "figure"),
    Output("status-bar", "children"),
    Output("live-button", "style"),
    Output("clear-anchor-button", "style"),
    Input("tick", "n_intervals"),
    Input("line-toggles", "value"),
    Input("intensity-chart", "relayoutData"),
    Input("intensity-chart", "clickData"),
    Input("live-button", "n_clicks"),
    Input("drop-anchor-button", "n_clicks"),
    Input("clear-anchor-button", "n_clicks"),
)
def update_chart(*args):
    # Serialize concurrent requests — see _callback_lock.
    with _callback_lock:
        return _update_chart(*args)


def _update_chart(
    _n: int,
    toggles: list[str],
    relayout_data: Optional[dict],
    click_data: Optional[dict],
    _live_clicks: int,
    _drop_clicks: int,
    _clear_clicks: int,
):
    global _last_seen_tick_ts, _last_seen_wall, _last_live_build_ts, \
        _last_build_toggles

    toggles = toggles or []

    # Live state for the status bar. Always live values, even while
    # panned back — deliberate: pattern from the chart, numbers from
    # the bar. Do not make this offset-aware.
    state = processor.get_state()

    # Staleness tracking, wall-clock anchored.
    now_wall = _time.monotonic()
    if _last_seen_tick_ts is None or state.timestamp != _last_seen_tick_ts:
        _last_seen_tick_ts = state.timestamp
        _last_seen_wall = now_wall
    staleness_seconds = now_wall - _last_seen_wall

    # Scrollback + anchor transitions. One callback handles all inputs
    # because Dash allows only one writer per output, and every input
    # needs to redraw the same figure. Buttons are detected by n_clicks
    # value change (see _btn_seen — trigger attribution loses presses
    # coalesced into in-flight tick requests); relayoutData and
    # clickData share the component id, so those branch on the full
    # prop string rather than ctx.triggered_id.
    global _relayout_seen, _click_seen
    live_pressed = _button_pressed("live", _live_clicks)
    drop_pressed = _button_pressed("drop", _drop_clicks)
    clear_pressed = _button_pressed("clear", _clear_clicks)
    relayout_changed = relayout_data != _relayout_seen
    _relayout_seen = relayout_data
    click_changed = click_data != _click_seen
    _click_seen = click_data

    prop = ctx.triggered[0]["prop_id"] if ctx.triggered else ""
    rebuild = True
    if live_pressed or drop_pressed or clear_pressed:
        if live_pressed:
            _set_live()
        if drop_pressed:
            # Anchor at the frozen moment when panned back, else at
            # the live edge — "press it as price enters the zone".
            _drop_anchor_at(_panned_end_ts if _view_mode == "panned" else None)
        if clear_pressed:
            _clear_anchor()
    elif relayout_changed:
        # User pan/zoom (or an echo of our own axis update). Only
        # rebuild when the view state really changed, so we neither
        # fight the drag nor loop on echoes.
        rebuild = _handle_relayout(relayout_data)
    elif click_changed:
        # Click drops (or moves) the CVD anchor.
        rebuild = _handle_click(click_data)
    elif (prop == "tick.n_intervals"
          and frozenset(toggles) == _last_build_toggles
          and _view_mode == "panned"):
        # Interval tick while panned: the slice is frozen. Skip the
        # figure (no_update) but let the status bar refresh below —
        # this is why the interval stays enabled in panned mode.
        # (A changed toggle set falls through and rebuilds.)
        rebuild = False
    elif (prop == "tick.n_intervals"
          and frozenset(toggles) == _last_build_toggles
          and state.timestamp == _last_live_build_ts):
        # Live tick with no new data since the last build: the figure
        # would be identical. Skipping the rebuild also stops needless
        # Plotly.react churn from clearing hover state (see
        # _last_live_build_ts).
        rebuild = False

    btn_style = _LIVE_BTN_VISIBLE if _view_mode == "panned" else _LIVE_BTN_HIDDEN
    anchor_style = (_CLEAR_BTN_VISIBLE if _cvd_anchor_ts is not None
                    else _CLEAR_BTN_HIDDEN)
    rvol_now = _current_rvol()

    if not rebuild:
        return (no_update, _build_status(state, staleness_seconds, rvol_now),
                btn_style, anchor_style)

    # Select the visible slice plus the RVOL lookback in one fetch.
    # Live: trailing window off the newest snapshot (Phase 7a). Panned:
    # the frozen absolute window (Phase 7b). Past the buffer edge we
    # show only what exists — no query extension, the x-axis just spans
    # less time.
    fetch_span = VISIBLE_WINDOW_SECONDS + RVOL_SLOW_SECONDS
    if _view_mode == "panned" and _panned_end_ts is not None:
        ext = processor.get_history_window(
            _panned_end_ts - timedelta(seconds=fetch_span), _panned_end_ts)
        window_start = _panned_end_ts - timedelta(seconds=VISIBLE_WINDOW_SECONDS)
    else:
        ext = processor.get_recent_history(fetch_span)
        window_start = (ext[-1].timestamp
                        - timedelta(seconds=VISIBLE_WINDOW_SECONDS)) if ext else None
    recent = [s for s in ext if s.timestamp >= window_start]
    if not recent:
        return _empty_figure(), "no data", _LIVE_BTN_HIDDEN, anchor_style

    fig = _build_figure(recent, ext, toggles)
    _last_build_toggles = frozenset(toggles)
    if _view_mode == "live":
        _last_live_build_ts = state.timestamp
    return (fig, _build_status(state, staleness_seconds, rvol_now),
            btn_style, anchor_style)

def _build_figure(recent: list, ext: list, toggles: list[str]) -> go.Figure:
    """Build the stacked-panel figure for the visible slice.

    recent: states in the visible 3-minute window (all panels share x).
    ext: recent plus the RVOL lookback, for computing the RVOL series.

    Panels are rows of ONE Plotly figure with a shared x-axis, not
    separate Graph components: unified hover, pan/scrollback, and
    uirevision then apply to all panels at once, while separate graphs
    would each need their own synced pan-state machine (see CLAUDE.md,
    Phase 7b — that machinery is deliberately singular).
    """
    show_buy = "buy" in toggles
    show_sell = "sell" in toggles
    show_net = "net" in toggles
    show_price = "price" in toggles
    show_cvd = "cvd" in toggles
    show_rvol = "rvol" in toggles

    xs: list[datetime] = []
    buys: list[float] = []
    sells: list[float] = []
    nets: list[float] = []
    prices: list[float] = []
    cvds: list[int] = []

    for s in recent:
        xs.append(s.timestamp.astimezone(display_tz))
        buys.append(s.buy_rate)
        sells.append(s.sell_rate)
        nets.append(s.net_rate)
        prices.append(s.last_price)
        cvds.append(s.cvd)

    # Row layout: main intensity panel on top, then optional CVD and
    # RVOL panels. A toggled-off panel's row collapses entirely so the
    # main panel regains the height.
    rows = 1 + int(show_cvd) + int(show_rvol)
    heights = [1.0 - (0.24 if show_cvd else 0.0) - (0.16 if show_rvol else 0.0)]
    if show_cvd:
        heights.append(0.24)
    if show_rvol:
        heights.append(0.16)
    cvd_row = 2 if show_cvd else None
    rvol_row = 2 + int(show_cvd) if show_rvol else None

    fig = make_subplots(
        rows=rows, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        row_heights=heights,
        specs=[[{"secondary_y": True}]] + [[{}] for _ in range(rows - 1)],
    )

    # Intensity traces go on the RIGHT axis of the main panel
    # (secondary_y=True). This is where the eye tracks aggression
    # imbalance — the focus of the indicator. Price (when shown) is
    # context on the left.
    if show_buy:
        fig.add_trace(
            go.Scatter(x=xs, y=buys, name="Buy",
                       line=dict(color=COLOR_BUY, width=2),
                       hovertemplate="%{y:.1f} c/s<extra>buy</extra>"),
            row=1, col=1, secondary_y=True,
        )
    if show_sell:
        fig.add_trace(
            go.Scatter(x=xs, y=sells, name="Sell",
                       line=dict(color=COLOR_SELL, width=2),
                       hovertemplate="%{y:.1f} c/s<extra>sell</extra>"),
            row=1, col=1, secondary_y=True,
        )
    if show_net:
        fig.add_trace(
            go.Scatter(x=xs, y=nets, name="Net",
                       line=dict(color=COLOR_NET, width=1.5, dash="dot"),
                       hovertemplate="%{y:+.1f} c/s<extra>net</extra>"),
            row=1, col=1, secondary_y=True,
        )
    if show_price:
        fig.add_trace(
            go.Scatter(x=xs, y=prices, name="Price",
                       line=dict(color=COLOR_PRICE, width=1.5),
                       hovertemplate=f"%{{y:.{price_decimals}f}}<extra>price</extra>"),
            row=1, col=1, secondary_y=False,
        )

    # Right axis (intensity, the focus axis). Gridlines, zero anchoring,
    # and tozero rangemode all live here. If net is visible, allow
    # negative space; otherwise pin baseline at zero for a cleaner look.
    if show_net:
        fig.update_yaxes(secondary_y=True, row=1, col=1,
                         color=COLOR_FG, title_text="contracts/sec",
                         gridcolor="#222222", showgrid=True,
                         zeroline=True, zerolinecolor=COLOR_ZEROLINE,
                         zerolinewidth=1)
    else:
        fig.update_yaxes(rangemode="tozero", secondary_y=True, row=1, col=1,
                         color=COLOR_FG, title_text="contracts/sec",
                         gridcolor="#222222", showgrid=True,
                         zeroline=True, zerolinecolor=COLOR_ZEROLINE,
                         zerolinewidth=1)

    # Left axis (price, context only). Visible only when price toggle
    # is on. No gridlines — price gridlines would clutter the focus on
    # intensity.
    if show_price and prices:
        p_min, p_max = min(prices), max(prices)
        p_pad = max((p_max - p_min) * 0.1, 0.25)
        # Explicit range, re-applied every build; combined with the
        # live-edge uirevision below it takes effect on each redraw,
        # not just the first (the Phase 6 "price stuck" fix).
        fig.update_yaxes(range=[p_min - p_pad, p_max + p_pad],
                         secondary_y=False, row=1, col=1, showgrid=False,
                         color=COLOR_PRICE, title_text="",
                         visible=True)
    else:
        # Hide the left axis entirely when price is off. This gives
        # the intensity lines the full chart width on the right axis.
        fig.update_yaxes(secondary_y=False, row=1, col=1, visible=False)

    # CVD panel: session-baselined cumulative volume delta, with the
    # anchor marker when one is dropped inside the visible window.
    if show_cvd:
        fig.add_trace(
            go.Scatter(x=xs, y=cvds, name="CVD",
                       line=dict(color=COLOR_CVD, width=1.5),
                       hovertemplate="%{y:+} c<extra>cvd</extra>"),
            row=cvd_row, col=1,
        )
        fig.update_yaxes(row=cvd_row, col=1, color=COLOR_CVD,
                         title_text="cvd", gridcolor="#222222",
                         showgrid=True, zeroline=True,
                         zerolinecolor=COLOR_ZEROLINE, zerolinewidth=1)
        if _cvd_anchor_ts is not None and xs:
            anchor_local = _cvd_anchor_ts.astimezone(display_tz)
            if xs[0] <= anchor_local <= xs[-1]:
                fig.add_vline(x=anchor_local, row=cvd_row, col=1,
                              line_width=1, line_dash="dot",
                              line_color="#888888")

    # RVOL panel: unitless multiple with a 1.0x reference line. Gaps
    # (None) mean the baseline window is still too short.
    if show_rvol:
        rvols = _rvol_series(recent, ext)
        fig.add_trace(
            go.Scatter(x=xs, y=rvols, name="RVOL",
                       line=dict(color=COLOR_RVOL, width=1.5),
                       connectgaps=False,
                       hovertemplate="%{y:.2f}x<extra>rvol</extra>"),
            row=rvol_row, col=1,
        )
        # RVOL's neutral reference is 1.0x, not 0 — a ratio's "zero".
        # (0 sits on the axis floor under rangemode="tozero".) White to
        # match the other panels' reference lines; kept dotted so it
        # still reads as a ratio marker rather than a hard zero.
        fig.add_hline(y=1.0, row=rvol_row, col=1, line_width=1,
                      line_dash="dot", line_color=COLOR_ZEROLINE)
        fig.update_yaxes(row=rvol_row, col=1, color=COLOR_RVOL,
                         title_text="rvol", gridcolor="#222222",
                         showgrid=True, rangemode="tozero")

    # uirevision controls when Plotly preserves vs. resets view state
    # across redraws (Phase 6 gotcha #4).
    # Live: key on the live edge, quantized to LIVE_UIREV_BUCKET_SECONDS.
    #   Advancing the key is what makes Plotly re-apply autorange as data
    #   arrives — i.e. what keeps the x-axis scrolling and BOTH y-axes
    #   auto-scaling. A CONSTANT key is the gotcha #4 lockout: once the
    #   user zooms/pans, the view freezes and never re-follows the live
    #   edge (this was the price-off freeze — the old key was the price
    #   range, dynamic only while the price line showed). The bucket
    #   keeps the key steady across ~1s windows so react does not
    #   re-autorange mid-gesture and clobber a pan-back before it
    #   registers; a stray zoom that stays within the live edge instead
    #   self-heals at the next bucket flip. _live_epoch still prefixes
    #   it so a panned->live return can't restore a stale drag position.
    # Panned (Phase 7b): key tied to the frozen window, so the user's
    #   pan position survives refreshes of the same slice, while each
    #   NEW pan changes the key and lets our axis range through.
    if _view_mode == "panned" and _panned_end_ts is not None:
        uirev = f"panned_{int(_panned_end_ts.timestamp())}"
    else:
        # Bucket on WALL-CLOCK, not tick time: the stability window is a
        # real-interaction window (how long a human gesture takes), so
        # it must not stretch or shrink with replay speed. At live 1x
        # this equals tick time; under accelerated replay it stays a
        # true ~1s. (Same wall-clock framing as the health light.)
        bucket = int(_time.monotonic() / LIVE_UIREV_BUCKET_SECONDS)
        uirev = f"live_{_live_epoch}_{bucket}"

    fig.update_layout(
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_BG,
        font=dict(color=COLOR_FG, family="monospace"),
        margin=dict(l=50, r=50, t=20, b=40),
        showlegend=True,
        legend=dict(orientation="h", yanchor="top", y=1.08,
                    xanchor="right", x=1.0,
                    bgcolor="rgba(0,0,0,0)"),
        hovermode="x unified",
        # dragmode="pan" makes click-drag pan the chart instead of
        # drawing a zoom box. Combined with scrollZoom in the Graph
        # config and doubleClick="autosize", this gives a much more
        # natural feel: drag to pan, scroll to zoom, double-click to
        # reset.
        dragmode="pan",
        uirevision=uirev,
    )

    # Shared x styling applies to every row's axis (shared_xaxes keeps
    # them matched). Explicit range while panned implements the
    # hard-stop snap and the shorter-axis case at the buffer edge.
    # showspikes=False: the crosshair is drawn by assets/crosshair.js as a
    # DOM overlay (Plotly's own spike can only span the hovered panel, and
    # it snaps to data, so leaving it on would double the vertical line a
    # few pixels off the overlay's cursor-tracked one).
    fig.update_xaxes(color=COLOR_FG, gridcolor="#222222", showspikes=False)
    if _view_mode == "panned" and _panned_end_ts is not None:
        fig.update_xaxes(range=[xs[0], xs[-1]])

    return fig


def _build_status(state, staleness_seconds: float,
                  rvol_now: Optional[float]) -> html.Div:
    """Build the status bar. Always live values — never offset-aware."""
    health_color, health_label = _health_from_age(staleness_seconds)
    last_tick_str = state.timestamp.astimezone(display_tz).strftime("%H:%M:%S")
    price_str = f"{state.last_price:.{price_decimals}f}"

    children = [
        html.Span([
            html.Span("●", style={"color": health_color,
                                   "fontSize": "20px",
                                   "marginRight": "6px"}),
            html.Span(health_label),
        ]),
        html.Span(f"last tick: {last_tick_str} CST"),
        html.Span(f"buy {state.buy_rate:>5.1f}/s",
                  style={"color": COLOR_BUY}),
        html.Span(f"sell {state.sell_rate:>5.1f}/s",
                  style={"color": COLOR_SELL}),
        html.Span(f"net {state.net_rate:>+5.1f}/s",
                  style={"color": COLOR_NET}),
        html.Span(f"price {price_str}",
                  style={"color": COLOR_PRICE}),
        html.Span(f"cvd {state.cvd:+d}",
                  style={"color": COLOR_CVD}),
    ]
    # Δ since the dropped anchor — the "net aggression since my zone"
    # readout. Only present while an anchor is set.
    if _cvd_anchor_value is not None:
        children.append(
            html.Span(f"Δcvd {state.cvd - _cvd_anchor_value:+d}",
                      style={"color": COLOR_CVD, "fontWeight": "bold"}))
    children.append(
        html.Span(f"rvol {rvol_now:.2f}x" if rvol_now is not None
                  else "rvol --",
                  style={"color": COLOR_RVOL}))
    children.append(
        html.Span(f"buf {state.tick_count_in_window}",
                  style={"color": "#888888"}))

    return html.Div(
        style={"display": "flex", "gap": "20px", "alignItems": "center"},
        children=children,
    )


def _empty_figure() -> go.Figure:
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_layout(
        paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG,
        font=dict(color=COLOR_FG),
        margin=dict(l=50, r=50, t=20, b=40),
        xaxis=dict(color=COLOR_FG, gridcolor="#222222"),
        yaxis=dict(color=COLOR_FG, gridcolor="#222222"),
        dragmode="pan",
        annotations=[dict(text="waiting for data...",
                          xref="paper", yref="paper",
                          x=0.5, y=0.5, showarrow=False,
                          font=dict(color=COLOR_FG, size=18))],
    )
    return fig


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m app.dashboard",
        description="Tape intensity dashboard (Phase 5).",
    )
    p.add_argument(
        "instrument",
        nargs="?",
        default="MNQ",
        help="Instrument code from config/instruments.py "
             "(MNQ, NQ, MGC, GC, ES, MES). Default: MNQ",
    )
    p.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Path to a Databento trades CSV. "
             "Default: sample_data/glbx-mdp3-20260410.trades.csv. "
             "Ignored if --live is set.",
    )
    p.add_argument(
        "--live",
        action="store_true",
        help="Connect to Databento Live instead of replaying a CSV. "
             "Requires DATABENTO_API_KEY environment variable and an "
             "active live data subscription.",
    )
    p.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Replay speed multiplier (1.0 = real-time). "
             "Ignored if --live is set. Default: 1.0",
    )
    p.add_argument(
        "--port",
        type=int,
        default=8050,
        help="HTTP port to serve on. Default: 8050",
    )
    return p.parse_args()


def main():
    global processor, display_tz, display_name, symbol, price_decimals

    logging.basicConfig(
        # TAPE_LOG_LEVEL=DEBUG surfaces the scrollback/anchor state
        # transitions when diagnosing pan behavior.
        level=os.environ.get("TAPE_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    args = _parse_args()

    try:
        cfg = instrument_config.get(args.instrument)
    except KeyError as e:
        raise SystemExit(str(e))

    # Resolve CSV path only if we'll need it (replay mode).
    csv_path: Optional[Path] = None
    if not args.live:
        if args.csv is not None:
            csv_path = args.csv
        else:
            csv_path = (Path(__file__).parent.parent / "sample_data"
                        / "glbx-mdp3-20260410.trades.csv")
        if not csv_path.exists():
            raise SystemExit(f"CSV not found: {csv_path}")

    # Initialize module globals that the callback reads.
    # session_open drives the CVD baseline snap (Phase 8): launched
    # pre-market, CVD re-zeros at the open; launched mid-session, it
    # counts from launch. Replay is unaffected in practice because the
    # adapter already gates ticks to the session window.
    processor = IntensityProcessor(
        window_seconds=cfg["smoothing_seconds"],
        session_open=cfg["session_start"],
        session_timezone=cfg["session_timezone"],
    )
    display_tz = ZoneInfo(cfg["session_timezone"])
    display_name = cfg["display_name"]
    symbol = cfg["live_symbol"] if args.live else cfg["symbol"]
    price_decimals = cfg["price_decimals"]

    app.title = f"Tape Intensity {symbol}"
    app.layout = _build_layout()

    if args.live:
        logger.info(
            "Starting LIVE: instrument=%s live_symbol=%s smoothing=%ss",
            args.instrument, cfg["live_symbol"], cfg["smoothing_seconds"],
        )
    else:
        logger.info(
            "Starting REPLAY: instrument=%s symbol=%s smoothing=%ss "
            "session=%s-%s %s speed=%sx csv=%s",
            args.instrument, cfg["symbol"], cfg["smoothing_seconds"],
            cfg["session_start"], cfg["session_end"], cfg["session_timezone"],
            args.speed, csv_path,
        )

    # Spawn the data pipeline thread.
    t = threading.Thread(
        target=run_data_pipeline,
        kwargs=dict(
            mode="live" if args.live else "replay",
            csv_path=csv_path,
            symbol_filter=cfg["symbol"] if not args.live else None,
            speed=args.speed,
            session_start=cfg["session_start"],
            session_end=cfg["session_end"],
            session_timezone=cfg["session_timezone"],
            live_symbol=cfg["live_symbol"] if args.live else None,
        ),
        daemon=True,
    )
    t.start()

    logger.info("Starting Dash server on http://0.0.0.0:%d", args.port)
    app.run(host="0.0.0.0", port=args.port, debug=False)


if __name__ == "__main__":
    main()

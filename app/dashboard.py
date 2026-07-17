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
import logging
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

COLOR_BUY = "#00d68f"
COLOR_SELL = "#ff5b5b"
COLOR_PRICE = "#ffffff"
COLOR_NET = "#ffcc00"
COLOR_BG = "#111111"
COLOR_FG = "#cccccc"

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

    try:
        # Plotly reports date-axis ranges as naive strings in the axis's
        # display timezone.
        right_edge = datetime.fromisoformat(str(right_raw)).replace(
            tzinfo=display_tz
        )
    except ValueError:
        return False

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
                style={"marginBottom": "4px"},
                children=[
                    dcc.Checklist(
                        id="line-toggles",
                        options=[
                            {"label": " buy", "value": "buy"},
                            {"label": " sell", "value": "sell"},
                            {"label": " net delta", "value": "net"},
                            {"label": " price", "value": "price"},
                        ],
                        value=["buy", "sell", "net", "price"],
                        style={"color": COLOR_FG, "fontSize": "13px",
                               "display": "flex", "gap": "16px"},
                        inputStyle={"marginRight": "6px"},
                    ),
                ],
            ),
            html.Div(
                # Relative wrapper so the LIVE badge can corner-mount
                # over the chart with absolute positioning.
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
    Input("tick", "n_intervals"),
    Input("line-toggles", "value"),
    Input("intensity-chart", "relayoutData"),
    Input("live-button", "n_clicks"),
)
def update_chart(
    _n: int,
    toggles: list[str],
    relayout_data: Optional[dict],
    _live_clicks: int,
):
    global _last_seen_tick_ts, _last_seen_wall

    toggles = toggles or []
    show_buy = "buy" in toggles
    show_sell = "sell" in toggles
    show_net = "net" in toggles
    show_price = "price" in toggles

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

    # Scrollback state transitions (Phase 7b). One callback handles all
    # four inputs because Dash allows only one writer per output, and
    # tick, toggles, pan, and the LIVE button all need to redraw the
    # same figure.
    trigger = ctx.triggered_id
    rebuild = True
    if trigger == "live-button":
        _set_live()
    elif trigger == "intensity-chart":
        # User pan/zoom (or an echo of our own axis update). Only
        # rebuild when the view state really changed, so we neither
        # fight the drag nor loop on echoes.
        rebuild = _handle_relayout(relayout_data)
    elif trigger == "tick" and _view_mode == "panned":
        # Interval tick while panned: the slice is frozen. Skip the
        # figure (no_update) but let the status bar refresh below —
        # this is why the interval stays enabled in panned mode.
        # (Toggle changes still rebuild, in either mode.)
        rebuild = False

    btn_style = _LIVE_BTN_VISIBLE if _view_mode == "panned" else _LIVE_BTN_HIDDEN

    if not rebuild:
        return no_update, _build_status(state, staleness_seconds), btn_style

    # Select the visible slice. Live: trailing window off the newest
    # snapshot (Phase 7a behavior). Panned: the frozen absolute window.
    # Past the buffer edge we show only what exists — no query
    # extension, the x-axis just spans less time.
    if _view_mode == "panned" and _panned_end_ts is not None:
        recent = processor.get_history_window(
            _panned_end_ts - timedelta(seconds=VISIBLE_WINDOW_SECONDS),
            _panned_end_ts,
        )
    else:
        recent = processor.get_recent_history(VISIBLE_WINDOW_SECONDS)
    if not recent:
        return _empty_figure(), "no data", _LIVE_BTN_HIDDEN

    xs: list[datetime] = []
    buys: list[float] = []
    sells: list[float] = []
    nets: list[float] = []
    prices: list[float] = []

    for s in recent:
        xs.append(s.timestamp.astimezone(display_tz))
        buys.append(s.buy_rate)
        sells.append(s.sell_rate)
        nets.append(s.net_rate)
        prices.append(s.last_price)

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Intensity traces go on the RIGHT axis (secondary_y=True).
    # This is where the eye tracks aggression imbalance — the focus of
    # the indicator. Price (when shown) is context on the left.
    if show_buy:
        fig.add_trace(
            go.Scatter(x=xs, y=buys, name="Buy",
                       line=dict(color=COLOR_BUY, width=2),
                       hovertemplate="%{y:.1f} c/s<extra>buy</extra>"),
            secondary_y=True,
        )
    if show_sell:
        fig.add_trace(
            go.Scatter(x=xs, y=sells, name="Sell",
                       line=dict(color=COLOR_SELL, width=2),
                       hovertemplate="%{y:.1f} c/s<extra>sell</extra>"),
            secondary_y=True,
        )
    if show_net:
        fig.add_trace(
            go.Scatter(x=xs, y=nets, name="Net",
                       line=dict(color=COLOR_NET, width=1.5, dash="dot"),
                       hovertemplate="%{y:+.1f} c/s<extra>net</extra>"),
            secondary_y=True,
        )
    if show_price:
        fig.add_trace(
            go.Scatter(x=xs, y=prices, name="Price",
                       line=dict(color=COLOR_PRICE, width=1.5),
                       hovertemplate=f"%{{y:.{price_decimals}f}}<extra>price</extra>"),
            secondary_y=False,
        )

    # Right axis (intensity, the focus axis). Gridlines, zero anchoring,
    # and tozero rangemode all live here. If net is visible, allow
    # negative space; otherwise pin baseline at zero for a cleaner look.
    if show_net:
        fig.update_yaxes(secondary_y=True,
                         color=COLOR_FG, title_text="contracts/sec",
                         gridcolor="#222222", showgrid=True,
                         zeroline=True, zerolinecolor="#444444",
                         zerolinewidth=1)
    else:
        fig.update_yaxes(rangemode="tozero", secondary_y=True,
                         color=COLOR_FG, title_text="contracts/sec",
                         gridcolor="#222222", showgrid=True)

    # Left axis (price, context only). Visible only when price toggle
    # is on. No gridlines — price gridlines would clutter the focus on
    # intensity.
    price_range_key = "off"
    if show_price and prices:
        p_min, p_max = min(prices), max(prices)
        p_pad = max((p_max - p_min) * 0.1, 0.25)
        fig.update_yaxes(range=[p_min - p_pad, p_max + p_pad],
                         secondary_y=False, showgrid=False,
                         color=COLOR_PRICE, title_text="",
                         visible=True)
        # Dynamic uirevision so the price axis auto-scale takes effect
        # on each redraw, not just the first one. See uirevision in
        # update_layout below.
        price_range_key = f"{round(p_min, 1)}_{round(p_max, 1)}"
    else:
        # Hide the left axis entirely when price is off. This gives
        # the intensity lines the full chart width on the right axis.
        fig.update_yaxes(secondary_y=False, visible=False)

    # uirevision controls when Plotly preserves vs. resets view state
    # across redraws (Phase 6 gotcha #4).
    # Live: dynamic key tied to the price range, so the right-axis
    #   auto-scale keeps taking effect as price drifts — unchanged
    #   behavior. Without this the price line gets stuck at whatever
    #   range was set on the first draw.
    # Panned (Phase 7b): key tied to the frozen window, so the user's
    #   pan position survives refreshes of the same slice, while each
    #   NEW pan changes the key and lets our axis range through. This
    #   is delicate — a constant key here would let Plotly ignore the
    #   clamped range on buffer-edge hits, re-showing empty space.
    if _view_mode == "panned" and _panned_end_ts is not None:
        uirev = f"panned_{int(_panned_end_ts.timestamp())}"
        # Explicit x-range = the slice's actual data extent. Implements
        # the hard stop (a drag past the edge snaps back to the clamped
        # window) and shows a shorter axis when less data exists.
        xaxis_cfg = dict(color=COLOR_FG, gridcolor="#222222",
                         range=[xs[0], xs[-1]])
    else:
        # _live_epoch guarantees this key is never a key the user
        # dragged under before a scrollback excursion (see its comment).
        uirev = f"price_{_live_epoch}_{price_range_key}"
        xaxis_cfg = dict(color=COLOR_FG, gridcolor="#222222")

    fig.update_layout(
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_BG,
        font=dict(color=COLOR_FG, family="monospace"),
        margin=dict(l=50, r=50, t=20, b=40),
        xaxis=xaxis_cfg,
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

    return fig, _build_status(state, staleness_seconds), btn_style


def _build_status(state, staleness_seconds: float) -> html.Div:
    """Build the status bar. Always live values — never offset-aware."""
    health_color, health_label = _health_from_age(staleness_seconds)
    last_tick_str = state.timestamp.astimezone(display_tz).strftime("%H:%M:%S")
    price_str = f"{state.last_price:.{price_decimals}f}"
    return html.Div(
        style={"display": "flex", "gap": "20px", "alignItems": "center"},
        children=[
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
            html.Span(f"buf {state.tick_count_in_window}",
                      style={"color": "#888888"}),
        ],
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
        level=logging.INFO,
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
    processor = IntensityProcessor(window_seconds=cfg["smoothing_seconds"])
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

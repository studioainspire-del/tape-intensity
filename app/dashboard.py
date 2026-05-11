"""
Phase 4: Dash dashboard for the tape intensity indicator.

Architecture:
    - Main thread: Dash/Flask server. Handles HTTP requests and runs
      the layout/callback machinery.
    - Background thread: a dedicated asyncio event loop that owns the
      adapter and runs the processor's consume() task.
    - Shared state: a single IntensityProcessor instance, written by
      the consume task (in the background thread) and read by the Dash
      callback (in the main thread). Reads and writes are individually
      atomic at the bytecode level (Python GIL + deque semantics), so
      no explicit lock is needed for this design.

What the user sees:
    A three-line scrolling chart of the most recent 3 minutes:
        - Green: buy intensity (contracts/sec)
        - Red: sell intensity (contracts/sec)
        - White: price (secondary right-side Y-axis)
    Plus a small text panel showing current state and the data-feed
    health indicator.

Update cadence:
    Browser polls every 200ms via dcc.Interval (5 Hz). Each poll
    triggers update_chart(), which appends a new ProcessorState sample
    to a bounded history buffer and rebuilds the figure from it.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time as _time
from collections import deque
from datetime import datetime, time as dt_time, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, callback
from plotly.subplots import make_subplots

from adapters.csv_adapter import CSVAdapter
from adapters.replay_adapter import ReplayAdapter
from processor.intensity import IntensityProcessor, ProcessorState


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Display configuration
# ---------------------------------------------------------------------------

# Visible time window for the rolling chart (seconds).
VISIBLE_WINDOW_SECONDS = 180  # 3 minutes

# Dashboard polling rate (milliseconds). 200ms = 5 Hz.
POLL_INTERVAL_MS = 200

# Cap on the history buffer of ProcessorState snapshots. At 5 Hz over
# 3 minutes that's 900; we keep a little extra headroom.
HISTORY_MAX = 1200

# Display timezone for the X-axis ticks.
DISPLAY_TZ = ZoneInfo("America/Chicago")

# Color discipline (your spec).
COLOR_BUY = "#00d68f"     # green
COLOR_SELL = "#ff5b5b"    # red
COLOR_PRICE = "#ffffff"   # white
COLOR_NET = "#ffcc00"     # yellow (net delta = buy - sell)
COLOR_BG = "#111111"      # near-black chart background
COLOR_FG = "#cccccc"      # light gray text/axis


# ---------------------------------------------------------------------------
# Shared state: the processor + a rolling snapshot history
# ---------------------------------------------------------------------------

# Module-level singletons. Threading note: these objects are created
# once at process start, before the background thread is spawned and
# before the Dash server begins serving requests. So there's no race
# between construction and access.
processor = IntensityProcessor(window_seconds=10.0)
history: deque[ProcessorState] = deque(maxlen=HISTORY_MAX)

# Staleness tracking for the health light. Anchored to wall-clock,
# not tick-time, because the question is "is the data pipeline alive?"
# We remember the last tick timestamp we observed, and the wall-clock
# time at which we first observed it. As long as new tick timestamps
# keep arriving, the staleness counter stays near zero.
_last_seen_tick_ts: Optional[datetime] = None
_last_seen_wall: float = 0.0


# ---------------------------------------------------------------------------
# Background thread: asyncio event loop running the consume task
# ---------------------------------------------------------------------------

def run_data_pipeline(csv_path: Path) -> None:
    """Run the adapter -> processor pipeline forever in this thread.

    This function blocks. Run it as the target of a daemon thread.
    """
    async def _pipeline():
        inner = CSVAdapter(csv_path, symbol_filter="MNQM6")
        adapter = ReplayAdapter(
            inner,
            speed=1.0,  # real-time for the dashboard view
            session_start=dt_time(8, 30),
            session_end=dt_time(15, 0),
            session_timezone="America/Chicago",
        )
        await adapter.connect()
        try:
            await processor.consume(adapter)
        finally:
            await adapter.disconnect()

    asyncio.run(_pipeline())


# ---------------------------------------------------------------------------
# Dash app
# ---------------------------------------------------------------------------

app = Dash(__name__)
app.title = "Tape Intensity"

app.layout = html.Div(
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
                html.H2("Tape Intensity (MNQM6)", style={"margin": 0}),
                html.Div(id="status-bar", style={"fontSize": "14px"}),
            ],
        ),
        html.Div(
            style={"marginBottom": "4px"},
            children=[
                dcc.Checklist(
                    id="line-toggles",
                    options=[
                        {"label": " net delta (buy - sell)", "value": "net"},
                    ],
                    value=["net"],
                    style={"color": COLOR_FG, "fontSize": "13px"},
                    inputStyle={"marginRight": "6px"},
                ),
            ],
        ),
        dcc.Graph(
            id="intensity-chart",
            style={"height": "70vh"},
            config={"displayModeBar": False},
        ),
        dcc.Interval(id="tick", interval=POLL_INTERVAL_MS, n_intervals=0),
    ],
)


def _health_from_age(age_seconds: float) -> tuple[str, str]:
    """Return (color_hex, label) for the connection health indicator."""
    if age_seconds < 1.0:
        return ("#00d68f", "GREEN")
    if age_seconds < 5.0:
        return ("#ffcc00", "YELLOW")
    return ("#ff5b5b", "RED")


@callback(
    Output("intensity-chart", "figure"),
    Output("status-bar", "children"),
    Input("tick", "n_intervals"),
    Input("line-toggles", "value"),
)
def update_chart(_n: int, toggles: list[str]):
    """Called every POLL_INTERVAL_MS. Pulls a state snapshot, redraws."""
    global _last_seen_tick_ts, _last_seen_wall

    state = processor.get_state()
    history.append(state)

    # Staleness tracking. If the consume task pushed a new tick since
    # the last poll, the latest state's timestamp will have advanced.
    # If it hasn't, this state is stale and the wall-clock counter
    # measures exactly how long it's been stale.
    now_wall = _time.monotonic()
    if _last_seen_tick_ts is None or state.timestamp != _last_seen_tick_ts:
        _last_seen_tick_ts = state.timestamp
        _last_seen_wall = now_wall
    staleness_seconds = now_wall - _last_seen_wall

    # Trim history to the visible window. We keep this cheap: just
    # build lists from the deque and filter by timestamp.
    if not history:
        return _empty_figure(), "no data"

    # X-axis is tick-time, not wall-clock time. During replay this
    # makes the chart move forward at the replay speed; during live
    # they coincide.
    latest_tick_ts = history[-1].timestamp
    window_start = latest_tick_ts.timestamp() - VISIBLE_WINDOW_SECONDS

    xs: list[datetime] = []
    buys: list[float] = []
    sells: list[float] = []
    nets: list[float] = []
    prices: list[float] = []

    for s in history:
        if s.timestamp.timestamp() < window_start:
            continue
        xs.append(s.timestamp.astimezone(DISPLAY_TZ))
        buys.append(s.buy_rate)
        sells.append(s.sell_rate)
        nets.append(s.net_rate)
        prices.append(s.last_price)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=xs, y=buys, name="Buy intensity",
                   line=dict(color=COLOR_BUY, width=2),
                   hovertemplate="%{y:.1f} contracts/s<extra>buy</extra>"),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=xs, y=sells, name="Sell intensity",
                   line=dict(color=COLOR_SELL, width=2),
                   hovertemplate="%{y:.1f} contracts/s<extra>sell</extra>"),
        secondary_y=False,
    )
    if "net" in (toggles or []):
        fig.add_trace(
            go.Scatter(x=xs, y=nets, name="Net delta",
                       line=dict(color=COLOR_NET, width=1.5, dash="dot"),
                       hovertemplate="%{y:+.1f} contracts/s<extra>net</extra>"),
            secondary_y=False,
        )
    fig.add_trace(
        go.Scatter(x=xs, y=prices, name="Price",
                   line=dict(color=COLOR_PRICE, width=1.5),
                   hovertemplate="%{y:.2f}<extra>price</extra>"),
        secondary_y=True,
    )

    # Tight auto-scale on price (right axis): use the visible-window
    # min/max with a small pad. Plotly's default auto-scale tends to
    # over-pad, so we set the range explicitly.
    if prices:
        p_min, p_max = min(prices), max(prices)
        p_pad = max((p_max - p_min) * 0.1, 0.25)  # at least one NQ tick
        fig.update_yaxes(range=[p_min - p_pad, p_max + p_pad],
                         secondary_y=True, showgrid=False,
                         color=COLOR_PRICE, title_text="")
    # Left axis: must accommodate the net delta if it's visible, since
    # net can go negative. Default rangemode=tozero hides negative space;
    # we override based on the toggle.
    if "net" in (toggles or []):
        fig.update_yaxes(secondary_y=False,
                         color=COLOR_FG, title_text="contracts/sec",
                         gridcolor="#222222",
                         zeroline=True, zerolinecolor="#444444",
                         zerolinewidth=1)
    else:
        fig.update_yaxes(rangemode="tozero", secondary_y=False,
                         color=COLOR_FG, title_text="contracts/sec",
                         gridcolor="#222222")

    fig.update_layout(
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_BG,
        font=dict(color=COLOR_FG, family="monospace"),
        margin=dict(l=50, r=50, t=20, b=40),
        xaxis=dict(color=COLOR_FG, gridcolor="#222222"),
        showlegend=True,
        legend=dict(orientation="h", yanchor="top", y=1.08,
                    xanchor="right", x=1.0,
                    bgcolor="rgba(0,0,0,0)"),
        hovermode="x unified",
        uirevision="static",  # don't reset zoom/pan on refresh
    )

    # Status bar: health light, last-tick time, current rates.
    health_color, health_label = _health_from_age(staleness_seconds)
    last_tick_str = state.timestamp.astimezone(DISPLAY_TZ).strftime("%H:%M:%S")
    status = html.Div(
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
            html.Span(f"price {state.last_price:.2f}",
                      style={"color": COLOR_PRICE}),
            html.Span(f"buf {state.tick_count_in_window}",
                      style={"color": "#888888"}),
        ],
    )
    return fig, status


def _empty_figure() -> go.Figure:
    """Placeholder figure shown before any data arrives."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_layout(
        paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG,
        font=dict(color=COLOR_FG),
        margin=dict(l=50, r=50, t=20, b=40),
        xaxis=dict(color=COLOR_FG, gridcolor="#222222"),
        yaxis=dict(color=COLOR_FG, gridcolor="#222222"),
        annotations=[dict(text="waiting for data...",
                          xref="paper", yref="paper",
                          x=0.5, y=0.5, showarrow=False,
                          font=dict(color=COLOR_FG, size=18))],
    )
    return fig


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    csv_path = Path(__file__).parent.parent / "sample_data" / "glbx-mdp3-20260410.trades.csv"
    if not csv_path.exists():
        # Fall back to the variant with underscore (e.g. testing env)
        alt = csv_path.with_name("glbx-mdp3-20260410_trades.csv")
        if alt.exists():
            csv_path = alt
        else:
            raise FileNotFoundError(
                f"Neither {csv_path} nor {alt} exists. "
                "Put a Databento trades CSV in sample_data/."
            )

    logger.info("Starting data pipeline thread (csv=%s)", csv_path)
    t = threading.Thread(target=run_data_pipeline, args=(csv_path,), daemon=True)
    t.start()

    logger.info("Starting Dash server on http://0.0.0.0:8050")
    # host=0.0.0.0 so Tailscale peers can reach it.
    app.run(host="0.0.0.0", port=8050, debug=False)


if __name__ == "__main__":
    main()

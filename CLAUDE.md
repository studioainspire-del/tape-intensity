# CLAUDE.md

Project context for Claude Code sessions on this repository.

## What this is

A custom real-time orderflow indicator for day-trading CME futures (MNQ, MGC, with config for NQ/GC/ES/MES). Displays aggressive buy intensity, aggressive sell intensity, net delta, and price on a 3-minute scrolling chart, plus CVD and RVOL panels stacked below it (Phase 8). Used as a confirmation read at user-defined areas of interest, alongside Jigsaw Trading. **Not** a primary signal generator.

The chart works because aggressor side is asymmetric even though volume is symmetric. Volume always balances; aggression rarely does. Buy intensity = contracts/sec hitting the ask, sell intensity = contracts/sec hitting the bid, both smoothed over a rolling window.

## Stack

- Python 3.12, venv at `.venv/` (Windows: `.venv\Scripts\activate`)
- Dash 4.1+, Plotly 6.7+, databento 0.78+ (pinned as compatible releases in `requirements.txt`; pandas was dropped — nothing imports it)
- Async throughout (`asyncio`). Push-model adapters via async generators.
- Plotly Dash dashboard runs on `0.0.0.0:8050` (bound this way for Tailscale LAN access)

## Project structure

```
tape-intensity/
  adapters/            base.py, csv_adapter.py, replay_adapter.py, live_databento.py
  processor/           intensity.py
  app/                 dashboard.py, assets/crosshair.js
  config/              instruments.py
  sample_data/         CSV files for replay (gitignored except sample_nq.csv)
  main.py              unused initialization stub
  requirements.txt
```

## Three-layer architecture (strict separation — preserve when editing)

1. **Adapter layer** (`adapters/`): async generators yielding `TickEvent` objects. Anything source-specific (Databento side codes, vendor field names, timestamp formats) is translated to `TickEvent` *inside the adapter*. The processor must never see source-specific data.
2. **Processor layer** (`processor/intensity.py`): source-agnostic. Maintains rolling buffer, computes smoothed rates, and keeps cumulative CVD/volume counters (Phase 8). Push model: `consume(adapter)` runs in a background task; `get_state()` returns a snapshot for the dashboard to read.
3. **Visualization layer** (`app/dashboard.py`): Dash callback polls `processor.get_state()` at 5 Hz, builds Plotly figure. Doesn't know what adapter is feeding the processor.

### Adapter swappability

The whole point of the architecture: switching data source = one-line change. `dashboard.py`'s `--live` flag picks `DatabentoLiveAdapter` vs `CSVAdapter` + `ReplayAdapter`. Anything that breaks this swappability is a regression.

## Running

All commands assume venv is activated and you're at the project root.

```
python -m app.dashboard MNQ --live          # live MNQ via Databento
python -m app.dashboard MGC --live          # live MGC via Databento
python -m app.dashboard MNQ --csv path.csv  # replay from CSV
python -m app.dashboard --help              # see all options
```

`python -m app.dashboard` (not `python app/dashboard.py`) — the `-m` flag is required for relative imports to resolve.

## Configuration

`config/instruments.py` holds per-instrument defaults. Each entry has:
- `display_name`, `symbol` (raw contract code, e.g. `MNQM6`), `live_symbol` (continuous, e.g. `MNQ.c.0`)
- `tick_size`, `price_decimals`
- `smoothing_seconds` (rolling window for rate calculation — 10s for index futures, 17s for metals)
- `session_start`, `session_end`, `session_timezone` (RTH filter for replay mode; ignored in live mode)

Raw `symbol` field needs manual edit at contract roll. Live mode auto-rolls via continuous symbology.

## Secrets

`DATABENTO_API_KEY` is a Windows user-scope environment variable. **Never** log it, print it, or include it in error messages. The SDK picks it up automatically when present; the live adapter resolves it explicitly in `__init__` and fails loudly if missing.

## Diagnostics: where to look when things break

### Symptom: dashboard starts but chart shows "waiting for data..." forever

Most likely an adapter problem. Check the terminal log for the data pipeline thread.

For **live mode**, expected log sequence within ~1 second of startup:
```
INFO databento.live.client: subscribing to schema=trades ...
INFO databento.live.session: connecting to remote gateway
INFO databento.live.session: authenticated session_id='...'
INFO databento.live.client: starting live client
INFO ...: system message code=subscription_ack ...
INFO ...: added symbology mapping <RAW_CONTRACT> to <instrument_id>
```

- Missing `authenticated session_id`: connection didn't complete. Possibly API key issue, subscription tier issue, or rate-limited reconnect. Wait 60s before assuming it's broken.
- Missing `subscription_ack`: subscription request rejected. Check symbol spelling and `stype_in`.
- Missing `symbology mapping`: subscription accepted but symbol couldn't be resolved. Likely wrong continuous symbol format. Format is `<ROOT>.c.0` for front-month, e.g. `MNQ.c.0`, `GC.c.0`.
- Auth succeeded but no trades flow: could be quiet market, OR could be a record-translation bug in `live_databento.py` (every trade gets dropped silently). To verify, run a diagnostic script that prints raw SDK records directly (see "running the diagnostic" below).

For **replay mode**, check the CSVAdapter counters (printed at the end of stream). `dropped_no_aggressor` should be a tiny fraction. `dropped_other_symbol` is high if the CSV contains multiple instruments and the symbol filter is doing its job. `parse_errors` should be zero on Databento-format CSVs.

### Symptom: chart shows weird flat lines for a few minutes after startup

This is the warmup-zero-state bug pattern. Should be fixed already (see `app/dashboard.py` — we skip appending to history when `state.tick_count_in_window == 0`). If it returns, the fix has regressed.

### Symptom: price line stuck at unrealistic value (e.g. flat at 30000 when actual price is 29620)

Y-axis auto-scale isn't updating across refreshes. In live mode the dashboard uses a `uirevision` that advances with the live edge (a wall-clock bucket) precisely so autorange keeps re-applying. If you change the uirevision behavior, test that auto-scale still tracks price drift over a few minutes. See the pan/zoom symptom below for the full uirevision mechanics.

### Symptom: toggling the price line off freezes the chart (x-scroll and/or intensity auto-scale stop)

Fixed (Phase 8.1). Root cause was the gotcha #4 lockout: the live `uirevision` used to be keyed on the price range, which is only dynamic while the price line is shown. With price off the key became a constant `"off"`, so once the user zoomed or panned, Plotly retained that interaction forever — the x-axis stopped scrolling and the intensity y-axis stopped auto-scaling. The live key is now the wall-clock–bucketed live edge (`LIVE_UIREV_BUCKET_SECONDS`), independent of which traces are shown. If this returns, check that the live-mode `uirev` still advances when price is off.

### Symptom: health light always red

The health light tracks **wall-clock** time since the most recent unique tick timestamp arrived, not tick-time age. This is intentional — tick-time age is meaningless for replay (timestamps are months old) and the question is always "is the data pipeline alive?", which is a wall-clock question.

If always red:
- The `_last_seen_tick_ts` and `_last_seen_wall` module globals aren't being updated. They live in `app/dashboard.py` at module scope.
- During replay, ticks arrive at replay speed; staleness threshold is 1s (green) / 5s (yellow). Replays at speed < 1.0x or quiet midday CSV sections may legitimately show yellow.
- During live, quiet markets can produce 1-3 second gaps between trades. Yellow flicker during midday lulls is normal. Persistent red means the connection died.

### Symptom: browser tab title flickers "Updating..." every 200ms

Fixed by `update_title=None` in the `Dash(...)` constructor. If it returns, that arg was removed.

### Symptom: chart shows sparse straight-line segments when returning from a backgrounded tab

Fixed in Phase 7a: history is sampled server-side by a 5 Hz task in the data pipeline (`record_snapshot()` in `processor/intensity.py`), so browser tab throttling no longer affects it. If this symptom returns, check that the sampler task is actually running alongside `consume()` in `run_data_pipeline` — the callback only *reads* history now (`get_recent_history`), it must never populate it.

### Symptom: pan/zoom gets "stuck" and chart freezes

Since Phase 7b, panning back is a *feature*: any leftward drag enters scrollback mode, which deliberately freezes the chart on a fixed 3-minute slice (the LIVE badge appears top-right). Click LIVE, pan the right edge back to within ~2s of the newest snapshot, or double-click, to resume live updates. A page refresh always resets to live view. If the chart is frozen *without* the LIVE badge showing, that's a real bug — check `_view_mode` consistency in `app/dashboard.py`.

uirevision mechanics (three interacting pieces — change nothing here without re-testing all of it):
- **Live mode** keys uirevision on the live edge bucketed to `LIVE_UIREV_BUCKET_SECONDS` (~1s, wall-clock). It must *advance* so Plotly re-applies autorange (x-scroll + both y-axes auto-scale, gotcha #4) — a constant key freezes the view after any interaction (that was the price-off freeze bug). It must *not* advance every tick, or react re-autoranges mid-gesture and clobbers a pan-back before it registers as scrollback. The ~1s bucket is the balance: steady across a human gesture, advancing fast enough to feel live. It's **wall-clock** bucketed, not tick-time, so replay speed doesn't stretch/shrink the stability window (same wall-clock framing as the health light). A stray zoom that stays within the live edge self-heals at the next bucket flip.
- **Panned mode** keys uirevision to the frozen window's end timestamp, so the pan position survives refreshes while each new pan lets the clamped axis range through.
- **`_live_epoch`** prefixes the live key and bumps on every panned→live transition. Plotly stores drag state keyed to the uirevision value active when the user dragged; without the bump, returning to live with an unchanged key makes Plotly *restore the stale panned position* instead of snapping to the live edge (whose relayout echo then flips the server back to panned — symptom: LIVE click appears to do nothing, or the badge blinks off and back on).

If pan feels like it's "fighting" or snapping unpredictably, look here first. Note: scrollback only engages once the buffer holds a full `VISIBLE_WINDOW_SECONDS` (there's nothing to pan into before that) — a fresh start "ignoring" pans for the first ~3 minutes is that guard, not a bug.

## Running the live diagnostic

When the live adapter behaves strangely and you need to see raw SDK records, this snippet bypasses the adapter and prints the first 20 records straight from the SDK. Useful for verifying field types and message order without our translation layer in the way.

```python
# test_live_diag.py at project root
import asyncio, os
import databento as db

async def main():
    client = db.Live(key=os.environ["DATABENTO_API_KEY"])
    client.subscribe(
        dataset="GLBX.MDP3",
        schema="trades",
        stype_in="continuous",
        symbols=["MNQ.c.0"],
    )
    # Do NOT call client.start() — iteration starts streaming.

    count = 0
    async for record in client:
        count += 1
        print(f"--- {count} {type(record).__name__} ---")
        print(repr(record))
        if hasattr(record, 'side'):
            print(f"  side={record.side!r} type={type(record.side).__name__}")
        if count >= 20:
            break
    client.stop()

asyncio.run(main())
```

**Important**: `client.start()` must NOT be called before iteration. The SDK raises `ValueError` if you do. The live adapter has this same constraint baked in.

## Critical gotchas (these have already burned us)

1. **`Live.start()` rejection.** The Databento SDK refuses iteration if `.start()` was called explicitly. Our adapter relies on iteration starting the stream implicitly. Don't add `client.start()` to `connect()`.

2. **`record.side` is a `Side` enum, not a string.** The CSV has `'A'`/`'B'`/`'N'` as plain chars. The live SDK returns `Side.ASK`/`Side.BID`/`Side.NONE`. Extract via `.value` to get the char. The live adapter handles this; the CSV adapter handles strings directly.

3. **Warmup zero-states.** `processor.get_state()` returns a state with `last_price=0.0` before any tick has been ingested. If those zero-states reach the chart, they stretch the price Y-axis from 0 to current price, making the line look flat against the top. The dashboard skips appending these to `history` — preserve that.

4. **uirevision lockout.** Static `uirevision` strings in Plotly cause the chart to ignore subsequent axis-range updates. Use a dynamic key tied to the data range when you want auto-scale to keep working.

5. **The 87MB Databento CSV in `sample_data/`** is gitignored. Don't commit it. `glbx-mdp3-*.csv` is the pattern. `sample_nq.csv` (1.4KB synthetic fixture) is committed and useful for tests.

6. **Browser cache strikes after JS changes.** When editing layout/Dash config and the browser doesn't reflect the change after restart, hard refresh with Ctrl+Shift+R. Python code reloads on restart; bundled JavaScript caches aggressively.

7. **Dash trigger attribution is lossy under the 5 Hz interval — detect user events by VALUE, not by `ctx.triggered`.** A button press or pan that lands while a callback request is in flight gets coalesced into the next tick request: the input's new value arrives, but `changedPropIds` says "tick" (measured ~1-in-6 press loss before the fix). Hence `_btn_seen` (monotonic n_clicks tracking; only a zero resets, because a stale concurrent request carrying an old count must not regress it) and `_relayout_seen`/`_click_seen` (single-deep value diff — a rare stale re-apply self-corrects on the next tick, whereas deeper matching would permanently swallow legitimate repeats). Related: the whole callback body is serialized by `_callback_lock` — Flask's threaded server otherwise interleaves request bodies mid-bookkeeping, which produced 50% scrollback-state corruption in browser tests. Do not remove the lock or revert to trigger-attribution routing.

8. **The figure-rebuild guard doubles as a hover fix.** Tick refreshes skip rebuilding when nothing changed (`_last_live_build_ts` + `_last_build_toggles`); besides saving CPU, this stops needless `Plotly.react` churn from clearing the browser's hover state, which made chart clicks unreliable. The "drop Δ anchor" button exists because chart-click anchor drops depend on that hover pipeline; the button path must stay.

## History buffer behavior (Phase 7a)

The processor owns a server-side history of sampled states (`_history` in `processor/intensity.py`), populated at 5 Hz by a sampler task in the dashboard's pipeline thread. Things to know:

- History starts **empty at processor startup** and fills at 5 Hz from that moment forward. There is no backfill — asking for a time before the processor started returns no data. Not a bug.
- The buffer holds **30 minutes max** (9,000 snapshots at 5 Hz); oldest snapshots roll off first. Sized deliberately for Phase 7b's scrollback range.
- **Warmup zero-states are not recorded.** Before the first tick arrives, `get_state()` returns `last_price=0.0` states; `record_snapshot()` skips them (same rule the browser-side history used, gotcha #3).
- The sampler runs on the same asyncio loop as `consume()` — no extra thread. History reads from the Dash callback cross the thread boundary, which is why `_history` is lock-guarded while the tick buffer is not.

## Scrollback UI (Phase 7b)

Drag the chart left to pan back through the server-side history buffer; the visible slice is always 3 minutes wide — panning picks *which* 3 minutes. Behavior contract:

- **Entering scrollback**: any pan that leaves the right edge more than ~2 seconds behind the newest snapshot. No threshold beyond that — you're either at the live edge or you're not.
- **Returning to live**: click the LIVE badge (top-right of the chart, visible only while panned), pan the right edge back to within ~2 seconds of the newest snapshot, or double-click (autosize).
- **Buffer edge**: hard stop at the earliest snapshot. A drag past it snaps back to the edge window on release — no empty space, no message. Until the buffer holds a full 3 minutes there is nothing to pan into and the chart stays live.
- **Status bar and health light always show live values**, never the historical moment. Deliberate: pattern from the chart, numbers from the bar. Do not build offset-aware status logic.
- **Auto-refresh while panned**: the figure is frozen via `dash.no_update`, but the `dcc.Interval` keeps firing so the status bar stays live — do not disable the interval, that freezes the health light. The 5 Hz sampler keeps filling the buffer regardless of view mode.
- **View state** (`_view_mode`, `_view_offset_seconds`, `_panned_end_ts`) is module globals in `app/dashboard.py`, not a `dcc.Store` — single-user dashboard, same pattern as the staleness trackers. `_panned_end_ts` anchors the frozen slice absolutely; the offset alone would drift as the live edge advances.

## Volume panels (Phase 8): CVD + RVOL

Two panels stacked under the aggressor chart. They are rows of the *same* Plotly figure with a shared x-axis, so unified hover, scrollback, and uirevision apply to all panels at once — never split them into separate `dcc.Graph`s, each would need its own synced pan-state machine. Toggling a panel off collapses its row; the main panel regains the height.

### CVD (cumulative volume delta)

- Running sum of signed volume (+size for aggressive buys, −size for sells), accumulated in the processor and sampled into history like everything else — scrollback shows historical CVD for free.
- **Baseline snaps at the session open** (`session_start` from the instrument config — 8:30 America/Chicago for MNQ/NQ/ES/MES, 7:20 for MGC/GC). Launched pre-market: pre-open CVD accumulates visibly, then the reported value re-zeros at the first tick at/after the open. Launched mid-session: counts from launch. The snap fires once per local day, so a left-running live instance re-baselines at the next open.
- **"drop Δ anchor" button (toggles row) is the primary way to set the anchor** — anchors at the live edge, or at the frozen moment while panned back (pan to the zone touch, press drop). Press again any time to re-anchor. Chart clicks also drop the anchor at the clicked time, but that path depends on Plotly's hover pipeline having points under the cursor and silently misses sometimes on an updating figure — the button never misses; treat clicks as a bonus, not the contract. The status bar gains a bold `Δcvd` readout (CVD now − CVD at anchor); a dotted vertical marker shows on the CVD panel while the anchor is in view. "clear Δ anchor" (visible only while anchored) removes it.
- The anchor stores its CVD value as a **scalar at drop time** — do not "improve" this into a history lookup. The history buffer holds only 30 minutes; the scalar keeps Δcvd working all session after the anchor's timestamp rolls off.

### RVOL (relative volume)

- `RVOL = (volume rate over the trailing 60s) / (volume rate over the trailing 300s)` — "is this minute busier than the last five?" Both windows span-normalized by the history actually available, so partial windows degrade gracefully instead of spiking. Constants: `RVOL_FAST_SECONDS` / `RVOL_SLOW_SECONDS` / `RVOL_MIN_BASELINE_SECONDS` in `app/dashboard.py`.
- Reads as a multiple: 1.0x = this minute at the recent norm, 2.0x = double, 0.5x = half. Dotted reference line at 1.0x.
- **The line is coloured by slope**: green while RVOL is rising (participation building), red while falling, and the original purple while flat. The status bar's `rvol` readout takes the same colour, so the direction reads without looking at the panel.
  - Direction is measured against the value `RVOL_SLOPE_LOOKBACK_SECONDS` (5s) earlier, **not** against the previous sample, and changes smaller than `RVOL_SLOPE_DEADBAND` (0.02) stay neutral. This is the whole point: RVOL is a 60s rate over a 300s rate, so two consecutive 5 Hz samples differ only by noise and their sign flips at random while RVOL is flat — sample-to-sample colouring strobes green/red exactly when nothing is happening. Verified: perfectly steady flow colours 0% of points. Raise the lookback for a calmer read, raise the deadband to demand a bigger move.
  - Two consequences that are correct, not bugs: direction **lags a turn** slightly (RVOL stays "higher than 5s ago" for a beat past the peak), and a **step up in volume reads green then red** — the 60s numerator reacts within a minute, then the 300s denominator catches up over the next five, so RVOL genuinely spikes and decays.
  - Drawn as **three traces**, not one: Plotly cannot colour segments within a single line trace (`line.color` rejects a list). `_rvol_colored_series` emits rising/falling/flat masks that carry `None` outside their own stretches, with `connectgaps=False`; a point where two stretches meet appears in **both** masks so the line has no hairline gap at the handoff. Only the neutral trace carries `showlegend`, so RVOL still appears once in the legend. If you touch this, keep the shared-boundary rule — dropping it puts a visible break at every colour change.
- **Shows gaps (and `rvol --` in the status bar) until history spans ≥2 minutes.** Not a bug — the baseline is warming up. Expect it after every processor start.
- Computed dashboard-side from history samples of the raw cumulative volume counter; the figure fetch pulls `VISIBLE_WINDOW_SECONDS + RVOL_SLOW_SECONDS` of history so the panel's left edge still has a full lookback. The counter is never baselined — RVOL only takes differences, so offsets cancel.
- Status bar shows live `cvd`, `Δcvd` (when anchored), and `rvol` — live values only, per the Phase 7b rule.

## Crosshair overlay (`app/assets/crosshair.js`)

A vertical line spanning **all** panels (continuous through the gaps) plus a horizontal line floating at the cursor's y inside the hovered panel. Anything in `app/assets/` is auto-served by Dash — no wiring needed beyond the file existing.

It is a **DOM overlay, not Plotly spikes**, for two reasons: Plotly's x spike can only span the subplot it's drawn in (`hoversubplots: "axis"` does *not* draw spikes across rows — verified on plotly.js 3.7 including the documented minimal example), and the figure rebuilds up to 5x/sec, with every `Plotly.react` clearing hover state (gotcha #8) — a spike-based crosshair would flicker. The overlay is driven by mousemove only: zero server round-trips, immune to rebuilds.

Rules if you touch it:
- **`pointer-events: none` on both lines is load-bearing.** They sit over Plotly's drag layer; if they ever capture events, panning and chart clicks break.
- z-index stays below the LIVE badge (10) so the badge remains clickable.
- Plot bounds are read from the `.nsewdrag` rects (Plotly's per-panel drag layers) via `getBoundingClientRect`, re-read on every move — no `_fullLayout` internals, no stale geometry after a rebuild.
- `showspikes=False` on the x-axes in `_build_figure` exists so Plotly's own snapped spike doesn't double the overlay's cursor-tracked line. Removing it brings back a fuzzy double line.

## What is NOT yet built (deliberate scope, not bugs)

- Automatic live-feed reconnection on disconnect. Currently you restart the dashboard manually.
- Databento sequence-gap detection.
- Heartbeat-based health light integration (the wall-clock-staleness one works fine for most cases).
- RVOL against a multi-day historical baseline ("volume vs. typical for this time of day"). Current RVOL is intraday-relative — last minute vs. the last five.
- Front-month auto-resolution for CSV mode raw symbols (live mode handles this via continuous symbology).
- Tailscale on the home machine (installed but not verified working from phone / work MacBook).

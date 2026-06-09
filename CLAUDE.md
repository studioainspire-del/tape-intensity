# CLAUDE.md

Project context for Claude Code sessions on this repository.

## What this is

A custom real-time orderflow indicator for day-trading CME futures (MNQ, MGC, with config for NQ/GC/ES/MES). Displays aggressive buy intensity, aggressive sell intensity, net delta, and price on a 3-minute scrolling chart. Used as a confirmation read at user-defined areas of interest, alongside Jigsaw Trading. **Not** a primary signal generator.

The chart works because aggressor side is asymmetric even though volume is symmetric. Volume always balances; aggression rarely does. Buy intensity = contracts/sec hitting the ask, sell intensity = contracts/sec hitting the bid, both smoothed over a rolling window.

## Stack

- Python 3.12, venv at `.venv/` (Windows: `.venv\Scripts\activate`)
- Dash 4.1, Plotly 6.7, pandas, databento 0.78 (full list in `requirements.txt`)
- Async throughout (`asyncio`). Push-model adapters via async generators.
- Plotly Dash dashboard runs on `0.0.0.0:8050` (bound this way for Tailscale LAN access)

## Project structure

```
tape-intensity/
  adapters/            base.py, csv_adapter.py, replay_adapter.py, live_databento.py
  processor/           intensity.py
  app/                 dashboard.py
  config/              instruments.py
  sample_data/         CSV files for replay (gitignored except sample_nq.csv)
  main.py              unused initialization stub
  requirements.txt
```

## Three-layer architecture (strict separation — preserve when editing)

1. **Adapter layer** (`adapters/`): async generators yielding `TickEvent` objects. Anything source-specific (Databento side codes, vendor field names, timestamp formats) is translated to `TickEvent` *inside the adapter*. The processor must never see source-specific data.
2. **Processor layer** (`processor/intensity.py`): source-agnostic. Maintains rolling buffer, computes smoothed rates. Push model: `consume(adapter)` runs in a background task; `get_state()` returns a snapshot for the dashboard to read.
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

Y-axis auto-scale isn't updating across refreshes. The dashboard uses a *dynamic* `uirevision` tied to `price_range_key` precisely to prevent this. If you change the uirevision behavior, test that auto-scale still tracks price drift over a few minutes.

### Symptom: health light always red

The health light tracks **wall-clock** time since the most recent unique tick timestamp arrived, not tick-time age. This is intentional — tick-time age is meaningless for replay (timestamps are months old) and the question is always "is the data pipeline alive?", which is a wall-clock question.

If always red:
- The `_last_seen_tick_ts` and `_last_seen_wall` module globals aren't being updated. They live in `app/dashboard.py` at module scope.
- During replay, ticks arrive at replay speed; staleness threshold is 1s (green) / 5s (yellow). Replays at speed < 1.0x or quiet midday CSV sections may legitimately show yellow.
- During live, quiet markets can produce 1-3 second gaps between trades. Yellow flicker during midday lulls is normal. Persistent red means the connection died.

### Symptom: browser tab title flickers "Updating..." every 200ms

Fixed by `update_title=None` in the `Dash(...)` constructor. If it returns, that arg was removed.

### Symptom: chart shows sparse straight-line segments when returning from a backgrounded tab

Known limitation. Browser throttles `setInterval` calls in background tabs, so the dashboard's history deque (populated by the callback) accumulates sparse points. Data on the server is correct; the visualization fills back in within ~3 minutes of foreground time. Fix would be moving history population to a server-side task (decouple from the callback), not yet implemented.

### Symptom: pan/zoom gets "stuck" and chart freezes

Plotly's `uirevision` preserves user view state across refreshes. The current dynamic uirevision tag changes as the price range changes, which limits stickiness but lets auto-scale keep working. If you want stronger pan stickiness, coarsen the rounding in `price_range_key`. A page refresh always resets to live view.

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

## What is NOT yet built (deliberate scope, not bugs)

- Automatic live-feed reconnection on disconnect. Currently you restart the dashboard manually.
- Databento sequence-gap detection.
- Heartbeat-based health light integration (the wall-clock-staleness one works fine for most cases).
- Server-side history (browser-tab-backgrounding workaround).
- "Live" button to snap back to live view after pan/zoom interaction.
- Front-month auto-resolution for CSV mode raw symbols (live mode handles this via continuous symbology).
- Tailscale on the home machine (installed but not verified working from phone / work MacBook).

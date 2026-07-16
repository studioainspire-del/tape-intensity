"""
Live feed diagnostic script.

Bypasses the DatabentoLiveAdapter and prints raw SDK records directly.
Use when the live adapter shows unexpected behavior (flat lines, silent
drops, etc.) to verify whether the issue is in the SDK data itself or
in our translation logic.

See CLAUDE.md, section "Running the live diagnostic", for context.

Usage:
    python test_live_diag.py

Prints the first 20 records from a live subscription to MNQ.c.0, then exits.
Requires DATABENTO_API_KEY env var and an active Databento Live subscription.
"""

"""Diagnostic: connect to live, print first 20 records of every type."""
import asyncio
import logging
import databento as db
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

async def main():
    client = db.Live(key=os.environ["DATABENTO_API_KEY"])
    client.subscribe(
        dataset="GLBX.MDP3",
        schema="trades",
        stype_in="continuous",
        symbols=["MNQ.c.0"],
    )
    # NOTE: no client.start() call. Iteration starts streaming.

    count = 0
    async for record in client:
        count += 1
        print(f"--- record {count} ---")
        print(f"  type: {type(record).__name__}")
        print(f"  repr: {record!r}")
        if hasattr(record, 'side'):
            print(f"  side value: {record.side!r}")
            print(f"  side type: {type(record.side).__name__}")
        if count >= 20:
            break
    client.stop()

asyncio.run(main())
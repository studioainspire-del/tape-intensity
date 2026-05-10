"""
Phase 1 entry point: read a CSV and print ticks to the console.

Usage:
    python main.py                       # uses sample_data/sample_nq.csv
    python main.py path/to/trades.csv    # uses an explicit file
    python main.py path/to/trades.csv NQZ5   # filter to a single symbol

The output is intentionally verbose so you can eyeball the side
translation, the timestamp parsing, and the dropped-row counters.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from adapters.csv_adapter import CSVAdapter


DEFAULT_CSV = Path(__file__).parent / "sample_data" / "sample_nq.csv"


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if len(sys.argv) >= 2:
        csv_path = Path(sys.argv[1])
    else:
        csv_path = DEFAULT_CSV

    symbol_filter = sys.argv[2] if len(sys.argv) >= 3 else None

    adapter = CSVAdapter(csv_path, symbol_filter=symbol_filter)
    await adapter.connect()
    print(f"Connected. Health: {adapter.health}. Reading {csv_path}...")
    print("-" * 80)

    try:
        async for tick in adapter.stream():
            # Format: timestamp | symbol | side | size @ price
            print(
                f"{tick.timestamp.isoformat()} | "
                f"{tick.instrument_symbol:<8} | "
                f"{tick.aggressor_side:<4} | "
                f"{tick.size:>4} @ {tick.price:>10.4f}"
            )
    finally:
        await adapter.disconnect()

    print("-" * 80)
    print(f"Rows read:             {adapter.rows_read}")
    print(f"Ticks yielded:         {adapter.ticks_yielded}")
    print(f"Dropped (no aggressor): {adapter.dropped_no_aggressor}")
    print(f"Dropped (non-trade):    {adapter.dropped_non_trade}")
    print(f"Dropped (other symbol): {adapter.dropped_other_symbol}")
    print(f"Parse errors:          {adapter.parse_errors}")
    print(f"Final health:          {adapter.health}")


if __name__ == "__main__":
    asyncio.run(main())

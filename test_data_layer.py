"""
Smoke test for the data and state layers.
Loads sample positions CSV, fetches prices for all unique tickers,
and prints a table with ticker, current_price, and VIX level.
"""

import csv
import sys
from pathlib import Path

from options_cio.core.state_cache import StateCache
from options_cio.data.feed_adapter import YFinanceFeed, get_feed


def main() -> None:
    # --- Load positions from CSV ---
    csv_path = Path(__file__).parent / "options_cio" / "active_positions.csv"
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        positions = list(reader)

    # Normalize numeric types for StateCache
    for p in positions:
        p["strike"] = float(p["strike"])
        p["qty"] = int(p["qty"])
        p["entry_price"] = float(p["entry_price"])

    print(f"Loaded {len(positions)} positions from {csv_path.name}")

    # --- StateCache round-trip ---
    cache = StateCache(":memory:")
    cache.save_positions(positions)
    stored = cache.get_positions()
    print(f"StateCache: saved and retrieved {len(stored)} positions  [OK]")

    # Test greeks snapshot
    cache.save_greeks_snapshot(
        "2026-03-13T09:30:00", "P1",
        {"delta": 45.2, "gamma": 0.012, "theta": -8.5, "vega": 22.1}
    )
    latest = cache.get_latest_greeks("P1")
    assert latest["delta"] == 45.2, "Greeks round-trip failed"
    print("StateCache: greeks snapshot round-trip  [OK]")

    # Test violation
    cache.save_violation(
        "2026-03-13T09:31:00", "MAX_DELTA", "P2", "WARN", "Delta exceeds 50%"
    )
    violations = cache.get_violations_today("P2")
    assert len(violations) >= 1, "Violation round-trip failed"
    print("StateCache: violation round-trip  [OK]")

    # Test portfolio state
    cache.save_portfolio_state("P1", {
        "capital": 125000, "deployed": 42500,
        "deployment_pct": 0.34, "pnl_open": 1200, "hedge_ratio": 0.15,
    })
    state = cache.get_portfolio_state("P1")
    assert state["capital"] == 125000, "Portfolio state round-trip failed"
    print("StateCache: portfolio state round-trip  [OK]")

    # --- Feed adapter: fetch prices ---
    print("\n--- Market Data (yfinance) ---")
    feed = get_feed("yfinance")

    # Unique tickers from positions
    tickers = sorted(set(p["ticker"] for p in positions))
    print(f"Fetching prices for {len(tickers)} tickers: {tickers}")

    prices = feed.get_prices(tickers)

    # VIX
    try:
        vix = feed.get_vix()
    except ValueError:
        vix = None

    # Print table
    print(f"\n{'Ticker':<10} {'Price':>12}")
    print("-" * 24)
    for t in tickers:
        p = prices.get(t)
        print(f"{t:<10} {f'${p:,.2f}' if p else 'N/A':>12}")
    print("-" * 24)
    print(f"{'VIX':<10} {f'{vix:.2f}' if vix else 'N/A':>12}")

    print("\nAll checks passed.")


if __name__ == "__main__":
    main()

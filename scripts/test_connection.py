#!/usr/bin/env python3
"""
Tastytrade connection test — validates authentication, account discovery,
balances, positions, and live streaming Greeks.

Usage:
    python scripts/test_connection.py

Requires:
    TASTYTRADE_CLIENT_SECRET  — OAuth client secret
    TASTYTRADE_REFRESH_TOKEN  — OAuth refresh token
"""

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Ensure project root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def check_env():
    """Verify required environment variables are set."""
    missing = []
    for var in ("TASTYTRADE_CLIENT_SECRET", "TASTYTRADE_REFRESH_TOKEN"):
        if not os.environ.get(var):
            missing.append(var)
    if missing:
        print("\n[ERROR] Missing required environment variables:")
        for var in missing:
            print(f"  - {var}")
        print(
            "\nSet these in your environment or .env file:\n"
            "  export TASTYTRADE_CLIENT_SECRET='your-secret'\n"
            "  export TASTYTRADE_REFRESH_TOKEN='your-token'\n"
        )
        sys.exit(1)


def main():
    check_env()

    print("=" * 60)
    print("TASTYTRADE CONNECTION TEST")
    print("=" * 60)

    # Step 1: Authenticate
    print("\n[1/5] Authenticating with tastytrade...")
    from options_cio.data.tastytrade_adapter import TastytradeAdapter

    try:
        adapter = TastytradeAdapter()
        print("  ✓ Session established successfully")
    except Exception as e:
        print(f"  ✗ Authentication failed: {e}")
        sys.exit(1)

    # Step 2: List accounts
    print("\n[2/5] Discovering accounts...")
    try:
        accounts = adapter.get_accounts()
        for pid, acct in accounts.items():
            print(f"  {pid}: {acct.account_number} — found")
        if len(accounts) < 4:
            print(f"  ⚠ Only {len(accounts)}/4 accounts mapped (update accounts.yaml)")
        else:
            print("  ✓ All 4 portfolio accounts found")
    except Exception as e:
        print(f"  ✗ Account discovery failed: {e}")
        sys.exit(1)

    # Step 3: Balances for each account
    print("\n[3/5] Fetching balances...")
    for pid in accounts:
        try:
            bal = adapter.get_balances(pid)
            print(
                f"  {pid}: Net Liq ${bal['net_liquidating_value']:,.2f}  |  "
                f"OBP ${bal['option_buying_power']:,.2f}  |  "
                f"Deployed {bal['deployment_pct']:.1f}%"
            )
        except Exception as e:
            print(f"  {pid}: ✗ Balance fetch failed: {e}")

    # System totals
    try:
        sys_bal = adapter.get_system_balances()
        print(
            f"\n  SYSTEM: Net Liq ${sys_bal['system_net_liquidating_value']:,.2f}  |  "
            f"OBP ${sys_bal['system_option_buying_power']:,.2f}  |  "
            f"Deployed {sys_bal['system_deployment_pct']:.1f}%"
        )
    except Exception as e:
        print(f"  ✗ System balances failed: {e}")

    # Step 4: Positions
    print("\n[4/5] Fetching positions...")
    all_option_symbols = []
    for pid in accounts:
        try:
            positions = adapter.get_positions(pid)
            option_count = sum(
                1 for p in positions
                if p["instrument_type"] in ("Equity Option", "Future Option")
            )
            equity_count = sum(
                1 for p in positions if p["instrument_type"] == "Equity"
            )
            print(
                f"  {pid}: {len(positions)} positions "
                f"({option_count} options, {equity_count} equity)"
            )

            # Collect option symbols for streaming test
            for p in positions:
                if p["instrument_type"] in ("Equity Option", "Future Option"):
                    all_option_symbols.append(p["symbol"])

            # Show first 3 positions as sample
            for p in positions[:3]:
                direction = "+" if p["quantity_direction"] == "Long" else "-"
                print(
                    f"    {direction}{p['quantity']} {p['symbol']} "
                    f"@ ${p['average_open_price']:.2f}"
                )
            if len(positions) > 3:
                print(f"    ... and {len(positions) - 3} more")
        except Exception as e:
            print(f"  {pid}: ✗ Position fetch failed: {e}")

    # Step 5: Stream Greeks for one option (5 seconds)
    print("\n[5/5] Testing live Greeks stream...")
    if not all_option_symbols:
        print("  ⚠ No option positions found — skipping stream test")
    else:
        test_symbol = all_option_symbols[0]
        print(f"  Streaming Greeks for {test_symbol} (5 seconds)...")
        try:
            adapter._event_loop.run_until_complete(_stream_test(adapter.session, test_symbol))
        except Exception as e:
            print(f"  ✗ Streaming failed: {e}")

    print("\n" + "=" * 60)
    print("CONNECTION TEST COMPLETE")
    print("=" * 60)


async def _stream_test(session, symbol: str):
    """Stream Greeks for a single symbol for 5 seconds."""
    from options_cio.data.streamer import TastytradeStreamer

    async with TastytradeStreamer(session, [symbol]) as streamer:
        await streamer.run_for(seconds=5)

        greeks = streamer.get_greeks(symbol)
        quote = streamer.get_quote(symbol)

        if quote:
            print(
                f"  Quote: bid=${quote['bid_price']}  ask=${quote['ask_price']}"
            )
        else:
            print("  ⚠ No quote received in 5 seconds")

        if greeks:
            print(
                f"  Greeks: Δ={greeks['delta']:.4f}  Γ={greeks['gamma']:.4f}  "
                f"Θ={greeks['theta']:.4f}  V={greeks['vega']:.4f}  "
                f"IV={greeks['volatility']:.4f}"
            )
            print("  ✓ Live streaming works")
        else:
            print(
                "  ⚠ No Greeks received in 5 seconds "
                "(market may be closed — try during trading hours)"
            )


if __name__ == "__main__":
    main()

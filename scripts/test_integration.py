#!/usr/bin/env python3
"""
End-to-end integration test for Options CIO.

Validates the complete pipeline:
  1. Authenticate with tastytrade
  2. Discover all 4 accounts
  3. Fetch positions and balances
  4. Stream live Greeks
  5. Calculate portfolio Greeks
  6. Run full rules evaluation
  7. Generate daily CIO review
  8. Run what-if simulation
  9. Sync trade journal
  10. Validate config files

Usage:
    python scripts/test_integration.py
"""

import asyncio
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from options_cio.logging_config import setup_logging

setup_logging()

BASE_DIR = Path(__file__).resolve().parent.parent / "options_cio"

PASS = "\u2713"
FAIL = "\u2717"
WARN = "\u26a0"

results: list[tuple[str, str, str]] = []  # (status, test_name, detail)


def record(status: str, name: str, detail: str = "") -> None:
    results.append((status, name, detail))
    icon = PASS if status == "pass" else FAIL if status == "fail" else WARN
    print(f"  {icon} {name}" + (f" — {detail}" if detail else ""))


def main():
    print("=" * 60)
    print("  OPTIONS CIO — INTEGRATION TEST")
    print("=" * 60)

    # ── 0. Config validation ──────────────────────────────────
    print("\n[0/9] Validating configuration files...")
    from options_cio.main import _validate_configs
    errors = _validate_configs()
    if errors:
        for e in errors:
            record("fail", f"Config: {e}")
    else:
        record("pass", "All config files valid")

    # ── 1. Authenticate ───────────────────────────────────────
    print("\n[1/9] Authenticating with tastytrade...")
    try:
        from options_cio.data.tastytrade_adapter import TastytradeAdapter
        adapter = TastytradeAdapter()
        record("pass", "Authentication")
    except Exception as e:
        record("fail", "Authentication", str(e))
        _print_summary()
        sys.exit(1)

    # ── 2. Discover accounts ──────────────────────────────────
    print("\n[2/9] Discovering accounts...")
    try:
        accounts = adapter.get_accounts()
        found = len(accounts)
        if found == 4:
            record("pass", f"All 4 accounts found")
        else:
            record("warn", f"Found {found}/4 accounts", ", ".join(sorted(accounts.keys())))
        for pid, acct in sorted(accounts.items()):
            print(f"    {pid}: {acct.account_number}")
    except Exception as e:
        record("fail", "Account discovery", str(e))

    # ── 3. Fetch positions and balances ───────────────────────
    print("\n[3/9] Fetching positions and balances...")
    total_positions = 0
    total_net_liq = 0.0
    option_symbols = []
    for pid in sorted(accounts.keys()):
        try:
            positions = adapter.get_positions(pid)
            bal = adapter.get_balances(pid)
            total_positions += len(positions)
            total_net_liq += bal["net_liquidating_value"]
            for p in positions:
                if p.get("instrument_type") in ("Equity Option", "Future Option"):
                    option_symbols.append(p["symbol"])
            record("pass", f"{pid}: {len(positions)} positions, "
                   f"NLV ${bal['net_liquidating_value']:,.0f}, "
                   f"Deploy {bal['deployment_pct']:.1f}%")
        except Exception as e:
            record("fail", f"{pid} positions/balances", str(e))

    print(f"    Total: {total_positions} positions, NLV ${total_net_liq:,.0f}")

    # ── 4. Stream live Greeks ─────────────────────────────────
    print("\n[4/9] Streaming live Greeks (5 seconds)...")
    greeks_received = 0
    if not option_symbols:
        record("warn", "No option positions to stream")
    else:
        test_symbols = option_symbols[:5]  # test a subset
        try:
            from options_cio.data.streamer import TastytradeStreamer

            async def _stream():
                async with TastytradeStreamer(adapter.session, test_symbols) as s:
                    await s.run_for(seconds=5)
                    return sum(1 for sym in test_symbols if s.get_greeks(sym) is not None)

            greeks_received = adapter._event_loop.run_until_complete(_stream())
            if greeks_received > 0:
                record("pass", f"Greeks received for {greeks_received}/{len(test_symbols)} symbols")
            else:
                record("warn", "No Greeks received (market may be closed)")
        except Exception as e:
            record("fail", "Streamer", str(e))

    # ── 5. Calculate portfolio Greeks ─────────────────────────
    print("\n[5/9] Calculating portfolio-level Greeks...")
    if option_symbols and greeks_received > 0:
        try:
            from options_cio.data.streamer import TastytradeStreamer
            from options_cio.core.greeks_engine import GreeksEngine

            async def _greeks():
                async with TastytradeStreamer(adapter.session, option_symbols) as s:
                    await s.run_for(seconds=5)
                    engine = GreeksEngine(adapter, s)
                    summaries = {}
                    for pid in accounts:
                        summaries[pid] = engine.summary(pid)
                    return summaries, engine

            summaries, engine = adapter._event_loop.run_until_complete(_greeks())
            for pid, g in sorted(summaries.items()):
                record("pass", f"{pid}: D={g['delta']:+.1f} G={g['gamma']:+.4f} "
                       f"T={g['theta']:+.1f} V={g['vega']:+.1f}")
        except Exception as e:
            record("fail", "Portfolio Greeks", str(e))
    else:
        record("warn", "Skipped — no streamer data available")

    # ── 6. Run full rules evaluation ──────────────────────────
    print("\n[6/9] Running rules evaluation...")
    try:
        import json
        from options_cio.core.rules_engine import RulesEngine

        with open(BASE_DIR / "config" / "portfolios.json") as f:
            portfolios_config = json.load(f)
        rules_path = BASE_DIR / "config" / "trading_rules.json"
        rules = RulesEngine(rules_path, portfolios_config, adapter, None)
        result = rules.evaluate_all()
        record("pass", f"Rules: {len(result.alerts)} alerts, "
               f"state={result.system_state.value}, "
               f"compliance={result.compliance_score:.0f}%")
        for alert in result.alerts[:5]:
            print(f"    [{alert.severity.value}] {alert.message[:60]}")
        if len(result.alerts) > 5:
            print(f"    ... and {len(result.alerts) - 5} more")
    except Exception as e:
        record("fail", "Rules evaluation", str(e))

    # ── 7. Generate daily CIO review ──────────────────────────
    print("\n[7/9] Generating daily review...")
    import yaml
    try:
        with open(BASE_DIR / "config" / "settings.yaml") as f:
            settings = yaml.safe_load(f)

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            settings["ai_offline"] = True

        from options_cio.daily_review.cio_review import CIODailyReview
        review = CIODailyReview(
            positions_path=BASE_DIR / "active_positions.csv",
            portfolios_config_path=BASE_DIR / "config" / "portfolios.json",
            rules_path=BASE_DIR / "config" / "trading_rules.json",
            settings=settings,
            db_path=settings.get("db_path", "./options_cio.db"),
            api_key=api_key,
        )
        output = review.run()
        lines = output.strip().split("\n")
        record("pass", f"Daily review generated ({len(lines)} lines)")
        # Show first and last few lines
        for line in lines[:3]:
            print(f"    {line}")
        print("    ...")
        for line in lines[-3:]:
            print(f"    {line}")
    except Exception as e:
        record("fail", "Daily review", str(e))

    # ── 8. What-if simulation ─────────────────────────────────
    print("\n[8/9] Running what-if simulation...")
    try:
        from options_cio.simulator.what_if import WhatIfSimulator, SCENARIOS

        sim = WhatIfSimulator()
        greeks_map = {}
        capital_map = {}
        for pid, cfg in portfolios_config.get("portfolios", {}).items():
            greeks_map[pid] = {"delta": 0, "gamma": 0, "theta": 0, "vega": 0}
            capital_map[pid] = cfg["capital"]

        results_sim = sim.run_scenario("crash_20", greeks_map, capital_map)
        record("pass", f"Scenario crash_20: {len(results_sim)} portfolio results")
        for r in results_sim:
            print(f"    {r}")
    except Exception as e:
        record("fail", "What-if simulation", str(e))

    # ── 9. Journal sync ───────────────────────────────────────
    print("\n[9/9] Syncing trade journal...")
    try:
        from options_cio.journal.trade_journal import TradeJournal
        journal = TradeJournal(settings.get("db_path", "./options_cio.db"))
        sync_result = journal.sync_from_broker(adapter, lookback_days=7)
        record("pass", f"Journal sync: {sync_result}")
    except Exception as e:
        record("fail", "Journal sync", str(e))

    _print_summary()


def _print_summary():
    print("\n" + "=" * 60)
    print("  INTEGRATION TEST SUMMARY")
    print("=" * 60)
    passed = sum(1 for s, _, _ in results if s == "pass")
    failed = sum(1 for s, _, _ in results if s == "fail")
    warned = sum(1 for s, _, _ in results if s == "warn")
    total = len(results)

    print(f"\n  {PASS} Passed: {passed}/{total}")
    if warned:
        print(f"  {WARN} Warnings: {warned}")
    if failed:
        print(f"  {FAIL} Failed: {failed}")
        print("\n  Failed tests:")
        for s, name, detail in results:
            if s == "fail":
                print(f"    {FAIL} {name}: {detail}")

    print()
    if failed == 0:
        print("  All critical tests passed.")
    else:
        print("  Some tests failed — see details above.")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()

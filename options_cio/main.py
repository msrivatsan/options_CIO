"""
Options CIO — main entry point.

Startup validation sequence (tastytrade mode):
  a. Check env vars (TASTYTRADE_CLIENT_SECRET, TASTYTRADE_REFRESH_TOKEN)
  b. Test authentication (Account.get)
  c. Load and validate accounts.yaml
  d. Test streamer connectivity (SPY quote, 3s)
  e. Verify config files (trading_rules.json, portfolios.json, accounts.yaml, settings.yaml)
  f. Verify Claude API key or ai_offline
  g. Print startup checklist summary

Usage:
    python -m options_cio.main              # Launch TUI dashboard
    python -m options_cio.main --review     # Run daily review and print to stdout
    python -m options_cio.main --offline    # Force AI offline mode
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

import yaml

BASE_DIR = Path(__file__).parent


def load_settings() -> dict:
    settings_path = BASE_DIR / "config" / "settings.yaml"
    with open(settings_path) as f:
        return yaml.safe_load(f)


# ------------------------------------------------------------------
# Startup validation
# ------------------------------------------------------------------

def _validate_configs() -> list[str]:
    """Validate all config files. Returns list of errors (empty = OK)."""
    errors = []
    config_dir = BASE_DIR / "config"

    # settings.yaml
    try:
        with open(config_dir / "settings.yaml") as f:
            settings = yaml.safe_load(f)
        required_keys = ["data_source", "db_path"]
        for key in required_keys:
            if key not in settings:
                errors.append(f"settings.yaml: missing required key '{key}'")
    except Exception as e:
        errors.append(f"settings.yaml: {e}")

    # portfolios.json
    try:
        with open(config_dir / "portfolios.json") as f:
            portfolios = json.load(f)
        pf = portfolios.get("portfolios", {})
        for pid in ["P1", "P2", "P3", "P4"]:
            if pid not in pf:
                errors.append(f"portfolios.json: portfolio {pid} not defined")
            else:
                band = pf[pid].get("deployment_band", [])
                if len(band) != 2 or band[0] > band[1]:
                    errors.append(f"portfolios.json: {pid} has invalid deployment_band")
    except Exception as e:
        errors.append(f"portfolios.json: {e}")

    # trading_rules.json
    try:
        with open(config_dir / "trading_rules.json") as f:
            rules = json.load(f)
        rule_ids = [r["id"] for r in rules.get("rules", [])]
        rule_ids += [r["id"] for r in rules.get("system_rules", [])]
        if len(rule_ids) != len(set(rule_ids)):
            errors.append("trading_rules.json: duplicate rule IDs found")
    except Exception as e:
        errors.append(f"trading_rules.json: {e}")

    # accounts.yaml
    try:
        with open(config_dir / "accounts.yaml") as f:
            accts = yaml.safe_load(f)
        entries = accts.get("accounts", [])
        if len(entries) != 4:
            errors.append(f"accounts.yaml: expected 4 accounts, found {len(entries)}")
    except Exception as e:
        errors.append(f"accounts.yaml: {e}")

    return errors


def _validate_database(db_path: str) -> None:
    """Run integrity check on SQLite database. Rename if corrupt."""
    db = Path(db_path)
    if not db.exists():
        return  # will be created on first use

    try:
        conn = sqlite3.connect(str(db))
        result = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        if result[0] != "ok":
            raise sqlite3.DatabaseError(f"integrity_check returned: {result[0]}")
    except Exception as e:
        print(f"  ! Database corrupt: {e}")
        corrupt_path = db.with_suffix(".db.corrupt")
        print(f"  ! Renaming to {corrupt_path.name} and creating fresh database")
        if corrupt_path.exists():
            corrupt_path.unlink()
        db.rename(corrupt_path)
        # Remove WAL files too
        for suffix in (".db-shm", ".db-wal"):
            wal = db.with_suffix(suffix)
            if wal.exists():
                wal.unlink()


def _run_startup_checks(settings: dict, api_key: str) -> dict:
    """Run full startup validation for tastytrade mode.

    Returns a dict with:
      - ok: bool — all critical checks passed
      - adapter: TastytradeAdapter or None
      - accounts_found: int
      - accounts_missing: list[str]
      - streamer_ok: bool
      - messages: list of (symbol, message) tuples for the checklist
    """
    result = {
        "ok": True,
        "adapter": None,
        "accounts_found": 0,
        "accounts_missing": [],
        "streamer_ok": False,
        "messages": [],
    }

    # (a) Check environment variables
    for var in ("TASTYTRADE_CLIENT_SECRET", "TASTYTRADE_REFRESH_TOKEN"):
        if not os.environ.get(var):
            print(f"Missing {var}. Set it via: export {var}='your_value'")
            sys.exit(1)
    result["messages"].append(("pass", "Tastytrade credentials found"))

    # (b) Test authentication
    try:
        from options_cio.data.tastytrade_adapter import TastytradeAdapter
        adapter = TastytradeAdapter()
        accounts = adapter.get_accounts()
        result["adapter"] = adapter
        result["accounts_found"] = len(accounts)
    except Exception as e:
        error_str = str(e)
        if "401" in error_str or "Unauthorized" in error_str:
            print(
                "Authentication failed. Your refresh token may be revoked.\n"
                "Regenerate it at tastytrade.com -> OAuth Applications -> Manage -> Create Grant"
            )
            sys.exit(1)
        print(f"Tastytrade authentication error: {e}")
        sys.exit(1)

    # (c) Verify all 4 accounts
    expected = {"P1", "P2", "P3", "P4"}
    found = set(accounts.keys())
    missing = expected - found
    if missing:
        result["accounts_missing"] = sorted(missing)
        for pid in sorted(missing):
            print(f"  ! Account for {pid} not found — check config/accounts.yaml mapping")
        result["messages"].append(("warn", f"Authenticated ({len(found)}/4 accounts — {', '.join(sorted(missing))} missing)"))
    else:
        result["messages"].append(("pass", f"Authenticated ({len(found)}/4 accounts found)"))

    # (d) Test streamer connectivity
    try:
        import asyncio
        from options_cio.data.streamer import TastytradeStreamer

        async def _test_streamer():
            async with TastytradeStreamer(adapter.session, [".SPY"]) as s:
                await s.run_for(seconds=3)
                return s.get_quote(".SPY") is not None

        got_data = adapter._event_loop.run_until_complete(_test_streamer())
        if got_data:
            result["streamer_ok"] = True
            result["messages"].append(("pass", "Streamer connected"))
        else:
            result["messages"].append(("warn", "Streamer timeout — will retry on dashboard launch"))
    except Exception as e:
        result["messages"].append(("warn", f"Streamer test failed: {e} — will retry on dashboard launch"))

    # (e) Verify config files
    config_errors = _validate_configs()
    if config_errors:
        for err in config_errors:
            print(f"  Config error: {err}")
        sys.exit(1)
    result["messages"].append(("pass", "Config files valid"))

    # (f) Verify Claude API key or ai_offline
    ai_offline = settings.get("ai_offline", False)
    if ai_offline:
        result["messages"].append(("pass", "AI offline mode"))
    elif api_key:
        result["messages"].append(("pass", "Claude API ready"))
    else:
        print("Set ANTHROPIC_API_KEY or enable ai_offline in settings.yaml")
        sys.exit(1)

    # (g) Verify database
    db_path = settings.get("db_path", "./options_cio.db")
    _validate_database(db_path)
    result["messages"].append(("pass", "Database initialized"))

    return result


def _print_checklist(result: dict) -> None:
    """Print the startup checklist summary."""
    print()
    for symbol, message in result["messages"]:
        if symbol == "pass":
            print(f"  \u2713 {message}")
        elif symbol == "warn":
            print(f"  \u26a0 {message}")
        else:
            print(f"  \u2717 {message}")
    print()


# ------------------------------------------------------------------
# Run modes
# ------------------------------------------------------------------

def run_dashboard(settings: dict, api_key: str) -> None:
    from options_cio.ui.dashboard import OptionsCIODashboard
    app = OptionsCIODashboard(settings=settings, api_key=api_key)
    app.run()


def run_daily_review(settings: dict, api_key: str) -> None:
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
    print(output)


def main() -> None:
    parser = argparse.ArgumentParser(description="Options CIO — Systematic Options Portfolio Manager")
    parser.add_argument("--review", action="store_true", help="Run daily review (CLI mode, no TUI)")
    parser.add_argument("--offline", action="store_true", help="Force AI offline mode")
    parser.add_argument("--api-key", type=str, default="", help="Anthropic API key (overrides env var)")
    parser.add_argument("--skip-checks", action="store_true", help="Skip startup validation")
    args = parser.parse_args()

    # Set up logging first
    from options_cio.logging_config import setup_logging
    setup_logging()

    # Load .env if available
    try:
        from dotenv import load_dotenv
        load_dotenv(BASE_DIR.parent / ".env")
    except ImportError:
        pass

    settings = load_settings()

    if args.offline:
        settings["ai_offline"] = True

    api_key = args.api_key or os.environ.get("ANTHROPIC_API_KEY", "")

    # Run startup checks for tastytrade mode
    is_tastytrade = settings.get("data_source") == "tastytrade"
    if is_tastytrade and not args.skip_checks:
        print("=" * 50)
        print("  OPTIONS CIO — Startup Checks")
        print("=" * 50)

        result = _run_startup_checks(settings, api_key)
        _print_checklist(result)

        if args.review:
            print("Launching daily review...")
        else:
            print("Launching dashboard...")
    elif not is_tastytrade:
        # Minimal validation for non-tastytrade mode
        config_errors = _validate_configs()
        if config_errors:
            for err in config_errors:
                print(f"Config error: {err}")
            sys.exit(1)

    if args.review:
        run_daily_review(settings, api_key)
    else:
        run_dashboard(settings, api_key)


if __name__ == "__main__":
    main()

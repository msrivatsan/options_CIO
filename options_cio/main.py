"""
Options CIO — main entry point.

Usage:
    python main.py              # Launch TUI dashboard
    python main.py --review     # Run daily review and print to stdout
    python main.py --offline    # Force AI offline mode
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import yaml

BASE_DIR = Path(__file__).parent


def load_settings() -> dict:
    settings_path = BASE_DIR / "config" / "settings.yaml"
    with open(settings_path) as f:
        return yaml.safe_load(f)


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
    args = parser.parse_args()

    settings = load_settings()

    if args.offline:
        settings["ai_offline"] = True

    api_key = args.api_key or os.environ.get("ANTHROPIC_API_KEY", "")

    if args.review:
        run_daily_review(settings, api_key)
    else:
        run_dashboard(settings, api_key)


if __name__ == "__main__":
    main()

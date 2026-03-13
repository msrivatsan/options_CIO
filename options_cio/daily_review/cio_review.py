"""
CIO Daily Review — orchestrates the full morning review pipeline:
  1. Load positions
  2. Fetch market data
  3. Compute Greeks
  4. Run rules engine
  5. Call AI brain
  6. Log to journal
  7. Return formatted review text
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Optional

from options_cio.core.greeks_engine import GreeksEngine
from options_cio.core.portfolio_manager import PortfolioManager
from options_cio.core.rules_engine import RulesEngine
from options_cio.core.state_cache import StateCache
from options_cio.ai.cio_brain import CIOBrain
from options_cio.data.feed_adapter import YFinanceFeed
from options_cio.journal.trade_journal import TradeJournal
from options_cio.simulator.what_if import WhatIfSimulator


class CIODailyReview:
    """
    Runs the complete daily review and returns formatted output.
    Can be triggered manually or on a schedule.
    """

    def __init__(
        self,
        positions_path: str | Path,
        portfolios_config_path: str | Path,
        rules_path: str | Path,
        settings: dict,
        db_path: str = "./options_cio.db",
        api_key: Optional[str] = None,
    ) -> None:
        self.settings = settings
        self.positions_path = Path(positions_path)

        with open(portfolios_config_path) as f:
            self.portfolios_config = json.load(f)

        self.capital_map = {
            pid: cfg["capital"]
            for pid, cfg in self.portfolios_config["portfolios"].items()
        }

        self.pm = PortfolioManager(positions_path, db_path).load()
        self.rules = RulesEngine(rules_path)
        self.greeks = GreeksEngine()
        self.feed = YFinanceFeed()
        self.brain = CIOBrain(
            model=settings.get("api_model", "claude-sonnet-4-20250514"),
            ai_offline=settings.get("ai_offline", False),
            max_daily_cost=settings.get("max_api_cost_per_day", 5.00),
            api_key=api_key,
        )
        self.journal = TradeJournal(db_path)
        self.cache = StateCache(db_path)
        self.simulator = WhatIfSimulator()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> str:
        # 1. Collect unique tickers
        all_positions = self.pm.get_all_positions()
        tickers = list(set(p["ticker"] for p in all_positions))

        # 2. Fetch prices
        price_map = self.feed.get_prices(tickers)
        iv_map = {}
        for t in tickers:
            try:
                iv_map[t] = self.feed.get_iv_rank(t) / 100.0
            except ValueError:
                iv_map[t] = 0.30
        market_snapshot = self.feed.get_market_snapshot()
        vix = float(market_snapshot.get("vix") or 20.0)

        # 3. Compute Greeks per portfolio
        greeks_summaries = []
        all_alerts: list[str] = []

        portfolio_states = self.pm.get_all_portfolio_states(price_map, self.capital_map, vix)

        for pid in self.pm.get_portfolio_ids():
            positions = self.pm.get_positions_for_portfolio(pid)
            pg = self.greeks.compute_portfolio_greeks(pid, positions, price_map, iv_map)
            summary = pg.summary()
            greeks_summaries.append(summary)

            # 4. Rules evaluation
            alerts = self.rules.evaluate_portfolio(
                portfolio_id=pid,
                positions=positions,
                greeks_summary=summary,
                capital=self.capital_map.get(pid, 125000),
            )
            for alert in alerts:
                all_alerts.append(str(alert))
                self.journal.log_alert(
                    rule_id=alert.rule_id,
                    severity=alert.severity.value,
                    message=alert.message,
                    portfolio=alert.portfolio,
                    ticker=alert.ticker,
                    value=alert.value,
                    threshold=alert.threshold,
                )

        # System-level rules
        system_alerts = self.rules.evaluate_system(portfolio_states)
        for alert in system_alerts:
            all_alerts.append(str(alert))

        # 5. Positions summary for AI
        positions_summary = self._format_positions_summary(all_positions)

        # 6. AI review
        ai_output = self.brain.daily_review(
            portfolio_states=portfolio_states,
            greeks_summaries=greeks_summaries,
            rule_alerts=all_alerts,
            market_snapshot=market_snapshot,
            positions_summary=positions_summary,
        )

        # 7. Log AI review
        self.journal.log_ai_review(
            review_type="daily",
            ai_output=ai_output,
            input_summary=f"VIX={vix} | alerts={len(all_alerts)}",
            cost_usd=self.brain.get_daily_cost(),
        )

        return self._format_output(market_snapshot, greeks_summaries, all_alerts, ai_output)

    # ------------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------------

    def _format_positions_summary(self, positions: list[dict]) -> str:
        lines = []
        for p in positions:
            lines.append(
                f"{p['portfolio']} | {p['ticker']} {p['option_type']} {p['strike']} "
                f"exp:{p['expiry']} qty:{p['qty']} @ ${p['entry_price']} [{p['structure_tag']}] "
                f"DTE:{p.get('dte', '?')}"
            )
        return "\n".join(lines)

    def _format_output(
        self,
        market_snapshot: dict,
        greeks_summaries: list[dict],
        alerts: list[str],
        ai_output: str,
    ) -> str:
        separator = "=" * 60
        lines = [
            separator,
            f"  OPTIONS CIO — DAILY REVIEW  {date.today()}",
            separator,
            "",
            f"  SPX: {market_snapshot.get('spx', 'N/A')}  |  VIX: {market_snapshot.get('vix', 'N/A')}",
            f"  BTC: {market_snapshot.get('btc', 'N/A')}  |  10Y: {market_snapshot.get('yield_10y', 'N/A')}",
            "",
            "── Greeks Summary ──────────────────────────────────────",
        ]
        for g in greeks_summaries:
            lines.append(
                f"  {g['portfolio']:4s}  Δ={g['delta']:+.1f}  Γ={g['gamma']:+.4f}  "
                f"Θ={g['theta']:+.1f}  V={g['vega']:+.1f}"
            )
        lines.append("")
        if alerts:
            lines.append("── Active Alerts ───────────────────────────────────────")
            for a in alerts:
                lines.append(f"  {a}")
            lines.append("")
        lines.append("── CIO Analysis ────────────────────────────────────────")
        lines.append(ai_output)
        lines.append("")
        lines.append(separator)
        return "\n".join(lines)

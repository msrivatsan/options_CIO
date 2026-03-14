"""
CIO Daily Review — orchestrates the full morning review pipeline:
  1. Load positions
  2. Fetch market data
  3. Compute Greeks (live from tastytrade DXLink, or zeroed in offline/yfinance mode)
  4. Run rules engine
  5. Call AI brain
  6. Log to journal
  7. Return formatted review text
"""

from __future__ import annotations

import json
import logging
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

logger = logging.getLogger(__name__)

# How long to collect live Greeks before the daily review proceeds
_STREAM_COLLECT_SECONDS = 10


def _zero_greeks_summary(portfolio_id: str) -> dict:
    return {
        "portfolio": portfolio_id,
        "delta": 0.0,
        "gamma": 0.0,
        "theta": 0.0,
        "vega": 0.0,
        "position_count": 0,
        "pending_count": 0,
        "stale_count": 0,
    }


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
        self._data_source = settings.get("data_source", "yfinance")

        with open(portfolios_config_path) as f:
            self.portfolios_config = json.load(f)

        self.capital_map = {
            pid: cfg["capital"]
            for pid, cfg in self.portfolios_config["portfolios"].items()
        }

        self.pm = PortfolioManager(positions_path, db_path).load()
        self.rules = RulesEngine(rules_path)
        self.feed = YFinanceFeed()  # always used for market snapshot (VIX, SPX)
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
        market_snapshot = self.feed.get_market_snapshot()
        vix = float(market_snapshot.get("vix") or 20.0)

        portfolio_states = self.pm.get_all_portfolio_states(
            {}, self.capital_map, vix
        )

        greeks_summaries: list[dict] = []
        all_alerts: list[str] = []

        if self._data_source == "tastytrade":
            greeks_engine = self._build_live_greeks_engine()
        else:
            greeks_engine = None

        for pid in self.pm.get_portfolio_ids():
            positions = self.pm.get_positions_for_portfolio(pid)

            if greeks_engine is not None:
                try:
                    summary = greeks_engine.summary(pid)
                except Exception as e:
                    logger.warning("Greeks fetch failed for %s: %s", pid, e)
                    summary = _zero_greeks_summary(pid)
            else:
                summary = _zero_greeks_summary(pid)

            greeks_summaries.append(summary)

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

        system_alerts = self.rules.evaluate_system(portfolio_states)
        for alert in system_alerts:
            all_alerts.append(str(alert))

        all_positions = self.pm.get_all_positions()
        positions_summary = self._format_positions_summary(all_positions)

        ai_output = self.brain.daily_review(
            portfolio_states=portfolio_states,
            greeks_summaries=greeks_summaries,
            rule_alerts=all_alerts,
            market_snapshot=market_snapshot,
            positions_summary=positions_summary,
        )

        self.journal.log_ai_review(
            review_type="daily",
            ai_output=ai_output,
            input_summary=f"VIX={vix} | alerts={len(all_alerts)}",
            cost_usd=self.brain.get_daily_cost(),
        )

        return self._format_output(market_snapshot, greeks_summaries, all_alerts, ai_output)

    # ------------------------------------------------------------------
    # Live Greeks via tastytrade
    # ------------------------------------------------------------------

    def _build_live_greeks_engine(self) -> GreeksEngine | None:
        """
        Create a TastytradeAdapter, collect live Greeks for all option
        positions for _STREAM_COLLECT_SECONDS, then return a GreeksEngine
        backed by the populated streamer snapshot.

        The streamer context exits after collection but live_data remains
        readable — sufficient for a one-shot daily review.
        """
        try:
            from options_cio.data.tastytrade_adapter import TastytradeAdapter
            from options_cio.data.streamer import TastytradeStreamer

            adapter = TastytradeAdapter()
            accounts = adapter.get_accounts()

            # Collect all option symbols across all portfolios
            symbols: list[str] = []
            for pid in accounts:
                positions = adapter.get_positions(pid)
                for pos in positions:
                    if pos.get("instrument_type") in ("Equity Option", "Future Option"):
                        sym = pos["symbol"]
                        if sym not in symbols:
                            symbols.append(sym)

            if not symbols:
                logger.warning("No option positions found — skipping live Greeks")
                return None

            logger.info("Collecting live Greeks for %d symbols (%ds)...",
                        len(symbols), _STREAM_COLLECT_SECONDS)

            # Run the streamer on the adapter's event loop
            async def _collect(streamer: TastytradeStreamer):
                async with streamer:
                    await streamer.run_for(seconds=_STREAM_COLLECT_SECONDS)

            streamer = TastytradeStreamer(adapter.session, symbols)
            adapter._event_loop.run_until_complete(_collect(streamer))

            received = sum(1 for s in symbols if streamer.get_greeks(s) is not None)
            logger.info("Greeks received for %d/%d symbols", received, len(symbols))

            return GreeksEngine(adapter, streamer)

        except Exception as e:
            logger.error("Could not build live Greeks engine: %s", e)
            return None

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
            pending = g.get("pending_count", 0)
            stale = g.get("stale_count", 0)
            flags = ""
            if pending:
                flags += f" [{pending} PENDING]"
            if stale:
                flags += f" [{stale} STALE]"
            lines.append(
                f"  {g['portfolio']:4s}  Δ={g['delta']:+.1f}  Γ={g['gamma']:+.4f}  "
                f"Θ={g['theta']:+.1f}  V={g['vega']:+.1f}{flags}"
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

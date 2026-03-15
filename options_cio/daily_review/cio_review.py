"""
CIO Daily Review — orchestrates the full morning review pipeline:
  1. Connect to tastytrade (or fall back to CSV/yfinance)
  2. Fetch market data — adapter.get_market_metrics() for IV, yfinance for prices
  3. Compute Greeks via DXLink streamer (live, not estimated)
  4. Run rules engine against live positions and balances
  5. Call AI brain with full live context
  6. Log to journal
  7. Return formatted review text

In tastytrade mode, the review runs fully automatically with zero manual
input — no screenshots, no CSV exports.  All data is pulled live from the
broker API and DXLink streaming websocket.
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

        self._rules_path = rules_path
        self._adapter = None
        self._greeks_engine: GreeksEngine | None = None
        self.pm = None  # initialized after adapter or in fallback mode
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
        if self._data_source == "tastytrade":
            return self._run_live()

        # Non-tastytrade: fall back to CSV + yfinance
        from options_cio.data.feed_adapter import YFinanceFeed

        feed = YFinanceFeed()
        market_snapshot = feed.get_market_snapshot()
        vix = float(market_snapshot.get("vix") or 20.0)
        return self._run_csv_fallback(market_snapshot, vix)

    # ------------------------------------------------------------------
    # Live tastytrade review (zero manual input)
    # ------------------------------------------------------------------

    def _run_live(self) -> str:
        """Full daily review using only live tastytrade data.

        Data sources:
          - Market context: adapter.get_market_metrics() for IV data,
            yfinance for VIX/SPX/BTC/10Y prices (tastytrade doesn't serve these)
          - Capital & risk: adapter.get_balances() for OBP per account
          - Greeks: DXLink streamer — always visible when connected
          - Positions: adapter.get_positions() per account
          - Rules: RulesEngine consuming live adapter + greeks engine
        """
        # 1. Connect to tastytrade and stream Greeks
        adapter, greeks_engine = self._build_live_greeks_engine()
        if adapter is None:
            logger.error("Tastytrade connection failed — falling back to CSV")
            from options_cio.data.feed_adapter import YFinanceFeed

            feed = YFinanceFeed()
            market_snapshot = feed.get_market_snapshot()
            vix = float(market_snapshot.get("vix") or 20.0)
            return self._run_csv_fallback(market_snapshot, vix)

        self._adapter = adapter
        self._greeks_engine = greeks_engine
        self.brain._adapter = adapter
        self.simulator._adapter = adapter
        streamer = greeks_engine.streamer if greeks_engine else None
        if streamer is not None:
            self.brain._streamer = streamer
            self.simulator._streamer = streamer

        self.pm = PortfolioManager(
            self.portfolios_config, adapter, self.cache,
        )

        # 2. Build market snapshot: yfinance prices + tastytrade IV metrics
        market_snapshot = self._build_market_snapshot(adapter)
        vix = float(market_snapshot.get("vix") or 20.0)

        # 3. Auto-sync journal from broker transactions
        try:
            self.journal.sync_from_broker(adapter)
            self.journal.detect_position_changes(adapter, streamer)
        except Exception as e:
            logger.warning("Journal broker sync failed: %s", e)

        # 4. Check account connectivity
        connectivity = self.pm.check_account_connectivity()
        if not connectivity["all_connected"]:
            logger.warning(
                "Account connectivity issues: %s",
                connectivity["disconnected"],
            )

        # 5. Run rules engine against live data
        rules = RulesEngine(
            self._rules_path, self.portfolios_config,
            adapter, greeks_engine,
        )
        result = rules.evaluate_all()

        all_alerts: list[str] = []
        for alert in result.alerts:
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

        # 6. Portfolio states from live PM (balances from adapter.get_balances)
        portfolio_states = self.pm.get_all_portfolio_states(vix=vix)

        # 7. Collect greeks summaries — streamer always has data when connected
        greeks_summaries = self._collect_greeks_summaries(adapter, greeks_engine)

        # 8. Format positions from live adapter
        positions_summary = self._format_live_positions()

        # 9. AI review
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

        return self._format_output(
            market_snapshot, greeks_summaries, all_alerts, ai_output,
            streamer_connected=streamer is not None,
        )

    # ------------------------------------------------------------------
    # Market snapshot: yfinance prices + tastytrade IV metrics
    # ------------------------------------------------------------------

    def _build_market_snapshot(self, adapter) -> dict:
        """Build market snapshot combining yfinance prices with tastytrade IV.

        VIX, SPX, BTC, and 10Y prices come from yfinance (tastytrade doesn't
        serve index prices).  IV rank, IV percentile, and other vol metrics
        come from adapter.get_market_metrics() on the book's underlyings.
        """
        from options_cio.data.feed_adapter import YFinanceFeed

        feed = YFinanceFeed()
        snapshot = feed.get_market_snapshot()

        # Collect unique underlyings across all portfolios
        underlyings: list[str] = []
        for pid in self.pm.get_portfolio_ids():
            for pos in adapter.get_positions(pid):
                sym = pos.get("underlying_symbol", "")
                if sym and sym not in underlyings:
                    underlyings.append(sym)

        # Fetch tastytrade market metrics for all book underlyings
        if underlyings:
            try:
                metrics = adapter.get_market_metrics(underlyings)
                snapshot["market_metrics"] = metrics
            except Exception as e:
                logger.warning("Failed to fetch market metrics: %s", e)
                snapshot["market_metrics"] = {}
        else:
            snapshot["market_metrics"] = {}

        # Add system-level balances for the capital/risk section
        try:
            snapshot["system_balances"] = adapter.get_system_balances()
        except Exception as e:
            logger.warning("Failed to fetch system balances: %s", e)

        snapshot["data_source"] = "tastytrade"
        return snapshot

    # ------------------------------------------------------------------
    # Greeks collection with streamer status awareness
    # ------------------------------------------------------------------

    def _collect_greeks_summaries(
        self, adapter, greeks_engine: GreeksEngine | None,
    ) -> list[dict]:
        """Collect greeks summaries from the live engine.

        When the streamer is connected, vega and gamma are always visible —
        no YELLOW for missing data unless the streamer is actually down or
        data is genuinely stale.
        """
        summaries: list[dict] = []

        if greeks_engine is not None:
            for pid in adapter.get_accounts():
                try:
                    summary = greeks_engine.summary(pid)
                    # Tag with streamer status so the formatter can display it
                    summary["streamer_connected"] = True
                    summaries.append(summary)
                except Exception as e:
                    logger.warning("Greeks summary failed for %s: %s", pid, e)
                    s = _zero_greeks_summary(pid)
                    s["streamer_connected"] = False
                    summaries.append(s)
        else:
            # No streamer — zero greeks, flag as disconnected
            for pid in self.pm.get_portfolio_ids():
                s = _zero_greeks_summary(pid)
                s["streamer_connected"] = False
                summaries.append(s)

        return summaries

    # ------------------------------------------------------------------
    # Live position formatting
    # ------------------------------------------------------------------

    def _format_live_positions(self) -> str:
        """Format positions from live adapter data (no CSV parsing)."""
        lines = []
        for pid in self.pm.get_portfolio_ids():
            for pos in self.pm.get_positions(pid):
                lines.append(
                    f"{pid} | {pos.get('underlying_symbol', '')} "
                    f"{pos.get('option_type', pos.get('instrument_type', ''))} "
                    f"{pos.get('strike_price', '')} "
                    f"exp:{pos.get('expiration_date', '')} "
                    f"qty:{pos.get('quantity', '')} "
                    f"@ ${pos.get('average_open_price', '')} "
                    f"[{pos.get('role', '')}] "
                    f"DTE:{pos.get('dte', '?')}"
                )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Live Greeks via tastytrade
    # ------------------------------------------------------------------

    def _build_live_greeks_engine(self) -> tuple[object | None, GreeksEngine | None]:
        """
        Create a TastytradeAdapter, collect live Greeks for all option
        positions for _STREAM_COLLECT_SECONDS, then return the adapter
        and a GreeksEngine backed by the populated streamer snapshot.

        Returns (adapter, greeks_engine) or (None, None) on failure.
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
                return adapter, None

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

            return adapter, GreeksEngine(adapter, streamer)

        except Exception as e:
            logger.error("Could not build live Greeks engine: %s", e)
            return None, None

    # ------------------------------------------------------------------
    # CSV fallback (yfinance / offline mode)
    # ------------------------------------------------------------------

    def _run_csv_fallback(self, market_snapshot: dict, vix: float) -> str:
        """Run rules evaluation using CSV positions (non-tastytrade mode)."""
        from options_cio.core.portfolio_manager import CsvPortfolioManager

        csv_pm = CsvPortfolioManager(self.positions_path)
        capital_map = {
            pid: cfg["capital"]
            for pid, cfg in self.portfolios_config.get("portfolios", {}).items()
        }
        portfolio_states = csv_pm.get_all_portfolio_states({}, capital_map, vix)

        greeks_summaries: list[dict] = []
        all_alerts: list[str] = []

        # Create a minimal RulesEngine for CSV mode
        from options_cio.core.rules_engine import RulesEngine as _RE
        rules = _RE.__new__(_RE)
        with open(self._rules_path) as f:
            data = json.load(f)
        rules.rules = data.get("rules", [])
        rules.system_rules = data.get("system_rules", [])
        rules.portfolios_config = self.portfolios_config.get("portfolios", self.portfolios_config)
        rules.adapter = None
        rules.greeks_engine = None
        rules._system_state = "GREEN"
        rules._violation_log = []

        for pid in csv_pm.get_portfolio_ids():
            positions = csv_pm.get_positions_for_portfolio(pid)
            summary = _zero_greeks_summary(pid)
            greeks_summaries.append(summary)

            alerts = rules.evaluate_portfolio(
                portfolio_id=pid,
                positions=positions,
                greeks_summary=summary,
                capital=capital_map.get(pid, 125000),
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

        system_alerts = rules.evaluate_system(portfolio_states)
        for alert in system_alerts:
            all_alerts.append(str(alert))

        all_positions = csv_pm.get_all_positions()
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
        streamer_connected: bool = False,
    ) -> str:
        separator = "=" * 60
        source_tag = "LIVE" if market_snapshot.get("data_source") == "tastytrade" else "DELAYED"
        lines = [
            separator,
            f"  OPTIONS CIO — DAILY REVIEW  {date.today()}  [{source_tag}]",
            separator,
            "",
            f"  SPX: {market_snapshot.get('spx', 'N/A')}  |  VIX: {market_snapshot.get('vix', 'N/A')}",
            f"  BTC: {market_snapshot.get('btc', 'N/A')}  |  10Y: {market_snapshot.get('yield_10y', 'N/A')}",
        ]

        # IV metrics from tastytrade (Section 0 enrichment)
        metrics = market_snapshot.get("market_metrics", {})
        if metrics:
            lines.append("")
            lines.append("── IV Context (tastytrade) ─────────────────────────────")
            for sym, m in sorted(metrics.items()):
                iv_rank = m.get("tw_iv_rank")
                iv_pct = m.get("iv_percentile")
                iv_30 = m.get("iv_30_day")
                parts = [f"  {sym:6s}"]
                if iv_rank is not None:
                    parts.append(f"IVR={iv_rank:.0f}")
                if iv_pct is not None:
                    parts.append(f"IV%={iv_pct:.0f}")
                if iv_30 is not None:
                    parts.append(f"IV30={iv_30:.1%}")
                lines.append("  ".join(parts))

        # System balances (Section 1)
        sys_bal = market_snapshot.get("system_balances", {})
        if sys_bal:
            lines.append("")
            lines.append("── Capital & Risk (live OBP) ────────────────────────────")
            lines.append(
                f"  System Net Liq: ${sys_bal.get('system_net_liquidating_value', 0):,.0f}"
                f"  |  System OBP: ${sys_bal.get('system_option_buying_power', 0):,.0f}"
                f"  |  Deployed: {sys_bal.get('system_deployment_pct', 0):.1f}%"
            )
            for pid, bal in sorted(sys_bal.get("portfolios", {}).items()):
                lines.append(
                    f"    {pid}: Net Liq ${bal.get('net_liquidating_value', 0):,.0f}"
                    f"  OBP ${bal.get('option_buying_power', 0):,.0f}"
                    f"  Deploy {bal.get('deployment_pct', 0):.1f}%"
                )

        lines.append("")

        # Greeks (Section 1.3) — streamer status determines display
        streamer_label = "LIVE via DXLink" if streamer_connected else "OFFLINE"
        lines.append(f"── Greeks Summary ({streamer_label}) ─────────────────────")
        for g in greeks_summaries:
            connected = g.get("streamer_connected", False)
            pending = g.get("pending_count", 0)
            stale = g.get("stale_count", 0)
            flags = ""
            if not connected:
                flags = " [STREAMER DOWN]"
            elif stale:
                flags = f" [{stale} STALE]"
            elif pending:
                flags = f" [{pending} PENDING]"
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

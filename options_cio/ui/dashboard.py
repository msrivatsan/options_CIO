"""
Textual TUI Dashboard for Options CIO.
Refreshes market data and Greeks on a configurable interval.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, ScrollableContainer
from textual.widgets import (
    Button, DataTable, Footer, Header, Label, Log, Static, TabbedContent, TabPane
)
from textual.timer import Timer

from options_cio.core.greeks_engine import GreeksEngine
from options_cio.core.portfolio_manager import PortfolioManager
from options_cio.core.rules_engine import RulesEngine, INCOME_ROLES, HEDGE_ROLES
from options_cio.core.state_cache import StateCache
from options_cio.ai.cio_brain import CIOBrain
from options_cio.data.feed_adapter import YFinanceFeed
from options_cio.journal.trade_journal import TradeJournal
from options_cio.simulator.what_if import WhatIfSimulator, SCENARIOS
from options_cio.ui.widgets import AlertsPanel, AIReviewPanel, PortfolioStatusBar

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent

_ZERO_SUMMARY = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0,
                 "position_count": 0, "pending_count": 0, "stale_count": 0}


class _NullAdapter:
    """Stub adapter for CSV-fallback mode — RulesEngine requires an adapter."""
    def get_accounts(self) -> dict:
        return {}
    def get_positions(self, portfolio_id: str) -> list[dict]:
        return []
    def get_balances(self, portfolio_id: str) -> dict:
        return {"net_liquidating_value": 0, "option_buying_power": 0,
                "deployment_pct": 0, "committed_obp": 0}
    def get_market_metrics(self, symbols: list[str]) -> dict:
        return {}


class OptionsCIODashboard(App):
    """Main Textual TUI application."""

    CSS = """
    Screen {
        background: $surface;
    }
    .portfolio-row {
        height: 7;
        margin: 0 1;
    }
    .section-title {
        background: $primary;
        color: $text;
        padding: 0 2;
        text-style: bold;
    }
    .alert-panel {
        height: 12;
        margin: 0 1;
        overflow-y: auto;
    }
    .ai-panel {
        margin: 0 1;
        height: 1fr;
        overflow-y: auto;
    }
    .status-bar {
        dock: bottom;
        height: 1;
        background: $primary-darken-2;
        color: $text-muted;
    }
    DataTable {
        height: 12;
        margin: 0 1;
    }
    Button {
        margin: 0 1;
    }
    """

    BINDINGS = [
        Binding("r", "refresh", "Refresh"),
        Binding("a", "run_ai", "AI Review"),
        Binding("s", "run_scenario", "Scenario"),
        Binding("q", "quit", "Quit"),
        Binding("1", "show_tab('overview')", "Overview"),
        Binding("2", "show_tab('positions')", "Positions"),
        Binding("3", "show_tab('journal')", "Journal"),
    ]

    TITLE = "Options CIO"
    SUB_TITLE = "Systematic Options Portfolio Manager"

    def __init__(self, settings: dict, api_key: Optional[str] = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.settings = settings
        self._data_source = settings.get("data_source", "yfinance")
        config_dir = BASE_DIR / "config"

        with open(config_dir / "portfolios.json") as f:
            self.portfolios_config = json.load(f)

        self.capital_map = {
            pid: cfg["capital"]
            for pid, cfg in self.portfolios_config["portfolios"].items()
        }

        positions_path = BASE_DIR / "active_positions.csv"
        self.pm = PortfolioManager(positions_path, settings.get("db_path", "./options_cio.db")).load()
        self._rules_path = config_dir / "trading_rules.json"
        self.rules = None  # initialized after adapter is ready
        self.feed = YFinanceFeed()  # always used for market snapshot
        self.brain = CIOBrain(
            model=settings.get("api_model", "claude-sonnet-4-20250514"),
            ai_offline=settings.get("ai_offline", False),
            max_daily_cost=settings.get("max_api_cost_per_day", 5.00),
            api_key=api_key,
        )
        self.journal = TradeJournal(settings.get("db_path", "./options_cio.db"))
        self.simulator = WhatIfSimulator()
        self.cache = StateCache(settings.get("db_path", "./options_cio.db"))

        # Live data adapter + streamer (tastytrade mode only)
        self._adapter = None
        self._streamer = None
        self._greeks_engine: Optional[GreeksEngine] = None

        if self._data_source == "tastytrade":
            try:
                from options_cio.data.tastytrade_adapter import TastytradeAdapter
                self._adapter = TastytradeAdapter()
            except Exception as e:
                logger.error("Could not create TastytradeAdapter: %s", e)
                self.notify(f"Tastytrade init failed: {e}", severity="error")

        # State
        self._price_map: dict[str, float] = {}
        self._greeks_summaries: list[dict] = []
        self._portfolio_states: list[dict] = []
        self._alerts: list[str] = []
        self._ai_review: str = "Press [A] to run AI review."
        self._market_snapshot: dict = {}
        self._refresh_timer: Optional[Timer] = None

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent(initial="overview"):
            with TabPane("Overview [1]", id="overview"):
                yield Static("Portfolio Deployment", classes="section-title")
                with Horizontal(classes="portfolio-row"):
                    for pid in ["P1", "P2", "P3", "P4"]:
                        yield Static(f"Loading {pid}...", id=f"status-{pid}")
                yield Static("Greeks", classes="section-title")
                yield DataTable(id="greeks-table")
                yield Static("Active Alerts", classes="section-title")
                yield ScrollableContainer(
                    Static("Loading alerts...", id="alerts-display"),
                    classes="alert-panel",
                )
                yield Static("CIO Analysis", classes="section-title")
                yield ScrollableContainer(
                    Static(self._ai_review, id="ai-review"),
                    classes="ai-panel",
                )
                with Horizontal():
                    yield Button("Refresh [R]", id="btn-refresh", variant="primary")
                    yield Button("AI Review [A]", id="btn-ai", variant="success")
                    yield Button("Scenario [S]", id="btn-scenario", variant="warning")

            with TabPane("Positions [2]", id="positions"):
                yield DataTable(id="positions-table")

            with TabPane("Journal [3]", id="journal"):
                yield DataTable(id="journal-table")

        yield Footer()

    def on_mount(self) -> None:
        self._setup_tables()
        if self._adapter is not None:
            # Start the persistent streamer before the first refresh
            self.run_worker(self._start_streamer(), exclusive=False, name="streamer")
        else:
            self.run_worker(self._refresh_data(), exclusive=True, name="initial_refresh")
        interval = self.settings.get("refresh_interval_seconds", 60)
        self._refresh_timer = self.set_interval(interval, self._auto_refresh)

    # ------------------------------------------------------------------
    # Table setup
    # ------------------------------------------------------------------

    def _setup_tables(self) -> None:
        greeks_table = self.query_one("#greeks-table", DataTable)
        greeks_table.add_columns("Portfolio", "Delta", "Gamma", "Theta", "Vega", "Positions", "Pending")

        pos_table = self.query_one("#positions-table", DataTable)
        pos_table.add_columns(
            "Portfolio", "Ticker", "Type", "Strike", "Expiry", "Qty",
            "Entry $", "Tag", "DTE"
        )

        journal_table = self.query_one("#journal-table", DataTable)
        journal_table.add_columns("Time", "Portfolio", "Rule", "Severity", "Message")

    # ------------------------------------------------------------------
    # Streamer lifecycle
    # ------------------------------------------------------------------

    async def _start_streamer(self) -> None:
        """
        Collect all option symbols, open the DXLink streamer, and keep it
        running in the background. Triggers the initial data refresh once
        the streamer is subscribed.
        """
        from options_cio.data.streamer import TastytradeStreamer

        try:
            symbols = await asyncio.get_event_loop().run_in_executor(
                None, self._collect_option_symbols
            )

            if not symbols:
                self.notify("No option positions — Greeks unavailable", severity="warning")
                await self._refresh_data()
                return

            self.notify(f"Streaming Greeks for {len(symbols)} symbols...", severity="information")

            async with TastytradeStreamer(self._adapter.session, symbols) as streamer:
                self._streamer = streamer
                self._greeks_engine = GreeksEngine(self._adapter, streamer)

                # Subscribe then do initial refresh after a short warm-up
                await streamer.subscribe()
                await asyncio.sleep(2)
                await self._refresh_data()

                # Keep listening indefinitely (cancelled when app exits)
                await streamer.run_continuous()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.notify(f"Streamer error: {e}", severity="error")
            logger.error("Streamer error: %s", e, exc_info=True)
            # Fall back to a refresh without live Greeks
            await self._refresh_data()

    def _collect_option_symbols(self) -> list[str]:
        """Fetch all option symbols across accounts (sync, for executor)."""
        symbols: list[str] = []
        accounts = self._adapter.get_accounts()
        for pid in accounts:
            positions = self._adapter.get_positions(pid)
            for pos in positions:
                if pos.get("instrument_type") in ("Equity Option", "Future Option"):
                    sym = pos["symbol"]
                    if sym not in symbols:
                        symbols.append(sym)
        return symbols

    # ------------------------------------------------------------------
    # Data refresh
    # ------------------------------------------------------------------

    async def _refresh_data(self) -> None:
        self.notify("Refreshing market data...", severity="information")
        try:
            self._market_snapshot = await asyncio.get_event_loop().run_in_executor(
                None, self.feed.get_market_snapshot
            )
            vix = float(self._market_snapshot.get("vix") or 20.0)

            self._greeks_summaries = []
            self._alerts = []

            if self._adapter is not None:
                # Live tastytrade evaluation via evaluate_all()
                rules = RulesEngine(
                    self._rules_path, self.portfolios_config,
                    self._adapter, self._greeks_engine,
                )
                self.rules = rules

                result = await asyncio.get_event_loop().run_in_executor(
                    None, rules.evaluate_all
                )
                self._alerts.extend(str(a) for a in result.alerts)

                # Build portfolio states from live balances
                self._portfolio_states = []
                accounts = self._adapter.get_accounts()
                for pid in accounts:
                    balances = await asyncio.get_event_loop().run_in_executor(
                        None, lambda p=pid: self._adapter.get_balances(p)
                    )
                    positions = await asyncio.get_event_loop().run_in_executor(
                        None, lambda p=pid: self._adapter.get_positions(p)
                    )
                    cfg = self.portfolios_config["portfolios"].get(pid, {})
                    income_count = sum(1 for p in positions if p.get("role") in INCOME_ROLES)
                    hedge_count = sum(1 for p in positions if p.get("role") in HEDGE_ROLES)

                    self._portfolio_states.append({
                        "portfolio_id": pid,
                        "deployment_pct": balances["deployment_pct"] / 100.0,
                        "deployed_capital": balances["committed_obp"],
                        "capital": balances["net_liquidating_value"],
                        "has_income_positions": income_count > 0,
                        "has_hedge": hedge_count > 0,
                        "position_count": len(positions),
                        "vix": vix,
                    })

                    if self._greeks_engine is not None:
                        try:
                            summary = await asyncio.get_event_loop().run_in_executor(
                                None, lambda p=pid: self._greeks_engine.summary(p)
                            )
                        except Exception as e:
                            logger.warning("Greeks failed for %s: %s", pid, e)
                            summary = {**_ZERO_SUMMARY, "portfolio": pid}
                    else:
                        summary = {**_ZERO_SUMMARY, "portfolio": pid}
                    self._greeks_summaries.append(summary)
            else:
                # CSV fallback (yfinance / offline mode)
                self._portfolio_states = self.pm.get_all_portfolio_states(
                    {}, self.capital_map, vix
                )
                # Lazy-init rules for CSV mode
                if self.rules is None:
                    self.rules = RulesEngine(
                        self._rules_path, self.portfolios_config,
                        adapter=_NullAdapter(), greeks_engine=None,
                    )

                for pid in self.pm.get_portfolio_ids():
                    positions = self.pm.get_positions_for_portfolio(pid)
                    summary = {**_ZERO_SUMMARY, "portfolio": pid}
                    self._greeks_summaries.append(summary)

                    alerts = self.rules.evaluate_portfolio(
                        portfolio_id=pid, positions=positions,
                        greeks_summary=summary,
                        capital=self.capital_map.get(pid, 125000),
                    )
                    self._alerts.extend(str(a) for a in alerts)

                system_alerts = self.rules.evaluate_system(self._portfolio_states)
                self._alerts.extend(str(a) for a in system_alerts)

            self._update_ui()
            self.notify("Refresh complete.", severity="information")
        except Exception as e:
            self.notify(f"Refresh error: {e}", severity="error")

    def _update_ui(self) -> None:
        # Portfolio status widgets
        for state in self._portfolio_states:
            pid = state["portfolio_id"]
            cfg = self.portfolios_config["portfolios"].get(pid, {})
            dep = state.get("deployment_pct", 0)
            band = cfg.get("deployment_band", [0, 1])
            target = cfg.get("target_zone", band)
            income = state.get("has_income_positions", False)
            hedge = state.get("has_hedge", False)

            if dep < band[0]:
                status_color = "yellow"; status = "UNDER"
            elif dep > band[1]:
                status_color = "red"; status = "OVER"
            elif target[0] <= dep <= target[1]:
                status_color = "green"; status = "TARGET"
            else:
                status_color = "cyan"; status = "IN BAND"

            text = (
                f"[bold]{pid}[/bold] — {cfg.get('name', '')}\n"
                f"[{status_color}]{dep:.1%}[/{status_color}] [{status}] | "
                f"${state.get('deployed_capital', 0):,.0f} / ${state.get('capital', 0):,.0f}\n"
                f"Income: {income} | Hedge: {hedge} | Positions: {state.get('position_count', 0)}"
            )
            try:
                self.query_one(f"#status-{pid}", Static).update(text)
            except Exception:
                pass

        # Greeks table
        greeks_table = self.query_one("#greeks-table", DataTable)
        greeks_table.clear()
        for g in self._greeks_summaries:
            pending = g.get("pending_count", 0)
            greeks_table.add_row(
                g.get("portfolio", ""),
                f"{g.get('delta', 0):+.1f}",
                f"{g.get('gamma', 0):+.4f}",
                f"{g.get('theta', 0):+.1f}",
                f"{g.get('vega', 0):+.1f}",
                str(g.get("position_count", 0)),
                str(pending) if pending else "-",
            )

        # Alerts
        alerts_text = (
            "\n".join(
                f"[{'red' if 'CRITICAL' in a else 'yellow' if 'WARN' in a else 'cyan'}]{a}[/]"
                for a in self._alerts
            )
            or "[green]No active alerts[/green]"
        )
        self.query_one("#alerts-display", Static).update(alerts_text)

        # Positions table
        pos_table = self.query_one("#positions-table", DataTable)
        pos_table.clear()
        for p in self.pm.get_all_positions():
            pos_table.add_row(
                p.get("portfolio", ""), p.get("ticker", ""),
                p.get("option_type", ""), str(p.get("strike", "")),
                str(p.get("expiry", "")), str(p.get("qty", "")),
                f"${p.get('entry_price', 0):.2f}",
                p.get("structure_tag", ""), str(p.get("dte", "?")),
            )

        # Journal alerts
        journal_table = self.query_one("#journal-table", DataTable)
        journal_table.clear()
        for row in self.journal.get_recent_alerts(limit=30):
            sev_color = "red" if row["severity"] == "CRITICAL" else "yellow" if row["severity"] == "WARN" else "cyan"
            journal_table.add_row(
                row["ts"][:16], row.get("portfolio", ""),
                row["rule_id"],
                f"[{sev_color}]{row['severity']}[/{sev_color}]",
                row.get("message", "")[:60],
            )

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def action_refresh(self) -> None:
        self.run_worker(self._refresh_data(), exclusive=True, name="manual_refresh")

    def action_run_ai(self) -> None:
        self.run_worker(self._run_ai_review(), exclusive=False, name="ai_review")

    def action_run_scenario(self) -> None:
        self.run_worker(self._run_scenario_analysis(), exclusive=False, name="scenario")

    def action_show_tab(self, tab_id: str) -> None:
        self.query_one(TabbedContent).active = tab_id

    async def _auto_refresh(self) -> None:
        await self._refresh_data()

    async def _run_ai_review(self) -> None:
        self.notify("Running AI review...", severity="information")
        try:
            all_positions = self.pm.get_all_positions()
            positions_summary = "\n".join(
                f"{p['portfolio']} {p['ticker']} {p['option_type']} {p['strike']} "
                f"exp:{p['expiry']} qty:{p['qty']} [{p['structure_tag']}]"
                for p in all_positions
            )
            review = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.brain.daily_review(
                    portfolio_states=self._portfolio_states,
                    greeks_summaries=self._greeks_summaries,
                    rule_alerts=self._alerts,
                    market_snapshot=self._market_snapshot,
                    positions_summary=positions_summary,
                ),
            )
            self.query_one("#ai-review", Static).update(review)
            self.journal.log_ai_review(
                review_type="daily",
                ai_output=review,
                cost_usd=self.brain.get_daily_cost(),
            )
            self.notify("AI review complete.", severity="information")
        except Exception as e:
            self.notify(f"AI review error: {e}", severity="error")

    async def _run_scenario_analysis(self) -> None:
        scenario_key = "crash_20"
        self.notify(f"Running scenario: {scenario_key}...", severity="information")
        try:
            greeks_map = {g["portfolio"]: g for g in self._greeks_summaries}
            results = self.simulator.run_scenario(
                scenario_key=scenario_key,
                portfolio_greeks_map=greeks_map,
                capital_map=self.capital_map,
            )
            output = f"=== Scenario: {SCENARIOS[scenario_key]['name']} ===\n\n"
            for r in results:
                output += str(r) + "\n"
                for note in r.notes:
                    output += f"  !! {note}\n"
            self.query_one("#ai-review", Static).update(output)
            self.notify("Scenario complete.", severity="information")
        except Exception as e:
            self.notify(f"Scenario error: {e}", severity="error")

    # ------------------------------------------------------------------
    # Button handlers
    # ------------------------------------------------------------------

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-refresh":
            self.action_refresh()
        elif event.button.id == "btn-ai":
            self.action_run_ai()
        elif event.button.id == "btn-scenario":
            self.action_run_scenario()

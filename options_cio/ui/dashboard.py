"""
Textual TUI Dashboard for Options CIO.
Refreshes market data and Greeks on a configurable interval.
"""

from __future__ import annotations

import asyncio
import json
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
from options_cio.core.rules_engine import RulesEngine
from options_cio.core.state_cache import StateCache
from options_cio.ai.cio_brain import CIOBrain
from options_cio.data.feed_adapter import YFinanceFeed
from options_cio.journal.trade_journal import TradeJournal
from options_cio.simulator.what_if import WhatIfSimulator, SCENARIOS
from options_cio.ui.widgets import AlertsPanel, AIReviewPanel, PortfolioStatusBar


BASE_DIR = Path(__file__).parent.parent


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
        config_dir = BASE_DIR / "config"

        with open(config_dir / "portfolios.json") as f:
            self.portfolios_config = json.load(f)

        self.capital_map = {
            pid: cfg["capital"]
            for pid, cfg in self.portfolios_config["portfolios"].items()
        }

        positions_path = BASE_DIR / "active_positions.csv"
        self.pm = PortfolioManager(positions_path, settings.get("db_path", "./options_cio.db")).load()
        self.rules = RulesEngine(config_dir / "trading_rules.json")
        self.greeks_engine = GreeksEngine()
        self.feed = YFinanceFeed(cache_ttl_seconds=settings.get("refresh_interval_seconds", 60))
        self.brain = CIOBrain(
            model=settings.get("api_model", "claude-sonnet-4-20250514"),
            ai_offline=settings.get("ai_offline", False),
            max_daily_cost=settings.get("max_api_cost_per_day", 5.00),
            api_key=api_key,
        )
        self.journal = TradeJournal(settings.get("db_path", "./options_cio.db"))
        self.simulator = WhatIfSimulator()
        self.cache = StateCache()

        # State
        self._price_map: dict[str, float] = {}
        self._iv_map: dict[str, float] = {}
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
        self.run_worker(self._refresh_data(), exclusive=True, name="initial_refresh")
        interval = self.settings.get("refresh_interval_seconds", 60)
        self._refresh_timer = self.set_interval(interval, self._auto_refresh)

    # ------------------------------------------------------------------
    # Table setup
    # ------------------------------------------------------------------

    def _setup_tables(self) -> None:
        greeks_table = self.query_one("#greeks-table", DataTable)
        greeks_table.add_columns("Portfolio", "Delta", "Gamma", "Theta", "Vega", "Positions")

        pos_table = self.query_one("#positions-table", DataTable)
        pos_table.add_columns(
            "Portfolio", "Ticker", "Type", "Strike", "Expiry", "Qty",
            "Entry $", "Tag", "DTE"
        )

        journal_table = self.query_one("#journal-table", DataTable)
        journal_table.add_columns("Time", "Portfolio", "Rule", "Severity", "Message")

    # ------------------------------------------------------------------
    # Data refresh
    # ------------------------------------------------------------------

    async def _refresh_data(self) -> None:
        self.notify("Refreshing market data...", severity="information")
        try:
            all_positions = self.pm.get_all_positions()
            tickers = list(set(p["ticker"] for p in all_positions))

            self._price_map = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.feed.get_prices(tickers)
            )
            self._iv_map = {t: self.feed.get_historical_volatility(t) or 0.30 for t in tickers}
            self._market_snapshot = await asyncio.get_event_loop().run_in_executor(
                None, self.feed.get_market_snapshot
            )
            vix = float(self._market_snapshot.get("vix") or 20.0)

            self._portfolio_states = self.pm.get_all_portfolio_states(
                self._price_map, self.capital_map, vix
            )
            self._greeks_summaries = []
            self._alerts = []

            for pid in self.pm.get_portfolio_ids():
                positions = self.pm.get_positions_for_portfolio(pid)
                pg = self.greeks_engine.compute_portfolio_greeks(
                    pid, positions, self._price_map, self._iv_map
                )
                summary = pg.summary()
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
            greeks_table.add_row(
                g.get("portfolio", ""),
                f"{g.get('delta', 0):+.1f}",
                f"{g.get('gamma', 0):+.4f}",
                f"{g.get('theta', 0):+.1f}",
                f"{g.get('vega', 0):+.1f}",
                str(g.get("position_count", 0)),
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
        scenario_key = "crash_20"  # default; could be made interactive
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

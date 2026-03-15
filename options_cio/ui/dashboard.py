"""
Textual TUI Dashboard for Options CIO.

Live streaming architecture:
  - Greeks and quotes: CONTINUOUS via DXLink websocket (updated every 2s from streamer snapshot)
  - Positions and balances: REST polling every 60s
  - Rules evaluation: Runs on every position/balance refresh
  - AI calls: Event-driven (violation, user query, or 5-min summary)

Connection status indicator:
  - GREEN LIVE: streamer connected, data fresh (< 30s)
  - YELLOW DELAYED: streamer reconnecting or data > 30s old
  - RED OFFLINE: disconnected — using cached data
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, ScrollableContainer
from textual.widgets import (
    Button, DataTable, Footer, Header, Static, TabbedContent, TabPane
)
from textual.timer import Timer

from options_cio.core.greeks_engine import GreeksEngine
from options_cio.core.portfolio_manager import PortfolioManager
from options_cio.core.rules_engine import RulesEngine
from options_cio.core.state_cache import StateCache
from options_cio.ai.cio_brain import CIOBrain
from options_cio.journal.trade_journal import TradeJournal
from options_cio.simulator.what_if import WhatIfSimulator, SCENARIOS
from options_cio.ui.widgets import AlertsPanel, AIReviewPanel, PortfolioStatusBar  # noqa: F401

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent

_ZERO_SUMMARY = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0,
                 "position_count": 0, "pending_count": 0, "stale_count": 0}

# Data older than this from the streamer is considered stale
_STALE_THRESHOLD_SECONDS = 30

# How often to push streamer data to the UI (seconds)
_STREAM_UI_INTERVAL = 2

# How often to poll REST for positions/balances (seconds)
_REST_POLL_INTERVAL = 60


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
    """Main Textual TUI application with live streaming data."""

    CSS = """
    Screen {
        background: $surface;
    }
    #connection-status {
        dock: top;
        height: 1;
        text-align: right;
        padding: 0 2;
        background: $surface-darken-1;
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
    #connection-diag {
        height: auto;
        max-height: 20;
        margin: 0 1;
        overflow-y: auto;
        display: none;
    }
    #log-panel {
        height: 12;
        margin: 0 1;
        overflow-y: auto;
        display: none;
        background: $surface-darken-2;
    }
    #error-status {
        dock: bottom;
        height: 1;
        background: $error-darken-1;
        color: $text;
        padding: 0 2;
        display: none;
    }
    DataTable {
        height: 12;
        margin: 0 1;
    }
    #positions-table {
        height: 20;
    }
    Button {
        margin: 0 1;
    }
    """

    BINDINGS = [
        Binding("r", "refresh", "Refresh"),
        Binding("a", "run_ai", "AI Review"),
        Binding("s", "run_scenario", "Scenario"),
        Binding("c", "connection_diag", "Connection"),
        Binding("l", "toggle_log", "Log"),
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

        self._positions_path = BASE_DIR / "active_positions.csv"
        self._rules_path = config_dir / "trading_rules.json"
        self.pm = None  # initialized after adapter or in fallback mode
        self.rules = None  # initialized after adapter is ready
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
        self._init_error: str | None = None

        # Connection tracking
        self._connection_state = "OFFLINE"  # LIVE, DELAYED, OFFLINE
        self._account_status: dict[str, str] = {}  # pid -> CONNECTED/STALE/DISCONNECTED
        self._streamer_connected = False
        self._last_stream_ts: float = 0.0  # epoch of last streamer data received

        # Position-symbol mapping for live Greeks display
        self._position_symbols: dict[str, list[dict]] = {}  # pid -> list of position dicts

        # State
        self._greeks_summaries: list[dict] = []
        self._portfolio_states: list[dict] = []
        self._alerts: list[str] = []
        self._ai_review: str = "Press [A] to run AI review."
        self._market_snapshot: dict = {}
        self._rest_timer: Optional[Timer] = None
        self._stream_ui_timer: Optional[Timer] = None
        self._integrity_timer: Optional[Timer] = None
        self._config_watch_timer: Optional[Timer] = None
        self._diag_visible = False
        self._log_visible = False

        # Config hot-reload tracking
        self._config_mtimes: dict[str, float] = {}
        self._track_config_mtimes()

        # Session metrics (for graceful shutdown summary)
        self._session_start = time.time()
        self._api_call_count = 0
        self._total_api_cost = 0.0
        self._violations_encountered = 0

        # Previous balance cache for sanity check
        self._prev_net_liq: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="connection-status")
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

        yield ScrollableContainer(
            Static("", id="diag-content"),
            id="connection-diag",
        )
        yield ScrollableContainer(
            Static("", id="log-content"),
            id="log-panel",
        )
        yield Static("", id="error-status")
        yield Footer()

    # ------------------------------------------------------------------
    # Startup sequence
    # ------------------------------------------------------------------

    def on_mount(self) -> None:
        self._setup_tables()
        self._update_connection_indicator()

        if self._data_source == "tastytrade":
            self.run_worker(self._startup_live(), exclusive=False, name="startup")
        else:
            self.run_worker(self._startup_csv(), exclusive=True, name="startup_csv")

    async def _startup_live(self) -> None:
        """Tastytrade startup sequence:
        1. Authenticate
        2. Discover accounts and map to portfolios
        3. Fetch initial positions and balances
        4. Open DXLink streamer and subscribe
        5. Calculate initial Greeks
        6. Run initial rules check
        7. Display — all panels populate within 5-10 seconds
        """
        # Step 1: Authenticate
        self._update_status_text("Authenticating with tastytrade...")
        try:
            from options_cio.data.tastytrade_adapter import TastytradeAdapter
            self._adapter = TastytradeAdapter()
        except EnvironmentError as e:
            self._init_error = (
                "CANNOT CONNECT TO TASTYTRADE\n\n"
                f"{e}\n\n"
                "Troubleshooting:\n"
                "  1. Set TASTYTRADE_CLIENT_SECRET in your environment\n"
                "  2. Set TASTYTRADE_REFRESH_TOKEN in your environment\n"
                "  3. Check that your OAuth credentials are still valid\n"
                "  4. Verify network connectivity to api.tastytrade.com"
            )
            self._connection_state = "OFFLINE"
            self._update_connection_indicator()
            self.query_one("#ai-review", Static).update(
                f"[bold red]{self._init_error}[/bold red]"
            )
            return
        except Exception as e:
            self._init_error = f"CANNOT CONNECT TO TASTYTRADE\n\n{e}"
            self._connection_state = "OFFLINE"
            self._update_connection_indicator()
            self.query_one("#ai-review", Static).update(
                f"[bold red]{self._init_error}[/bold red]"
            )
            return

        # Step 2: Discover accounts
        self._update_status_text("Discovering accounts...")
        self.brain._adapter = self._adapter
        self.simulator._adapter = self._adapter
        self.pm = PortfolioManager(
            self.portfolios_config, self._adapter, self.cache,
        )

        # Check per-account connectivity
        try:
            connectivity = await asyncio.get_event_loop().run_in_executor(
                None, self.pm.check_account_connectivity,
            )
            for pid, info in connectivity.get("accounts", {}).items():
                self._account_status[pid] = info if isinstance(info, str) else info.get("status", "UNKNOWN")
        except Exception as e:
            logger.warning("Account connectivity check failed: %s", e)

        # Step 3: Fetch initial positions and balances
        self._update_status_text("Fetching positions and balances...")
        await self._refresh_rest_data()

        # Step 4: Open streamer
        self._update_status_text("Opening DXLink streamer...")
        self.run_worker(self._start_streamer(), exclusive=False, name="streamer")

        # Step 5-6: Initial Greeks and rules happen after streamer warm-up (in _start_streamer)

        # Start REST polling timer (positions/balances every 60s)
        self._rest_timer = self.set_interval(
            _REST_POLL_INTERVAL, self._auto_rest_refresh
        )

        # Config hot-reload (poll every 30s)
        self._config_watch_timer = self.set_interval(30, self._check_config_changes)

        # Data integrity checks (every 5 minutes)
        self._integrity_timer = self.set_interval(300, self._run_integrity_checks)

    async def _startup_csv(self) -> None:
        """Non-tastytrade startup — CSV + yfinance fallback."""
        self._connection_state = "OFFLINE"
        self._update_connection_indicator()
        await self._refresh_csv_data()

        interval = self.settings.get("refresh_interval_seconds", 60)
        self._rest_timer = self.set_interval(interval, self._auto_rest_refresh)

    # ------------------------------------------------------------------
    # Table setup
    # ------------------------------------------------------------------

    def _setup_tables(self) -> None:
        greeks_table = self.query_one("#greeks-table", DataTable)
        greeks_table.add_columns(
            "Portfolio", "Delta", "Gamma", "Theta", "Vega", "Positions", "Status"
        )

        pos_table = self.query_one("#positions-table", DataTable)
        pos_table.add_columns(
            "Portfolio", "Ticker", "Type", "Strike", "Expiry", "Qty",
            "Entry $", "Mark", "Delta", "Theta", "Vega", "DTE",
        )

        journal_table = self.query_one("#journal-table", DataTable)
        journal_table.add_columns("Time", "Portfolio", "Rule", "Severity", "Message")

    # ------------------------------------------------------------------
    # Connection status indicator
    # ------------------------------------------------------------------

    def _update_connection_indicator(self) -> None:
        """Update the connection status widget (top-right)."""
        if self._connection_state == "LIVE":
            text = "[bold green]\U0001f7e2 LIVE[/bold green]"
        elif self._connection_state == "DELAYED":
            text = "[bold yellow]\U0001f7e1 DELAYED[/bold yellow]"
        else:
            text = "[bold red]\U0001f534 OFFLINE[/bold red]"
        try:
            self.query_one("#connection-status", Static).update(text)
        except Exception:
            pass

    def _update_status_text(self, msg: str) -> None:
        """Show a startup progress message in the AI review area."""
        try:
            self.query_one("#ai-review", Static).update(f"[dim]{msg}[/dim]")
        except Exception:
            pass

    def _assess_connection_state(self) -> None:
        """Determine overall connection state from streamer health."""
        if not self._streamer_connected:
            if self._adapter is not None:
                self._connection_state = "DELAYED"  # REST still works
            else:
                self._connection_state = "OFFLINE"
        elif self._last_stream_ts and (time.time() - self._last_stream_ts) > _STALE_THRESHOLD_SECONDS:
            self._connection_state = "DELAYED"
        else:
            self._connection_state = "LIVE"
        self._update_connection_indicator()

    # ------------------------------------------------------------------
    # Streamer lifecycle
    # ------------------------------------------------------------------

    async def _start_streamer(self) -> None:
        """Open DXLink streamer, subscribe to all option symbols, and run
        continuously.  Pushes live Greeks/quotes to the UI via a fast timer.
        """
        from options_cio.data.streamer import TastytradeStreamer

        try:
            symbols = await asyncio.get_event_loop().run_in_executor(
                None, self._collect_option_symbols
            )

            if not symbols:
                self.notify("No option positions -- Greeks unavailable", severity="warning")
                self._streamer_connected = False
                self._assess_connection_state()
                return

            self.notify(f"Streaming Greeks for {len(symbols)} symbols...", severity="information")

            async with TastytradeStreamer(self._adapter.session, symbols) as streamer:
                self._streamer = streamer
                self.brain._streamer = streamer
                self.simulator._streamer = streamer
                self._greeks_engine = GreeksEngine(self._adapter, streamer)
                self._streamer_connected = True

                # Subscribe then warm up
                await streamer.subscribe()
                await asyncio.sleep(2)
                self._last_stream_ts = time.time()
                self._assess_connection_state()

                # Do initial Greeks + rules refresh now that streamer has data
                await self._refresh_greeks_and_rules()
                self._update_ui()
                self.notify("Dashboard ready.", severity="information")

                # Start fast timer for streaming UI updates
                self._stream_ui_timer = self.set_interval(
                    _STREAM_UI_INTERVAL, self._stream_ui_tick,
                )

                # Keep listening (cancelled when app exits)
                await streamer.run_continuous()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._streamer_connected = False
            self._assess_connection_state()
            self.notify(f"Streamer error: {e} -- switching to REST polling", severity="error")
            logger.error("Streamer error: %s", e, exc_info=True)
            # Fall back to REST-only polling
            if self._stream_ui_timer is not None:
                self._stream_ui_timer.stop()
                self._stream_ui_timer = None

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
    # Streaming UI tick — fast updates from streamer snapshot
    # ------------------------------------------------------------------

    async def _stream_ui_tick(self) -> None:
        """Called every 2s.  Reads latest Greeks/quotes from the streamer
        and pushes them to the positions table and Greeks summary.
        No REST calls — purely reads the in-memory streamer snapshot.
        """
        if not self._streamer_connected or self._greeks_engine is None:
            return

        try:
            # Check streamer health
            self._last_stream_ts = time.time()
            self._assess_connection_state()

            # Update greeks summaries from streamer
            self._greeks_summaries = []
            for pid in (self.pm.get_portfolio_ids() if self.pm else []):
                try:
                    summary = self._greeks_engine.summary(pid)
                    summary["streamer_connected"] = True
                    self._greeks_summaries.append(summary)
                except Exception:
                    self._greeks_summaries.append(
                        {**_ZERO_SUMMARY, "portfolio": pid, "streamer_connected": False}
                    )

            # Update Greeks table
            self._update_greeks_table()

            # Update positions table with live Greeks columns
            self._update_positions_table_live()

        except Exception as e:
            logger.debug("Stream UI tick error: %s", e)

    # ------------------------------------------------------------------
    # REST data refresh (positions, balances — every 60s)
    # ------------------------------------------------------------------

    async def _refresh_rest_data(self) -> None:
        """Fetch positions, balances, and market data via REST API.
        Called on startup and every 60 seconds.
        """
        try:
            # Market snapshot (yfinance for prices)
            from options_cio.data.feed_adapter import YFinanceFeed
            feed = YFinanceFeed()
            self._market_snapshot = await asyncio.get_event_loop().run_in_executor(
                None, feed.get_market_snapshot,
            )
            vix = float(self._market_snapshot.get("vix") or 20.0)

            if self._adapter is not None and self.pm is not None:
                # Enrich with tastytrade market metrics
                try:
                    underlyings = await asyncio.get_event_loop().run_in_executor(
                        None, self._collect_underlyings,
                    )
                    if underlyings:
                        metrics = await asyncio.get_event_loop().run_in_executor(
                            None, lambda: self._adapter.get_market_metrics(underlyings),
                        )
                        self._market_snapshot["market_metrics"] = metrics
                except Exception as e:
                    logger.debug("Market metrics fetch: %s", e)

                # Journal sync
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None, lambda: self.journal.detect_position_changes(
                            self._adapter, self._streamer,
                        )
                    )
                except Exception as e:
                    logger.debug("Journal position change detection: %s", e)

                # Portfolio states from live PM (balances from adapter)
                self._portfolio_states = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self.pm.get_all_portfolio_states(vix=vix),
                )

                # Cache positions for live display
                for pid in self.pm.get_portfolio_ids():
                    self._position_symbols[pid] = await asyncio.get_event_loop().run_in_executor(
                        None, lambda p=pid: self.pm.get_positions(p),
                    )

                # Check per-account status
                try:
                    connectivity = await asyncio.get_event_loop().run_in_executor(
                        None, self.pm.check_account_connectivity,
                    )
                    for pid, info in connectivity.get("accounts", {}).items():
                        self._account_status[pid] = info if isinstance(info, str) else info.get("status", "UNKNOWN")
                except Exception:
                    pass

        except Exception as e:
            logger.warning("REST data refresh error: %s", e)
            self.notify(f"REST refresh error: {e}", severity="error")

    async def _refresh_greeks_and_rules(self) -> None:
        """Run rules evaluation using current live data."""
        try:
            vix = float(self._market_snapshot.get("vix") or 20.0)

            if self._adapter is not None and self.pm is not None:
                # Rules evaluation against live data
                rules = RulesEngine(
                    self._rules_path, self.portfolios_config,
                    self._adapter, self._greeks_engine,
                )
                self.rules = rules

                result = await asyncio.get_event_loop().run_in_executor(
                    None, rules.evaluate_all,
                )
                self._alerts = [str(a) for a in result.alerts]

                # Greeks summaries from live engine
                self._greeks_summaries = []
                for pid in self.pm.get_portfolio_ids():
                    if self._greeks_engine is not None:
                        try:
                            summary = await asyncio.get_event_loop().run_in_executor(
                                None, lambda p=pid: self._greeks_engine.summary(p),
                            )
                            summary["streamer_connected"] = self._streamer_connected
                        except Exception as e:
                            logger.warning("Greeks failed for %s: %s", pid, e)
                            summary = {**_ZERO_SUMMARY, "portfolio": pid,
                                       "streamer_connected": False}
                    else:
                        summary = {**_ZERO_SUMMARY, "portfolio": pid,
                                   "streamer_connected": False}
                    self._greeks_summaries.append(summary)

        except Exception as e:
            logger.warning("Greeks/rules refresh error: %s", e)

    async def _refresh_csv_data(self) -> None:
        """CSV fallback refresh for non-tastytrade mode."""
        try:
            from options_cio.data.feed_adapter import YFinanceFeed
            from options_cio.core.portfolio_manager import CsvPortfolioManager

            feed = YFinanceFeed()
            self._market_snapshot = await asyncio.get_event_loop().run_in_executor(
                None, feed.get_market_snapshot,
            )
            vix = float(self._market_snapshot.get("vix") or 20.0)

            csv_pm = CsvPortfolioManager(self._positions_path)
            self._portfolio_states = csv_pm.get_all_portfolio_states(
                {}, self.capital_map, vix,
            )

            if self.rules is None:
                self.rules = RulesEngine(
                    self._rules_path, self.portfolios_config,
                    adapter=_NullAdapter(), greeks_engine=None,
                )

            self._greeks_summaries = []
            self._alerts = []
            for pid in csv_pm.get_portfolio_ids():
                positions = csv_pm.get_positions_for_portfolio(pid)
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

    def _collect_underlyings(self) -> list[str]:
        """Collect unique underlying symbols across all portfolios."""
        underlyings: list[str] = []
        for pid in self.pm.get_portfolio_ids():
            for pos in self._adapter.get_positions(pid):
                sym = pos.get("underlying_symbol", "")
                if sym and sym not in underlyings:
                    underlyings.append(sym)
        return underlyings

    # ------------------------------------------------------------------
    # Auto-refresh callbacks
    # ------------------------------------------------------------------

    async def _auto_rest_refresh(self) -> None:
        """Called every 60s — refresh positions, balances, and run rules."""
        if self._adapter is not None and self.pm is not None:
            await self._refresh_rest_data()
            await self._refresh_greeks_and_rules()
            self._update_ui()
        else:
            await self._refresh_csv_data()

    # ------------------------------------------------------------------
    # UI update
    # ------------------------------------------------------------------

    def _update_ui(self) -> None:
        """Full UI update — called after REST refresh or on startup."""
        self._update_portfolio_status()
        self._update_greeks_table()
        self._update_alerts()
        self._update_positions_table_live()
        self._update_journal_table()

    def _update_portfolio_status(self) -> None:
        """Update the portfolio deployment widgets."""
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

            # Per-account connection status
            acct_status = self._account_status.get(pid, "")
            if acct_status == "DISCONNECTED":
                acct_tag = " [bold red]DATA UNAVAILABLE[/bold red]"
            elif acct_status == "STALE":
                acct_tag = " [yellow]STALE[/yellow]"
            else:
                acct_tag = ""

            text = (
                f"[bold]{pid}[/bold] -- {cfg.get('name', '')}{acct_tag}\n"
                f"[{status_color}]{dep:.1%}[/{status_color}] [{status}] | "
                f"${state.get('deployed_capital', 0):,.0f} / ${state.get('capital', 0):,.0f}\n"
                f"Income: {income} | Hedge: {hedge} | Positions: {state.get('position_count', 0)}"
            )
            try:
                self.query_one(f"#status-{pid}", Static).update(text)
            except Exception:
                pass

    def _update_greeks_table(self) -> None:
        """Update the Greeks summary table."""
        try:
            greeks_table = self.query_one("#greeks-table", DataTable)
        except Exception:
            return
        greeks_table.clear()
        for g in self._greeks_summaries:
            connected = g.get("streamer_connected", False)
            pending = g.get("pending_count", 0)
            stale = g.get("stale_count", 0)
            if not connected:
                status_str = "[red]OFFLINE[/red]"
            elif stale:
                status_str = f"[yellow]{stale} STALE[/yellow]"
            elif pending:
                status_str = f"[yellow]{pending} PENDING[/yellow]"
            else:
                status_str = "[green]LIVE[/green]"
            greeks_table.add_row(
                g.get("portfolio", ""),
                f"{g.get('delta', 0):+.1f}",
                f"{g.get('gamma', 0):+.4f}",
                f"{g.get('theta', 0):+.1f}",
                f"{g.get('vega', 0):+.1f}",
                str(g.get("position_count", 0)),
                status_str,
            )

    def _update_positions_table_live(self) -> None:
        """Update positions table with live Greeks from the streamer."""
        try:
            pos_table = self.query_one("#positions-table", DataTable)
        except Exception:
            return
        pos_table.clear()

        if self.pm is not None and self._adapter is not None:
            for pid in self.pm.get_portfolio_ids():
                positions = self._position_symbols.get(pid)
                if positions is None:
                    continue
                for p in positions:
                    sym = p.get("symbol", "")
                    # Pull live Greeks from streamer
                    delta_str = ""
                    theta_str = ""
                    vega_str = ""
                    mark_str = ""
                    if self._streamer is not None:
                        greeks = self._streamer.get_greeks(sym)
                        quote = self._streamer.get_quote(sym)
                        if greeks:
                            qty = p.get("quantity", 0)
                            mult = p.get("multiplier", 100) or 100
                            direction = 1 if p.get("quantity_direction", "Long") == "Long" else -1
                            d = greeks.get("delta", 0) or 0
                            t = greeks.get("theta", 0) or 0
                            v = greeks.get("vega", 0) or 0
                            delta_str = f"{d * qty * direction * mult / 100:+.1f}"
                            theta_str = f"{t * qty * direction * mult / 100:+.1f}"
                            vega_str = f"{v * qty * direction * mult / 100:+.1f}"
                        if quote:
                            bid = quote.get("bid_price", 0) or 0
                            ask = quote.get("ask_price", 0) or 0
                            mid = (bid + ask) / 2 if bid and ask else (bid or ask)
                            mark_str = f"${mid:.2f}" if mid else ""

                    pos_table.add_row(
                        pid,
                        p.get("underlying_symbol", p.get("symbol", "")),
                        p.get("option_type", p.get("instrument_type", "")),
                        str(p.get("strike_price", "")),
                        str(p.get("expiration_date", "")),
                        str(p.get("quantity", "")),
                        f"${p.get('average_open_price', 0):.2f}",
                        mark_str,
                        delta_str,
                        theta_str,
                        vega_str,
                        str(p.get("dte", "?")),
                    )
        else:
            # CSV fallback
            from options_cio.core.portfolio_manager import CsvPortfolioManager
            csv_pm = CsvPortfolioManager(self._positions_path)
            for p in csv_pm.get_all_positions():
                pos_table.add_row(
                    p.get("portfolio", ""), p.get("ticker", ""),
                    p.get("option_type", ""), str(p.get("strike", "")),
                    str(p.get("expiry", "")), str(p.get("qty", "")),
                    f"${p.get('entry_price', 0):.2f}",
                    "", "", "", "",
                    str(p.get("dte", "?")),
                )

    def _update_alerts(self) -> None:
        """Update the alerts display."""
        alerts_text = (
            "\n".join(
                f"[{'red' if 'CRITICAL' in a else 'yellow' if 'WARN' in a else 'cyan'}]{a}[/]"
                for a in self._alerts
            )
            or "[green]No active alerts[/green]"
        )
        try:
            self.query_one("#alerts-display", Static).update(alerts_text)
        except Exception:
            pass

    def _update_journal_table(self) -> None:
        """Update the journal tab."""
        try:
            journal_table = self.query_one("#journal-table", DataTable)
        except Exception:
            return
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
    # Connection diagnostics ('c' key)
    # ------------------------------------------------------------------

    def action_connection_diag(self) -> None:
        """Toggle connection diagnostics panel."""
        self._diag_visible = not self._diag_visible
        diag_container = self.query_one("#connection-diag")
        diag_container.display = self._diag_visible
        if self._diag_visible:
            self._render_diagnostics()

    def _render_diagnostics(self) -> None:
        """Build connection diagnostics text."""
        lines = ["[bold]Connection Diagnostics[/bold]\n"]

        # Overall state
        lines.append(f"Data source: {self._data_source}")
        lines.append(f"Connection: {self._connection_state}")
        lines.append(f"Streamer: {'Connected' if self._streamer_connected else 'Disconnected'}")
        if self._last_stream_ts:
            age = time.time() - self._last_stream_ts
            lines.append(f"Last stream data: {age:.0f}s ago")
        lines.append("")

        # Per-account status
        lines.append("[bold]Account Status:[/bold]")
        for pid in sorted(self._account_status):
            status = self._account_status[pid]
            color = "green" if status == "CONNECTED" else "yellow" if status == "STALE" else "red"
            lines.append(f"  {pid}: [{color}]{status}[/{color}]")
        lines.append("")

        # Per-symbol streamer data freshness
        if self._streamer is not None:
            lines.append("[bold]Symbol Data Freshness:[/bold]")
            now = time.time()
            all_greeks = self._streamer.get_all_greeks()
            for sym in sorted(all_greeks.keys()):
                ts = self._streamer.get_timestamp(sym)
                greeks = all_greeks[sym]
                if ts is None:
                    age_str = "[red]NO DATA[/red]"
                else:
                    age = now - ts
                    if age > _STALE_THRESHOLD_SECONDS:
                        age_str = f"[yellow]{age:.0f}s ago (STALE)[/yellow]"
                    else:
                        age_str = f"[green]{age:.0f}s ago[/green]"
                has_greeks = "yes" if greeks else "no"
                lines.append(f"  {sym[:30]:30s}  {age_str}  greeks={has_greeks}")

        try:
            self.query_one("#diag-content", Static).update("\n".join(lines))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def action_refresh(self) -> None:
        if self._adapter is not None and self.pm is not None:
            self.run_worker(self._manual_refresh_live(), exclusive=True, name="manual_refresh")
        else:
            self.run_worker(self._refresh_csv_data(), exclusive=True, name="manual_refresh")

    async def _manual_refresh_live(self) -> None:
        self.notify("Refreshing positions and balances...", severity="information")
        await self._refresh_rest_data()
        await self._refresh_greeks_and_rules()
        self._update_ui()
        self.notify("Refresh complete.", severity="information")

    def action_run_ai(self) -> None:
        self.run_worker(self._run_ai_review(), exclusive=False, name="ai_review")

    def action_run_scenario(self) -> None:
        self.run_worker(self._run_scenario_analysis(), exclusive=False, name="scenario")

    def action_show_tab(self, tab_id: str) -> None:
        self.query_one(TabbedContent).active = tab_id

    async def _run_ai_review(self) -> None:
        self.notify("Running AI review...", severity="information")
        try:
            if self.pm is not None and self._adapter is not None:
                all_positions = self.pm.get_all_positions()
                positions_summary = "\n".join(
                    f"{p.get('portfolio_id', '')} {p.get('underlying_symbol', '')} "
                    f"{p.get('option_type', p.get('instrument_type', ''))} "
                    f"{p.get('strike_price', '')} "
                    f"exp:{p.get('expiration_date', '')} qty:{p.get('quantity', '')} "
                    f"[{p.get('role', '')}]"
                    for p in all_positions
                )
            else:
                from options_cio.core.portfolio_manager import CsvPortfolioManager
                csv_pm = CsvPortfolioManager(self._positions_path)
                all_positions = csv_pm.get_all_positions()
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
    # Log panel ('l' key)
    # ------------------------------------------------------------------

    def action_toggle_log(self) -> None:
        """Toggle the scrollable log panel."""
        self._log_visible = not self._log_visible
        try:
            self.query_one("#log-panel").display = self._log_visible
        except Exception:
            pass
        if self._log_visible:
            self._refresh_log_panel()

    def _refresh_log_panel(self) -> None:
        """Read the last 50 lines from options_cio.log."""
        try:
            from options_cio.logging_config import LOG_FILE
            if LOG_FILE.exists():
                lines = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()[-50:]
                self.query_one("#log-content", Static).update(
                    "\n".join(f"[dim]{line}[/dim]" for line in lines)
                )
            else:
                self.query_one("#log-content", Static).update("[dim]No log file yet.[/dim]")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Error status bar
    # ------------------------------------------------------------------

    def _show_error_status(self, module: str, message: str) -> None:
        """Show a persistent error in the bottom status bar."""
        try:
            widget = self.query_one("#error-status", Static)
            widget.update(f"ERROR: [{module}] {message} -- see log for details")
            widget.display = True
        except Exception:
            pass

    def _clear_error_status(self) -> None:
        try:
            widget = self.query_one("#error-status", Static)
            widget.display = False
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Config hot-reload
    # ------------------------------------------------------------------

    def _track_config_mtimes(self) -> None:
        """Record current mtime for each config file."""
        config_dir = BASE_DIR / "config"
        for name in ("trading_rules.json", "portfolios.json", "settings.yaml", "accounts.yaml"):
            path = config_dir / name
            if path.exists():
                self._config_mtimes[name] = path.stat().st_mtime

    async def _check_config_changes(self) -> None:
        """Poll config files for changes (every 30s)."""
        config_dir = BASE_DIR / "config"
        for name in ("trading_rules.json", "portfolios.json", "settings.yaml", "accounts.yaml"):
            path = config_dir / name
            if not path.exists():
                continue
            current_mtime = path.stat().st_mtime
            prev_mtime = self._config_mtimes.get(name, 0)
            if current_mtime <= prev_mtime:
                continue

            self._config_mtimes[name] = current_mtime
            logger.info("Config file changed: %s", name)

            if name == "accounts.yaml":
                self.notify("Account mapping changed -- restart required.", severity="warning")
                continue

            if name == "trading_rules.json":
                self.notify("Rules updated -- re-evaluating...", severity="information")
                try:
                    await self._refresh_greeks_and_rules()
                    self._update_ui()
                except Exception as e:
                    self.notify(f"Rules reload error: {e}", severity="error")

            elif name == "portfolios.json":
                try:
                    with open(config_dir / "portfolios.json") as f:
                        self.portfolios_config = json.load(f)
                    self.capital_map = {
                        pid: cfg["capital"]
                        for pid, cfg in self.portfolios_config["portfolios"].items()
                    }
                    self.notify("Portfolio config reloaded.", severity="information")
                    self._update_ui()
                except Exception as e:
                    self.notify(f"Portfolio config reload error: {e}", severity="error")

            elif name == "settings.yaml":
                try:
                    with open(config_dir / "settings.yaml") as f:
                        new_settings = __import__("yaml").safe_load(f)
                    self.settings.update(new_settings)
                    self.brain.max_daily_cost = new_settings.get("max_api_cost_per_day", 5.00)
                    self.brain.ai_offline = new_settings.get("ai_offline", False)
                    self.notify("Settings reloaded.", severity="information")
                except Exception as e:
                    self.notify(f"Settings reload error: {e}", severity="error")

    # ------------------------------------------------------------------
    # Data integrity checks (every 5 minutes)
    # ------------------------------------------------------------------

    async def _run_integrity_checks(self) -> None:
        """Position reconciliation, balance sanity, Greeks sanity."""
        if self._adapter is None or self.pm is None:
            return

        try:
            # 1. Position reconciliation — check for new/closed positions
            if self._streamer is not None:
                current_symbols = set()
                for pid in self.pm.get_portfolio_ids():
                    positions = await asyncio.get_event_loop().run_in_executor(
                        None, lambda p=pid: self._adapter.get_positions(p),
                    )
                    for pos in positions:
                        if pos.get("instrument_type") in ("Equity Option", "Future Option"):
                            current_symbols.add(pos["symbol"])

                streamed_symbols = set(self._streamer.live_data.keys())

                # New positions — auto-subscribe
                new_syms = current_symbols - streamed_symbols
                if new_syms:
                    logger.info("New positions detected: %s — subscribing", new_syms)
                    self._streamer.add_symbols(list(new_syms))
                    try:
                        await self._streamer.subscribe()
                    except Exception:
                        pass

                # Closed positions — unsubscribe
                closed_syms = streamed_symbols - current_symbols
                if closed_syms:
                    logger.info("Closed positions detected: %s — unsubscribing", closed_syms)
                    self._streamer.remove_symbols(list(closed_syms))

            # 2. Balance sanity check — flag >10% change
            for pid in self.pm.get_portfolio_ids():
                try:
                    bal = await asyncio.get_event_loop().run_in_executor(
                        None, lambda p=pid: self._adapter.get_balances(p),
                    )
                    net_liq = bal.get("net_liquidating_value", 0)
                    prev = self._prev_net_liq.get(pid)
                    if prev and prev > 0 and net_liq > 0:
                        change = abs(net_liq - prev) / prev
                        if change > 0.10:
                            # Double-check with second API call
                            bal2 = await asyncio.get_event_loop().run_in_executor(
                                None, lambda p=pid: self._adapter.get_balances(p),
                            )
                            net_liq2 = bal2.get("net_liquidating_value", 0)
                            if abs(net_liq2 - prev) / prev > 0.10:
                                logger.warning(
                                    "Suspicious balance change for %s: $%.0f -> $%.0f (%.1f%%)",
                                    pid, prev, net_liq2, change * 100,
                                )
                                self.notify(
                                    f"Large balance change {pid}: {change:.0%} -- verified",
                                    severity="warning",
                                )
                    self._prev_net_liq[pid] = net_liq
                except Exception:
                    pass

        except Exception as e:
            logger.debug("Integrity check error: %s", e)

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------

    async def action_quit(self) -> None:
        """Clean shutdown: close streamer, flush writes, print summary."""
        logger.info("Shutting down Options CIO dashboard...")

        # 1. Stop timers
        for timer in (self._rest_timer, self._stream_ui_timer,
                      self._integrity_timer, self._config_watch_timer):
            if timer is not None:
                timer.stop()

        # 2. Close streamer
        if self._streamer is not None:
            try:
                await self._streamer.__aexit__(None, None, None)
            except Exception:
                pass

        # 3. Save state snapshot
        if self.pm is not None and self.cache is not None:
            try:
                for state in self._portfolio_states:
                    self.cache.save_portfolio_state(state["portfolio_id"], state)
                if self._market_snapshot:
                    self.cache.save_market_context(self._market_snapshot)
            except Exception as e:
                logger.debug("State save on shutdown: %s", e)

        # 4. Print session summary
        runtime = time.time() - self._session_start
        hours = int(runtime // 3600)
        minutes = int((runtime % 3600) // 60)
        cost = self.brain.get_daily_cost() if self.brain else 0
        logger.info(
            "Session summary: runtime=%dh%dm, api_cost=$%.2f, violations=%d",
            hours, minutes, cost, self._violations_encountered,
        )
        print(f"\nSession: {hours}h{minutes}m | API cost: ${cost:.2f} | "
              f"Violations: {self._violations_encountered}")

        # 5. Exit
        self.exit()

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

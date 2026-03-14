"""
Portfolio Manager — live portfolio state from tastytrade.

Capital and deployment tracking comes directly from the broker's balance API.
OBP (option buying power) is the SOLE deployment metric — no CSV calculations.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from options_cio.core.rules_engine import INCOME_ROLES, HEDGE_ROLES, CONVEX_ROLES

logger = logging.getLogger(__name__)

# VIX regime thresholds
_VIX_LOW = 15.0
_VIX_ELEVATED = 20.0
_VIX_HIGH = 25.0
_VIX_SPIKE = 30.0


class PortfolioManager:
    """
    Manages portfolio state using live tastytrade data.

    All capital, deployment, and position data comes from the broker API.
    The CSV-based approach is retained only as an offline fallback.
    """

    def __init__(
        self,
        portfolios_config: dict,
        adapter: object,
        state_cache: object,
    ) -> None:
        self.portfolios_config = portfolios_config.get("portfolios", portfolios_config)
        self.adapter = adapter
        self.state_cache = state_cache

    # ------------------------------------------------------------------
    # Portfolio IDs
    # ------------------------------------------------------------------

    def get_portfolio_ids(self) -> list[str]:
        """Return all portfolio IDs from the broker accounts."""
        accounts = self.adapter.get_accounts()
        return sorted(accounts.keys())

    # ------------------------------------------------------------------
    # Live positions
    # ------------------------------------------------------------------

    def get_positions(self, portfolio_id: str) -> list[dict]:
        """Fetch live positions from tastytrade for a portfolio."""
        return self.adapter.get_positions(portfolio_id)

    def get_all_positions(self) -> list[dict]:
        """Fetch live positions across all portfolios."""
        all_pos = []
        for pid in self.get_portfolio_ids():
            positions = self.adapter.get_positions(pid)
            for pos in positions:
                pos["portfolio_id"] = pid
            all_pos.extend(positions)
        return all_pos

    # ------------------------------------------------------------------
    # Live capital & deployment (broker is single source of truth)
    # ------------------------------------------------------------------

    def get_balances(self, portfolio_id: str) -> dict:
        """
        Fetch live balances from tastytrade.

        Returns:
            net_liquidating_value: account net liq (direct from broker)
            option_buying_power: OBP — THE deployment metric
            committed_obp: net_liq - OBP = capital in use
            deployment_pct: committed_obp / net_liq as percentage
        """
        return self.adapter.get_balances(portfolio_id)

    def get_system_balances(self) -> dict:
        """
        Aggregate balances across all 4 portfolio accounts.

        System deployment = total committed OBP / total net liq.
        """
        return self.adapter.get_system_balances()

    # ------------------------------------------------------------------
    # Portfolio state snapshots (used by rules engine and AI)
    # ------------------------------------------------------------------

    def get_portfolio_state(
        self,
        portfolio_id: str,
        vix: float = 20.0,
        classified_positions: list[dict] | None = None,
    ) -> dict:
        """
        Build a complete portfolio state from live broker data.

        No manual input required — positions, capital, deployment all
        come from the tastytrade API.
        """
        balances = self.adapter.get_balances(portfolio_id)
        positions = classified_positions or self.adapter.get_positions(portfolio_id)

        net_liq = balances["net_liquidating_value"]
        obp = balances["option_buying_power"]
        committed = balances["committed_obp"]
        deployment_pct = balances["deployment_pct"] / 100.0  # normalize to 0-1

        p_config = self.portfolios_config.get(portfolio_id, {})
        band = p_config.get("deployment_band", [0, 1.0])
        target = p_config.get("target_zone", band)

        # Classify positions
        income_positions = [p for p in positions if p.get("role") in INCOME_ROLES]
        hedge_positions = [p for p in positions if p.get("role") in HEDGE_ROLES]
        option_positions = [
            p for p in positions
            if p.get("instrument_type") in ("Equity Option", "Future Option")
        ]

        # Deployment assessment
        deployment_status = self._assess_deployment(deployment_pct, band, target)

        # Deployment decision based on VIX regime
        deployment_decision = self._deployment_decision(
            deployment_pct, band, target, vix,
        )

        state = {
            "portfolio_id": portfolio_id,
            "name": p_config.get("name", portfolio_id),
            # Capital (live from broker)
            "net_liquidating_value": net_liq,
            "option_buying_power": obp,
            "committed_obp": committed,
            "capital": net_liq,
            "deployed_capital": committed,
            # Deployment (live from broker — OBP is sole metric)
            "deployment_pct": deployment_pct,
            "deployment_band": band,
            "target_zone": target,
            "deployment_status": deployment_status,
            "deployment_decision": deployment_decision,
            # Positions
            "position_count": len(option_positions),
            "total_position_count": len(positions),
            "has_income_positions": len(income_positions) > 0,
            "has_hedge": len(hedge_positions) > 0,
            "hedge_count": len(hedge_positions),
            "income_count": len(income_positions),
            # Volatility context
            "vix": vix,
            "vix_regime": self._vix_regime(vix),
            # Tracking flags
            "hedge_removed": False,
            "deployment_increased": False,
        }

        # Persist to state cache
        self.state_cache.save_portfolio_state(portfolio_id, {
            "capital": net_liq,
            "deployed": committed,
            "deployment_pct": deployment_pct,
            "pnl_open": 0.0,
            "pnl_day": 0.0,
            "hedge_ratio": (
                len(hedge_positions) / len(income_positions)
                if income_positions else 0.0
            ),
        })

        return state

    def get_all_portfolio_states(self, vix: float = 20.0) -> list[dict]:
        """Build state snapshots for all portfolios using live data."""
        states = []
        for pid in self.get_portfolio_ids():
            try:
                state = self.get_portfolio_state(pid, vix=vix)
                states.append(state)
            except Exception as e:
                logger.error("Could not get state for %s: %s", pid, e)
        return states

    # ------------------------------------------------------------------
    # Holdings summary
    # ------------------------------------------------------------------

    def get_holdings_summary(self, portfolio_id: str) -> dict:
        """
        Fully populated holdings summary from live data.

        Fetches positions and balances from the adapter, classifies
        each position by role, and returns a complete summary with
        zero manual input.
        """
        positions = self.adapter.get_positions(portfolio_id)
        balances = self.adapter.get_balances(portfolio_id)
        p_config = self.portfolios_config.get(portfolio_id, {})

        # Group by role
        by_role: dict[str, list[dict]] = {}
        for pos in positions:
            role = pos.get("role", "unclassified")
            by_role.setdefault(role, []).append(pos)

        # Group by underlying
        by_underlying: dict[str, list[dict]] = {}
        for pos in positions:
            und = pos.get("underlying_symbol", "OTHER")
            by_underlying.setdefault(und, []).append(pos)

        # Compute unrealized PnL from mark vs average_open_price
        total_unrealized = 0.0
        total_day_gain = 0.0
        for pos in positions:
            mark = pos.get("mark")
            avg_open = pos.get("average_open_price", 0)
            qty = int(pos.get("quantity", 0))
            direction = pos.get("quantity_direction", "Long")
            signed_qty = qty if direction == "Long" else -qty
            multiplier = int(pos.get("multiplier", 100) or 100)

            if mark is not None:
                total_unrealized += (mark - avg_open) * signed_qty * multiplier

            day_gain = pos.get("realized_day_gain", 0)
            total_day_gain += day_gain

        return {
            "portfolio_id": portfolio_id,
            "name": p_config.get("name", portfolio_id),
            # Capital
            "net_liquidating_value": balances["net_liquidating_value"],
            "option_buying_power": balances["option_buying_power"],
            "committed_obp": balances["committed_obp"],
            "deployment_pct": balances["deployment_pct"],
            "cash_balance": balances.get("cash_balance", 0),
            # Positions
            "total_positions": len(positions),
            "positions_by_role": {
                role: len(plist) for role, plist in by_role.items()
            },
            "positions_by_underlying": {
                und: len(plist) for und, plist in by_underlying.items()
            },
            "positions": positions,
            # PnL
            "unrealized_pnl": round(total_unrealized, 2),
            "day_gain": round(total_day_gain, 2),
        }

    # ------------------------------------------------------------------
    # Account health monitoring
    # ------------------------------------------------------------------

    def check_account_connectivity(self) -> dict:
        """
        Ping each account to verify the API is responding.

        Returns status per account:
        - CONNECTED: API responding, fresh data
        - STALE: API responded but data may be delayed
        - DISCONNECTED: API call failed

        If any account is disconnected, system should go YELLOW.
        """
        results: dict[str, dict] = {}
        all_connected = True

        for pid in sorted(self.portfolios_config.keys()):
            start = time.time()
            try:
                balances = self.adapter.get_balances(pid)
                elapsed = time.time() - start

                if elapsed > 10.0:
                    status = "STALE"
                    all_connected = False
                elif balances.get("net_liquidating_value", 0) == 0:
                    status = "STALE"
                    all_connected = False
                else:
                    status = "CONNECTED"

                results[pid] = {
                    "status": status,
                    "net_liq": balances.get("net_liquidating_value", 0),
                    "response_ms": round(elapsed * 1000),
                }
            except Exception as e:
                elapsed = time.time() - start
                all_connected = False
                results[pid] = {
                    "status": "DISCONNECTED",
                    "error": str(e),
                    "response_ms": round(elapsed * 1000),
                }

        system_status = "GREEN" if all_connected else "YELLOW"
        disconnected = [pid for pid, r in results.items() if r["status"] == "DISCONNECTED"]
        if disconnected:
            system_status = "YELLOW"

        return {
            "system_status": system_status,
            "accounts": results,
            "all_connected": all_connected,
            "disconnected": disconnected,
        }

    # ------------------------------------------------------------------
    # Deployment assessment
    # ------------------------------------------------------------------

    def _assess_deployment(
        self,
        deployment_pct: float,
        band: list[float],
        target: list[float],
    ) -> str:
        """Classify current deployment vs mandate band."""
        if target[0] <= deployment_pct <= target[1]:
            return "TARGET"
        if band[0] <= deployment_pct <= band[1]:
            return "IN_BAND"
        if deployment_pct > band[1]:
            return "OVERDEPLOYED"
        return "UNDERDEPLOYED"

    def deployment_band_check(self, portfolio_id: str) -> dict:
        """
        Check deployment vs mandate bands using live broker balances.

        OBP is the sole deployment metric — no margin or notional calculations.
        """
        balances = self.adapter.get_balances(portfolio_id)
        deployment_pct = balances["deployment_pct"] / 100.0  # normalize
        p_config = self.portfolios_config.get(portfolio_id, {})
        band = p_config.get("deployment_band", [0, 1.0])
        target = p_config.get("target_zone", band)

        return {
            "portfolio_id": portfolio_id,
            "deployment_pct": deployment_pct,
            "band_low": band[0],
            "band_high": band[1],
            "target_low": target[0],
            "target_high": target[1],
            "in_band": band[0] <= deployment_pct <= band[1],
            "in_target": target[0] <= deployment_pct <= target[1],
            "status": self._assess_deployment(deployment_pct, band, target),
            # Live broker capital
            "net_liq": balances["net_liquidating_value"],
            "option_buying_power": balances["option_buying_power"],
            "committed_obp": balances["committed_obp"],
        }

    # ------------------------------------------------------------------
    # Deployment decisions (VIX-aware)
    # ------------------------------------------------------------------

    def _deployment_decision(
        self,
        deployment_pct: float,
        band: list[float],
        target: list[float],
        vix: float,
    ) -> str:
        """
        Determine deployment action based on current deployment and VIX regime.

        Rules:
        - VIX > 30 (SPIKE): FREEZE — no deployment increases allowed
        - VIX > 25 (HIGH): REDUCE if overdeployed, else MAINTAIN
        - VIX > 20 (ELEVATED): MAINTAIN if in band, cautious increase if under
        - VIX <= 20 (LOW/NORMAL): normal deployment rules apply
        """
        regime = self._vix_regime(vix)

        if regime == "SPIKE":
            if deployment_pct > band[1]:
                return "REDUCE"
            return "FREEZE"

        if regime == "HIGH":
            if deployment_pct > band[1]:
                return "REDUCE"
            return "MAINTAIN"

        if regime == "ELEVATED":
            if deployment_pct > band[1]:
                return "REDUCE"
            if deployment_pct < band[0]:
                return "CAUTIOUS_INCREASE"
            return "MAINTAIN"

        # LOW or NORMAL
        if deployment_pct > band[1]:
            return "REDUCE"
        if deployment_pct < target[0]:
            return "INCREASE"
        if target[0] <= deployment_pct <= target[1]:
            return "MAINTAIN"
        return "MAINTAIN"

    @staticmethod
    def _vix_regime(vix: float) -> str:
        """Classify VIX into regime buckets."""
        if vix >= _VIX_SPIKE:
            return "SPIKE"
        if vix >= _VIX_HIGH:
            return "HIGH"
        if vix >= _VIX_ELEVATED:
            return "ELEVATED"
        return "LOW"

    # ------------------------------------------------------------------
    # Input governance
    # ------------------------------------------------------------------

    def validate_deployment_metric(self) -> dict:
        """
        Enforce that OBP is the sole deployment metric.

        Returns validation result confirming that all deployment
        calculations use option_buying_power from the broker,
        not margin, notional, or CSV-derived values.
        """
        issues = []
        for pid in self.get_portfolio_ids():
            balances = self.adapter.get_balances(pid)
            if "option_buying_power" not in balances:
                issues.append(f"{pid}: missing option_buying_power in balance response")
            if "derivative_buying_power" not in balances:
                issues.append(f"{pid}: missing derivative_buying_power in balance response")

        return {
            "metric": "option_buying_power",
            "source": "tastytrade balance API",
            "valid": len(issues) == 0,
            "issues": issues,
        }


# ======================================================================
# CSV fallback for offline/yfinance mode
# ======================================================================

_CSV_INCOME_TAGS = {"short_put", "jade_lizard_put", "jade_lizard_call", "income_bwb_short"}
_CSV_HEDGE_TAGS = {"spx_hedge", "spx_structural_hedge", "vix_hedge", "crash_hedge_put"}


class CsvPortfolioManager:
    """
    Lightweight CSV-based portfolio manager for offline/yfinance mode.

    This is the fallback when tastytrade is not available.  Capital and
    deployment are estimated from position data, not broker balances.
    """

    def __init__(self, positions_path: str | Path) -> None:
        import pandas as pd

        self.positions_path = Path(positions_path)
        self._positions_df = None
        if self.positions_path.exists():
            df = pd.read_csv(self.positions_path)
            df["expiry"] = pd.to_datetime(df["expiry"]).dt.date
            df["strike"] = df["strike"].astype(float)
            df["qty"] = df["qty"].astype(int)
            df["entry_price"] = df["entry_price"].astype(float)
            df["dte"] = df["expiry"].apply(
                lambda e: max((e - date.today()).days, 0)
            )
            self._positions_df = df

    def get_positions_for_portfolio(self, portfolio_id: str) -> list[dict]:
        if self._positions_df is None:
            return []
        df = self._positions_df[self._positions_df["portfolio"] == portfolio_id]
        return df.to_dict(orient="records")

    def get_all_positions(self) -> list[dict]:
        if self._positions_df is None:
            return []
        return self._positions_df.to_dict(orient="records")

    def get_portfolio_ids(self) -> list[str]:
        if self._positions_df is None:
            return []
        return sorted(self._positions_df["portfolio"].unique().tolist())

    def get_portfolio_state(
        self,
        portfolio_id: str,
        price_map: dict[str, float],
        capital_map: dict[str, float],
    ) -> dict:
        positions = self.get_positions_for_portfolio(portfolio_id)
        capital = capital_map.get(portfolio_id, 125000)

        income_positions = [p for p in positions if p.get("structure_tag") in _CSV_INCOME_TAGS]
        hedge_positions = [p for p in positions if p.get("structure_tag") in _CSV_HEDGE_TAGS]

        deployed = sum(
            abs(p["entry_price"]) * abs(p["qty"]) * 100
            for p in positions
        )
        deployment_pct = deployed / capital if capital > 0 else 0

        return {
            "portfolio_id": portfolio_id,
            "position_count": len(positions),
            "deployed_capital": deployed,
            "deployment_pct": deployment_pct,
            "capital": capital,
            "unrealised_pnl": 0.0,
            "has_income_positions": len(income_positions) > 0,
            "has_hedge": len(hedge_positions) > 0,
            "hedge_count": len(hedge_positions),
            "income_count": len(income_positions),
            "hedge_removed": False,
            "deployment_increased": False,
        }

    def get_all_portfolio_states(
        self,
        price_map: dict[str, float],
        capital_map: dict[str, float],
        vix: float = 20.0,
    ) -> list[dict]:
        states = []
        for pid in self.get_portfolio_ids():
            state = self.get_portfolio_state(pid, price_map, capital_map)
            state["vix"] = vix
            states.append(state)
        return states

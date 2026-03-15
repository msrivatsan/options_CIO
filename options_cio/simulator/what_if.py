"""
What-If Simulator — models portfolio P&L under shock scenarios
using first-order (delta/vega) and second-order (gamma) approximations.

Baseline data comes from live tastytrade positions, Greeks, and quotes.
Falls back to caller-supplied greeks maps when no live data is available.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# Preset scenarios
SCENARIOS: dict[str, dict] = {
    "crash_20": {
        "name": "Equity Crash -20%",
        "description": "Broad equity sell-off, SPX -20%, VIX +15, crypto -30%",
        "shocks": {"spx_pct": -0.20, "vix_delta": 15, "crypto_pct": -0.30, "rates_bp": -30},
    },
    "vol_spike": {
        "name": "Vol Spike Only",
        "description": "VIX spikes +12 with minimal price move",
        "shocks": {"spx_pct": -0.03, "vix_delta": 12, "crypto_pct": -0.05, "rates_bp": -10},
    },
    "btc_halving_rip": {
        "name": "BTC +80% Rally",
        "description": "Crypto bull run, BTC +80%, equities sideways",
        "shocks": {"spx_pct": 0.02, "vix_delta": -2, "crypto_pct": 0.80, "rates_bp": 10},
    },
    "rates_shock": {
        "name": "Rates Shock +100bp",
        "description": "10Y yield spikes 100bp, equities -8%, vol up",
        "shocks": {"spx_pct": -0.08, "vix_delta": 8, "crypto_pct": -0.12, "rates_bp": 100},
    },
    "soft_landing": {
        "name": "Soft Landing",
        "description": "Rates down 50bp, equities +5%, vol crushed",
        "shocks": {"spx_pct": 0.05, "vix_delta": -5, "crypto_pct": 0.10, "rates_bp": -50},
    },
}

PORTFOLIO_IDS = ["P1", "P2", "P3", "P4"]


@dataclass
class PositionResult:
    """Per-position impact from a scenario shock."""
    symbol: str
    underlying: str
    portfolio_id: str
    qty: int = 0
    delta_pnl: float = 0.0
    gamma_pnl: float = 0.0
    vega_pnl: float = 0.0
    theta_pnl: float = 0.0
    total_pnl: float = 0.0
    live_iv: Optional[float] = None
    shocked_iv: Optional[float] = None
    notes: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        iv_str = ""
        if self.live_iv is not None and self.shocked_iv is not None:
            iv_str = f"  IV: {self.live_iv:.1%}→{self.shocked_iv:.1%}"
        return (
            f"  {self.symbol} qty={self.qty}: "
            f"Δ=${self.delta_pnl:,.0f} Γ=${self.gamma_pnl:,.0f} "
            f"V=${self.vega_pnl:,.0f} Θ=${self.theta_pnl:,.0f} "
            f"total=${self.total_pnl:,.0f}{iv_str}"
        )


@dataclass
class ScenarioResult:
    scenario_name: str
    portfolio_id: str
    delta_pnl: float = 0.0
    gamma_pnl: float = 0.0
    vega_pnl: float = 0.0
    theta_pnl: float = 0.0
    total_estimated_pnl: float = 0.0
    positions: list[PositionResult] = field(default_factory=list)
    hedge_suggestions: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    data_source: str = "live"

    def __str__(self) -> str:
        return (
            f"{self.portfolio_id} | {self.scenario_name}: "
            f"delta_pnl=${self.delta_pnl:,.0f}  gamma_pnl=${self.gamma_pnl:,.0f}  "
            f"vega_pnl=${self.vega_pnl:,.0f}  theta_pnl=${self.theta_pnl:,.0f}  "
            f"total=${self.total_estimated_pnl:,.0f}  [{self.data_source}]"
        )


@dataclass
class _LiveSnapshot:
    """Internal snapshot of live broker data used as simulation baseline."""
    positions: dict[str, list[dict]]  # pid → list of position dicts
    greeks: dict[str, dict]           # symbol → greeks dict
    quotes: dict[str, dict]           # symbol → quote dict
    timestamp: float = 0.0


class WhatIfSimulator:
    """
    First-order P&L approximation under shock scenarios.

    When adapter and streamer are provided, simulations start from a live
    broker snapshot.  Otherwise falls back to caller-supplied greeks maps
    (legacy CSV mode).
    """

    CRYPTO_TICKERS = {"IBIT", "ETHA", "BTC-USD", "ETH-USD"}
    RATES_TICKERS = {"TLT", "IEF", "ZB", "ZN"}

    def __init__(self, adapter=None, streamer=None) -> None:
        self._adapter = adapter
        self._streamer = streamer

    # ------------------------------------------------------------------
    # Live snapshot
    # ------------------------------------------------------------------

    def _snapshot(self) -> Optional[_LiveSnapshot]:
        """Capture current positions, greeks, and quotes from the broker."""
        if not self._adapter:
            return None

        positions: dict[str, list[dict]] = {}
        for pid in PORTFOLIO_IDS:
            try:
                positions[pid] = self._adapter.get_positions(pid)
            except Exception as e:
                logger.warning("Could not snapshot positions for %s: %s", pid, e)
                positions[pid] = []

        greeks: dict[str, dict] = {}
        quotes: dict[str, dict] = {}
        if self._streamer:
            greeks = self._streamer.get_all_greeks()
            quotes = self._streamer.get_all_quotes()

        return _LiveSnapshot(
            positions=positions,
            greeks=greeks,
            quotes=quotes,
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_scenario(
        self,
        scenario_key: str,
        portfolio_greeks_map: Optional[dict[str, dict]] = None,
        capital_map: Optional[dict[str, float]] = None,
        custom_scenario: Optional[dict] = None,
        position_detail: bool = True,
    ) -> list[ScenarioResult]:
        """Run a scenario against live data or fallback greeks.

        Parameters
        ----------
        scenario_key : str
            Key into SCENARIOS or "custom" when custom_scenario is provided.
        portfolio_greeks_map : dict, optional
            Legacy fallback: {portfolio_id: {delta, gamma, theta, vega, ...}}.
            Ignored when live adapter/streamer are available.
        capital_map : dict, optional
            {portfolio_id: capital_float}. Used for impact-% notes.
        custom_scenario : dict, optional
            Override scenario definition.
        position_detail : bool
            If True, include per-position breakdown in results.
        """
        scenario = custom_scenario or SCENARIOS.get(scenario_key)
        if not scenario:
            raise ValueError(f"Unknown scenario: {scenario_key}. Available: {list(SCENARIOS)}")

        shocks = scenario.get("shocks", {})
        cap = capital_map or {}

        # Try live snapshot first
        snap = self._snapshot()
        if snap and any(snap.positions.values()):
            return self._run_from_snapshot(snap, shocks, scenario, cap, position_detail)

        # Fallback to legacy greeks map
        if portfolio_greeks_map:
            return self._run_from_greeks_map(portfolio_greeks_map, shocks, scenario, cap)

        raise ValueError("No live data and no portfolio_greeks_map provided")

    def price_shock(
        self,
        underlying: str,
        shock_pct: float,
        portfolio_id: Optional[str] = None,
    ) -> list[ScenarioResult]:
        """Pure price shock on a single underlying."""
        scenario = {
            "name": f"{underlying} {shock_pct:+.0%}",
            "description": f"Price shock: {underlying} moves {shock_pct:+.0%}",
            "shocks": {"custom_underlying": underlying, "custom_pct": shock_pct},
        }
        snap = self._snapshot()
        if not snap:
            raise ValueError("price_shock requires live adapter/streamer")

        pids = [portfolio_id] if portfolio_id else list(snap.positions.keys())
        results = []
        for pid in pids:
            results.append(self._shock_portfolio_positions(
                pid, snap, underlying, shock_pct, 0, scenario,
            ))
        return results

    def iv_crush(
        self,
        crush_pct: float = -0.20,
        portfolio_id: Optional[str] = None,
    ) -> list[ScenarioResult]:
        """IV crush applied to all positions using live IV as baseline."""
        scenario = {
            "name": f"IV Crush {crush_pct:+.0%}",
            "description": f"Implied volatility drops {abs(crush_pct):.0%} from current levels",
            "shocks": {"iv_crush_pct": crush_pct},
        }
        snap = self._snapshot()
        if not snap:
            raise ValueError("iv_crush requires live adapter/streamer")

        pids = [portfolio_id] if portfolio_id else list(snap.positions.keys())
        results = []
        for pid in pids:
            results.append(self._iv_crush_portfolio(pid, snap, crush_pct, scenario))
        return results

    def time_decay(
        self,
        days: int = 1,
        portfolio_id: Optional[str] = None,
    ) -> list[ScenarioResult]:
        """Estimate theta decay over N days using live theta."""
        scenario = {
            "name": f"{days}-Day Theta Decay",
            "description": f"Pure time decay over {days} trading days",
            "shocks": {"time_days": days},
        }
        snap = self._snapshot()
        if not snap:
            raise ValueError("time_decay requires live adapter/streamer")

        pids = [portfolio_id] if portfolio_id else list(snap.positions.keys())
        results = []
        for pid in pids:
            results.append(self._theta_decay_portfolio(pid, snap, days, scenario))
        return results

    def combined_scenario(
        self,
        spx_pct: float = 0.0,
        vix_delta: float = 0.0,
        crypto_pct: float = 0.0,
        rates_bp: float = 0.0,
        days: int = 0,
        name: str = "Combined",
        description: str = "",
    ) -> list[ScenarioResult]:
        """Build and run a custom combined scenario against live data."""
        scenario = self.custom_scenario(
            name=name,
            spx_pct=spx_pct,
            vix_delta=vix_delta,
            crypto_pct=crypto_pct,
            rates_bp=rates_bp,
            description=description,
        )
        scenario["shocks"]["time_days"] = days
        return self.run_scenario("custom", custom_scenario=scenario)

    def system_stress_test(self) -> dict[str, list[ScenarioResult]]:
        """Run ALL preset scenarios and return results keyed by scenario."""
        all_results: dict[str, list[ScenarioResult]] = {}
        for key in SCENARIOS:
            try:
                all_results[key] = self.run_scenario(key)
            except Exception as e:
                logger.warning("Stress test scenario %s failed: %s", key, e)
        return all_results

    def list_scenarios(self) -> list[dict]:
        return [
            {"key": k, "name": v["name"], "description": v["description"]}
            for k, v in SCENARIOS.items()
        ]

    def custom_scenario(
        self,
        name: str,
        spx_pct: float = 0.0,
        vix_delta: float = 0.0,
        crypto_pct: float = 0.0,
        rates_bp: float = 0.0,
        description: str = "",
    ) -> dict:
        return {
            "name": name,
            "description": description or f"Custom: SPX{spx_pct:+.0%} VIX{vix_delta:+.0f} BTC{crypto_pct:+.0%}",
            "shocks": {
                "spx_pct": spx_pct,
                "vix_delta": vix_delta,
                "crypto_pct": crypto_pct,
                "rates_bp": rates_bp,
            },
        }

    def get_hedge_suggestions(
        self,
        portfolio_id: str,
        scenario_result: ScenarioResult,
    ) -> list[str]:
        """Use live option chain to suggest hedges for a scenario's exposure."""
        if not self._adapter:
            return []

        suggestions: list[str] = []
        if scenario_result.total_estimated_pnl >= 0:
            return suggestions

        loss = abs(scenario_result.total_estimated_pnl)

        # Determine the primary underlying to hedge
        underlying = self._primary_underlying(portfolio_id)
        if not underlying:
            return suggestions

        try:
            chain = self._adapter.get_option_chain(underlying)
        except Exception as e:
            logger.warning("Could not fetch option chain for %s: %s", underlying, e)
            return suggestions

        # Find near-money puts 30-60 DTE
        puts = [
            c for c in chain
            if c.get("option_type") == "P"
            and 30 <= (c.get("days_to_expiration") or 0) <= 60
            and c.get("active", True)
        ]
        if not puts:
            return suggestions

        # Sort by distance from ATM
        snap = self._snapshot()
        current_quote = None
        if snap and self._streamer:
            # Try to get underlying price from quotes
            for sym, q in snap.quotes.items():
                if underlying.upper() in sym.upper() and q:
                    bid = q.get("bid_price") or 0
                    ask = q.get("ask_price") or 0
                    if bid and ask:
                        current_quote = (bid + ask) / 2
                        break

        if current_quote:
            puts.sort(key=lambda c: abs(c.get("strike_price", 0) - current_quote))
            nearest = puts[:3]
            for p in nearest:
                strike = p.get("strike_price", 0)
                dte = p.get("days_to_expiration", 0)
                suggestions.append(
                    f"Buy {underlying} {strike} put ({dte} DTE) "
                    f"— covers ~${loss:,.0f} scenario loss"
                )

        return suggestions

    # ------------------------------------------------------------------
    # Internal — live snapshot simulation
    # ------------------------------------------------------------------

    def _run_from_snapshot(
        self,
        snap: _LiveSnapshot,
        shocks: dict,
        scenario: dict,
        capital_map: dict[str, float],
        position_detail: bool,
    ) -> list[ScenarioResult]:
        """Run scenario using live positions + streamed greeks as baseline."""
        results = []
        for pid, positions in snap.positions.items():
            result = ScenarioResult(
                scenario_name=scenario.get("name", ""),
                portfolio_id=pid,
                data_source="live",
            )

            for pos in positions:
                sym = pos.get("symbol", "")
                underlying = pos.get("underlying_symbol", "")
                qty = pos.get("quantity", 0) or 0
                multiplier = pos.get("multiplier", 100) or 100
                direction = 1 if pos.get("quantity_direction") == "Long" else -1
                signed_qty = qty * direction

                greeks = snap.greeks.get(sym) or {}
                delta = greeks.get("delta") or 0
                gamma = greeks.get("gamma") or 0
                vega = greeks.get("vega") or 0
                theta = greeks.get("theta") or 0
                live_iv = greeks.get("volatility")

                # Determine underlying move for this position
                underlying_move_pct = self._underlying_shock(underlying, pid, shocks)
                # Get a reference price for the underlying
                ref_price = self._ref_price(underlying, snap)
                underlying_move_pts = underlying_move_pct * ref_price

                vix_move = shocks.get("vix_delta", 0)
                time_days = shocks.get("time_days", 0)

                # Per-contract greeks × qty × multiplier
                pos_delta_pnl = delta * underlying_move_pts * signed_qty * multiplier
                pos_gamma_pnl = 0.5 * gamma * (underlying_move_pts ** 2) * signed_qty * multiplier
                pos_vega_pnl = vega * vix_move * signed_qty * multiplier
                pos_theta_pnl = theta * time_days * signed_qty * multiplier

                pos_total = pos_delta_pnl + pos_gamma_pnl + pos_vega_pnl + pos_theta_pnl

                # Compute shocked IV for IV crush scenarios
                shocked_iv = None
                if live_iv is not None and shocks.get("iv_crush_pct"):
                    shocked_iv = live_iv * (1 + shocks["iv_crush_pct"])

                pos_result = PositionResult(
                    symbol=sym,
                    underlying=underlying,
                    portfolio_id=pid,
                    qty=signed_qty,
                    delta_pnl=pos_delta_pnl,
                    gamma_pnl=pos_gamma_pnl,
                    vega_pnl=pos_vega_pnl,
                    theta_pnl=pos_theta_pnl,
                    total_pnl=pos_total,
                    live_iv=live_iv,
                    shocked_iv=shocked_iv,
                )

                result.delta_pnl += pos_delta_pnl
                result.gamma_pnl += pos_gamma_pnl
                result.vega_pnl += pos_vega_pnl
                result.theta_pnl += pos_theta_pnl
                result.total_estimated_pnl += pos_total

                if position_detail:
                    result.positions.append(pos_result)

            # Portfolio-level notes
            capital = capital_map.get(pid, 125000)
            self._add_portfolio_notes(result, pid, shocks, capital)

            # Hedge suggestions for lossy scenarios
            if result.total_estimated_pnl < 0:
                result.hedge_suggestions = self.get_hedge_suggestions(pid, result)

            results.append(result)

        return results

    def _iv_crush_portfolio(
        self,
        pid: str,
        snap: _LiveSnapshot,
        crush_pct: float,
        scenario: dict,
    ) -> ScenarioResult:
        """Apply IV crush using live IV as baseline."""
        result = ScenarioResult(
            scenario_name=scenario.get("name", ""),
            portfolio_id=pid,
            data_source="live",
        )

        for pos in snap.positions.get(pid, []):
            sym = pos.get("symbol", "")
            qty = pos.get("quantity", 0) or 0
            multiplier = pos.get("multiplier", 100) or 100
            direction = 1 if pos.get("quantity_direction") == "Long" else -1
            signed_qty = qty * direction

            greeks = snap.greeks.get(sym) or {}
            vega = greeks.get("vega") or 0
            live_iv = greeks.get("volatility")

            if live_iv is None:
                continue

            # IV crush expressed as vol points
            iv_change_pts = live_iv * crush_pct * 100  # vega is per 1 vol point
            pos_vega_pnl = vega * iv_change_pts * signed_qty * multiplier
            shocked_iv = live_iv * (1 + crush_pct)

            pos_result = PositionResult(
                symbol=sym,
                underlying=pos.get("underlying_symbol", ""),
                portfolio_id=pid,
                qty=signed_qty,
                vega_pnl=pos_vega_pnl,
                total_pnl=pos_vega_pnl,
                live_iv=live_iv,
                shocked_iv=shocked_iv,
            )

            result.vega_pnl += pos_vega_pnl
            result.total_estimated_pnl += pos_vega_pnl
            result.positions.append(pos_result)

        return result

    def _theta_decay_portfolio(
        self,
        pid: str,
        snap: _LiveSnapshot,
        days: int,
        scenario: dict,
    ) -> ScenarioResult:
        """Pure theta decay over N days."""
        result = ScenarioResult(
            scenario_name=scenario.get("name", ""),
            portfolio_id=pid,
            data_source="live",
        )

        for pos in snap.positions.get(pid, []):
            sym = pos.get("symbol", "")
            qty = pos.get("quantity", 0) or 0
            multiplier = pos.get("multiplier", 100) or 100
            direction = 1 if pos.get("quantity_direction") == "Long" else -1
            signed_qty = qty * direction

            greeks = snap.greeks.get(sym) or {}
            theta = greeks.get("theta") or 0

            pos_theta_pnl = theta * days * signed_qty * multiplier

            pos_result = PositionResult(
                symbol=sym,
                underlying=pos.get("underlying_symbol", ""),
                portfolio_id=pid,
                qty=signed_qty,
                theta_pnl=pos_theta_pnl,
                total_pnl=pos_theta_pnl,
            )

            result.theta_pnl += pos_theta_pnl
            result.total_estimated_pnl += pos_theta_pnl
            result.positions.append(pos_result)

        return result

    def _shock_portfolio_positions(
        self,
        pid: str,
        snap: _LiveSnapshot,
        underlying: str,
        shock_pct: float,
        vix_move: float,
        scenario: dict,
    ) -> ScenarioResult:
        """Apply a price shock to positions in a single underlying."""
        result = ScenarioResult(
            scenario_name=scenario.get("name", ""),
            portfolio_id=pid,
            data_source="live",
        )

        ref_price = self._ref_price(underlying, snap)
        move_pts = shock_pct * ref_price

        for pos in snap.positions.get(pid, []):
            if (pos.get("underlying_symbol") or "").upper() != underlying.upper():
                continue

            sym = pos.get("symbol", "")
            qty = pos.get("quantity", 0) or 0
            multiplier = pos.get("multiplier", 100) or 100
            direction = 1 if pos.get("quantity_direction") == "Long" else -1
            signed_qty = qty * direction

            greeks = snap.greeks.get(sym) or {}
            delta = greeks.get("delta") or 0
            gamma = greeks.get("gamma") or 0
            vega = greeks.get("vega") or 0

            pos_delta_pnl = delta * move_pts * signed_qty * multiplier
            pos_gamma_pnl = 0.5 * gamma * (move_pts ** 2) * signed_qty * multiplier
            pos_vega_pnl = vega * vix_move * signed_qty * multiplier
            pos_total = pos_delta_pnl + pos_gamma_pnl + pos_vega_pnl

            pos_result = PositionResult(
                symbol=sym,
                underlying=pos.get("underlying_symbol", ""),
                portfolio_id=pid,
                qty=signed_qty,
                delta_pnl=pos_delta_pnl,
                gamma_pnl=pos_gamma_pnl,
                vega_pnl=pos_vega_pnl,
                total_pnl=pos_total,
                live_iv=greeks.get("volatility"),
            )

            result.delta_pnl += pos_delta_pnl
            result.gamma_pnl += pos_gamma_pnl
            result.vega_pnl += pos_vega_pnl
            result.total_estimated_pnl += pos_total
            result.positions.append(pos_result)

        return result

    # ------------------------------------------------------------------
    # Internal — legacy fallback (no live data)
    # ------------------------------------------------------------------

    def _run_from_greeks_map(
        self,
        portfolio_greeks_map: dict[str, dict],
        shocks: dict,
        scenario: dict,
        capital_map: dict[str, float],
    ) -> list[ScenarioResult]:
        """Legacy path: use caller-supplied aggregate greeks per portfolio."""
        results = []
        for pid, greeks in portfolio_greeks_map.items():
            result = self._apply_shocks_legacy(pid, greeks, shocks, capital_map.get(pid, 125000), scenario)
            results.append(result)
        return results

    def _apply_shocks_legacy(
        self,
        portfolio_id: str,
        greeks: dict,
        shocks: dict,
        capital: float,
        scenario: dict,
    ) -> ScenarioResult:
        delta = greeks.get("delta", 0)
        vega = greeks.get("vega", 0)

        spx_move = shocks.get("spx_pct", 0)
        vix_move = shocks.get("vix_delta", 0)

        if portfolio_id == "P1":
            underlying_move_pct = shocks.get("crypto_pct", spx_move)
            underlying_move = underlying_move_pct * 100
        elif portfolio_id in ("P2", "P4"):
            underlying_move = spx_move * 5300
        elif portfolio_id == "P3":
            underlying_move = shocks.get("rates_bp", 0) * 0.5
        else:
            underlying_move = spx_move * 5300

        delta_pnl = delta * underlying_move
        vega_pnl = vega * vix_move

        total = delta_pnl + vega_pnl
        pct_of_capital = total / capital * 100 if capital > 0 else 0

        notes = []
        if portfolio_id == "P1" and shocks.get("crypto_pct", 0) < -0.30:
            notes.append("WARNING: Crypto drawdown > 30% — verify hedge layer coverage")
        if portfolio_id in ("P2", "P4") and not greeks.get("has_hedge", True):
            notes.append("CRITICAL: No hedge detected — unprotected on this shock")
        if abs(pct_of_capital) > 10:
            notes.append(f"LARGE IMPACT: {pct_of_capital:.1f}% of capital at risk")

        return ScenarioResult(
            scenario_name=scenario.get("name", ""),
            portfolio_id=portfolio_id,
            delta_pnl=delta_pnl,
            vega_pnl=vega_pnl,
            total_estimated_pnl=total,
            notes=notes,
            data_source="fallback",
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _underlying_shock(self, underlying: str, portfolio_id: str, shocks: dict) -> float:
        """Determine the appropriate shock % for a given underlying."""
        u = (underlying or "").upper()

        if u in {t.upper() for t in self.CRYPTO_TICKERS}:
            return shocks.get("crypto_pct", shocks.get("spx_pct", 0))
        if u in {t.upper() for t in self.RATES_TICKERS}:
            # Rough: rates_bp → TLT move (approx -0.15% per bp for TLT)
            return shocks.get("rates_bp", 0) * -0.0015
        if shocks.get("custom_underlying") and u == shocks["custom_underlying"].upper():
            return shocks.get("custom_pct", 0)

        # Default to SPX shock for equity names
        return shocks.get("spx_pct", 0)

    def _ref_price(self, underlying: str, snap: _LiveSnapshot) -> float:
        """Get a reference price for the underlying from live quotes."""
        # Try to find a quote for the underlying
        for sym, q in snap.quotes.items():
            if (underlying or "").upper() in sym.upper() and q:
                bid = q.get("bid_price") or 0
                ask = q.get("ask_price") or 0
                if bid and ask:
                    return (bid + ask) / 2
                if bid:
                    return bid
                if ask:
                    return ask

        # Fallback reference prices
        u = (underlying or "").upper()
        defaults = {
            "SPX": 5300, "SPY": 530, "QQQ": 450, "IWM": 200,
            "IBIT": 55, "ETHA": 25,
            "TLT": 90, "IEF": 95, "GLD": 220,
            "AAPL": 200, "MSFT": 420, "NVDA": 130,
        }
        return defaults.get(u, 100)

    def _primary_underlying(self, portfolio_id: str) -> Optional[str]:
        """Determine the primary underlying for hedge suggestions."""
        mapping = {"P1": "IBIT", "P2": "SPX", "P3": "TLT", "P4": "SPY"}
        return mapping.get(portfolio_id)

    def _add_portfolio_notes(
        self,
        result: ScenarioResult,
        pid: str,
        shocks: dict,
        capital: float,
    ) -> None:
        pct_of_capital = result.total_estimated_pnl / capital * 100 if capital > 0 else 0

        if pid == "P1" and shocks.get("crypto_pct", 0) < -0.30:
            result.notes.append("WARNING: Crypto drawdown > 30% — verify hedge layer coverage")
        if pid in ("P2", "P4") and not any(
            "hedge" in (p.symbol or "").lower() for p in result.positions
        ):
            result.notes.append("CRITICAL: No hedge detected — unprotected on this shock")
        if abs(pct_of_capital) > 10:
            result.notes.append(f"LARGE IMPACT: {pct_of_capital:.1f}% of capital at risk")

"""
What-If Simulator — models portfolio P&L under shock scenarios
using simple first-order (delta/vega) approximations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


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


@dataclass
class ScenarioResult:
    scenario_name: str
    portfolio_id: str
    delta_pnl: float = 0.0
    vega_pnl: float = 0.0
    total_estimated_pnl: float = 0.0
    notes: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"{self.portfolio_id} | {self.scenario_name}: "
            f"delta_pnl=${self.delta_pnl:,.0f}  vega_pnl=${self.vega_pnl:,.0f}  "
            f"total=${self.total_estimated_pnl:,.0f}"
        )


class WhatIfSimulator:
    """
    First-order P&L approximation under shock scenarios.
    Full re-pricing requires live option marks — this provides directional guidance.
    """

    # Rough multipliers (per unit of underlying move)
    CRYPTO_TICKERS = {"IBIT", "ETHA", "BTC-USD", "ETH-USD"}
    RATES_TICKERS = {"TLT", "IEF", "ZB", "ZN"}

    def run_scenario(
        self,
        scenario_key: str,
        portfolio_greeks_map: dict[str, dict],
        capital_map: dict[str, float],
        custom_scenario: Optional[dict] = None,
    ) -> list[ScenarioResult]:
        """
        portfolio_greeks_map: {portfolio_id: {delta, gamma, theta, vega, ...}}
        Returns one ScenarioResult per portfolio.
        """
        scenario = custom_scenario or SCENARIOS.get(scenario_key)
        if not scenario:
            raise ValueError(f"Unknown scenario: {scenario_key}. Available: {list(SCENARIOS)}")

        shocks = scenario.get("shocks", {})
        results = []

        for pid, greeks in portfolio_greeks_map.items():
            result = self._apply_shocks(pid, greeks, shocks, capital_map.get(pid, 125000), scenario)
            results.append(result)

        return results

    def _apply_shocks(
        self,
        portfolio_id: str,
        greeks: dict,
        shocks: dict,
        capital: float,
        scenario: dict,
    ) -> ScenarioResult:
        delta = greeks.get("delta", 0)
        vega = greeks.get("vega", 0)

        # Map portfolio to primary shock driver
        spx_move = shocks.get("spx_pct", 0)
        vix_move = shocks.get("vix_delta", 0)

        if portfolio_id == "P1":
            # Crypto exposure — use crypto shock
            underlying_move_pct = shocks.get("crypto_pct", spx_move)
            # Approximate underlying move in points (assume $100 ref)
            underlying_move = underlying_move_pct * 100
        elif portfolio_id in ("P2", "P4"):
            # SPX/equity exposure
            underlying_move = spx_move * 5300  # approx SPX level
        elif portfolio_id == "P3":
            # Macro — use rates + smaller equity
            underlying_move = shocks.get("rates_bp", 0) * 0.5  # rough TLT sensitivity
        else:
            underlying_move = spx_move * 5300

        # First-order approximation
        delta_pnl = delta * underlying_move
        # Vega in $/1% vol move; vix_delta is in vol points
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
        )

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

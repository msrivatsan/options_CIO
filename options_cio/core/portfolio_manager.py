"""
Portfolio Manager — loads positions from CSV/DB and provides portfolio state
snapshots for the rules engine, greeks engine, and AI brain.
"""

from __future__ import annotations

import csv
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd


INCOME_TAGS = {"short_put", "jade_lizard_put", "jade_lizard_call", "income_bwb_short"}
HEDGE_TAGS = {"spx_hedge", "spx_structural_hedge", "vix_hedge", "crash_hedge_put"}


class PortfolioManager:
    """
    Loads and manages option positions across all 4 portfolios.
    Positions are loaded from a CSV file (dev/testing) or SQLite DB (production).
    """

    def __init__(self, positions_path: str | Path, db_path: Optional[str] = None) -> None:
        self.positions_path = Path(positions_path)
        self.db_path = db_path
        self._positions_df: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def load(self) -> "PortfolioManager":
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
        return self

    @property
    def positions_df(self) -> pd.DataFrame:
        if self._positions_df is None:
            self.load()
        return self._positions_df  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_positions_for_portfolio(self, portfolio_id: str) -> list[dict]:
        df = self.positions_df[self.positions_df["portfolio"] == portfolio_id]
        return df.to_dict(orient="records")

    def get_all_positions(self) -> list[dict]:
        return self.positions_df.to_dict(orient="records")

    def get_portfolio_ids(self) -> list[str]:
        return sorted(self.positions_df["portfolio"].unique().tolist())

    # ------------------------------------------------------------------
    # State snapshots (used by rules engine and AI)
    # ------------------------------------------------------------------

    def get_portfolio_state(
        self,
        portfolio_id: str,
        price_map: dict[str, float],
        capital_map: dict[str, float],
    ) -> dict:
        positions = self.get_positions_for_portfolio(portfolio_id)
        capital = capital_map.get(portfolio_id, 125000)

        income_positions = [p for p in positions if p.get("structure_tag") in INCOME_TAGS]
        hedge_positions = [p for p in positions if p.get("structure_tag") in HEDGE_TAGS]

        deployed = sum(
            abs(p["entry_price"]) * abs(p["qty"]) * 100
            for p in positions
        )
        deployment_pct = deployed / capital if capital > 0 else 0

        unrealised_pnl = self._estimate_pnl(positions, price_map)

        return {
            "portfolio_id": portfolio_id,
            "position_count": len(positions),
            "deployed_capital": deployed,
            "deployment_pct": deployment_pct,
            "capital": capital,
            "unrealised_pnl": unrealised_pnl,
            "has_income_positions": len(income_positions) > 0,
            "has_hedge": len(hedge_positions) > 0,
            "hedge_count": len(hedge_positions),
            "income_count": len(income_positions),
            "hedge_removed": False,          # updated by adapter
            "deployment_increased": False,   # updated by adapter
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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _estimate_pnl(self, positions: list[dict], price_map: dict[str, float]) -> float:
        """Rough mark-to-market based on entry price vs current underlying move."""
        total = 0.0
        for pos in positions:
            entry = pos.get("entry_price", 0)
            # Without live option marks, we can't compute true PnL here.
            # Return 0 until data feed provides current option prices.
            _ = price_map.get(pos.get("ticker", ""), 0)
        return total

    def deployment_band_check(
        self,
        portfolio_id: str,
        portfolios_config: dict,
        price_map: dict[str, float],
        capital_map: dict[str, float],
    ) -> dict:
        state = self.get_portfolio_state(portfolio_id, price_map, capital_map)
        cfg = portfolios_config["portfolios"].get(portfolio_id, {})
        band = cfg.get("deployment_band", [0, 1])
        target = cfg.get("target_zone", band)
        dep_pct = state["deployment_pct"]
        return {
            "portfolio_id": portfolio_id,
            "deployment_pct": dep_pct,
            "band_low": band[0],
            "band_high": band[1],
            "target_low": target[0],
            "target_high": target[1],
            "in_band": band[0] <= dep_pct <= band[1],
            "in_target": target[0] <= dep_pct <= target[1],
            "status": (
                "TARGET" if target[0] <= dep_pct <= target[1]
                else "IN_BAND" if band[0] <= dep_pct <= band[1]
                else "OVERDEPLOYED" if dep_pct > band[1]
                else "UNDERDEPLOYED"
            ),
        }

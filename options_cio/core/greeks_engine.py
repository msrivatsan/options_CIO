"""
Greeks Engine — computes option Greeks using Black-Scholes (via py_vollib)
and provides portfolio-level aggregation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

import numpy as np

try:
    from py_vollib.black_scholes import black_scholes as bs_price
    from py_vollib.black_scholes.greeks.analytical import (
        delta, gamma, theta, vega, rho
    )
    PY_VOLLIB_AVAILABLE = True
except ImportError:
    PY_VOLLIB_AVAILABLE = False


@dataclass
class OptionGreeks:
    ticker: str
    option_type: str          # 'c' or 'p'
    strike: float
    expiry: date
    qty: int
    underlying_price: float
    iv: float                 # implied vol (annualised, decimal)
    risk_free_rate: float = 0.05

    delta: float = field(init=False, default=0.0)
    gamma: float = field(init=False, default=0.0)
    theta: float = field(init=False, default=0.0)
    vega: float = field(init=False, default=0.0)
    rho: float = field(init=False, default=0.0)
    theo_price: float = field(init=False, default=0.0)

    def __post_init__(self) -> None:
        self._compute()

    @property
    def dte(self) -> float:
        today = date.today()
        return max((self.expiry - today).days, 0)

    @property
    def t(self) -> float:
        """Time to expiry in years."""
        return self.dte / 365.0

    def _compute(self) -> None:
        if not PY_VOLLIB_AVAILABLE or self.t <= 0 or self.iv <= 0:
            return
        flag = self.option_type.lower()[0]
        S = self.underlying_price
        K = self.strike
        t = self.t
        r = self.risk_free_rate
        sigma = self.iv
        try:
            self.theo_price = bs_price(flag, S, K, t, r, sigma)
            self.delta = delta(flag, S, K, t, r, sigma) * self.qty * 100
            self.gamma = gamma(flag, S, K, t, r, sigma) * self.qty * 100
            self.theta = theta(flag, S, K, t, r, sigma) * self.qty * 100
            self.vega = vega(flag, S, K, t, r, sigma) * self.qty * 100
            self.rho = rho(flag, S, K, t, r, sigma) * self.qty * 100
        except Exception:
            pass  # leave defaults on pricing errors


@dataclass
class PortfolioGreeks:
    portfolio_id: str
    positions: list[OptionGreeks] = field(default_factory=list)

    @property
    def net_delta(self) -> float:
        return sum(p.delta for p in self.positions)

    @property
    def net_gamma(self) -> float:
        return sum(p.gamma for p in self.positions)

    @property
    def net_theta(self) -> float:
        return sum(p.theta for p in self.positions)

    @property
    def net_vega(self) -> float:
        return sum(p.vega for p in self.positions)

    def summary(self) -> dict:
        return {
            "portfolio": self.portfolio_id,
            "delta": round(self.net_delta, 4),
            "gamma": round(self.net_gamma, 4),
            "theta": round(self.net_theta, 4),
            "vega": round(self.net_vega, 4),
            "position_count": len(self.positions),
        }


class GreeksEngine:
    """
    Computes per-position and portfolio-level Greeks.
    Accepts a list of position dicts (from portfolio_manager) plus
    a price/IV map, and returns PortfolioGreeks objects.
    """

    def __init__(self, risk_free_rate: float = 0.05) -> None:
        self.risk_free_rate = risk_free_rate

    def compute_position_greeks(
        self,
        ticker: str,
        option_type: str,
        strike: float,
        expiry: date,
        qty: int,
        underlying_price: float,
        iv: float,
    ) -> OptionGreeks:
        return OptionGreeks(
            ticker=ticker,
            option_type=option_type,
            strike=strike,
            expiry=expiry,
            qty=qty,
            underlying_price=underlying_price,
            iv=iv,
            risk_free_rate=self.risk_free_rate,
        )

    def compute_portfolio_greeks(
        self,
        portfolio_id: str,
        positions: list[dict],
        price_map: dict[str, float],
        iv_map: dict[str, float],
    ) -> PortfolioGreeks:
        """
        positions: list of dicts with keys matching active_positions.csv columns.
        price_map: {ticker: current_price}
        iv_map: {ticker: implied_vol}  (annualised decimal, e.g. 0.35)
        """
        greeks_list: list[OptionGreeks] = []
        for pos in positions:
            ticker = pos["ticker"]
            underlying_price = price_map.get(ticker, 0.0)
            iv = iv_map.get(ticker, 0.30)
            if underlying_price <= 0:
                continue
            expiry_date = (
                pos["expiry"] if isinstance(pos["expiry"], date)
                else datetime.strptime(str(pos["expiry"]), "%Y-%m-%d").date()
            )
            g = self.compute_position_greeks(
                ticker=ticker,
                option_type=pos["option_type"],
                strike=float(pos["strike"]),
                expiry=expiry_date,
                qty=int(pos["qty"]),
                underlying_price=underlying_price,
                iv=iv,
            )
            greeks_list.append(g)
        return PortfolioGreeks(portfolio_id=portfolio_id, positions=greeks_list)

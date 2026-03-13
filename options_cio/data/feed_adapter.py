"""
Feed Adapter — abstract base + yfinance implementation for market data.
Fetches current prices, historical volatility, and VIX.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf


class FeedAdapter(ABC):
    """Abstract market data feed."""

    @abstractmethod
    def get_price(self, ticker: str) -> Optional[float]:
        ...

    @abstractmethod
    def get_prices(self, tickers: list[str]) -> dict[str, float]:
        ...

    @abstractmethod
    def get_iv_rank(self, ticker: str, window: int = 252) -> Optional[float]:
        ...

    @abstractmethod
    def get_vix(self) -> Optional[float]:
        ...

    @abstractmethod
    def get_market_snapshot(self) -> dict:
        ...


class YFinanceFeed(FeedAdapter):
    """
    yfinance-backed data feed.
    Uses historical close prices and HV as IV proxy when live IV is unavailable.
    """

    def __init__(self, cache_ttl_seconds: int = 60) -> None:
        self._cache: dict[str, tuple[float, float]] = {}  # ticker -> (value, timestamp)
        self.cache_ttl = cache_ttl_seconds

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_price(self, ticker: str) -> Optional[float]:
        cached = self._get_cached(f"price:{ticker}")
        if cached is not None:
            return cached
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="2d")
            if hist.empty:
                return None
            price = float(hist["Close"].iloc[-1])
            self._set_cache(f"price:{ticker}", price)
            return price
        except Exception:
            return None

    def get_prices(self, tickers: list[str]) -> dict[str, float]:
        result: dict[str, float] = {}
        uncached = [t for t in tickers if self._get_cached(f"price:{t}") is None]
        if uncached:
            try:
                data = yf.download(uncached, period="2d", auto_adjust=True, progress=False)
                if "Close" in data.columns:
                    closes = data["Close"].iloc[-1]
                    for t in uncached:
                        if t in closes:
                            val = float(closes[t])
                            self._set_cache(f"price:{t}", val)
            except Exception:
                pass
        for t in tickers:
            cached = self._get_cached(f"price:{t}")
            if cached is not None:
                result[t] = cached
            else:
                single = self.get_price(t)
                if single is not None:
                    result[t] = single
        return result

    def get_historical_volatility(self, ticker: str, window: int = 30) -> Optional[float]:
        """Returns annualised historical volatility (decimal)."""
        cache_key = f"hv:{ticker}:{window}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="1y")
            if len(hist) < window:
                return None
            log_returns = np.log(hist["Close"] / hist["Close"].shift(1)).dropna()
            hv = float(log_returns.rolling(window).std().iloc[-1]) * np.sqrt(252)
            self._set_cache(cache_key, hv, ttl=3600)
            return hv
        except Exception:
            return None

    def get_iv_rank(self, ticker: str, window: int = 252) -> Optional[float]:
        """
        IV Rank approximation using 52-week HV range.
        Returns 0-100 rank.
        """
        cache_key = f"ivr:{ticker}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="2y")
            if len(hist) < window:
                return None
            log_returns = np.log(hist["Close"] / hist["Close"].shift(1)).dropna()
            rolling_hv = log_returns.rolling(30).std() * np.sqrt(252)
            recent = rolling_hv.iloc[-window:]
            current = float(rolling_hv.iloc[-1])
            hv_min = float(recent.min())
            hv_max = float(recent.max())
            if hv_max == hv_min:
                return 50.0
            ivr = ((current - hv_min) / (hv_max - hv_min)) * 100
            self._set_cache(cache_key, ivr, ttl=3600)
            return ivr
        except Exception:
            return None

    def get_vix(self) -> Optional[float]:
        return self.get_price("^VIX")

    def get_market_snapshot(self) -> dict:
        tickers = {"SPX": "^GSPC", "VIX": "^VIX", "BTC": "BTC-USD", "TNX": "^TNX"}
        snapshot = {"date": str(date.today())}
        for name, sym in tickers.items():
            price = self.get_price(sym)
            snapshot[name.lower()] = f"{price:,.2f}" if price else "N/A"
        snapshot["vix"] = self.get_price("^VIX")
        snapshot["yield_10y"] = (
            f"{self.get_price('^TNX'):.2f}%" if self.get_price("^TNX") else "N/A"
        )
        return snapshot

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _get_cached(self, key: str) -> Optional[float]:
        if key in self._cache:
            val, ts = self._cache[key]
            if time.time() - ts < self.cache_ttl:
                return val
        return None

    def _set_cache(self, key: str, value: float, ttl: Optional[int] = None) -> None:
        self._cache[key] = (value, time.time())
        if ttl is not None:
            # Store with custom TTL by overriding the timestamp offset
            self._cache[key] = (value, time.time() - (self.cache_ttl - ttl))

"""
Feed Adapter — abstract base class + yfinance implementation for market data.
Provides prices, option chains, IV, IV rank/percentile, and VIX.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


class DataFeedAdapter(ABC):
    """Abstract market data feed. All concrete adapters must implement these."""

    @abstractmethod
    def get_price(self, ticker: str) -> float:
        """Return the latest price for a ticker. Raises ValueError if unavailable."""
        ...

    @abstractmethod
    def get_option_chain(self, ticker: str, expiry: str) -> pd.DataFrame:
        """
        Return the option chain for a ticker and expiry date (YYYY-MM-DD).
        Returns a DataFrame with columns: strike, type (call/put), bid, ask,
        last, volume, openInterest, impliedVolatility.
        Returns empty DataFrame if unavailable.
        """
        ...

    @abstractmethod
    def get_iv(
        self, ticker: str, strike: float, expiry: str, option_type: str
    ) -> float:
        """
        Return implied volatility for a specific contract.
        option_type: 'call' or 'put'.
        Raises ValueError if unavailable.
        """
        ...

    @abstractmethod
    def get_vix(self) -> float:
        """Return the current VIX level. Raises ValueError if unavailable."""
        ...

    @abstractmethod
    def get_iv_rank(self, ticker: str, lookback_days: int = 252) -> float:
        """
        IV Rank: where current IV sits relative to its high-low range
        over the lookback period. Returns 0-100.
        Raises ValueError if unavailable.
        """
        ...

    @abstractmethod
    def get_iv_percentile(self, ticker: str, lookback_days: int = 252) -> float:
        """
        IV Percentile: percentage of days in the lookback period where IV
        was lower than today. Returns 0-100.
        Raises ValueError if unavailable.
        """
        ...


# ---------------------------------------------------------------------------
# yfinance implementation
# ---------------------------------------------------------------------------


class YFinanceFeed(DataFeedAdapter):
    """
    yfinance-backed data feed.

    - 15-second cache on price calls to avoid rate limits.
    - Graceful fallback: if option chain unavailable, logs warning and returns empty.
    - Uses HV as IV proxy for IV rank/percentile when live IV history is unavailable.
    """

    PRICE_CACHE_TTL = 15  # seconds
    HV_CACHE_TTL = 3600  # 1 hour for historical vol calculations

    def __init__(self) -> None:
        self._cache: dict[str, tuple[float, float]] = {}  # key -> (value, timestamp)
        self._df_cache: dict[str, tuple[pd.DataFrame, float]] = {}

    # ------------------------------------------------------------------
    # Prices
    # ------------------------------------------------------------------

    def get_price(self, ticker: str) -> float:
        cached = self._get_cached(f"price:{ticker}", self.PRICE_CACHE_TTL)
        if cached is not None:
            return cached
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="5d")
            if hist.empty:
                raise ValueError(f"No price data for {ticker}")
            price = float(hist["Close"].iloc[-1])
            self._set_cached(f"price:{ticker}", price)
            return price
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Failed to fetch price for {ticker}: {e}") from e

    def get_prices(self, tickers: list[str]) -> dict[str, float]:
        """Batch fetch prices. Returns dict of ticker -> price (skips failures)."""
        result: dict[str, float] = {}
        uncached: list[str] = []
        for t in tickers:
            cached = self._get_cached(f"price:{t}", self.PRICE_CACHE_TTL)
            if cached is not None:
                result[t] = cached
            else:
                uncached.append(t)
        if uncached:
            try:
                data = yf.download(
                    uncached, period="5d", auto_adjust=True, progress=False
                )
                if not data.empty:
                    if len(uncached) == 1:
                        # yf.download returns flat columns for single ticker
                        price = float(data["Close"].iloc[-1])
                        self._set_cached(f"price:{uncached[0]}", price)
                        result[uncached[0]] = price
                    else:
                        closes = data["Close"].iloc[-1]
                        for t in uncached:
                            if t in closes and not pd.isna(closes[t]):
                                val = float(closes[t])
                                self._set_cached(f"price:{t}", val)
                                result[t] = val
            except Exception as e:
                logger.warning("Batch download failed: %s", e)

        # Fall back to individual fetch for any remaining
        for t in tickers:
            if t not in result:
                try:
                    result[t] = self.get_price(t)
                except ValueError:
                    logger.warning("Could not fetch price for %s", t)
        return result

    # ------------------------------------------------------------------
    # Option chains & IV
    # ------------------------------------------------------------------

    def get_option_chain(self, ticker: str, expiry: str) -> pd.DataFrame:
        cache_key = f"chain:{ticker}:{expiry}"
        cached_df = self._df_cache.get(cache_key)
        if cached_df and (time.time() - cached_df[1]) < self.PRICE_CACHE_TTL:
            return cached_df[0]
        try:
            t = yf.Ticker(ticker)
            chain = t.option_chain(expiry)
        except Exception as e:
            logger.warning("Option chain unavailable for %s exp %s: %s", ticker, expiry, e)
            return pd.DataFrame()

        frames = []
        for opt_type, df in [("call", chain.calls), ("put", chain.puts)]:
            subset = df[
                ["strike", "bid", "ask", "lastPrice", "volume", "openInterest", "impliedVolatility"]
            ].copy()
            subset = subset.rename(columns={"lastPrice": "last"})
            subset["type"] = opt_type
            frames.append(subset)

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)
        self._df_cache[cache_key] = (result, time.time())
        return result

    def get_iv(
        self, ticker: str, strike: float, expiry: str, option_type: str
    ) -> float:
        chain = self.get_option_chain(ticker, expiry)
        if chain.empty:
            raise ValueError(
                f"No option chain for {ticker} exp {expiry} — cannot get IV"
            )
        match = chain[
            (chain["type"] == option_type)
            & (np.isclose(chain["strike"], strike, atol=0.01))
        ]
        if match.empty:
            raise ValueError(
                f"No contract found: {ticker} {strike} {option_type} exp {expiry}"
            )
        iv = float(match["impliedVolatility"].iloc[0])
        if iv <= 0 or pd.isna(iv):
            raise ValueError(f"IV not available for {ticker} {strike} {option_type}")
        return iv

    # ------------------------------------------------------------------
    # VIX
    # ------------------------------------------------------------------

    def get_vix(self) -> float:
        return self.get_price("^VIX")

    # ------------------------------------------------------------------
    # IV rank & percentile (HV-proxy)
    # ------------------------------------------------------------------

    def _get_rolling_hv_series(
        self, ticker: str, lookback_days: int, window: int = 30
    ) -> pd.Series:
        """Compute rolling 30-day HV over a lookback period."""
        cache_key = f"hv_series:{ticker}:{lookback_days}"
        cached = self._df_cache.get(cache_key)
        if cached and (time.time() - cached[1]) < self.HV_CACHE_TTL:
            return cached[0]

        t = yf.Ticker(ticker)
        # Fetch extra history to have enough data for rolling window
        hist = t.history(period="2y")
        if len(hist) < window + 10:
            raise ValueError(f"Insufficient history for {ticker} IV rank")

        log_ret = np.log(hist["Close"] / hist["Close"].shift(1)).dropna()
        rolling_hv = log_ret.rolling(window).std() * np.sqrt(252)
        rolling_hv = rolling_hv.dropna()

        # Trim to lookback period
        if len(rolling_hv) > lookback_days:
            rolling_hv = rolling_hv.iloc[-lookback_days:]

        self._df_cache[cache_key] = (rolling_hv, time.time())
        return rolling_hv

    def get_iv_rank(self, ticker: str, lookback_days: int = 252) -> float:
        try:
            series = self._get_rolling_hv_series(ticker, lookback_days)
        except Exception as e:
            raise ValueError(f"IV rank unavailable for {ticker}: {e}") from e

        current = float(series.iloc[-1])
        hv_min = float(series.min())
        hv_max = float(series.max())
        if hv_max == hv_min:
            return 50.0
        return round(((current - hv_min) / (hv_max - hv_min)) * 100, 1)

    def get_iv_percentile(self, ticker: str, lookback_days: int = 252) -> float:
        try:
            series = self._get_rolling_hv_series(ticker, lookback_days)
        except Exception as e:
            raise ValueError(f"IV percentile unavailable for {ticker}: {e}") from e

        current = float(series.iloc[-1])
        pct = float((series < current).sum()) / len(series) * 100
        return round(pct, 1)

    # ------------------------------------------------------------------
    # Market snapshot
    # ------------------------------------------------------------------

    def get_market_snapshot(self) -> dict:
        tickers = {"SPX": "^GSPC", "VIX": "^VIX", "BTC": "BTC-USD", "TNX": "^TNX"}
        snapshot: dict = {"date": str(date.today())}
        prices = self.get_prices(list(tickers.values()))
        for name, sym in tickers.items():
            val = prices.get(sym)
            snapshot[name.lower()] = val
        return snapshot

    # ------------------------------------------------------------------
    # Cache internals
    # ------------------------------------------------------------------

    def _get_cached(self, key: str, ttl: float) -> Optional[float]:
        entry = self._cache.get(key)
        if entry and (time.time() - entry[1]) < ttl:
            return entry[0]
        return None

    def _set_cached(self, key: str, value: float) -> None:
        self._cache[key] = (value, time.time())


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_feed(source: str = "yfinance") -> DataFeedAdapter:
    """Return a DataFeedAdapter for the given source name."""
    if source == "tastytrade":
        from options_cio.data.tastytrade_adapter import TastytradeAdapter

        return TastytradeAdapter()
    if source == "yfinance":
        return YFinanceFeed()
    if source == "ibkr":
        from options_cio.data.ibkr_adapter import IBKRFeed

        return IBKRFeed()
    raise ValueError(
        f"Unknown data source: {source!r}. Use 'tastytrade', 'yfinance', or 'ibkr'."
    )

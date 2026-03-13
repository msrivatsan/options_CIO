"""
IBKR Adapter — stub for Interactive Brokers TWS/IB Gateway integration.
Implements DataFeedAdapter using ib_insync (when available).

This is the upgrade path from yfinance. Each method documents which
ib_insync calls would replace the NotImplementedError.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from options_cio.data.feed_adapter import DataFeedAdapter


class IBKRFeed(DataFeedAdapter):
    """
    Interactive Brokers data feed via ib_insync.

    To enable:
      1. pip install ib_insync
      2. Start TWS or IB Gateway
      3. Set data_source: ibkr in settings.yaml
    """

    def __init__(
        self, host: str = "127.0.0.1", port: int = 7497, client_id: int = 1
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self._ib = None
        self._connected = False

    def connect(self) -> bool:
        """Connect to TWS/IB Gateway."""
        try:
            import ib_insync  # type: ignore[import-untyped]

            self._ib = ib_insync.IB()
            self._ib.connect(self.host, self.port, clientId=self.client_id)
            self._connected = True
            return True
        except Exception as e:
            print(f"[IBKR] Connection failed: {e}")
            return False

    def disconnect(self) -> None:
        if self._ib and self._connected:
            self._ib.disconnect()
            self._connected = False

    # ------------------------------------------------------------------
    # DataFeedAdapter interface — stubs
    # ------------------------------------------------------------------

    def get_price(self, ticker: str) -> float:
        # ib_insync approach:
        #   contract = Stock(ticker, 'SMART', 'USD')
        #   self._ib.qualifyContracts(contract)
        #   ticker_data = self._ib.reqMktData(contract, '', False, False)
        #   self._ib.sleep(2)
        #   return ticker_data.marketPrice()
        raise NotImplementedError("IBKR get_price not yet implemented")

    def get_option_chain(self, ticker: str, expiry: str) -> pd.DataFrame:
        # ib_insync approach:
        #   underlying = Stock(ticker, 'SMART', 'USD')
        #   chains = self._ib.reqSecDefOptParams(underlying.symbol, '', ...)
        #   Filter chains for matching expiry, build Option contracts,
        #   qualify and request market data for each strike.
        raise NotImplementedError("IBKR get_option_chain not yet implemented")

    def get_iv(
        self, ticker: str, strike: float, expiry: str, option_type: str
    ) -> float:
        # ib_insync approach:
        #   contract = Option(ticker, expiry, strike, option_type[0].upper(), 'SMART')
        #   self._ib.qualifyContracts(contract)
        #   ticker_data = self._ib.reqMktData(contract, '106', False, False)
        #   self._ib.sleep(2)
        #   return ticker_data.modelGreeks.impliedVol
        raise NotImplementedError("IBKR get_iv not yet implemented")

    def get_vix(self) -> float:
        # ib_insync approach:
        #   contract = Index('VIX', 'CBOE')
        #   self._ib.qualifyContracts(contract)
        #   ticker_data = self._ib.reqMktData(contract, '', False, False)
        #   self._ib.sleep(2)
        #   return ticker_data.marketPrice()
        raise NotImplementedError("IBKR get_vix not yet implemented")

    def get_iv_rank(self, ticker: str, lookback_days: int = 252) -> float:
        # ib_insync approach:
        #   Request historical IV via reqHistoricalData with whatToShow='OPTION_IMPLIED_VOLATILITY'
        #   Compute rank from the time series.
        raise NotImplementedError("IBKR get_iv_rank not yet implemented")

    def get_iv_percentile(self, ticker: str, lookback_days: int = 252) -> float:
        # ib_insync approach:
        #   Same historical IV request as get_iv_rank, but compute percentile
        #   (% of days current IV exceeds).
        raise NotImplementedError("IBKR get_iv_percentile not yet implemented")

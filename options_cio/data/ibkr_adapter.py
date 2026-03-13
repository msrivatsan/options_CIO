"""
IBKR Adapter — stub for Interactive Brokers TWS/IB Gateway integration.
Implements the FeedAdapter interface using ib_insync (when available).
"""

from __future__ import annotations

from typing import Optional

from .feed_adapter import FeedAdapter


class IBKRAdapter(FeedAdapter):
    """
    Interactive Brokers data feed via ib_insync.

    To enable: install ib_insync and ensure TWS/IB Gateway is running.
    Configure host/port in settings.yaml.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 7497, client_id: int = 1) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self._ib = None
        self._connected = False

    def connect(self) -> bool:
        try:
            import ib_insync  # type: ignore[import]
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
    # FeedAdapter interface (stubs — implement when ib_insync is available)
    # ------------------------------------------------------------------

    def get_price(self, ticker: str) -> Optional[float]:
        if not self._connected:
            return None
        raise NotImplementedError("IBKR live price feed not yet implemented.")

    def get_prices(self, tickers: list[str]) -> dict[str, float]:
        raise NotImplementedError("IBKR live prices not yet implemented.")

    def get_iv_rank(self, ticker: str, window: int = 252) -> Optional[float]:
        raise NotImplementedError("IBKR IV rank not yet implemented.")

    def get_vix(self) -> Optional[float]:
        raise NotImplementedError("IBKR VIX not yet implemented.")

    def get_market_snapshot(self) -> dict:
        raise NotImplementedError("IBKR market snapshot not yet implemented.")

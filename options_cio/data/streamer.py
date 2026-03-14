"""
Tastytrade DXLink streamer — real-time quotes and Greeks for option positions.

Uses the DXLinkStreamer websocket from the tastytrade SDK to subscribe to
Quote and Greeks events for all position symbols.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class TastytradeStreamer:
    """
    Real-time quote and Greeks streamer via tastytrade's DXLink websocket.

    Usage (async context):
        async with TastytradeStreamer(session, symbols) as streamer:
            await streamer.run_for(seconds=5)
            greeks = streamer.get_all_greeks()
    """

    def __init__(self, session, symbols: list[str]) -> None:
        """
        Args:
            session: An authenticated tastytrade.Session
            symbols: List of streamer symbols to subscribe to
        """
        self._session = session
        self._symbols = symbols
        self._streamer = None

        # Live data store — updated continuously as events arrive
        self.live_data: dict[str, dict] = {}
        for sym in symbols:
            self.live_data[sym] = {"quote": None, "greeks": None}

    async def __aenter__(self):
        from tastytrade.streamer import DXLinkStreamer

        self._streamer = await DXLinkStreamer.__aenter__(
            DXLinkStreamer(self._session)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._streamer:
            await self._streamer.__aexit__(exc_type, exc_val, exc_tb)

    async def subscribe(self) -> None:
        """Subscribe to Quote and Greeks events for all symbols."""
        from tastytrade.dxfeed import Quote, Greeks

        if not self._symbols:
            logger.warning("No symbols to subscribe to")
            return

        await self._streamer.subscribe(Quote, self._symbols)
        await self._streamer.subscribe(Greeks, self._symbols)
        logger.info("Subscribed to %d symbols for quotes and greeks", len(self._symbols))

    async def _listen_quotes(self) -> None:
        """Listen for Quote events and update live_data."""
        from tastytrade.dxfeed import Quote

        async for event in self._streamer.listen(Quote):
            sym = event.event_symbol
            if sym in self.live_data:
                self.live_data[sym]["quote"] = {
                    "bid_price": float(event.bid_price) if event.bid_price else None,
                    "ask_price": float(event.ask_price) if event.ask_price else None,
                    "bid_size": float(event.bid_size) if event.bid_size else None,
                    "ask_size": float(event.ask_size) if event.ask_size else None,
                }

    async def _listen_greeks(self) -> None:
        """Listen for Greeks events and update live_data."""
        from tastytrade.dxfeed import Greeks

        async for event in self._streamer.listen(Greeks):
            sym = event.event_symbol
            if sym in self.live_data:
                self.live_data[sym]["greeks"] = {
                    "delta": float(event.delta) if event.delta else None,
                    "gamma": float(event.gamma) if event.gamma else None,
                    "theta": float(event.theta) if event.theta else None,
                    "vega": float(event.vega) if event.vega else None,
                    "rho": float(event.rho) if event.rho else None,
                    "volatility": float(event.volatility) if event.volatility else None,
                    "price": float(event.price) if event.price else None,
                }

    async def run_for(self, seconds: float = 5.0) -> None:
        """
        Subscribe and collect data for a fixed duration.
        Useful for one-shot data collection.
        """
        await self.subscribe()

        async def _stop_after():
            await asyncio.sleep(seconds)

        # Run listeners + timeout concurrently; stop when timeout fires
        quote_task = asyncio.create_task(self._listen_quotes())
        greeks_task = asyncio.create_task(self._listen_greeks())
        stop_task = asyncio.create_task(_stop_after())

        await stop_task
        quote_task.cancel()
        greeks_task.cancel()

        # Suppress cancellation errors
        for task in (quote_task, greeks_task):
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def run_continuous(self) -> None:
        """
        Subscribe and listen continuously. Call this in a background task
        to keep live_data updated. Cancel the task to stop.
        """
        await self.subscribe()
        await asyncio.gather(self._listen_quotes(), self._listen_greeks())

    # ------------------------------------------------------------------
    # Data access
    # ------------------------------------------------------------------

    def get_greeks(self, symbol: str) -> Optional[dict]:
        """Return latest greeks for a symbol, or None if not yet received."""
        entry = self.live_data.get(symbol)
        return entry["greeks"] if entry else None

    def get_quote(self, symbol: str) -> Optional[dict]:
        """Return latest quote for a symbol, or None if not yet received."""
        entry = self.live_data.get(symbol)
        return entry["quote"] if entry else None

    def get_all_greeks(self) -> dict[str, Optional[dict]]:
        """Return greeks for all subscribed symbols."""
        return {sym: data["greeks"] for sym, data in self.live_data.items()}

    def get_all_quotes(self) -> dict[str, Optional[dict]]:
        """Return quotes for all subscribed symbols."""
        return {sym: data["quote"] for sym, data in self.live_data.items()}

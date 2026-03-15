"""
Tastytrade DXLink streamer — real-time quotes and Greeks for option positions.

Uses the DXLinkStreamer websocket from the tastytrade SDK to subscribe to
Quote and Greeks events for all position symbols.

Reconnection logic:
  - On disconnect: log, set all greeks to STALE, attempt reconnect every 10s
  - On reconnect: resubscribe to all position symbols automatically
  - If down > 60s: caller should switch to REST-based polling as fallback
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

_RECONNECT_INTERVAL = 10  # seconds between reconnect attempts
_STALE_AFTER_DISCONNECT = 60  # seconds before flagging all data as stale


class TastytradeStreamer:
    """
    Real-time quote and Greeks streamer via tastytrade's DXLink websocket.

    Usage (async context):
        async with TastytradeStreamer(session, symbols) as streamer:
            await streamer.run_for(seconds=5)
            greeks = streamer.get_all_greeks()
    """

    def __init__(self, session, symbols: list[str]) -> None:
        self._session = session
        self._symbols = symbols
        self._streamer = None
        self._connected = False
        self._disconnect_ts: float | None = None
        self._reconnect_count = 0

        # Live data store — updated continuously as events arrive
        self.live_data: dict[str, dict] = {}
        for sym in symbols:
            self.live_data[sym] = {"quote": None, "greeks": None, "updated_at": None}

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def seconds_since_disconnect(self) -> float | None:
        """Seconds since last disconnect, or None if connected."""
        if self._connected or self._disconnect_ts is None:
            return None
        return time.time() - self._disconnect_ts

    async def __aenter__(self):
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._connected = False
        if self._streamer:
            try:
                await self._streamer.__aexit__(exc_type, exc_val, exc_tb)
            except Exception as e:
                logger.debug("Streamer exit error: %s", e)

    async def _connect(self) -> None:
        """Open the DXLink websocket connection."""
        from tastytrade.streamer import DXLinkStreamer

        self._streamer = await DXLinkStreamer.__aenter__(
            DXLinkStreamer(self._session)
        )
        self._connected = True
        self._disconnect_ts = None
        logger.info("DXLink streamer connected.")

    async def _disconnect(self) -> None:
        """Mark as disconnected and record the timestamp."""
        self._connected = False
        self._disconnect_ts = time.time()
        logger.warning("DXLink streamer disconnected.")

    async def subscribe(self) -> None:
        """Subscribe to Quote and Greeks events for all symbols."""
        from tastytrade.dxfeed import Quote, Greeks

        if not self._symbols:
            logger.warning("No symbols to subscribe to")
            return

        await self._streamer.subscribe(Quote, self._symbols)
        await self._streamer.subscribe(Greeks, self._symbols)
        logger.info("Subscribed to %d symbols for quotes and greeks", len(self._symbols))

    def add_symbols(self, new_symbols: list[str]) -> None:
        """Add symbols to track (for position reconciliation).

        Call subscribe() again after adding to actually subscribe on the wire.
        """
        for sym in new_symbols:
            if sym not in self.live_data:
                self.live_data[sym] = {"quote": None, "greeks": None, "updated_at": None}
                self._symbols.append(sym)
                logger.info("Added symbol for streaming: %s", sym)

    def remove_symbols(self, old_symbols: list[str]) -> None:
        """Remove symbols from tracking (closed positions)."""
        for sym in old_symbols:
            if sym in self.live_data:
                del self.live_data[sym]
                if sym in self._symbols:
                    self._symbols.remove(sym)
                logger.info("Removed symbol from streaming: %s", sym)

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
                self.live_data[sym]["updated_at"] = time.time()

    async def _listen_greeks(self) -> None:
        """Listen for Greeks events and update live_data."""
        from tastytrade.dxfeed import Greeks

        async for event in self._streamer.listen(Greeks):
            sym = event.event_symbol
            if sym in self.live_data:
                greeks = {
                    "delta": float(event.delta) if event.delta else None,
                    "gamma": float(event.gamma) if event.gamma else None,
                    "theta": float(event.theta) if event.theta else None,
                    "vega": float(event.vega) if event.vega else None,
                    "rho": float(event.rho) if event.rho else None,
                    "volatility": float(event.volatility) if event.volatility else None,
                    "price": float(event.price) if event.price else None,
                }
                # Greeks sanity check: delta for a single option must be in [-1, 1]
                d = greeks.get("delta")
                if d is not None and (d != d or abs(d) > 1.0):  # NaN or out of range
                    logger.warning("Suspicious delta=%.4f for %s — using previous value", d, sym)
                    prev = self.live_data[sym].get("greeks")
                    if prev:
                        greeks["delta"] = prev.get("delta")
                else:
                    self.live_data[sym]["greeks"] = greeks
                    self.live_data[sym]["updated_at"] = time.time()

    async def run_for(self, seconds: float = 5.0) -> None:
        """Subscribe and collect data for a fixed duration."""
        await self.subscribe()

        async def _stop_after():
            await asyncio.sleep(seconds)

        quote_task = asyncio.create_task(self._listen_quotes())
        greeks_task = asyncio.create_task(self._listen_greeks())
        stop_task = asyncio.create_task(_stop_after())

        await stop_task
        quote_task.cancel()
        greeks_task.cancel()

        for task in (quote_task, greeks_task):
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def run_continuous(self) -> None:
        """Subscribe and listen continuously with automatic reconnection.

        If the streamer disconnects, waits _RECONNECT_INTERVAL seconds then
        attempts to reconnect and resubscribe.  Caller should monitor
        self.connected and self.seconds_since_disconnect.
        """
        while True:
            try:
                if not self._connected:
                    await self._reconnect()

                await self.subscribe()
                self._connected = True
                self._disconnect_ts = None
                logger.info("Streamer running continuously (%d symbols)", len(self._symbols))

                await asyncio.gather(self._listen_quotes(), self._listen_greeks())

            except asyncio.CancelledError:
                logger.info("Streamer cancelled — shutting down.")
                await self._disconnect()
                return

            except Exception as e:
                logger.error("Streamer error: %s — will reconnect in %ds", e, _RECONNECT_INTERVAL)
                await self._disconnect()
                self._reconnect_count += 1

                try:
                    await asyncio.sleep(_RECONNECT_INTERVAL)
                except asyncio.CancelledError:
                    return

    async def _reconnect(self) -> None:
        """Attempt to reconnect the websocket."""
        logger.info("Reconnecting streamer (attempt %d)...", self._reconnect_count + 1)
        try:
            if self._streamer:
                try:
                    await self._streamer.__aexit__(None, None, None)
                except Exception:
                    pass
            await self._connect()
            self._reconnect_count = 0
        except Exception as e:
            logger.error("Reconnection failed: %s", e)
            self._connected = False
            raise

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

    def get_timestamp(self, symbol: str) -> Optional[float]:
        """Return epoch timestamp of last update for a symbol, or None."""
        entry = self.live_data.get(symbol)
        return entry["updated_at"] if entry else None

    def get_all_greeks(self) -> dict[str, Optional[dict]]:
        """Return greeks for all subscribed symbols."""
        return {sym: data["greeks"] for sym, data in self.live_data.items()}

    def get_all_quotes(self) -> dict[str, Optional[dict]]:
        """Return quotes for all subscribed symbols."""
        return {sym: data["quote"] for sym, data in self.live_data.items()}

"""
State Cache — in-memory TTL cache for market data, greeks, and AI outputs.
Prevents redundant API calls within a refresh window.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class CacheEntry:
    value: Any
    timestamp: float = field(default_factory=time.time)
    ttl: float = 60.0  # seconds

    @property
    def is_stale(self) -> bool:
        return (time.time() - self.timestamp) > self.ttl


class StateCache:
    """
    Simple TTL-based in-memory cache.

    Usage:
        cache = StateCache(default_ttl=60)
        cache.set("prices:AAPL", 182.50, ttl=30)
        val = cache.get("prices:AAPL")  # None if expired
    """

    def __init__(self, default_ttl: float = 60.0) -> None:
        self.default_ttl = default_ttl
        self._store: dict[str, CacheEntry] = {}

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        self._store[key] = CacheEntry(
            value=value,
            timestamp=time.time(),
            ttl=ttl if ttl is not None else self.default_ttl,
        )

    def get(self, key: str) -> Optional[Any]:
        entry = self._store.get(key)
        if entry is None or entry.is_stale:
            return None
        return entry.value

    def invalidate(self, key: str) -> None:
        self._store.pop(key, None)

    def invalidate_prefix(self, prefix: str) -> None:
        keys = [k for k in self._store if k.startswith(prefix)]
        for k in keys:
            del self._store[k]

    def clear(self) -> None:
        self._store.clear()

    def stats(self) -> dict:
        total = len(self._store)
        stale = sum(1 for e in self._store.values() if e.is_stale)
        return {"total_entries": total, "stale": stale, "fresh": total - stale}

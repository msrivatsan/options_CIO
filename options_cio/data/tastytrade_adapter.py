# READ-ONLY ADAPTER — No order imports permitted
#
# This module provides read-only access to tastytrade accounts, positions,
# balances, option chains, market metrics, and transaction history.
# It must NEVER import from tastytrade.order or any order-related module.

"""
Tastytrade data adapter — live positions, balances, option chains, and market metrics.

Error handling:
  - Rate limiting: token-bucket limiter (~2 req/s) on all REST calls
  - Retry: 3 attempts with exponential backoff for transient errors (5xx, timeouts)
  - 401: auto-retry session refresh up to 3 times, then raise with re-auth instructions
  - 403: never retry (scope issue), log clearly
  - Timeouts: 10s on all REST calls, fall back to cached value if available
  - Network errors: retry with backoff, flag STALE if all retries fail
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional

import yaml

from options_cio.data.feed_adapter import DataFeedAdapter

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "accounts.yaml"

# Rate limiter: token bucket — max 2 requests/second, burst of 3
_RATE_LIMIT_INTERVAL = 0.5  # seconds between tokens
_RATE_LIMIT_BURST = 3

# Retry configuration
_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 1.0  # seconds
_RETRY_MAX_DELAY = 30.0  # seconds
_REQUEST_TIMEOUT = 10.0  # seconds


def _require_env(name: str) -> str:
    """Return environment variable or raise with clear instructions."""
    value = os.environ.get(name)
    if not value:
        msg = (
            f"Missing {name}. Set it via:\n"
            f"  export {name}='your_value'"
        )
        raise EnvironmentError(msg)
    return value


def _load_account_map() -> dict[str, dict]:
    """Load account -> portfolio mapping from config/accounts.yaml."""
    with open(_CONFIG_PATH) as f:
        data = yaml.safe_load(f)
    mapping = {}
    for entry in data["accounts"]:
        mapping[entry["account_number"]] = {
            "portfolio": entry["portfolio"],
            "name": entry["name"],
        }
    return mapping


class _TokenBucket:
    """Simple token-bucket rate limiter for REST API calls."""

    def __init__(self, rate: float = _RATE_LIMIT_INTERVAL, burst: int = _RATE_LIMIT_BURST):
        self._rate = rate
        self._burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self._burst, self._tokens + elapsed / self._rate)
            self._last_refill = now

            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) * self._rate
                logger.debug("Rate limiter: waiting %.2fs", wait)
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


class TastytradeAdapter(DataFeedAdapter):
    """
    Live tastytrade data feed via the tastyware/tastytrade SDK.

    Provides positions, balances, option chains, market metrics, and
    transaction history. Wraps async SDK methods with sync interfaces.

    Rate-limited to ~2 req/s. Retries transient failures with exponential backoff.
    """

    def __init__(self) -> None:
        import asyncio

        from tastytrade import Session

        client_secret = _require_env("TASTYTRADE_CLIENT_SECRET")
        refresh_token = _require_env("TASTYTRADE_REFRESH_TOKEN")

        self._account_map = _load_account_map()
        self._portfolio_map: dict[str, str] = {}
        for acct_num, info in self._account_map.items():
            self._portfolio_map[info["portfolio"]] = acct_num

        logger.info("Authenticating with tastytrade (OAuth refresh)...")
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self.session = self._create_session(client_secret, refresh_token)
        logger.info("tastytrade session established.")

        self._accounts: dict[str, object] | None = None
        self._event_loop = asyncio.new_event_loop()
        self._rate_limiter = _TokenBucket()

        # Cache for stale-data fallback
        self._cache: dict[str, object] = {}

    @staticmethod
    def _create_session(client_secret: str, refresh_token: str):
        """Create a tastytrade session. Separated for retry logic."""
        from tastytrade import Session
        return Session(
            provider_secret=client_secret,
            refresh_token=refresh_token,
        )

    def _refresh_session(self) -> bool:
        """Attempt to refresh the session. Returns True on success."""
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                logger.info("Refreshing tastytrade session (attempt %d/%d)...",
                            attempt, _MAX_RETRIES)
                self.session = self._create_session(self._client_secret, self._refresh_token)
                self._accounts = None  # force re-fetch
                logger.info("Session refreshed successfully.")
                return True
            except Exception as e:
                logger.warning("Session refresh attempt %d failed: %s", attempt, e)
                if attempt < _MAX_RETRIES:
                    time.sleep(2.0)
        logger.error(
            "All session refresh attempts failed. "
            "Your refresh token may be revoked. "
            "Regenerate it at tastytrade.com -> OAuth Applications -> Manage -> Create Grant"
        )
        return False

    # ------------------------------------------------------------------
    # Retry wrapper
    # ------------------------------------------------------------------

    async def _api_call(self, coro_factory, cache_key: str = ""):
        """Execute an async API call with rate limiting, retries, and error handling.

        Args:
            coro_factory: A callable that returns a new coroutine for each attempt.
            cache_key: Optional key for stale-data fallback cache.
        """
        await self._rate_limiter.acquire()

        last_error = None
        for attempt in range(1, _MAX_RETRIES + 1):
            start = time.monotonic()
            try:
                result = await asyncio.wait_for(
                    coro_factory(), timeout=_REQUEST_TIMEOUT,
                )
                elapsed_ms = (time.monotonic() - start) * 1000
                logger.debug("API call OK (%s) %.0fms", cache_key or "?", elapsed_ms)
                if cache_key:
                    self._cache[cache_key] = result
                return result

            except asyncio.TimeoutError:
                elapsed_ms = (time.monotonic() - start) * 1000
                logger.warning("API timeout (%s) after %.0fms (attempt %d/%d)",
                               cache_key, elapsed_ms, attempt, _MAX_RETRIES)
                last_error = TimeoutError(f"API call timed out after {_REQUEST_TIMEOUT}s")

            except Exception as e:
                elapsed_ms = (time.monotonic() - start) * 1000
                error_str = str(e)

                # 403: never retry (scope issue)
                if "403" in error_str or "Forbidden" in error_str:
                    logger.error("403 Forbidden (%s): %s — not retrying (likely scope issue)",
                                 cache_key, e)
                    raise

                # 401: try session refresh
                if "401" in error_str or "Unauthorized" in error_str:
                    logger.warning("401 Unauthorized (%s) — attempting session refresh", cache_key)
                    refreshed = await self._event_loop.run_in_executor(
                        None, self._refresh_session,
                    ) if False else self._refresh_session()  # sync since we're in the adapter loop
                    if not refreshed:
                        raise RuntimeError(
                            "Authentication failed. Your refresh token may be revoked. "
                            "Regenerate it at tastytrade.com -> OAuth Applications -> Manage -> Create Grant"
                        ) from e
                    last_error = e
                    continue

                # 429: rate limited
                if "429" in error_str or "Too Many" in error_str:
                    delay = min(_RETRY_BASE_DELAY * (2 ** attempt), _RETRY_MAX_DELAY)
                    logger.warning("429 Rate limited (%s) — backing off %.1fs", cache_key, delay)
                    await asyncio.sleep(delay)
                    last_error = e
                    continue

                # 5xx or network error: retry with backoff
                logger.warning("API error (%s) %.0fms attempt %d/%d: %s",
                               cache_key, elapsed_ms, attempt, _MAX_RETRIES, e)
                last_error = e

            if attempt < _MAX_RETRIES:
                delay = min(_RETRY_BASE_DELAY * (2 ** (attempt - 1)), _RETRY_MAX_DELAY)
                await asyncio.sleep(delay)

        # All retries exhausted — try cache
        if cache_key and cache_key in self._cache:
            logger.warning("Using cached data for %s (all retries failed)", cache_key)
            return self._cache[cache_key]

        raise last_error or RuntimeError(f"API call failed after {_MAX_RETRIES} retries")

    # ------------------------------------------------------------------
    # Accounts
    # ------------------------------------------------------------------

    def get_accounts(self) -> dict:
        """Fetch all accounts, mapping portfolio_id -> Account object."""
        return self._event_loop.run_until_complete(self._get_accounts_async())

    async def _get_accounts_async(self) -> dict:
        from tastytrade import Account

        if self._accounts is not None:
            return self._accounts

        all_accounts = await self._api_call(
            lambda: Account.get(self.session),
            cache_key="accounts",
        )
        acct_by_number = {a.account_number: a for a in all_accounts}

        result = {}
        missing = []
        for acct_num, info in self._account_map.items():
            if acct_num in acct_by_number:
                result[info["portfolio"]] = acct_by_number[acct_num]
            else:
                missing.append(f"{info['portfolio']} ({acct_num})")

        if missing:
            logger.warning("Account(s) not found on tastytrade: %s", ", ".join(missing))

        self._accounts = result
        return result

    async def _get_account_async(self, portfolio_id: str):
        accounts = await self._get_accounts_async()
        if portfolio_id not in accounts:
            raise ValueError(f"Portfolio {portfolio_id} not mapped to a tastytrade account")
        return accounts[portfolio_id]

    def _get_account(self, portfolio_id: str):
        return self._event_loop.run_until_complete(self._get_account_async(portfolio_id))

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    def get_positions(self, portfolio_id: str) -> list[dict]:
        """Fetch current positions for a portfolio."""
        return self._event_loop.run_until_complete(self._get_positions_async(portfolio_id))

    async def _get_positions_async(self, portfolio_id: str) -> list[dict]:
        account = await self._get_account_async(portfolio_id)

        async def _fetch():
            return await account.get_positions(self.session, include_marks=True)

        positions = await self._api_call(_fetch, cache_key=f"positions:{portfolio_id}")

        result = []
        for pos in positions:
            entry = {
                "portfolio_id": portfolio_id,
                "symbol": pos.symbol,
                "underlying_symbol": pos.underlying_symbol,
                "instrument_type": str(pos.instrument_type.value),
                "quantity": int(pos.quantity),
                "quantity_direction": pos.quantity_direction,
                "average_open_price": float(pos.average_open_price),
                "close_price": float(pos.close_price),
                "multiplier": pos.multiplier,
                "realized_day_gain": float(pos.realized_day_gain),
                "mark": float(pos.mark) if pos.mark is not None else None,
                "mark_price": float(pos.mark_price) if pos.mark_price is not None else None,
            }
            if pos.instrument_type.value in ("Equity Option", "Future Option"):
                entry.update(_parse_option_symbol(pos.symbol, pos.instrument_type.value))
            result.append(entry)

        return result

    # ------------------------------------------------------------------
    # Balances
    # ------------------------------------------------------------------

    def get_balances(self, portfolio_id: str) -> dict:
        """Fetch account balances for a portfolio."""
        return self._event_loop.run_until_complete(self._get_balances_async(portfolio_id))

    async def _get_balances_async(self, portfolio_id: str) -> dict:
        account = await self._get_account_async(portfolio_id)

        async def _fetch():
            return await account.get_balances(self.session)

        bal = await self._api_call(_fetch, cache_key=f"balances:{portfolio_id}")

        net_liq = float(bal.net_liquidating_value)
        obp = float(bal.derivative_buying_power)
        committed_obp = net_liq - obp
        deployment_pct = (committed_obp / net_liq * 100) if net_liq > 0 else 0.0

        return {
            "portfolio_id": portfolio_id,
            "net_liquidating_value": net_liq,
            "option_buying_power": obp,
            "cash_balance": float(bal.cash_balance),
            "equity_buying_power": float(bal.equity_buying_power),
            "derivative_buying_power": obp,
            "maintenance_requirement": float(bal.maintenance_requirement),
            "committed_obp": committed_obp,
            "deployment_pct": round(deployment_pct, 2),
        }

    def get_system_balances(self) -> dict:
        """Aggregate balances across all 4 portfolio accounts."""
        return self._event_loop.run_until_complete(self._get_system_balances_async())

    async def _get_system_balances_async(self) -> dict:
        accounts = await self._get_accounts_async()
        total_net_liq = 0.0
        total_obp = 0.0
        total_cash = 0.0
        portfolio_balances = {}

        for pid in accounts:
            bal = await self._get_balances_async(pid)
            total_net_liq += bal["net_liquidating_value"]
            total_obp += bal["option_buying_power"]
            total_cash += bal["cash_balance"]
            portfolio_balances[pid] = bal

        committed = total_net_liq - total_obp
        system_deployment = (committed / total_net_liq * 100) if total_net_liq > 0 else 0.0

        return {
            "system_net_liquidating_value": total_net_liq,
            "system_option_buying_power": total_obp,
            "system_cash_balance": total_cash,
            "system_committed_obp": committed,
            "system_deployment_pct": round(system_deployment, 2),
            "portfolios": portfolio_balances,
        }

    # ------------------------------------------------------------------
    # DataFeedAdapter interface (prices, chains, IV, VIX)
    # ------------------------------------------------------------------

    def get_price(self, ticker: str) -> float:
        raise ValueError(
            f"Use get_quote() via the streamer for real-time prices, "
            f"or use YFinanceFeed as fallback for {ticker}"
        )

    def get_option_chain(self, ticker: str, expiry: str = None) -> list[dict]:
        return self._event_loop.run_until_complete(self._get_option_chain_async(ticker, expiry))

    async def _get_option_chain_async(self, ticker: str, expiry: str = None) -> list[dict]:
        from tastytrade.instruments import get_option_chain

        async def _fetch():
            return await get_option_chain(self.session, ticker)

        chain = await self._api_call(_fetch, cache_key=f"chain:{ticker}")
        result = []
        for exp_date, options in chain.items():
            if expiry and str(exp_date) != expiry:
                continue
            for opt in options:
                result.append({
                    "symbol": opt.symbol,
                    "streamer_symbol": opt.streamer_symbol,
                    "underlying_symbol": opt.underlying_symbol,
                    "strike_price": float(opt.strike_price),
                    "expiration_date": str(opt.expiration_date),
                    "option_type": opt.option_type.value,
                    "days_to_expiration": opt.days_to_expiration,
                    "active": opt.active,
                    "is_closing_only": opt.is_closing_only,
                })
        return result

    def get_iv(self, ticker: str, strike: float, expiry: str, option_type: str) -> float:
        metrics = self.get_market_metrics([ticker])
        if ticker in metrics and metrics[ticker].get("iv_index") is not None:
            return metrics[ticker]["iv_index"]
        raise ValueError(f"IV unavailable for {ticker} via tastytrade")

    def get_vix(self) -> float:
        raise ValueError("VIX not available from tastytrade. Use YFinanceFeed fallback.")

    def get_iv_rank(self, ticker: str, lookback_days: int = 252) -> float:
        metrics = self.get_market_metrics([ticker])
        if ticker in metrics:
            rank = metrics[ticker].get("tw_iv_rank")
            if rank is not None:
                return rank
        raise ValueError(f"IV rank unavailable for {ticker}")

    def get_iv_percentile(self, ticker: str, lookback_days: int = 252) -> float:
        metrics = self.get_market_metrics([ticker])
        if ticker in metrics:
            pct = metrics[ticker].get("iv_percentile")
            if pct is not None:
                return pct
        raise ValueError(f"IV percentile unavailable for {ticker}")

    # ------------------------------------------------------------------
    # Market metrics
    # ------------------------------------------------------------------

    def get_market_metrics(self, symbols: list[str]) -> dict:
        return self._event_loop.run_until_complete(self._get_market_metrics_async(symbols))

    async def _get_market_metrics_async(self, symbols: list[str]) -> dict:
        from tastytrade.metrics import get_market_metrics

        async def _fetch():
            return await get_market_metrics(self.session, symbols)

        metrics = await self._api_call(_fetch, cache_key=f"metrics:{','.join(sorted(symbols))}")
        result = {}
        for m in metrics:
            result[m.symbol] = {
                "iv_index": float(m.implied_volatility_index) if m.implied_volatility_index else None,
                "iv_index_5d_change": float(m.implied_volatility_index_5_day_change) if m.implied_volatility_index_5_day_change else None,
                "tw_iv_rank": float(m.tw_implied_volatility_index_rank) if m.tw_implied_volatility_index_rank else None,
                "iv_percentile": float(m.implied_volatility_percentile) if m.implied_volatility_percentile else None,
                "iv_30_day": float(m.implied_volatility_30_day) if m.implied_volatility_30_day else None,
                "hv_30_day": float(m.historical_volatility_30_day) if m.historical_volatility_30_day else None,
                "hv_60_day": float(m.historical_volatility_60_day) if m.historical_volatility_60_day else None,
                "hv_90_day": float(m.historical_volatility_90_day) if m.historical_volatility_90_day else None,
                "iv_hv_diff": float(m.iv_hv_30_day_difference) if m.iv_hv_30_day_difference else None,
                "beta": float(m.beta) if m.beta else None,
                "liquidity_rating": m.liquidity_rating,
                "market_cap": float(m.market_cap) if m.market_cap else None,
                "earnings": {
                    "expected_date": str(m.earnings.expected_report_date) if m.earnings and m.earnings.expected_report_date else None,
                    "time_of_day": m.earnings.time_of_day if m.earnings else None,
                } if m.earnings else None,
            }
        return result

    # ------------------------------------------------------------------
    # Transaction history
    # ------------------------------------------------------------------

    def get_transactions(
        self, portfolio_id: str,
        start_date: date | None = None, end_date: date | None = None,
    ) -> list[dict]:
        return self._event_loop.run_until_complete(
            self._get_transactions_async(portfolio_id, start_date, end_date)
        )

    async def _get_transactions_async(
        self, portfolio_id: str,
        start_date: date | None = None, end_date: date | None = None,
    ) -> list[dict]:
        account = await self._get_account_async(portfolio_id)
        kwargs = {}
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date

        async def _fetch():
            return await account.get_history(self.session, **kwargs)

        transactions = await self._api_call(_fetch, cache_key=f"txns:{portfolio_id}")
        result = []
        for txn in transactions:
            result.append({
                "id": txn.id,
                "portfolio_id": portfolio_id,
                "transaction_type": txn.transaction_type,
                "transaction_sub_type": txn.transaction_sub_type,
                "description": txn.description,
                "executed_at": str(txn.executed_at),
                "transaction_date": str(txn.transaction_date),
                "symbol": txn.symbol,
                "underlying_symbol": txn.underlying_symbol,
                "instrument_type": str(txn.instrument_type.value) if txn.instrument_type else None,
                "action": str(txn.action.value) if txn.action else None,
                "quantity": float(txn.quantity) if txn.quantity else None,
                "price": float(txn.price) if txn.price else None,
                "value": float(txn.value),
                "net_value": float(txn.net_value),
                "commission": float(txn.commission) if txn.commission else None,
                "clearing_fees": float(txn.clearing_fees) if txn.clearing_fees else None,
                "regulatory_fees": float(txn.regulatory_fees) if txn.regulatory_fees else None,
            })
        return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_option_symbol(symbol: str, instrument_type: str) -> dict:
    """
    Extract strike, expiry, and option_type from an OCC-format option symbol.
    E.g. 'AAPL  260320C00200000' -> strike=200.0, expiry=2026-03-20, type=Call
    """
    result = {}
    try:
        if instrument_type == "Equity Option":
            raw = symbol.rstrip()
            rest = raw[-15:]
            yy, mm, dd = rest[0:2], rest[2:4], rest[4:6]
            opt_char = rest[6]
            strike_raw = rest[7:]
            result["expiration_date"] = f"20{yy}-{mm}-{dd}"
            result["option_type"] = "Call" if opt_char == "C" else "Put"
            result["strike_price"] = int(strike_raw) / 1000.0
    except Exception:
        logger.debug("Could not parse option symbol: %s", symbol)
    return result

# READ-ONLY ADAPTER — No order imports permitted
#
# This module provides read-only access to tastytrade accounts, positions,
# balances, option chains, market metrics, and transaction history.
# It must NEVER import from tastytrade.order or any order-related module.

"""
Tastytrade data adapter — live positions, balances, option chains, and market metrics.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Optional

import yaml

from options_cio.data.feed_adapter import DataFeedAdapter

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "accounts.yaml"


def _require_env(name: str) -> str:
    """Return environment variable or exit with clear instructions."""
    value = os.environ.get(name)
    if not value:
        print(
            f"\n[ERROR] Environment variable {name} is not set.\n"
            f"\n"
            f"To authenticate with tastytrade you need:\n"
            f"  TASTYTRADE_CLIENT_SECRET  — your OAuth client secret\n"
            f"  TASTYTRADE_REFRESH_TOKEN  — your OAuth refresh token\n"
            f"\n"
            f"Set them in your environment or .env file before running.\n",
            file=sys.stderr,
        )
        raise EnvironmentError(f"Missing required environment variable: {name}")
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


class TastytradeAdapter(DataFeedAdapter):
    """
    Live tastytrade data feed via the tastyware/tastytrade SDK.

    Provides positions, balances, option chains, market metrics, and
    transaction history. All methods are synchronous (the SDK handles
    async internally for streaming; REST calls are sync).
    """

    def __init__(self) -> None:
        from tastytrade import Session

        client_secret = _require_env("TASTYTRADE_CLIENT_SECRET")
        refresh_token = _require_env("TASTYTRADE_REFRESH_TOKEN")

        self._account_map = _load_account_map()  # acct_number -> {portfolio, name}
        self._portfolio_map: dict[str, str] = {}  # portfolio_id -> acct_number
        for acct_num, info in self._account_map.items():
            self._portfolio_map[info["portfolio"]] = acct_num

        logger.info("Authenticating with tastytrade (OAuth refresh)...")
        self.session = Session(
            provider_secret=client_secret,
            refresh_token=refresh_token,
        )
        logger.info("tastytrade session established.")

        # Cache Account objects after first fetch
        self._accounts: dict[str, object] | None = None

    # ------------------------------------------------------------------
    # Accounts
    # ------------------------------------------------------------------

    def get_accounts(self) -> dict:
        """
        Fetch all accounts and return dict mapping portfolio_id -> Account object.
        Validates that all 4 expected account numbers are present.
        """
        from tastytrade import Account

        if self._accounts is not None:
            return self._accounts

        all_accounts = Account.get(self.session)
        acct_by_number = {a.account_number: a for a in all_accounts}

        result = {}
        missing = []
        for acct_num, info in self._account_map.items():
            if acct_num in acct_by_number:
                result[info["portfolio"]] = acct_by_number[acct_num]
            else:
                missing.append(f"{info['portfolio']} ({acct_num})")

        if missing:
            logger.warning(
                "Account(s) not found on tastytrade: %s", ", ".join(missing)
            )

        self._accounts = result
        return result

    def _get_account(self, portfolio_id: str):
        """Get the Account object for a portfolio, raising if not found."""
        accounts = self.get_accounts()
        if portfolio_id not in accounts:
            raise ValueError(
                f"Portfolio {portfolio_id} not mapped to a tastytrade account"
            )
        return accounts[portfolio_id]

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    def get_positions(self, portfolio_id: str) -> list[dict]:
        """
        Fetch current positions for a portfolio. Returns list of dicts with
        symbol, underlying, instrument_type, quantity, direction, prices,
        and option details where applicable.
        """
        account = self._get_account(portfolio_id)
        positions = account.get_positions(self.session, include_marks=True)

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

            # Parse option details from the OCC symbol if it's an option
            if pos.instrument_type.value in ("Equity Option", "Future Option"):
                entry.update(_parse_option_symbol(pos.symbol, pos.instrument_type.value))

            result.append(entry)

        return result

    # ------------------------------------------------------------------
    # Balances
    # ------------------------------------------------------------------

    def get_balances(self, portfolio_id: str) -> dict:
        """
        Fetch account balances for a portfolio. Returns net_liq, OBP,
        cash_balance, deployment metrics.
        """
        account = self._get_account(portfolio_id)
        bal = account.get_balances(self.session)

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
        accounts = self.get_accounts()
        total_net_liq = 0.0
        total_obp = 0.0
        total_cash = 0.0
        portfolio_balances = {}

        for pid in accounts:
            bal = self.get_balances(pid)
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
        """Get latest price via market metrics endpoint."""
        from tastytrade.metrics import get_market_metrics

        metrics = get_market_metrics(self.session, [ticker])
        if not metrics:
            raise ValueError(f"No market data for {ticker}")
        # Use the IV index as a proxy — for actual price we need a quote
        # Fall back to streaming or raise
        raise ValueError(
            f"Use get_quote() via the streamer for real-time prices, "
            f"or use YFinanceFeed as fallback for {ticker}"
        )

    def get_option_chain(self, ticker: str, expiry: str = None) -> list[dict]:
        """
        Fetch option chain from tastytrade. Returns list of option dicts
        grouped by expiration date.
        """
        from tastytrade.instruments import get_option_chain

        chain = get_option_chain(self.session, ticker)
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

    def get_iv(
        self, ticker: str, strike: float, expiry: str, option_type: str
    ) -> float:
        """Get IV for a specific contract via market metrics."""
        metrics = self.get_market_metrics([ticker])
        if ticker in metrics and metrics[ticker].get("iv_index") is not None:
            return metrics[ticker]["iv_index"]
        raise ValueError(f"IV unavailable for {ticker} via tastytrade")

    def get_vix(self) -> float:
        """VIX not directly available from tastytrade — delegate to fallback."""
        raise ValueError(
            "VIX not available from tastytrade. Use YFinanceFeed fallback."
        )

    def get_iv_rank(self, ticker: str, lookback_days: int = 252) -> float:
        """IV rank from tastytrade market metrics (pre-computed)."""
        metrics = self.get_market_metrics([ticker])
        if ticker in metrics:
            rank = metrics[ticker].get("tw_iv_rank")
            if rank is not None:
                return rank
        raise ValueError(f"IV rank unavailable for {ticker}")

    def get_iv_percentile(self, ticker: str, lookback_days: int = 252) -> float:
        """IV percentile from tastytrade market metrics (pre-computed)."""
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
        """
        Fetch market metrics from tastytrade. Returns dict keyed by symbol with
        IV rank, IV percentile, historical volatility, beta, etc.
        """
        from tastytrade.metrics import get_market_metrics

        metrics = get_market_metrics(self.session, symbols)
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
        self,
        portfolio_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict]:
        """
        Fetch transaction history for a portfolio account.
        Returns list of transaction dicts for journal auto-population.
        """
        account = self._get_account(portfolio_id)
        kwargs = {}
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date

        transactions = account.get_history(self.session, **kwargs)
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
            # OCC format: SYMBOL(padded) YYMMDD C/P SSSSSPPP
            # Find the date portion — last 15 chars: YYMMDDTSSSSSSSS
            raw = symbol.rstrip()
            # Strip underlying (variable length, space-padded to 6)
            rest = raw[-15:]  # YYMMDDC00200000
            yy, mm, dd = rest[0:2], rest[2:4], rest[4:6]
            opt_char = rest[6]  # C or P
            strike_raw = rest[7:]  # 00200000 -> 200.000
            result["expiration_date"] = f"20{yy}-{mm}-{dd}"
            result["option_type"] = "Call" if opt_char == "C" else "Put"
            result["strike_price"] = int(strike_raw) / 1000.0
    except Exception:
        logger.debug("Could not parse option symbol: %s", symbol)
    return result

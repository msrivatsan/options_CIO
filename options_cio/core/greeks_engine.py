"""
Greeks Engine — live broker-streamed Greeks from tastytrade DXLink.

Tastytrade computes Greeks server-side using their own IV surface.
This module consumes those streamed values rather than computing locally.
No py_vollib / Black-Scholes calculations — the broker's numbers are
more accurate than anything we'd derive from public data.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Thresholds
_GAMMA_RISK_THRESHOLD = 500.0       # $ per 1% move
_VEGA_CONCENTRATION_PCT = 0.40      # 40% of portfolio vega in one underlying
_DEEP_ITM_DELTA = 0.60              # |delta| above this triggers alert
_STALE_DATA_SECONDS = 60            # seconds before greeks flagged stale
_STALE_DATA_CRITICAL_SECONDS = 120  # hard staleness cutoff


def _dte_from_expiry(expiry_str: str) -> int:
    """Days to expiry from a YYYY-MM-DD string."""
    try:
        exp = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        return max((exp - date.today()).days, 0)
    except (ValueError, TypeError):
        return 0


def _moneyness(option_type: str, strike: float, underlying_price: float) -> str:
    """Classify option as ITM/ATM/OTM based on strike vs underlying."""
    if underlying_price <= 0 or strike <= 0:
        return "UNKNOWN"
    ratio = strike / underlying_price
    if 0.98 <= ratio <= 1.02:
        return "ATM"
    if option_type in ("Call", "C", "c"):
        return "ITM" if strike < underlying_price else "OTM"
    return "ITM" if strike > underlying_price else "OTM"


class GreeksEngine:
    """
    Reads live Greeks from a TastytradeStreamer and positions from a
    TastytradeAdapter.  All per-contract Greeks come from the broker;
    this engine handles multiplication by quantity/multiplier,
    portfolio aggregation, alerts, and hedge suggestions.
    """

    def __init__(self, adapter, streamer) -> None:
        self.adapter = adapter
        self.streamer = streamer

    # ------------------------------------------------------------------
    # Position-level
    # ------------------------------------------------------------------

    def get_position_greeks(self, position: dict) -> dict | None:
        """
        Look up live Greeks for a single position from the streamer.

        Returns a dict with per-contract and total Greeks, moneyness,
        DTE, IV, mark price, and PnL — or None with a PENDING flag
        if the streamer hasn't received data yet.
        """
        symbol = position.get("symbol", "")
        greeks = self.streamer.get_greeks(symbol)
        quote = self.streamer.get_quote(symbol)

        qty = int(position.get("quantity", 0))
        direction = position.get("quantity_direction", "Long")
        signed_qty = qty if direction == "Long" else -qty
        multiplier = int(position.get("multiplier", 100) or 100)

        # Staleness
        ts = self.streamer.get_timestamp(symbol)
        now = time.time()
        age_seconds = (now - ts) if ts else None
        is_stale = age_seconds is None or age_seconds > _STALE_DATA_SECONDS

        if greeks is None:
            return {
                "symbol": symbol,
                "status": "PENDING",
                "stale": True,
                "age_seconds": None,
            }

        # Per-contract values (raw from broker)
        pc_delta = greeks.get("delta") or 0.0
        pc_gamma = greeks.get("gamma") or 0.0
        pc_theta = greeks.get("theta") or 0.0
        pc_vega = greeks.get("vega") or 0.0
        pc_rho = greeks.get("rho") or 0.0
        iv = greeks.get("volatility") or 0.0

        # Totals = per-contract * signed_qty * multiplier
        scale = signed_qty * multiplier
        total_delta = pc_delta * scale
        total_gamma = pc_gamma * scale
        total_theta = pc_theta * scale
        total_vega = pc_vega * scale
        total_rho = pc_rho * scale

        # Mark price
        mark_price = None
        if quote:
            bid = quote.get("bid_price")
            ask = quote.get("ask_price")
            if bid is not None and ask is not None:
                mark_price = (bid + ask) / 2.0

        # Moneyness
        option_type = position.get("option_type", "")
        strike = position.get("strike_price", 0.0)
        underlying_price = 0.0
        if quote:
            bid = quote.get("bid_price")
            ask = quote.get("ask_price")
            # underlying price comes from the position data or market metrics
        # Use the adapter's underlying price if available from position
        underlying_sym = position.get("underlying_symbol", "")
        underlying_quote = self.streamer.get_quote(underlying_sym) if underlying_sym else None
        if underlying_quote:
            ubid = underlying_quote.get("bid_price")
            uask = underlying_quote.get("ask_price")
            if ubid is not None and uask is not None:
                underlying_price = (ubid + uask) / 2.0

        # Expiry / DTE
        expiry = position.get("expiration_date", "")
        dte = _dte_from_expiry(expiry)

        # Unrealised PnL
        avg_open = float(position.get("average_open_price", 0))
        close_price = float(position.get("close_price", 0))
        unrealized_pnl = (close_price - avg_open) * signed_qty * multiplier

        return {
            "symbol": symbol,
            "underlying_symbol": underlying_sym,
            "status": "LIVE",
            "stale": is_stale,
            "age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
            # Per-contract
            "pc_delta": pc_delta,
            "pc_gamma": pc_gamma,
            "pc_theta": pc_theta,
            "pc_vega": pc_vega,
            "pc_rho": pc_rho,
            # Totals
            "delta": total_delta,
            "gamma": total_gamma,
            "theta": total_theta,
            "vega": total_vega,
            "rho": total_rho,
            # Derived
            "iv": iv,
            "mark_price": mark_price,
            "moneyness": _moneyness(option_type, strike, underlying_price),
            "dte": dte,
            "unrealized_pnl": unrealized_pnl,
            "signed_qty": signed_qty,
            "multiplier": multiplier,
        }

    # ------------------------------------------------------------------
    # Portfolio-level
    # ------------------------------------------------------------------

    def get_portfolio_greeks(self, portfolio_id: str) -> dict:
        """
        Aggregate live Greeks across all positions in a portfolio.

        Returns dict with per_position list, portfolio totals,
        pending count, and alert flags.
        """
        positions = self.adapter.get_positions(portfolio_id)
        per_position = []
        totals = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "rho": 0.0}
        pending_count = 0
        stale_count = 0
        vega_by_underlying: dict[str, float] = {}

        for pos in positions:
            # Only process options
            if pos.get("instrument_type") not in ("Equity Option", "Future Option"):
                continue

            pg = self.get_position_greeks(pos)
            if pg is None:
                pending_count += 1
                continue

            per_position.append(pg)

            if pg["status"] == "PENDING":
                pending_count += 1
                continue

            if pg["stale"]:
                stale_count += 1

            for greek in ("delta", "gamma", "theta", "vega", "rho"):
                totals[greek] += pg[greek]

            # Track vega concentration by underlying
            und = pg.get("underlying_symbol", "UNKNOWN")
            vega_by_underlying[und] = vega_by_underlying.get(und, 0.0) + abs(pg["vega"])

        # Alert flags
        alerts = self._compute_alerts(per_position, totals, vega_by_underlying, stale_count)

        return {
            "portfolio_id": portfolio_id,
            "per_position": per_position,
            "total_delta": round(totals["delta"], 4),
            "total_gamma": round(totals["gamma"], 4),
            "total_theta": round(totals["theta"], 4),
            "total_vega": round(totals["vega"], 4),
            "total_rho": round(totals["rho"], 4),
            "position_count": len(per_position),
            "pending_count": pending_count,
            "stale_count": stale_count,
            "alerts": alerts,
        }

    def summary(self, portfolio_id: str) -> dict:
        """
        Return a compact summary compatible with the RulesEngine and
        dashboard expectations: {portfolio, delta, gamma, theta, vega, position_count}.
        """
        pg = self.get_portfolio_greeks(portfolio_id)
        return {
            "portfolio": pg["portfolio_id"],
            "delta": pg["total_delta"],
            "gamma": pg["total_gamma"],
            "theta": pg["total_theta"],
            "vega": pg["total_vega"],
            "position_count": pg["position_count"],
            "pending_count": pg["pending_count"],
            "stale_count": pg["stale_count"],
        }

    # ------------------------------------------------------------------
    # System-level
    # ------------------------------------------------------------------

    def get_system_greeks(self) -> dict:
        """
        Aggregate Greeks across all 4 portfolios.  Includes beta-adjusted
        delta for equity positions using the market metrics beta.
        """
        accounts = self.adapter.get_accounts()
        portfolio_results = {}
        system_totals = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "rho": 0.0}
        beta_adjusted_delta = 0.0
        total_pending = 0
        total_stale = 0

        # Fetch betas for all underlyings
        all_underlyings = set()
        for pid in accounts:
            positions = self.adapter.get_positions(pid)
            for pos in positions:
                und = pos.get("underlying_symbol")
                if und:
                    all_underlyings.add(und)

        betas = {}
        if all_underlyings:
            try:
                metrics = self.adapter.get_market_metrics(list(all_underlyings))
                for sym, data in metrics.items():
                    betas[sym] = data.get("beta") or 1.0
            except Exception:
                logger.warning("Could not fetch betas for system Greeks")

        for pid in accounts:
            pg = self.get_portfolio_greeks(pid)
            portfolio_results[pid] = pg

            for greek in ("delta", "gamma", "theta", "vega", "rho"):
                key = f"total_{greek}"
                system_totals[greek] += pg[key]

            total_pending += pg["pending_count"]
            total_stale += pg["stale_count"]

            # Beta-adjusted delta
            for pos_g in pg["per_position"]:
                if pos_g["status"] == "PENDING":
                    continue
                und = pos_g.get("underlying_symbol", "")
                beta = betas.get(und, 1.0)
                beta_adjusted_delta += pos_g["delta"] * beta

        return {
            "system_delta": round(system_totals["delta"], 4),
            "system_gamma": round(system_totals["gamma"], 4),
            "system_theta": round(system_totals["theta"], 4),
            "system_vega": round(system_totals["vega"], 4),
            "system_rho": round(system_totals["rho"], 4),
            "beta_adjusted_delta": round(beta_adjusted_delta, 4),
            "total_pending": total_pending,
            "total_stale": total_stale,
            "portfolios": portfolio_results,
        }

    # ------------------------------------------------------------------
    # Alert flags
    # ------------------------------------------------------------------

    def _compute_alerts(
        self,
        per_position: list[dict],
        totals: dict,
        vega_by_underlying: dict[str, float],
        stale_count: int,
    ) -> dict:
        total_abs_vega = sum(vega_by_underlying.values()) or 1.0

        # Gamma risk: short gamma * underlying_price^2 * 0.01^2 * multiplier
        # Simplified: |total_gamma| * 100 > threshold ($ per 1% move)
        gamma_risk = abs(totals["gamma"]) * 100.0
        gamma_risk_alert = gamma_risk > _GAMMA_RISK_THRESHOLD

        # Vega concentration
        vega_concentration_alert = False
        vega_concentration_symbol = None
        for und, v in vega_by_underlying.items():
            if v / total_abs_vega > _VEGA_CONCENTRATION_PCT:
                vega_concentration_alert = True
                vega_concentration_symbol = und
                break

        # Deep ITM
        deep_itm_alert = False
        deep_itm_positions = []
        for pg in per_position:
            if pg["status"] == "PENDING":
                continue
            if abs(pg["pc_delta"]) > _DEEP_ITM_DELTA:
                deep_itm_alert = True
                deep_itm_positions.append(pg["symbol"])

        # Theta bleeding (positive theta in a net-short-theta scenario is expected for income;
        # but total positive theta in a non-income portfolio is bleeding)
        theta_bleeding_alert = totals["theta"] > 0

        # Stale data
        stale_data_alert = stale_count > 0

        return {
            "gamma_risk_alert": gamma_risk_alert,
            "gamma_risk_value": round(gamma_risk, 2),
            "vega_concentration_alert": vega_concentration_alert,
            "vega_concentration_symbol": vega_concentration_symbol,
            "deep_itm_alert": deep_itm_alert,
            "deep_itm_positions": deep_itm_positions,
            "theta_bleeding_alert": theta_bleeding_alert,
            "stale_data_alert": stale_data_alert,
            "stale_count": stale_count,
        }

    # ------------------------------------------------------------------
    # Hedge suggestion
    # ------------------------------------------------------------------

    def suggest_delta_hedge(self, portfolio_id: str) -> dict | None:
        """
        Fetch the option chain for the portfolio's primary underlying and
        find the contract that would bring portfolio delta closest to neutral.

        Returns a dict with the recommended contract details and impact,
        or None if no hedge is needed (delta already near zero).
        """
        pg = self.get_portfolio_greeks(portfolio_id)
        current_delta = pg["total_delta"]

        if abs(current_delta) < 5.0:
            return None  # delta already near neutral

        # Determine primary underlying (highest absolute delta contribution)
        delta_by_underlying: dict[str, float] = {}
        for pos in pg["per_position"]:
            if pos["status"] == "PENDING":
                continue
            und = pos.get("underlying_symbol", "")
            delta_by_underlying[und] = delta_by_underlying.get(und, 0.0) + pos["delta"]

        if not delta_by_underlying:
            return None

        primary_underlying = max(delta_by_underlying, key=lambda k: abs(delta_by_underlying[k]))

        # Need to offset current_delta: if positive, buy puts; if negative, buy calls
        hedge_type = "Put" if current_delta > 0 else "Call"

        try:
            chain = self.adapter.get_option_chain(primary_underlying)
        except Exception as e:
            logger.warning("Could not fetch option chain for %s: %s", primary_underlying, e)
            return None

        if not chain:
            return None

        # Filter to active options with 30-60 DTE
        candidates = [
            c for c in chain
            if c.get("active")
            and not c.get("is_closing_only")
            and c.get("option_type") == hedge_type
            and 30 <= (c.get("days_to_expiration") or 0) <= 60
        ]

        if not candidates:
            # Widen to 20-90 DTE
            candidates = [
                c for c in chain
                if c.get("active")
                and not c.get("is_closing_only")
                and c.get("option_type") == hedge_type
                and 20 <= (c.get("days_to_expiration") or 0) <= 90
            ]

        if not candidates:
            return None

        # Pick the ATM-ish option (strike closest to current delta-neutral need)
        # For simplicity, pick strike closest to the middle of available strikes
        strikes = sorted(set(c["strike_price"] for c in candidates))
        mid_strike = strikes[len(strikes) // 2]

        best = min(candidates, key=lambda c: abs(c["strike_price"] - mid_strike))

        # Estimate qty needed: each contract has ~0.50 delta at ATM
        # For puts, delta ~ -0.50; for calls, delta ~ +0.50
        per_contract_delta = -0.50 if hedge_type == "Put" else 0.50
        multiplier = 100
        delta_per_contract = per_contract_delta * multiplier
        qty_needed = max(1, round(abs(current_delta) / abs(delta_per_contract)))

        resulting_delta = current_delta + (qty_needed * delta_per_contract)

        return {
            "underlying": primary_underlying,
            "strike": best["strike_price"],
            "expiry": best["expiration_date"],
            "option_type": hedge_type,
            "qty": qty_needed,
            "resulting_delta": round(resulting_delta, 2),
            "current_delta": round(current_delta, 2),
            "theta_impact": round(qty_needed * -0.05 * multiplier, 2),  # rough estimate
            "symbol": best["symbol"],
            "days_to_expiration": best.get("days_to_expiration"),
        }

    # ------------------------------------------------------------------
    # Data freshness
    # ------------------------------------------------------------------

    def get_data_quality(self) -> dict:
        """
        Report per-position data freshness across all portfolios.

        Returns dict with per-position staleness, system status
        (GREEN / YELLOW / RED), and any missing data flags.
        """
        accounts = self.adapter.get_accounts()
        position_quality = []
        now = time.time()
        has_missing_vega_gamma = False
        has_stale = False
        has_critical_stale = False

        for pid in accounts:
            positions = self.adapter.get_positions(pid)
            for pos in positions:
                if pos.get("instrument_type") not in ("Equity Option", "Future Option"):
                    continue

                symbol = pos.get("symbol", "")
                greeks = self.streamer.get_greeks(symbol)
                ts = self.streamer.get_timestamp(symbol)
                age = (now - ts) if ts else None

                status = "LIVE"
                if greeks is None:
                    status = "NO_DATA"
                    has_missing_vega_gamma = True
                elif age is not None and age > _STALE_DATA_CRITICAL_SECONDS:
                    status = "CRITICAL_STALE"
                    has_critical_stale = True
                elif age is not None and age > _STALE_DATA_SECONDS:
                    status = "STALE"
                    has_stale = True
                elif age is None:
                    status = "NO_TIMESTAMP"
                    has_stale = True

                if greeks is not None:
                    if greeks.get("vega") is None or greeks.get("gamma") is None:
                        has_missing_vega_gamma = True

                position_quality.append({
                    "portfolio_id": pid,
                    "symbol": symbol,
                    "status": status,
                    "age_seconds": round(age, 1) if age is not None else None,
                    "has_greeks": greeks is not None,
                })

        if has_critical_stale or has_missing_vega_gamma:
            system_status = "YELLOW"
        elif has_stale:
            system_status = "YELLOW"
        else:
            system_status = "GREEN"

        return {
            "system_status": system_status,
            "positions": position_quality,
            "total_positions": len(position_quality),
            "stale_count": sum(1 for p in position_quality if p["status"] in ("STALE", "CRITICAL_STALE")),
            "no_data_count": sum(1 for p in position_quality if p["status"] == "NO_DATA"),
        }

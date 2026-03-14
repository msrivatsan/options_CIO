"""
Rules Engine — evaluates trading_rules.json against live tastytrade data.

Pulls positions, balances, and greeks from the TastytradeAdapter and
GreeksEngine rather than CSV files.  Includes data quality checks,
position classification, and hedge effectiveness analysis using live data.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Staleness threshold — data older than this triggers YELLOW state
_STALE_DATA_SECONDS = 120

# Position role classification tags
INCOME_ROLES = {
    "income_short_put",
    "income_strangle",
    "jade_lizard_put",
    "jade_lizard_call",
    "income_bwb_short",
    "income_covered_call",
}
HEDGE_ROLES = {
    "spx_structural_hedge",
    "vix_convex_hedge",
    "crash_hedge_put",
    "put_backspread_hedge",
    "long_put_hedge",
}
CONVEX_ROLES = {"convex_long_leap", "long_leap_call", "long_leap_put"}
CALENDAR_ROLES = {"calendar_long_leg", "calendar_short_leg", "diagonal_long_leg", "diagonal_short_leg"}


class Severity(str, Enum):
    INFO = "INFO"
    WARN = "WARN"
    CRITICAL = "CRITICAL"


class SystemState(str, Enum):
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    RED = "RED"


@dataclass
class RuleAlert:
    rule_id: str
    severity: Severity
    message: str
    portfolio: str = ""
    ticker: str = ""
    value: float = 0.0
    threshold: float = 0.0

    def __str__(self) -> str:
        loc = f"[{self.portfolio}/{self.ticker}]" if self.ticker else f"[{self.portfolio}]"
        return f"[{self.severity}] {loc} {self.rule_id}: {self.message} (value={self.value:.4f}, threshold={self.threshold})"


@dataclass
class RulesResult:
    """Complete result from a rules evaluation pass."""
    alerts: list[RuleAlert] = field(default_factory=list)
    system_state: SystemState = SystemState.GREEN
    data_quality_ok: bool = True
    data_quality_message: str = ""
    compliance_score: float = 1.0


class RulesEngine:
    """
    Evaluates trading rules against live tastytrade data.

    Reads positions and balances from the TastytradeAdapter, greeks from
    the GreeksEngine (DXLink streamer), and market metrics from the
    tastytrade API.  No CSV dependency.
    """

    def __init__(
        self,
        rules_path: str | Path,
        portfolios_config: dict,
        adapter: object,
        greeks_engine: Optional[object] = None,
    ) -> None:
        with open(rules_path) as f:
            data = json.load(f)
        self.rules: list[dict] = data.get("rules", [])
        self.system_rules: list[dict] = data.get("system_rules", [])
        self.portfolios_config = portfolios_config.get("portfolios", portfolios_config)
        self.adapter = adapter
        self.greeks_engine = greeks_engine

        # Track system state
        self._system_state = SystemState.GREEN
        self._violation_log: list[dict] = []

    @property
    def system_state(self) -> SystemState:
        return self._system_state

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate_all(self) -> RulesResult:
        """
        Full rules evaluation pass across all portfolios using live data.

        1. Check data quality (staleness, streamer connectivity)
        2. Fetch live positions, balances, greeks per portfolio
        3. Classify positions by role
        4. Run position-level and portfolio-level rules
        5. Run system-level rules (hedging, deployment, vol guardrails)
        6. Compute compliance score and escalate system state
        """
        result = RulesResult()

        # Step 1: Data quality check
        dq_alert = self._check_data_quality()
        if dq_alert:
            result.alerts.append(dq_alert)
            result.data_quality_ok = False
            result.data_quality_message = dq_alert.message
            self._system_state = SystemState.YELLOW
            logger.warning("STALE DATA — rules evaluation may be inaccurate")

        # Step 2-4: Per-portfolio evaluation
        accounts = self.adapter.get_accounts()
        all_portfolio_states: list[dict] = []

        for portfolio_id in accounts:
            try:
                portfolio_alerts, state = self._evaluate_portfolio_live(portfolio_id)
                result.alerts.extend(portfolio_alerts)
                all_portfolio_states.append(state)
            except Exception as e:
                logger.error("Rules evaluation failed for %s: %s", portfolio_id, e)
                result.alerts.append(RuleAlert(
                    rule_id="evaluation_error",
                    severity=Severity.WARN,
                    portfolio=portfolio_id,
                    message=f"Could not evaluate rules: {e}",
                ))

        # Step 5: System-level rules
        system_alerts = self.evaluate_system(all_portfolio_states)
        result.alerts.extend(system_alerts)

        # Step 6: State escalation and compliance
        result.system_state = self._compute_system_state(result.alerts)
        self._system_state = result.system_state
        result.compliance_score = self._compute_compliance_score(result.alerts)

        return result

    def evaluate_portfolio(
        self,
        portfolio_id: str,
        positions: list[dict],
        greeks_summary: dict,
        capital: float,
    ) -> list[RuleAlert]:
        """
        Evaluate rules for a single portfolio.

        Accepts pre-fetched positions and greeks for backward compatibility
        with callers that manage their own data fetching.
        """
        alerts: list[RuleAlert] = []
        for rule in self.rules:
            rule_alerts = self._eval_rule(rule, portfolio_id, positions, greeks_summary, capital)
            alerts.extend(rule_alerts)
        return alerts

    def evaluate_system(
        self,
        all_portfolio_states: list[dict],
    ) -> list[RuleAlert]:
        """Check system-wide rules across all portfolios."""
        alerts: list[RuleAlert] = []
        for rule in self.system_rules:
            alert = self._eval_system_rule(rule, all_portfolio_states)
            if alert:
                alerts.append(alert)
        return alerts

    # ------------------------------------------------------------------
    # Data quality check
    # ------------------------------------------------------------------

    def _check_data_quality(self) -> RuleAlert | None:
        """
        Verify that all position data is fresh before evaluating rules.

        If the streamer is disconnected or data is stale > 2 minutes,
        returns a WARN alert and the system enters YELLOW state.
        """
        if self.greeks_engine is None:
            return RuleAlert(
                rule_id="data_quality_check",
                severity=Severity.WARN,
                message="No GreeksEngine available — evaluating with stale/missing greeks data",
            )

        try:
            dq = self.greeks_engine.get_data_quality()
        except Exception as e:
            return RuleAlert(
                rule_id="data_quality_check",
                severity=Severity.WARN,
                message=f"Data quality check failed: {e}",
            )

        if dq["system_status"] != "GREEN":
            stale = dq.get("stale_count", 0)
            no_data = dq.get("no_data_count", 0)
            total = dq.get("total_positions", 0)
            return RuleAlert(
                rule_id="data_quality_check",
                severity=Severity.WARN,
                message=(
                    f"STALE DATA — {stale} stale, {no_data} missing out of "
                    f"{total} positions. Rules evaluation may be inaccurate."
                ),
                value=stale + no_data,
                threshold=0,
            )

        return None

    # ------------------------------------------------------------------
    # Live portfolio evaluation
    # ------------------------------------------------------------------

    def _evaluate_portfolio_live(self, portfolio_id: str) -> tuple[list[RuleAlert], dict]:
        """
        Evaluate all rules for a portfolio using live tastytrade data.

        Returns (alerts, portfolio_state_dict).
        """
        # Fetch live data
        positions = self.adapter.get_positions(portfolio_id)
        balances = self.adapter.get_balances(portfolio_id)

        # Live greeks from streamer
        if self.greeks_engine is not None:
            greeks_summary = self.greeks_engine.summary(portfolio_id)
        else:
            greeks_summary = {
                "portfolio": portfolio_id, "delta": 0.0, "gamma": 0.0,
                "theta": 0.0, "vega": 0.0, "position_count": 0,
                "pending_count": 0, "stale_count": 0,
            }

        # Classify positions by role
        classified = self._classify_positions(portfolio_id, positions)

        # Live deployment and capital from broker
        net_liq = balances["net_liquidating_value"]
        obp = balances["option_buying_power"]
        deployment_pct = balances["deployment_pct"]

        # Portfolio config
        p_config = self.portfolios_config.get(portfolio_id, {})
        deploy_band = p_config.get("deployment_band", [0, 1.0])

        # Build portfolio state for system rules
        income_positions = [p for p in classified if p.get("role") in INCOME_ROLES]
        hedge_positions = [p for p in classified if p.get("role") in HEDGE_ROLES]

        state = {
            "portfolio_id": portfolio_id,
            "net_liq": net_liq,
            "option_buying_power": obp,
            "deployment_pct": deployment_pct / 100.0,  # normalize to 0-1
            "has_income_positions": len(income_positions) > 0,
            "has_hedge": len(hedge_positions) > 0,
            "hedge_count": len(hedge_positions),
            "income_count": len(income_positions),
            "hedge_removed": False,
            "deployment_increased": False,
        }

        # Run rules with live data
        alerts: list[RuleAlert] = []

        # Position-level and portfolio-level rules
        for rule in self.rules:
            rule_alerts = self._eval_rule(
                rule, portfolio_id, classified, greeks_summary, net_liq,
            )
            alerts.extend(rule_alerts)

        # Deployment band check
        deploy_alert = self._check_deployment_band(
            portfolio_id, deployment_pct, deploy_band,
        )
        if deploy_alert:
            alerts.append(deploy_alert)

        # Mandate-specific rules
        mandate_alerts = self._check_mandate_rules(
            portfolio_id, classified, p_config,
        )
        alerts.extend(mandate_alerts)

        # Hedge effectiveness (live greeks)
        hedge_alerts = self._check_hedge_effectiveness(
            portfolio_id, classified, p_config,
        )
        alerts.extend(hedge_alerts)

        return alerts, state

    # ------------------------------------------------------------------
    # Position classification
    # ------------------------------------------------------------------

    def _classify_positions(
        self, portfolio_id: str, positions: list[dict],
    ) -> list[dict]:
        """
        Classify each tastytrade position's ROLE based on portfolio mandate,
        instrument type, direction, and allowed structures.

        Mutates positions in-place by adding 'role' and 'dte' fields.
        """
        p_config = self.portfolios_config.get(portfolio_id, {})
        allowed_structures = p_config.get("allowed_structures", [])

        for pos in positions:
            pos["role"] = self._infer_role(portfolio_id, pos, p_config)
            # Compute DTE from expiration_date if present
            expiry = pos.get("expiration_date", "")
            if expiry:
                pos["dte"] = self._dte_from_expiry(expiry)
            else:
                pos["dte"] = 999  # non-option or unknown

            # Normalize ticker field for alert display
            pos["ticker"] = pos.get("underlying_symbol", pos.get("symbol", ""))

        return positions

    def _infer_role(self, portfolio_id: str, pos: dict, p_config: dict) -> str:
        """
        Infer position role from portfolio context and position attributes.

        Classification logic by portfolio:
        - P1: all long options = convex_long_leap
        - P2: long options = hedge, short options = income_bwb_short
        - P3: classify by DTE split (calendar/diagonal legs)
        - P4: short puts/strangles = income, long puts on SPX/VIX = hedge
        """
        inst_type = pos.get("instrument_type", "")
        direction = pos.get("quantity_direction", "Long")
        underlying = pos.get("underlying_symbol", "")
        option_type = pos.get("option_type", "")

        is_option = inst_type in ("Equity Option", "Future Option")
        is_long = direction == "Long"
        is_short = not is_long

        if not is_option:
            return "equity_position"

        if portfolio_id == "P1":
            # P1: Crypto Convexity — all long options are LEAPS
            if is_long:
                return "convex_long_leap"
            return "unknown_short"  # shouldn't exist in P1

        elif portfolio_id == "P2":
            # P2: Hedged Index Income — BWBs + structural hedge
            if is_long and option_type == "Put":
                return "spx_structural_hedge"
            if is_short:
                return "income_bwb_short"
            if is_long and option_type == "Call":
                return "income_bwb_short"  # long call wing of BWB
            return "income_bwb_short"

        elif portfolio_id == "P3":
            # P3: Macro Stability — calendars/diagonals
            expiry = pos.get("expiration_date", "")
            dte = self._dte_from_expiry(expiry) if expiry else 0
            if is_long:
                if dte >= 60:
                    return "calendar_long_leg"
                return "diagonal_long_leg"
            else:
                if dte <= 45:
                    return "calendar_short_leg"
                return "diagonal_short_leg"

        elif portfolio_id == "P4":
            # P4: Hedged Equity Income
            hedge_instruments = p_config.get("hedge_instruments", ["SPX", "VIX"])
            if underlying in hedge_instruments:
                if is_long and option_type == "Put":
                    return "spx_structural_hedge"
                if is_long and option_type == "Call" and underlying == "VIX":
                    return "vix_convex_hedge"

            if is_short and option_type == "Put":
                return "income_short_put"
            if is_short and option_type == "Call":
                return "jade_lizard_call"
            if is_long and option_type == "Put":
                return "long_put_hedge"
            return "income_strangle"

        return "unclassified"

    # ------------------------------------------------------------------
    # Rule evaluation (position-level and portfolio-level)
    # ------------------------------------------------------------------

    def _eval_rule(
        self,
        rule: dict,
        portfolio_id: str,
        positions: list[dict],
        greeks_summary: dict,
        capital: float,
    ) -> list[RuleAlert]:
        """Evaluate a single rule, returning zero or more alerts."""
        rid = rule["id"]
        metric = rule.get("metric", "")
        threshold = rule.get("threshold", 0)
        severity = Severity(rule.get("severity", "INFO"))
        alerts: list[RuleAlert] = []

        if metric == "total_delta":
            value = abs(greeks_summary.get("delta", 0))
            if value > threshold:
                alerts.append(RuleAlert(
                    rule_id=rid, severity=severity, portfolio=portfolio_id,
                    message=f"Portfolio delta {value:.2f} exceeds threshold {threshold}",
                    value=value, threshold=threshold,
                ))

        elif metric == "position_risk_pct":
            for pos in positions:
                avg_price = abs(float(pos.get("average_open_price", pos.get("entry_price", 0))))
                qty = abs(int(pos.get("quantity", pos.get("qty", 0))))
                multiplier = int(pos.get("multiplier", 100) or 100)
                max_loss = avg_price * qty * multiplier
                risk_pct = max_loss / capital if capital > 0 else 0
                if risk_pct > threshold:
                    alerts.append(RuleAlert(
                        rule_id=rid, severity=severity, portfolio=portfolio_id,
                        ticker=pos.get("ticker", pos.get("underlying_symbol", "")),
                        message=f"Position risk {risk_pct:.2%} exceeds {threshold:.2%} of capital",
                        value=risk_pct, threshold=threshold,
                    ))

        elif metric == "dte_remaining":
            for pos in positions:
                dte = pos.get("dte", 999)
                if 0 < dte < threshold:
                    alerts.append(RuleAlert(
                        rule_id=rid, severity=severity, portfolio=portfolio_id,
                        ticker=pos.get("ticker", pos.get("underlying_symbol", "")),
                        message=f"Position DTE {dte} below threshold {threshold} — consider closing",
                        value=dte, threshold=threshold,
                    ))

        elif metric == "profit_pct_of_max":
            for pos in positions:
                pnl_pct = pos.get("profit_pct_of_max", 0)
                if pnl_pct >= threshold:
                    alerts.append(RuleAlert(
                        rule_id=rid, severity=severity, portfolio=portfolio_id,
                        ticker=pos.get("ticker", pos.get("underlying_symbol", "")),
                        message=f"Position at {pnl_pct:.0%} of max profit — take profit",
                        value=pnl_pct, threshold=threshold,
                    ))

        elif metric == "loss_pct_of_credit":
            for pos in positions:
                loss_pct = pos.get("loss_pct_of_credit", 0)
                if loss_pct >= threshold:
                    alerts.append(RuleAlert(
                        rule_id=rid, severity=severity, portfolio=portfolio_id,
                        ticker=pos.get("ticker", pos.get("underlying_symbol", "")),
                        message=f"Position loss {loss_pct:.0%}x credit — stop loss triggered",
                        value=loss_pct, threshold=threshold,
                    ))

        elif metric == "iv_rank":
            # Check IV rank from market metrics for entry suitability
            underlyings = {pos.get("underlying_symbol") for pos in positions if pos.get("underlying_symbol")}
            if underlyings:
                try:
                    metrics = self.adapter.get_market_metrics(list(underlyings))
                    for sym, data in metrics.items():
                        iv_rank = data.get("tw_iv_rank")
                        if iv_rank is not None and iv_rank < threshold:
                            alerts.append(RuleAlert(
                                rule_id=rid, severity=severity, portfolio=portfolio_id,
                                ticker=sym,
                                message=f"IV rank {iv_rank:.0f} below entry threshold {threshold}",
                                value=iv_rank, threshold=threshold,
                            ))
                except Exception as e:
                    logger.debug("Could not fetch IV rank for %s: %s", underlyings, e)

        elif metric == "earnings_within_dte":
            underlyings = {pos.get("underlying_symbol") for pos in positions if pos.get("underlying_symbol")}
            if underlyings:
                try:
                    metrics = self.adapter.get_market_metrics(list(underlyings))
                    for pos in positions:
                        und = pos.get("underlying_symbol", "")
                        if und not in metrics:
                            continue
                        earnings = metrics[und].get("earnings")
                        if not earnings or not earnings.get("expected_date"):
                            continue
                        try:
                            earn_date = datetime.strptime(earnings["expected_date"], "%Y-%m-%d").date()
                            days_to_earnings = (earn_date - date.today()).days
                            if 0 < days_to_earnings <= threshold:
                                dte = pos.get("dte", 999)
                                if dte > days_to_earnings:
                                    alerts.append(RuleAlert(
                                        rule_id=rid, severity=severity, portfolio=portfolio_id,
                                        ticker=und,
                                        message=f"Earnings in {days_to_earnings}d — position has {dte} DTE",
                                        value=days_to_earnings, threshold=threshold,
                                    ))
                        except (ValueError, TypeError):
                            pass
                except Exception as e:
                    logger.debug("Could not check earnings: %s", e)

        return alerts

    # ------------------------------------------------------------------
    # Deployment band check
    # ------------------------------------------------------------------

    def _check_deployment_band(
        self,
        portfolio_id: str,
        deployment_pct: float,
        band: list[float],
    ) -> RuleAlert | None:
        """Check if deployment % from broker balances is within mandate band."""
        low = band[0] * 100 if band[0] <= 1.0 else band[0]
        high = band[1] * 100 if band[1] <= 1.0 else band[1]

        if deployment_pct > high:
            return RuleAlert(
                rule_id="deployment_over_band",
                severity=Severity.WARN,
                portfolio=portfolio_id,
                message=f"Deployment {deployment_pct:.1f}% exceeds band ceiling {high:.0f}%",
                value=deployment_pct,
                threshold=high,
            )
        elif deployment_pct < low:
            return RuleAlert(
                rule_id="deployment_under_band",
                severity=Severity.INFO,
                portfolio=portfolio_id,
                message=f"Deployment {deployment_pct:.1f}% below band floor {low:.0f}%",
                value=deployment_pct,
                threshold=low,
            )
        return None

    # ------------------------------------------------------------------
    # Mandate-specific rules
    # ------------------------------------------------------------------

    def _check_mandate_rules(
        self,
        portfolio_id: str,
        positions: list[dict],
        p_config: dict,
    ) -> list[RuleAlert]:
        """Check mandate-specific prohibitions and constraints."""
        alerts: list[RuleAlert] = []
        prohibitions = p_config.get("prohibitions", [])

        for pos in positions:
            role = pos.get("role", "")
            direction = pos.get("quantity_direction", "Long")
            option_type = pos.get("option_type", "")
            is_short = direction != "Long"

            # No naked short positions (absolute system rule)
            if is_short and role == "unknown_short":
                alerts.append(RuleAlert(
                    rule_id="naked_short_detected",
                    severity=Severity.CRITICAL,
                    portfolio=portfolio_id,
                    ticker=pos.get("ticker", ""),
                    message="Unclassified short position detected — may be naked",
                ))

            # P1: income generation prohibited
            if portfolio_id == "P1" and role in INCOME_ROLES:
                alerts.append(RuleAlert(
                    rule_id="p1_income_prohibited",
                    severity=Severity.CRITICAL,
                    portfolio=portfolio_id,
                    ticker=pos.get("ticker", ""),
                    message="Income position in P1 — explicitly prohibited by mandate",
                ))

            # P1: LEAPS tenor check
            if portfolio_id == "P1" and role == "convex_long_leap":
                leap_rules = p_config.get("leap_rules", {})
                dte = pos.get("dte", 0)
                roll_mandatory = leap_rules.get("roll_mandatory_months", 12) * 30
                if 0 < dte < roll_mandatory:
                    alerts.append(RuleAlert(
                        rule_id="p1_leap_needs_roll",
                        severity=Severity.WARN,
                        portfolio=portfolio_id,
                        ticker=pos.get("ticker", ""),
                        message=f"LEAPS DTE {dte} below mandatory roll threshold ({roll_mandatory}d)",
                        value=dte, threshold=roll_mandatory,
                    ))

        return alerts

    # ------------------------------------------------------------------
    # Hedge effectiveness (live greeks)
    # ------------------------------------------------------------------

    def _check_hedge_effectiveness(
        self,
        portfolio_id: str,
        positions: list[dict],
        p_config: dict,
    ) -> list[RuleAlert]:
        """
        Verify hedge positions exist and are effective using live data.

        - Check that hedge positions are present in each account
        - Calculate hedge coverage: delta of hedges vs delta of income positions
        - Check DTE of hedges vs DTE of income positions
        """
        alerts: list[RuleAlert] = []
        min_coverage = p_config.get("min_hedge_coverage", 0)
        hedge_mandatory = p_config.get("hedge_always_mandatory", False)

        income_positions = [p for p in positions if p.get("role") in INCOME_ROLES]
        hedge_positions = [p for p in positions if p.get("role") in HEDGE_ROLES]

        if not income_positions:
            return alerts  # No income exposure to hedge

        if not hedge_positions:
            if hedge_mandatory or min_coverage > 0:
                alerts.append(RuleAlert(
                    rule_id="no_hedge_for_income",
                    severity=Severity.CRITICAL,
                    portfolio=portfolio_id,
                    message="Income positions exist but no hedge positions found",
                ))
            return alerts

        # Use live greeks to compute hedge coverage
        if self.greeks_engine is not None:
            income_delta = 0.0
            hedge_delta = 0.0

            for pos in income_positions:
                pg = self.greeks_engine.get_position_greeks(pos)
                if pg and pg.get("status") == "LIVE":
                    income_delta += abs(pg.get("delta", 0))

            for pos in hedge_positions:
                pg = self.greeks_engine.get_position_greeks(pos)
                if pg and pg.get("status") == "LIVE":
                    hedge_delta += abs(pg.get("delta", 0))

            if income_delta > 0:
                coverage = hedge_delta / income_delta
                if min_coverage > 0 and coverage < min_coverage:
                    alerts.append(RuleAlert(
                        rule_id="hedge_coverage_low",
                        severity=Severity.WARN,
                        portfolio=portfolio_id,
                        message=(
                            f"Hedge delta coverage {coverage:.0%} below "
                            f"minimum {min_coverage:.0%} (hedge Δ={hedge_delta:.1f} "
                            f"vs income Δ={income_delta:.1f})"
                        ),
                        value=coverage,
                        threshold=min_coverage,
                    ))

        # DTE check: hedge should outlast or match income positions
        income_dtes = [p.get("dte", 999) for p in income_positions if p.get("dte", 999) < 999]
        hedge_dtes = [p.get("dte", 999) for p in hedge_positions if p.get("dte", 999) < 999]

        if income_dtes and hedge_dtes:
            max_income_dte = max(income_dtes)
            min_hedge_dte = min(hedge_dtes)
            if min_hedge_dte < max_income_dte:
                alerts.append(RuleAlert(
                    rule_id="hedge_expires_before_income",
                    severity=Severity.WARN,
                    portfolio=portfolio_id,
                    message=(
                        f"Hedge expires before income: hedge min DTE={min_hedge_dte} "
                        f"vs income max DTE={max_income_dte}"
                    ),
                    value=min_hedge_dte,
                    threshold=max_income_dte,
                ))

        return alerts

    # ------------------------------------------------------------------
    # System-level rules
    # ------------------------------------------------------------------

    def _eval_system_rule(
        self,
        rule: dict,
        all_portfolio_states: list[dict],
    ) -> RuleAlert | None:
        rid = rule["id"]
        severity = Severity(rule.get("severity", "CRITICAL"))
        description = rule.get("description", rid)

        if rid == "no_income_without_hedge":
            for state in all_portfolio_states:
                has_income = state.get("has_income_positions", False)
                has_hedge = state.get("has_hedge", False)
                if has_income and not has_hedge:
                    return RuleAlert(
                        rule_id=rid, severity=severity,
                        portfolio=state.get("portfolio_id", ""),
                        message=f"SYSTEM RULE BREACH: {description}",
                        value=0, threshold=0,
                    )

        elif rid == "no_hedge_removal":
            for state in all_portfolio_states:
                if state.get("hedge_removed", False):
                    return RuleAlert(
                        rule_id=rid, severity=severity,
                        portfolio=state.get("portfolio_id", ""),
                        message=f"SYSTEM RULE BREACH: {description}",
                        value=0, threshold=0,
                    )

        elif rid == "no_deploy_increase_vol_spike":
            for state in all_portfolio_states:
                vix = state.get("vix", 20)
                deployment_increased = state.get("deployment_increased", False)
                if vix > 30 and deployment_increased:
                    return RuleAlert(
                        rule_id=rid, severity=severity,
                        portfolio=state.get("portfolio_id", ""),
                        message=f"SYSTEM RULE BREACH: {description} (VIX={vix:.1f})",
                        value=vix, threshold=30,
                    )

        return None

    # ------------------------------------------------------------------
    # State escalation
    # ------------------------------------------------------------------

    def _compute_system_state(self, alerts: list[RuleAlert]) -> SystemState:
        """
        GREEN  — no CRITICAL alerts
        YELLOW — data quality issues OR WARN-level mandate breaches
        RED    — any CRITICAL alert
        """
        has_critical = any(a.severity == Severity.CRITICAL for a in alerts)
        has_warn = any(a.severity == Severity.WARN for a in alerts)
        has_data_issue = any(a.rule_id == "data_quality_check" for a in alerts)

        if has_critical:
            return SystemState.RED
        if has_warn or has_data_issue:
            return SystemState.YELLOW
        return SystemState.GREEN

    # ------------------------------------------------------------------
    # Weekly compliance scoring
    # ------------------------------------------------------------------

    def _compute_compliance_score(self, alerts: list[RuleAlert]) -> float:
        """
        Score from 0.0 (fully non-compliant) to 1.0 (fully compliant).

        Deductions:
        - CRITICAL alert: -0.15 each
        - WARN alert: -0.05 each
        - INFO alert: -0.01 each
        - Data quality issue: -0.10
        """
        score = 1.0
        for a in alerts:
            if a.severity == Severity.CRITICAL:
                score -= 0.15
            elif a.severity == Severity.WARN:
                score -= 0.05
            elif a.severity == Severity.INFO:
                score -= 0.01
            if a.rule_id == "data_quality_check":
                score -= 0.10
        return max(0.0, round(score, 2))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _dte_from_expiry(expiry_str: str) -> int:
        """Days to expiry from a YYYY-MM-DD string."""
        try:
            exp = datetime.strptime(expiry_str, "%Y-%m-%d").date()
            return max((exp - date.today()).days, 0)
        except (ValueError, TypeError):
            return 0

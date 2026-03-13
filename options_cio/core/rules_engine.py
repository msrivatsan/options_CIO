"""
Rules Engine — evaluates trading_rules.json against live position/portfolio state.
Returns a list of RuleAlert objects with severity and context.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class Severity(str, Enum):
    INFO = "INFO"
    WARN = "WARN"
    CRITICAL = "CRITICAL"


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


class RulesEngine:
    """
    Loads rules from trading_rules.json and evaluates them against
    current portfolio state snapshots.
    """

    def __init__(self, rules_path: str | Path) -> None:
        with open(rules_path) as f:
            data = json.load(f)
        self.rules: list[dict] = data.get("rules", [])
        self.system_rules: list[dict] = data.get("system_rules", [])

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate_portfolio(
        self,
        portfolio_id: str,
        positions: list[dict],
        greeks_summary: dict,
        capital: float,
    ) -> list[RuleAlert]:
        alerts: list[RuleAlert] = []
        for rule in self.rules:
            alert = self._eval_rule(rule, portfolio_id, positions, greeks_summary, capital)
            if alert:
                alerts.append(alert)
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
    # Internal evaluation logic
    # ------------------------------------------------------------------

    def _eval_rule(
        self,
        rule: dict,
        portfolio_id: str,
        positions: list[dict],
        greeks_summary: dict,
        capital: float,
    ) -> RuleAlert | None:
        rid = rule["id"]
        metric = rule.get("metric", "")
        threshold = rule.get("threshold", 0)
        severity = Severity(rule.get("severity", "INFO"))

        if metric == "total_delta":
            value = abs(greeks_summary.get("delta", 0))
            if value > threshold:
                return RuleAlert(
                    rule_id=rid, severity=severity, portfolio=portfolio_id,
                    message=f"Portfolio delta {value:.2f} exceeds threshold {threshold}",
                    value=value, threshold=threshold,
                )

        elif metric == "position_risk_pct":
            for pos in positions:
                max_loss = abs(float(pos.get("entry_price", 0))) * abs(int(pos.get("qty", 0))) * 100
                risk_pct = max_loss / capital if capital > 0 else 0
                if risk_pct > threshold:
                    return RuleAlert(
                        rule_id=rid, severity=severity, portfolio=portfolio_id,
                        ticker=pos.get("ticker", ""),
                        message=f"Position risk {risk_pct:.2%} exceeds {threshold:.2%} of capital",
                        value=risk_pct, threshold=threshold,
                    )

        elif metric == "dte_remaining":
            for pos in positions:
                dte = pos.get("dte", 999)
                if 0 < dte < threshold:
                    return RuleAlert(
                        rule_id=rid, severity=severity, portfolio=portfolio_id,
                        ticker=pos.get("ticker", ""),
                        message=f"Position DTE {dte} below threshold {threshold} — consider closing",
                        value=dte, threshold=threshold,
                    )

        elif metric == "profit_pct_of_max":
            for pos in positions:
                pnl_pct = pos.get("profit_pct_of_max", 0)
                if pnl_pct >= threshold:
                    return RuleAlert(
                        rule_id=rid, severity=severity, portfolio=portfolio_id,
                        ticker=pos.get("ticker", ""),
                        message=f"Position at {pnl_pct:.0%} of max profit — take profit",
                        value=pnl_pct, threshold=threshold,
                    )

        elif metric == "loss_pct_of_credit":
            for pos in positions:
                loss_pct = pos.get("loss_pct_of_credit", 0)
                if loss_pct >= threshold:
                    return RuleAlert(
                        rule_id=rid, severity=severity, portfolio=portfolio_id,
                        ticker=pos.get("ticker", ""),
                        message=f"Position loss {loss_pct:.0%}x credit — stop loss triggered",
                        value=loss_pct, threshold=threshold,
                    )

        return None

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

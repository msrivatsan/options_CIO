"""
CIO Brain — wraps the Anthropic Claude API to provide AI-driven
portfolio review, trade evaluation, and scenario analysis.
Respects ai_offline mode and daily cost limits.
"""

from __future__ import annotations

import json
import os
import time
from datetime import date
from pathlib import Path
from typing import Optional

import anthropic

from .prompts import (
    SYSTEM_PROMPT,
    build_review_prompt,
    build_trade_review_prompt,
    build_what_if_prompt,
)


# Approximate cost per 1K tokens (claude-sonnet pricing, rough estimate)
COST_PER_1K_INPUT = 0.003
COST_PER_1K_OUTPUT = 0.015


class CIOBrain:
    """
    AI CIO powered by Claude.

    Provides:
    - daily_review(): full portfolio review with alerts and recommendations
    - review_trade(): mandate compliance check for a proposed trade
    - what_if(): scenario shock analysis
    """

    def __init__(
        self,
        model: str = "claude-sonnet-4-20250514",
        ai_offline: bool = False,
        max_daily_cost: float = 5.00,
        api_key: Optional[str] = None,
    ) -> None:
        self.model = model
        self.ai_offline = ai_offline
        self.max_daily_cost = max_daily_cost
        self._daily_cost: float = 0.0
        self._cost_date: date = date.today()
        self._client: Optional[anthropic.Anthropic] = None

        if not ai_offline:
            key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
            if key:
                self._client = anthropic.Anthropic(api_key=key)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def daily_review(
        self,
        portfolio_states: list[dict],
        greeks_summaries: list[dict],
        rule_alerts: list[str],
        market_snapshot: dict,
        positions_summary: str,
    ) -> str:
        if self.ai_offline or not self._client:
            return self._offline_review(portfolio_states, rule_alerts, market_snapshot)

        if not self._check_budget():
            return "[AI BUDGET LIMIT] Daily API cost limit reached. Showing cached view."

        prompt = build_review_prompt(
            portfolio_states=portfolio_states,
            greeks_summaries=greeks_summaries,
            rule_alerts=rule_alerts,
            market_snapshot=market_snapshot,
            positions_summary=positions_summary,
        )
        return self._call(prompt)

    def review_trade(
        self,
        trade: dict,
        portfolio_id: str,
        portfolio_config: dict,
    ) -> str:
        if self.ai_offline or not self._client:
            return "[AI OFFLINE] Trade review unavailable in offline mode."

        if not self._check_budget():
            return "[AI BUDGET LIMIT] Cannot review trade — daily cost limit reached."

        prompt = build_trade_review_prompt(
            trade=trade,
            portfolio_id=portfolio_id,
            portfolio_config=portfolio_config,
        )
        return self._call(prompt)

    def what_if(
        self,
        scenario: dict,
        portfolio_states: list[dict],
        greeks_summaries: list[dict],
    ) -> str:
        if self.ai_offline or not self._client:
            return "[AI OFFLINE] What-if analysis unavailable in offline mode."

        if not self._check_budget():
            return "[AI BUDGET LIMIT] Cannot run scenario — daily cost limit reached."

        prompt = build_what_if_prompt(
            scenario=scenario,
            portfolio_states=portfolio_states,
            greeks_summaries=greeks_summaries,
        )
        return self._call(prompt)

    def get_daily_cost(self) -> float:
        self._reset_cost_if_new_day()
        return self._daily_cost

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _call(self, user_prompt: str) -> str:
        try:
            response = self._client.messages.create(  # type: ignore[union-attr]
                model=self.model,
                max_tokens=2048,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )
            text = response.content[0].text
            self._track_cost(response.usage)
            return text
        except anthropic.APIError as e:
            return f"[AI ERROR] API call failed: {e}"
        except Exception as e:
            return f"[AI ERROR] Unexpected error: {e}"

    def _track_cost(self, usage: anthropic.types.Usage) -> None:
        self._reset_cost_if_new_day()
        cost = (usage.input_tokens / 1000 * COST_PER_1K_INPUT
                + usage.output_tokens / 1000 * COST_PER_1K_OUTPUT)
        self._daily_cost += cost

    def _reset_cost_if_new_day(self) -> None:
        today = date.today()
        if today != self._cost_date:
            self._daily_cost = 0.0
            self._cost_date = today

    def _check_budget(self) -> bool:
        self._reset_cost_if_new_day()
        return self._daily_cost < self.max_daily_cost

    def _offline_review(
        self,
        portfolio_states: list[dict],
        rule_alerts: list[str],
        market_snapshot: dict,
    ) -> str:
        lines = ["[AI OFFLINE MODE — Rule-based summary]", ""]
        lines.append(f"VIX: {market_snapshot.get('vix', 'N/A')}  |  SPX: {market_snapshot.get('spx', 'N/A')}")
        lines.append("")
        for state in portfolio_states:
            pid = state.get("portfolio_id", "")
            dep = state.get("deployment_pct", 0)
            income = state.get("has_income_positions", False)
            hedge = state.get("has_hedge", False)
            hedge_flag = "" if not income or hedge else "  *** NO HEDGE — CRITICAL ***"
            lines.append(f"{pid}: {dep:.1%} deployed | income={income} | hedge={hedge}{hedge_flag}")
        if rule_alerts:
            lines.append("")
            lines.append("Active Alerts:")
            lines.extend(f"  {a}" for a in rule_alerts)
        return "\n".join(lines)

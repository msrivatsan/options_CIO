"""
CIO Brain — wraps the Anthropic Claude API to provide AI-driven
portfolio review, trade evaluation, and scenario analysis.
Respects ai_offline mode and daily cost limits.

Data context is built from live tastytrade API (positions, balances,
Greeks via DXLink streaming, market metrics).
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

# Data freshness threshold — beyond this, flag as STALE
STALE_THRESHOLD_SECONDS = 30

PORTFOLIO_IDS = ["P1", "P2", "P3", "P4"]

# Appended to the base system prompt when live data is available
LIVE_DATA_PROMPT_ADDITION = (
    "\n\nData source: Live tastytrade API (read-only). All position, balance, "
    "and Greeks data is real-time from the broker. OBP values come directly "
    "from tastytrade's balance API. Greeks are streamed via DXLink. You may "
    "rely on this data as current and accurate unless flagged as STALE."
)


class CIOBrain:
    """
    AI CIO powered by Claude.

    Provides:
    - daily_review(): full portfolio review with alerts and recommendations
    - review_trade(): mandate compliance check for a proposed trade
    - what_if(): scenario shock analysis
    - query(): natural language position/balance/greeks queries against live data
    - build_context(): assembles live portfolio context from tastytrade
    """

    def __init__(
        self,
        model: str = "claude-sonnet-4-20250514",
        ai_offline: bool = False,
        max_daily_cost: float = 5.00,
        api_key: Optional[str] = None,
        adapter=None,
        streamer=None,
    ) -> None:
        self.model = model
        self.ai_offline = ai_offline
        self.max_daily_cost = max_daily_cost
        self._daily_cost: float = 0.0
        self._cost_date: date = date.today()
        self._client: Optional[anthropic.Anthropic] = None
        self._adapter = adapter
        self._streamer = streamer

        if not ai_offline:
            key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
            if key:
                self._client = anthropic.Anthropic(api_key=key)

    # ------------------------------------------------------------------
    # Live data context
    # ------------------------------------------------------------------

    def build_context(self) -> dict:
        """Build a full portfolio context dict from live tastytrade data.

        Returns a dict with keys: portfolios, greeks, market_metrics,
        data_freshness.  Position sizes are expressed as percentages of
        portfolio net-liq (no raw dollar values) for PII stripping.
        """
        if not self._adapter:
            return {}

        portfolios: list[dict] = []
        all_symbols: list[str] = []

        for pid in PORTFOLIO_IDS:
            positions = self._adapter.get_positions(pid)
            balances = self._adapter.get_balances(pid)
            net_liq = balances.get("net_liquidating_value", 0) or 1

            stripped_positions = []
            for pos in positions:
                symbol = pos.get("symbol", "")
                all_symbols.append(symbol)
                mark = pos.get("mark", 0) or 0
                qty = pos.get("quantity", 0) or 0
                multiplier = pos.get("multiplier", 100) or 100
                notional = abs(mark * qty * multiplier)
                stripped_positions.append({
                    "symbol": symbol,
                    "underlying": pos.get("underlying_symbol", ""),
                    "instrument_type": pos.get("instrument_type", ""),
                    "option_type": pos.get("option_type", ""),
                    "strike": pos.get("strike_price"),
                    "expiry": pos.get("expiration_date"),
                    "qty": qty,
                    "direction": pos.get("quantity_direction", ""),
                    "pct_of_portfolio": round(notional / net_liq * 100, 2),
                })

            portfolios.append({
                "portfolio_id": pid,
                "positions": stripped_positions,
                "position_count": len(positions),
                "option_buying_power": balances.get("option_buying_power"),
                "deployment_pct": balances.get("deployment_pct"),
                "net_liquidating_value_pct": 100.0,  # self-referential baseline
                "committed_obp_pct": round(
                    balances.get("committed_obp", 0) / net_liq * 100, 2
                ) if net_liq else 0,
            })

        # Live Greeks from streamer
        greeks_data = {}
        freshness = {}
        if self._streamer:
            greeks_data = self._streamer.get_all_greeks()
            now = time.time()
            for sym in all_symbols:
                ts = self._streamer.get_timestamp(sym)
                if ts is None:
                    freshness[sym] = "NO DATA"
                else:
                    age = now - ts
                    if age > STALE_THRESHOLD_SECONDS:
                        freshness[sym] = f"STALE: {age:.0f} seconds"
                    else:
                        freshness[sym] = f"Last update: {age:.0f}s ago"

        # Market metrics for underlyings
        underlyings = list({
            pos.get("underlying", "")
            for p in portfolios
            for pos in p["positions"]
            if pos.get("underlying")
        })
        market_metrics = {}
        if underlyings and self._adapter:
            try:
                market_metrics = self._adapter.get_market_metrics(underlyings)
            except Exception:
                pass

        return {
            "portfolios": portfolios,
            "greeks": greeks_data,
            "market_metrics": market_metrics,
            "data_freshness": freshness,
        }

    def _build_context_text(self) -> str:
        """Render build_context() into a human-readable text block for prompts."""
        ctx = self.build_context()
        if not ctx:
            return ""

        lines = ["## Live Portfolio Data (tastytrade)\n"]

        for pf in ctx.get("portfolios", []):
            pid = pf["portfolio_id"]
            lines.append(f"### {pid}")
            lines.append(
                f"- OBP deployment: {pf.get('deployment_pct', 'N/A')}"
                f"  |  Committed: {pf.get('committed_obp_pct', 'N/A')}%"
            )
            lines.append(f"- Positions: {pf.get('position_count', 0)}")
            for pos in pf.get("positions", []):
                sym = pos["symbol"]
                greeks = ctx["greeks"].get(sym) or {}
                fresh = ctx["data_freshness"].get(sym, "")
                g_str = (
                    f"Δ={greeks.get('delta', '?')} Γ={greeks.get('gamma', '?')} "
                    f"Θ={greeks.get('theta', '?')} V={greeks.get('vega', '?')}"
                ) if greeks else "Greeks: awaiting stream"
                lines.append(
                    f"  {sym} {pos['direction']} {pos['qty']}  "
                    f"({pos['pct_of_portfolio']}% of portfolio)  "
                    f"{g_str}  [{fresh}]"
                )
            lines.append("")

        # Market metrics summary
        mm = ctx.get("market_metrics", {})
        if mm:
            lines.append("### Market Metrics (tastytrade)")
            for sym, metrics in mm.items():
                iv_rank = metrics.get("tw_iv_rank", "N/A")
                iv_pct = metrics.get("iv_percentile", "N/A")
                lines.append(f"  {sym}: IV Rank={iv_rank}  IV %ile={iv_pct}")
            lines.append("")

        return "\n".join(lines)

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

        # Append live context from tastytrade if available
        live_ctx = self._build_context_text()
        if live_ctx:
            prompt += f"\n\n{live_ctx}"

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

        live_ctx = self._build_context_text()
        if live_ctx:
            prompt += f"\n\n{live_ctx}"

        return self._call(prompt)

    def query(self, question: str) -> str:
        """Answer a natural language question using live tastytrade data.

        Routes questions about positions, buying power, and Greeks directly
        to the adapter/streamer before sending to Claude for formatting.
        """
        if self.ai_offline or not self._client:
            return "[AI OFFLINE] Queries unavailable in offline mode."

        if not self._check_budget():
            return "[AI BUDGET LIMIT] Cannot answer query — daily cost limit reached."

        # Gather targeted data based on the question
        data_context = self._resolve_query_data(question)
        live_ctx = self._build_context_text()

        prompt = f"""=== PORTFOLIO QUERY ===

User question: {question}

## Targeted Data
{data_context}

## Full Live Context
{live_ctx if live_ctx else "No live data available."}

Answer the user's question using the data above. Be precise and use numbers.
"""
        return self._call(prompt)

    def get_daily_cost(self) -> float:
        self._reset_cost_if_new_day()
        return self._daily_cost

    # ------------------------------------------------------------------
    # Query data resolution
    # ------------------------------------------------------------------

    def _resolve_query_data(self, question: str) -> str:
        """Pull targeted live data relevant to the user's question."""
        if not self._adapter:
            return "No live data source configured."

        lines = []
        q = question.lower()

        # Detect which portfolio(s) the question targets
        target_pids = [pid for pid in PORTFOLIO_IDS if pid.lower() in q]
        if not target_pids:
            target_pids = PORTFOLIO_IDS  # default to all

        # Position queries
        if any(kw in q for kw in ["position", "holding", "what do i have", "what are my"]):
            for pid in target_pids:
                positions = self._adapter.get_positions(pid)
                lines.append(f"### {pid} Positions ({len(positions)})")
                for pos in positions:
                    lines.append(
                        f"  {pos.get('symbol')} "
                        f"{pos.get('quantity_direction', '')} {pos.get('quantity', '')} "
                        f"@ {pos.get('mark', 'N/A')}"
                    )
                lines.append("")

        # Balance / buying power queries
        if any(kw in q for kw in ["buying power", "balance", "capital", "obp", "deployment", "net liq"]):
            for pid in target_pids:
                bal = self._adapter.get_balances(pid)
                lines.append(f"### {pid} Balances")
                lines.append(f"  Net Liq: ${bal.get('net_liquidating_value', 0):,.2f}")
                lines.append(f"  Option Buying Power: ${bal.get('option_buying_power', 0):,.2f}")
                lines.append(f"  Deployment: {bal.get('deployment_pct', 0):.1%}")
                lines.append(f"  Cash: ${bal.get('cash_balance', 0):,.2f}")
                lines.append("")

        # Greeks queries
        if any(kw in q for kw in ["delta", "gamma", "theta", "vega", "greek"]):
            if self._streamer:
                for pid in target_pids:
                    positions = self._adapter.get_positions(pid)
                    lines.append(f"### {pid} Greeks")
                    for pos in positions:
                        sym = pos.get("symbol", "")
                        greeks = self._streamer.get_greeks(sym)
                        if greeks:
                            lines.append(
                                f"  {sym}: Δ={greeks.get('delta')} "
                                f"Γ={greeks.get('gamma')} "
                                f"Θ={greeks.get('theta')} "
                                f"V={greeks.get('vega')}"
                            )
                    lines.append("")

        if not lines:
            # Fallback: provide balances for targeted portfolios
            for pid in target_pids:
                bal = self._adapter.get_balances(pid)
                positions = self._adapter.get_positions(pid)
                lines.append(f"### {pid}: {len(positions)} positions, "
                             f"deployment {bal.get('deployment_pct', 0):.1%}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_system_prompt(self) -> str:
        """Return the system prompt, with live-data addendum if adapter is set."""
        if self._adapter:
            return SYSTEM_PROMPT + LIVE_DATA_PROMPT_ADDITION
        return SYSTEM_PROMPT

    def _call(self, user_prompt: str) -> str:
        try:
            response = self._client.messages.create(  # type: ignore[union-attr]
                model=self.model,
                max_tokens=2048,
                system=self._get_system_prompt(),
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

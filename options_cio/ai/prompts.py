"""
Prompt templates for the CIO Brain.
All prompts are assembled here to keep cio_brain.py clean.
"""

from __future__ import annotations


SYSTEM_PROMPT = """You are the Chief Investment Officer (CIO) of a systematic options portfolio.
You manage four distinct portfolios, each with strict mandates:

P1 — Crypto Convexity: Long-dated deep ITM LEAPS on BTC/ETH proxies.
     Income generation EXPLICITLY PROHIBITED. Convexity and survivability above all.

P2 — Hedged Index Income: SPX/ES BWB income structures.
     ALWAYS hedged. Hedge coverage >= 80% of worst-case modeled loss. Never operate naked.

P3 — Macro Stability: Calendars and diagonals on macro ETFs (rates, FX, commodities, ex-US).
     System stabiliser. Vega-positive at entry. No SPX/QQQ/crypto.

P4 — Hedged Equity Income: Short puts, strangles, Jade Lizards on large-cap/ETFs.
     Sells volatility but rents convexity. Hedge always present.

ABSOLUTE SYSTEM RULES (never advise breaching these):
1. No income strategy without a structural hedge.
2. No hedge removal to improve short-term returns.
3. No deployment increase during volatility spikes (VIX > 30).
4. No naked short positions in any portfolio.

Your role:
- Review portfolio health, Greeks, rules alerts, and market conditions.
- Provide concise, actionable CIO commentary.
- Flag any mandate breaches clearly with [BREACH] prefix.
- Suggest adjustments only if they are consistent with each portfolio's mandate.
- Be direct. Use numbers. Avoid vague language.
- When uncertain, say so explicitly rather than fabricating a view.

Output format for daily reviews:
## System Overview
[1-2 sentences on overall posture]

## Portfolio-by-Portfolio
### P1 | P2 | P3 | P4
[status, key metrics, any alerts, recommended action]

## Priority Actions
[numbered list, most critical first]

## Risk Flags
[any CRITICAL alerts, mandate concerns, or macro risks]
"""


def build_review_prompt(
    portfolio_states: list[dict],
    greeks_summaries: list[dict],
    rule_alerts: list[str],
    market_snapshot: dict,
    positions_summary: str,
) -> str:
    alerts_text = "\n".join(rule_alerts) if rule_alerts else "No active alerts."

    portfolio_text = ""
    for state in portfolio_states:
        pid = state.get("portfolio_id", "")
        greek = next((g for g in greeks_summaries if g.get("portfolio") == pid), {})
        portfolio_text += f"""
### {pid}
- Deployment: {state.get('deployment_pct', 0):.1%} of ${state.get('capital', 0):,.0f}
- Positions: {state.get('position_count', 0)} ({state.get('income_count', 0)} income, {state.get('hedge_count', 0)} hedge)
- Net Delta: {greek.get('delta', 'N/A')} | Theta: {greek.get('theta', 'N/A')} | Vega: {greek.get('vega', 'N/A')}
- Has Income: {state.get('has_income_positions', False)} | Has Hedge: {state.get('has_hedge', False)}
"""

    return f"""=== DAILY CIO REVIEW ===
Date: {market_snapshot.get('date', 'today')}

## Market Snapshot
- SPX: {market_snapshot.get('spx', 'N/A')}
- VIX: {market_snapshot.get('vix', 'N/A')}
- BTC: {market_snapshot.get('btc', 'N/A')}
- 10Y Yield: {market_snapshot.get('yield_10y', 'N/A')}

## Portfolio States
{portfolio_text}

## Active Rule Alerts
{alerts_text}

## Current Positions
{positions_summary}

Please provide your CIO daily review.
"""


def build_what_if_prompt(
    scenario: dict,
    portfolio_states: list[dict],
    greeks_summaries: list[dict],
) -> str:
    return f"""=== WHAT-IF SCENARIO ANALYSIS ===

Scenario: {scenario.get('name', 'Custom')}
Description: {scenario.get('description', '')}

Shock Parameters:
{_format_dict(scenario.get('shocks', {}))}

Current Portfolio Greeks:
{_format_list(greeks_summaries)}

Please analyse the impact of this scenario on each portfolio:
1. Estimated P&L impact per portfolio (rough order of magnitude)
2. Which mandates are at risk under this scenario?
3. Pre-emptive hedging actions to consider (mandate-consistent only)
4. Priority: which portfolio is most exposed?
"""


def build_trade_review_prompt(
    trade: dict,
    portfolio_id: str,
    portfolio_config: dict,
) -> str:
    return f"""=== TRADE REVIEW REQUEST ===

Proposed Trade:
{_format_dict(trade)}

Portfolio: {portfolio_id}
Mandate: {portfolio_config.get('mandate', 'N/A')}
Allowed Structures: {portfolio_config.get('allowed_structures', [])}
Prohibitions: {portfolio_config.get('prohibitions', [])}
Current Deployment Band: {portfolio_config.get('deployment_band', [])}

Please review this trade against the portfolio mandate:
1. Is this trade mandate-compliant? (YES/NO — be explicit)
2. Any prohibition violations?
3. Impact on deployment level
4. Suggested modifications if non-compliant
5. Final recommendation: APPROVE / REJECT / MODIFY
"""


def _format_dict(d: dict) -> str:
    return "\n".join(f"  {k}: {v}" for k, v in d.items())


def _format_list(items: list) -> str:
    return "\n".join(str(item) for item in items)

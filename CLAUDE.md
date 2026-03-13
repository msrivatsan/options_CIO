# Options CIO — Claude Project Context

## Project Overview

**options-cio** is a systematic options portfolio management system for a $500K book split across 4 mandate-driven portfolios. It provides real-time Greeks, rules enforcement, AI-driven daily reviews (Claude), what-if scenario analysis, and a Textual TUI dashboard.

**Location:** `C:\Users\madha\options-cio\`
**Python:** 3.13 | **Entry point:** `python -m options_cio.main`

---

## Source Control — GitHub Required

**All changes must be committed and pushed to GitHub.**

- Every code change, config update, or new file requires a git commit with a clear, descriptive message.
- Never leave meaningful changes uncommitted. After completing any task, stage and commit before considering the work done.
- Commit messages must describe *why*, not just *what*. Reference the portfolio or module affected.
- Push to `origin main` after each logical unit of work.
- Do not force-push to `main`. If a rebase is needed, confirm with the user first.

**Workflow for every change:**
```bash
git add <specific files>
git commit -m "meaningful message"
git push origin main
```

**Do not:**
- Use `git add -A` or `git add .` without reviewing what's staged.
- Amend published commits.
- Skip commits because a change is "small".

---

## Architecture

```
options_cio/
├── config/              # trading_rules.json, portfolios.json, settings.yaml
├── core/                # GreeksEngine, RulesEngine, PortfolioManager, StateCache
├── ai/                  # CIOBrain (Claude API) + prompt templates
├── ui/                  # Textual TUI dashboard + widgets
├── data/                # YFinanceFeed (live), IBKRAdapter (stub)
├── journal/             # SQLite trade + alert log (TradeJournal)
├── simulator/           # WhatIfSimulator — 5 preset + custom scenarios
├── daily_review/        # CIODailyReview — full orchestration pipeline
├── active_positions.csv # Live book — edit to reflect current positions
└── main.py              # Entry point (--review, --offline flags)
```

---

## Portfolios

| ID | Name | Strategy | Capital | Deployment Band |
|----|------|----------|---------|-----------------|
| P1 | Crypto Convexity | Long-dated deep ITM LEAPS (IBIT/ETHA) | $125,000 | 30–50% |
| P2 | Hedged Index Income | SPX/ES BWBs + structural hedge | $125,000 | 50–65% |
| P3 | Macro Stability | Calendars/diagonals on macro ETFs | $125,000 | 0–20% |
| P4 | Hedged Equity Income | Short puts, strangles, Jade Lizards | $125,000 | 55–70% |

**Absolute System Rules (never advise or implement code that breaches these):**
1. No income strategy without a structural hedge.
2. No hedge removal to improve short-term returns.
3. No deployment increase during volatility spikes (VIX > 30).
4. No naked short positions in any portfolio.
5. P1: income generation is explicitly prohibited.

---

## Key Files

| File | Purpose |
|------|---------|
| `config/settings.yaml` | API model, refresh interval, cost limit, data source |
| `config/portfolios.json` | Full mandate definitions — deployment bands, prohibitions, hedge rules |
| `config/trading_rules.json` | Quantitative rules — DTE, profit targets, stop losses |
| `active_positions.csv` | Current live book; columns: portfolio, ticker, option_type, strike, expiry, qty, entry_price, structure_tag |
| `options_cio.db` | SQLite journal — created on first run |

---

## Development Conventions

- **No backwards-compat hacks.** Change the code, don't shim it.
- **No speculative features.** Only build what is explicitly requested.
- **No unhedged income logic.** Any code path that creates income exposure must verify hedge presence.
- **Config is authoritative.** Don't hardcode portfolio rules in Python — read from `portfolios.json`.
- **AI calls are gated.** Always check `ai_offline` and daily cost budget before calling Claude.
- **Imports:** absolute imports from `options_cio.*` — no relative imports across modules.

---

## Running the Project

```bash
cd C:\Users\madha\options-cio

# TUI dashboard (default)
python -m options_cio.main

# CLI daily review to stdout
python -m options_cio.main --review

# Force AI offline (no API calls)
python -m options_cio.main --offline

# With explicit API key
python -m options_cio.main --api-key sk-ant-...
```

Environment variable: `ANTHROPIC_API_KEY`

---

## Dependencies (pinned)

```
anthropic==0.76.0   textual==8.1.1    yfinance==1.1.0
scipy==1.17.1       numpy==2.3.2      pandas==2.3.2
py_vollib==1.0.1    PyYAML==6.0.3     rich==14.3.1
```

Install: `pip install -r requirements.txt --break-system-packages`

---

## Change Log

| Date | Change | Commit |
|------|--------|--------|
| 2026-03-13 | Initial project scaffold — all modules, configs, sample positions | _(pending first push)_ |

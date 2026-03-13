# Options CIO

A systematic options portfolio management system powered by Claude AI. Manages 4 distinct portfolios with strict mandates, real-time Greeks, rules enforcement, and AI-driven daily reviews.

## Portfolios

| ID | Name | Strategy | Capital |
|----|------|----------|---------|
| P1 | Crypto Convexity | Long-dated deep ITM LEAPS on IBIT/ETHA | $125,000 |
| P2 | Hedged Index Income | SPX/ES BWBs with structural hedge | $125,000 |
| P3 | Macro Stability | Calendars/diagonals on macro ETFs | $125,000 |
| P4 | Hedged Equity Income | Short puts, strangles, Jade Lizards | $125,000 |

## Quick Start

```bash
cd options-cio

# Set your Anthropic API key
export ANTHROPIC_API_KEY=sk-ant-...

# Launch TUI dashboard
python -m options_cio.main

# Run daily review to stdout (no TUI)
python -m options_cio.main --review

# Offline mode (no AI calls)
python -m options_cio.main --offline
```

## Structure

```
options_cio/
├── config/              # Trading rules, portfolio mandates, settings
├── core/                # Greeks engine, rules engine, portfolio manager, state cache
├── ai/                  # CIO Brain (Claude) + prompt templates
├── ui/                  # Textual TUI dashboard + widgets
├── data/                # yfinance + IBKR adapters
├── journal/             # SQLite trade + alert journal
├── simulator/           # What-if scenario analysis
├── daily_review/        # Orchestrated daily review pipeline
├── active_positions.csv # Current positions (edit to reflect live book)
└── main.py              # Entry point
```

## Dashboard Keybindings

| Key | Action |
|-----|--------|
| `R` | Refresh market data & Greeks |
| `A` | Run AI CIO review |
| `S` | Run crash scenario (SPX -20%) |
| `1` | Overview tab |
| `2` | Positions tab |
| `3` | Journal/alerts tab |
| `Q` | Quit |

## Configuration

- `config/settings.yaml` — API model, refresh intervals, cost limits, data source
- `config/portfolios.json` — Full portfolio mandates, deployment bands, prohibitions
- `config/trading_rules.json` — Quantitative rules (DTE, profit targets, stop losses)
- `active_positions.csv` — Live book (update daily or integrate with IBKR adapter)

## AI Cost Management

The CIO Brain tracks daily API spend against `max_api_cost_per_day` (default $5.00).
Once the limit is reached, AI calls are skipped and a rule-based summary is shown instead.
Set `ai_offline: true` in settings.yaml to disable all AI calls.

## Adding a Data Source

Implement `FeedAdapter` (in `data/feed_adapter.py`) and swap in `data/ibkr_adapter.py`
for live IBKR data. Set `data_source: ibkr` in settings.yaml.

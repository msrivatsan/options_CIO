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

### 1. Set Environment Variables
```bash
# Anthropic Claude API
export ANTHROPIC_API_KEY=sk-ant-...

# Tastytrade OAuth (for live data)
export TASTYTRADE_CLIENT_SECRET=your-client-secret
export TASTYTRADE_REFRESH_TOKEN=your-refresh-token
```

### 2. Configure Accounts
Edit `config/accounts.yaml` — replace placeholders with your actual tastytrade account numbers:
```yaml
accounts:
  - account_number: "ACCOUNT_NUM_FOR_P1"
    portfolio: P1
    name: "Crypto Convexity"
  # ... repeat for P2, P3, P4
```

### 3. Test Connection
```bash
python scripts/test_connection.py
```
This validates authentication, discovers accounts, fetches balances & positions, and streams live Greeks.

### 4. Launch Dashboard
```bash
# Launch TUI dashboard (live tastytrade data)
python -m options_cio.main

# Run daily review to stdout (no TUI)
python -m options_cio.main --review

# Offline mode (no API calls, no tastytrade)
python -m options_cio.main --offline
```

## Structure

```
options_cio/
├── config/              # Trading rules, portfolio mandates, settings, account mapping
├── core/                # Greeks engine, rules engine, portfolio manager, state cache
├── ai/                  # CIO Brain (Claude) + prompt templates
├── ui/                  # Textual TUI dashboard + widgets
├── data/                # Live data adapters (tastytrade primary, yfinance fallback, IBKR stub)
│   ├── tastytrade_adapter.py   # OAuth-backed live positions, balances, Greeks
│   ├── streamer.py             # Real-time DXLink Quote & Greeks via websocket
│   └── feed_adapter.py          # Abstract interface + yfinance fallback
├── journal/             # SQLite trade + alert journal
├── simulator/           # What-if scenario analysis
├── daily_review/        # Orchestrated daily review pipeline
├── scripts/
│   └── test_connection.py       # Validate tastytrade auth & live streaming
├── active_positions.csv # Legacy CSV (kept for offline reference)
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

- `config/settings.yaml` — API model, refresh intervals, cost limits, data source (default: `tastytrade`)
- `config/portfolios.json` — Full portfolio mandates, deployment bands, prohibitions
- `config/trading_rules.json` — Quantitative rules (DTE, profit targets, stop losses)
- `config/accounts.yaml` — Tastytrade account ↔ portfolio mapping (required for live data)
- `active_positions.csv` — Legacy CSV (kept for offline/fallback reference)

## AI Cost Management

The CIO Brain tracks daily API spend against `max_api_cost_per_day` (default $5.00).
Once the limit is reached, AI calls are skipped and a rule-based summary is shown instead.
Set `ai_offline: true` in settings.yaml to disable all AI calls.

## Live Data Integration

### Tastytrade (Primary)
The system uses **tastytrade** as the primary data source for real-time positions, balances, option chains, and Greeks.

**Setup:**
1. Create an OAuth app in your tastytrade account
2. Obtain `TASTYTRADE_CLIENT_SECRET` and `TASTYTRADE_REFRESH_TOKEN`
3. Set these as environment variables
4. Configure `config/accounts.yaml` with your account numbers
5. Run `python scripts/test_connection.py` to validate

**What you get:**
- Real-time position data (live OBP = option buying power = deployment metric)
- Streaming Quote & Greeks via DXLink websocket
- Option chains, market metrics (IV rank, IV percentile, historical volatility)
- Transaction history for journal auto-population

### Fallback & Offline
- **YFinance fallback:** If tastytrade connection fails, the system automatically falls back to yfinance for prices and historical volatility
- **Offline mode:** Run with `--offline` flag to use cached data only (no API calls)

### Switching Data Sources
Change `data_source:` in `config/settings.yaml`:
- `tastytrade` — primary (requires OAuth env vars + accounts.yaml)
- `yfinance` — public, no auth required, ~15s price cache
- `ibkr` — IBKR API stub (not yet implemented)

## Security

**API Keys & Credentials:**
- `ANTHROPIC_API_KEY` — your Claude API key
- `TASTYTRADE_CLIENT_SECRET`, `TASTYTRADE_REFRESH_TOKEN` — OAuth secrets
- **Never commit these to Git.** Use environment variables or a `.env` file (already in `.gitignore`)

If credentials are accidentally exposed, revoke them immediately and rotate.

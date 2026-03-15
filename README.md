# Options CIO

A systematic options portfolio management system powered by Claude AI. Manages 4 distinct portfolios with strict mandates, real-time Greeks via tastytrade DXLink, rules enforcement, and AI-driven daily reviews.

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set Up Tastytrade Credentials

Create an OAuth application at [tastytrade.com](https://tastytrade.com) under **My Account > OAuth Applications > Manage > Create Grant**.

```bash
# Add to .env file (already in .gitignore)
export TASTYTRADE_CLIENT_SECRET='your-client-secret'
export TASTYTRADE_REFRESH_TOKEN='your-refresh-token'
```

### 3. Configure Account Mapping

Edit `config/accounts.yaml` with your tastytrade account numbers:
```yaml
accounts:
  - account_number: "YOUR_P1_ACCOUNT"
    portfolio: P1
    name: "Crypto Convexity"
  - account_number: "YOUR_P2_ACCOUNT"
    portfolio: P2
    name: "Hedged Index Income"
  - account_number: "YOUR_P3_ACCOUNT"
    portfolio: P3
    name: "Macro Stability"
  - account_number: "YOUR_P4_ACCOUNT"
    portfolio: P4
    name: "Hedged Equity Income"
```

Find your account numbers in tastytrade under **My Account > Account Details**.

### 4. Test Connection
```bash
python scripts/test_connection.py
```
Validates authentication, discovers accounts, fetches balances and positions, and streams live Greeks.

### 5. Launch
```bash
# TUI dashboard (live streaming data)
python -m options_cio.main

# Daily review to stdout
python -m options_cio.main --review

# Offline mode (no API calls)
python -m options_cio.main --offline

# Skip startup validation
python -m options_cio.main --skip-checks
```

## Portfolios

| ID | Name | Strategy | Deployment Band |
|----|------|----------|-----------------|
| P1 | Crypto Convexity | Long-dated deep ITM LEAPS (IBIT/ETHA) | 30-50% |
| P2 | Hedged Index Income | SPX/ES BWBs + structural hedge | 50-65% |
| P3 | Macro Stability | Calendars/diagonals on macro ETFs | 0-20% |
| P4 | Hedged Equity Income | Short puts, strangles, Jade Lizards | 55-70% |

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `R` | Refresh positions, balances, and rules |
| `A` | Run AI CIO review |
| `S` | Run crash scenario (SPX -20%) |
| `C` | Toggle connection diagnostics (API latency, streamer status, per-symbol freshness) |
| `L` | Toggle log panel (last 50 lines from options_cio.log) |
| `1` | Overview tab |
| `2` | Positions tab |
| `3` | Journal/alerts tab |
| `Q` | Quit (graceful shutdown with session summary) |

## Architecture

```
tastytrade REST API ──> TastytradeAdapter ──> PortfolioManager ──> RulesEngine ──> Dashboard
                    \                     \                                    /
                     ──> DXLinkStreamer ──> GreeksEngine ─────────────────────/
                                                                             \
                                                                     CIOBrain (Claude AI)
```

```
options_cio/
├── config/              # Trading rules, portfolio mandates, settings, account mapping
├── core/                # Greeks engine, rules engine, portfolio manager, state cache
├── ai/                  # CIO Brain (Claude) + prompt templates
├── ui/                  # Textual TUI dashboard + widgets
├── data/                # Live data adapters (tastytrade primary, yfinance fallback)
│   ├── tastytrade_adapter.py   # OAuth REST — positions, balances, metrics, transactions
│   ├── streamer.py             # DXLink websocket — real-time quotes & Greeks
│   └── feed_adapter.py         # Abstract interface + yfinance fallback
├── journal/             # SQLite trade + alert journal with auto-sync
├── simulator/           # What-if scenario analysis
├── daily_review/        # Orchestrated daily review pipeline
├── logging_config.py    # Centralized logging (rotating file + console)
├── scripts/
│   └── test_connection.py
└── main.py              # Entry point with startup validation
```

## Data Flow

```
REST API (every 60s)           WebSocket (continuous)       AI (event-driven)
├── Positions                  ├── Quotes (bid/ask)         ├── On violation
├── Balances (OBP)             └── Greeks (delta/gamma/     ├── On user query
├── Market metrics (IV)            theta/vega per contract)  └── On 5-min summary
└── Transactions
```

- **Greeks and quotes**: CONTINUOUS via DXLink websocket, UI updates every 2 seconds
- **Positions and balances**: REST polling every 60 seconds
- **Rules evaluation**: Runs on every position/balance refresh
- **AI calls**: Event-driven (new violation, user query, or periodic summary)

## Configuration Reference

### `config/settings.yaml`
```yaml
ai_offline: false              # true to disable all Claude API calls
api_model: claude-sonnet-4-20250514  # Claude model for AI reviews
refresh_interval_seconds: 60   # REST polling interval
ai_call_interval_seconds: 300  # Auto AI review interval
data_source: tastytrade        # tastytrade | yfinance | ibkr
db_path: ./options_cio.db      # SQLite database path
max_api_cost_per_day: 5.00     # Daily Claude API budget
```

### `config/accounts.yaml`
Maps tastytrade account numbers to portfolio IDs. Find account numbers in tastytrade under My Account > Account Details.

### `config/portfolios.json`
Full portfolio mandates: deployment bands, target zones, allowed instruments, hedge requirements, prohibitions. Each portfolio has a `mandate` field describing its governance rules.

### `config/trading_rules.json`
Quantitative rules: max risk per trade, delta limits, profit targets, stop losses, DTE thresholds. System rules: no income without hedge, no hedge removal, no deployment increase during vol spikes.

All config files support hot-reload (except `accounts.yaml`, which requires restart). Changes are detected every 30 seconds and applied immediately.

## Error Handling

### Tastytrade API
- **Rate limiting**: Token-bucket limiter (~2 req/s) prevents 429 errors
- **Retries**: 3 attempts with exponential backoff for transient errors (5xx, timeouts)
- **401 Unauthorized**: Auto-refreshes session up to 3 times, then prints re-auth instructions
- **403 Forbidden**: Never retried (scope issue), logged clearly
- **Timeouts**: 10s on all REST calls, falls back to cached data if available

### DXLink Streamer
- **Disconnect**: Logged, all Greeks flagged STALE, auto-reconnect every 10 seconds
- **Reconnect**: Resubscribes to all position symbols automatically
- **Down > 60s**: Dashboard shows DELAYED indicator, REST polling continues

### Claude API
- **Failure**: Logged, "AI OFFLINE" shown in panel, dashboard continues without AI
- **Budget exceeded**: All AI calls stopped, budget status shown in panel

### SQLite
- **Corrupt**: Detected via PRAGMA integrity_check on startup, renamed and fresh DB created
- **Locked**: Falls back to in-memory dict for the session with warning

### General TUI Rule
The dashboard never crashes. All exceptions in the main event loop are caught, logged, and displayed in the error status bar.

## Read-Only Safety

- OAuth scope is read-only
- The adapter module header explicitly prohibits order imports
- No execution capability exists in the codebase
- The system can only observe, analyze, and recommend — never trade

## Cost Breakdown

| Service | Cost |
|---------|------|
| tastytrade API | Free |
| DXLink streaming | Free |
| Claude API | ~$2-8/month depending on query frequency |
| **Total** | **$2-8/month** |

The CIO Brain tracks daily spend against `max_api_cost_per_day` (default $5.00). Set `ai_offline: true` to eliminate all API costs.

## Logging

Two log handlers:
- **File**: `options_cio.log` — rotating (10MB max, 5 backups), DEBUG level
- **Console**: stderr, INFO level

Log levels:
- **DEBUG**: All API calls (endpoint, response time, status), streamer events
- **INFO**: State changes, position changes, rules results, AI calls
- **WARNING**: Stale data, streamer reconnection, rate limits, fallback mode
- **ERROR**: API failures, auth issues, config parse errors
- **CRITICAL**: Rule breaches (CRITICAL severity), system RED, hedge insufficient

Press `L` in the dashboard to view the log panel.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "Authentication failed" | Regenerate refresh token at tastytrade.com > OAuth Applications |
| "Account not found" | Check account number in `config/accounts.yaml` |
| "Streamer timeout" | Check internet; tastytrade may be in maintenance (try during market hours) |
| "STALE DATA warnings" | Streamer disconnected — will auto-reconnect in 10 seconds |
| "AI OFFLINE" | Check `ANTHROPIC_API_KEY` env var; check daily budget not exhausted |
| "Rules not updating" | Verify `trading_rules.json` is valid JSON (use a JSON linter) |
| "Database corrupt" | Auto-detected on startup; corrupt file renamed, fresh DB created |
| "Missing env var" | Set `TASTYTRADE_CLIENT_SECRET` and `TASTYTRADE_REFRESH_TOKEN` in `.env` |

## Security

**Never commit credentials to Git.** All sensitive values use environment variables or `.env` (already in `.gitignore`):
- `ANTHROPIC_API_KEY` — Claude API key
- `TASTYTRADE_CLIENT_SECRET` — OAuth client secret
- `TASTYTRADE_REFRESH_TOKEN` — OAuth refresh token

If credentials are accidentally exposed, revoke them immediately and rotate.

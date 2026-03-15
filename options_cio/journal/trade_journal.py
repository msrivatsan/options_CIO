"""
Trade Journal — SQLite-backed log of all trades, AI reviews, and rule alerts.

Auto-populates from tastytrade transaction history. Tracks position changes,
records Greeks at entry/exit, and provides P&L attribution analytics.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import sqlite3
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


DDL = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    portfolio TEXT NOT NULL,
    ticker TEXT NOT NULL,
    action TEXT NOT NULL,
    option_type TEXT,
    strike REAL,
    expiry TEXT,
    qty INTEGER,
    price REAL,
    structure_tag TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS broker_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_txn_id INTEGER UNIQUE,
    ts TEXT NOT NULL,
    portfolio TEXT NOT NULL,
    symbol TEXT NOT NULL,
    underlying TEXT,
    instrument_type TEXT,
    transaction_type TEXT,
    transaction_sub_type TEXT,
    action TEXT,
    qty REAL,
    price REAL,
    value REAL,
    net_value REAL,
    commission REAL DEFAULT 0,
    clearing_fees REAL DEFAULT 0,
    regulatory_fees REAL DEFAULT 0,
    description TEXT,
    option_type TEXT,
    strike REAL,
    expiry TEXT
);

CREATE TABLE IF NOT EXISTS realized_pnl (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    portfolio TEXT NOT NULL,
    symbol TEXT NOT NULL,
    underlying TEXT,
    open_txn_id INTEGER,
    close_txn_id INTEGER,
    qty REAL,
    open_price REAL,
    close_price REAL,
    gross_pnl REAL,
    total_fees REAL DEFAULT 0,
    net_pnl REAL,
    holding_days INTEGER,
    option_type TEXT,
    strike REAL,
    expiry TEXT,
    entry_greeks TEXT,
    exit_greeks TEXT
);

CREATE TABLE IF NOT EXISTS position_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    portfolio TEXT NOT NULL,
    symbol TEXT NOT NULL,
    underlying TEXT,
    qty REAL,
    direction TEXT,
    mark_price REAL,
    greeks_json TEXT,
    event TEXT
);

CREATE TABLE IF NOT EXISTS rule_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    severity TEXT NOT NULL,
    portfolio TEXT,
    ticker TEXT,
    message TEXT,
    value REAL,
    threshold REAL
);

CREATE TABLE IF NOT EXISTS ai_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    review_type TEXT NOT NULL,
    portfolio TEXT,
    input_summary TEXT,
    ai_output TEXT,
    cost_usd REAL DEFAULT 0
);
"""


class TradeJournal:
    """Persistent journal backed by SQLite with tastytrade auto-sync.

    Falls back to in-memory SQLite if the database file is locked or corrupt.
    """

    def __init__(self, db_path: str | Path = "./options_cio.db") -> None:
        self.db_path = Path(db_path)
        self._last_positions: dict[str, dict[str, dict]] = {}
        self._fallback_memory = False
        self._init_db()

    def _init_db(self) -> None:
        try:
            with self._conn() as conn:
                conn.executescript(DDL)
        except (sqlite3.DatabaseError, sqlite3.OperationalError) as e:
            logger.warning("Journal DB init failed (%s) — falling back to in-memory", e)
            self._fallback_memory = True
            with self._conn() as conn:
                conn.executescript(DDL)

    def _conn(self) -> sqlite3.Connection:
        if self._fallback_memory:
            return sqlite3.connect(":memory:")
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            conn.execute("PRAGMA journal_mode=WAL")
            return conn
        except (sqlite3.DatabaseError, sqlite3.OperationalError) as e:
            logger.warning("Journal DB connection failed (%s) — using in-memory fallback", e)
            self._fallback_memory = True
            return sqlite3.connect(":memory:")

    # ------------------------------------------------------------------
    # Broker transaction sync
    # ------------------------------------------------------------------

    def sync_from_broker(
        self,
        adapter,
        lookback_days: int = 30,
        portfolio_ids: Optional[list[str]] = None,
    ) -> dict[str, int]:
        """Pull transactions from tastytrade and store new ones.

        Returns dict with counts: {"synced": N, "skipped": N, "matched": N}
        """
        end = date.today()
        start = end - timedelta(days=lookback_days)
        pids = portfolio_ids or ["P1", "P2", "P3", "P4"]

        synced = 0
        skipped = 0

        for pid in pids:
            try:
                txns = adapter.get_transactions(pid, start_date=start, end_date=end)
            except Exception as e:
                logger.warning("Failed to fetch transactions for %s: %s", pid, e)
                continue

            for txn in txns:
                broker_id = txn.get("id")
                if broker_id and self._txn_exists(broker_id):
                    skipped += 1
                    continue

                self._store_broker_txn(pid, txn)
                synced += 1

        # Match opens to closes for realized P&L
        matched = self._match_realized_pnl()

        logger.info(
            "Broker sync complete: %d synced, %d skipped, %d P&L matched",
            synced, skipped, matched,
        )
        return {"synced": synced, "skipped": skipped, "matched": matched}

    def _txn_exists(self, broker_txn_id: int) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM broker_transactions WHERE broker_txn_id = ?",
                (broker_txn_id,),
            ).fetchone()
            return row is not None

    def _store_broker_txn(self, portfolio_id: str, txn: dict) -> None:
        ts = txn.get("executed_at") or txn.get("transaction_date") or datetime.utcnow().isoformat()
        symbol = txn.get("symbol") or ""
        option_type = None
        strike = None
        expiry = None

        # Parse option details from symbol if it's an equity option
        if txn.get("instrument_type") == "Equity Option" and symbol:
            parsed = _parse_option_fields(symbol)
            option_type = parsed.get("option_type")
            strike = parsed.get("strike")
            expiry = parsed.get("expiry")

        with self._conn() as conn:
            conn.execute(
                """INSERT INTO broker_transactions
                   (broker_txn_id, ts, portfolio, symbol, underlying,
                    instrument_type, transaction_type, transaction_sub_type,
                    action, qty, price, value, net_value,
                    commission, clearing_fees, regulatory_fees,
                    description, option_type, strike, expiry)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    txn.get("id"),
                    ts,
                    portfolio_id,
                    symbol,
                    txn.get("underlying_symbol"),
                    txn.get("instrument_type"),
                    txn.get("transaction_type"),
                    txn.get("transaction_sub_type"),
                    txn.get("action"),
                    txn.get("quantity"),
                    txn.get("price"),
                    txn.get("value"),
                    txn.get("net_value"),
                    txn.get("commission") or 0,
                    txn.get("clearing_fees") or 0,
                    txn.get("regulatory_fees") or 0,
                    txn.get("description"),
                    option_type,
                    strike,
                    expiry,
                ),
            )

    def _match_realized_pnl(self) -> int:
        """Match open/close transaction pairs and compute realized P&L.

        Uses FIFO matching: earliest unmatched open is paired with the
        earliest unmatched close for the same symbol+portfolio.
        """
        matched = 0

        with self._conn() as conn:
            conn.row_factory = sqlite3.Row

            # Already-matched txn IDs
            existing = set()
            for row in conn.execute(
                "SELECT open_txn_id, close_txn_id FROM realized_pnl"
            ).fetchall():
                existing.add(row["open_txn_id"])
                existing.add(row["close_txn_id"])

            # Get all trade fills (Buy/Sell actions)
            fills = conn.execute(
                """SELECT id, ts, portfolio, symbol, underlying, action,
                          qty, price, option_type, strike, expiry,
                          commission, clearing_fees, regulatory_fees
                   FROM broker_transactions
                   WHERE action IN ('Buy to Open', 'Sell to Open',
                                    'Buy to Close', 'Sell to Close')
                   ORDER BY ts ASC"""
            ).fetchall()

        # Group by (portfolio, symbol)
        groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for f in fills:
            key = (f["portfolio"], f["symbol"])
            groups[key].append(dict(f))

        new_records = []
        for (portfolio, symbol), txn_list in groups.items():
            opens = [
                t for t in txn_list
                if t["action"] in ("Buy to Open", "Sell to Open")
                and t["id"] not in existing
            ]
            closes = [
                t for t in txn_list
                if t["action"] in ("Buy to Close", "Sell to Close")
                and t["id"] not in existing
            ]

            for close_txn in closes:
                if not opens:
                    break
                open_txn = opens.pop(0)

                qty = min(
                    abs(open_txn.get("qty") or 0),
                    abs(close_txn.get("qty") or 0),
                )
                if qty == 0:
                    continue

                open_price = open_txn.get("price") or 0
                close_price = close_txn.get("price") or 0

                # Determine sign: selling to open means credit, buying to close is debit
                if open_txn["action"] == "Sell to Open":
                    gross_pnl = (open_price - close_price) * qty * 100
                else:
                    gross_pnl = (close_price - open_price) * qty * 100

                open_fees = sum(
                    open_txn.get(f) or 0
                    for f in ("commission", "clearing_fees", "regulatory_fees")
                )
                close_fees = sum(
                    close_txn.get(f) or 0
                    for f in ("commission", "clearing_fees", "regulatory_fees")
                )
                total_fees = open_fees + close_fees
                net_pnl = gross_pnl - total_fees

                # Holding period
                holding_days = 0
                try:
                    open_dt = datetime.fromisoformat(open_txn["ts"].replace("Z", "+00:00"))
                    close_dt = datetime.fromisoformat(close_txn["ts"].replace("Z", "+00:00"))
                    holding_days = (close_dt - open_dt).days
                except Exception:
                    pass

                new_records.append((
                    close_txn["ts"],
                    portfolio,
                    symbol,
                    open_txn.get("underlying"),
                    open_txn["id"],
                    close_txn["id"],
                    qty,
                    open_price,
                    close_price,
                    gross_pnl,
                    total_fees,
                    net_pnl,
                    holding_days,
                    open_txn.get("option_type"),
                    open_txn.get("strike"),
                    open_txn.get("expiry"),
                ))
                matched += 1

        if new_records:
            with self._conn() as conn:
                conn.executemany(
                    """INSERT INTO realized_pnl
                       (ts, portfolio, symbol, underlying, open_txn_id, close_txn_id,
                        qty, open_price, close_price, gross_pnl, total_fees, net_pnl,
                        holding_days, option_type, strike, expiry)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    new_records,
                )

        return matched

    # ------------------------------------------------------------------
    # Position change detection
    # ------------------------------------------------------------------

    def detect_position_changes(
        self,
        adapter,
        streamer=None,
        portfolio_ids: Optional[list[str]] = None,
    ) -> list[dict]:
        """Compare current positions to last snapshot and log changes.

        Returns list of detected events: {"event": "opened"/"closed", ...}
        """
        pids = portfolio_ids or ["P1", "P2", "P3", "P4"]
        events: list[dict] = []

        for pid in pids:
            try:
                current_positions = adapter.get_positions(pid)
            except Exception as e:
                logger.warning("Could not get positions for %s: %s", pid, e)
                continue

            current_map = {p["symbol"]: p for p in current_positions}
            previous_map = self._last_positions.get(pid, {})

            # Detect new positions (opened)
            for sym, pos in current_map.items():
                if sym not in previous_map:
                    greeks = None
                    if streamer:
                        greeks = streamer.get_greeks(sym)

                    event = {
                        "event": "opened",
                        "portfolio": pid,
                        "symbol": sym,
                        "underlying": pos.get("underlying_symbol"),
                        "qty": pos.get("quantity"),
                        "direction": pos.get("quantity_direction"),
                        "price": pos.get("average_open_price"),
                        "greeks": greeks,
                    }
                    events.append(event)
                    self._snapshot_position(pid, pos, greeks, "opened")

            # Detect closed positions (disappeared)
            for sym, prev_pos in previous_map.items():
                if sym not in current_map:
                    greeks = None
                    if streamer:
                        greeks = streamer.get_greeks(sym)

                    event = {
                        "event": "closed",
                        "portfolio": pid,
                        "symbol": sym,
                        "underlying": prev_pos.get("underlying_symbol"),
                        "qty": prev_pos.get("quantity"),
                        "direction": prev_pos.get("quantity_direction"),
                        "last_mark": prev_pos.get("mark"),
                        "greeks": greeks,
                    }
                    events.append(event)
                    self._snapshot_position(pid, prev_pos, greeks, "closed")

                    # Try to pull transaction details for the close
                    self._backfill_close_txn(adapter, pid, sym)

            # Update stored positions
            self._last_positions[pid] = current_map

        if events:
            logger.info("Detected %d position changes", len(events))

        return events

    def _snapshot_position(
        self,
        portfolio: str,
        pos: dict,
        greeks: Optional[dict],
        event: str,
    ) -> None:
        ts = datetime.utcnow().isoformat()
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO position_snapshots
                   (ts, portfolio, symbol, underlying, qty, direction,
                    mark_price, greeks_json, event)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ts,
                    portfolio,
                    pos.get("symbol", ""),
                    pos.get("underlying_symbol"),
                    pos.get("quantity"),
                    pos.get("quantity_direction"),
                    pos.get("mark"),
                    json.dumps(greeks) if greeks else None,
                    event,
                ),
            )

    def _backfill_close_txn(self, adapter, portfolio: str, symbol: str) -> None:
        """Pull recent transactions to find the closing fill for a symbol."""
        try:
            txns = adapter.get_transactions(
                portfolio, start_date=date.today() - timedelta(days=2),
            )
            for txn in txns:
                if txn.get("symbol") == symbol and txn.get("action") in (
                    "Buy to Close", "Sell to Close",
                ):
                    broker_id = txn.get("id")
                    if broker_id and not self._txn_exists(broker_id):
                        self._store_broker_txn(portfolio, txn)
        except Exception as e:
            logger.debug("Could not backfill close txn for %s: %s", symbol, e)

    # ------------------------------------------------------------------
    # Greeks at entry/exit (called by position change detection)
    # ------------------------------------------------------------------

    def get_entry_greeks(self, symbol: str, portfolio: str) -> Optional[dict]:
        """Retrieve the Greeks snapshot recorded when a position was opened."""
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT greeks_json FROM position_snapshots
                   WHERE symbol = ? AND portfolio = ? AND event = 'opened'
                   ORDER BY ts DESC LIMIT 1""",
                (symbol, portfolio),
            ).fetchone()
            if row and row["greeks_json"]:
                return json.loads(row["greeks_json"])
        return None

    def get_exit_greeks(self, symbol: str, portfolio: str) -> Optional[dict]:
        """Retrieve the Greeks snapshot recorded when a position was closed."""
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT greeks_json FROM position_snapshots
                   WHERE symbol = ? AND portfolio = ? AND event = 'closed'
                   ORDER BY ts DESC LIMIT 1""",
                (symbol, portfolio),
            ).fetchone()
            if row and row["greeks_json"]:
                return json.loads(row["greeks_json"])
        return None

    def update_realized_pnl_greeks(self, portfolio: str, symbol: str) -> None:
        """Attach entry/exit Greeks to the most recent realized P&L record."""
        entry = self.get_entry_greeks(symbol, portfolio)
        exit_ = self.get_exit_greeks(symbol, portfolio)

        if not entry and not exit_:
            return

        with self._conn() as conn:
            conn.execute(
                """UPDATE realized_pnl
                   SET entry_greeks = ?, exit_greeks = ?
                   WHERE portfolio = ? AND symbol = ?
                   AND id = (
                       SELECT id FROM realized_pnl
                       WHERE portfolio = ? AND symbol = ?
                       ORDER BY ts DESC LIMIT 1
                   )""",
                (
                    json.dumps(entry) if entry else None,
                    json.dumps(exit_) if exit_ else None,
                    portfolio, symbol,
                    portfolio, symbol,
                ),
            )

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------

    def weekly_pnl_attribution(
        self,
        weeks_back: int = 1,
        portfolio: Optional[str] = None,
    ) -> dict:
        """P&L attribution broken down by delta, theta, vega for recent weeks."""
        cutoff = (date.today() - timedelta(weeks=weeks_back)).isoformat()
        query = """
            SELECT portfolio, symbol, underlying, net_pnl, holding_days,
                   option_type, entry_greeks, exit_greeks, ts
            FROM realized_pnl
            WHERE ts >= ?
        """
        params: list[Any] = [cutoff]
        if portfolio:
            query += " AND portfolio = ?"
            params.append(portfolio)
        query += " ORDER BY ts DESC"

        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()

        result: dict[str, dict] = {}
        for row in rows:
            pid = row["portfolio"]
            if pid not in result:
                result[pid] = {
                    "total_pnl": 0.0,
                    "delta_pnl": 0.0,
                    "theta_pnl": 0.0,
                    "vega_pnl": 0.0,
                    "fees": 0.0,
                    "trade_count": 0,
                    "trades": [],
                }

            entry_g = json.loads(row["entry_greeks"]) if row["entry_greeks"] else {}
            exit_g = json.loads(row["exit_greeks"]) if row["exit_greeks"] else {}

            # Approximate attribution from Greek differences
            delta_attr = 0.0
            theta_attr = 0.0
            vega_attr = 0.0

            if entry_g and exit_g:
                # Theta P&L ≈ avg_theta × holding_days
                avg_theta = ((entry_g.get("theta") or 0) + (exit_g.get("theta") or 0)) / 2
                theta_attr = avg_theta * (row["holding_days"] or 0) * 100  # per contract

                # Vega P&L ≈ avg_vega × IV change
                entry_iv = entry_g.get("volatility") or 0
                exit_iv = exit_g.get("volatility") or 0
                avg_vega = ((entry_g.get("vega") or 0) + (exit_g.get("vega") or 0)) / 2
                iv_change = (exit_iv - entry_iv) * 100  # in vol points
                vega_attr = avg_vega * iv_change * 100

                # Delta residual = net P&L - theta - vega
                delta_attr = row["net_pnl"] - theta_attr - vega_attr

            result[pid]["total_pnl"] += row["net_pnl"]
            result[pid]["delta_pnl"] += delta_attr
            result[pid]["theta_pnl"] += theta_attr
            result[pid]["vega_pnl"] += vega_attr
            result[pid]["trade_count"] += 1
            result[pid]["trades"].append({
                "symbol": row["symbol"],
                "underlying": row["underlying"],
                "net_pnl": row["net_pnl"],
                "holding_days": row["holding_days"],
                "delta_attr": delta_attr,
                "theta_attr": theta_attr,
                "vega_attr": vega_attr,
            })

        return result

    def win_rate_by(
        self,
        group_by: str = "portfolio",
        lookback_days: int = 90,
    ) -> dict[str, dict]:
        """Win rate grouped by portfolio, underlying, or option_type.

        Returns {group_key: {wins, losses, total, win_rate, avg_win, avg_loss}}
        """
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
        valid_groups = {"portfolio", "underlying", "option_type"}
        if group_by not in valid_groups:
            raise ValueError(f"group_by must be one of {valid_groups}")

        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""SELECT {group_by}, net_pnl FROM realized_pnl
                    WHERE ts >= ? ORDER BY ts DESC""",
                (cutoff,),
            ).fetchall()

        groups: dict[str, dict] = {}
        for row in rows:
            key = row[group_by] or "unknown"
            if key not in groups:
                groups[key] = {
                    "wins": 0, "losses": 0, "total": 0,
                    "win_pnl": 0.0, "loss_pnl": 0.0,
                }
            g = groups[key]
            g["total"] += 1
            if row["net_pnl"] >= 0:
                g["wins"] += 1
                g["win_pnl"] += row["net_pnl"]
            else:
                g["losses"] += 1
                g["loss_pnl"] += row["net_pnl"]

        result = {}
        for key, g in groups.items():
            result[key] = {
                "wins": g["wins"],
                "losses": g["losses"],
                "total": g["total"],
                "win_rate": g["wins"] / g["total"] if g["total"] else 0,
                "avg_win": g["win_pnl"] / g["wins"] if g["wins"] else 0,
                "avg_loss": g["loss_pnl"] / g["losses"] if g["losses"] else 0,
                "total_pnl": g["win_pnl"] + g["loss_pnl"],
            }
        return result

    def strategy_performance(
        self,
        lookback_days: int = 90,
        portfolio: Optional[str] = None,
    ) -> dict[str, dict]:
        """Performance summary grouped by underlying (strategy proxy).

        Returns {underlying: {total_pnl, trade_count, avg_pnl, avg_holding,
        win_rate, total_fees}}
        """
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
        query = """
            SELECT underlying, net_pnl, holding_days,
                   open_price, close_price, qty
            FROM realized_pnl WHERE ts >= ?
        """
        params: list[Any] = [cutoff]
        if portfolio:
            query += " AND portfolio = ?"
            params.append(portfolio)

        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()

        groups: dict[str, dict] = {}
        for row in rows:
            key = row["underlying"] or "unknown"
            if key not in groups:
                groups[key] = {
                    "total_pnl": 0.0,
                    "trade_count": 0,
                    "wins": 0,
                    "total_holding_days": 0,
                    "pnl_list": [],
                }
            g = groups[key]
            g["total_pnl"] += row["net_pnl"]
            g["trade_count"] += 1
            g["total_holding_days"] += row["holding_days"] or 0
            if row["net_pnl"] >= 0:
                g["wins"] += 1
            g["pnl_list"].append(row["net_pnl"])

        result = {}
        for key, g in groups.items():
            count = g["trade_count"]
            result[key] = {
                "total_pnl": g["total_pnl"],
                "trade_count": count,
                "avg_pnl": g["total_pnl"] / count if count else 0,
                "avg_holding_days": g["total_holding_days"] / count if count else 0,
                "win_rate": g["wins"] / count if count else 0,
            }
        return result

    # ------------------------------------------------------------------
    # Natural language query support
    # ------------------------------------------------------------------

    def query_trades(self, question: str) -> str:
        """Answer a natural language question about the trade journal.

        Detects intent and routes to the appropriate analytics method.
        """
        q = question.lower()

        if any(kw in q for kw in ["win rate", "win%", "batting"]):
            group = "portfolio"
            if "underlying" in q or "ticker" in q:
                group = "underlying"
            elif "option" in q or "put" in q or "call" in q:
                group = "option_type"
            data = self.win_rate_by(group_by=group)
            lines = [f"Win rate by {group}:"]
            for key, stats in data.items():
                lines.append(
                    f"  {key}: {stats['win_rate']:.0%} "
                    f"({stats['wins']}W/{stats['losses']}L) "
                    f"avg_win=${stats['avg_win']:,.0f} "
                    f"avg_loss=${stats['avg_loss']:,.0f} "
                    f"net=${stats['total_pnl']:,.0f}"
                )
            return "\n".join(lines)

        if any(kw in q for kw in ["pnl", "p&l", "attribution", "breakdown"]):
            weeks = 1
            if "month" in q:
                weeks = 4
            portfolio = None
            for pid in ["P1", "P2", "P3", "P4"]:
                if pid.lower() in q:
                    portfolio = pid
                    break
            data = self.weekly_pnl_attribution(weeks_back=weeks, portfolio=portfolio)
            lines = ["P&L Attribution:"]
            for pid, stats in data.items():
                lines.append(
                    f"  {pid}: net=${stats['total_pnl']:,.0f} "
                    f"(Δ=${stats['delta_pnl']:,.0f} "
                    f"Θ=${stats['theta_pnl']:,.0f} "
                    f"V=${stats['vega_pnl']:,.0f}) "
                    f"trades={stats['trade_count']}"
                )
            return "\n".join(lines) if len(lines) > 1 else "No realized P&L data for this period."

        if any(kw in q for kw in ["strategy", "performance", "by underlying"]):
            portfolio = None
            for pid in ["P1", "P2", "P3", "P4"]:
                if pid.lower() in q:
                    portfolio = pid
                    break
            data = self.strategy_performance(portfolio=portfolio)
            lines = ["Strategy performance by underlying:"]
            for key, stats in data.items():
                lines.append(
                    f"  {key}: ${stats['total_pnl']:,.0f} "
                    f"({stats['trade_count']} trades, "
                    f"{stats['win_rate']:.0%} win, "
                    f"avg hold {stats['avg_holding_days']:.0f}d)"
                )
            return "\n".join(lines) if len(lines) > 1 else "No strategy performance data."

        if any(kw in q for kw in ["recent", "last", "latest", "trade"]):
            portfolio = None
            for pid in ["P1", "P2", "P3", "P4"]:
                if pid.lower() in q:
                    portfolio = pid
                    break
            trades = self.get_recent_trades(portfolio=portfolio, limit=10)
            if not trades:
                return "No recent trades found."
            lines = ["Recent trades:"]
            for t in trades:
                lines.append(
                    f"  {t.get('ts', '')[:16]} {t.get('portfolio', '')} "
                    f"{t.get('ticker', '')} {t.get('action', '')} "
                    f"qty={t.get('qty', '')} @ ${t.get('price', '')}"
                )
            return "\n".join(lines)

        return "Could not parse query. Try: win rate, P&L attribution, strategy performance, or recent trades."

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_realized_pnl_csv(
        self,
        filepath: Optional[str | Path] = None,
        lookback_days: int = 90,
    ) -> str:
        """Export realized P&L to CSV. Returns CSV string if no filepath."""
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT ts, portfolio, symbol, underlying, qty,
                          open_price, close_price, gross_pnl, total_fees,
                          net_pnl, holding_days, option_type, strike, expiry
                   FROM realized_pnl WHERE ts >= ?
                   ORDER BY ts DESC""",
                (cutoff,),
            ).fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "date", "portfolio", "symbol", "underlying", "qty",
            "open_price", "close_price", "gross_pnl", "fees",
            "net_pnl", "holding_days", "option_type", "strike", "expiry",
        ])
        for row in rows:
            writer.writerow([
                row["ts"][:10], row["portfolio"], row["symbol"],
                row["underlying"], row["qty"],
                row["open_price"], row["close_price"],
                f"{row['gross_pnl']:.2f}", f"{row['total_fees']:.2f}",
                f"{row['net_pnl']:.2f}", row["holding_days"],
                row["option_type"], row["strike"], row["expiry"],
            ])

        csv_text = output.getvalue()

        if filepath:
            Path(filepath).write_text(csv_text)
            logger.info("Exported %d P&L records to %s", len(rows), filepath)

        return csv_text

    def export_broker_transactions_csv(
        self,
        filepath: Optional[str | Path] = None,
        lookback_days: int = 90,
    ) -> str:
        """Export raw broker transactions to CSV."""
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT ts, portfolio, symbol, underlying, instrument_type,
                          transaction_type, action, qty, price, value,
                          net_value, commission, clearing_fees, regulatory_fees,
                          description
                   FROM broker_transactions WHERE ts >= ?
                   ORDER BY ts DESC""",
                (cutoff,),
            ).fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "date", "portfolio", "symbol", "underlying", "instrument_type",
            "type", "action", "qty", "price", "value", "net_value",
            "commission", "clearing_fees", "reg_fees", "description",
        ])
        for row in rows:
            writer.writerow([
                row["ts"][:19], row["portfolio"], row["symbol"],
                row["underlying"], row["instrument_type"],
                row["transaction_type"], row["action"],
                row["qty"], row["price"], row["value"], row["net_value"],
                row["commission"], row["clearing_fees"],
                row["regulatory_fees"], row["description"],
            ])

        csv_text = output.getvalue()

        if filepath:
            Path(filepath).write_text(csv_text)

        return csv_text

    # ------------------------------------------------------------------
    # Trade logging (original manual API — still available)
    # ------------------------------------------------------------------

    def log_trade(
        self,
        portfolio: str,
        ticker: str,
        action: str,
        option_type: Optional[str] = None,
        strike: Optional[float] = None,
        expiry: Optional[str] = None,
        qty: Optional[int] = None,
        price: Optional[float] = None,
        structure_tag: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> int:
        ts = datetime.utcnow().isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """INSERT INTO trades
                   (ts, portfolio, ticker, action, option_type, strike, expiry, qty, price, structure_tag, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ts, portfolio, ticker, action, option_type, strike, expiry, qty, price, structure_tag, notes),
            )
            return cur.lastrowid  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Alert logging
    # ------------------------------------------------------------------

    def log_alert(
        self,
        rule_id: str,
        severity: str,
        message: str,
        portfolio: str = "",
        ticker: str = "",
        value: float = 0.0,
        threshold: float = 0.0,
    ) -> None:
        ts = datetime.utcnow().isoformat()
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO rule_alerts
                   (ts, rule_id, severity, portfolio, ticker, message, value, threshold)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (ts, rule_id, severity, portfolio, ticker, message, value, threshold),
            )

    # ------------------------------------------------------------------
    # AI review logging
    # ------------------------------------------------------------------

    def log_ai_review(
        self,
        review_type: str,
        ai_output: str,
        portfolio: str = "",
        input_summary: str = "",
        cost_usd: float = 0.0,
    ) -> None:
        ts = datetime.utcnow().isoformat()
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO ai_reviews
                   (ts, review_type, portfolio, input_summary, ai_output, cost_usd)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (ts, review_type, portfolio, input_summary, ai_output, cost_usd),
            )

    # ------------------------------------------------------------------
    # Queries (original API)
    # ------------------------------------------------------------------

    def get_recent_trades(self, portfolio: Optional[str] = None, limit: int = 50) -> list[dict]:
        query = "SELECT * FROM trades"
        params: list[Any] = []
        if portfolio:
            query += " WHERE portfolio = ?"
            params.append(portfolio)
        query += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def get_recent_alerts(self, severity: Optional[str] = None, limit: int = 100) -> list[dict]:
        query = "SELECT * FROM rule_alerts"
        params: list[Any] = []
        if severity:
            query += " WHERE severity = ?"
            params.append(severity)
        query += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def get_recent_ai_reviews(self, review_type: Optional[str] = None, limit: int = 10) -> list[dict]:
        query = "SELECT * FROM ai_reviews"
        params: list[Any] = []
        if review_type:
            query += " WHERE review_type = ?"
            params.append(review_type)
        query += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def get_daily_ai_cost(self, date_str: Optional[str] = None) -> float:
        if date_str is None:
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
        with self._conn() as conn:
            row = conn.execute(
                "SELECT SUM(cost_usd) FROM ai_reviews WHERE ts LIKE ?",
                (f"{date_str}%",),
            ).fetchone()
            return float(row[0] or 0.0)

    def get_realized_pnl(
        self,
        portfolio: Optional[str] = None,
        lookback_days: int = 90,
        limit: int = 100,
    ) -> list[dict]:
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
        query = "SELECT * FROM realized_pnl WHERE ts >= ?"
        params: list[Any] = [cutoff]
        if portfolio:
            query += " AND portfolio = ?"
            params.append(portfolio)
        query += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def get_broker_transactions(
        self,
        portfolio: Optional[str] = None,
        lookback_days: int = 30,
        limit: int = 200,
    ) -> list[dict]:
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
        query = "SELECT * FROM broker_transactions WHERE ts >= ?"
        params: list[Any] = [cutoff]
        if portfolio:
            query += " AND portfolio = ?"
            params.append(portfolio)
        query += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_option_fields(symbol: str) -> dict:
    """Extract option_type, strike, expiry from OCC option symbol."""
    result: dict[str, Any] = {}
    try:
        raw = symbol.rstrip()
        rest = raw[-15:]
        yy, mm, dd = rest[0:2], rest[2:4], rest[4:6]
        opt_char = rest[6]
        strike_raw = rest[7:]
        result["expiry"] = f"20{yy}-{mm}-{dd}"
        result["option_type"] = "Call" if opt_char == "C" else "Put"
        result["strike"] = int(strike_raw) / 1000.0
    except Exception:
        pass
    return result

"""
Trade Journal — SQLite-backed log of all trades, AI reviews, and rule alerts.
Provides query methods for historical analysis.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


DDL = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    portfolio TEXT NOT NULL,
    ticker TEXT NOT NULL,
    action TEXT NOT NULL,         -- OPEN / CLOSE / ROLL / ADJUST
    option_type TEXT,             -- call / put
    strike REAL,
    expiry TEXT,
    qty INTEGER,
    price REAL,
    structure_tag TEXT,
    notes TEXT
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
    review_type TEXT NOT NULL,    -- daily / trade_review / what_if
    portfolio TEXT,
    input_summary TEXT,
    ai_output TEXT,
    cost_usd REAL DEFAULT 0
);
"""


class TradeJournal:
    """Persistent journal backed by SQLite."""

    def __init__(self, db_path: str | Path = "./options_cio.db") -> None:
        self.db_path = Path(db_path)
        self._init_db()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(DDL)

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    # ------------------------------------------------------------------
    # Trade logging
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
    # Queries
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

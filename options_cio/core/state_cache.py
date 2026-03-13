"""
State Cache — SQLite-backed persistent cache for positions, greeks snapshots,
rule violations, portfolio state, and market context.

Uses WAL mode for concurrent reads and a thread-local connection pool.
Tables are auto-created on first run.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional


class StateCache:
    """
    SQLite-backed state cache.

    Usage:
        cache = StateCache("options_cio.db")
        cache.save_positions([{...}, ...])
        greeks = cache.get_latest_greeks("P1")
    """

    _SCHEMA = [
        """
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio TEXT NOT NULL,
            ticker TEXT NOT NULL,
            option_type TEXT NOT NULL,
            strike REAL NOT NULL,
            expiry TEXT NOT NULL,
            qty INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            structure_tag TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS greeks_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            portfolio_id TEXT NOT NULL,
            delta REAL,
            gamma REAL,
            theta REAL,
            vega REAL,
            rho REAL,
            net_delta REAL,
            net_gamma REAL,
            net_theta REAL,
            net_vega REAL,
            extra_json TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS rule_violations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            rule_id TEXT NOT NULL,
            portfolio_id TEXT NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS portfolio_state (
            portfolio_id TEXT PRIMARY KEY,
            capital REAL,
            deployed REAL,
            deployment_pct REAL,
            pnl_open REAL,
            pnl_day REAL,
            hedge_ratio REAL,
            state_json TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS market_context (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            spx REAL,
            vix REAL,
            tnx REAL,
            btc REAL,
            context_json TEXT
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_greeks_portfolio ON greeks_snapshots(portfolio_id, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_violations_date ON rule_violations(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_violations_portfolio ON rule_violations(portfolio_id)",
        "CREATE INDEX IF NOT EXISTS idx_market_ts ON market_context(timestamp)",
    ]

    def __init__(self, db_path: str = "options_cio.db") -> None:
        self.db_path = db_path if db_path == ":memory:" else str(Path(db_path).resolve())
        self._local = threading.local()
        self._init_db()

    # ------------------------------------------------------------------
    # Connection management (thread-safe)
    # ------------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        """Return a thread-local connection, creating one if needed."""
        conn: Optional[sqlite3.Connection] = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        for stmt in self._SCHEMA:
            conn.execute(stmt)
        conn.commit()

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    def save_positions(self, positions: list[dict]) -> None:
        """Replace all positions with a fresh snapshot from the CSV."""
        conn = self._get_conn()
        conn.execute("DELETE FROM positions")
        for p in positions:
            conn.execute(
                """
                INSERT INTO positions
                    (portfolio, ticker, option_type, strike, expiry, qty, entry_price, structure_tag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    p["portfolio"],
                    p["ticker"],
                    p["option_type"],
                    p["strike"],
                    p["expiry"],
                    p["qty"],
                    p["entry_price"],
                    p.get("structure_tag", ""),
                ),
            )
        conn.commit()

    def get_positions(self, portfolio_id: Optional[str] = None) -> list[dict]:
        conn = self._get_conn()
        if portfolio_id:
            rows = conn.execute(
                "SELECT * FROM positions WHERE portfolio = ?", (portfolio_id,)
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM positions").fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Greeks snapshots
    # ------------------------------------------------------------------

    def save_greeks_snapshot(
        self, timestamp: str, portfolio_id: str, greeks: dict
    ) -> None:
        """Save a point-in-time greeks snapshot for a portfolio."""
        conn = self._get_conn()
        extra = {
            k: v
            for k, v in greeks.items()
            if k
            not in (
                "delta",
                "gamma",
                "theta",
                "vega",
                "rho",
                "net_delta",
                "net_gamma",
                "net_theta",
                "net_vega",
            )
        }
        conn.execute(
            """
            INSERT INTO greeks_snapshots
                (timestamp, portfolio_id, delta, gamma, theta, vega, rho,
                 net_delta, net_gamma, net_theta, net_vega, extra_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                portfolio_id,
                greeks.get("delta"),
                greeks.get("gamma"),
                greeks.get("theta"),
                greeks.get("vega"),
                greeks.get("rho"),
                greeks.get("net_delta"),
                greeks.get("net_gamma"),
                greeks.get("net_theta"),
                greeks.get("net_vega"),
                json.dumps(extra) if extra else None,
            ),
        )
        conn.commit()

    def get_latest_greeks(self, portfolio_id: str) -> dict:
        """Return the most recent greeks snapshot for a portfolio."""
        conn = self._get_conn()
        row = conn.execute(
            """
            SELECT * FROM greeks_snapshots
            WHERE portfolio_id = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (portfolio_id,),
        ).fetchone()
        if row is None:
            return {}
        result = dict(row)
        if result.get("extra_json"):
            result.update(json.loads(result["extra_json"]))
            del result["extra_json"]
        return result

    # ------------------------------------------------------------------
    # Rule violations
    # ------------------------------------------------------------------

    def save_violation(
        self,
        timestamp: str,
        rule_id: str,
        portfolio_id: str,
        severity: str,
        message: str,
    ) -> None:
        conn = self._get_conn()
        conn.execute(
            """
            INSERT INTO rule_violations (timestamp, rule_id, portfolio_id, severity, message)
            VALUES (?, ?, ?, ?, ?)
            """,
            (timestamp, rule_id, portfolio_id, severity, message),
        )
        conn.commit()

    def get_violations_today(
        self, portfolio_id: Optional[str] = None
    ) -> list[dict]:
        """Return all violations logged today, optionally filtered by portfolio."""
        conn = self._get_conn()
        today = date.today().isoformat()
        if portfolio_id:
            rows = conn.execute(
                """
                SELECT * FROM rule_violations
                WHERE timestamp >= ? AND portfolio_id = ?
                ORDER BY timestamp DESC
                """,
                (today, portfolio_id),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM rule_violations
                WHERE timestamp >= ?
                ORDER BY timestamp DESC
                """,
                (today,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Portfolio state
    # ------------------------------------------------------------------

    def save_portfolio_state(self, portfolio_id: str, state: dict) -> None:
        """Upsert the aggregate state for a portfolio."""
        conn = self._get_conn()
        conn.execute(
            """
            INSERT INTO portfolio_state
                (portfolio_id, capital, deployed, deployment_pct,
                 pnl_open, pnl_day, hedge_ratio, state_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(portfolio_id) DO UPDATE SET
                capital = excluded.capital,
                deployed = excluded.deployed,
                deployment_pct = excluded.deployment_pct,
                pnl_open = excluded.pnl_open,
                pnl_day = excluded.pnl_day,
                hedge_ratio = excluded.hedge_ratio,
                state_json = excluded.state_json,
                updated_at = excluded.updated_at
            """,
            (
                portfolio_id,
                state.get("capital"),
                state.get("deployed"),
                state.get("deployment_pct"),
                state.get("pnl_open"),
                state.get("pnl_day"),
                state.get("hedge_ratio"),
                json.dumps(
                    {
                        k: v
                        for k, v in state.items()
                        if k
                        not in (
                            "capital",
                            "deployed",
                            "deployment_pct",
                            "pnl_open",
                            "pnl_day",
                            "hedge_ratio",
                        )
                    }
                ),
            ),
        )
        conn.commit()

    def get_portfolio_state(self, portfolio_id: str) -> dict:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM portfolio_state WHERE portfolio_id = ?",
            (portfolio_id,),
        ).fetchone()
        if row is None:
            return {}
        result = dict(row)
        if result.get("state_json"):
            result.update(json.loads(result["state_json"]))
            del result["state_json"]
        return result

    # ------------------------------------------------------------------
    # Market context
    # ------------------------------------------------------------------

    def save_market_context(self, context: dict) -> None:
        """Save a market environment snapshot."""
        conn = self._get_conn()
        extra = {
            k: v for k, v in context.items() if k not in ("spx", "vix", "tnx", "btc")
        }
        conn.execute(
            """
            INSERT INTO market_context (timestamp, spx, vix, tnx, btc, context_json)
            VALUES (datetime('now'), ?, ?, ?, ?, ?)
            """,
            (
                context.get("spx"),
                context.get("vix"),
                context.get("tnx"),
                context.get("btc"),
                json.dumps(extra) if extra else None,
            ),
        )
        conn.commit()

    def get_latest_market_context(self) -> dict:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM market_context ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return {}
        result = dict(row)
        if result.get("context_json"):
            result.update(json.loads(result["context_json"]))
            del result["context_json"]
        return result

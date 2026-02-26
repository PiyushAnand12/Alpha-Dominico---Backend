"""
Database layer — SQLite via sqlite3
Tables:
  subscribers     — email, stripe info, subscription status
  daily_reports   — each day's screener output (JSON blob)
  focus_list      — top 10 focus stocks per day
  report_sends    — email delivery log per subscriber per day
"""

import sqlite3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("APP_DB_PATH", "sepa_app.db")


def get_conn() -> sqlite3.Connection:
    """Return a thread-safe SQLite connection with row_factory."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ── DDL ────────────────────────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS subscribers (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    email                   TEXT    NOT NULL UNIQUE,
    stripe_customer_id      TEXT    DEFAULT NULL,
    stripe_subscription_id  TEXT    DEFAULT NULL,
    subscription_status     TEXT    DEFAULT 'none',
    -- 'none' | 'trialing' | 'active' | 'past_due' | 'canceled' | 'unpaid'
    trial_end               TEXT    DEFAULT NULL,
    current_period_end      TEXT    DEFAULT NULL,
    plan                    TEXT    DEFAULT 'growth_pro',
    created_at              TEXT    NOT NULL,
    updated_at              TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_reports (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date     TEXT    NOT NULL UNIQUE,    -- YYYY-MM-DD
    regime          TEXT,
    total_scanned   INTEGER DEFAULT 0,
    full_qualifiers INTEGER DEFAULT 0,
    strict_qualifiers INTEGER DEFAULT 0,
    new_count       INTEGER DEFAULT 0,
    dropped_count   INTEGER DEFAULT 0,
    breadth_pct     REAL    DEFAULT NULL,
    report_html     TEXT,                       -- full HTML blob
    report_json     TEXT,                       -- JSON summary
    created_at      TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS focus_list (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date TEXT    NOT NULL,
    ticker      TEXT    NOT NULL,
    rank        INTEGER NOT NULL,
    sepa_score  INTEGER,
    rs_rank     INTEGER,
    price       REAL,
    sector      TEXT,
    change_type TEXT,
    is_strict   INTEGER DEFAULT 0,
    UNIQUE(report_date, ticker)
);

CREATE TABLE IF NOT EXISTS report_sends (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    subscriber_id   INTEGER NOT NULL REFERENCES subscribers(id),
    report_date     TEXT    NOT NULL,
    sent_at         TEXT,
    status          TEXT    DEFAULT 'pending',   -- 'sent' | 'failed' | 'pending'
    error_msg       TEXT    DEFAULT NULL,
    UNIQUE(subscriber_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_sub_stripe_cust ON subscribers(stripe_customer_id);
CREATE INDEX IF NOT EXISTS idx_sub_stripe_sub  ON subscribers(stripe_subscription_id);
CREATE INDEX IF NOT EXISTS idx_focus_date       ON focus_list(report_date);
CREATE INDEX IF NOT EXISTS idx_sends_sub        ON report_sends(subscriber_id);
"""


def init_db() -> None:
    """Create all tables if they don't exist."""
    with get_conn() as conn:
        conn.executescript(DDL)


# ── Subscriber helpers ─────────────────────────────────────────────────

def upsert_subscriber(email: str) -> dict:
    """Insert subscriber if not exists, return row."""
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO subscribers (email, created_at, updated_at)
               VALUES (?, ?, ?)""",
            (email.lower().strip(), now, now)
        )
        row = conn.execute(
            "SELECT * FROM subscribers WHERE email = ?",
            (email.lower().strip(),)
        ).fetchone()
    return dict(row)


def get_subscriber_by_email(email: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM subscribers WHERE email = ?",
            (email.lower().strip(),)
        ).fetchone()
    return dict(row) if row else None


def get_subscriber_by_stripe_customer(customer_id: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM subscribers WHERE stripe_customer_id = ?",
            (customer_id,)
        ).fetchone()
    return dict(row) if row else None


def get_subscriber_by_stripe_sub(sub_id: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM subscribers WHERE stripe_subscription_id = ?",
            (sub_id,)
        ).fetchone()
    return dict(row) if row else None


def update_subscriber_stripe(
    email: str,
    customer_id: str,
    subscription_id: str,
    status: str,
    trial_end: str | None = None,
    period_end: str | None = None,
) -> None:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """UPDATE subscribers
               SET stripe_customer_id     = ?,
                   stripe_subscription_id = ?,
                   subscription_status    = ?,
                   trial_end              = ?,
                   current_period_end     = ?,
                   updated_at             = ?
               WHERE email = ?""",
            (customer_id, subscription_id, status,
             trial_end, period_end, now, email.lower().strip())
        )


def update_subscription_status(subscription_id: str, status: str, period_end: str | None = None) -> None:
    """Called from Stripe webhook events."""
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """UPDATE subscribers
               SET subscription_status = ?,
                   current_period_end  = COALESCE(?, current_period_end),
                   updated_at          = ?
               WHERE stripe_subscription_id = ?""",
            (status, period_end, now, subscription_id)
        )


def get_active_subscribers() -> list[dict]:
    """Return all subscribers eligible to receive reports."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT * FROM subscribers
               WHERE subscription_status IN ('active', 'trialing')"""
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_subscribers() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM subscribers ORDER BY created_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


# ── Report helpers ─────────────────────────────────────────────────────

def save_daily_report(
    report_date: str,
    regime: str,
    total_scanned: int,
    full_qualifiers: int,
    strict_qualifiers: int,
    new_count: int,
    dropped_count: int,
    breadth_pct: float | None,
    report_html: str,
    report_json: str,
) -> None:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO daily_reports
               (report_date, regime, total_scanned, full_qualifiers,
                strict_qualifiers, new_count, dropped_count, breadth_pct,
                report_html, report_json, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (report_date, regime, total_scanned, full_qualifiers,
             strict_qualifiers, new_count, dropped_count, breadth_pct,
             report_html, report_json, now)
        )


def save_focus_list(report_date: str, focus_stocks: list[dict]) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM focus_list WHERE report_date = ?", (report_date,))
        for stock in focus_stocks:
            conn.execute(
                """INSERT OR IGNORE INTO focus_list
                   (report_date, ticker, rank, sepa_score, rs_rank, price,
                    sector, change_type, is_strict)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    report_date,
                    stock.get("ticker"),
                    stock.get("rank"),
                    stock.get("sepa_score"),
                    stock.get("rs_rank"),
                    stock.get("price"),
                    stock.get("sector"),
                    stock.get("change_type"),
                    1 if stock.get("is_strict") else 0,
                )
            )


def get_latest_report() -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM daily_reports ORDER BY report_date DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def get_focus_list(report_date: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM focus_list WHERE report_date = ? ORDER BY rank",
            (report_date,)
        ).fetchall()
    return [dict(r) for r in rows]


# ── Send log helpers ───────────────────────────────────────────────────

def log_report_send(subscriber_id: int, report_date: str, status: str, error: str | None = None) -> None:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO report_sends
               (subscriber_id, report_date, sent_at, status, error_msg)
               VALUES (?,?,?,?,?)""",
            (subscriber_id, report_date, now, status, error)
        )

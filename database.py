"""
Database layer — ALPHAdominico (database.py)

Audit fix [C-05] applied (March 2026):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PROBLEM:  SQLite was used for ALL data, including user records
          (subscribers, waitlist leads). Render.com resets its
          filesystem on every deployment, wiping the SQLite file
          and permanently deleting all user data.

SOLUTION: Two-tier storage strategy:
  • Supabase (PostgreSQL) — user-facing data that must survive
    deployments:
      - subscribers       (Stripe-linked paid users)
    Note: waitlist_leads and waitlist_feedback are managed by
    waitlist_service.py using its own Supabase client.

  • SQLite (local file)  — screener operational data that is
    ephemeral by design and regenerated daily:
      - daily_reports     (today's screener output HTML/JSON)
      - focus_list        (top 10 stocks per day)
      - report_sends      (email delivery log)
    These tables are rebuilt from screener runs, so losing them
    on a redeploy is acceptable — we just re-run the screener.

HOW TO TOGGLE:
  Set USE_SUPABASE=true in your Render environment variables to
  use Supabase for subscriber data (required in production).
  Set USE_SUPABASE=false (or omit it) for local SQLite-only dev.

SUPABASE SETUP:
  Run the SQL in the docstring of `_supabase_ddl_comment()` in
  your Supabase SQL editor before enabling USE_SUPABASE=true.

Required environment variables:
  SUPABASE_URL       — https://xxxx.supabase.co
  SUPABASE_KEY       — your service-role key (keep secret — never
                       expose to the frontend)
  USE_SUPABASE       — "true" | "false"  (default: false)
  APP_DB_PATH        — path to SQLite file (default: sepa_app.db)

Required packages (add to requirements.txt if not present):
  supabase>=2.0.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os
import sqlite3
import logging
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ── Feature flag ───────────────────────────────────────────────────────
USE_SUPABASE: bool = os.getenv("USE_SUPABASE", "false").lower() == "true"
DB_PATH: str      = os.getenv("APP_DB_PATH", "sepa_app.db")


# ══════════════════════════════════════════════════════════════════════
#  SUPABASE — user-facing persistent data
# ══════════════════════════════════════════════════════════════════════

def _get_supabase_client():
    """
    Return a Supabase client.  Raises a clear error if credentials
    are missing so the issue is obvious at startup rather than at
    the first user interaction.
    """
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_KEY", "").strip()

    if not url or not key:
        raise EnvironmentError(
            "SUPABASE_URL and SUPABASE_KEY must be set when USE_SUPABASE=true. "
            "Add them to your .env file or Render environment variables."
        )

    try:
        from supabase import create_client
        return create_client(url, key)
    except ImportError:
        raise ImportError(
            "The 'supabase' package is required when USE_SUPABASE=true. "
            "Run: pip install supabase>=2.0.0"
        )


def _supabase_ddl_comment():
    """
    NOT CALLED AT RUNTIME — documentation only.

    Copy and run this SQL in your Supabase SQL editor
    (Dashboard → SQL Editor → New query) BEFORE setting
    USE_SUPABASE=true:

    ── subscribers ────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS public.subscribers (
        id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        email                   text NOT NULL UNIQUE,
        stripe_customer_id      text DEFAULT NULL,
        stripe_subscription_id  text DEFAULT NULL,
        subscription_status     text NOT NULL DEFAULT 'none',
        trial_end               timestamptz DEFAULT NULL,
        current_period_end      timestamptz DEFAULT NULL,
        plan                    text DEFAULT 'free',
        created_at              timestamptz NOT NULL DEFAULT now(),
        updated_at              timestamptz NOT NULL DEFAULT now()
    );

    -- Indexes for Stripe webhook lookups
    CREATE INDEX IF NOT EXISTS idx_sub_stripe_cust
        ON public.subscribers(stripe_customer_id);
    CREATE INDEX IF NOT EXISTS idx_sub_stripe_sub
        ON public.subscribers(stripe_subscription_id);

    -- Row Level Security: block all direct frontend access.
    -- Only your backend service-role key can read/write.
    ALTER TABLE public.subscribers ENABLE ROW LEVEL SECURITY;
    -- No SELECT/INSERT/UPDATE/DELETE policies for 'authenticated'
    -- or 'anon' roles — only the service key bypasses RLS.

    ── reviews (for the review form on index.html) ─────────────────
    CREATE TABLE IF NOT EXISTS public.reviews (
        id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        rating     smallint NOT NULL CHECK (rating BETWEEN 1 AND 5),
        name       text NOT NULL,
        email      text NOT NULL,
        text       text NOT NULL,
        role       text DEFAULT '',
        created_at timestamptz NOT NULL DEFAULT now()
    );
    ALTER TABLE public.reviews ENABLE ROW LEVEL SECURITY;
    """
    pass  # documentation only — never called


# ══════════════════════════════════════════════════════════════════════
#  SQLITE — screener operational data (ephemeral, regenerated daily)
# ══════════════════════════════════════════════════════════════════════

# DDL for screener-only tables that live in SQLite.
# subscribers table kept here ONLY as local-dev fallback when
# USE_SUPABASE=false.
_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS subscribers (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    email                   TEXT    NOT NULL UNIQUE,
    stripe_customer_id      TEXT    DEFAULT NULL,
    stripe_subscription_id  TEXT    DEFAULT NULL,
    subscription_status     TEXT    NOT NULL DEFAULT 'none',
    trial_end               TEXT    DEFAULT NULL,
    current_period_end      TEXT    DEFAULT NULL,
    plan                    TEXT    DEFAULT 'free',
    created_at              TEXT    NOT NULL,
    updated_at              TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_reports (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date       TEXT NOT NULL UNIQUE,
    regime            TEXT,
    total_scanned     INTEGER DEFAULT 0,
    full_qualifiers   INTEGER DEFAULT 0,
    strict_qualifiers INTEGER DEFAULT 0,
    new_count         INTEGER DEFAULT 0,
    dropped_count     INTEGER DEFAULT 0,
    breadth_pct       REAL    DEFAULT NULL,
    report_html       TEXT,
    report_json       TEXT,
    created_at        TEXT    NOT NULL
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
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    subscriber_id INTEGER NOT NULL REFERENCES subscribers(id),
    report_date   TEXT    NOT NULL,
    sent_at       TEXT,
    status        TEXT    NOT NULL DEFAULT 'pending',
    error_msg     TEXT    DEFAULT NULL,
    UNIQUE(subscriber_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_sub_stripe_cust ON subscribers(stripe_customer_id);
CREATE INDEX IF NOT EXISTS idx_sub_stripe_sub  ON subscribers(stripe_subscription_id);
CREATE INDEX IF NOT EXISTS idx_focus_date      ON focus_list(report_date);
CREATE INDEX IF NOT EXISTS idx_sends_sub       ON report_sends(subscriber_id);
"""


def _get_sqlite_conn() -> sqlite3.Connection:
    """Return a WAL-mode SQLite connection with row factory."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """
    Initialise all database layers:
    - Always create SQLite tables for screener operational data.
    - If USE_SUPABASE=true, verify the Supabase connection is healthy
      and log a warning if credentials are missing.
    - Also initialises waitlist tables via waitlist_service.
    """
    # SQLite — screener tables always created locally
    try:
        with _get_sqlite_conn() as conn:
            conn.executescript(_SQLITE_DDL)
        log.info("SQLite tables initialised at: %s", DB_PATH)
    except Exception as exc:
        log.error("Failed to initialise SQLite: %s", exc)
        raise

    # Waitlist tables (managed by waitlist_service.py)
    try:
        from waitlist_service import WAITLIST_TABLES_SQL
        with _get_sqlite_conn() as conn:
            conn.executescript(WAITLIST_TABLES_SQL)
        log.info("Waitlist tables initialised.")
    except ImportError:
        log.warning("waitlist_service not found — waitlist tables skipped.")
    except Exception as exc:
        log.error("Failed to initialise waitlist tables: %s", exc)

    # Supabase — verify connection if enabled
    if USE_SUPABASE:
        try:
            sb = _get_supabase_client()
            # Lightweight connectivity check
            sb.table("subscribers").select("id").limit(1).execute()
            log.info("Supabase connection verified — subscriber data will use Supabase.")
        except Exception as exc:
            log.error(
                "Supabase connection check FAILED: %s\n"
                "Subscriber data will fall back to SQLite. "
                "This is NOT safe for production — fix SUPABASE_URL / SUPABASE_KEY.",
                exc,
            )
    else:
        log.warning(
            "USE_SUPABASE=false — subscriber data is stored in SQLite (%s). "
            "This is fine for local development but MUST be set to true in "
            "production to prevent data loss on deployment.",
            DB_PATH,
        )


# ══════════════════════════════════════════════════════════════════════
#  SUBSCRIBER HELPERS — route through Supabase or SQLite based on flag
# ══════════════════════════════════════════════════════════════════════

def upsert_subscriber(email: str) -> dict:
    """
    Insert subscriber if not exists; return the full row.
    Uses Supabase when USE_SUPABASE=true, SQLite otherwise.
    """
    clean_email = email.lower().strip()
    now = datetime.utcnow().isoformat()

    if USE_SUPABASE:
        try:
            sb = _get_supabase_client()
            # upsert: insert or do nothing on conflict, then fetch
            sb.table("subscribers").upsert(
                {"email": clean_email, "updated_at": now},
                on_conflict="email",
                ignore_duplicates=True,
            ).execute()
            result = (
                sb.table("subscribers")
                .select("*")
                .eq("email", clean_email)
                .single()
                .execute()
            )
            return result.data or {}
        except Exception as exc:
            log.error("upsert_subscriber (Supabase) failed for email hash: %s",
                      _hash_for_log(clean_email), exc_info=True)
            raise

    # SQLite fallback (local dev)
    with _get_sqlite_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO subscribers (email, created_at, updated_at) VALUES (?,?,?)",
            (clean_email, now, now),
        )
        row = conn.execute(
            "SELECT * FROM subscribers WHERE email = ?", (clean_email,)
        ).fetchone()
    return dict(row) if row else {}


def get_subscriber_by_email(email: str) -> Optional[dict]:
    clean_email = email.lower().strip()

    if USE_SUPABASE:
        try:
            sb = _get_supabase_client()
            result = (
                sb.table("subscribers")
                .select("*")
                .eq("email", clean_email)
                .maybe_single()
                .execute()
            )
            return result.data
        except Exception as exc:
            log.error("get_subscriber_by_email (Supabase) error: %s", exc, exc_info=True)
            raise

    with _get_sqlite_conn() as conn:
        row = conn.execute(
            "SELECT * FROM subscribers WHERE email = ?", (clean_email,)
        ).fetchone()
    return dict(row) if row else None


def get_subscriber_by_stripe_customer(customer_id: str) -> Optional[dict]:
    if USE_SUPABASE:
        try:
            sb = _get_supabase_client()
            result = (
                sb.table("subscribers")
                .select("*")
                .eq("stripe_customer_id", customer_id)
                .maybe_single()
                .execute()
            )
            return result.data
        except Exception as exc:
            log.error("get_subscriber_by_stripe_customer (Supabase) error: %s", exc)
            raise

    with _get_sqlite_conn() as conn:
        row = conn.execute(
            "SELECT * FROM subscribers WHERE stripe_customer_id = ?", (customer_id,)
        ).fetchone()
    return dict(row) if row else None


def get_subscriber_by_stripe_sub(sub_id: str) -> Optional[dict]:
    if USE_SUPABASE:
        try:
            sb = _get_supabase_client()
            result = (
                sb.table("subscribers")
                .select("*")
                .eq("stripe_subscription_id", sub_id)
                .maybe_single()
                .execute()
            )
            return result.data
        except Exception as exc:
            log.error("get_subscriber_by_stripe_sub (Supabase) error: %s", exc)
            raise

    with _get_sqlite_conn() as conn:
        row = conn.execute(
            "SELECT * FROM subscribers WHERE stripe_subscription_id = ?", (sub_id,)
        ).fetchone()
    return dict(row) if row else None


def update_subscriber_stripe(
    email: str,
    customer_id: str,
    subscription_id: str,
    status: str,
    trial_end: Optional[str] = None,
    period_end: Optional[str] = None,
) -> None:
    clean_email = email.lower().strip()
    now = datetime.utcnow().isoformat()

    if USE_SUPABASE:
        try:
            sb = _get_supabase_client()
            sb.table("subscribers").update({
                "stripe_customer_id":     customer_id,
                "stripe_subscription_id": subscription_id,
                "subscription_status":    status,
                "trial_end":              trial_end,
                "current_period_end":     period_end,
                "updated_at":             now,
            }).eq("email", clean_email).execute()
            return
        except Exception as exc:
            log.error("update_subscriber_stripe (Supabase) error: %s", exc)
            raise

    with _get_sqlite_conn() as conn:
        conn.execute(
            """UPDATE subscribers
               SET stripe_customer_id=?, stripe_subscription_id=?,
                   subscription_status=?, trial_end=?,
                   current_period_end=?, updated_at=?
               WHERE email=?""",
            (customer_id, subscription_id, status,
             trial_end, period_end, now, clean_email),
        )


def update_subscription_status(
    subscription_id: str,
    status: str,
    period_end: Optional[str] = None,
) -> None:
    """Called from Stripe webhook events."""
    now = datetime.utcnow().isoformat()

    if USE_SUPABASE:
        try:
            sb = _get_supabase_client()
            update_payload = {"subscription_status": status, "updated_at": now}
            if period_end:
                update_payload["current_period_end"] = period_end
            sb.table("subscribers").update(update_payload).eq(
                "stripe_subscription_id", subscription_id
            ).execute()
            return
        except Exception as exc:
            log.error("update_subscription_status (Supabase) error: %s", exc)
            raise

    with _get_sqlite_conn() as conn:
        conn.execute(
            """UPDATE subscribers
               SET subscription_status=?,
                   current_period_end=COALESCE(?, current_period_end),
                   updated_at=?
               WHERE stripe_subscription_id=?""",
            (status, period_end, now, subscription_id),
        )


def get_active_subscribers() -> list[dict]:
    """Return all subscribers eligible to receive daily reports."""
    if USE_SUPABASE:
        try:
            sb = _get_supabase_client()
            result = (
                sb.table("subscribers")
                .select("*")
                .in_("subscription_status", ["active", "trialing"])
                .execute()
            )
            return result.data or []
        except Exception as exc:
            log.error("get_active_subscribers (Supabase) error: %s", exc)
            raise

    with _get_sqlite_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM subscribers WHERE subscription_status IN ('active','trialing')"
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_subscribers() -> list[dict]:
    if USE_SUPABASE:
        try:
            sb = _get_supabase_client()
            result = (
                sb.table("subscribers")
                .select("*")
                .order("created_at", desc=True)
                .execute()
            )
            return result.data or []
        except Exception as exc:
            log.error("get_all_subscribers (Supabase) error: %s", exc)
            raise

    with _get_sqlite_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM subscribers ORDER BY created_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════
#  SCREENER DATA HELPERS — always use SQLite (operational/ephemeral)
# ══════════════════════════════════════════════════════════════════════

def save_daily_report(
    report_date: str,
    regime: str,
    total_scanned: int,
    full_qualifiers: int,
    strict_qualifiers: int,
    new_count: int,
    dropped_count: int,
    breadth_pct: Optional[float],
    report_html: str,
    report_json: str,
) -> None:
    now = datetime.utcnow().isoformat()
    with _get_sqlite_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO daily_reports
               (report_date, regime, total_scanned, full_qualifiers,
                strict_qualifiers, new_count, dropped_count, breadth_pct,
                report_html, report_json, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (report_date, regime, total_scanned, full_qualifiers,
             strict_qualifiers, new_count, dropped_count, breadth_pct,
             report_html, report_json, now),
        )


def save_focus_list(report_date: str, focus_stocks: list[dict]) -> None:
    with _get_sqlite_conn() as conn:
        conn.execute("DELETE FROM focus_list WHERE report_date = ?", (report_date,))
        for stock in focus_stocks:
            conn.execute(
                """INSERT OR IGNORE INTO focus_list
                   (report_date, ticker, rank, sepa_score, rs_rank,
                    price, sector, change_type, is_strict)
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
                ),
            )


def get_latest_report() -> Optional[dict]:
    with _get_sqlite_conn() as conn:
        row = conn.execute(
            "SELECT * FROM daily_reports ORDER BY report_date DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def get_focus_list(report_date: str) -> list[dict]:
    with _get_sqlite_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM focus_list WHERE report_date = ? ORDER BY rank",
            (report_date,),
        ).fetchall()
    return [dict(r) for r in rows]


def log_report_send(
    subscriber_id: int,
    report_date: str,
    status: str,
    error: Optional[str] = None,
) -> None:
    now = datetime.utcnow().isoformat()
    with _get_sqlite_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO report_sends
               (subscriber_id, report_date, sent_at, status, error_msg)
               VALUES (?,?,?,?,?)""",
            (subscriber_id, report_date, now, status, error),
        )


# ── Internal helpers ───────────────────────────────────────────────────

def _hash_for_log(value: str) -> str:
    """
    Return a short, non-reversible token for logging user identifiers.
    Prevents email addresses from appearing in plain text in log files.
    """
    import hashlib
    return hashlib.sha256(value.encode()).hexdigest()[:12]

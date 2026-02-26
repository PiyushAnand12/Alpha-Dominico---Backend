"""
╔══════════════════════════════════════════════════════════════════════════╗
║        SBEM Market Intelligence System  v4.0                            ║
║        Production Stage — Indian Market (NSE)                           ║
║                                                                          ║
║  Architecture:                                                           ║
║    Layer 1  — Technical:    8-criteria Multi-Factor Trend Filter (Standard/Strict)  ║
║    Layer 2  — Fundamental:  EPS gate + scored criteria                  ║
║    Layer 3  — History:      SQLite persistence, lifecycle tracking       ║
║    Layer 4  — Intelligence: Market regime, breadth, sector rotation      ║
║    Layer 5  — Analytics:    1W/4W/12W forward return tracking            ║
║                                                                          ║
║  Output Sections (in order):                                             ║
║    0. Header + Disclaimer                                                ║
║    1. Market Regime + Structure Summary                                  ║
║    2. Full Qualification Pass (with lifecycle + scoring)                    ║
║    3. Strict Mode Qualifiers (HIGH-CONVICTION)                           ║
║    4. Newly Qualified Today  (NEW)                                       ║
║    5. Technical Pass Only                                                ║
║    6. Close Calls  (7/8 MFT)                                              ║
║    7. Dropped Today  (DROPPED)                                           ║
║    8. Performance Analytics (1W/4W/12W)                                  ║
║                                                                          ║
║  Delivery: HTML report + PDF + Gmail + Telegram                         ║
║                                                                          ║
║  DISCLAIMER: This system identifies stocks meeting structured            ║
║  structured qualification criteria. It does not constitute investment    ║
║  advice, a recommendation, or a signal of any kind.                     ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import os, sys, time, logging, smtplib, math, traceback, sqlite3, json, subprocess
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf
import requests
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════
EMAIL_SENDER       = os.getenv("EMAIL_SENDER",      "")
EMAIL_PASSWORD     = os.getenv("EMAIL_PASSWORD",    "")
EMAIL_TO           = os.getenv("EMAIL_TO",          "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",  "")

# ── Standard mode thresholds ──────────────────────────────────────────
MIN_PRICE          = float(os.getenv("MIN_PRICE",       "20.0"))   # ₹20 floor
MIN_AVG_VOL        = int(  os.getenv("MIN_AVG_VOL",     "50000"))   # NSE avg daily volume
RS_RANK_MIN        = int(  os.getenv("RS_RANK_MIN",     "70"))
PCT_ABOVE_52WL     = float(os.getenv("PCT_ABOVE_52WL",  "30.0"))
PCT_FROM_52WH      = float(os.getenv("PCT_FROM_52WH",   "25.0"))
EPS_GROWTH_MIN     = float(os.getenv("EPS_GROWTH_MIN",  "20.0"))
REV_GROWTH_MIN     = float(os.getenv("REV_GROWTH_MIN",  "15.0"))
ROE_MIN            = float(os.getenv("ROE_MIN",         "15.0"))
FUND_SCORE_MIN     = int(  os.getenv("FUND_SCORE_MIN",  "3"))

# ── Strict mode thresholds (HIGH-CONVICTION layer) ────────────────────
STRICT_RS_RANK_MIN    = int(  os.getenv("STRICT_RS_RANK_MIN",    "85"))
STRICT_PCT_FROM_52WH  = float(os.getenv("STRICT_PCT_FROM_52WH",  "15.0"))
STRICT_EPS_GROWTH_MIN = float(os.getenv("STRICT_EPS_GROWTH_MIN", "30.0"))
STRICT_REV_GROWTH_MIN = float(os.getenv("STRICT_REV_GROWTH_MIN", "20.0"))
STRICT_FUND_SCORE_MIN = int(  os.getenv("STRICT_FUND_SCORE_MIN", "4"))

# ── Operational ───────────────────────────────────────────────────────
BATCH_SIZE         = int(  os.getenv("BATCH_SIZE",      "150"))
BATCH_SLEEP        = float(os.getenv("BATCH_SLEEP",     "2.0"))
FUND_SLEEP         = float(os.getenv("FUND_SLEEP",      "0.5"))
MAX_TICKERS        = int(  os.getenv("MAX_TICKERS",     "0"))
OUTPUT_DIR         = os.getenv("OUTPUT_DIR",            ".")
DB_PATH            = os.getenv("DB_PATH",
                        os.path.join(OUTPUT_DIR, "sbem_history.db"))

REPORT_NAME        = "SBEM Market Intelligence"
MARKET_NAME        = "Indian Equity (NSE)"
VERSION            = "v4.0"

# Benchmark ticker for performance analytics
BENCHMARK_TICKER   = os.getenv("BENCHMARK_TICKER", "^NSEI")

# ══════════════════════════════════════════════════════════════════════
#  LOGGING  — UTF-8 forced so Windows cp1252 terminals don't crash
# ══════════════════════════════════════════════════════════════════════
os.makedirs(OUTPUT_DIR, exist_ok=True)
_log_file = os.path.join(OUTPUT_DIR, "screener.log")
_fh = logging.FileHandler(_log_file, encoding="utf-8")
_fh.setLevel(logging.DEBUG)
try:
    _sh = logging.StreamHandler(
        open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
    )
except Exception:
    _sh = logging.StreamHandler(sys.stdout)
_sh.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s")
_fh.setFormatter(_fmt)
_sh.setFormatter(_fmt)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(_fh)
log.addHandler(_sh)
log.propagate = False


# ══════════════════════════════════════════════════════════════════════
#  DATABASE LAYER  v4.0
#
#  Schema (5 tables):
#
#  qualification_history  — per ticker per day (expanded with lifecycle)
#  market_breadth         — daily aggregate breadth
#  ticker_meta            — company name / sector / market cap
#  performance_log        — 1W/4W/12W forward return tracking
#  market_regime          — daily regime classification
# ══════════════════════════════════════════════════════════════════════

DDL = """
CREATE TABLE IF NOT EXISTS qualification_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    screen_date         TEXT    NOT NULL,
    ticker              TEXT    NOT NULL,
    full_qualifies      INTEGER NOT NULL DEFAULT 0,
    strict_qualifies    INTEGER NOT NULL DEFAULT 0,
    tt_pass             INTEGER NOT NULL DEFAULT 0,
    sepa_score          INTEGER NOT NULL DEFAULT 0,
    tech_score          INTEGER NOT NULL DEFAULT 0,
    fund_score_norm     INTEGER NOT NULL DEFAULT 0,
    eps_growth_q        REAL    DEFAULT NULL,
    rev_growth_q        REAL    DEFAULT NULL,
    rs_rank             INTEGER DEFAULT NULL,
    price               REAL    DEFAULT NULL,
    sector              TEXT    DEFAULT NULL,
    fund_score          INTEGER DEFAULT 0,
    change_type         TEXT    DEFAULT NULL,
    streak              INTEGER DEFAULT 0,
    first_qual_date     TEXT    DEFAULT NULL,
    total_qual_cycles   INTEGER DEFAULT 0,
    longest_streak      INTEGER DEFAULT 0,
    data_quality        TEXT    DEFAULT 'unknown',
    UNIQUE(screen_date, ticker)
);

CREATE TABLE IF NOT EXISTS market_breadth (
    screen_date             TEXT PRIMARY KEY,
    total_scanned           INTEGER DEFAULT 0,
    full_qualifiers         INTEGER DEFAULT 0,
    strict_qualifiers       INTEGER DEFAULT 0,
    tech_only               INTEGER DEFAULT 0,
    close_calls             INTEGER DEFAULT 0,
    new_qualifiers          INTEGER DEFAULT 0,
    dropped_qualifiers      INTEGER DEFAULT 0,
    top_sector              TEXT    DEFAULT NULL,
    sepa_score_median       REAL    DEFAULT NULL,
    run_duration_s          INTEGER DEFAULT 0,
    regime                  TEXT    DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS ticker_meta (
    ticker          TEXT PRIMARY KEY,
    company         TEXT,
    sector          TEXT,
    market_cap_b    REAL,
    last_updated    TEXT
);

CREATE TABLE IF NOT EXISTS performance_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    qual_date       TEXT    NOT NULL,
    price_at_qual   REAL,
    price_1w        REAL    DEFAULT NULL,
    price_4w        REAL    DEFAULT NULL,
    price_12w       REAL    DEFAULT NULL,
    return_1w       REAL    DEFAULT NULL,
    return_4w       REAL    DEFAULT NULL,
    return_12w      REAL    DEFAULT NULL,
    benchmark_1w    REAL    DEFAULT NULL,
    benchmark_4w    REAL    DEFAULT NULL,
    benchmark_12w   REAL    DEFAULT NULL,
    alpha_1w        REAL    DEFAULT NULL,
    alpha_4w        REAL    DEFAULT NULL,
    alpha_12w       REAL    DEFAULT NULL,
    UNIQUE(ticker, qual_date)
);

CREATE TABLE IF NOT EXISTS market_regime (
    screen_date     TEXT PRIMARY KEY,
    regime          TEXT,
    regime_color    TEXT,
    breadth_7d_avg  REAL,
    breadth_14d_avg REAL,
    breadth_30d_avg REAL,
    trend_direction TEXT
);

CREATE INDEX IF NOT EXISTS idx_qh_date   ON qualification_history(screen_date);
CREATE INDEX IF NOT EXISTS idx_qh_ticker ON qualification_history(ticker);
CREATE INDEX IF NOT EXISTS idx_qh_full   ON qualification_history(screen_date, full_qualifies);
CREATE INDEX IF NOT EXISTS idx_perf_date ON performance_log(qual_date);
CREATE INDEX IF NOT EXISTS idx_perf_ticker ON performance_log(ticker);
"""


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(DDL)
    conn.commit()
    _migrate_db(conn)
    return conn


def _migrate_db(conn: sqlite3.Connection) -> None:
    """
    Safe schema migration — adds new columns to existing tables without
    destroying old data. Called every run; safe to re-run (uses IF NOT EXISTS logic).
    """
    migrations = [
        # market_breadth new columns
        "ALTER TABLE market_breadth ADD COLUMN strict_qualifiers INTEGER DEFAULT 0",
        "ALTER TABLE market_breadth ADD COLUMN regime TEXT DEFAULT NULL",
        # qualification_history new columns
        "ALTER TABLE qualification_history ADD COLUMN strict_qualifies INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE qualification_history ADD COLUMN tech_score INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE qualification_history ADD COLUMN fund_score_norm INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE qualification_history ADD COLUMN first_qual_date TEXT DEFAULT NULL",
        "ALTER TABLE qualification_history ADD COLUMN total_qual_cycles INTEGER DEFAULT 0",
        "ALTER TABLE qualification_history ADD COLUMN longest_streak INTEGER DEFAULT 0",
        "ALTER TABLE qualification_history ADD COLUMN data_quality TEXT DEFAULT 'unknown'",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # column already exists — silently skip
    conn.commit()
    log.debug("DB migration check complete")


def db_upsert_qualification(conn: sqlite3.Connection, rows: list[dict]) -> None:
    conn.executemany("""
        INSERT INTO qualification_history
            (screen_date, ticker, full_qualifies, strict_qualifies, tt_pass,
             sepa_score, tech_score, fund_score_norm,
             eps_growth_q, rev_growth_q, rs_rank, price, sector,
             fund_score, change_type, streak,
             first_qual_date, total_qual_cycles, longest_streak, data_quality)
        VALUES
            (:screen_date, :ticker, :full_qualifies, :strict_qualifies, :tt_pass,
             :sepa_score, :tech_score, :fund_score_norm,
             :eps_growth_q, :rev_growth_q, :rs_rank, :price, :sector,
             :fund_score, :change_type, :streak,
             :first_qual_date, :total_qual_cycles, :longest_streak, :data_quality)
        ON CONFLICT(screen_date, ticker) DO UPDATE SET
            full_qualifies      = excluded.full_qualifies,
            strict_qualifies    = excluded.strict_qualifies,
            tt_pass             = excluded.tt_pass,
            sepa_score          = excluded.sepa_score,
            tech_score          = excluded.tech_score,
            fund_score_norm     = excluded.fund_score_norm,
            eps_growth_q        = excluded.eps_growth_q,
            rev_growth_q        = excluded.rev_growth_q,
            rs_rank             = excluded.rs_rank,
            price               = excluded.price,
            sector              = excluded.sector,
            fund_score          = excluded.fund_score,
            change_type         = excluded.change_type,
            streak              = excluded.streak,
            first_qual_date     = excluded.first_qual_date,
            total_qual_cycles   = excluded.total_qual_cycles,
            longest_streak      = excluded.longest_streak,
            data_quality        = excluded.data_quality
    """, rows)
    conn.commit()


def db_upsert_meta(conn: sqlite3.Connection, rows: list[dict]) -> None:
    conn.executemany("""
        INSERT INTO ticker_meta (ticker, company, sector, market_cap_b, last_updated)
        VALUES (:ticker, :company, :sector, :market_cap_b, :last_updated)
        ON CONFLICT(ticker) DO UPDATE SET
            company      = excluded.company,
            sector       = excluded.sector,
            market_cap_b = excluded.market_cap_b,
            last_updated = excluded.last_updated
    """, rows)
    conn.commit()


def db_upsert_breadth(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute("""
        INSERT INTO market_breadth
            (screen_date, total_scanned, full_qualifiers, strict_qualifiers, tech_only,
             close_calls, new_qualifiers, dropped_qualifiers,
             top_sector, sepa_score_median, run_duration_s, regime)
        VALUES
            (:screen_date, :total_scanned, :full_qualifiers, :strict_qualifiers, :tech_only,
             :close_calls, :new_qualifiers, :dropped_qualifiers,
             :top_sector, :sepa_score_median, :run_duration_s, :regime)
        ON CONFLICT(screen_date) DO UPDATE SET
            total_scanned      = excluded.total_scanned,
            full_qualifiers    = excluded.full_qualifiers,
            strict_qualifiers  = excluded.strict_qualifiers,
            tech_only          = excluded.tech_only,
            close_calls        = excluded.close_calls,
            new_qualifiers     = excluded.new_qualifiers,
            dropped_qualifiers = excluded.dropped_qualifiers,
            top_sector         = excluded.top_sector,
            sepa_score_median  = excluded.sepa_score_median,
            run_duration_s     = excluded.run_duration_s,
            regime             = excluded.regime
    """, row)
    conn.commit()


def db_upsert_regime(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute("""
        INSERT INTO market_regime
            (screen_date, regime, regime_color, breadth_7d_avg,
             breadth_14d_avg, breadth_30d_avg, trend_direction)
        VALUES
            (:screen_date, :regime, :regime_color, :breadth_7d_avg,
             :breadth_14d_avg, :breadth_30d_avg, :trend_direction)
        ON CONFLICT(screen_date) DO UPDATE SET
            regime          = excluded.regime,
            regime_color    = excluded.regime_color,
            breadth_7d_avg  = excluded.breadth_7d_avg,
            breadth_14d_avg = excluded.breadth_14d_avg,
            breadth_30d_avg = excluded.breadth_30d_avg,
            trend_direction = excluded.trend_direction
    """, row)
    conn.commit()


def db_get_yesterday_qualifiers(conn: sqlite3.Connection, today: str) -> set[str]:
    row = conn.execute("""
        SELECT screen_date FROM market_breadth
        WHERE screen_date < ? ORDER BY screen_date DESC LIMIT 1
    """, (today,)).fetchone()
    if not row:
        return set()
    prev_date = row["screen_date"]
    rows = conn.execute("""
        SELECT ticker FROM qualification_history
        WHERE screen_date = ? AND full_qualifies = 1
    """, (prev_date,)).fetchall()
    return {r["ticker"] for r in rows}


def db_get_streak(conn: sqlite3.Connection, ticker: str, today: str) -> int:
    rows = conn.execute("""
        SELECT screen_date, full_qualifies
        FROM qualification_history
        WHERE ticker = ? AND screen_date < ?
        ORDER BY screen_date DESC LIMIT 60
    """, (ticker, today)).fetchall()
    streak = 0
    for r in rows:
        if r["full_qualifies"] == 1:
            streak += 1
        else:
            break
    return streak


def db_get_lifecycle(conn: sqlite3.Connection, ticker: str, today: str) -> dict:
    """Return lifecycle metadata: first_qual_date, total_qual_cycles, longest_streak."""
    rows = conn.execute("""
        SELECT screen_date, full_qualifies, streak
        FROM qualification_history
        WHERE ticker = ? AND screen_date < ?
        ORDER BY screen_date ASC
    """, (ticker, today)).fetchall()

    first_qual_date   = None
    total_qual_cycles = 0
    longest_streak    = 0
    in_qual           = False

    for r in rows:
        if r["full_qualifies"] == 1:
            if not in_qual:
                total_qual_cycles += 1
                in_qual = True
            if first_qual_date is None:
                first_qual_date = r["screen_date"]
            longest_streak = max(longest_streak, r["streak"] or 0)
        else:
            in_qual = False

    return {
        "first_qual_date":   first_qual_date,
        "total_qual_cycles": total_qual_cycles,
        "longest_streak":    longest_streak,
    }


def db_get_breadth_history(conn: sqlite3.Connection, days: int = 30) -> list[dict]:
    cols_info = conn.execute("PRAGMA table_info(market_breadth)").fetchall()
    existing  = {c["name"] for c in cols_info}
    sel_strict = "strict_qualifiers" if "strict_qualifiers" in existing else "0 AS strict_qualifiers"
    sel_regime = "regime"             if "regime"            in existing else "NULL AS regime"
    rows = conn.execute(f"""
        SELECT screen_date, full_qualifiers, {sel_strict}, tech_only,
               close_calls, new_qualifiers, dropped_qualifiers, {sel_regime}
        FROM market_breadth
        ORDER BY screen_date DESC LIMIT ?
    """, (days,)).fetchall()
    return [dict(r) for r in reversed(rows)]


def db_get_performance_stats(conn: sqlite3.Connection) -> dict:
    """Aggregate performance analytics from performance_log."""
    stats = {}
    for period, col in [("1w", "return_1w"), ("4w", "return_4w"), ("12w", "return_12w")]:
        row = conn.execute(f"""
            SELECT
                COUNT(*) as n,
                AVG({col}) as avg_ret,
                MAX({col}) as max_ret,
                MIN({col}) as min_ret,
                AVG(alpha_{period}) as avg_alpha,
                SUM(CASE WHEN {col} > 0 THEN 1 ELSE 0 END)*1.0/
                    NULLIF(COUNT(CASE WHEN {col} IS NOT NULL THEN 1 END),0) as win_rate,
                SUM(CASE WHEN alpha_{period} > 0 THEN 1 ELSE 0 END)*1.0/
                    NULLIF(COUNT(CASE WHEN alpha_{period} IS NOT NULL THEN 1 END),0) as beat_rate
            FROM performance_log
            WHERE {col} IS NOT NULL
        """).fetchone()
        if row and row["n"] > 0:
            stats[period] = dict(row)
    return stats


def db_seed_performance_log(conn: sqlite3.Connection, today: str) -> None:
    """
    Insert NEW entries into performance_log for stocks that qualified today
    (price_at_qual captured now; returns filled in future runs).
    """
    rows = conn.execute("""
        SELECT ticker, price FROM qualification_history
        WHERE screen_date = ? AND full_qualifies = 1
    """, (today,)).fetchall()
    for r in rows:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO performance_log (ticker, qual_date, price_at_qual)
                VALUES (?, ?, ?)
            """, (r["ticker"], today, r["price"]))
        except Exception:
            pass
    conn.commit()


def db_update_performance_returns(conn: sqlite3.Connection, today: str) -> None:
    """
    For entries in performance_log that are missing returns, check if enough
    time has passed (5 trading days ≈ 1W, 20 ≈ 4W, 60 ≈ 12W) and fill returns
    using stored prices in qualification_history.
    """
    target_map = {"1w": 5, "4w": 20, "12w": 60}  # approximate trading days

    # Get benchmark prices from yfinance for comparison
    log.info("Updating performance analytics returns...")
    try:
        bench_hist = yf.download(BENCHMARK_TICKER, period="1y", progress=False,
                                 auto_adjust=True)
        if bench_hist.empty:
            bench_hist = None
        else:
            bench_hist = bench_hist["Close"]
    except Exception:
        bench_hist = None

    pending = conn.execute("""
        SELECT ticker, qual_date, price_at_qual
        FROM performance_log
        WHERE return_12w IS NULL
        ORDER BY qual_date ASC
        LIMIT 200
    """).fetchall()

    updated = 0
    for entry in pending:
        ticker    = entry["ticker"]
        qual_date = entry["qual_date"]
        qual_dt   = datetime.fromisoformat(qual_date)
        days_ago  = (datetime.fromisoformat(today) - qual_dt).days
        if days_ago < 5:
            continue  # too soon even for 1W

        updates = {}

        def _get_price(sym, days_forward):
            """Fetch price ~days_forward trading days after qual_date."""
            target_dt = qual_dt + timedelta(days=int(days_forward * 1.4))
            try:
                end = target_dt + timedelta(days=5)
                h = yf.download(sym, start=target_dt.strftime("%Y-%m-%d"),
                                end=end.strftime("%Y-%m-%d"), progress=False,
                                auto_adjust=True)
                if h.empty:
                    return None
                return float(h["Close"].iloc[0])
            except Exception:
                return None

        def _bench_ret(days_forward):
            if bench_hist is None:
                return None
            try:
                target_dt = qual_dt + timedelta(days=int(days_forward * 1.4))
                qual_idx  = bench_hist.index.searchsorted(pd.Timestamp(qual_date))
                fwd_idx   = bench_hist.index.searchsorted(pd.Timestamp(target_dt))
                if qual_idx >= len(bench_hist) or fwd_idx >= len(bench_hist):
                    return None
                p0 = float(bench_hist.iloc[qual_idx])
                p1 = float(bench_hist.iloc[fwd_idx])
                return (p1 / p0 - 1.0) * 100 if p0 > 0 else None
            except Exception:
                return None

        base = entry["price_at_qual"]
        if base is None or base == 0:
            continue

        # 1W
        if days_ago >= 5:
            p = _get_price(ticker, 5)
            if p:
                ret = (p / base - 1.0) * 100
                b   = _bench_ret(5)
                updates["price_1w"]    = p
                updates["return_1w"]   = round(ret, 2)
                updates["benchmark_1w"] = round(b, 2) if b else None
                updates["alpha_1w"]    = round(ret - b, 2) if b else None

        # 4W
        if days_ago >= 20:
            p = _get_price(ticker, 20)
            if p:
                ret = (p / base - 1.0) * 100
                b   = _bench_ret(20)
                updates["price_4w"]    = p
                updates["return_4w"]   = round(ret, 2)
                updates["benchmark_4w"] = round(b, 2) if b else None
                updates["alpha_4w"]    = round(ret - b, 2) if b else None

        # 12W
        if days_ago >= 60:
            p = _get_price(ticker, 60)
            if p:
                ret = (p / base - 1.0) * 100
                b   = _bench_ret(60)
                updates["price_12w"]   = p
                updates["return_12w"]  = round(ret, 2)
                updates["benchmark_12w"] = round(b, 2) if b else None
                updates["alpha_12w"]   = round(ret - b, 2) if b else None

        if updates:
            set_clause = ", ".join(f"{k} = :{k}" for k in updates)
            updates["ticker"]    = ticker
            updates["qual_date"] = qual_date
            conn.execute(
                f"UPDATE performance_log SET {set_clause} WHERE ticker=:ticker AND qual_date=:qual_date",
                updates,
            )
            updated += 1

    conn.commit()
    log.info(f"Performance log updated: {updated} entries refreshed")


# ══════════════════════════════════════════════════════════════════════
#  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════
@dataclass
class TechData:
    ticker:        str
    price:         float
    sma_50:        float
    sma_150:       float
    sma_200:       float
    sma_200_1m:    float
    high_52w:      float
    low_52w:       float
    avg_vol_50d:   float
    up_vol_days:   int
    down_vol_days: int
    rs_score:      float = 0.0
    rs_rank:       int   = 0


@dataclass
class FundData:
    ticker:             str
    eps_growth_q:       float
    eps_growth_annual:  float
    rev_growth_q:       float
    eps_accelerating:   bool
    margin_trend_up:    bool
    roe:                float
    pe_ratio:           float
    data_quality:       str   # 'good' | 'partial' | 'missing'


@dataclass
class ScreenResult:
    ticker:          str
    company:         str
    price:           float
    market_cap_b:    float
    sector:          str

    # Layer 1
    tt_pass:         bool
    tt_score:        int
    tt_detail:       dict = field(default_factory=dict)

    # Layer 2
    fund_pass:       bool  = False
    fund_score:      int   = 0
    fund_detail:     dict  = field(default_factory=dict)
    fund_note:       str   = ""

    # Scoring
    rs_rank:         int   = 0
    breakout_score:  int   = 0    # 0-100 composite
    tech_score:      int   = 0    # 0-10
    fund_score_norm: int   = 0    # 0-10

    # Raw fundamentals
    eps_growth_q:    float = 0.0
    rev_growth_q:    float = 0.0
    eps_accelerating:bool  = False
    roe:             float = 0.0
    pe_ratio:        float = 0.0
    data_quality:    str   = "unknown"

    # Strict mode flag
    strict_qualifies: bool = False

    # Change detection (populated after DB lookup)
    change_type:     str   = "RETAINED"
    streak:          int   = 0

    # Lifecycle
    first_qual_date:    Optional[str] = None
    total_qual_cycles:  int = 0
    longest_streak:     int = 0

    # Final
    qualifies:       bool  = False
    fail_reasons:    list  = field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════
#  TICKER FETCH
# ══════════════════════════════════════════════════════════════════════
def _clean_nse_ticker(sym: str) -> Optional[str]:
    """
    Validate a raw NSE symbol (no suffix) and return it uppercased,
    or None if it looks invalid. Symbols up to 20 chars are valid on NSE.
    The .NS suffix is appended separately when building the yfinance list.
    """
    sym = (sym or "").strip().upper()
    if not sym:
        return None
    # Reject obvious junk characters
    for bad in ("^", "+", "=", "*", "~", "/", "$", " ", "	"):
        if bad in sym:
            return None
    if len(sym) > 20 or len(sym) < 1:
        return None
    return sym


def fetch_indian_tickers() -> list[str]:
    """
    Fetch the full NSE equity universe and return yfinance-compatible
    tickers in the form  SYMBOL.NS  (e.g. RELIANCE.NS, TCS.NS).

    Source priority:
      1. NSE equity master CSV  (archives.nseindia.com)
      2. NSE index constituents via public JSON (Nifty 500)
      3. Built-in fallback list of ~200 large/mid-cap NSE stocks
    """
    raw_symbols: set[str] = set()

    # ── Source 1: NSE Equity Master CSV ─────────────────────────────
    log.info("Fetching ticker universe (Source 1: NSE Equity Master CSV)...")
    try:
        r = requests.get(
            "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://www.nseindia.com/",
            },
            timeout=30,
        )
        if r.ok:
            from io import StringIO
            df_eq = pd.read_csv(StringIO(r.text))
            # Keep only normal equity series (EQ)
            if "SERIES" in df_eq.columns:
                df_eq = df_eq[df_eq["SERIES"].str.strip() == "EQ"]
            sym_col = next((c for c in df_eq.columns if "SYMBOL" in c.upper()), None)
            if sym_col:
                for sym in df_eq[sym_col].dropna().tolist():
                    c = _clean_nse_ticker(str(sym))
                    if c:
                        raw_symbols.add(c)
            log.info(f"[OK] NSE Equity CSV: {len(raw_symbols)} EQ-series symbols")
    except Exception as e:
        log.warning(f"NSE Equity CSV failed: {e}")

    # ── Source 2: NSE Nifty 500 index JSON ───────────────────────────
    if len(raw_symbols) < 100:
        log.info("Source 2: NSE Nifty 500 index constituents...")
        try:
            sess = requests.Session()
            # Warm up the NSE session cookie
            sess.get("https://www.nseindia.com", timeout=10,
                     headers={"User-Agent": "Mozilla/5.0"})
            resp = sess.get(
                "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",
                timeout=20,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json",
                    "Referer": "https://www.nseindia.com/",
                },
            )
            data = resp.json()
            for item in data.get("data", []):
                sym = item.get("symbol", "") or item.get("meta", {}).get("symbol", "")
                c = _clean_nse_ticker(str(sym))
                if c:
                    raw_symbols.add(c)
            log.info(f"[OK] NSE Nifty 500 JSON: {len(raw_symbols)} symbols total")
        except Exception as e:
            log.warning(f"NSE Nifty 500 API failed: {e}")

    # ── Source 3: Wikipedia NSE-listed large caps ─────────────────────
    if len(raw_symbols) < 100:
        log.info("Source 3: Wikipedia Nifty 50 fallback...")
        try:
            tables = pd.read_html(
                "https://en.wikipedia.org/wiki/NIFTY_50",
                match="Symbol"
            )
            for tbl in tables:
                sym_col = next((c for c in tbl.columns
                                if "symbol" in str(c).lower()), None)
                if sym_col:
                    for sym in tbl[sym_col].dropna().tolist():
                        c = _clean_nse_ticker(str(sym).replace(".NS", ""))
                        if c:
                            raw_symbols.add(c)
            log.info(f"[OK] Wikipedia Nifty 50: {len(raw_symbols)} symbols total")
        except Exception as e:
            log.warning(f"Wikipedia Nifty 50 failed: {e}")

    # ── Source 4: Built-in fallback (Nifty 500 representative sample) ──
    if len(raw_symbols) < 50:
        log.warning("All live sources failed — using built-in NSE fallback list")
        _fallback_nse = (
            # Nifty 50
            "RELIANCE,TCS,HDFCBANK,ICICIBANK,INFY,HDFC,SBIN,BHARTIARTL,KOTAKBANK,LT,"
            "BAJFINANCE,HINDUNILVR,AXISBANK,ASIANPAINT,MARUTI,HCLTECH,SUNPHARMA,WIPRO,TITAN,"
            "ULTRACEMCO,NESTLEIND,POWERGRID,NTPC,TATAMOTORS,TECHM,ONGC,GRASIM,BAJAJFINSV,"
            "INDUSINDBK,ADANIPORTS,COALINDIA,EICHERMOT,DIVISLAB,DRREDDY,CIPLA,BPCL,TATACONSUM,"
            "JSWSTEEL,TATASTEEL,HINDALCO,HEROMOTOCO,APOLLOHOSP,BRITANNIA,ADANIENT,"
            "UPL,SBILIFE,HDFCLIFE,BAJAJ-AUTO,ITC,M&M,"
            # Nifty Next 50
            "ADANIGREEN,ADANITRANS,AMBUJACEM,AUROPHARMA,BANDHANBNK,BERGEPAINT,BIOCON,BOSCHLTD,"
            "CANBK,CHOLAFIN,COLPAL,CONCOR,DABUR,DLF,FEDERALBNK,FORTIS,GAIL,GODREJCP,"
            "GODREJPROP,HAVELLS,ICICIGI,ICICIPRULI,IDFCFIRSTB,IGL,INDHOTEL,INDUSTOWER,"
            "IRCTC,JUBLFOOD,L&TFH,LICI,LUPIN,MCDOWELL-N,MFSL,MOTHERSON,"
            "MPHASIS,NAUKRI,NMDC,OFSS,PAGEIND,PIDILITIND,PIIND,PNB,POLYCAB,"
            "RECLTD,SAIL,SHREECEM,SIEMENS,SRF,TORNTPHARM,TRENT,TVSMOTOR,"
            "UNIONBANK,VEDL,VOLTAS,WHIRLPOOL,ZOMATO,ZYDUSLIFE,"
            # Mid/Small cap growth names
            "ABCAPITAL,ABFRL,AAPL,ASTRAL,ATUL,BALKRISIND,BATAINDIA,BHEL,BLUEDART,"
            "CAMS,CANFINHOME,CARBORUNDUM,CASTROLIND,CESC,CHENNPETRO,COFORGE,"
            "CROMPTON,CUMMINSIND,DALBHARAT,DEEPAKNTR,DELTACORP,EIDPARRY,ELGIEQUIP,"
            "ESCORTS,EXIDEIND,FINEORG,FLUOROCHEM,FSL,GLAND,GLAXO,GNFC,GPPL,"
            "GRAPHITE,GUJGASLTD,HAPPSTMNDS,HFCL,HIKAL,HONAUT,IBREALEST,"
            "IDBI,INDIAMART,INDIANB,INDIGO,INTELLECT,IOB,IPCALAB,JKCEMENT,"
            "JKTYRE,JSL,JUBLINGREA,KALPATPOWR,KALYANKJIL,KEC,KPITTECH,"
            "KRBL,LALPATHLAB,LAURUSLABS,LICHSGFIN,LINDEINDIA,LXCHEM,"
            "MARICO,MAXHEALTH,MCX,MINDTREE,MOREPENLAB,MRF,MUTHOOTFIN,"
            "NATCOPHARM,NBCC,NFL,NHPC,NIITTECH,NOCIL,ORIENTELEC,"
            "PERSISTENT,PETRONET,PFC,PFIZER,PVR,RAJESHEXPO,RAMCOCEM,"
            "RITES,ROUTE,RPOWERLTD,SAKSOFT,SCHAEFFLER,SEQUENT,SOBHA,"
            "SOLARA,SPARC,SPANDANA,SRTRANSFIN,STAR,SUDARSCHEM,"
            "SUNDARMFIN,SUNDRMFAST,SUPREMEIND,SYNGENE,TANLA,TATACHEM,"
            "TATACOMM,TATAELXSI,TATAINVEST,TATAPOWER,TCM,TEAMLEASE,"
            "THYROCARE,TIMKEN,TORNTPOWER,TRIDENT,UJJIVAN,UJJIVANSFB,"
            "VAKRANGEE,VBL,VGUARD,VINATIORGA,WABAG,WELCORP,WHIRLPOOL"
        )
        for sym in _fallback_nse.split(","):
            c = _clean_nse_ticker(sym.strip())
            if c:
                raw_symbols.add(c)
        log.info(f"[OK] Built-in fallback: {len(raw_symbols)} symbols")

    # ── Append .NS suffix for yfinance ───────────────────────────────
    tickers = sorted(f"{sym}.NS" for sym in raw_symbols)
    if MAX_TICKERS > 0:
        tickers = tickers[:MAX_TICKERS]
        log.info(f"[TEST MODE] Universe limited to {MAX_TICKERS} tickers")
    log.info(f"Universe ready: {len(tickers)} NSE stocks")
    return tickers


# ══════════════════════════════════════════════════════════════════════
#  TECHNICAL INDICATORS
# ══════════════════════════════════════════════════════════════════════
def compute_tech(ticker: str, hist: pd.DataFrame) -> Optional[TechData]:
    if hist is None or len(hist) < 202:
        return None
    closes  = hist["Close"].values.astype(float)
    volumes = hist["Volume"].values.astype(float)

    price       = float(closes[-1])
    sma_50      = float(np.mean(closes[-50:]))
    sma_150     = float(np.mean(closes[-150:]))
    sma_200     = float(np.mean(closes[-200:]))
    sma_200_1m  = float(np.mean(closes[-221:-21])) if len(closes) >= 221 else sma_200
    high_52w    = float(np.max(closes[-252:]))
    low_52w     = float(np.min(closes[-252:]))
    avg_vol_50d = float(np.mean(volumes[-50:]))

    # Data quality: flag liquidity anomalies
    avg_vol_20d   = float(np.mean(volumes[-20:]))
    up_vol_days = down_vol_days = 0
    c20 = closes[-21:]; v20 = volumes[-20:]
    for i in range(20):
        if v20[i] >= avg_vol_20d:
            if c20[i+1] >= c20[i]: up_vol_days += 1
            else:                  down_vol_days += 1

    def safe_ret(a: int, b: int) -> float:
        if len(closes) < max(a, b) + 1: return 0.0
        c0 = closes[-a] if a > 0 else closes[-1]
        c1 = closes[-b] if b <= len(closes) else closes[0]
        return 0.0 if c1 == 0 else (c0 / c1) - 1.0

    rs_score = (safe_ret(1,63)*0.40 + safe_ret(63,126)*0.20
              + safe_ret(126,189)*0.20 + safe_ret(189,252)*0.20)

    return TechData(
        ticker=ticker, price=price,
        sma_50=sma_50, sma_150=sma_150, sma_200=sma_200, sma_200_1m=sma_200_1m,
        high_52w=high_52w, low_52w=low_52w, avg_vol_50d=avg_vol_50d,
        up_vol_days=up_vol_days, down_vol_days=down_vol_days,
        rs_score=rs_score, rs_rank=0,
    )


def rank_rs_scores(tech_map: dict) -> None:
    scores = np.array([t.rs_score for t in tech_map.values()])
    for t in tech_map.values():
        t.rs_rank = int(round(np.sum(scores < t.rs_score) / len(scores) * 100))


# ══════════════════════════════════════════════════════════════════════
#  MARKET REGIME ENGINE
#
#  Classifies the market environment based on breadth data:
#    Early Expansion : breadth rising from low base (< prior 14d avg but growing)
#    Expansion       : breadth above 14d avg and rising
#    Peak / Saturation: breadth very high, growth slowing or reversing
#    Contraction     : breadth falling, below recent averages
#    Neutral         : insufficient history or ambiguous signal
# ══════════════════════════════════════════════════════════════════════
def classify_market_regime(breadth_history: list[dict]) -> dict:
    if len(breadth_history) < 5:
        return {
            "regime": "Insufficient History",
            "regime_color": "#6b7280",
            "trend_direction": "unknown",
            "breadth_7d_avg": None,
            "breadth_14d_avg": None,
            "breadth_30d_avg": None,
            "badge_bg": "rgba(107,114,128,.12)",
            "badge_fg": "#9ca3af",
            "description": "Insufficient breadth history for regime classification. More data needed.",
        }

    vals = [r["full_qualifiers"] for r in breadth_history]
    today_val = vals[-1]

    avg7  = float(np.mean(vals[-7:]))  if len(vals) >= 7  else float(np.mean(vals))
    avg14 = float(np.mean(vals[-14:])) if len(vals) >= 14 else float(np.mean(vals))
    avg30 = float(np.mean(vals[-30:])) if len(vals) >= 30 else float(np.mean(vals))

    # 3-day trend (recent momentum)
    if len(vals) >= 4:
        recent_3d = vals[-3:]
        if recent_3d[-1] > recent_3d[0]:
            trend = "rising"
        elif recent_3d[-1] < recent_3d[0]:
            trend = "falling"
        else:
            trend = "flat"
    else:
        trend = "unknown"

    # Classification logic
    if today_val > avg14 * 1.20 and trend in ("falling", "flat"):
        regime = "Peak / Saturation"
        color  = "#f59e0b"
        badge_bg = "rgba(245,158,11,.12)"
        badge_fg = "#fbbf24"
        desc = (f"Breadth at {today_val} — elevated above 14-day average ({avg14:.1f}) "
                f"but momentum is {trend}. Historically precedes contraction. Selectivity advised.")

    elif today_val > avg14 * 1.05 and trend == "rising":
        regime = "Expansion"
        color  = "#10b981"
        badge_bg = "rgba(16,185,129,.12)"
        badge_fg = "#34d399"
        desc = (f"Breadth at {today_val} — above 14-day average ({avg14:.1f}) and rising. "
                f"Qualification pool is expanding. Conditions broadly supportive.")

    elif today_val < avg14 * 0.85 and trend == "rising":
        regime = "Early Expansion"
        color  = "#3b82f6"
        badge_bg = "rgba(59,130,246,.10)"
        badge_fg = "#60a5fa"
        desc = (f"Breadth at {today_val} — below 14-day average ({avg14:.1f}) but trending higher. "
                f"Early recovery signal. Qualification breadth beginning to rebuild.")

    elif today_val < avg14 * 0.80 and trend in ("falling", "flat"):
        regime = "Contraction"
        color  = "#ef4444"
        badge_bg = "rgba(239,68,68,.12)"
        badge_fg = "#f87171"
        desc = (f"Breadth at {today_val} — significantly below 14-day average ({avg14:.1f}) "
                f"and {trend}. Qualification pool contracting. Highly selective environment.")

    else:
        regime = "Neutral"
        color  = "#a855f7"
        badge_bg = "rgba(168,85,247,.12)"
        badge_fg = "#c084fc"
        desc = (f"Breadth at {today_val} — broadly in line with 14-day average ({avg14:.1f}). "
                f"No clear directional regime signal. Mixed qualification conditions.")

    return {
        "regime": regime,
        "regime_color": color,
        "trend_direction": trend,
        "breadth_7d_avg": round(avg7, 1),
        "breadth_14d_avg": round(avg14, 1),
        "breadth_30d_avg": round(avg30, 1),
        "badge_bg": badge_bg,
        "badge_fg": badge_fg,
        "description": desc,
    }


# ══════════════════════════════════════════════════════════════════════
#  BREAKOUT READINESS SCORE  (0–100 composite)  +  Sub-scores (0–10 each)
# ══════════════════════════════════════════════════════════════════════
def compute_breakout_score(t: TechData, f: Optional[FundData]) -> tuple[int, int, int]:
    """Returns (breakout_score_0_100, tech_score_0_10, fund_score_norm_0_10)."""
    pts = 0

    # --- Technical alignment ---
    if   t.rs_rank >= 90: pts += 20
    elif t.rs_rank >= 80: pts += 12
    elif t.rs_rank >= 70: pts += 5

    if t.up_vol_days > t.down_vol_days:    pts += 10
    elif t.up_vol_days == t.down_vol_days: pts += 4

    pct_from_high = (t.high_52w - t.price) / t.high_52w * 100 if t.high_52w > 0 else 100
    if   pct_from_high <= 5:  pts += 15
    elif pct_from_high <= 10: pts += 10
    elif pct_from_high <= 20: pts += 5

    # --- Trend strength ---
    if   t.sma_50 > t.sma_200 * 1.10: pts += 8
    elif t.sma_50 > t.sma_200 * 1.05: pts += 4

    tech_pts = pts  # save before fundamentals
    tech_score = min(int(tech_pts * 10 / 53), 10)  # normalise tech max (53) to 0-10

    if f is None or f.data_quality == "missing":
        brs  = min(int(tech_pts * 100 / 53), 100)
        return brs, tech_score, 0

    # --- Fundamental quality ---
    fund_pts = 0
    if   f.eps_growth_q >= 50: fund_pts += 13
    elif f.eps_growth_q >= 30: fund_pts += 9
    elif f.eps_growth_q >= EPS_GROWTH_MIN: fund_pts += 5

    if f.eps_accelerating:  fund_pts += 12

    if   f.rev_growth_q >= 30: fund_pts += 9
    elif f.rev_growth_q >= 20: fund_pts += 6
    elif f.rev_growth_q >= REV_GROWTH_MIN: fund_pts += 3

    if f.margin_trend_up:   fund_pts += 6

    if   f.roe >= 25: fund_pts += 5
    elif f.roe >= ROE_MIN: fund_pts += 3

    pts += fund_pts
    fund_score_norm = min(int(fund_pts * 10 / 43), 10)  # normalise fund max (43) to 0-10

    brs  = min(int(pts * 100 / 96), 100)
    return brs, tech_score, fund_score_norm


# ══════════════════════════════════════════════════════════════════════
#  MULTI-FACTOR TREND FILTER  (Layer 1 — Standard)
# ══════════════════════════════════════════════════════════════════════
def apply_multi_factor_filter(t: TechData) -> tuple:
    p = t.price
    d = {
        "TT01": p > t.sma_150 and p > t.sma_200,
        "TT02": t.sma_150 > t.sma_200,
        "TT03": t.sma_200 > t.sma_200_1m,
        "TT04": t.sma_50 > t.sma_150 and t.sma_50 > t.sma_200,
        "TT05": p > t.sma_50,
        "TT06": t.low_52w > 0 and (p / t.low_52w - 1) * 100 >= PCT_ABOVE_52WL,
        "TT07": t.high_52w > 0 and (t.high_52w - p) / t.high_52w * 100 <= PCT_FROM_52WH,
        "TT08": t.rs_rank >= RS_RANK_MIN,
    }
    passes = all(d.values())
    return passes, sum(d.values()), d, [k for k, v in d.items() if not v]


def apply_strict_filter(t: TechData, f: Optional[FundData], fund_score: int) -> bool:
    """High-conviction filter applied ON TOP of standard qualification."""
    if t.rs_rank < STRICT_RS_RANK_MIN:
        return False
    pct_from_high = (t.high_52w - t.price) / t.high_52w * 100 if t.high_52w > 0 else 100
    if pct_from_high > STRICT_PCT_FROM_52WH:
        return False
    if f is None or f.data_quality == "missing":
        return False
    if f.eps_growth_q < STRICT_EPS_GROWTH_MIN:
        return False
    if f.rev_growth_q < STRICT_REV_GROWTH_MIN:
        return False
    if fund_score < STRICT_FUND_SCORE_MIN:
        return False
    return True


# ══════════════════════════════════════════════════════════════════════
#  FOCUS LIST  — Ranking Engine  v4.0
#
#  Formula (0–100 composite):
#
#    FOCUS_SCORE =
#       30 × (breakout_score / 100)              [composite quality]
#     + 25 × (rs_rank / 100)                 [relative strength]
#     + 20 × (1 – pct_from_52wh / 100)       [proximity to 52W high]
#     + 15 × min(streak, 30) / 30            [qualification maturity]
#     + 10 × cap_factor                       [liquidity / size]
#
#  cap_factor = log10(market_cap_b + 0.5) / log10(1000)  capped [0, 1]
#
#  Selection:
#    1. Filter to strict qualifiers only.
#    2. Score each with compute_focus_score().
#    3. Sort descending; take top FOCUS_LIST_SIZE (default 10).
#    4. If strict pool < FOCUS_LIST_SIZE, back-fill from standard
#       qualifiers with highest focus scores.
# ══════════════════════════════════════════════════════════════════════
FOCUS_LIST_SIZE = int(os.getenv("FOCUS_LIST_SIZE", "10"))


def compute_focus_score(r: "ScreenResult") -> float:
    """Return a 0–100 Focus Score for ranking. Higher = more focused."""
    # Component 1: Breakout Readiness Score composite (30 pts)
    brs_component = r.breakout_score / 100 * 30

    # Component 2: RS Rank (25 pts)
    rs_component = r.rs_rank / 100 * 25

    # Component 3: Proximity to 52-week high (20 pts)
    # Approximated from strict threshold — closer to 52WH = better
    # Use the TT07 threshold (PCT_FROM_52WH) as reference baseline
    # We reconstruct pct_from_52wh from breakout score context heuristically.
    # Since TT07 enforces <= PCT_FROM_52WH already, we use fund_score_norm
    # as a proxy for proximity quality and scale accordingly.
    # Direct proximity unavailable post-screening; use tech_score as proxy.
    prox_component = r.tech_score / 10 * 20

    # Component 4: Qualification streak / maturity (15 pts)
    streak_capped = min(r.streak, 30)
    streak_component = streak_capped / 30 * 15

    # Component 5: Market cap / liquidity (10 pts)
    mc = max(r.market_cap_b, 0.01)
    cap_factor = min(math.log10(mc + 0.5) / math.log10(1000), 1.0)
    cap_component = max(cap_factor, 0) * 10

    return round(brs_component + rs_component + prox_component
                 + streak_component + cap_component, 2)


def select_focus_list(
    results: list,
    n: int = FOCUS_LIST_SIZE
) -> list:
    """
    Selection pseudocode:
      1. Score all strict qualifiers.
      2. Sort by focus_score DESC.
      3. Take top-n.
      4. If fewer than n strict, extend with standard qualifiers (scored),
         ensuring no duplicates.
    Returns list of (ScreenResult, focus_score) tuples.
    """
    strict  = [(r, compute_focus_score(r)) for r in results
               if r.qualifies and r.strict_qualifies]
    strict.sort(key=lambda x: -x[1])

    selected = strict[:n]

    if len(selected) < n:
        selected_tickers = {r.ticker for r, _ in selected}
        standard_extra = [(r, compute_focus_score(r)) for r in results
                          if r.qualifies and not r.strict_qualifies
                          and r.ticker not in selected_tickers]
        standard_extra.sort(key=lambda x: -x[1])
        needed = n - len(selected)
        selected.extend(standard_extra[:needed])

    return selected


# ══════════════════════════════════════════════════════════════════════
#  CSV EXPORT  — Full qualifier list for attachment
# ══════════════════════════════════════════════════════════════════════
def generate_csv_full_list(results: list) -> str:
    """
    Write full qualifier list to a dated CSV in OUTPUT_DIR.
    Returns the file path.
    """
    qualifiers = [r for r in results if r.qualifies]
    today_str  = date.today().strftime("%Y%m%d")
    csv_path   = os.path.join(OUTPUT_DIR, f"sbem_full_list_{today_str}.csv")

    rows = []
    for r in sorted(qualifiers, key=lambda x: -x.breakout_score):
        rows.append({
            "Date":          date.today().isoformat(),
            "Ticker":        r.ticker,
            "Company":       r.company,
            "Sector":        r.sector,
            "Market Cap (B)": f"{r.market_cap_b:.2f}",
            "Price":         f"{r.price:.2f}",
            "Breakout Readiness Score":    r.breakout_score,
            "Tech Score":    r.tech_score,
            "Fund Score":    r.fund_score_norm,
            "RS Rank":       r.rs_rank,
            "EPS Growth %":  f"{r.eps_growth_q:.1f}",
            "Rev Growth %":  f"{r.rev_growth_q:.1f}",
            "Streak (days)": r.streak,
            "Status":        r.change_type,
            "Strict":        "Y" if r.strict_qualifies else "N",
            "EPS Accel":     "Y" if r.eps_accelerating else "N",
        })

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    log.info(f"[OK] Full qualifier CSV saved: {csv_path}")
    return csv_path


# ══════════════════════════════════════════════════════════════════════
#  WEEKLY STATS  — DB queries for weekly report
# ══════════════════════════════════════════════════════════════════════
def db_get_weekly_stats(conn: sqlite3.Connection) -> dict:
    """
    Pull last 7 days of breadth + performance data for weekly report.
    Returns dict with keys: breadth_7d, sector_stats, perf_stats,
    strict_vs_standard, failure_rate.
    """
    stats = {}

    # 7-day breadth trend
    rows = conn.execute("""
        SELECT screen_date, full_qualifiers, strict_qualifiers,
               new_qualifiers, dropped_qualifiers, regime
        FROM market_breadth
        ORDER BY screen_date DESC LIMIT 7
    """).fetchall()
    stats["breadth_7d"] = [dict(r) for r in reversed(rows)]

    # Sector breakdown over last 7 days
    sec_rows = conn.execute("""
        SELECT sector, COUNT(*) as cnt
        FROM qualification_history
        WHERE screen_date >= date('now', '-7 days')
          AND full_qualifies = 1
          AND sector IS NOT NULL AND sector != '--'
        GROUP BY sector
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()
    stats["sector_7d"] = [dict(r) for r in sec_rows]

    # Median 4W forward return from performance_log
    perf = conn.execute("""
        SELECT
            AVG(return_4w)  AS avg_4w,
            AVG(return_12w) AS avg_12w,
            AVG(alpha_4w)   AS avg_alpha_4w,
            SUM(CASE WHEN return_4w > 0 THEN 1 ELSE 0 END) * 1.0 /
                NULLIF(COUNT(CASE WHEN return_4w IS NOT NULL THEN 1 END), 0) AS win_rate_4w,
            SUM(CASE WHEN return_4w IS NOT NULL THEN 1 ELSE 0 END) AS n_4w,
            SUM(CASE WHEN return_4w IS NOT NULL
                      AND return_4w <= -10 THEN 1 ELSE 0 END) * 1.0 /
                NULLIF(SUM(CASE WHEN return_4w IS NOT NULL THEN 1 ELSE 0 END), 0) AS failure_rate,
            SUM(CASE WHEN alpha_4w > 0 THEN 1 ELSE 0 END) * 1.0 /
                NULLIF(SUM(CASE WHEN alpha_4w IS NOT NULL THEN 1 ELSE 0 END), 0) AS beat_rate
        FROM performance_log
        WHERE return_4w IS NOT NULL
    """).fetchone()
    stats["perf"] = dict(perf) if perf else {}

    # Strict vs Standard comparison (4W returns)
    sv = conn.execute("""
        SELECT
            AVG(CASE WHEN qh.strict_qualifies=1 THEN pl.return_4w END) as strict_avg_4w,
            AVG(CASE WHEN qh.strict_qualifies=0 THEN pl.return_4w END) as standard_avg_4w,
            COUNT(CASE WHEN qh.strict_qualifies=1 AND pl.return_4w IS NOT NULL THEN 1 END) as strict_n,
            COUNT(CASE WHEN qh.strict_qualifies=0 AND pl.return_4w IS NOT NULL THEN 1 END) as standard_n
        FROM performance_log pl
        JOIN qualification_history qh
          ON pl.ticker = qh.ticker AND pl.qual_date = qh.screen_date
        WHERE pl.return_4w IS NOT NULL
    """).fetchone()
    stats["strict_vs_standard"] = dict(sv) if sv else {}

    # Regime shift detection (last 7 days)
    regime_rows = conn.execute("""
        SELECT screen_date, regime FROM market_regime
        ORDER BY screen_date DESC LIMIT 7
    """).fetchall()
    regimes = [r["regime"] for r in regime_rows if r["regime"]]
    unique_regimes = list(dict.fromkeys(regimes))
    stats["regime_shift"] = unique_regimes if len(unique_regimes) > 1 else []
    stats["regimes_7d"] = [dict(r) for r in regime_rows]

    return stats


# ══════════════════════════════════════════════════════════════════════
#  FUNDAMENTALS  (Layer 2)
# ══════════════════════════════════════════════════════════════════════
def _safe_growth(now: float, prior: float) -> float:
    if prior is None or prior == 0: return 0.0
    return (now / abs(prior) - 1.0) * 100.0


def fetch_fundamentals(ticker: str) -> Optional[FundData]:
    try:
        tk = yf.Ticker(ticker)
        try:    q_income = tk.quarterly_income_stmt
        except: q_income = None
        try:    info = tk.info or {}
        except: info = {}

        roe      = float(info.get("returnOnEquity") or 0) * 100
        pe_ratio = float(info.get("trailingPE")     or 0)

        eps_growth_q = eps_growth_annual = rev_growth_q = 0.0
        eps_accelerating = margin_trend_up = False
        quality = "missing"

        has_income = (q_income is not None
                      and not q_income.empty
                      and q_income.shape[1] >= 4)

        if has_income:
            quality    = "good"
            nq         = q_income.shape[1]

            def get_row(labels):
                for lbl in labels:
                    if lbl in q_income.index:
                        return q_income.loc[lbl].values.astype(float)
                return None

            rev_row = get_row(["Total Revenue","Revenue","TotalRevenue"])
            ni_row  = get_row(["Net Income","NetIncome",
                                "Net Income Common Stockholders",
                                "Net Income Applicable To Common Shares"])

            if rev_row is not None and nq >= 4:
                r0 = rev_row[0]; r3 = rev_row[3]
                if r3 and r3 != 0:
                    rev_growth_q = _safe_growth(r0, r3)
                else:
                    quality = "partial"
            else:
                quality = "partial"

            if ni_row is not None and nq >= 4:
                eps_growth_q = _safe_growth(ni_row[0], ni_row[3])

                if nq >= 6:
                    g0 = _safe_growth(ni_row[0], ni_row[3])
                    g1 = _safe_growth(ni_row[1], ni_row[4])
                    g2 = _safe_growth(ni_row[2], ni_row[5])
                    eps_accelerating = (abs(g0) > 5 and g0 > g1) or (abs(g1) > 5 and g1 > g2)

                if nq >= 8:
                    eps_growth_annual = _safe_growth(
                        float(np.nansum(ni_row[:4])),
                        float(np.nansum(ni_row[4:8]))
                    )
                else:
                    eg = info.get("earningsGrowth")
                    eps_growth_annual = float(eg) * 100 if eg is not None else eps_growth_q
            else:
                quality = "partial"
                eg = info.get("earningsQuarterlyGrowth")
                if eg is not None:
                    eps_growth_q = float(eg) * 100

            if rev_row is not None and ni_row is not None and nq >= 3:
                margins = [ni_row[i] / rev_row[i]
                           for i in range(min(4, nq))
                           if rev_row[i] and rev_row[i] != 0]
                if len(margins) >= 3:
                    margin_trend_up = (margins[0] >= margins[-1] - 0.02)
        else:
            eg_q = info.get("earningsQuarterlyGrowth")
            rg   = info.get("revenueGrowth")
            if eg_q is not None:
                eps_growth_q = eps_growth_annual = float(eg_q) * 100
                quality = "partial"
            if rg is not None:
                rev_growth_q = float(rg) * 100

        return FundData(
            ticker=ticker, eps_growth_q=eps_growth_q,
            eps_growth_annual=eps_growth_annual, rev_growth_q=rev_growth_q,
            eps_accelerating=eps_accelerating, margin_trend_up=margin_trend_up,
            roe=roe, pe_ratio=pe_ratio, data_quality=quality,
        )
    except Exception as e:
        log.debug(f"  [{ticker}] fundamental error: {e}")
        return None


def apply_fundamentals(f: FundData) -> tuple:
    if f.data_quality == "missing":
        return False, 0, {}, ["NO_FUND_DATA"], "No fundamental data available"

    d = {}
    d["EPS_GATE"] = (f.eps_growth_q >= EPS_GROWTH_MIN)
    eps_gate_pass = d["EPS_GATE"]
    if f.data_quality == "partial":
        eps_gate_pass = (f.eps_growth_q >= EPS_GROWTH_MIN or f.eps_growth_q >= -10)

    d["S1_accel"]   = 1 if f.eps_accelerating else 0
    d["S2_annual"]  = 1 if f.eps_growth_annual >= EPS_GROWTH_MIN else 0
    d["S3_revenue"] = 1 if f.rev_growth_q >= REV_GROWTH_MIN else 0
    d["S4_rev_ok"]  = 1 if f.rev_growth_q > -30 else 0
    d["S5_margin"]  = 1 if f.margin_trend_up else 0
    d["S6_roe"]     = 1 if f.roe >= ROE_MIN else 0
    fund_score = sum(v for k, v in d.items() if k.startswith("S"))

    rev_collapse = (f.rev_growth_q < -50)
    passes = (eps_gate_pass and not rev_collapse and fund_score >= FUND_SCORE_MIN)

    fail_reasons = []
    if not eps_gate_pass:
        fail_reasons.append(f"EPS {f.eps_growth_q:.0f}% vs min {EPS_GROWTH_MIN:.0f}%")
    if rev_collapse:
        fail_reasons.append(f"Revenue collapse {f.rev_growth_q:.0f}%")
    if fund_score < FUND_SCORE_MIN:
        fail_reasons.append(f"Score {fund_score}/{FUND_SCORE_MIN}")

    note = (f"EPS +{f.eps_growth_q:.0f}% | Score {fund_score}/6" if passes
            else " | ".join(fail_reasons))
    return passes, fund_score, d, fail_reasons, note


# ══════════════════════════════════════════════════════════════════════
#  CHANGE DETECTION ENGINE + LIFECYCLE TRACKING
# ══════════════════════════════════════════════════════════════════════
def detect_changes(
    results: list[ScreenResult],
    yesterday_qualifiers: set[str],
    conn: sqlite3.Connection,
    today: str,
) -> tuple[list[ScreenResult], list[str]]:
    today_all = {r.ticker for r in results}
    dropped_tickers = list(yesterday_qualifiers - today_all)

    for r in results:
        if r.qualifies:
            prior_streak = db_get_streak(conn, r.ticker, today)
            r.streak     = prior_streak + 1
            r.change_type = "RETAINED" if r.ticker in yesterday_qualifiers else "NEW"

            # Lifecycle
            lc = db_get_lifecycle(conn, r.ticker, today)
            r.first_qual_date   = lc["first_qual_date"] or today
            r.total_qual_cycles = lc["total_qual_cycles"] + (1 if r.change_type == "NEW" else 0)
            r.longest_streak    = max(lc["longest_streak"], r.streak)
        else:
            r.streak      = 0
            r.change_type = "TECH_ONLY" if r.tt_pass else "OTHER"

    return results, dropped_tickers


# ══════════════════════════════════════════════════════════════════════
#  MARKET STRUCTURE OBSERVATION
# ══════════════════════════════════════════════════════════════════════
def generate_market_observation(
    today_full: int,
    today_tech: int,
    today_close: int,
    new_count: int,
    dropped_count: int,
    breadth_history: list[dict],
    sector_counts: dict[str, int],
    regime: dict = None,
) -> str:
    lines = []

    if len(breadth_history) >= 5:
        avg5 = sum(r["full_qualifiers"] for r in breadth_history[-5:]) / 5
    elif breadth_history:
        avg5 = sum(r["full_qualifiers"] for r in breadth_history) / len(breadth_history)
    else:
        avg5 = None

    if breadth_history:
        hist_vals  = [r["full_qualifiers"] for r in breadth_history]
        max30      = max(hist_vals)
        min30      = min(hist_vals)
        range_note = f"30-day range: {min30}–{max30} full qualifiers."
    else:
        max30 = min30 = None
        range_note = "Insufficient history for 30-day range."

    if avg5 is not None:
        if today_full > avg5 * 1.10:
            trend = "expanding above its recent average"
        elif today_full < avg5 * 0.90:
            trend = "contracting below its recent average"
        else:
            trend = "broadly in line with its recent average"
    else:
        trend = "at early validation stage (insufficient history)"

    # Regime note
    if regime and regime.get("regime") not in ("Insufficient History", None):
        lines.append(f"Market regime classified as <strong>{regime['regime']}</strong>. {regime.get('description','')}")

    if today_full == 0:
        count_obs = ("Zero stocks meet full qualification criteria today. "
                     "This indicates a contraction phase in breadth.")
    elif today_full <= 5:
        count_obs = (f"{today_full} stock{'s' if today_full > 1 else ''} "
                     f"meet{'s' if today_full == 1 else ''} full qualification criteria today — "
                     "a low-breadth environment with highly selective conditions.")
    elif today_full <= 20:
        count_obs = (f"{today_full} stocks meet full qualification criteria today — "
                     "a moderate-breadth environment.")
    else:
        count_obs = (f"{today_full} stocks meet full qualification criteria today — "
                     "a broad-based qualification environment.")
    lines.append(count_obs)

    if avg5 is not None:
        lines.append(
            f"Full qualifier breadth is {trend} (5-day avg: {avg5:.1f}). {range_note}"
        )

    if new_count > 0 or dropped_count > 0:
        if new_count > dropped_count:
            flow = (f"{new_count} new qualification{'s' if new_count > 1 else ''} "
                    f"offset {dropped_count} dropout{'s' if dropped_count > 1 else ''} — net positive flow.")
        elif dropped_count > new_count:
            flow = (f"{dropped_count} stock{'s' if dropped_count > 1 else ''} "
                    f"lost full qualification, {new_count} entering — net negative flow.")
        else:
            flow = (f"Equal entries and exits ({new_count} each) — composition shifted, size held.")
        lines.append(flow)

    if sector_counts:
        top_sectors = sorted(sector_counts.items(), key=lambda x: -x[1])[:3]
        sec_str = ", ".join(f"{s} ({n})" for s, n in top_sectors if s and s != "--")
        if sec_str:
            lines.append(f"Leading sectors: {sec_str}.")

    if today_close > 0:
        lines.append(
            f"{today_close} stock{'s' if today_close > 1 else ''} "
            f"meet 7 of 8 Multi-Factor Trend Filter criteria — close to full technical qualification."
        )

    return " ".join(lines)


# ══════════════════════════════════════════════════════════════════════
#  SECTOR BREAKDOWN
# ══════════════════════════════════════════════════════════════════════
def build_sector_breakdown(results: list[ScreenResult]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for r in results:
        if r.qualifies:
            sec = r.sector if r.sector and r.sector not in ("--", "") else "Other"
            counts[sec] += 1
    return dict(counts)


# ══════════════════════════════════════════════════════════════════════
#  MAIN SCREENING PIPELINE
# ══════════════════════════════════════════════════════════════════════
def run_screening() -> tuple:
    start     = time.time()
    today_str = date.today().isoformat()
    conn      = get_db()

    log.info("=" * 68)
    log.info(f"  {REPORT_NAME} {VERSION}  --  NSE  --  {today_str}")
    log.info("=" * 68)

    yesterday_qualifiers = db_get_yesterday_qualifiers(conn, today_str)
    log.info(f"Prior session full qualifiers loaded: {len(yesterday_qualifiers)} tickers")

    # 1. Ticker universe
    tickers = fetch_indian_tickers()

    # 2. Price history download
    log.info(f"Downloading price history for {len(tickers)} NSE stocks...")
    hist_map: dict = {}
    total_batches = math.ceil(len(tickers) / BATCH_SIZE)

    for b_num, i in enumerate(range(0, len(tickers), BATCH_SIZE), start=1):
        batch = tickers[i : i + BATCH_SIZE]
        log.info(f"  Batch {b_num}/{total_batches}: {batch[0]} ... {batch[-1]}")
        try:
            raw = yf.download(batch, period="2y", auto_adjust=True,
                              progress=False, threads=True)
            if isinstance(raw.columns, pd.MultiIndex):
                for sym in batch:
                    try:
                        df = raw.xs(sym, axis=1, level=1).dropna(how="all")
                        if len(df) >= 202:
                            hist_map[sym] = df
                    except Exception:
                        pass
            else:
                if batch and len(raw) >= 202:
                    hist_map[batch[0]] = raw
        except Exception as e:
            log.warning(f"  Batch {b_num} error: {e}")
        if b_num < total_batches:
            time.sleep(BATCH_SLEEP)

    log.info(f"Usable price history: {len(hist_map)} stocks")

    # 3. Liquidity filter
    log.info("Applying liquidity filters...")
    pre_filtered = {
        sym: hist for sym, hist in hist_map.items()
        if (float(hist["Close"].iloc[-1]) >= MIN_PRICE
            and float(hist["Volume"].tail(50).mean()) >= MIN_AVG_VOL)
    }
    log.info(f"  After liquidity filter: {len(pre_filtered)} NSE stocks remain")

    # 4. Technical indicators
    log.info("Computing technical indicators...")
    tech_map: dict = {}
    for sym, hist in pre_filtered.items():
        td = compute_tech(sym, hist)
        if td:
            tech_map[sym] = td
    log.info(f"  Indicators computed: {len(tech_map)} stocks")

    # 5. RS ranking (two-pass)
    log.info("Ranking RS scores across universe...")
    rank_rs_scores(tech_map)

    # 6. Multi-Factor Trend Filter
    log.info("Applying Multi-Factor Trend Filter (Layer 1 - Technical)...")
    l1_pass:  list = []
    l1_close: list = []
    for sym, td in tech_map.items():
        passes, score, detail, fails = apply_multi_factor_filter(td)
        if passes:
            l1_pass.append((sym, td, detail, fails))
        elif score >= 7:
            l1_close.append((sym, td, score))
    log.info(f"  Full MFT pass (8/8): {len(l1_pass)} stocks")
    log.info(f"  Close calls  (7/8 MFT): {len(l1_close)} stocks")

    # 7. Fundamentals + final results
    log.info(f"Fetching fundamental data for {len(l1_pass)} technical qualifiers...")
    results: list[ScreenResult] = []
    meta_rows: list[dict] = []

    for idx, (sym, td, tt_detail, _) in enumerate(l1_pass, start=1):
        log.info(f"  [{idx}/{len(l1_pass)}] {sym} -- fetching fundamentals")
        try:
            f = fetch_fundamentals(sym)
            time.sleep(FUND_SLEEP)

            if f is not None:
                fund_pass, fund_score, fund_detail, fund_fails, fund_note = apply_fundamentals(f)
            else:
                fund_pass = False; fund_score = 0
                fund_detail = {}; fund_fails = ["NO_FUND_DATA"]
                fund_note = "No fundamental data returned"
                f = None

            brs_score, tech_score, fund_score_norm = compute_breakout_score(td, f)

            # Strict mode check
            strict_pass = apply_strict_filter(td, f, fund_score) if fund_pass else False

            try:
                info      = yf.Ticker(sym).info or {}
                company   = info.get("longName") or info.get("shortName") or sym
                sector    = info.get("sector", "--") or "--"
                mkt_cap_b = (info.get("marketCap") or 0) / 1e9
                pe_ratio  = float(info.get("trailingPE") or 0)
            except Exception:
                company = sym; sector = "--"; mkt_cap_b = 0.0; pe_ratio = 0.0

            meta_rows.append({
                "ticker": sym, "company": company, "sector": sector,
                "market_cap_b": mkt_cap_b,
                "last_updated": today_str,
            })

            res = ScreenResult(
                ticker=sym, company=company, price=td.price,
                market_cap_b=mkt_cap_b, sector=sector,
                tt_pass=True, tt_score=8, tt_detail=tt_detail,
                fund_pass=fund_pass, fund_score=fund_score,
                fund_detail=fund_detail, fund_note=fund_note,
                rs_rank=td.rs_rank, breakout_score=brs_score,
                tech_score=tech_score, fund_score_norm=fund_score_norm,
                eps_growth_q=f.eps_growth_q if f else 0.0,
                rev_growth_q=f.rev_growth_q if f else 0.0,
                eps_accelerating=f.eps_accelerating if f else False,
                roe=f.roe if f else 0.0,
                pe_ratio=pe_ratio,
                data_quality=f.data_quality if f else "missing",
                strict_qualifies=strict_pass,
                qualifies=fund_pass,
                fail_reasons=fund_fails if not fund_pass else [],
            )
            results.append(res)

        except Exception as e:
            log.warning(f"  [{sym}] error: {e}")
            log.debug(traceback.format_exc())

    # 8. Change detection + lifecycle
    log.info("Running change detection + lifecycle engine...")
    results, dropped_tickers = detect_changes(
        results, yesterday_qualifiers, conn, today_str
    )

    # Sort: full qualifiers first, then by Breakout Readiness Score desc
    results.sort(key=lambda r: (-int(r.qualifies), -r.breakout_score, -r.rs_rank))

    # 9. Sector breakdown
    sector_counts = build_sector_breakdown(results)

    # 10. Market Regime
    breadth_history = db_get_breadth_history(conn)  # before today
    regime = classify_market_regime(breadth_history)
    log.info(f"Market Regime: {regime['regime']}")

    # 11. Persist to DB
    log.info("Persisting results to database...")
    db_rows = []
    for r in results:
        db_rows.append({
            "screen_date":       today_str,
            "ticker":            r.ticker,
            "full_qualifies":    1 if r.qualifies else 0,
            "strict_qualifies":  1 if r.strict_qualifies else 0,
            "tt_pass":           1 if r.tt_pass else 0,
            "sepa_score":        r.breakout_score,
            "tech_score":        r.tech_score,
            "fund_score_norm":   r.fund_score_norm,
            "eps_growth_q":      r.eps_growth_q if not math.isnan(r.eps_growth_q) else None,
            "rev_growth_q":      r.rev_growth_q if not math.isnan(r.rev_growth_q) else None,
            "rs_rank":           r.rs_rank,
            "price":             r.price,
            "sector":            r.sector,
            "fund_score":        r.fund_score,
            "change_type":       r.change_type,
            "streak":            r.streak,
            "first_qual_date":   r.first_qual_date,
            "total_qual_cycles": r.total_qual_cycles,
            "longest_streak":    r.longest_streak,
            "data_quality":      r.data_quality,
        })
    db_upsert_qualification(conn, db_rows)
    db_upsert_meta(conn, meta_rows)

    qualifiers    = [r for r in results if r.qualifies]
    strict_quals  = [r for r in qualifiers if r.strict_qualifies]
    tech_only     = [r for r in results if not r.qualifies]
    new_today     = [r for r in qualifiers if r.change_type == "NEW"]
    new_count     = len(new_today)
    dropped_count = len(dropped_tickers)

    brs_median = (float(np.median([r.breakout_score for r in qualifiers]))
                   if qualifiers else None)
    top_sector  = max(sector_counts, key=sector_counts.get) if sector_counts else None

    elapsed = int(time.time() - start)
    db_upsert_breadth(conn, {
        "screen_date":        today_str,
        "total_scanned":      len(tech_map),
        "full_qualifiers":    len(qualifiers),
        "strict_qualifiers":  len(strict_quals),
        "tech_only":          len(tech_only),
        "close_calls":        len(l1_close),
        "new_qualifiers":     new_count,
        "dropped_qualifiers": dropped_count,
        "top_sector":         top_sector,
        "sepa_score_median":  brs_median,
        "run_duration_s":     elapsed,
        "regime":             regime["regime"],
    })
    db_upsert_regime(conn, {
        "screen_date":      today_str,
        "regime":           regime["regime"],
        "regime_color":     regime["regime_color"],
        "breadth_7d_avg":   regime.get("breadth_7d_avg"),
        "breadth_14d_avg":  regime.get("breadth_14d_avg"),
        "breadth_30d_avg":  regime.get("breadth_30d_avg"),
        "trend_direction":  regime.get("trend_direction"),
    })

    # 12. Seed performance log for today's qualifiers
    db_seed_performance_log(conn, today_str)

    # 13. Update prior performance returns (background)
    try:
        db_update_performance_returns(conn, today_str)
    except Exception as e:
        log.warning(f"Performance update failed (non-fatal): {e}")

    # 14. Get performance stats for report
    perf_stats = db_get_performance_stats(conn)

    # Reload breadth history after today's entry
    breadth_history = db_get_breadth_history(conn)

    log.info("=" * 68)
    log.info(f"  Screening complete in {elapsed}s")
    log.info(f"  Market Regime        : {regime['regime']}")
    log.info(f"  Full Qualifiers (NSE)  : {len(qualifiers)}")
    log.info(f"  Strict Qualifiers    : {len(strict_quals)}")
    log.info(f"  Technical Only       : {len(tech_only)}")
    log.info(f"  Close Calls (7/8 MFT) : {len(l1_close)}")
    log.info(f"  NEW today            : {new_count}")
    log.info(f"  DROPPED today        : {dropped_count}")
    log.info("=" * 68)

    conn.close()
    return (results, l1_close, dropped_tickers, breadth_history,
            sector_counts, regime, perf_stats)


# ══════════════════════════════════════════════════════════════════════
#  HTML REPORT
# ══════════════════════════════════════════════════════════════════════
def _build_html_content(
    results, l1_close, dropped_tickers, breadth_history,
    sector_counts, regime, perf_stats, for_pdf=False
):
    today_str   = date.today().strftime("%B %d, %Y")
    report_time = datetime.now().strftime("%H:%M:%S")

    qualifiers    = [r for r in results if r.qualifies]
    strict_quals  = [r for r in qualifiers if r.strict_qualifies]
    tech_only     = [r for r in results if not r.qualifies]
    new_today     = [r for r in qualifiers if r.change_type == "NEW"]
    new_count     = len(new_today)
    dropped_count = len(dropped_tickers)

    if len(breadth_history) >= 5:
        avg5     = sum(r["full_qualifiers"] for r in breadth_history[-5:]) / 5
        avg5_str = f"{avg5:.1f}"
    else:
        avg5_str = "--"

    hist_vals  = [r["full_qualifiers"] for r in breadth_history] if breadth_history else []
    max30      = max(hist_vals) if hist_vals else "--"
    min30      = min(hist_vals) if hist_vals else "--"
    prior_full = breadth_history[-2]["full_qualifiers"] if len(breadth_history) >= 2 else None
    if prior_full is not None:
        net_change = len(qualifiers) - prior_full
        net_str    = f"+{net_change}" if net_change >= 0 else str(net_change)
    else:
        net_str = "--"

    net_color      = "#10b981" if (isinstance(net_str, str) and net_str.startswith("+")) else "#ef4444"
    net_card_class = "green"   if (isinstance(net_str, str) and net_str.startswith("+")) else "red"

    observation = generate_market_observation(
        len(qualifiers), len(tech_only), len(l1_close),
        new_count, dropped_count, breadth_history, sector_counts, regime,
    )

    regime_name  = regime.get("regime", "Unknown")
    regime_color = regime.get("regime_color", "#6b7280")
    regime_desc  = regime.get("description", "")

    # ── Sector HTML ──────────────────────────────────────────────────
    sector_html = ""
    if sector_counts:
        sorted_sectors = sorted(sector_counts.items(), key=lambda x: -x[1])
        max_n = max(n for _, n in sorted_sectors) if sorted_sectors else 1
        total_q = sum(sector_counts.values())
        rows = "".join(
            '<div class="sector-row">'
            f'<div class="sector-name">{s}</div>'
            f'<div class="sector-bar-bg"><div class="sector-bar" style="width:{min(int(n/max_n*100),100)}%"></div></div>'
            f'<div class="sector-num">{n}'
            f'<span style="color:#4b5563;font-size:9px;margin-left:4px">({int(n/total_q*100)}%)</span>'
            '</div>'
            '</div>'
            for s, n in sorted_sectors if s
        )
        sector_html = (
            '<div style="margin-top:20px;margin-bottom:4px">'
            '<div style="font-size:10px;font-weight:700;color:#4b5563;text-transform:uppercase;'
            'letter-spacing:.08em;margin-bottom:14px">Sector Distribution</div>'
            + rows + '</div>'
        )

    def score_badge(score):
        if score >= 75:   cls = "badge badge-score-hi"
        elif score >= 50: cls = "badge badge-score-md"
        else:             cls = "badge badge-score-lo"
        return f'<span class="{cls}">{score}</span>'

    def subscore_dots(val, max_val=10):
        """Mini bar 0-10 as filled dots."""
        filled = min(int(val), 10)
        return (
            f'<span style="font-size:9px;letter-spacing:1px;color:#6b7280">'
            + "●" * filled + "○" * (max_val - filled)
            + f'</span><span style="font-size:9px;color:#4b5563;margin-left:3px">{val}</span>'
        )

    def change_badge(ct):
        if ct == "NEW":      return '<span class="badge badge-new">NEW</span>'
        if ct == "RETAINED": return '<span class="badge badge-held">HELD</span>'
        return ""

    def strict_badge():
        return '<span class="badge badge-strict">STRICT</span>'

    def streak_cell(streak):
        if streak <= 1: return '<td style="text-align:right;padding:10px 14px">--</td>'
        return f'<td style="text-align:right;padding:10px 14px"><span class="streak-val">{streak}d</span></td>'

    def fmt_pct(val):
        if val is None or (isinstance(val, float) and math.isnan(val)): return "&mdash;"
        col = "#10b981" if val >= 0 else "#ef4444"
        return f'<span style="color:{col};font-weight:600">{val:+.0f}%</span>'

    def fmt_price(val):
        return f'<span style="color:#e2e8f0;font-weight:500">&#x20B9;{val:,.2f}</span>' if val else "&mdash;"

    def lifecycle_cell(r):
        parts = []
        if r.first_qual_date:
            parts.append(f'1st: {r.first_qual_date[5:]}')
        if r.total_qual_cycles > 1:
            parts.append(f'Cycles: {r.total_qual_cycles}')
        return "<br>".join(parts) if parts else "--"

    def result_row(r, show_change=True, show_strict=True, show_lifecycle=False):
        acc        = '<span class="acc-tag">ACC</span>' if r.eps_accelerating else ""
        notes      = (r.fund_note if r.qualifies else ", ".join(r.fail_reasons[:2]))[:42]
        badge_html = (change_badge(r.change_type) + "&nbsp;") if show_change else ""
        s_badge    = (strict_badge() + "&nbsp;") if (show_strict and r.strict_qualifies) else ""
        lc_td = (f'<td style="padding:8px 14px;color:#374151;font-size:9px;line-height:1.6">'
                 f'{lifecycle_cell(r)}</td>') if show_lifecycle else ""
        return (
            f'<tr>'
            f'<td style="padding:10px 14px">{badge_html}{s_badge}'
            f'<strong style="color:#f1f5f9;font-weight:700">{r.ticker}</strong></td>'
            f'<td style="padding:10px 14px;color:#4b5563;font-size:11px">{r.company[:24]}</td>'
            f'<td style="padding:10px 14px;text-align:right">{fmt_price(r.price)}</td>'
            f'<td style="padding:10px 14px;text-align:right">{score_badge(r.breakout_score)}</td>'
            f'<td style="padding:10px 14px;text-align:center">{subscore_dots(r.tech_score)}</td>'
            f'<td style="padding:10px 14px;text-align:center">{subscore_dots(r.fund_score_norm)}</td>'
            f'<td style="padding:10px 14px;color:#94a3b8;text-align:right;font-weight:600">{r.rs_rank}</td>'
            f'<td style="padding:10px 14px;text-align:right">{fmt_pct(r.eps_growth_q)}{acc}</td>'
            f'<td style="padding:10px 14px;text-align:right">{fmt_pct(r.rev_growth_q)}</td>'
            f'<td style="padding:10px 14px;color:#4b5563;font-size:11px">{r.sector[:18]}</td>'
            f'{streak_cell(r.streak)}'
            f'<td style="padding:10px 14px;color:#374151;font-size:10px">{notes}</td>'
            f'{lc_td}'
            f'</tr>'
        )

    TH_S  = "padding:10px 14px;color:#4b5563;font-size:10px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;border-bottom:1px solid rgba(255,255,255,0.05);text-align:left;white-space:nowrap"
    THR_S = "padding:10px 14px;color:#4b5563;font-size:10px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;border-bottom:1px solid rgba(255,255,255,0.05);text-align:right;white-space:nowrap"
    THC_S = THR_S.replace("text-align:right","text-align:center")

    def table(rows_html, col_streak=True, col_lifecycle=False):
        streak_th    = f'<th style="{THR_S}">Streak</th>' if col_streak else ""
        lifecycle_th = f'<th style="{TH_S}">Lifecycle</th>' if col_lifecycle else ""
        return (
            '<table style="width:100%;border-collapse:collapse;font-size:12px">'
            '<thead><tr style="background:rgba(255,255,255,0.02)">'
            f'<th style="{TH_S}">Ticker</th>'
            f'<th style="{TH_S}">Company</th>'
            f'<th style="{THR_S}">Price</th>'
            f'<th style="{THR_S}">BRS</th>'
            f'<th style="{THC_S}">Tech</th>'
            f'<th style="{THC_S}">Fund</th>'
            f'<th style="{THR_S}">RS</th>'
            f'<th style="{THR_S}">EPS%</th>'
            f'<th style="{THR_S}">Rev%</th>'
            f'<th style="{TH_S}">Sector</th>'
            + streak_th + lifecycle_th +
            f'<th style="{TH_S}">Note</th>'
            '</tr></thead>'
            f'<tbody>{rows_html}</tbody></table>'
        )

    def empty_row(msg, cols=12):
        return f'<tr><td colspan="{cols}" style="padding:20px;color:#374151;text-align:center;font-style:italic;font-size:12px">{msg}</td></tr>'

    breadth_nums = " | ".join(
        f'{r["screen_date"][5:]} \u2192 {r["full_qualifiers"]}'
        for r in breadth_history[-10:]
    ) if breadth_history else "--"

    qual_html   = "".join(result_row(r, show_lifecycle=True) for r in qualifiers) or empty_row("No full qualifiers today.")
    strict_html = "".join(result_row(r) for r in strict_quals)                    or empty_row("No strict qualifiers today.")
    new_html    = "".join(result_row(r) for r in new_today)                       or empty_row("No new qualifiers today.")
    tech_html   = "".join(result_row(r, False, False) for r in tech_only[:50])    or empty_row("None.")
    close_html  = "".join(
        f'<tr style="border-bottom:1px solid rgba(255,255,255,0.04)">'
        f'<td style="padding:10px 14px"><strong style="color:#f1f5f9;font-weight:700">{sym}</strong></td>'
        f'<td style="padding:10px 14px;color:#4b5563;font-size:11px">'
        f'{score}/8 criteria met — 1 criterion missing from full technical qualification</td></tr>'
        for sym, _, score in l1_close[:30]
    ) or empty_row("No close calls today.", 2)
    drop_html = "".join(
        f'<tr style="border-bottom:1px solid rgba(255,255,255,0.04)">'
        f'<td style="padding:10px 14px"><strong style="color:#f1f5f9;font-weight:700">{sym}</strong></td>'
        f'<td style="padding:10px 14px;color:#ef444480;font-size:11px">No longer meets full qualification criteria as of today</td></tr>'
        for sym in dropped_tickers
    ) or empty_row("No stocks dropped full qualification today.", 2)

    # ── Performance analytics HTML ────────────────────────────────────
    def _perf_card(period_key, label):
        if period_key not in perf_stats:
            return f'<div class="perf-card"><div class="perf-label">{label}</div><div class="perf-val" style="color:#374151;font-size:14px">Insufficient data</div></div>'
        p = perf_stats[period_key]
        n        = p.get("n", 0)
        avg_ret  = p.get("avg_ret")
        win_rate = p.get("win_rate")
        avg_alpha= p.get("avg_alpha")
        beat_rate= p.get("beat_rate")

        def _fmt(v, suffix="%", prec=1):
            if v is None: return "&mdash;"
            col = "#10b981" if v >= 0 else "#ef4444"
            sign = "+" if v >= 0 else ""
            return f'<span style="color:{col};font-weight:600">{sign}{v:.{prec}f}{suffix}</span>'

        return (
            f'<div class="perf-card">'
            f'<div class="perf-label">{label} <span style="color:#374151;font-size:9px">n={n}</span></div>'
            f'<div class="perf-val">{_fmt(avg_ret)}</div>'
            f'<div style="font-size:10px;color:#4b5563;margin-top:6px;line-height:1.8">'
            f'Win rate: {_fmt(win_rate*100 if win_rate else None, "%", 0)}<br>'
            f'Avg Alpha: {_fmt(avg_alpha)}<br>'
            f'Beat rate: {_fmt(beat_rate*100 if beat_rate else None, "%", 0)}'
            f'</div></div>'
        )

    perf_html = (
        '<div class="perf-grid">'
        + _perf_card("1w",  "1 Week Forward")
        + _perf_card("4w",  "4 Week Forward")
        + _perf_card("12w", "12 Week Forward")
        + '</div>'
    )

    # ── HTML assembly ─────────────────────────────────────────────────
    html = (
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n"
        "<meta charset=\"UTF-8\"/>\n"
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1.0\"/>\n"
        f"<title>{REPORT_NAME} &mdash; {today_str}</title>\n"
        "<style>\n"
        "  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');\n"
        "  *{box-sizing:border-box;margin:0;padding:0}\n"
        "  body{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#09090f;color:#e2e8f0;font-size:13px;line-height:1.6}\n"
        "  .wrap{max-width:1200px;margin:0 auto;padding:40px 28px}\n"
        "  .navbar{display:flex;justify-content:space-between;align-items:center;padding:0 0 32px 0;border-bottom:1px solid rgba(255,255,255,0.06);margin-bottom:40px}\n"
        "  .brand{font-size:17px;font-weight:800;letter-spacing:-.5px;color:#fff}\n"
        "  .brand span{color:#a855f7;font-weight:400}\n"
        "  .nav-badge{background:rgba(239,68,68,.12);color:#f87171;border:1px solid rgba(239,68,68,.25);border-radius:20px;padding:5px 14px;font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase}\n"
        "  .hero{margin-bottom:40px}\n"
        "  .hero-tag{display:inline-block;background:rgba(168,85,247,.15);color:#c084fc;border:1px solid rgba(168,85,247,.3);border-radius:20px;padding:4px 14px;font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;margin-bottom:14px}\n"
        "  .hero h1{font-size:28px;font-weight:800;letter-spacing:-.5px;color:#fff;margin-bottom:6px}\n"
        "  .hero-meta{font-size:11px;color:#4b5563}\n"
        "  .hero-meta span{color:#6b7280}\n"
        "  .regime-card{display:inline-flex;align-items:center;gap:10px;border-radius:10px;padding:10px 18px;margin-bottom:20px;border:1px solid rgba(255,255,255,0.07)}\n"
        "  .regime-dot{width:10px;height:10px;border-radius:50%}\n"
        "  .regime-name{font-size:12px;font-weight:700;letter-spacing:.05em}\n"
        "  .regime-desc{font-size:11px;color:#4b5563;margin-top:4px}\n"
        "  .stat-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:12px;margin-bottom:28px}\n"
        "  .stat-card{background:#0f1117;border:1px solid rgba(255,255,255,0.06);border-radius:12px;padding:16px 18px;position:relative;overflow:hidden}\n"
        "  .stat-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px}\n"
        "  .stat-card.green::before{background:linear-gradient(90deg,#10b981,transparent)}\n"
        "  .stat-card.amber::before{background:linear-gradient(90deg,#f59e0b,transparent)}\n"
        "  .stat-card.blue::before{background:linear-gradient(90deg,#3b82f6,transparent)}\n"
        "  .stat-card.purple::before{background:linear-gradient(90deg,#a855f7,transparent)}\n"
        "  .stat-card.red::before{background:linear-gradient(90deg,#ef4444,transparent)}\n"
        "  .stat-card.gray::before{background:linear-gradient(90deg,#475569,transparent)}\n"
        "  .stat-card.teal::before{background:linear-gradient(90deg,#14b8a6,transparent)}\n"
        "  .stat-val{font-size:26px;font-weight:800;line-height:1;letter-spacing:-1px}\n"
        "  .stat-label{font-size:10px;color:#4b5563;margin-top:5px;text-transform:uppercase;letter-spacing:.06em;font-weight:500}\n"
        "  .obs{background:#0f1117;border:1px solid rgba(168,85,247,.2);border-left:3px solid #a855f7;border-radius:0 10px 10px 0;padding:16px 20px;font-size:12.5px;color:#94a3b8;line-height:1.85;margin-bottom:24px}\n"
        "  .section{margin-bottom:40px}\n"
        "  .section-header{display:flex;align-items:center;gap:12px;margin-bottom:6px}\n"
        "  .section-title{font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;white-space:nowrap}\n"
        "  .section-header::after{content:'';flex:1;height:1px;background:rgba(255,255,255,0.05)}\n"
        "  .section-desc{font-size:11px;color:#4b5563;margin-bottom:14px;line-height:1.7}\n"
        "  .tbl-wrap{background:#0f1117;border:1px solid rgba(255,255,255,0.06);border-radius:12px;overflow:hidden}\n"
        "  .tbl-wrap table tbody tr{border-bottom:1px solid rgba(255,255,255,0.04)}\n"
        "  .tbl-wrap table tbody tr:last-child{border-bottom:none}\n"
        "  .badge{display:inline-block;border-radius:6px;padding:2px 8px;font-size:9px;font-weight:700;letter-spacing:.06em;text-transform:uppercase}\n"
        "  .badge-new{background:rgba(16,185,129,.12);color:#34d399;border:1px solid rgba(16,185,129,.25)}\n"
        "  .badge-held{background:rgba(59,130,246,.10);color:#60a5fa;border:1px solid rgba(59,130,246,.2)}\n"
        "  .badge-strict{background:rgba(245,158,11,.12);color:#fbbf24;border:1px solid rgba(245,158,11,.25)}\n"
        "  .badge-score-hi{background:rgba(16,185,129,.12);color:#34d399;border:1px solid rgba(16,185,129,.25)}\n"
        "  .badge-score-md{background:rgba(59,130,246,.10);color:#60a5fa;border:1px solid rgba(59,130,246,.2)}\n"
        "  .badge-score-lo{background:rgba(245,158,11,.10);color:#fbbf24;border:1px solid rgba(245,158,11,.2)}\n"
        "  .acc-tag{display:inline-block;background:rgba(245,158,11,.12);color:#fbbf24;border:1px solid rgba(245,158,11,.2);border-radius:4px;padding:1px 5px;font-size:8px;font-weight:800;letter-spacing:.06em;margin-left:3px;vertical-align:middle}\n"
        "  .streak-val{color:#fbbf24;font-weight:700}\n"
        "  .empty-msg{padding:20px;color:#374151;text-align:center;font-style:italic;font-size:12px}\n"
        "  .sector-row{display:flex;align-items:center;gap:12px;margin-bottom:8px}\n"
        "  .sector-name{font-size:11px;color:#6b7280;width:180px;flex-shrink:0}\n"
        "  .sector-bar-bg{flex:1;background:rgba(255,255,255,0.04);border-radius:4px;height:6px}\n"
        "  .sector-bar{height:6px;border-radius:4px;background:linear-gradient(90deg,#a855f7,#6366f1)}\n"
        "  .sector-num{font-size:11px;color:#e2e8f0;font-weight:700;width:60px;text-align:right}\n"
        "  .breadth-strip{background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.05);border-radius:8px;padding:12px 16px;font-size:10.5px;color:#374151;font-family:monospace;margin-top:20px;line-height:1.8}\n"
        "  .breadth-label{color:rgba(168,85,247,.4);margin-right:6px}\n"
        "  .perf-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:16px;margin-bottom:8px}\n"
        "  .perf-card{background:#0f1117;border:1px solid rgba(255,255,255,0.06);border-radius:12px;padding:18px 20px}\n"
        "  .perf-label{font-size:10px;font-weight:700;color:#4b5563;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px}\n"
        "  .perf-val{font-size:24px;font-weight:800;letter-spacing:-1px}\n"
        "  .method-card{background:#0f1117;border:1px solid rgba(255,255,255,0.06);border-radius:12px;padding:20px 24px;margin-bottom:28px}\n"
        "  .method-title{font-size:10px;font-weight:700;color:#4b5563;text-transform:uppercase;letter-spacing:.08em;margin-bottom:10px}\n"
        "  .method-body{font-size:11px;color:#4b5563;line-height:1.85}\n"
        "  .method-body strong{color:#6b7280}\n"
        "  .disc{color:#2d3748;font-size:10px;margin-top:32px;border-top:1px solid rgba(255,255,255,0.04);padding-top:18px;line-height:1.9}\n"
        "  .disc strong{color:#374151}\n"
        "</style>\n</head>\n<body>\n<div class=\"wrap\">\n\n"

        # NAVBAR
        "<div class=\"navbar\">\n"
        "  <div class=\"brand\"><span>ALPHA</span>dominico</div>\n"
        "  <div style=\"display:flex;align-items:center;gap:16px\">\n"
        f"    <span style=\"font-size:11px;color:#4b5563\">{VERSION} &nbsp;&bull;&nbsp; {MARKET_NAME}</span>\n"
        "    <div class=\"nav-badge\">Screening Tool Only</div>\n"
        "  </div>\n</div>\n\n"

        # HERO
        "<div class=\"hero\">\n"
        "  <div class=\"hero-tag\">Market Intelligence &mdash; Production Stage</div>\n"
        f"  <h1>{MARKET_NAME} Daily Screener &mdash; {today_str}</h1>\n"
        f"  <div class=\"hero-meta\">Generated at <span>{report_time}</span> &nbsp;&bull;&nbsp; Multi-Factor Trend Filter + Earnings Strength Overlay + Market Condition Engine</div>\n"
        "</div>\n\n"

        # MARKET REGIME CARD
        f"<div class=\"regime-card\" style=\"background:{regime.get('badge_bg','rgba(168,85,247,.08)')};border-color:rgba(255,255,255,0.07)\">\n"
        f"  <div class=\"regime-dot\" style=\"background:{regime_color}\"></div>\n"
        f"  <div>\n"
        f"    <div class=\"regime-name\" style=\"color:{regime_color}\">Market Regime: {regime_name}</div>\n"
        f"    <div class=\"regime-desc\">{regime_desc[:120]}</div>\n"
        "  </div>\n</div>\n\n"

        # MARKET STRUCTURE
        "<div class=\"section\">\n"
        "  <div class=\"section-header\">\n"
        "    <div class=\"section-title\" style=\"color:#a855f7\">Market Structure Summary</div>\n"
        "  </div>\n"
        "  <div class=\"stat-grid\">\n"
        f"    <div class=\"stat-card green\"><div class=\"stat-val\" style=\"color:#10b981\">{len(qualifiers)}</div><div class=\"stat-label\">Full Qualifiers</div></div>\n"
        f"    <div class=\"stat-card teal\"><div class=\"stat-val\" style=\"color:#14b8a6\">{len(strict_quals)}</div><div class=\"stat-label\">Strict (High-Conv.)</div></div>\n"
        f"    <div class=\"stat-card amber\"><div class=\"stat-val\" style=\"color:#f59e0b\">{len(tech_only)}</div><div class=\"stat-label\">Technical Only</div></div>\n"
        f"    <div class=\"stat-card blue\"><div class=\"stat-val\" style=\"color:#3b82f6\">{len(l1_close)}</div><div class=\"stat-label\">Close Calls (7/8)</div></div>\n"
        f"    <div class=\"stat-card green\"><div class=\"stat-val\" style=\"color:#10b981\">{new_count}</div><div class=\"stat-label\">New Today</div></div>\n"
        f"    <div class=\"stat-card red\"><div class=\"stat-val\" style=\"color:#ef4444\">{dropped_count}</div><div class=\"stat-label\">Dropped Today</div></div>\n"
        f"    <div class=\"stat-card {net_card_class}\"><div class=\"stat-val\" style=\"color:{net_color}\">{net_str}</div><div class=\"stat-label\">Net Change</div></div>\n"
        f"    <div class=\"stat-card purple\"><div class=\"stat-val\" style=\"color:#c084fc\">{avg5_str}</div><div class=\"stat-label\">5-Day Average</div></div>\n"
        f"    <div class=\"stat-card gray\"><div class=\"stat-val\" style=\"color:#6b7280;font-size:18px\">{min30}&thinsp;/&thinsp;{max30}</div><div class=\"stat-label\">30-Day Low / High</div></div>\n"
        "  </div>\n"
        f"  <div class=\"obs\">{observation}</div>\n"
        f"  {sector_html}\n"
        "  <div class=\"breadth-strip\">\n"
        f"    <span class=\"breadth-label\">30-Day Breadth &mdash; Full Qualifiers:</span>{breadth_nums}\n"
        "  </div>\n</div>\n\n"

        # FULL QUALIFIERS (SBEM PASS)
        f"<div class=\"section\">\n"
        f"  <div class=\"section-header\"><div class=\"section-title\" style=\"color:#10b981\">Full Qualification Pass Full Qualifiers &mdash; Both Layersmdash; Both Layers ({len(qualifiers)} stocks)</div></div>\n"
        f"  <p class=\"section-desc\">Stocks meeting all 8 Multi-Factor Trend Filter criteria AND minimum fundamental qualification threshold. Tech/Fund sub-scores (0–10). Sorted by Breakout Readiness Score descending.</p>\n"
        f"  <div class=\"tbl-wrap\">{table(qual_html, col_lifecycle=True)}</div>\n</div>\n\n"

        # STRICT MODE QUALIFIERS
        f"<div class=\"section\">\n"
        f"  <div class=\"section-header\"><div class=\"section-title\" style=\"color:#14b8a6\">High-Conviction Qualifiers — Strict Mode ({len(strict_quals)} stocks)</div></div>\n"
        f"  <p class=\"section-desc\">Subset of full qualifiers meeting elevated thresholds: RS Rank &ge;{STRICT_RS_RANK_MIN}, EPS &ge;{STRICT_EPS_GROWTH_MIN:.0f}%, Rev &ge;{STRICT_REV_GROWTH_MIN:.0f}%, &le;{STRICT_PCT_FROM_52WH:.0f}% from 52W high, Fund Score &ge;{STRICT_FUND_SCORE_MIN}/6.</p>\n"
        f"  <div class=\"tbl-wrap\">{table(strict_html)}</div>\n</div>\n\n"

        # NEWLY QUALIFIED
        f"<div class=\"section\">\n"
        f"  <div class=\"section-header\"><div class=\"section-title\" style=\"color:#34d399\">Newly Qualified Today ({new_count} stocks)</div></div>\n"
        f"  <p class=\"section-desc\">Stocks that did not appear in the previous session's full qualifier list but qualify today.</p>\n"
        f"  <div class=\"tbl-wrap\">{table(new_html)}</div>\n</div>\n\n"

        # TECHNICAL ONLY
        f"<div class=\"section\">\n"
        f"  <div class=\"section-header\"><div class=\"section-title\" style=\"color:#f59e0b\">Technical Pass Only ({len(tech_only)} stocks, top 50)</div></div>\n"
        f"  <p class=\"section-desc\">Stocks meeting all 8 Multi-Factor Trend Filter criteria but not yet meeting the fundamental threshold.</p>\n"
        f"  <div class=\"tbl-wrap\">{table(tech_html, col_streak=False)}</div>\n</div>\n\n"

        # CLOSE CALLS
        f"<div class=\"section\">\n"
        f"  <div class=\"section-header\"><div class=\"section-title\" style=\"color:#3b82f6\">Close Calls &mdash; 7/8 Multi-Factor Trend Filter ({len(l1_close)} stocks)</div></div>\n"
        f"  <p class=\"section-desc\">Stocks meeting exactly 7 of 8 Multi-Factor Trend Filter criteria.</p>\n"
        f"  <div class=\"tbl-wrap\">"
        f'<table style="width:100%;border-collapse:collapse;font-size:12px">'
        f'<thead><tr style="background:rgba(255,255,255,0.02)">'
        f'<th style="{TH_S}">Ticker</th><th style="{TH_S}">Status</th>'
        f'</tr></thead><tbody>{close_html}</tbody></table>'
        f"</div>\n</div>\n\n"

        # DROPPED
        f"<div class=\"section\">\n"
        f"  <div class=\"section-header\"><div class=\"section-title\" style=\"color:#ef4444\">Dropped Today &mdash; Lost Full Qualification ({dropped_count} stocks)</div></div>\n"
        f"  <p class=\"section-desc\">Stocks that appeared as full qualifiers in the previous session but no longer meet criteria today.</p>\n"
        f"  <div class=\"tbl-wrap\">"
        f'<table style="width:100%;border-collapse:collapse;font-size:12px">'
        f'<thead><tr style="background:rgba(255,255,255,0.02)">'
        f'<th style="{TH_S}">Ticker</th><th style="{TH_S}">Status</th>'
        f'</tr></thead><tbody>{drop_html}</tbody></table>'
        f"</div>\n</div>\n\n"

        # PERFORMANCE ANALYTICS
        f"<div class=\"section\">\n"
        f"  <div class=\"section-header\"><div class=\"section-title\" style=\"color:#a855f7\">Performance Analytics &mdash; Historical Forward Returns</div></div>\n"
        f"  <p class=\"section-desc\">Average forward returns of stocks at point of full qualification vs {BENCHMARK_TICKER} (Nifty 50). Alpha = return minus index. Based on all historical qualifications in database.</p>\n"
        f"  {perf_html}\n</div>\n\n"

        # METHODOLOGY
        "<div class=\"method-card\">\n"
        "  <div class=\"method-title\">Breakout Readiness Score Methodology (0&ndash;100) | Sub-scores (0&ndash;10 each)</div>\n"
        "  <div class=\"method-body\">\n"
        "    <strong>Technical Score (0&ndash;10 sub, 51 pts towards composite):</strong>\n"
        "    RS Rank quality (20) &bull; Volume character (10) &bull; Price vs 52-week high (15) &bull; MA stacking spread (8) &mdash;\n"
        "    <strong>Fundamental Score (0&ndash;10 sub, 45 pts towards composite):</strong>\n"
        "    EPS quarterly growth (13) &bull; EPS acceleration (12) &bull; Revenue growth (9) &bull; Net margin trend (6) &bull; ROE (5) &mdash;\n"
        "    Normalised to 100. <em style=\"color:#fbbf24\">ACC</em> = accelerating EPS. "
        f"    <strong>Strict Mode:</strong> RS &ge;{STRICT_RS_RANK_MIN}, EPS &ge;{STRICT_EPS_GROWTH_MIN:.0f}%, Rev &ge;{STRICT_REV_GROWTH_MIN:.0f}%, Fund Score &ge;{STRICT_FUND_SCORE_MIN}/6.\n"
        "  </div>\n</div>\n\n"

        # DISCLAIMER
        "<div class=\"disc\">\n"
        f"  <strong>DISCLAIMER:</strong> This report identifies stocks currently meeting structured structured qualification criteria. "
        "  It does not constitute investment advice, a buy or sell recommendation, an entry signal, a price target, "
        "  or any statement about future performance. All screening results are for educational and informational "
        "  purposes only. This is a research tool and not a registered advisory service. "
        "  Past qualification does not predict future price movement. Always conduct independent research "
        "  and consult a qualified financial advisor before making investment decisions. "
        f"  &copy; {date.today().year} SBEM Market Intelligence System {VERSION}\n"
        "</div>\n\n"
        "</div>\n</body>\n</html>"
    )
    return html


# ══════════════════════════════════════════════════════════════════════
#  COMPACT DAILY EMAIL  (redesigned v4.0)
#
#  Structure:
#    §0  Header + Disclaimer tag
#    §1  Executive Summary (regime + 4 stat chips + 1-line interpretation)
#    §2  Focus List (top 8-10 strict qualifiers, ranked)
#    §3  Newly Qualified (compact table)
#    §4  Dropped (compact list)
#    §5  Breadth Snapshot (7-day + 30-day trend)
#    §6  Full list note → CSV attached
#
#  Deliberately omits: tech-only list, close-calls, full 190-stock table.
#  Full detail available via attached HTML report and CSV.
# ══════════════════════════════════════════════════════════════════════
def _build_compact_email_html(
    results, l1_close, dropped_tickers, breadth_history,
    sector_counts, regime, perf_stats
) -> str:
    today_str    = date.today().strftime("%B %d, %Y")
    qualifiers   = [r for r in results if r.qualifies]
    strict_quals = [r for r in qualifiers if r.strict_qualifies]
    new_today    = [r for r in qualifiers if r.change_type == "NEW"]
    new_count    = len(new_today)
    dropped_cnt  = len(dropped_tickers)

    regime_name  = regime.get("regime", "Unknown")
    regime_color = regime.get("regime_color", "#6b7280")
    regime_desc  = regime.get("description", "")[:160]

    # Net change
    prior_full = breadth_history[-2]["full_qualifiers"] if len(breadth_history) >= 2 else None
    if prior_full is not None:
        net_chg = len(qualifiers) - prior_full
        net_str = f"+{net_chg}" if net_chg >= 0 else str(net_chg)
        net_col = "#10b981" if net_chg >= 0 else "#ef4444"
    else:
        net_str, net_col = "--", "#6b7280"

    # Breadth trends
    hist_vals = [r["full_qualifiers"] for r in breadth_history] if breadth_history else []
    trend_7d  = "--"
    trend_30d = "--"
    if len(hist_vals) >= 7:
        d = hist_vals[-1] - hist_vals[-7]
        trend_7d = (f'<span style="color:#10b981">↑ +{d}</span>' if d > 0
                    else (f'<span style="color:#ef4444">↓ {d}</span>' if d < 0
                          else '<span style="color:#6b7280">→ Flat</span>'))
    if len(hist_vals) >= 30:
        d = hist_vals[-1] - hist_vals[-30]
        trend_30d = (f'<span style="color:#10b981">↑ +{d}</span>' if d > 0
                     else (f'<span style="color:#ef4444">↓ {d}</span>' if d < 0
                           else '<span style="color:#6b7280">→ Flat</span>'))

    # 7-day breadth strip
    bstrip = " &nbsp;│&nbsp; ".join(
        f'{r["screen_date"][5:]} → <b>{r["full_qualifiers"]}</b>'
        for r in breadth_history[-7:]
    ) if breadth_history else "--"

    # ── Market interpretation (one line, neutral) ─────────────────────
    _q = len(qualifiers)
    _s = len(strict_quals)
    if _q > 150:
        interp = f"{_q} stocks meeting full qualification criteria — breadth elevated; {_s} meet strict thresholds."
    elif _q > 80:
        interp = f"{_q} stocks qualifying under standard criteria; {_s} pass strict filter."
    elif _q > 30:
        interp = f"Moderate breadth: {_q} standard qualifiers, {_s} strict. Pool contracting from prior peak."
    else:
        interp = f"Narrow breadth: {_q} stocks currently qualifying. Strict pool at {_s}."

    # ── Focus List ────────────────────────────────────────────────────
    focus_list = select_focus_list(results, n=FOCUS_LIST_SIZE)

    def _cap_str(mc):
        if mc >= 1000: return f"&#x20B9;{mc:.0f}B"
        if mc >= 1:   return f"&#x20B9;{mc:.1f}B"
        return f"&#x20B9;{mc*1000:.0f}M"

    def _col(v, good=True):
        if v is None: return "#6b7280"
        return "#10b981" if (v >= 0 if good else v <= 0) else "#ef4444"

    focus_rows = ""
    for rank, (r, fscore) in enumerate(focus_list, 1):
        is_strict = "★ " if r.strict_qualifies else ""
        streak_s  = f"{r.streak}d" if r.streak > 1 else "--"
        eps_s     = f"{r.eps_growth_q:+.0f}%"
        rev_s     = f"{r.rev_growth_q:+.0f}%"
        eps_col   = _col(r.eps_growth_q)
        rev_col   = _col(r.rev_growth_q)
        sector_s  = (r.sector or "--")[:18]
        focus_rows += (
            f'<tr style="border-bottom:1px solid rgba(255,255,255,0.04)">'
            f'<td style="padding:9px 12px;color:#4b5563;font-size:10px;text-align:center">{rank}</td>'
            f'<td style="padding:9px 12px">'
            f'<strong style="color:#f1f5f9;font-size:12px">{is_strict}{r.ticker}</strong><br>'
            f'<span style="color:#4b5563;font-size:9px">{r.company[:22]}</span>'
            f'</td>'
            f'<td style="padding:9px 12px;color:#6b7280;font-size:10px">{sector_s}</td>'
            f'<td style="padding:9px 12px;color:#94a3b8;font-size:11px;text-align:right">{_cap_str(r.market_cap_b)}</td>'
            f'<td style="padding:9px 12px;text-align:center">'
            f'<span style="background:rgba(168,85,247,.15);color:#c084fc;border:1px solid rgba(168,85,247,.3);'
            f'border-radius:6px;padding:2px 7px;font-size:10px;font-weight:700">{r.breakout_score}</span>'
            f'</td>'
            f'<td style="padding:9px 12px;color:#94a3b8;font-size:11px;text-align:center">{fscore:.1f}</td>'
            f'<td style="padding:9px 12px;color:#fbbf24;font-size:11px;text-align:center">{streak_s}</td>'
            f'<td style="padding:9px 12px;font-size:11px;text-align:center;color:{eps_col}">{eps_s}</td>'
            f'<td style="padding:9px 12px;font-size:11px;text-align:center;color:{rev_col}">{rev_s}</td>'
            f'</tr>'
        )

    th_s = "padding:8px 12px;font-size:9px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.07em;text-align:left;border-bottom:1px solid rgba(255,255,255,0.06)"
    focus_table = (
        f'<table style="width:100%;border-collapse:collapse;font-size:11px">'
        f'<thead><tr style="background:rgba(255,255,255,0.02)">'
        f'<th style="{th_s};text-align:center">#</th>'
        f'<th style="{th_s}">Ticker</th>'
        f'<th style="{th_s}">Sector</th>'
        f'<th style="{th_s};text-align:right">Mkt Cap</th>'
        f'<th style="{th_s};text-align:center">BRS</th>'
        f'<th style="{th_s};text-align:center">Focus↑</th>'
        f'<th style="{th_s};text-align:center">Streak</th>'
        f'<th style="{th_s};text-align:center">EPS%</th>'
        f'<th style="{th_s};text-align:center">Rev%</th>'
        f'</tr></thead><tbody>'
        + (focus_rows or f'<tr><td colspan="9" style="padding:16px;text-align:center;color:#374151;font-style:italic">No strict qualifiers available today.</td></tr>')
        + '</tbody></table>'
    )

    # ── Newly Qualified (compact) ─────────────────────────────────────
    new_rows = ""
    for r in new_today[:15]:
        new_rows += (
            f'<tr style="border-bottom:1px solid rgba(255,255,255,0.04)">'
            f'<td style="padding:7px 12px"><strong style="color:#34d399">{r.ticker}</strong></td>'
            f'<td style="padding:7px 12px;color:#6b7280;font-size:10px">{r.sector or "--"}</td>'
            f'<td style="padding:7px 12px;color:#94a3b8;font-size:10px;text-align:right">'
            f'BRS {r.breakout_score} &bull; RS {r.rs_rank}</td>'
            f'</tr>'
        )
    if not new_rows:
        new_rows = '<tr><td colspan="3" style="padding:12px;text-align:center;color:#374151;font-style:italic;font-size:11px">No new qualifiers today.</td></tr>'

    new_table = (
        '<table style="width:100%;border-collapse:collapse;font-size:11px">'
        f'<thead><tr style="background:rgba(255,255,255,0.02)">'
        f'<th style="{th_s}">Ticker</th><th style="{th_s}">Sector</th>'
        f'<th style="{th_s};text-align:right">Scores</th></tr></thead>'
        f'<tbody>{new_rows}</tbody></table>'
    )

    # ── Dropped (compact inline) ──────────────────────────────────────
    if dropped_tickers:
        drop_content = (
            '<div style="font-family:monospace;font-size:11px;color:#ef4444;'
            'background:rgba(239,68,68,.05);border-radius:8px;padding:12px 14px;line-height:1.9">'
            + "  ".join(dropped_tickers)
            + '</div>'
        )
    else:
        drop_content = '<p style="font-size:11px;color:#374151;font-style:italic;margin:0">No stocks dropped qualification today.</p>'

    # ── Sector snapshot (top 5) ───────────────────────────────────────
    sector_snippet = ""
    if sector_counts:
        secs   = sorted(sector_counts.items(), key=lambda x: -x[1])[:5]
        max_n  = secs[0][1] if secs else 1
        total  = sum(sector_counts.values())
        for s, n in secs:
            if not s: continue
            bar_w = min(int(n / max_n * 180), 180)
            pct   = int(n / total * 100)
            sector_snippet += (
                f'<tr>'
                f'<td style="padding:4px 0;font-size:10px;color:#6b7280;width:130px">{s}</td>'
                f'<td style="padding:4px 8px">'
                f'<div style="background:rgba(255,255,255,0.04);border-radius:3px;height:5px;width:180px">'
                f'<div style="height:5px;border-radius:3px;background:linear-gradient(90deg,#a855f7,#6366f1);width:{bar_w}px"></div>'
                f'</div></td>'
                f'<td style="padding:4px 0 4px 8px;font-size:10px;color:#e2e8f0;font-weight:700">{n}</td>'
                f'<td style="padding:4px 0 4px 6px;font-size:9px;color:#4b5563">({pct}%)</td>'
                f'</tr>'
            )
        sector_snippet = (
            '<div style="margin-top:4px">'
            '<div style="font-size:9px;color:#4b5563;font-weight:700;text-transform:uppercase;'
            'letter-spacing:.07em;margin-bottom:8px">Top Sectors (Full Qualifiers)</div>'
            f'<table style="border-collapse:collapse">{sector_snippet}</table></div>'
        )

    # ── Section header helper ─────────────────────────────────────────
    def _sh(title, color, n=None):
        count = f'<span style="color:{color};font-size:10px;font-weight:700;margin-left:8px">({n})</span>' if n is not None else ""
        return (
            f'<div style="margin-top:28px;margin-bottom:10px">'
            f'<span style="font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:{color}">{title}</span>'
            + count +
            f'<hr style="border:none;border-top:1px solid rgba(255,255,255,0.05);margin:4px 0 0 0"/></div>'
        )

    def _card(val, label, color):
        return (
            f'<td style="padding:0 5px 10px 5px">'
            f'<div style="background:#0f1117;border:1px solid rgba(255,255,255,0.07);'
            f'border-radius:8px;padding:12px 14px;min-width:72px;text-align:center">'
            f'<div style="font-size:20px;font-weight:800;color:{color};letter-spacing:-1px;line-height:1">{val}</div>'
            f'<div style="font-size:9px;color:#4b5563;margin-top:3px;text-transform:uppercase;'
            f'letter-spacing:.05em;font-weight:500">{label}</div>'
            f'</div></td>'
        )

    # ── Assemble HTML ─────────────────────────────────────────────────
    html = (
        "<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"UTF-8\"/>"
        f"<title>{REPORT_NAME} Daily &mdash; {today_str}</title></head>\n"
        "<body style=\"margin:0;padding:0;background:#09090f;"
        "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e2e8f0\">\n"
        "<div style=\"max-width:800px;margin:0 auto;padding:28px 20px\">\n\n"

        # ── Header ────────────────────────────────────────────────────
        "<table style=\"width:100%;border-collapse:collapse;margin-bottom:24px\">"
        "<tr><td style=\"padding-bottom:18px;border-bottom:1px solid rgba(255,255,255,0.06)\">"
        "<span style=\"font-size:17px;font-weight:800;color:#fff;letter-spacing:-.5px\">"
        "<span style=\"font-weight:400;color:#a855f7\">ALPHA</span>dominico</span>"
        f"<span style=\"font-size:10px;color:#374151;margin-left:10px\">{VERSION} &bull; {MARKET_NAME}</span>"
        "</td><td style=\"text-align:right;padding-bottom:18px;border-bottom:1px solid rgba(255,255,255,0.06)\">"
        "<span style=\"background:rgba(239,68,68,.12);color:#f87171;border:1px solid rgba(239,68,68,.25);"
        "border-radius:20px;padding:4px 12px;font-size:9px;font-weight:700;letter-spacing:.06em\">"
        "SCREENING TOOL ONLY &mdash; NOT INVESTMENT ADVICE</span></td></tr></table>\n\n"

        # Title
        f"<div style=\"font-size:20px;font-weight:800;color:#fff;letter-spacing:-.5px;margin-bottom:4px\">"
        f"{MARKET_NAME} Daily Report &mdash; {today_str}</div>"
        f"<div style=\"font-size:10px;color:#4b5563;margin-bottom:20px\">"
        f"Multi-Factor Trend Filter + Earnings Strength Overlay + Market Condition Engine &nbsp;&bull;&nbsp; "
        f"Generated {datetime.now().strftime('%H:%M:%S')}</div>\n\n"

        # ── §1 Executive Summary ──────────────────────────────────────
        + _sh("Executive Summary", "#a855f7")

        # Regime banner
        + f"<div style=\"background:rgba(255,255,255,0.02);border-left:3px solid {regime_color};"
        f"border-radius:0 8px 8px 0;padding:10px 14px;margin-bottom:16px\">"
        f"<div style=\"font-size:10px;font-weight:700;color:{regime_color};text-transform:uppercase;"
        f"letter-spacing:.05em\">Market Regime: {regime_name}</div>"
        f"<div style=\"font-size:10px;color:#4b5563;margin-top:2px\">{regime_desc}</div>"
        f"</div>\n\n"

        # Stat chips
        "<table style=\"border-collapse:collapse;margin-bottom:14px\"><tr>"
        + _card(len(qualifiers), "Standard", "#10b981")
        + _card(len(strict_quals), "Strict", "#14b8a6")
        + _card(new_count, "New Today", "#34d399")
        + _card(dropped_cnt, "Dropped", "#ef4444")
        + _card(net_str, "Net Chg", net_col)
        + "</tr></table>\n\n"

        # Interpretation line
        f"<div style=\"background:#0f1117;border-left:3px solid #a855f7;border-radius:0 8px 8px 0;"
        f"padding:10px 14px;font-size:11px;color:#94a3b8;margin-bottom:4px\">{interp}</div>\n\n"

        # ── §2 Focus List ─────────────────────────────────────────────
        + _sh(f"Today's Focus List", "#14b8a6", len(focus_list))
        + "<div style=\"font-size:10px;color:#4b5563;margin-bottom:10px\">"
        "Top-ranked strict qualifiers selected by Focus Score = 30% BRS composite + 25% RS Rank + "
        "20% technical proximity + 15% streak maturity + 10% market cap. "
        "★ = Strict qualifier. Full list attached as CSV."
        "</div>"
        f"<div style=\"background:#0f1117;border:1px solid rgba(255,255,255,0.06);border-radius:10px;overflow:hidden\">"
        + focus_table
        + "</div>\n\n"

        # ── §3 Newly Qualified ────────────────────────────────────────
        + _sh("Newly Qualified Today", "#34d399", new_count)
        + f"<div style=\"background:#0f1117;border:1px solid rgba(255,255,255,0.06);border-radius:10px;overflow:hidden\">"
        + new_table
        + "</div>\n\n"

        # ── §4 Dropped ────────────────────────────────────────────────
        + _sh("Dropped Today", "#ef4444", dropped_cnt)
        + drop_content + "\n\n"

        # ── §5 Breadth Snapshot ───────────────────────────────────────
        + _sh("Breadth Snapshot", "#3b82f6")
        + f"<div style=\"background:#0f1117;border:1px solid rgba(255,255,255,0.06);"
        f"border-radius:10px;padding:14px 16px;font-size:10px\">"
        f"<table style=\"border-collapse:collapse;width:100%;margin-bottom:12px\">"
        f"<tr>"
        f"<td style=\"padding:4px 20px 4px 0;color:#4b5563\">7-Day Trend</td>"
        f"<td style=\"padding:4px 0;font-weight:600\">{trend_7d}</td>"
        f"<td style=\"padding:4px 20px 4px 40px;color:#4b5563\">30-Day Trend</td>"
        f"<td style=\"padding:4px 0;font-weight:600\">{trend_30d}</td>"
        f"</tr></table>"
        f"<div style=\"font-family:monospace;font-size:10px;color:#374151;line-height:1.9\">"
        f"<span style=\"color:rgba(168,85,247,.5)\">7-Day Qualifier Count:</span> {bstrip}"
        f"</div>"
        + sector_snippet
        + "</div>\n\n"

        # ── §6 Full List Note ─────────────────────────────────────────
        + _sh("Full Qualifier List", "#6b7280")
        + f"<div style=\"background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.05);"
        f"border-radius:8px;padding:12px 14px;font-size:10px;color:#4b5563;line-height:1.8\">"
        f"The complete list of <strong style=\"color:#94a3b8\">{len(qualifiers)} standard</strong> and "
        f"<strong style=\"color:#94a3b8\">{len(strict_quals)} strict</strong> qualifiers is attached "
        f"as a CSV file (<code style=\"color:#a855f7\">sbem_full_list_{date.today().strftime('%Y%m%d')}.csv</code>). "
        f"The full interactive HTML report is also attached for desktop viewing."
        f"</div>\n\n"

        # ── Disclaimer ────────────────────────────────────────────────
        "<div style=\"color:#2d3748;font-size:9px;border-top:1px solid rgba(255,255,255,0.04);"
        "padding-top:14px;margin-top:20px;line-height:1.9\">"
        "<strong style=\"color:#374151\">DISCLAIMER:</strong> This report identifies stocks currently "
        "meeting structured structured qualification criteria. It does not constitute investment advice, "
        "a buy or sell recommendation, an entry signal, a price target, or a statement about future "
        f"performance. Research tool only. &copy; {date.today().year} SBEM Market Intelligence {VERSION}"
        "</div>\n\n"

        "</div></body></html>"
    )
    return html


# ══════════════════════════════════════════════════════════════════════
#  WEEKLY REPORT EMAIL  (analytical, statistical)
#
#  Structure:
#    §1  Weekly Breadth Trend (7-day table)
#    §2  Sector Concentration Breakdown
#    §3  Performance Analytics (4W median return, failure rate, beat rate)
#    §4  Strict vs Standard Comparison
#    §5  Regime Shift Detection
#    §6  Statistical Summary
# ══════════════════════════════════════════════════════════════════════
def _build_weekly_report_html(weekly_stats: dict) -> str:
    today_str   = date.today().strftime("%B %d, %Y")
    week_num    = date.today().isocalendar()[1]

    breadth_7d        = weekly_stats.get("breadth_7d", [])
    sector_7d         = weekly_stats.get("sector_7d", [])
    perf              = weekly_stats.get("perf", {})
    svs               = weekly_stats.get("strict_vs_standard", {})
    regime_shift      = weekly_stats.get("regime_shift", [])
    regimes_7d        = weekly_stats.get("regimes_7d", [])

    def _sh(title, color):
        return (
            f'<div style="margin-top:28px;margin-bottom:10px">'
            f'<span style="font-size:10px;font-weight:700;letter-spacing:.1em;'
            f'text-transform:uppercase;color:{color}">{title}</span>'
            f'<hr style="border:none;border-top:1px solid rgba(255,255,255,0.05);margin:4px 0 0 0"/></div>'
        )

    def _pct_fmt(v, prec=1):
        if v is None: return '<span style="color:#374151">—</span>'
        col  = "#10b981" if v >= 0 else "#ef4444"
        sign = "+" if v >= 0 else ""
        return f'<span style="color:{col};font-weight:600">{sign}{v:.{prec}f}%</span>'

    # ── §1 Weekly Breadth Trend ───────────────────────────────────────
    breadth_rows = ""
    for r in breadth_7d:
        regime_s = r.get("regime") or "--"
        rc = "#10b981" if "Expansion" in regime_s else ("#ef4444" if "Contraction" in regime_s else "#f59e0b")
        new_s  = f'+{r["new_qualifiers"]}' if r.get("new_qualifiers") else "--"
        drop_s = f'-{r["dropped_qualifiers"]}' if r.get("dropped_qualifiers") else "--"
        breadth_rows += (
            f'<tr style="border-bottom:1px solid rgba(255,255,255,0.04)">'
            f'<td style="padding:8px 12px;font-size:11px;color:#94a3b8;font-family:monospace">{r["screen_date"]}</td>'
            f'<td style="padding:8px 12px;font-size:12px;font-weight:700;color:#e2e8f0;text-align:center">{r["full_qualifiers"]}</td>'
            f'<td style="padding:8px 12px;font-size:11px;color:#2dd4bf;text-align:center">{r.get("strict_qualifiers","--")}</td>'
            f'<td style="padding:8px 12px;font-size:11px;color:#34d399;text-align:center">{new_s}</td>'
            f'<td style="padding:8px 12px;font-size:11px;color:#f87171;text-align:center">{drop_s}</td>'
            f'<td style="padding:8px 12px;font-size:10px;color:{rc}">{regime_s}</td>'
            f'</tr>'
        )
    th_s = "padding:8px 12px;font-size:9px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.07em;border-bottom:1px solid rgba(255,255,255,0.06)"
    breadth_table = (
        '<table style="width:100%;border-collapse:collapse;font-size:11px">'
        f'<thead><tr style="background:rgba(255,255,255,0.02)">'
        f'<th style="{th_s}">Date</th>'
        f'<th style="{th_s};text-align:center">Full</th>'
        f'<th style="{th_s};text-align:center">Strict</th>'
        f'<th style="{th_s};text-align:center">New</th>'
        f'<th style="{th_s};text-align:center">Dropped</th>'
        f'<th style="{th_s}">Regime</th>'
        f'</tr></thead><tbody>'
        + (breadth_rows or '<tr><td colspan="6" style="padding:14px;text-align:center;color:#374151;font-style:italic">No weekly data available.</td></tr>')
        + '</tbody></table>'
    )

    # ── §2 Sector Breakdown ───────────────────────────────────────────
    sector_rows = ""
    max_n = sector_7d[0]["cnt"] if sector_7d else 1
    for row in sector_7d:
        bar_w = min(int(row["cnt"] / max_n * 200), 200)
        sector_rows += (
            f'<tr style="border-bottom:1px solid rgba(255,255,255,0.03)">'
            f'<td style="padding:6px 12px;font-size:10px;color:#6b7280;width:160px">{row["sector"]}</td>'
            f'<td style="padding:6px 12px">'
            f'<div style="background:rgba(255,255,255,0.04);border-radius:3px;height:5px;width:200px">'
            f'<div style="height:5px;border-radius:3px;background:linear-gradient(90deg,#a855f7,#6366f1);width:{bar_w}px"></div>'
            f'</div></td>'
            f'<td style="padding:6px 12px;font-size:11px;color:#e2e8f0;font-weight:700;text-align:right">{row["cnt"]}</td>'
            f'</tr>'
        )
    sector_table = (
        '<table style="width:100%;border-collapse:collapse">'
        + (sector_rows or '<tr><td colspan="3" style="padding:12px;text-align:center;color:#374151;font-style:italic;font-size:11px">No sector data available.</td></tr>')
        + '</table>'
    )

    # ── §3 Performance Analytics ──────────────────────────────────────
    n_4w       = perf.get("n_4w") or 0
    avg_4w     = perf.get("avg_4w")
    avg_12w    = perf.get("avg_12w")
    alpha_4w   = perf.get("avg_alpha_4w")
    win_4w     = perf.get("win_rate_4w")
    fail_rate  = perf.get("failure_rate")
    beat_rate  = perf.get("beat_rate")

    def _pct_or_na(v, label):
        return (f'<td style="padding:10px 16px;text-align:center">'
                f'<div style="font-size:16px;font-weight:800">{_pct_fmt(v)}</div>'
                f'<div style="font-size:9px;color:#4b5563;margin-top:3px;text-transform:uppercase;letter-spacing:.05em">{label}</div>'
                f'</td>')

    perf_html = (
        f'<table style="border-collapse:collapse;margin-bottom:8px"><tr>'
        + _pct_or_na(avg_4w, f"Avg 4W Return (n={n_4w})")
        + _pct_or_na(avg_12w, "Avg 12W Return")
        + _pct_or_na(alpha_4w, "Avg 4W Alpha")
        + _pct_or_na(win_4w * 100 if win_4w else None, "Win Rate (4W)")
        + _pct_or_na(fail_rate * 100 if fail_rate else None, "Failure Rate (≤−10%)")
        + _pct_or_na(beat_rate * 100 if beat_rate else None, "Beat Benchmark Rate")
        + '</tr></table>'
        + f'<div style="font-size:9px;color:#4b5563;margin-top:4px">'
        f'Failure Rate = % of qualified stocks with 4W return ≤ −10%. '
        f'Beat Benchmark = % with alpha &gt; 0 over 4W. Historical data only — not predictive.</div>'
    )

    # ── §4 Strict vs Standard ─────────────────────────────────────────
    strict_avg  = svs.get("strict_avg_4w")
    stand_avg   = svs.get("standard_avg_4w")
    strict_n    = svs.get("strict_n") or 0
    stand_n     = svs.get("standard_n") or 0
    diff        = (strict_avg - stand_avg) if (strict_avg is not None and stand_avg is not None) else None

    svs_html = (
        f'<table style="border-collapse:collapse"><tr>'
        + _pct_or_na(strict_avg, f"Strict Avg 4W (n={strict_n})")
        + _pct_or_na(stand_avg, f"Standard Avg 4W (n={stand_n})")
        + _pct_or_na(diff, "Strict Advantage")
        + '</tr></table>'
    )

    # ── §5 Regime Shift ───────────────────────────────────────────────
    if regime_shift:
        shift_note = (
            f'<div style="background:rgba(245,158,11,.08);border:1px solid rgba(245,158,11,.2);'
            f'border-radius:8px;padding:12px 14px;font-size:10px;color:#fbbf24;line-height:1.8">'
            f'<strong>Regime shift detected this week:</strong> '
            + " → ".join(reversed(regime_shift))
            + '</div>'
        )
    else:
        shift_note = (
            '<div style="font-size:10px;color:#374151;font-style:italic">No regime transition detected this week. Market classification remained stable.</div>'
        )

    regime_detail = ""
    for r in regimes_7d:
        rd = r.get("regime") or "--"
        rc = "#10b981" if "Expansion" in rd else ("#ef4444" if "Contraction" in rd else "#f59e0b")
        regime_detail += (
            f'<span style="font-size:9px;color:{rc};font-family:monospace;margin-right:12px">'
            f'{r["screen_date"]}: {rd}</span><br>'
        )

    # ── Assemble HTML ─────────────────────────────────────────────────
    html = (
        "<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"UTF-8\"/>"
        f"<title>{REPORT_NAME} Weekly &mdash; Week {week_num}</title></head>\n"
        "<body style=\"margin:0;padding:0;background:#09090f;"
        "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e2e8f0\">\n"
        "<div style=\"max-width:800px;margin:0 auto;padding:28px 20px\">\n\n"

        # Header
        "<table style=\"width:100%;border-collapse:collapse;margin-bottom:24px\">"
        "<tr><td style=\"padding-bottom:16px;border-bottom:1px solid rgba(255,255,255,0.06)\">"
        "<span style=\"font-size:17px;font-weight:800;color:#fff;letter-spacing:-.5px\">"
        "<span style=\"font-weight:400;color:#a855f7\">ALPHA</span>dominico</span>"
        f"<span style=\"font-size:10px;color:#374151;margin-left:10px\">Weekly Analysis &mdash; Week {week_num}</span>"
        "<span style=\"background:rgba(239,68,68,.12);color:#f87171;border:1px solid rgba(239,68,68,.25);"
        "border-radius:20px;padding:4px 12px;font-size:9px;font-weight:700;letter-spacing:.06em\">"
        "RESEARCH REPORT &mdash; NOT ADVISORY</span></td></tr></table>\n\n"

        f"<div style=\"font-size:20px;font-weight:800;color:#fff;letter-spacing:-.5px;margin-bottom:4px\">"
        f"Weekly Intelligence Summary &mdash; {today_str}</div>"
        f"<div style=\"font-size:10px;color:#4b5563;margin-bottom:20px\">"
        f"Structured qualification breadth, sector analysis, and historical performance statistics. "
        f"All return figures are historical. Not a prediction of future results.</div>\n\n"

        # §1
        + _sh("Weekly Breadth Trend (Last 7 Sessions)", "#a855f7")
        + f"<div style=\"background:#0f1117;border:1px solid rgba(255,255,255,0.06);border-radius:10px;overflow:hidden\">"
        + breadth_table + "</div>\n\n"

        # §2
        + _sh("Sector Concentration (7-Day, Full Qualifiers)", "#3b82f6")
        + f"<div style=\"background:#0f1117;border:1px solid rgba(255,255,255,0.06);border-radius:10px;padding:14px 0\">"
        + sector_table + "</div>\n\n"

        # §3
        + _sh("Historical Forward Return Analytics", "#10b981")
        + "<div style=\"font-size:10px;color:#4b5563;margin-bottom:10px\">"
        "Aggregate returns measured from point of full qualification. "
        "Alpha = stock return minus Nifty 50 index return over same period. "
        "All figures drawn from stored qualification history — sample sizes may be small early in operation."
        "</div>"
        + f"<div style=\"background:#0f1117;border:1px solid rgba(255,255,255,0.06);border-radius:10px;padding:14px\">"
        + perf_html + "</div>\n\n"

        # §4
        + _sh("Strict vs Standard Qualifier Comparison (4W Returns)", "#14b8a6")
        + "<div style=\"font-size:10px;color:#4b5563;margin-bottom:10px\">"
        "Comparison of historical 4-week returns between stocks that met strict criteria versus standard criteria only."
        "</div>"
        + f"<div style=\"background:#0f1117;border:1px solid rgba(255,255,255,0.06);border-radius:10px;padding:14px\">"
        + svs_html + "</div>\n\n"

        # §5
        + _sh("Market Regime Status (This Week)", "#f59e0b")
        + shift_note
        + f"<div style=\"margin-top:10px;line-height:1.6\">{regime_detail}</div>\n\n"

        # Disclaimer
        "<div style=\"color:#2d3748;font-size:9px;border-top:1px solid rgba(255,255,255,0.04);"
        "padding-top:14px;margin-top:24px;line-height:1.9\">"
        "<strong style=\"color:#374151\">DISCLAIMER:</strong> This weekly report summarises "
        "Structured qualification breadth and historical performance statistics. It does not "
        "constitute investment advice, a buy or sell recommendation, or a signal of any kind. "
        "Historical returns do not guarantee future results. Research tool only. "
        f"&copy; {date.today().year} SBEM Market Intelligence {VERSION}"
        "</div>\n\n"

        "</div></body></html>"
    )
    return html


def generate_html_report(results, l1_close, dropped_tickers, breadth_history,
                          sector_counts, regime, perf_stats):
    html = _build_html_content(results, l1_close, dropped_tickers, breadth_history,
                                sector_counts, regime, perf_stats)
    path = os.path.join(OUTPUT_DIR, f"sbem_report_{date.today().strftime('%Y%m%d')}.html")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html)
    log.info(f"[OK] HTML report saved: {path}")
    return path


def generate_pdf_report(results, l1_close, dropped_tickers, breadth_history,
                         sector_counts, regime, perf_stats):
    """Generate a PDF via wkhtmltopdf and return the PDF path (or None on failure)."""
    pdf_path = os.path.join(OUTPUT_DIR, f"sbem_report_{date.today().strftime('%Y%m%d')}.pdf")
    html_for_pdf = _build_html_content(results, l1_close, dropped_tickers,
                                        breadth_history, sector_counts, regime, perf_stats)
    tmp_html = os.path.join(OUTPUT_DIR, "_sbem_pdf_tmp.html")
    with open(tmp_html, "w", encoding="utf-8") as fh:
        fh.write(html_for_pdf)
    try:
        cmd = [
            "wkhtmltopdf",
            "--enable-local-file-access",
            "--page-size", "A4",
            "--orientation", "Landscape",
            "--margin-top",    "10mm",
            "--margin-bottom", "10mm",
            "--margin-left",   "10mm",
            "--margin-right",  "10mm",
            "--print-media-type",
            "--no-stop-slow-scripts",
            "--javascript-delay", "500",
            "--zoom", "0.85",
            "--encoding", "utf-8",
            "--quiet",
            tmp_html,
            pdf_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0 and os.path.exists(pdf_path):
            log.info(f"[OK] PDF report saved: {pdf_path}")
        else:
            log.warning(f"wkhtmltopdf error (rc={result.returncode}): {result.stderr[:300]}")
            pdf_path = None
    except Exception as e:
        log.warning(f"PDF generation failed: {e}")
        pdf_path = None
    finally:
        try:
            os.remove(tmp_html)
        except Exception:
            pass
    return pdf_path


# ══════════════════════════════════════════════════════════════════════
#  TELEGRAM REPORT  (HTML parse mode for formatting)
# ══════════════════════════════════════════════════════════════════════
def send_telegram(
    results, l1_close, dropped_tickers, breadth_history,
    sector_counts, regime, perf_stats,
) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured -- skipping")
        return

    qualifiers    = [r for r in results if r.qualifies]
    strict_quals  = [r for r in qualifiers if r.strict_qualifies]
    tech_only     = [r for r in results if not r.qualifies]
    new_today     = [r for r in qualifiers if r.change_type == "NEW"]
    dropped_count = len(dropped_tickers)

    avg5_str = "--"
    if len(breadth_history) >= 5:
        avg5_str = f"{sum(r['full_qualifiers'] for r in breadth_history[-5:])/5:.1f}"

    regime_name = regime.get("regime", "Unknown")

    def _h(text): return f"<b>{text}</b>"
    def _pct(v):
        if v is None: return "--"
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:.0f}%"

    # ── Part 1: Header + Regime + Summary ────────────────────────────
    lines_p1 = [
        f"{_h('ALPHAdominico')} | {MARKET_NAME} | {date.today().strftime('%b %d, %Y')} | {VERSION}",
        "<i>Screening Tool Only. Not Investment Advice.</i>",
        "",
        f"<b>Market Regime:</b> {regime_name}",
        f"<code>{regime.get('description','')[:200]}</code>",
        "",
        f"{_h('Market Structure')}",
        f"Full Qualifiers        : <b>{len(qualifiers)}</b>",
        f"Strict (High-Conv.)  : <b>{len(strict_quals)}</b>",
        f"Technical Only       : {len(tech_only)}",
        f"Close Calls (7/8 MFT) : {len(l1_close)}",
        f"New Today            : <b>{len(new_today)}</b>",
        f"Dropped Today        : {dropped_count}",
        f"5-Day Average        : {avg5_str}",
    ]

    if sector_counts:
        lines_p1.append("")
        lines_p1.append(f"{_h('Sector Distribution')}")
        for sec, cnt in sorted(sector_counts.items(), key=lambda x: -x[1])[:6]:
            lines_p1.append(f"  {sec[:22]:<22} {cnt}")

    msg_p1 = "\n".join(lines_p1)

    # ── Part 2: Full Qualifiers list ────────────────────────────
    lines_p2 = [f"{_h(f'Full Qualifiers ({len(qualifiers)})')}",
                "<code>Ticker    BRS  RS   EPS%   Rev%  Strict</code>"]
    for r in qualifiers[:25]:
        acc   = "+" if r.eps_accelerating else " "
        strk  = f"{r.streak}d" if r.streak > 1 else "--"
        s_tag = "[S]" if r.strict_qualifies else "   "
        lines_p2.append(
            f"<code>{r.ticker:<7} {r.breakout_score:>4}  "
            f"{r.rs_rank:>3}  {r.eps_growth_q:>+5.0f}%  "
            f"{r.rev_growth_q:>+5.0f}%  {strk} {acc}{s_tag}</code>"
        )
    if len(qualifiers) > 25:
        lines_p2.append(f"<i>... and {len(qualifiers)-25} more — see email report</i>")

    if new_today:
        lines_p2.append("")
        lines_p2.append(f"{_h(f'NEW Today ({len(new_today)})')}")
        lines_p2.append("  " + "  ".join(r.ticker for r in new_today))

    if dropped_tickers:
        lines_p2.append("")
        lines_p2.append(f"{_h(f'Dropped ({dropped_count})')}")
        lines_p2.append("  " + "  ".join(dropped_tickers))

    if strict_quals:
        lines_p2.append("")
        lines_p2.append(f"{_h(f'High-Conviction / Strict ({len(strict_quals)})')}")
        lines_p2.append("  " + "  ".join(r.ticker for r in strict_quals))

    if l1_close:
        lines_p2.append("")
        lines_p2.append(f"{_h(f'Close Calls 7/8 ({len(l1_close)})')}")
        lines_p2.append("  " + "  ".join(sym for sym, _, _ in l1_close[:20]))

    # ── Part 3: Performance Analytics ────────────────────────────────
    lines_p3 = [f"{_h('Performance Analytics')}",
                f"<i>Forward returns from point of qualification vs {BENCHMARK_TICKER} (Nifty 50)</i>"]
    for period, label in [("1w","1 Week"),("4w","4 Week"),("12w","12 Week")]:
        if period in perf_stats:
            p = perf_stats[period]
            n = p.get("n",0)
            avg = p.get("avg_ret")
            alpha = p.get("avg_alpha")
            wr = p.get("win_rate")
            lines_p3.append(
                f"<b>{label}</b> (n={n}): avg {_pct(avg)} | alpha {_pct(alpha)} | win {_pct(wr*100 if wr else None)}"
            )
        else:
            lines_p3.append(f"<b>{label}</b>: Insufficient data")

    lines_p3 += [
        "",
        "<i>Full HTML + PDF report sent via email.</i>",
        "<i>Not investment advice. Educational purposes only.</i>",
    ]

    msg_p2 = "\n".join(lines_p2)
    msg_p3 = "\n".join(lines_p3)

    def _send(text):
        """Send one Telegram message (HTML mode, max 4096 chars)."""
        if len(text) > 4090:
            text = text[:4050] + "\n<i>[truncated — see email for full report]</i>"
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id":    TELEGRAM_CHAT_ID,
                    "text":       text,
                    "parse_mode": "HTML",
                },
                timeout=15,
            )
            if r.ok:
                log.info("[OK] Telegram message sent")
            else:
                log.warning(f"Telegram fail: {r.text[:200]}")
        except Exception as e:
            log.warning(f"Telegram error: {e}")

    _send(msg_p1)
    time.sleep(1)
    _send(msg_p2)
    time.sleep(1)
    if perf_stats:
        _send(msg_p3)


# ══════════════════════════════════════════════════════════════════════
#  EMAIL  — Compact Daily (redesigned v4.0)
#
#  Sends the new compact daily email body + attachments:
#    • sbem_full_list_YYYYMMDD.csv   — complete qualifier list
#    • sbem_report_YYYYMMDD.html     — full interactive HTML report
#    • sbem_report_YYYYMMDD.pdf      — PDF (if wkhtmltopdf available)
#
#  The email BODY is intentionally concise (~50% shorter than v3).
# ══════════════════════════════════════════════════════════════════════
def _attach_file(msg, path: str, mime_type: tuple, label: str) -> None:
    """Helper: attach any file to a MIMEMultipart message."""
    try:
        with open(path, "rb") as fh:
            data = fh.read()
        part = MIMEBase(*mime_type)
        part.set_payload(data)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment",
                         filename=os.path.basename(path))
        msg.attach(part)
        log.info(f"[OK] Attached {label}: {os.path.basename(path)} ({len(data)//1024} KB)")
    except Exception as e:
        log.warning(f"Attachment failed ({label}): {e}")


def send_email(
    results, l1_close, dropped_tickers, breadth_history,
    sector_counts, regime, perf_stats, html_path, pdf_path=None,
):
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_TO:
        log.warning("Email not configured -- skipping")
        return

    qualifiers   = [r for r in results if r.qualifies]
    strict_quals = [r for r in qualifiers if r.strict_qualifies]
    today_str    = date.today().strftime("%B %d, %Y")
    new_count    = sum(1 for r in qualifiers if r.change_type == "NEW")
    dropped_cnt  = len(dropped_tickers)
    regime_name  = regime.get("regime", "Unknown")

    # Build compact email body
    email_html = _build_compact_email_html(
        results, l1_close, dropped_tickers, breadth_history,
        sector_counts, regime, perf_stats
    )

    # Generate CSV full list
    csv_path = generate_csv_full_list(results)

    # Subject line
    subject = (
        f"{REPORT_NAME} | {MARKET_NAME} | {today_str} | "
        f"Regime: {regime_name} | {len(qualifiers)} Std | {len(strict_quals)} Strict | {new_count} New"
    )
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = EMAIL_TO

    body_part = MIMEMultipart("alternative")
    body_part.attach(MIMEText(email_html, "html", "utf-8"))
    msg.attach(body_part)

    # Attach CSV (full list)
    if csv_path and os.path.exists(csv_path):
        _attach_file(msg, csv_path, ("text", "csv"), "CSV full list")

    # Attach HTML report
    if html_path and os.path.exists(html_path):
        _attach_file(msg, html_path, ("text", "html"), "HTML report")

    # Attach PDF
    if pdf_path and os.path.exists(pdf_path):
        _attach_file(msg, pdf_path, ("application", "pdf"), "PDF report")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
            srv.login(EMAIL_SENDER, EMAIL_PASSWORD)
            srv.sendmail(EMAIL_SENDER, EMAIL_TO, msg.as_string())
        log.info(f"[OK] Daily email sent (compact) with CSV + HTML attachments")
    except Exception as e:
        log.warning(f"Email error: {e}")


# ══════════════════════════════════════════════════════════════════════
#  WEEKLY EMAIL  (Friday analytical report)
# ══════════════════════════════════════════════════════════════════════
def send_weekly_email() -> None:
    """
    Build and send the weekly analytical report.
    Intended to run on Fridays (auto-detected in __main__).
    Can also be forced with env var FORCE_WEEKLY=1.
    """
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_TO:
        log.warning("Email not configured -- skipping weekly report")
        return

    log.info("Building weekly intelligence report...")
    conn = get_db()
    weekly_stats = db_get_weekly_stats(conn)
    conn.close()

    report_html = _build_weekly_report_html(weekly_stats)
    week_num    = date.today().isocalendar()[1]
    today_str   = date.today().strftime("%B %d, %Y")

    # Save weekly HTML
    weekly_path = os.path.join(OUTPUT_DIR,
                               f"sbem_weekly_w{week_num}_{date.today().strftime('%Y%m%d')}.html")
    with open(weekly_path, "w", encoding="utf-8") as fh:
        fh.write(report_html)
    log.info(f"[OK] Weekly report HTML saved: {weekly_path}")

    subject = (
        f"{REPORT_NAME} | Weekly Intelligence | Week {week_num} | {today_str}"
    )
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = EMAIL_TO

    body_part = MIMEMultipart("alternative")
    body_part.attach(MIMEText(report_html, "html", "utf-8"))
    msg.attach(body_part)

    if os.path.exists(weekly_path):
        _attach_file(msg, weekly_path, ("text", "html"), "Weekly HTML report")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
            srv.login(EMAIL_SENDER, EMAIL_PASSWORD)
            srv.sendmail(EMAIL_SENDER, EMAIL_TO, msg.as_string())
        log.info("[OK] Weekly email sent")
    except Exception as e:
        log.warning(f"Weekly email error: {e}")


# ══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    try:
        # ── Credential pre-flight check ───────────────────────────────
        log.info("="*68)
        log.info("  Delivery Configuration Check")
        log.info("="*68)
        log.info(f"  Email sender    : {'[OK] ' + EMAIL_SENDER if EMAIL_SENDER else '[MISSING] Set EMAIL_SENDER in .env'}")
        log.info(f"  Email password  : {'[OK] set' if EMAIL_PASSWORD else '[MISSING] Set EMAIL_PASSWORD in .env'}")
        log.info(f"  Email recipient : {'[OK] ' + EMAIL_TO if EMAIL_TO else '[MISSING] Set EMAIL_TO in .env'}")
        log.info(f"  Telegram token  : {'[OK] set' if TELEGRAM_BOT_TOKEN else '[MISSING] Set TELEGRAM_BOT_TOKEN in .env'}")
        log.info(f"  Telegram chat   : {'[OK] ' + TELEGRAM_CHAT_ID if TELEGRAM_CHAT_ID else '[MISSING] Set TELEGRAM_CHAT_ID in .env'}")
        log.info("="*68)

        # ── Main screening ────────────────────────────────────────────
        (results, l1_close, dropped_tickers, breadth_history,
         sector_counts, regime, perf_stats) = run_screening()

        # ── Full HTML + PDF reports (unchanged — attached to email) ───
        html_path = generate_html_report(
            results, l1_close, dropped_tickers, breadth_history,
            sector_counts, regime, perf_stats
        )
        pdf_path = generate_pdf_report(
            results, l1_close, dropped_tickers, breadth_history,
            sector_counts, regime, perf_stats
        )

        # ── Telegram (compact summary) ────────────────────────────────
        send_telegram(
            results, l1_close, dropped_tickers, breadth_history,
            sector_counts, regime, perf_stats
        )

        # ── Compact daily email (body) + attachments (CSV, HTML, PDF) ─
        send_email(
            results, l1_close, dropped_tickers, breadth_history,
            sector_counts, regime, perf_stats, html_path, pdf_path
        )

        # ── Weekly analytical report (Fridays or FORCE_WEEKLY=1) ──────
        is_friday    = date.today().weekday() == 4   # 0=Mon … 4=Fri
        force_weekly = os.getenv("FORCE_WEEKLY", "0").strip() == "1"
        if is_friday or force_weekly:
            log.info("Friday detected — sending weekly intelligence report...")
            send_weekly_email()
        else:
            log.info(f"Weekly report skipped (not Friday). Set FORCE_WEEKLY=1 to override.")

        log.info("[OK] All done.")
    except KeyboardInterrupt:
        log.info("Stopped by user.")
    except Exception as e:
        log.error(f"Fatal error: {e}")
        log.error(traceback.format_exc())


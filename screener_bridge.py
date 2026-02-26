"""
Screener Bridge
Connects the existing screener.py to the FastAPI app.
Responsibilities:
  - Run the screener (subprocess or direct import)
  - Parse results into our DB schema
  - Select top 10 Focus List stocks
  - Trigger email delivery to active subscribers
"""

import os
import sys
import json
import logging
import subprocess
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

SCREENER_PATH = os.getenv("SCREENER_PATH", "screener.py")
OUTPUT_DIR    = os.getenv("OUTPUT_DIR", ".")


# ── Top-level entry called by the scheduler ────────────────────────────

def run_and_store() -> dict:
    """
    Run the screener, parse output, save to DB, send emails.
    Returns a summary dict.
    """
    from .database import (
        save_daily_report,
        save_focus_list,
        get_active_subscribers,
        log_report_send,
    )
    from .email_service import send_daily_report_email

    today = date.today().isoformat()
    log.info(f"=== Screener run started for {today} ===")

    # ── 1. Run screener ────────────────────────────────────────────────
    results_dict = _run_screener()
    if not results_dict:
        log.error("Screener returned no results — aborting pipeline")
        return {"status": "error", "reason": "screener_failed"}

    # ── 2. Build focus list (top 10 by SEPA score) ─────────────────────
    focus = _build_focus_list(results_dict.get("qualifiers", []))

    # ── 3. Persist to DB ───────────────────────────────────────────────
    save_daily_report(
        report_date       = today,
        regime            = results_dict.get("regime", "Unknown"),
        total_scanned     = results_dict.get("total_scanned", 0),
        full_qualifiers   = results_dict.get("full_qualifiers", 0),
        strict_qualifiers = results_dict.get("strict_qualifiers", 0),
        new_count         = results_dict.get("new_count", 0),
        dropped_count     = results_dict.get("dropped_count", 0),
        breadth_pct       = results_dict.get("breadth_pct"),
        report_html       = results_dict.get("report_html", ""),
        report_json       = json.dumps(results_dict),
    )
    save_focus_list(today, focus)
    log.info(f"Saved daily report + {len(focus)} focus stocks for {today}")

    # ── 4. Email active subscribers ────────────────────────────────────
    subscribers = get_active_subscribers()
    sent = failed = 0
    for sub in subscribers:
        ok = send_daily_report_email(
            to_email              = sub["email"],
            report_date           = today,
            regime                = results_dict.get("regime", "Unknown"),
            stats                 = results_dict,
            focus_list            = focus,
            report_html_attachment= results_dict.get("report_html"),
        )
        status = "sent" if ok else "failed"
        log_report_send(sub["id"], today, status)
        if ok:
            sent += 1
        else:
            failed += 1

    # ── 5. Weekly report (Fridays) ─────────────────────────────────────
    if date.today().weekday() == 4 or os.getenv("FORCE_WEEKLY", "0") == "1":
        _send_weekly_to_all()

    log.info(f"Pipeline complete: {sent} sent, {failed} failed, {len(subscribers)} subscribers")
    return {"status": "ok", "date": today, "sent": sent, "failed": failed, "focus_count": len(focus)}


# ── Screener runner ────────────────────────────────────────────────────

def _run_screener() -> dict | None:
    """
    Option A: Direct Python import (faster, same process).
    Option B: subprocess (more isolation).
    
    This implementation uses direct import.
    Falls back to subprocess if direct import fails.
    """
    try:
        return _run_screener_direct()
    except Exception as e:
        log.warning(f"Direct import failed ({e}), falling back to subprocess")
        return _run_screener_subprocess()


def _run_screener_direct() -> dict:
    """
    Import screener module directly and call run_screening().
    Adjust the import path to match your project layout.
    """
    # Add screener directory to path
    screener_dir = os.path.dirname(os.path.abspath(SCREENER_PATH))
    if screener_dir not in sys.path:
        sys.path.insert(0, screener_dir)

    # Dynamic import — assumes screener.py has run_screening() function
    import importlib.util
    spec   = importlib.util.spec_from_file_location("screener", SCREENER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Run the screening
    (results, l1_close, dropped_tickers,
     breadth_history, sector_counts, regime, perf_stats) = module.run_screening()

    # ── Build HTML report ──────────────────────────────────────────────
    html_path = module.generate_html_report(
        results, l1_close, dropped_tickers,
        breadth_history, sector_counts, regime, perf_stats
    )
    report_html = ""
    if html_path and os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            report_html = f.read()

    # ── Extract summary data ───────────────────────────────────────────
    qualifiers       = [r for r in results if r.qualifies]
    strict_qualifiers= [r for r in qualifiers if r.strict_qualifies]
    new_qualifiers   = [r for r in qualifiers if r.change_type == "NEW"]

    breadth_pct = None
    if breadth_history:
        last = breadth_history[-1] if breadth_history else {}
        total    = last.get("total_scanned", 0)
        full_q   = last.get("full_qualifiers", 0)
        if total:
            breadth_pct = round((full_q / total) * 100, 2)

    # Convert result objects to dicts for JSON serialization
    qualifier_dicts = []
    for r in qualifiers:
        qualifier_dicts.append({
            "ticker":       r.ticker,
            "sepa_score":   getattr(r, "sepa_score", 0),
            "rs_rank":      getattr(r, "rs_rank", 0),
            "price":        getattr(r, "price", 0.0),
            "sector":       getattr(r, "sector", ""),
            "change_type":  getattr(r, "change_type", ""),
            "is_strict":    r.strict_qualifies,
            "fund_score":   getattr(r, "fund_score", 0),
        })

    return {
        "regime":            regime.get("regime", "Unknown") if isinstance(regime, dict) else str(regime),
        "total_scanned":     len(results),
        "full_qualifiers":   len(qualifiers),
        "strict_qualifiers": len(strict_qualifiers),
        "new_count":         len(new_qualifiers),
        "dropped_count":     len(dropped_tickers),
        "breadth_pct":       breadth_pct,
        "qualifiers":        qualifier_dicts,
        "report_html":       report_html,
    }


def _run_screener_subprocess() -> dict | None:
    """
    Run screener.py as a subprocess and read its JSON output file.
    screener.py must write a file sepa_summary.json to OUTPUT_DIR.
    """
    try:
        env = os.environ.copy()
        env["OUTPUT_JSON"] = "1"     # signal screener to write JSON output
        proc = subprocess.run(
            [sys.executable, SCREENER_PATH],
            env=env,
            capture_output=True,
            text=True,
            timeout=3600,   # 1 hour max
        )
        if proc.returncode != 0:
            log.error(f"Screener subprocess failed:\n{proc.stderr[-2000:]}")
            return None

        json_path = os.path.join(OUTPUT_DIR, "sepa_summary.json")
        if not os.path.exists(json_path):
            log.error(f"Expected JSON output not found: {json_path}")
            return None

        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)

    except subprocess.TimeoutExpired:
        log.error("Screener subprocess timed out")
        return None
    except Exception as e:
        log.error(f"Subprocess error: {e}")
        return None


# ── Focus list builder ─────────────────────────────────────────────────

def _build_focus_list(qualifiers: list[dict], max_stocks: int = 10) -> list[dict]:
    """
    Select top N stocks by SEPA score for the daily Focus List.
    Priority: strict qualifiers first, then by sepa_score descending.
    """
    if not qualifiers:
        return []

    # Sort: strict first, then by sepa_score descending
    sorted_stocks = sorted(
        qualifiers,
        key=lambda x: (-(x.get("is_strict", 0)), -(x.get("sepa_score", 0)))
    )

    focus = []
    for rank, stock in enumerate(sorted_stocks[:max_stocks], start=1):
        focus.append({
            "ticker":      stock.get("ticker"),
            "rank":        rank,
            "sepa_score":  stock.get("sepa_score", 0),
            "rs_rank":     stock.get("rs_rank", 0),
            "price":       stock.get("price", 0.0),
            "sector":      stock.get("sector", ""),
            "change_type": stock.get("change_type", ""),
            "is_strict":   bool(stock.get("is_strict", 0)),
        })
    return focus


def _send_weekly_to_all() -> None:
    """Build weekly report HTML and send to all active subscribers."""
    try:
        from .database import get_active_subscribers
        from .email_service import send_weekly_report_email
        import importlib.util

        screener_dir = os.path.dirname(os.path.abspath(SCREENER_PATH))
        if screener_dir not in sys.path:
            sys.path.insert(0, screener_dir)

        spec   = importlib.util.spec_from_file_location("screener", SCREENER_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        conn         = module.get_db()
        weekly_stats = module.db_get_weekly_stats(conn)
        conn.close()
        weekly_html  = module._build_weekly_report_html(weekly_stats)

        week_num     = date.today().isocalendar()[1]
        subscribers  = get_active_subscribers()
        for sub in subscribers:
            send_weekly_report_email(sub["email"], weekly_html, week_num)
            log.info(f"Weekly report sent to {sub['email']}")

    except Exception as e:
        log.error(f"Weekly send failed: {e}")

"""
Scheduler — runs the screener at a configured time each weekday.
Can be run as a standalone process alongside the FastAPI server.

Usage:
    python -m backend.scheduler

Or set up a cron job:
    # Run at 6:00 AM EST on weekdays (adjust for your timezone)
    0 6 * * 1-5 cd /path/to/project && python -m backend.scheduler --once
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")

# What time to run the screener each weekday (24h, UTC)
RUN_HOUR   = int(os.getenv("SCREENER_RUN_HOUR",   "11"))   # 11:00 UTC = 6 AM EST
RUN_MINUTE = int(os.getenv("SCREENER_RUN_MINUTE", "0"))


def should_run_today() -> bool:
    """Only run on weekdays (Mon–Fri)."""
    return date.today().weekday() < 5  # 0=Mon, 4=Fri


def run_once() -> None:
    """Execute the screener pipeline immediately."""
    from .screener_bridge import run_and_store
    if not should_run_today():
        log.info("Today is a weekend — skipping screener run")
        return
    log.info("=== Scheduled screener run starting ===")
    result = run_and_store()
    log.info(f"=== Screener run complete: {result} ===")


def run_loop() -> None:
    """
    Infinite loop — checks every minute if it's time to run.
    Designed to run as a long-lived process.
    """
    log.info(f"Scheduler started. Will run at {RUN_HOUR:02d}:{RUN_MINUTE:02d} UTC on weekdays.")
    last_run_date = None

    while True:
        now = datetime.utcnow()
        today = now.date()

        # Check if it's the right time and we haven't run today
        if (now.hour == RUN_HOUR and
                now.minute == RUN_MINUTE and
                last_run_date != today):
            try:
                run_once()
                last_run_date = today
            except Exception as e:
                log.error(f"Scheduled run failed: {e}", exc_info=True)

        time.sleep(30)   # check every 30 seconds


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SEPA Intelligence Scheduler")
    parser.add_argument("--once", action="store_true", help="Run immediately and exit")
    args = parser.parse_args()

    if args.once:
        run_once()
    else:
        run_loop()

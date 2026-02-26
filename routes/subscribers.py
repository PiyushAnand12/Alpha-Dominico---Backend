"""
Subscriber Routes
GET  /api/status?email=   — check subscription status
POST /api/subscribe       — email capture (pre-checkout lead)
GET  /api/report/latest   — latest focus list (active subs only)
"""

import logging
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, EmailStr

from ..database import (
    get_subscriber_by_email,
    upsert_subscriber,
    get_latest_report,
    get_focus_list,
)

log = logging.getLogger(__name__)
router = APIRouter()


class LeadRequest(BaseModel):
    email: EmailStr


@router.post("/subscribe")
async def capture_lead(body: LeadRequest):
    """
    Capture email lead before Stripe checkout.
    Creates subscriber row with status='none'.
    """
    sub = upsert_subscriber(body.email)
    return {"status": "ok", "email": sub["email"]}


@router.get("/status")
async def subscription_status(email: str = Query(...)):
    """Return subscription status for a given email."""
    sub = get_subscriber_by_email(email)
    if not sub:
        return {"email": email, "status": "none"}
    return {
        "email":               sub["email"],
        "status":              sub["subscription_status"],
        "trial_end":           sub.get("trial_end"),
        "current_period_end":  sub.get("current_period_end"),
        "plan":                sub.get("plan"),
    }


@router.get("/report/latest")
async def latest_report(email: str = Query(...)):
    """Return the latest focus list — only for active/trialing subscribers."""
    sub = get_subscriber_by_email(email)
    if not sub:
        raise HTTPException(status_code=403, detail="No subscription found")

    allowed = ("active", "trialing")
    if sub.get("subscription_status") not in allowed:
        raise HTTPException(status_code=403, detail="Active subscription required")

    report = get_latest_report()
    if not report:
        return {"message": "No reports published yet"}

    focus = get_focus_list(report["report_date"])
    return {
        "report_date":      report["report_date"],
        "regime":           report["regime"],
        "full_qualifiers":  report["full_qualifiers"],
        "strict_qualifiers":report["strict_qualifiers"],
        "new_count":        report["new_count"],
        "dropped_count":    report["dropped_count"],
        "focus_list":       focus,
    }

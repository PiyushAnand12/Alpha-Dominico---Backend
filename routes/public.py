"""
Public Routes
GET /success        — post-checkout success page
GET /reports/{date} — view daily report (subscribers only)
GET /unsubscribe    — cancel via link
"""

import logging
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from ..database import get_subscriber_by_email, get_latest_report, get_focus_list
from ..email_service import send_welcome_email

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/success", response_class=HTMLResponse)
async def success_page(session_id: str = Query(default="")):
    """Post-Stripe-checkout success page."""
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Welcome — SEPA Intelligence</title>
<style>
  body{margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f4f5f7;display:flex;align-items:center;justify-content:center;min-height:100vh}
  .card{background:#fff;border-radius:12px;padding:48px;text-align:center;max-width:480px;box-shadow:0 4px 24px rgba(0,0,0,.08)}
  .icon{font-size:56px;margin-bottom:16px}
  h1{margin:0 0 12px;font-size:24px;color:#0d1b2a}
  p{color:#666;font-size:15px;line-height:1.6;margin:0 0 24px}
  .btn{display:inline-block;padding:12px 28px;background:#1a6ef7;color:#fff;border-radius:6px;text-decoration:none;font-weight:600}
  .note{font-size:12px;color:#999;margin-top:16px}
</style>
</head>
<body>
<div class="card">
  <div class="icon">✅</div>
  <h1>You're in!</h1>
  <p>Your 7-day free trial is now active. Check your inbox — your first report arrives the next trading day morning.</p>
  <a href="/" class="btn">Return to Home</a>
  <p class="note">No charge during your trial. Cancel anytime.</p>
</div>
</body>
</html>"""
    return HTMLResponse(content=html)


@router.get("/reports/{report_date}", response_class=HTMLResponse)
async def view_report(report_date: str, email: str = Query(default="")):
    """Serve the full HTML report to authenticated subscribers."""
    sub = get_subscriber_by_email(email) if email else None
    allowed = ("active", "trialing")
    if not sub or sub.get("subscription_status") not in allowed:
        return HTMLResponse(content="""
<html><body style="font-family:sans-serif;text-align:center;padding:80px">
<h2>Active subscription required</h2>
<p>Please subscribe to view reports.</p>
<a href="/">Subscribe</a>
</body></html>""", status_code=403)

    from ..database import get_conn
    with get_conn() as conn:
        row = conn.execute(
            "SELECT report_html FROM daily_reports WHERE report_date = ?",
            (report_date,)
        ).fetchone()

    if not row or not row["report_html"]:
        return HTMLResponse(content="<html><body>Report not found.</body></html>", status_code=404)

    return HTMLResponse(content=row["report_html"])

"""
Email Service — SendGrid
Handles:
  - Daily report delivery to active subscribers
  - Weekly summary delivery
  - Welcome / trial confirmation email
  - Payment failure notice
"""

import os
import logging
from datetime import date, datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
import base64
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

SENDGRID_API_KEY    = os.getenv("SENDGRID_API_KEY", "")
FROM_EMAIL          = os.getenv("FROM_EMAIL", "reports@sepa-intelligence.com")
FROM_NAME           = os.getenv("FROM_NAME",  "SEPA Intelligence")
APP_URL             = os.getenv("APP_URL",    "http://localhost:8000")


# ── Core send helper ───────────────────────────────────────────────────

def _send(to_email: str, subject: str, html_body: str, attachments: list | None = None) -> bool:
    """Send a single email via SendGrid. Returns True on success."""
    if not SENDGRID_API_KEY:
        log.warning("SENDGRID_API_KEY not set — skipping email")
        return False

    message = Mail(
        from_email=(FROM_EMAIL, FROM_NAME),
        to_emails=to_email,
        subject=subject,
        html_content=html_body,
    )

    if attachments:
        for att in attachments:
            message.add_attachment(att)

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        log.info(f"Email sent to {to_email} | status={response.status_code}")
        return response.status_code in (200, 201, 202)
    except Exception as e:
        log.error(f"SendGrid error for {to_email}: {e}")
        return False


def _make_attachment(content: str, filename: str, mime_type: str) -> Attachment:
    """Create a SendGrid Attachment object from a string."""
    encoded = base64.b64encode(content.encode("utf-8")).decode()
    att = Attachment()
    att.file_content = FileContent(encoded)
    att.file_name    = FileName(filename)
    att.file_type    = FileType(mime_type)
    att.disposition  = Disposition("attachment")
    return att


# ── Email templates ────────────────────────────────────────────────────

def _base_layout(title: str, content: str) -> str:
    """Wrap email content in a clean, minimal HTML layout."""
    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>
  body{{margin:0;padding:0;background:#f4f5f7;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#1a1a2e}}
  .wrapper{{max-width:640px;margin:32px auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08)}}
  .header{{background:#0d1b2a;padding:28px 32px;text-align:center}}
  .header h1{{margin:0;color:#fff;font-size:18px;font-weight:600;letter-spacing:.5px}}
  .header p{{margin:4px 0 0;color:#8899aa;font-size:12px}}
  .body{{padding:32px}}
  .footer{{background:#f4f5f7;padding:20px 32px;text-align:center;font-size:11px;color:#888;border-top:1px solid #e8eaed}}
  .footer a{{color:#888;text-decoration:underline}}
  .btn{{display:inline-block;padding:12px 28px;background:#1a6ef7;color:#fff!important;border-radius:6px;text-decoration:none;font-weight:600;font-size:14px;margin:16px 0}}
  h2{{font-size:16px;color:#0d1b2a;margin:0 0 8px}}
  p{{font-size:14px;line-height:1.6;color:#444;margin:0 0 12px}}
  table{{width:100%;border-collapse:collapse;font-size:13px;margin:16px 0}}
  th{{background:#0d1b2a;color:#fff;padding:8px 12px;text-align:left;font-weight:500}}
  td{{padding:8px 12px;border-bottom:1px solid #f0f0f0;color:#333}}
  tr:last-child td{{border-bottom:none}}
  .badge{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600}}
  .badge-new{{background:#d4edda;color:#155724}}
  .badge-strict{{background:#cce5ff;color:#004085}}
  .badge-dropped{{background:#f8d7da;color:#721c24}}
  .stat-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:16px 0}}
  .stat-box{{background:#f8f9fa;border-radius:6px;padding:12px;text-align:center}}
  .stat-box .val{{font-size:22px;font-weight:700;color:#0d1b2a}}
  .stat-box .lbl{{font-size:11px;color:#888;margin-top:2px}}
  .divider{{border:none;border-top:1px solid #f0f0f0;margin:20px 0}}
</style>
</head>
<body>
<div class="wrapper">
  <div class="header">
    <h1>SEPA Market Intelligence</h1>
    <p>Daily US Growth Stock Qualification Report</p>
  </div>
  <div class="body">{content}</div>
  <div class="footer">
    <p>This is a data-driven screening tool, not investment advice.<br>
    No buy/sell signals. No recommendations. Structured qualification only.</p>
    <p><a href="{APP_URL}/unsubscribe">Unsubscribe</a> · 
       <a href="{APP_URL}/billing">Manage Billing</a> · 
       <a href="{APP_URL}">Website</a></p>
  </div>
</div>
</body>
</html>"""


# ── Public email functions ─────────────────────────────────────────────

def send_welcome_email(to_email: str) -> bool:
    """Sent immediately after checkout completion."""
    content = f"""
<h2>Welcome to SEPA Intelligence 👋</h2>
<p>Your 7-day free trial is now active. You'll receive your first daily report the next time our screener runs.</p>
<p><strong>What to expect:</strong></p>
<ul style="font-size:14px;line-height:1.8;color:#444;padding-left:20px">
  <li>Daily US Growth Stock Qualification Report (weekdays)</li>
  <li>Top 8–12 Focus List stocks ranked by SEPA score</li>
  <li>Newly qualified & dropped stocks highlighted</li>
  <li>Market Breadth Indicator + Regime classification</li>
  <li>Weekly Statistical Summary (every Friday)</li>
</ul>
<p>This is <strong>not</strong> financial advice or buy/sell signals — it is objective, rules-based screening.</p>
<a href="{APP_URL}" class="btn">Visit Dashboard</a>
<hr class="divider">
<p style="font-size:12px;color:#888">After your trial ends, you'll be charged $15/month. Cancel anytime from your billing portal — no questions asked.</p>
"""
    return _send(
        to_email=to_email,
        subject="Welcome to SEPA Intelligence — Your Trial is Active",
        html_body=_base_layout("Welcome", content),
    )


def send_daily_report_email(
    to_email: str,
    report_date: str,
    regime: str,
    stats: dict,
    focus_list: list[dict],
    report_html_attachment: str | None = None,
) -> bool:
    """
    Send the daily screening report to one subscriber.
    focus_list: list of dicts with ticker, sepa_score, rs_rank, sector, change_type, is_strict
    """
    today_str = datetime.strptime(report_date, "%Y-%m-%d").strftime("%B %d, %Y")
    new_count     = stats.get("new_count", 0)
    dropped_count = stats.get("dropped_count", 0)
    full_q        = stats.get("full_qualifiers", 0)
    strict_q      = stats.get("strict_qualifiers", 0)
    breadth       = stats.get("breadth_pct")
    breadth_str   = f"{breadth:.1f}%" if breadth is not None else "N/A"

    # ── Stats row ─────────────────────────────────────────────────────
    stat_html = f"""
<div class="stat-grid">
  <div class="stat-box"><div class="val">{full_q}</div><div class="lbl">Standard Qualifiers</div></div>
  <div class="stat-box"><div class="val">{strict_q}</div><div class="lbl">Strict (High-Conv.)</div></div>
  <div class="stat-box"><div class="val">{new_count}</div><div class="lbl">Newly Qualified</div></div>
</div>
<div class="stat-grid">
  <div class="stat-box"><div class="val">{dropped_count}</div><div class="lbl">Dropped Today</div></div>
  <div class="stat-box"><div class="val">{regime}</div><div class="lbl">Market Regime</div></div>
  <div class="stat-box"><div class="val">{breadth_str}</div><div class="lbl">Market Breadth</div></div>
</div>
"""

    # ── Focus list table ───────────────────────────────────────────────
    rows = ""
    for stock in focus_list[:12]:
        ticker      = stock.get("ticker", "")
        score       = stock.get("sepa_score", "")
        rs          = stock.get("rs_rank", "")
        sector      = stock.get("sector", "")
        change_type = stock.get("change_type", "")
        is_strict   = stock.get("is_strict", 0)

        badge = ""
        if change_type == "NEW":
            badge = '<span class="badge badge-new">NEW</span>'
        elif change_type == "DROPPED":
            badge = '<span class="badge badge-dropped">DROPPED</span>'
        if is_strict:
            badge += ' <span class="badge badge-strict">STRICT</span>'

        rows += f"""
<tr>
  <td><strong>{ticker}</strong> {badge}</td>
  <td>{score}</td>
  <td>{rs}</td>
  <td>{sector}</td>
</tr>"""

    focus_table = f"""
<h2 style="margin-top:24px">📋 Today's Focus List</h2>
<table>
  <thead><tr><th>Ticker</th><th>SEPA Score</th><th>RS Rank</th><th>Sector</th></tr></thead>
  <tbody>{rows}</tbody>
</table>
<p style="font-size:11px;color:#888">Full report attached as HTML. SEPA qualification criteria only — not a buy signal.</p>
"""

    content = f"""
<h2>Daily Report — {today_str}</h2>
<p>Market Regime: <strong>{regime}</strong></p>
{stat_html}
<hr class="divider">
{focus_table}
<a href="{APP_URL}/reports/{report_date}" class="btn">View Full Report Online</a>
"""

    # Optional: attach full HTML report
    attachments = []
    if report_html_attachment:
        att = _make_attachment(
            report_html_attachment,
            f"sepa_report_{report_date}.html",
            "text/html"
        )
        attachments.append(att)

    return _send(
        to_email=to_email,
        subject=f"SEPA Intelligence | {today_str} | Regime: {regime} | {full_q} Qualified | {new_count} New",
        html_body=_base_layout(f"Daily Report {today_str}", content),
        attachments=attachments or None,
    )


def send_weekly_report_email(to_email: str, weekly_html: str, week_num: int) -> bool:
    """Send the weekly analytical summary."""
    today_str = date.today().strftime("%B %d, %Y")
    content = f"""
<h2>Weekly Intelligence Report — Week {week_num}</h2>
<p>Your weekly SEPA analytics summary is attached. It covers qualification trends, sector rotation, and performance analytics for the past 7 days.</p>
<a href="{APP_URL}" class="btn">View Dashboard</a>
"""
    att = _make_attachment(weekly_html, f"sepa_weekly_w{week_num}.html", "text/html")
    return _send(
        to_email=to_email,
        subject=f"SEPA Intelligence | Weekly Summary | Week {week_num} | {today_str}",
        html_body=_base_layout(f"Weekly Report W{week_num}", content),
        attachments=[att],
    )


def send_payment_failed_email(to_email: str) -> bool:
    content = f"""
<h2>Payment Issue — Action Required</h2>
<p>We were unable to process your subscription payment. Your access to SEPA Intelligence has been paused.</p>
<p>Please update your payment method to continue receiving daily reports.</p>
<a href="{APP_URL}/billing" class="btn">Update Payment Method</a>
<p style="font-size:12px;color:#888;margin-top:16px">If you have questions, reply to this email.</p>
"""
    return _send(
        to_email=to_email,
        subject="SEPA Intelligence — Payment Failed, Action Required",
        html_body=_base_layout("Payment Issue", content),
    )


def send_trial_ending_email(to_email: str, days_left: int) -> bool:
    content = f"""
<h2>Your Trial Ends in {days_left} Day{'s' if days_left != 1 else ''}</h2>
<p>You've been receiving SEPA Intelligence daily reports during your free trial. Your subscription will automatically continue at <strong>$15/month</strong> when the trial ends.</p>
<p>No action needed if you'd like to continue. Cancel anytime from your billing portal.</p>
<a href="{APP_URL}/billing" class="btn">Manage Subscription</a>
"""
    return _send(
        to_email=to_email,
        subject=f"SEPA Intelligence — Trial ends in {days_left} day{'s' if days_left != 1 else ''}",
        html_body=_base_layout("Trial Ending Soon", content),
    )

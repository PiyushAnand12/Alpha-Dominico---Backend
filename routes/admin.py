"""
Admin Routes — Protected by API key header
GET  /admin/              — admin dashboard (HTML)
GET  /admin/subscribers   — all subscribers JSON
POST /admin/run-screener  — trigger screener manually
POST /admin/resend/{date} — resend report for a given date
GET  /admin/reports       — list recent reports
"""

import os
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.security import APIKeyHeader
from dotenv import load_dotenv

from ..database import (
    get_all_subscribers,
    get_latest_report,
    get_focus_list,
    get_active_subscribers,
    get_conn,
    log_report_send,
)
from ..email_service import send_daily_report_email

load_dotenv()
log = logging.getLogger(__name__)
router = APIRouter()

ADMIN_KEY = os.getenv("ADMIN_API_KEY", "change-me-in-production")
api_key_header = APIKeyHeader(name="X-Admin-Key", auto_error=False)


def require_admin(key: str = Depends(api_key_header)):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Invalid admin key")
    return key


# ── Admin Dashboard HTML ───────────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
async def admin_dashboard(request: Request, _=Depends(require_admin)):
    subscribers = get_all_subscribers()
    report      = get_latest_report()

    active_count   = sum(1 for s in subscribers if s["subscription_status"] in ("active", "trialing"))
    trial_count    = sum(1 for s in subscribers if s["subscription_status"] == "trialing")
    canceled_count = sum(1 for s in subscribers if s["subscription_status"] == "canceled")
    total_count    = len(subscribers)

    sub_rows = ""
    for s in subscribers:
        status = s["subscription_status"]
        badge_color = {
            "active":   "#28a745",
            "trialing": "#17a2b8",
            "past_due": "#ffc107",
            "canceled": "#dc3545",
            "none":     "#6c757d",
        }.get(status, "#6c757d")

        sub_rows += f"""
<tr>
  <td>{s['email']}</td>
  <td><span style="background:{badge_color};color:#fff;padding:2px 8px;border-radius:4px;font-size:11px">{status}</span></td>
  <td>{s.get('plan','')}</td>
  <td>{s.get('trial_end','')[:10] if s.get('trial_end') else '—'}</td>
  <td>{s.get('current_period_end','')[:10] if s.get('current_period_end') else '—'}</td>
  <td>{s.get('created_at','')[:10]}</td>
</tr>"""

    focus_rows = ""
    if report:
        focus = get_focus_list(report["report_date"])
        for stock in focus:
            badge = '<span style="background:#17a2b8;color:#fff;padding:1px 6px;border-radius:3px;font-size:10px">STRICT</span>' if stock.get("is_strict") else ""
            new_badge = '<span style="background:#28a745;color:#fff;padding:1px 6px;border-radius:3px;font-size:10px">NEW</span>' if stock.get("change_type") == "NEW" else ""
            focus_rows += f"""
<tr>
  <td>{stock['rank']}</td>
  <td><strong>{stock['ticker']}</strong> {badge} {new_badge}</td>
  <td>{stock['sepa_score']}</td>
  <td>{stock['rs_rank']}</td>
  <td>{stock['sector']}</td>
  <td>${stock['price']:.2f}</td>
</tr>"""

    report_info = ""
    if report:
        report_info = f"""
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0">
  <div style="background:#f8f9fa;border-radius:6px;padding:16px;text-align:center">
    <div style="font-size:28px;font-weight:700;color:#0d1b2a">{report['full_qualifiers']}</div>
    <div style="font-size:11px;color:#888;margin-top:4px">Standard Qualifiers</div>
  </div>
  <div style="background:#f8f9fa;border-radius:6px;padding:16px;text-align:center">
    <div style="font-size:28px;font-weight:700;color:#0d1b2a">{report['strict_qualifiers']}</div>
    <div style="font-size:11px;color:#888;margin-top:4px">Strict (High-Conv.)</div>
  </div>
  <div style="background:#f8f9fa;border-radius:6px;padding:16px;text-align:center">
    <div style="font-size:28px;font-weight:700;color:#0d1b2a">{report['new_count']}</div>
    <div style="font-size:11px;color:#888;margin-top:4px">Newly Qualified</div>
  </div>
  <div style="background:#f8f9fa;border-radius:6px;padding:16px;text-align:center">
    <div style="font-size:28px;font-weight:700;color:#0d1b2a">{report['regime']}</div>
    <div style="font-size:11px;color:#888;margin-top:4px">Market Regime</div>
  </div>
</div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SEPA Admin</title>
<style>
  *{{box-sizing:border-box}}
  body{{margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f4f5f7;color:#1a1a2e}}
  .sidebar{{position:fixed;top:0;left:0;bottom:0;width:220px;background:#0d1b2a;padding:24px 16px}}
  .sidebar h1{{color:#fff;font-size:15px;margin:0 0 4px;font-weight:700}}
  .sidebar p{{color:#8899aa;font-size:11px;margin:0 0 32px}}
  .sidebar a{{display:block;color:#8899aa;text-decoration:none;padding:8px 12px;border-radius:6px;font-size:13px;margin:2px 0;transition:all .2s}}
  .sidebar a:hover{{background:#1a3050;color:#fff}}
  .main{{margin-left:220px;padding:32px}}
  .page-title{{font-size:22px;font-weight:700;margin:0 0 24px;color:#0d1b2a}}
  .stat-bar{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:28px}}
  .stat-card{{background:#fff;border-radius:8px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
  .stat-card .val{{font-size:32px;font-weight:700;color:#0d1b2a}}
  .stat-card .lbl{{font-size:12px;color:#888;margin-top:4px}}
  .card{{background:#fff;border-radius:8px;padding:24px;box-shadow:0 1px 4px rgba(0,0,0,.06);margin-bottom:24px}}
  .card h2{{font-size:15px;font-weight:600;margin:0 0 16px;color:#0d1b2a}}
  table{{width:100%;border-collapse:collapse;font-size:13px}}
  th{{background:#f8f9fa;padding:10px 14px;text-align:left;font-weight:600;color:#555;font-size:12px;border-bottom:1px solid #e8eaed}}
  td{{padding:10px 14px;border-bottom:1px solid #f0f0f0;color:#333}}
  tr:last-child td{{border-bottom:none}}
  .btn{{display:inline-block;padding:8px 18px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;border:none;text-decoration:none}}
  .btn-primary{{background:#1a6ef7;color:#fff}}
  .btn-secondary{{background:#f0f0f0;color:#333}}
  .action-bar{{display:flex;gap:10px;margin-bottom:16px;align-items:center}}
  #toast{{position:fixed;top:20px;right:20px;background:#28a745;color:#fff;padding:12px 20px;border-radius:6px;display:none;font-size:13px;z-index:999}}
</style>
</head>
<body>
<div id="toast"></div>
<div class="sidebar">
  <h1>SEPA Intelligence</h1>
  <p>Admin Panel</p>
  <a href="/admin/?key={ADMIN_KEY}">📊 Dashboard</a>
  <a href="/admin/subscribers?key={ADMIN_KEY}">👥 Subscribers</a>
  <a href="/admin/reports?key={ADMIN_KEY}">📋 Reports</a>
  <a href="/" target="_blank">🌐 Landing Page</a>
  <a href="/api/docs" target="_blank">⚙️ API Docs</a>
</div>
<div class="main">
  <div class="page-title">Dashboard</div>
  
  <div class="stat-bar">
    <div class="stat-card"><div class="val">{total_count}</div><div class="lbl">Total Subscribers</div></div>
    <div class="stat-card"><div class="val">{active_count}</div><div class="lbl">Active / Trial</div></div>
    <div class="stat-card"><div class="val">{trial_count}</div><div class="lbl">In Trial</div></div>
    <div class="stat-card"><div class="val">{canceled_count}</div><div class="lbl">Canceled</div></div>
  </div>

  <div class="card">
    <h2>Latest Report — {report['report_date'] if report else 'No reports yet'}</h2>
    {report_info}
    <div class="action-bar">
      <button class="btn btn-primary" onclick="runScreener()">▶ Run Screener Now</button>
      <button class="btn btn-secondary" onclick="resendReport()">📧 Resend Today's Report</button>
      <span id="run-status" style="font-size:12px;color:#888"></span>
    </div>
  </div>

  <div class="card">
    <h2>Today's Focus List</h2>
    <table>
      <thead><tr><th>#</th><th>Ticker</th><th>SEPA Score</th><th>RS Rank</th><th>Sector</th><th>Price</th></tr></thead>
      <tbody>{focus_rows or '<tr><td colspan="6" style="text-align:center;color:#888;padding:24px">No focus list available</td></tr>'}</tbody>
    </table>
  </div>

  <div class="card">
    <h2>Subscribers</h2>
    <table>
      <thead><tr><th>Email</th><th>Status</th><th>Plan</th><th>Trial End</th><th>Period End</th><th>Joined</th></tr></thead>
      <tbody>{sub_rows or '<tr><td colspan="6" style="text-align:center;color:#888;padding:24px">No subscribers yet</td></tr>'}</tbody>
    </table>
  </div>
</div>

<script>
const ADMIN_KEY = "{ADMIN_KEY}";

function toast(msg, color="#28a745") {{
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.style.background = color;
  el.style.display = "block";
  setTimeout(() => el.style.display = "none", 4000);
}}

async function runScreener() {{
  document.getElementById("run-status").textContent = "Running… this may take several minutes";
  try {{
    const r = await fetch("/admin/run-screener", {{
      method: "POST",
      headers: {{"X-Admin-Key": ADMIN_KEY}}
    }});
    const d = await r.json();
    if (d.status === "ok") {{
      toast(`Screener complete: ${{d.sent}} emails sent`);
    }} else {{
      toast("Screener failed: " + (d.detail || JSON.stringify(d)), "#dc3545");
    }}
  }} catch(e) {{
    toast("Error: " + e.message, "#dc3545");
  }}
  document.getElementById("run-status").textContent = "";
}}

async function resendReport() {{
  const date = "{report['report_date'] if report else ''}";
  if (!date) {{ toast("No report to resend", "#ffc107"); return; }}
  try {{
    const r = await fetch(`/admin/resend/${{date}}`, {{
      method: "POST",
      headers: {{"X-Admin-Key": ADMIN_KEY}}
    }});
    const d = await r.json();
    toast(`Resent: ${{d.sent}} emails`);
  }} catch(e) {{
    toast("Error: " + e.message, "#dc3545");
  }}
}}
</script>
</body>
</html>"""
    return HTMLResponse(content=html)


# ── JSON API endpoints ─────────────────────────────────────────────────

@router.get("/subscribers")
async def list_subscribers(_=Depends(require_admin)):
    return get_all_subscribers()


@router.get("/reports")
async def list_reports(_=Depends(require_admin)):
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT report_date, regime, full_qualifiers, strict_qualifiers,
                      new_count, dropped_count, breadth_pct, created_at
               FROM daily_reports ORDER BY report_date DESC LIMIT 30"""
        ).fetchall()
    return [dict(r) for r in rows]


@router.post("/run-screener")
async def trigger_screener(background_tasks: BackgroundTasks, _=Depends(require_admin)):
    """Trigger screener in background — returns immediately."""
    background_tasks.add_task(_run_screener_task)
    return {"status": "started", "message": "Screener running in background"}


async def _run_screener_task():
    from ..screener_bridge import run_and_store
    try:
        result = run_and_store()
        log.info(f"Background screener run complete: {result}")
    except Exception as e:
        log.error(f"Background screener run failed: {e}")


@router.post("/resend/{report_date}")
async def resend_report(report_date: str, _=Depends(require_admin)):
    """Manually resend a report for a specific date to all active subscribers."""
    from ..database import get_focus_list
    import json

    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM daily_reports WHERE report_date = ?", (report_date,)
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Report not found")

    report   = dict(row)
    focus    = get_focus_list(report_date)
    subs     = get_active_subscribers()
    sent = failed = 0

    for sub in subs:
        ok = send_daily_report_email(
            to_email    = sub["email"],
            report_date = report_date,
            regime      = report["regime"],
            stats       = report,
            focus_list  = focus,
            report_html_attachment = report.get("report_html"),
        )
        log_report_send(sub["id"], report_date, "sent" if ok else "failed")
        if ok:
            sent += 1
        else:
            failed += 1

    return {"status": "ok", "sent": sent, "failed": failed}


# ── Public success/cancel pages ────────────────────────────────────────
@router.get("/", include_in_schema=False)
async def handle_public_routes():
    pass

"""
╔══════════════════════════════════════════════════════════════╗
║  ALPHAdominico — FastAPI Backend (main.py)                  ║
║  Handles: Auth, Stripe, SendGrid, Admin, Screener Bridge    ║
╚══════════════════════════════════════════════════════════════╝

Audit fixes applied (March 2026):
  [C-02] Swagger UI and ReDoc disabled in production (ENV=production).
         Accessible only in development.
  [C-03] CORS wildcard (*) replaced with explicit allowed origins
         loaded from ALLOWED_ORIGINS env var.
         allow_credentials removed — was True + wildcard, which is
         an invalid combination and a security misconfiguration.
  [H-01] slowapi rate limiter wired up (default 60/min, waitlist 5/min).
  [SEC]  Request body size capped at 64 KB via middleware.
  [SEC]  TrustedHostMiddleware added for production deployments.
  [SEC]  Security response headers (HSTS, CSP, X-Frame-Options, etc.)
         added to every response.
  [FIX]  Product name unified to ALPHAdominico throughout.

Required new packages (add to requirements.txt):
  slowapi
  limits

Required environment variables (.env / Render dashboard):
  ENV              — "production" | "development"  (default: development)
  ALLOWED_ORIGINS  — comma-separated frontend origins
                     e.g. https://alphadominico.com,https://www.alphadominico.com
  TRUSTED_HOSTS    — comma-separated allowed Host header values (prod only)
                     e.g. alphadominico.com,www.alphadominico.com
  RATE_LIMIT_DEFAULT   — e.g. "60/minute"  (default: 60/minute)
  RATE_LIMIT_WAITLIST  — e.g. "5/minute"   (default: 5/minute)
"""

import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from dotenv import load_dotenv

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from .database import init_db
from .routes import subscribers, stripe_routes, admin, public
from .routes.waitlist import router as waitlist_router

load_dotenv()


# ── Environment ────────────────────────────────────────────────────────
ENV     = os.getenv("ENV", "development").lower()
IS_PROD = ENV == "production"


# ── Logging ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO if IS_PROD else logging.DEBUG,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

if IS_PROD:
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


# ── CORS allowed origins ───────────────────────────────────────────────
# [C-03] Load from env — no wildcard ever allowed in production.
_raw_origins = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:3000,http://localhost:5500,http://127.0.0.1:5500",
)
ALLOWED_ORIGINS: list[str] = [o.strip() for o in _raw_origins.split(",") if o.strip()]

if IS_PROD and "*" in ALLOWED_ORIGINS:
    raise RuntimeError(
        "[SECURITY] ALLOWED_ORIGINS must not contain '*' in production. "
        "Set it to your exact frontend domain(s) in the Render environment variables."
    )

log.info("CORS allowed origins: %s", ALLOWED_ORIGINS)


# ── Rate limiter ───────────────────────────────────────────────────────
# [H-01] Keyed by client IP. Limits configurable without a redeploy.
_default_limit  = os.getenv("RATE_LIMIT_DEFAULT",  "60/minute")
_waitlist_limit = os.getenv("RATE_LIMIT_WAITLIST",  "5/minute")
limiter = Limiter(key_func=get_remote_address, default_limits=[_default_limit])


# ── Request body size cap ──────────────────────────────────────────────
# [SEC] Rejects payloads larger than 64 KB to prevent flood attacks.
class MaxBodySizeMiddleware(BaseHTTPMiddleware):
    MAX_BYTES = 64 * 1024  # 64 KB

    async def dispatch(self, request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > self.MAX_BYTES:
            return JSONResponse(
                status_code=413,
                content={"error": "Request body too large. Maximum allowed size is 64 KB."},
            )
        return await call_next(request)


# ── Lifespan ───────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting ALPHAdominico API (env=%s)...", ENV)
    init_db()
    log.info("Database layer initialised.")
    yield
    log.info("ALPHAdominico API shutting down.")


# ── App factory ────────────────────────────────────────────────────────
# [C-02] Swagger UI and ReDoc completely disabled in production.
#        Set ENV=development locally to restore them.
app = FastAPI(
    title="ALPHAdominico API",
    description=(
        "Daily Indian stock screener (NSE/BSE). "
        "Rules-based qualification engine — not investment advice."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url    ="/api/docs"   if not IS_PROD else None,
    redoc_url   ="/api/redoc"  if not IS_PROD else None,
    openapi_url ="/openapi.json" if not IS_PROD else None,
)


# ── Middleware stack ───────────────────────────────────────────────────
# Order matters: outermost middleware runs first on requests,
# last on responses.

# 1. TrustedHost — [SEC] rejects unexpected Host headers in production
if IS_PROD:
    _trusted_raw = os.getenv(
        "TRUSTED_HOSTS",
        "alphadominico.com,www.alphadominico.com",
    )
    _trusted_hosts = [h.strip() for h in _trusted_raw.split(",") if h.strip()]
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=_trusted_hosts)

# 2. GZip — compress HTML report payloads
app.add_middleware(GZipMiddleware, minimum_size=1000)

# 3. Body size cap — [SEC]
app.add_middleware(MaxBodySizeMiddleware)

# 4. CORS — [C-03] explicit origins, no wildcard, no credentials
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,        # was True + wildcard — invalid combination
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

# 5. Rate limiter — [H-01]
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)


# ── Security response headers ──────────────────────────────────────────
# [SEC] Added to every response. Not a replacement for a full
# security headers audit, but covers the most critical basics.
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"]         = "DENY"
    response.headers["Referrer-Policy"]          = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"]       = "geolocation=(), camera=(), microphone=()"
    if IS_PROD:
        # Tell browsers to use HTTPS-only for 1 year
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains; preload"
        )
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://challenges.cloudflare.com; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src https://fonts.gstatic.com; "
        "img-src 'self' data:; "
        "connect-src 'self';"
    )
    return response


# ── Static files & templates ───────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_static_dir = os.path.join(BASE_DIR, "frontend", "static")

if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")
else:
    log.warning(
        "Static directory not found at %s — /static not mounted. "
        "Create frontend/static/ or update BASE_DIR.",
        _static_dir,
    )

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "frontend"))


# ── Routers ────────────────────────────────────────────────────────────
app.include_router(public.router,                           tags=["Public"])
app.include_router(subscribers.router,   prefix="/api",    tags=["Subscribers"])
app.include_router(stripe_routes.router, prefix="/api",    tags=["Stripe"])
app.include_router(admin.router,         prefix="/admin",   tags=["Admin"])
app.include_router(waitlist_router)


# ── Health check ───────────────────────────────────────────────────────
@app.get("/health", tags=["System"], include_in_schema=not IS_PROD)
async def health():
    """
    Public health endpoint.
    Used by UptimeRobot, Render health checks, and the daily keepalive cron.
    Never returns sensitive internal state.
    """
    return {"status": "ok", "env": ENV, "version": "1.0.0"}


# ── Landing page ───────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def landing(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ── Global exception handler ───────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    # Log the full error internally but never expose stack traces to clients
    log.error(
        "Unhandled exception on %s %s: %r",
        request.method,
        request.url.path,
        exc,
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={"error": "An unexpected error occurred. Please try again."},
    )

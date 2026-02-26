"""
╔══════════════════════════════════════════════════════════════╗
║  SEPA Intelligence — FastAPI Backend                        ║
║  Handles: Auth, Stripe, SendGrid, Admin, Screener Bridge    ║
╚══════════════════════════════════════════════════════════════╝
"""

import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

from .database import init_db
from .routes import subscribers, stripe_routes, admin, public

load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s"
)
log = logging.getLogger(__name__)


# ── Lifespan: runs on startup / shutdown ───────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting SEPA Intelligence API...")
    init_db()          # ensure all tables exist
    log.info("Database initialized.")
    yield
    log.info("Shutting down.")


# ── App factory ────────────────────────────────────────────────────────
app = FastAPI(
    title="SEPA Intelligence API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",       # restrict swagger in prod if desired
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # tighten to your domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Static files & templates ───────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "frontend/static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "frontend"))


# ── Routers ────────────────────────────────────────────────────────────
app.include_router(public.router,           tags=["Public"])
app.include_router(subscribers.router,      prefix="/api",  tags=["Subscribers"])
app.include_router(stripe_routes.router,    prefix="/api",  tags=["Stripe"])
app.include_router(admin.router,            prefix="/admin",tags=["Admin"])


# ── Landing page ───────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def landing(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

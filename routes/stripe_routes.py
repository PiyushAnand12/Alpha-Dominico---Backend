"""
Stripe Routes
POST /api/checkout      — create checkout session, return URL
POST /api/webhook       — receive Stripe webhook events
GET  /api/billing       — redirect to customer billing portal
"""

import logging
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, EmailStr

from ..stripe_service import (
    create_checkout_session,
    handle_webhook,
    get_customer_portal_url,
)
from ..database import get_subscriber_by_email

log = logging.getLogger(__name__)
router = APIRouter()


class CheckoutRequest(BaseModel):
    email: EmailStr


@router.post("/checkout")
async def checkout(body: CheckoutRequest):
    """Create a Stripe Checkout Session and return the URL."""
    try:
        url = create_checkout_session(body.email)
        return {"checkout_url": url}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error(f"Checkout creation failed: {e}")
        raise HTTPException(status_code=500, detail="Payment setup failed. Please try again.")


@router.post("/webhook")
async def stripe_webhook(request: Request):
    """
    Stripe sends webhook events here.
    IMPORTANT: reads raw body to validate signature.
    """
    payload    = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        result = handle_webhook(payload, sig_header)
        return JSONResponse(content=result)
    except ValueError as e:
        log.warning(f"Webhook rejected: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error(f"Webhook processing error: {e}")
        raise HTTPException(status_code=500, detail="Webhook processing failed")


@router.get("/billing")
async def billing_portal(email: str):
    """Redirect subscriber to their Stripe Customer Portal."""
    sub = get_subscriber_by_email(email)
    if not sub or not sub.get("stripe_customer_id"):
        raise HTTPException(status_code=404, detail="No billing account found for this email")
    try:
        url = get_customer_portal_url(sub["stripe_customer_id"])
        return RedirectResponse(url=url)
    except Exception as e:
        log.error(f"Billing portal error: {e}")
        raise HTTPException(status_code=500, detail="Could not open billing portal")

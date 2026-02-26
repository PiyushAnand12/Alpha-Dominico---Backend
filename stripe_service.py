"""
Stripe Integration Module
Handles:
  - Create Stripe Customer
  - Create Checkout Session (with 7-day free trial)
  - Webhook event processing
  - Subscription status sync to DB
"""

import os
import json
import logging
import stripe
from datetime import datetime, timezone
from dotenv import load_dotenv

from .database import (
    get_subscriber_by_email,
    get_subscriber_by_stripe_customer,
    get_subscriber_by_stripe_sub,
    update_subscriber_stripe,
    update_subscription_status,
    upsert_subscriber,
)

load_dotenv()
log = logging.getLogger(__name__)

# ── Stripe config ──────────────────────────────────────────────────────
stripe.api_key          = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET   = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRICE_ID         = os.getenv("STRIPE_PRICE_ID", "")   # your $15/mo price ID
APP_URL                 = os.getenv("APP_URL", "http://localhost:8000")


# ── Checkout ───────────────────────────────────────────────────────────

def create_checkout_session(email: str) -> str:
    """
    Creates a Stripe Checkout Session with a 7-day free trial.
    Returns the checkout URL.
    """
    if not stripe.api_key:
        raise ValueError("STRIPE_SECRET_KEY not set")
    if not STRIPE_PRICE_ID:
        raise ValueError("STRIPE_PRICE_ID not set")

    # Ensure subscriber exists in our DB
    upsert_subscriber(email)
    sub = get_subscriber_by_email(email)

    # Reuse existing Stripe customer if available
    customer_id = sub.get("stripe_customer_id") if sub else None

    if not customer_id:
        customer = stripe.Customer.create(email=email)
        customer_id = customer.id

    session_params = {
        "customer": customer_id,
        "payment_method_types": ["card"],
        "line_items": [{"price": STRIPE_PRICE_ID, "quantity": 1}],
        "mode": "subscription",
        "subscription_data": {
            "trial_period_days": 7,         # <── real Stripe free trial
            "metadata": {"email": email},
        },
        "success_url": f"{APP_URL}/success?session_id={{CHECKOUT_SESSION_ID}}",
        "cancel_url":  f"{APP_URL}/?canceled=true",
        "customer_update": {"address": "auto"},
        "allow_promotion_codes": True,
        "metadata": {"email": email},
    }

    session = stripe.checkout.Session.create(**session_params)
    log.info(f"Checkout session created for {email}: {session.id}")
    return session.url


# ── Webhook handler ────────────────────────────────────────────────────

def handle_webhook(payload: bytes, sig_header: str) -> dict:
    """
    Verify and dispatch Stripe webhook events.
    Returns {"status": "ok"} or raises.
    """
    if not STRIPE_WEBHOOK_SECRET:
        raise ValueError("STRIPE_WEBHOOK_SECRET not set")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except stripe.error.SignatureVerificationError as e:
        log.warning(f"Webhook signature invalid: {e}")
        raise ValueError("Invalid webhook signature")

    event_type = event["type"]
    data_obj   = event["data"]["object"]
    log.info(f"Stripe webhook received: {event_type}")

    # ── Route events ──────────────────────────────────────────────────
    if event_type == "checkout.session.completed":
        _on_checkout_completed(data_obj)

    elif event_type in ("customer.subscription.created", "customer.subscription.updated"):
        _on_subscription_updated(data_obj)

    elif event_type == "customer.subscription.deleted":
        _on_subscription_deleted(data_obj)

    elif event_type in ("invoice.payment_succeeded",):
        _on_payment_succeeded(data_obj)

    elif event_type in ("invoice.payment_failed", "invoice.payment_action_required"):
        _on_payment_failed(data_obj)

    elif event_type == "customer.subscription.trial_will_end":
        log.info(f"Trial ending soon for subscription: {data_obj.get('id')}")
        # Optionally trigger a reminder email here

    else:
        log.debug(f"Unhandled event type: {event_type}")

    return {"status": "ok", "event": event_type}


# ── Event handlers ─────────────────────────────────────────────────────

def _on_checkout_completed(session: dict) -> None:
    """
    Fired when a user completes Stripe Checkout.
    Links customer_id + subscription_id to our subscriber record.
    """
    email           = session.get("customer_details", {}).get("email") or \
                      session.get("metadata", {}).get("email", "")
    customer_id     = session.get("customer")
    subscription_id = session.get("subscription")

    if not email or not customer_id:
        log.warning("checkout.session.completed missing email or customer_id")
        return

    # Fetch subscription details for trial/period info
    trial_end  = None
    period_end = None
    status     = "active"

    if subscription_id:
        try:
            sub        = stripe.Subscription.retrieve(subscription_id)
            status     = sub["status"]
            trial_end  = _ts_to_iso(sub.get("trial_end"))
            period_end = _ts_to_iso(sub.get("current_period_end"))
        except Exception as e:
            log.warning(f"Could not retrieve subscription: {e}")

    # Ensure subscriber row exists, then link Stripe data
    upsert_subscriber(email)
    update_subscriber_stripe(
        email=email,
        customer_id=customer_id,
        subscription_id=subscription_id or "",
        status=status,
        trial_end=trial_end,
        period_end=period_end,
    )
    log.info(f"[OK] Subscriber linked after checkout: {email} | status={status}")


def _on_subscription_updated(sub: dict) -> None:
    """Handle subscription created / updated events."""
    sub_id     = sub.get("id")
    status     = sub.get("status")
    period_end = _ts_to_iso(sub.get("current_period_end"))
    customer_id = sub.get("customer")

    # Also update customer_id link if missing (edge case)
    email = None
    if customer_id:
        existing = get_subscriber_by_stripe_customer(customer_id)
        if not existing:
            # Try to get email from Stripe customer object
            try:
                cust  = stripe.Customer.retrieve(customer_id)
                email = cust.get("email")
                if email:
                    upsert_subscriber(email)
                    update_subscriber_stripe(
                        email=email,
                        customer_id=customer_id,
                        subscription_id=sub_id,
                        status=status,
                        trial_end=_ts_to_iso(sub.get("trial_end")),
                        period_end=period_end,
                    )
                    return
            except Exception as e:
                log.warning(f"Could not retrieve customer: {e}")

    update_subscription_status(sub_id, status, period_end)
    log.info(f"Subscription updated: {sub_id} → {status}")


def _on_subscription_deleted(sub: dict) -> None:
    """Mark subscription as canceled."""
    sub_id = sub.get("id")
    update_subscription_status(sub_id, "canceled")
    log.info(f"Subscription canceled: {sub_id}")


def _on_payment_succeeded(invoice: dict) -> None:
    """Re-activate subscriber after successful payment."""
    sub_id = invoice.get("subscription")
    if sub_id:
        # Fetch fresh subscription period
        try:
            sub    = stripe.Subscription.retrieve(sub_id)
            status = sub["status"]
            period_end = _ts_to_iso(sub.get("current_period_end"))
        except Exception:
            status, period_end = "active", None
        update_subscription_status(sub_id, status, period_end)
        log.info(f"Payment succeeded — subscription reactivated: {sub_id}")


def _on_payment_failed(invoice: dict) -> None:
    """Mark subscriber as past_due if payment fails."""
    sub_id = invoice.get("subscription")
    if sub_id:
        update_subscription_status(sub_id, "past_due")
        log.warning(f"Payment failed — subscription marked past_due: {sub_id}")


# ── Utilities ──────────────────────────────────────────────────────────

def _ts_to_iso(ts: int | None) -> str | None:
    """Convert Unix timestamp to ISO string."""
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def get_customer_portal_url(customer_id: str) -> str:
    """Create a Stripe Customer Portal session for self-service billing."""
    session = stripe.billing_portal.Session.create(
        customer=customer_id,
        return_url=APP_URL,
    )
    return session.url

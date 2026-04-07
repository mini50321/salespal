"""
Bulk messaging stubs — wire real providers via env (Twilio, SendGrid, etc.).
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

log = logging.getLogger(__name__)


def send_sms_stub(to_e164: str, body: str) -> dict[str, Any]:
    """
    SMS via Twilio if TWILIO_ACCOUNT_SID + TWILIO_AUTH_TOKEN + TWILIO_FROM_NUMBER set.
    """
    sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    tok = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    from_n = (os.getenv("TWILIO_FROM_NUMBER") or "").strip()
    if not (sid and tok and from_n):
        return {"provider": "sms_mock", "to": to_e164, "status": "skipped", "body_preview": body[:80]}
    url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
    r = requests.post(
        url,
        data={"To": to_e164, "From": from_n, "Body": body[:1600]},
        auth=(sid, tok),
        timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"twilio sms {r.status_code}: {r.text[:200]}")
    return {"provider": "twilio", "status": "sent"}


def send_email_stub(to: str, subject: str, body: str) -> dict[str, Any]:
    """
    Email via SendGrid if SENDGRID_API_KEY set (simple mail send API).
    """
    key = (os.getenv("SENDGRID_API_KEY") or "").strip()
    from_email = (os.getenv("SENDGRID_FROM_EMAIL") or "").strip()
    if not (key and from_email):
        return {"provider": "email_mock", "to": to, "status": "skipped", "subject": subject[:80]}
    payload = {
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": from_email},
        "subject": subject[:998],
        "content": [{"type": "text/plain", "value": body[:50000]}],
    }
    r = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        json=payload,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"sendgrid {r.status_code}: {r.text[:200]}")
    return {"provider": "sendgrid", "status": "sent"}


def send_rcs_stub(to_e164: str, body: str) -> dict[str, Any]:
    """
    RCS often via same CPaaS as SMS — reuse Twilio or mock.
    """
    rcs = (os.getenv("RCS_USE_TWILIO") or "").lower() in ("1", "true", "yes")
    if rcs:
        return send_sms_stub(to_e164, f"[RCS] {body}")
    return {"provider": "rcs_mock", "to": to_e164, "status": "skipped"}

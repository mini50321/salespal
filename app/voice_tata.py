from __future__ import annotations

import hashlib
import hmac
import os
from typing import Any

import requests
from flask import Request


def verify_webhook(req: Request) -> bool:
    secret = (os.getenv("VOICE_TATA_WEBHOOK_SECRET") or "").strip()
    if not secret:
        return True
    sig = (
        req.headers.get("X-Tata-Signature")
        or req.headers.get("X-Tata-Signature-256")
        or req.headers.get("X-Signature")
        or ""
    ).strip()
    if sig.startswith("sha256="):
        sig = sig[7:].strip()
    if not sig:
        return False
    body = req.get_data()
    want = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig, want)


def _pick(d: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return None


def parse_event(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Best-effort normalization for Tata callback payloads.
    Keeps raw payload for troubleshooting and allows field-map evolution via env.
    """
    event = str(_pick(payload, "event", "type", "event_type") or "").strip().lower() or "unknown"
    call_id = str(_pick(payload, "call_id", "callId", "session_id", "sessionId") or "").strip()
    event_id = str(_pick(payload, "event_id", "eventId", "message_id", "messageId") or "").strip() or call_id
    brand_id = str(_pick(payload, "brand_id", "brandId", "tenant_id") or "").strip()
    from_phone = str(_pick(payload, "from", "from_phone", "caller", "customer_phone") or "").strip()
    transcript = str(_pick(payload, "transcript", "text", "utterance", "speech_text") or "").strip()
    locale = str(_pick(payload, "locale", "language_code", "lang") or "").strip() or None
    audio_url = str(_pick(payload, "audio_url", "recording_url", "recordingUrl") or "").strip() or None
    audio_b64 = str(_pick(payload, "audio_base64", "audioBase64") or "").strip() or None
    audio_mime = str(_pick(payload, "audio_mime_type", "audioMimeType", "mime_type") or "").strip() or None
    return {
        "event": event,
        "call_id": call_id,
        "event_id": event_id,
        "brand_id": brand_id,
        "from_phone": from_phone,
        "transcript": transcript,
        "locale": locale,
        "audio_url": audio_url,
        "audio_base64": audio_b64,
        "audio_mime_type": audio_mime,
        "raw": payload,
    }


def call_outbound(*, to_phone: str, brand_id: str, text: str, tts_payload: dict[str, Any] | None) -> dict[str, Any]:
    """
    Sends outbound call request to Tata if configured.
    If not configured, returns mock success to allow local/dev testing.
    """
    base = (os.getenv("VOICE_TATA_API_BASE") or "").strip().rstrip("/")
    key = (os.getenv("VOICE_TATA_API_KEY") or "").strip()
    if not base or not key:
        return {
            "status": "queued_mock",
            "provider": "tata_mock",
            "to": to_phone,
            "brand_id": brand_id,
            "voice_text": text,
            "tts": tts_payload or {},
        }
    endpoint = (os.getenv("VOICE_TATA_OUTBOUND_PATH") or "/v1/calls").strip()
    url = f"{base}{endpoint if endpoint.startswith('/') else '/' + endpoint}"
    timeout_s = int((os.getenv("VOICE_TATA_TIMEOUT_SECONDS") or "45").strip())
    body: dict[str, Any] = {
        "to": to_phone,
        "brand_id": brand_id,
        "voice_text": text[:2000],
    }
    if isinstance(tts_payload, dict) and tts_payload:
        body["tts"] = tts_payload
    webhook_url = (os.getenv("VOICE_TATA_WEBHOOK_URL") or "").strip()
    if webhook_url:
        body["webhook_url"] = webhook_url
    r = requests.post(
        url,
        json=body,
        timeout=max(5, min(300, timeout_s)),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    )
    if r.status_code >= 400:
        raise RuntimeError(f"tata outbound call failed {r.status_code}: {r.text[:400]}")
    try:
        j = r.json()
    except Exception:
        j = {"raw": r.text}
    out = j if isinstance(j, dict) else {"data": j}
    if "status" not in out:
        out["status"] = "queued"
    out["provider"] = "tata"
    return out


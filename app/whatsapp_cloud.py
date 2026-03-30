from __future__ import annotations

import hashlib
import hmac
import json
import os
from typing import Any

import requests


def verify_subscription(mode: str | None, token: str | None, challenge: str | None, expected: str | None) -> str | None:
    if (mode or "") == "subscribe" and expected and token == expected and challenge:
        return challenge
    return None


def verify_signature(raw_body: bytes, signature_256: str | None, app_secret: str) -> bool:
    if not signature_256 or not signature_256.startswith("sha256="):
        return False
    want = "sha256=" + hmac.new(app_secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(want, signature_256)


def resolve_brand_id(phone_number_id: str) -> str:
    raw = (os.getenv("WHATSAPP_NUMBER_BRAND_MAP") or "").strip()
    default = (os.getenv("WHATSAPP_DEFAULT_BRAND_ID") or "").strip()
    if raw:
        try:
            m = json.loads(raw)
        except json.JSONDecodeError:
            m = {}
        if isinstance(m, dict) and phone_number_id in m:
            bid = str(m[phone_number_id]).strip()
            if bid:
                return bid
    return default


def iter_inbound_text_messages(payload: dict[str, Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for entry in payload.get("entry") or []:
        if not isinstance(entry, dict):
            continue
        for ch in entry.get("changes") or []:
            if not isinstance(ch, dict):
                continue
            val = ch.get("value") or {}
            if not isinstance(val, dict):
                continue
            meta = val.get("metadata") or {}
            pnid = str((meta or {}).get("phone_number_id") or "")
            for msg in val.get("messages") or []:
                if not isinstance(msg, dict):
                    continue
                if msg.get("type") != "text":
                    continue
                text_obj = msg.get("text") or {}
                body = text_obj.get("body") if isinstance(text_obj, dict) else None
                if body is None:
                    continue
                out.append(
                    {
                        "wa_from": str(msg.get("from") or ""),
                        "body": str(body),
                        "phone_number_id": pnid,
                        "message_id": str(msg.get("id") or ""),
                    }
                )
    return out


def send_text(to_wa_id: str, text: str) -> dict[str, Any]:
    pnid = (os.getenv("WHATSAPP_PHONE_NUMBER_ID") or "").strip()
    token = (os.getenv("WHATSAPP_ACCESS_TOKEN") or "").strip()
    ver = (os.getenv("WHATSAPP_GRAPH_VERSION") or "v21.0").strip().strip("/")
    if not pnid or not token:
        raise RuntimeError("WHATSAPP_PHONE_NUMBER_ID and WHATSAPP_ACCESS_TOKEN are required to send")
    to = "".join(c for c in str(to_wa_id) if c.isdigit())
    if not to:
        raise ValueError("invalid whatsapp to id")
    url = f"https://graph.facebook.com/{ver}/{pnid}/messages"
    body: dict[str, Any] = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"preview_url": False, "body": str(text)[:4096]},
    }
    r = requests.post(url, json=body, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    try:
        j = r.json()
    except Exception:
        j = {"raw": r.text}
    if r.status_code >= 400:
        raise RuntimeError(f"whatsapp send failed {r.status_code}: {j}")
    return j if isinstance(j, dict) else {"data": j}


def default_locale() -> str:
    return (os.getenv("WHATSAPP_DEFAULT_LOCALE") or "en").strip().lower()[:8] or "en"

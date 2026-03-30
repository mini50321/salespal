from __future__ import annotations

import hmac
import os
import re
from typing import Any, Callable

from flask import Request

from .conversation_engine import conversation_to_qualification_dict, process_user_message
from .lead_ingest import sanitize_idempotency_key


def is_enabled() -> bool:
    return (os.getenv("PUBLIC_CHAT_ENABLED") or "").lower() in ("1", "true", "yes")


def _allowed_brands() -> set[str]:
    raw = (os.getenv("PUBLIC_CHAT_BRAND_IDS") or "").strip()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _allowed_origins() -> set[str]:
    raw = (os.getenv("PUBLIC_CHAT_CORS_ORIGINS") or "").strip()
    if not raw:
        return set()
    return {x.strip().rstrip("/") for x in raw.split(",") if x.strip()}


def brand_allowed(brand_id: str) -> bool:
    if not is_enabled() or not brand_id:
        return False
    allow = _allowed_brands()
    return brand_id in allow if allow else False


def verify_api_key(req: Request) -> bool:
    if not is_enabled():
        return False
    expected = (os.getenv("PUBLIC_CHAT_API_KEY") or "").strip()
    if not expected:
        return True
    got = (req.headers.get("X-Public-Chat-Key") or "").strip()
    if got and hmac.compare_digest(got, expected):
        return True
    auth = req.headers.get("Authorization") or ""
    if auth.startswith("Bearer "):
        tok = auth[7:].strip()
        return bool(tok) and hmac.compare_digest(tok, expected)
    return False


def cors_header_items(req: Request) -> dict[str, str]:
    origin = (req.headers.get("Origin") or "").strip().rstrip("/")
    if not origin:
        return {}
    allow = _allowed_origins()
    if origin not in allow:
        return {}
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Public-Chat-Key, Authorization",
        "Access-Control-Max-Age": "86400",
    }


def _session_id(raw: Any) -> str | None:
    s = str(raw or "").strip()
    if len(s) < 8 or len(s) > 128:
        return None
    if not re.match(r"^[a-zA-Z0-9._@-]+$", s):
        return None
    return s


def start_session(
    leads: Any,
    conversations: Any,
    body: dict[str, Any],
    *,
    auto_sync_lead: Callable[[Any], None] | None = None,
) -> tuple[dict[str, Any], int]:
    brand_id = str(body.get("brand_id") or "").strip()
    if not brand_allowed(brand_id):
        return {"error": "forbidden"}, 403
    sid = _session_id(body.get("session_id"))
    if not sid:
        return {"error": "invalid session_id"}, 400

    idem = sanitize_idempotency_key(f"pubchat.{sid}")
    if not idem:
        return {"error": "invalid session_id"}, 400

    locale = body.get("locale")
    name = body.get("name")
    email = body.get("email")
    phone = body.get("phone")
    company = body.get("company")
    page_url = body.get("page_url")
    meta_extras: dict[str, Any] = {}
    if isinstance(page_url, str) and page_url.strip():
        meta_extras["page_url"] = page_url.strip()[:2048]
    ref = body.get("referrer")
    if isinstance(ref, str) and ref.strip():
        meta_extras["referrer"] = ref.strip()[:2048]

    raw_embed: dict[str, Any] = {
        "public_embed": True,
        "embed_session_id": sid,
    }
    if meta_extras:
        raw_embed.update(meta_extras)

    lead, _created = leads.upsert(
        brand_id=brand_id,
        source=str(body.get("source") or "web_embed").strip() or "web_embed",
        name=str(name) if name is not None else None,
        email=str(email) if email is not None else None,
        phone=str(phone) if phone is not None else None,
        company=str(company) if company is not None else None,
        message=(str(body.get("message")) if body.get("message") is not None else None),
        utm=body.get("utm") if isinstance(body.get("utm"), dict) else None,
        raw=raw_embed,
        idempotency_key=idem,
    )
    if auto_sync_lead is not None:
        lead = leads.get(lead.id) or lead
        auto_sync_lead(lead)

    conv = conversations.get_latest_for_lead(brand_id, lead.id)
    if conv is None or conv.state == "complete":
        meta: dict[str, Any] = {"public_embed": True, "embed_session_id": sid}
        if meta_extras:
            meta.update(meta_extras)
        conv = conversations.create(
            lead_id=lead.id,
            brand_id=brand_id,
            channel="web",
            locale=str(locale) if locale is not None else None,
            metadata=meta,
        )
    assistant_reply = ""
    if conv.turns:
        assistant_reply = str(conv.turns[-1].get("content") or "")
    return (
        {
            "lead_id": lead.id,
            "conversation_id": conv.id,
            "assistant_reply": assistant_reply,
            "qualification": conversation_to_qualification_dict(conv),
        },
        200,
    )


def post_message(
    conversations: Any,
    conversation_id: str,
    text: str,
    *,
    sync_qualification_to_zoho: Callable[[str, Any], None],
) -> tuple[dict[str, Any], int]:
    conv = conversations.get(conversation_id)
    if not conv:
        return {"error": "not found"}, 404
    meta = conv.metadata if isinstance(conv.metadata, dict) else {}
    if not meta.get("public_embed"):
        return {"error": "not found"}, 404
    conv2, reply = process_user_message(conv, text)
    conversations.update(conv2)
    sync_qualification_to_zoho(conv2.lead_id, conv2)
    return (
        {
            "conversation": conv2.__dict__,
            "assistant_reply": reply,
            "qualification": conversation_to_qualification_dict(conv2),
        },
        200,
    )

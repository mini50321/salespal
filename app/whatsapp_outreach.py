from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .conversation_engine import opening_turn
from .whatsapp_cloud import default_locale, send_template, send_text

log = logging.getLogger(__name__)


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _allowed_statuses() -> set[str]:
    raw = (os.getenv("WHATSAPP_OUTREACH_STATUSES") or "captured,pushed").strip().lower()
    parts = {p.strip() for p in raw.split(",") if p.strip()}
    return parts or {"captured", "pushed"}


def _allowed_sources() -> set[str] | None:
    raw = (os.getenv("WHATSAPP_OUTREACH_SOURCES") or "").strip()
    if not raw:
        return None
    parts = {p.strip().lower() for p in raw.split(",") if p.strip()}
    return parts or None


def _template_body_params_for_lead(lead: Any) -> list[str]:
    raw = (os.getenv("WHATSAPP_OUTREACH_TEMPLATE_BODY_JSON") or "").strip()
    if raw:
        try:
            x = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if isinstance(x, list):
            return [str(s) for s in x][:20]
        return []
    mode = (os.getenv("WHATSAPP_OUTREACH_TEMPLATE_PARAM_MODE") or "").strip().lower()
    if mode == "lead_name":
        n = (getattr(lead, "name", None) or "").strip() or "there"
        return [n[:1024]]
    return []


def _outreach_skip_reason(lead: Any, conversations: Any, ignore_marker: bool) -> str | None:
    phone = getattr(lead, "phone", None)
    if not phone:
        return "no_phone"
    raw = lead.raw if isinstance(getattr(lead, "raw", None), dict) else {}
    if not ignore_marker and raw.get("wa_proactive_outreach_at"):
        return "already_outreached"
    st = str(getattr(lead, "status", "") or "")
    if st not in _allowed_statuses():
        return "status_not_eligible"
    src_allow = _allowed_sources()
    if src_allow is not None:
        src = str(getattr(lead, "source", "") or "").strip().lower()
        if src not in src_allow:
            return "source_not_eligible"
    bid = str(getattr(lead, "brand_id", "") or "")
    lid = str(getattr(lead, "id", "") or "")
    if not bid or not lid:
        return "invalid_lead"
    try:
        conv = conversations.get_latest_for_lead(bid, lid)
    except Exception:
        conv = None
    if conv is not None and getattr(conv, "state", "") != "complete":
        return "active_conversation"
    return None


def outreach_one_lead(
    leads: Any,
    conversations: Any,
    lead_id: str,
    *,
    auto_sync_lead: Callable[[Any], None] | None,
    ignore_outreach_marker: bool = False,
) -> dict[str, Any]:
    lead = leads.get(lead_id)
    if not lead:
        return {"lead_id": lead_id, "status": "skipped", "reason": "not_found"}
    reason = _outreach_skip_reason(lead, conversations, ignore_outreach_marker)
    if reason:
        return {"lead_id": lead_id, "status": "skipped", "reason": reason}
    if auto_sync_lead is not None:
        try:
            auto_sync_lead(lead)
            lead = leads.get(lead_id) or lead
        except Exception:
            log.exception("outreach auto_sync_lead")

    loc = default_locale()
    brand_id = str(lead.brand_id)
    ot = opening_turn(loc, "whatsapp")
    opening = str((ot.get("turn") or {}).get("content") or "")
    to = str(lead.phone)
    template = (os.getenv("WHATSAPP_OUTREACH_TEMPLATE") or "").strip()
    try:
        if template:
            lang = (os.getenv("WHATSAPP_OUTREACH_TEMPLATE_LANG") or "en_US").strip()
            params = _template_body_params_for_lead(lead)
            send_template(to, template, lang, params if params else None)
        else:
            if not opening:
                return {"lead_id": lead_id, "status": "failed", "error": "empty opening"}
            send_text(to, opening)
    except Exception as e:
        log.exception("whatsapp outreach")
        return {"lead_id": lead_id, "status": "failed", "error": str(e)}

    try:
        conv = conversations.create(
            lead_id=lead.id,
            brand_id=brand_id,
            channel="whatsapp",
            locale=loc,
            metadata={"wa_proactive": True},
        )
    except Exception as e:
        log.exception("whatsapp outreach conversation create")
        return {"lead_id": lead_id, "status": "failed", "error": f"sent but persist failed: {e}"}

    patch: dict[str, Any] = {
        "wa_proactive_outreach_at": _utc(),
        "wa_proactive_conversation_id": conv.id,
    }
    template_used = (os.getenv("WHATSAPP_OUTREACH_TEMPLATE") or "").strip()
    if template_used:
        patch["wa_proactive_template"] = template_used
    leads.merge_raw(lead.id, patch)
    return {
        "lead_id": lead_id,
        "status": "sent",
        "conversation_id": conv.id,
        "delivery": "template" if template_used else "text",
    }


def batch_whatsapp_outreach(
    leads: Any,
    conversations: Any,
    *,
    brand_id: str | None,
    limit: int,
    auto_sync_lead: Callable[[Any], None] | None,
    ignore_outreach_marker: bool = False,
) -> dict[str, Any]:
    if limit < 1 or limit > 200:
        return {"error": "invalid limit", "attempted": 0, "sent": 0, "skipped": 0, "failed": 0, "results": []}
    statuses = _allowed_statuses()
    try:
        items = leads.list(brand_id=brand_id, status=None)
    except TypeError:
        items = leads.list(brand_id=brand_id)
    src_allow = _allowed_sources()
    candidates: list[Any] = []
    for l in items:
        if str(getattr(l, "status", "")) not in statuses:
            continue
        if not getattr(l, "phone", None):
            continue
        if src_allow is not None:
            src = str(getattr(l, "source", "") or "").strip().lower()
            if src not in src_allow:
                continue
        candidates.append(l)
    results: list[dict[str, Any]] = []
    sent = skipped = failed = 0
    for lead in candidates[:limit]:
        out = outreach_one_lead(
            leads,
            conversations,
            lead.id,
            auto_sync_lead=auto_sync_lead,
            ignore_outreach_marker=ignore_outreach_marker,
        )
        results.append(out)
        st = out.get("status")
        if st == "sent":
            sent += 1
        elif st == "skipped":
            skipped += 1
        elif st == "failed":
            failed += 1
    return {
        "attempted": len(results),
        "sent": sent,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }

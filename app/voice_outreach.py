from __future__ import annotations

import logging
import os
from collections.abc import Callable
from datetime import date, datetime, timezone
from typing import Any

from .conversation_engine import opening_turn
from .salespal_scheduling import (
    compute_voice_retry_fire_utc,
    default_tz_name,
    local_date,
)
from .voice_tata import call_outbound
from .voice_tts import synthesize

log = logging.getLogger(__name__)


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _allowed_statuses() -> set[str]:
    raw = (os.getenv("VOICE_OUTREACH_STATUSES") or "captured,pushed").strip().lower()
    parts = {p.strip() for p in raw.split(",") if p.strip()}
    return parts or {"captured", "pushed"}


def _allowed_sources() -> set[str] | None:
    raw = (os.getenv("VOICE_OUTREACH_SOURCES") or "").strip()
    if not raw:
        return None
    parts = {p.strip().lower() for p in raw.split(",") if p.strip()}
    return parts or None


def _max_attempts() -> int:
    raw = (os.getenv("VOICE_MAX_ATTEMPTS") or "6").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 6
    return max(1, min(20, n))


def _lead_tz(raw: dict[str, Any]) -> str:
    return str(raw.get("sales_timezone") or default_tz_name())


def _parse_date(s: str) -> date | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _retry_due(raw: dict[str, Any]) -> bool:
    nxt = str(raw.get("voice_outreach_next_try_at") or "").strip()
    if not nxt:
        return False
    try:
        at = datetime.fromisoformat(nxt.replace("Z", "+00:00"))
        return at <= _now()
    except Exception:
        return False


def _outreach_skip_reason(lead: Any, conversations: Any, ignore_marker: bool) -> str | None:
    phone = getattr(lead, "phone", None)
    if not phone:
        return "no_phone"
    raw = lead.raw if isinstance(getattr(lead, "raw", None), dict) else {}
    completed = int(raw.get("voice_retry_completed_steps") or 0)
    if completed >= _max_attempts():
        return "max_attempts_reached"
    nxt_idx = int(raw.get("voice_next_step_index") or 0)
    if nxt_idx >= 6:
        return "max_attempts_reached"

    retry_due = _retry_due(raw)

    if not ignore_marker and raw.get("voice_proactive_outreach_at"):
        nxt = str(raw.get("voice_outreach_next_try_at") or "").strip()
        if nxt:
            try:
                at = datetime.fromisoformat(nxt.replace("Z", "+00:00"))
                if at > _now():
                    return "already_outreached"
            except Exception:
                return "already_outreached"
        else:
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
        if not retry_due:
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
            log.exception("voice outreach auto_sync_lead")

    raw = lead.raw if isinstance(getattr(lead, "raw", None), dict) else {}
    tz = _lead_tz(raw)
    now = _now()
    anchor = _parse_date(str(raw.get("sales_sequence_anchor_local_date") or ""))
    if anchor is None:
        anchor = local_date(now, tz)
    step_to_run = int(raw.get("voice_next_step_index") or 0)
    if step_to_run < 0:
        step_to_run = 0
    if step_to_run > 5:
        return {"lead_id": lead_id, "status": "skipped", "reason": "max_attempts_reached"}

    brand_id = str(lead.brand_id)
    phone = str(lead.phone)
    locale = (os.getenv("VOICE_DEFAULT_LOCALE") or "hing").strip()
    ot = opening_turn(locale, "voice")
    opening = str((ot.get("turn") or {}).get("content") or "")
    if not opening:
        return {"lead_id": lead_id, "status": "failed", "error": "empty opening"}
    tts_payload = synthesize(opening)

    try:
        call_resp = call_outbound(to_phone=phone, brand_id=brand_id, text=opening, tts_payload=tts_payload)
    except Exception as e:
        log.exception("voice outreach call")
        return {"lead_id": lead_id, "status": "failed", "error": str(e)}

    try:
        conv = conversations.create(
            lead_id=lead.id,
            brand_id=brand_id,
            channel="voice",
            locale=locale,
            metadata={"voice_proactive": True, "voice_call_init": call_resp, "voice_step": step_to_run},
        )
    except Exception as e:
        log.exception("voice outreach conversation create")
        return {"lead_id": lead_id, "status": "failed", "error": f"call queued but persist failed: {e}"}

    completed = int(raw.get("voice_retry_completed_steps") or 0) + 1
    prev_fire = raw.get("voice_last_call_utc")
    prev_utc = now
    if isinstance(prev_fire, str) and prev_fire.strip():
        try:
            prev_utc = datetime.fromisoformat(prev_fire.replace("Z", "+00:00"))
        except Exception:
            prev_utc = now

    next_step = step_to_run + 1
    patch: dict[str, Any] = {
        "voice_proactive_outreach_at": raw.get("voice_proactive_outreach_at") or _utc_iso(),
        "voice_proactive_conversation_id": conv.id,
        "voice_last_call_response": call_resp,
        "voice_last_call_utc": now.isoformat(),
        "voice_retry_completed_steps": completed,
        "voice_last_voice_step": step_to_run,
    }

    if completed >= _max_attempts() or next_step > 5:
        patch["voice_next_step_index"] = 6
        patch["voice_outreach_next_try_at"] = None
    else:
        nxt = compute_voice_retry_fire_utc(
            step_index=next_step,
            sequence_start_local_date=anchor,
            previous_fire_utc=prev_utc,
            now_utc=now,
            tz_name=tz,
        )
        if nxt is not None and nxt < now:
            nxt = now
        patch["voice_next_step_index"] = next_step
        patch["voice_outreach_next_try_at"] = nxt.isoformat() if nxt else None
    if raw.get("sales_sequence_anchor_local_date") is None:
        patch["sales_sequence_anchor_local_date"] = anchor.isoformat()
    if raw.get("sales_timezone") is None:
        patch["sales_timezone"] = tz

    leads.merge_raw(lead.id, patch)
    return {
        "lead_id": lead_id,
        "status": "queued",
        "conversation_id": conv.id,
        "delivery": "voice",
        "provider": str(call_resp.get("provider") or "tata"),
        "voice_step": step_to_run,
        "attempt": completed,
    }


def batch_voice_outreach(
    leads: Any,
    conversations: Any,
    *,
    brand_id: str | None,
    limit: int,
    auto_sync_lead: Callable[[Any], None] | None,
    ignore_outreach_marker: bool = False,
    retry_only: bool = False,
) -> dict[str, Any]:
    if limit < 1 or limit > 200:
        return {"error": "invalid limit", "attempted": 0, "queued": 0, "skipped": 0, "failed": 0, "results": []}
    statuses = _allowed_statuses()
    try:
        items = leads.list(brand_id=brand_id, status=None)
    except TypeError:
        items = leads.list(brand_id=brand_id)
    src_allow = _allowed_sources()
    cron_only = retry_only or (os.getenv("VOICE_CRON_RETRY_ONLY") or "").lower() in ("1", "true", "yes")
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
        raw = l.raw if isinstance(getattr(l, "raw", None), dict) else {}
        if cron_only:
            if not _retry_due(raw):
                continue
        candidates.append(l)
    results: list[dict[str, Any]] = []
    queued = skipped = failed = 0
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
        if st == "queued":
            queued += 1
        elif st == "skipped":
            skipped += 1
        elif st == "failed":
            failed += 1
    return {
        "attempted": len(results),
        "queued": queued,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }

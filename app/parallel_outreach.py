"""
Start voice + WhatsApp together; initialize sequence anchors for scheduling engine.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .conversation_engine import opening_turn
from .salespal_scheduling import (
    compute_voice_retry_fire_utc,
    default_tz_name,
    local_date,
    utc_now,
    whatsapp_nurture_touches,
)
from .voice_tata import call_outbound
from .voice_tts import synthesize
from .whatsapp_cloud import default_locale, send_text as whatsapp_send_text

log = logging.getLogger(__name__)


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def start_parallel_outreach(
    leads: Any,
    conversations: Any,
    lead_id: str,
    *,
    tz_name: str | None,
    auto_sync_lead: Callable[[Any], None] | None,
    brand_id_override: str | None = None,
) -> dict[str, Any]:
    lead = leads.get(lead_id)
    if not lead:
        return {"lead_id": lead_id, "status": "error", "reason": "not_found"}
    phone = getattr(lead, "phone", None)
    if not phone:
        return {"lead_id": lead_id, "status": "error", "reason": "no_phone"}

    tz = (tz_name or "").strip() or default_tz_name()
    brand_id = brand_id_override or str(lead.brand_id)
    now = utc_now()
    anchor_date = local_date(now, tz)
    enroll_iso = now.isoformat()

    if auto_sync_lead is not None:
        try:
            auto_sync_lead(lead)
            lead = leads.get(lead_id) or lead
        except Exception:
            log.exception("parallel auto_sync_lead")

    loc = default_locale()
    ot = opening_turn(loc, "whatsapp")
    opening_wa = str((ot.get("turn") or {}).get("content") or "")
    ot_v = opening_turn(os.getenv("VOICE_DEFAULT_LOCALE") or "hing", "voice")
    opening_voice = str((ot_v.get("turn") or {}).get("content") or "")

    wa_err = None
    try:
        whatsapp_send_text(str(phone), opening_wa[:4096])
    except Exception as e:
        log.exception("parallel whatsapp")
        wa_err = str(e)

    tts_payload = synthesize(opening_voice)
    call_err = None
    call_resp: dict[str, Any] = {}
    try:
        call_resp = call_outbound(
            to_phone=str(phone), brand_id=brand_id, text=opening_voice, tts_payload=tts_payload
        )
    except Exception as e:
        log.exception("parallel voice")
        call_err = str(e)

    conv_wa = conversations.create(
        lead_id=lead.id,
        brand_id=brand_id,
        channel="whatsapp",
        locale=loc,
        metadata={"parallel_start": True, "sequence": "sales360"},
    )
    conv_v = conversations.create(
        lead_id=lead.id,
        brand_id=brand_id,
        channel="voice",
        locale=os.getenv("VOICE_DEFAULT_LOCALE") or "hing",
        metadata={"parallel_start": True, "voice_call_init": call_resp},
    )

    v_next = compute_voice_retry_fire_utc(
        step_index=1,
        sequence_start_local_date=anchor_date,
        previous_fire_utc=now,
        now_utc=now,
        tz_name=tz,
    )
    touches = whatsapp_nurture_touches()
    wa_next = None
    if len(touches) > 1:
        from .salespal_scheduling import compute_whatsapp_nurture_fire_utc

        wa_next = compute_whatsapp_nurture_fire_utc(
            touch_index=1,
            enroll_local_date=anchor_date,
            enroll_utc=now,
            tz_name=tz,
        )

    patch: dict[str, Any] = {
        "sales_parallel_started_at": enroll_iso,
        "sales_sequence_anchor_local_date": anchor_date.isoformat(),
        "sales_timezone": tz,
        "voice_proactive_outreach_at": enroll_iso,
        "voice_retry_completed_steps": 1,
        "voice_next_step_index": 1,
        "voice_last_call_utc": enroll_iso,
        "voice_outreach_next_try_at": v_next.isoformat() if v_next else None,
        "wa_nurture_enrolled_at": enroll_iso,
        "wa_nurture_touch_index": 1,
        "wa_nurture_next_touch_at": wa_next.isoformat() if wa_next else None,
        "parallel_wa_conversation_id": conv_wa.id,
        "parallel_voice_conversation_id": conv_v.id,
    }
    if wa_err:
        patch["parallel_whatsapp_error"] = wa_err
    if call_err:
        patch["parallel_voice_error"] = call_err

    leads.merge_raw(lead.id, patch)

    return {
        "lead_id": lead_id,
        "status": "started",
        "timezone": tz,
        "whatsapp": {"sent": wa_err is None, "error": wa_err, "conversation_id": conv_wa.id},
        "voice": {"queued": call_err is None, "error": call_err, "conversation_id": conv_v.id, "provider": call_resp.get("provider")},
        "next_voice_retry_utc": patch.get("voice_outreach_next_try_at"),
        "next_whatsapp_nurture_utc": patch.get("wa_nurture_next_touch_at"),
    }

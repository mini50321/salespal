"""
Scheduled WhatsApp nurture (Day 1,3,5,7 after parallel start — Day 0 handled at parallel start).
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from datetime import datetime
from typing import Any

from .conversation_engine import opening_turn
from .salespal_scheduling import (
    compute_whatsapp_nurture_fire_utc,
    default_tz_name,
    local_date,
    utc_now,
    whatsapp_nurture_touches,
)
from .whatsapp_cloud import default_locale, send_text as whatsapp_send_text

log = logging.getLogger(__name__)


def _nurture_body(touch_index: int, locale: str) -> str:
    """Touch 0 is Day 0 (handled at parallel start); cron sends touch_index >= 1."""
    raw = (os.getenv("WHATSAPP_NURTURE_TOUCH_BODIES_JSON") or "").strip()
    if raw:
        try:
            import json

            arr = json.loads(raw)
            if isinstance(arr, list) and 0 <= touch_index < len(arr):
                return str(arr[touch_index])[:4096]
        except Exception:
            log.exception("WHATSAPP_NURTURE_TOUCH_BODIES_JSON")
    ot = opening_turn(locale, "whatsapp")
    return str((ot.get("turn") or {}).get("content") or "")[:4096]


def process_due_nurtures(
    leads: Any,
    *,
    brand_id: str | None,
    limit: int,
    auto_sync_lead: Callable[[Any], None] | None,
) -> dict[str, Any]:
    now = utc_now()
    try:
        items = leads.list(brand_id=brand_id, status=None)
    except TypeError:
        items = leads.list(brand_id=brand_id)
    results: list[dict[str, Any]] = []
    sent = skipped = 0
    touches = whatsapp_nurture_touches()
    for lead in items:
        if len(results) >= limit:
            break
        raw = lead.raw if isinstance(getattr(lead, "raw", None), dict) else {}
        if not raw.get("wa_nurture_enrolled_at"):
            continue
        nxt = str(raw.get("wa_nurture_next_touch_at") or "").strip()
        if not nxt:
            continue
        try:
            due = datetime.fromisoformat(nxt.replace("Z", "+00:00"))
        except Exception:
            continue
        if due > now:
            continue
        idx = int(raw.get("wa_nurture_touch_index") or 0)
        if idx < 0 or idx >= len(touches):
            skipped += 1
            continue
        phone = getattr(lead, "phone", None)
        if not phone:
            skipped += 1
            continue
        tz = str(raw.get("sales_timezone") or default_tz_name())
        enroll_iso = str(raw.get("wa_nurture_enrolled_at") or "")
        try:
            enroll_utc = datetime.fromisoformat(enroll_iso.replace("Z", "+00:00"))
        except Exception:
            enroll_utc = now
        enroll_date = local_date(enroll_utc, tz)

        if auto_sync_lead is not None:
            try:
                auto_sync_lead(lead)
                lead = leads.get(lead.id) or lead
                raw = lead.raw if isinstance(getattr(lead, "raw", None), dict) else {}
            except Exception:
                log.exception("wa nurture sync")

        loc = default_locale()
        text = _nurture_body(idx, loc)
        suffix = os.getenv("WHATSAPP_NURTURE_MESSAGE_SUFFIX") or ""
        if suffix:
            text = f"{text}\n{suffix}"[:4096]
        try:
            whatsapp_send_text(str(phone), text)
        except Exception as e:
            log.exception("wa nurture send")
            results.append({"lead_id": lead.id, "status": "failed", "error": str(e)})
            continue

        next_idx = idx + 1
        next_fire = None
        if next_idx < len(touches):
            next_fire = compute_whatsapp_nurture_fire_utc(
                touch_index=next_idx,
                enroll_local_date=enroll_date,
                enroll_utc=enroll_utc,
                tz_name=tz,
            )
        patch: dict[str, Any] = {
            "wa_nurture_touch_index": next_idx,
            "wa_nurture_last_sent_at": now.isoformat(),
            "wa_nurture_next_touch_at": next_fire.isoformat() if next_fire else None,
        }
        leads.merge_raw(lead.id, patch)
        results.append({"lead_id": lead.id, "status": "sent", "touch_index": idx})
        sent += 1

    return {"processed": len(results), "sent": sent, "skipped": skipped, "results": results}

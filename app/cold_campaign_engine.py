"""
4-week cold campaign: Tue WA, Fri SMS, Wed Email, Sun WA (timezone-aware).
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from datetime import datetime
from typing import Any

from .broadcast_providers import send_email_stub, send_rcs_stub, send_sms_stub
from .salespal_scheduling import (
    cold_campaign_steps,
    compute_cold_campaign_anchor_week_start,
    compute_cold_campaign_fire_utc,
    default_tz_name,
    local_date,
    utc_now,
)
from .whatsapp_cloud import send_text as whatsapp_send_text

log = logging.getLogger(__name__)


def enroll_cold_lead(leads: Any, lead_id: str, tz_name: str | None) -> dict[str, Any]:
    lead = leads.get(lead_id)
    if not lead:
        return {"status": "error", "reason": "not_found"}
    tz = (tz_name or default_tz_name()).strip()
    now = utc_now()
    d = local_date(now, tz)
    monday = compute_cold_campaign_anchor_week_start(d, tz)
    patch = {
        "cold_campaign_enrolled_at": now.isoformat(),
        "cold_campaign_timezone": tz,
        "cold_campaign_anchor_monday": monday.isoformat(),
        "cold_campaign_step_index": 0,
        "sales_lead_temperature": "cold",
    }
    step0 = compute_cold_campaign_fire_utc(step_index=0, campaign_anchor_monday=monday, tz_name=tz)
    patch["cold_campaign_next_fire_at"] = step0.isoformat() if step0 else None
    leads.merge_raw(lead.id, patch)
    return {"lead_id": lead_id, "status": "enrolled", "next_fire_utc": patch.get("cold_campaign_next_fire_at")}


def process_due_cold_campaigns(
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
    for lead in items:
        if len(results) >= limit:
            break
        raw = lead.raw if isinstance(getattr(lead, "raw", None), dict) else {}
        if not raw.get("cold_campaign_enrolled_at"):
            continue
        nxt = str(raw.get("cold_campaign_next_fire_at") or "").strip()
        if not nxt:
            continue
        try:
            due = datetime.fromisoformat(nxt.replace("Z", "+00:00"))
        except Exception:
            continue
        if due > now:
            continue
        idx = int(raw.get("cold_campaign_step_index") or 0)
        steps = cold_campaign_steps()
        if idx >= len(steps):
            continue
        phone = getattr(lead, "phone", None)
        email = getattr(lead, "email", None)
        tz = str(raw.get("cold_campaign_timezone") or default_tz_name())
        monday_s = str(raw.get("cold_campaign_anchor_monday") or "")
        try:
            from datetime import date as date_cls

            anchor_monday = date_cls.fromisoformat(monday_s) if monday_s else compute_cold_campaign_anchor_week_start(local_date(now, tz), tz)
        except Exception:
            anchor_monday = compute_cold_campaign_anchor_week_start(local_date(now, tz), tz)

        if auto_sync_lead is not None:
            try:
                auto_sync_lead(lead)
            except Exception:
                log.exception("cold campaign sync")

        step = steps[idx]
        msg = f"SalesPal update for {lead.brand_id} — stay connected with us."
        ch = step.channel
        err = None
        try:
            if ch == "whatsapp" and phone:
                whatsapp_send_text(str(phone), msg[:4096])
            elif ch == "sms" and phone:
                send_sms_stub(str(phone), msg)
            elif ch == "email" and email:
                send_email_stub(str(email), "SalesPal", msg)
            elif ch == "rcs" and phone:
                send_rcs_stub(str(phone), msg)
            else:
                err = f"missing_destination_for_{ch}"
        except Exception as e:
            err = str(e)
            log.exception("cold campaign send")

        next_idx = idx + 1
        next_fire = None
        if next_idx < len(steps):
            next_fire = compute_cold_campaign_fire_utc(
                step_index=next_idx, campaign_anchor_monday=anchor_monday, tz_name=tz
            )
        patch: dict[str, Any] = {
            "cold_campaign_step_index": next_idx,
            "cold_campaign_last_sent_at": now.isoformat(),
            "cold_campaign_last_channel": ch,
            "cold_campaign_next_fire_at": next_fire.isoformat() if next_fire else None,
        }
        if err:
            patch["cold_campaign_last_error"] = err
        leads.merge_raw(lead.id, patch)
        results.append(
            {"lead_id": lead.id, "status": "sent" if not err else "failed", "channel": ch, "error": err}
        )

    return {"processed": len(results), "results": results}

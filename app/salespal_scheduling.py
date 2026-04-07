"""
Timezone-aware schedules for SalesPal360:
- Voice call retry (6 steps, 9 AM–9 PM local)
- WhatsApp nurture (Day 0–7)
- Cold campaign (4-week multi-channel)

All times use IANA timezone names (e.g. Asia/Kolkata). Default from env LEAD_DEFAULT_TIMEZONE.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

UTC = timezone.utc


def default_tz_name() -> str:
    return (os.getenv("LEAD_DEFAULT_TIMEZONE") or "Asia/Kolkata").strip() or "Asia/Kolkata"


def _zi(tz_name: str) -> ZoneInfo:
    return ZoneInfo((tz_name or default_tz_name()).strip() or default_tz_name())


def utc_now() -> datetime:
    return datetime.now(UTC)


def to_utc(local: datetime, tz_name: str) -> datetime:
    if local.tzinfo is None:
        local = local.replace(tzinfo=_zi(tz_name))
    return local.astimezone(UTC)


def local_date(utc_dt: datetime, tz_name: str) -> date:
    return utc_dt.astimezone(_zi(tz_name)).date()


def at_local_clock(d: date, hour: int, minute: int, tz_name: str) -> datetime:
    zi = _zi(tz_name)
    return datetime(d.year, d.month, d.day, hour, minute, 0, tzinfo=zi).astimezone(UTC)


CALL_WINDOW_START = (9, 0)
CALL_WINDOW_END = (21, 0)  # exclusive upper bound for "before 21:00" — last slot 20:59


def clamp_to_call_window(utc_dt: datetime, tz_name: str) -> datetime:
    """
    Move utc_dt to the next moment inside [09:00, 21:00) local if outside.
    """
    zi = _zi(tz_name)
    loc = utc_dt.astimezone(zi)
    h, m = loc.hour, loc.minute
    start_m = CALL_WINDOW_START[0] * 60 + CALL_WINDOW_START[1]
    end_m = CALL_WINDOW_END[0] * 60 + CALL_WINDOW_END[1]  # 21:00 exclusive
    cur_m = h * 60 + m
    if start_m <= cur_m < end_m:
        return utc_dt
    if cur_m < start_m:
        new_loc = loc.replace(hour=CALL_WINDOW_START[0], minute=CALL_WINDOW_START[1], second=0, microsecond=0)
        return new_loc.astimezone(UTC)
    # at or after 21:00 → next day 09:00
    next_day = loc.date() + timedelta(days=1)
    new_loc = datetime(
        next_day.year, next_day.month, next_day.day,
        CALL_WINDOW_START[0], CALL_WINDOW_START[1], 0,
        tzinfo=zi,
    )
    return new_loc.astimezone(UTC)


# --- Voice retry: 6 steps (0..5) ---
# 1 immediate, 2 +2h, 3 same sequence-start-day 18:30, 4 start+1d 11:00, 5 start+2d 18:30, 6 start+4d 12:00


def compute_voice_retry_fire_utc(
    *,
    step_index: int,
    sequence_start_local_date: date,
    previous_fire_utc: datetime | None,
    now_utc: datetime,
    tz_name: str,
) -> datetime | None:
    """
    step_index 0..5. Returns UTC instant for this attempt (may be in past if overdue — caller should max with now).
    """
    zi = _zi(tz_name)
    if step_index < 0 or step_index > 5:
        return None

    if step_index == 0:
        return clamp_to_call_window(now_utc, tz_name)

    if step_index == 1:
        base = previous_fire_utc or now_utc
        return clamp_to_call_window(base + timedelta(hours=2), tz_name)

    if step_index == 2:
        t = at_local_clock(sequence_start_local_date, 18, 30, tz_name)
        if t < now_utc:
            t = at_local_clock(sequence_start_local_date + timedelta(days=1), 18, 30, tz_name)
        return clamp_to_call_window(max(t, now_utc), tz_name)

    if step_index == 3:
        d = sequence_start_local_date + timedelta(days=1)
        t = at_local_clock(d, 11, 0, tz_name)
        return clamp_to_call_window(max(t, now_utc), tz_name)

    if step_index == 4:
        d = sequence_start_local_date + timedelta(days=2)
        t = at_local_clock(d, 18, 30, tz_name)
        return clamp_to_call_window(max(t, now_utc), tz_name)

    if step_index == 5:
        d = sequence_start_local_date + timedelta(days=4)
        t = at_local_clock(d, 12, 0, tz_name)
        return clamp_to_call_window(max(t, now_utc), tz_name)

    return None


# --- WhatsApp nurture: Day 0..7 ---


@dataclass(frozen=True)
class WhatsAppNurtureTouch:
    day_offset: int
    hour: int
    minute: int
    within_minutes: int | None  # only for day 0


def whatsapp_nurture_touches() -> list[WhatsAppNurtureTouch]:
    return [
        WhatsAppNurtureTouch(0, 0, 0, within_minutes=5),
        WhatsAppNurtureTouch(1, 11, 0, None),
        WhatsAppNurtureTouch(3, 18, 30, None),
        WhatsAppNurtureTouch(5, 12, 0, None),
        WhatsAppNurtureTouch(7, 17, 0, None),
    ]


def compute_whatsapp_nurture_fire_utc(
    *,
    touch_index: int,
    enroll_local_date: date,
    enroll_utc: datetime,
    tz_name: str,
) -> datetime | None:
    touches = whatsapp_nurture_touches()
    if touch_index < 0 or touch_index >= len(touches):
        return None
    t = touches[touch_index]
    zi = _zi(tz_name)
    day = enroll_local_date + timedelta(days=t.day_offset)
    if t.within_minutes is not None:
        return enroll_utc + timedelta(minutes=t.within_minutes)
    return at_local_clock(day, t.hour, t.minute, tz_name)


# --- Cold campaign: 4 weeks ---


@dataclass(frozen=True)
class ColdCampaignStep:
    week_index: int  # 0..3
    weekday: int  # Monday=0 .. Sunday=6
    hour: int
    minute: int
    channel: str  # whatsapp | sms | email


def cold_campaign_steps() -> list[ColdCampaignStep]:
    return [
        ColdCampaignStep(0, 1, 11, 30, "whatsapp"),  # Tue
        ColdCampaignStep(1, 4, 18, 0, "sms"),  # Fri
        ColdCampaignStep(2, 2, 13, 0, "email"),  # Wed
        ColdCampaignStep(3, 6, 11, 0, "whatsapp"),  # Sun
    ]


def _next_weekday_on_or_after(d: date, target_weekday: int) -> date:
    # Monday=0
    delta = (target_weekday - d.weekday()) % 7
    return d + timedelta(days=delta)


def compute_cold_campaign_anchor_week_start(enroll_local_date: date, tz_name: str) -> date:
    """Week 1 starts on the Monday of the week containing enroll date (or next Monday if preferred)."""
    # Align to Monday of current week
    wd = enroll_local_date.weekday()
    monday = enroll_local_date - timedelta(days=wd)
    return monday


def compute_cold_campaign_fire_utc(
    *,
    step_index: int,
    campaign_anchor_monday: date,
    tz_name: str,
) -> datetime | None:
    steps = cold_campaign_steps()
    if step_index < 0 or step_index >= len(steps):
        return None
    s = steps[step_index]
    week_start = campaign_anchor_monday + timedelta(weeks=s.week_index)
    day = _next_weekday_on_or_after(week_start, s.weekday)
    return at_local_clock(day, s.hour, s.minute, tz_name)


def ai_timing_adjustment_minutes(lead_raw: dict[str, Any], channel: str) -> int:
    """
    Placeholder for "AI optimize timing": read optional offsets from lead.raw set by offline job or future model.
    """
    key = f"ai_timing_offset_minutes_{channel}"
    v = lead_raw.get(key)
    if isinstance(v, (int, float)):
        return int(max(-120, min(120, v)))
    return 0

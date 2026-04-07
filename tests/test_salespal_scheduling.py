from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from zoneinfo import ZoneInfo

from app.salespal_scheduling import (
    compute_cold_campaign_fire_utc,
    compute_voice_retry_fire_utc,
    compute_whatsapp_nurture_fire_utc,
    utc_now,
)


def test_voice_retry_six_steps_monotonic():
    tz = "Asia/Kolkata"
    d0 = date(2026, 4, 7)
    t0 = datetime(2026, 4, 7, 10, 0, tzinfo=timezone.utc)
    s0 = compute_voice_retry_fire_utc(
        step_index=0, sequence_start_local_date=d0, previous_fire_utc=None, now_utc=t0, tz_name=tz
    )
    assert s0 is not None
    s1 = compute_voice_retry_fire_utc(
        step_index=1, sequence_start_local_date=d0, previous_fire_utc=s0, now_utc=s0, tz_name=tz
    )
    assert s1 is not None and s1 > s0
    s2 = compute_voice_retry_fire_utc(
        step_index=2, sequence_start_local_date=d0, previous_fire_utc=s1, now_utc=s1, tz_name=tz
    )
    assert s2 is not None


def test_whatsapp_nurture_day0_offset():
    tz = "Asia/Kolkata"
    enroll = datetime(2026, 4, 7, 10, 0, tzinfo=timezone.utc)
    d = date(2026, 4, 7)
    t0 = compute_whatsapp_nurture_fire_utc(
        touch_index=0, enroll_local_date=d, enroll_utc=enroll, tz_name=tz
    )
    assert t0 == enroll + timedelta(minutes=5)


def test_cold_campaign_week1_tuesday():
    tz = "Asia/Kolkata"
    monday = date(2026, 4, 6)
    fire = compute_cold_campaign_fire_utc(step_index=0, campaign_anchor_monday=monday, tz_name=tz)
    assert fire is not None
    local = fire.astimezone(ZoneInfo(tz))
    assert local.weekday() == 1
    assert local.hour == 11 and local.minute == 30


def test_utc_now_naive_utc():
    assert utc_now().tzinfo == timezone.utc

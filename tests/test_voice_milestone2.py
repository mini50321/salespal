from __future__ import annotations

import importlib
import os
from pathlib import Path


def _build_client(tmp_path: Path):
    os.environ["STORE_BACKEND"] = "json"
    os.environ["LEAD_STORE_PATH"] = str(tmp_path / "lead_store.json")
    os.environ["JOB_STORE_PATH"] = str(tmp_path / "job_store.json")
    os.environ["POST_STORE_PATH"] = str(tmp_path / "post_store.json")
    os.environ["CONVERSATION_STORE_PATH"] = str(tmp_path / "conversation_store.json")
    os.environ["PUBLIC_RATE_LIMIT_ENABLED"] = "0"
    os.environ["ADMIN_API_KEY"] = ""
    os.environ["VOICE_DEFAULT_BRAND_ID"] = "brand1"
    os.environ["VOICE_STT_PROVIDER"] = "mock"
    os.environ["VOICE_TTS_PROVIDER"] = "mock"
    os.environ["VOICE_STT_MOCK_TEXT"] = "my budget is 10 lakh"

    main = importlib.import_module("app.main")
    importlib.reload(main)
    return main.app.test_client()


def test_tata_voice_webhook_creates_or_updates_conversation(tmp_path: Path):
    c = _build_client(tmp_path)
    p = {
        "event": "call.transcript",
        "event_id": "evt-1",
        "brand_id": "brand1",
        "from_phone": "+91 99999 88888",
        "transcript": "my budget is 10 lakh",
    }
    r = c.post("/v1/webhooks/voice/tata", json=p)
    assert r.status_code == 200, r.get_json()
    j = r.get_json() or {}
    assert j.get("processed") is True
    assert j.get("lead_id")
    assert j.get("conversation_id")

    # duplicate callback must be deduped by event id
    r2 = c.post("/v1/webhooks/voice/tata", json=p)
    assert r2.status_code == 200
    j2 = r2.get_json() or {}
    assert j2.get("processed") is False
    assert j2.get("reason") == "duplicate_event"


def test_voice_outreach_queues_call(tmp_path: Path):
    c = _build_client(tmp_path)
    lead_resp = c.post(
        "/v1/marketing/leads",
        json={
            "brand_id": "brand1",
            "source": "web",
            "name": "Voice Lead",
            "phone": "+91 9999911111",
            "message": "please call",
        },
    )
    assert lead_resp.status_code == 200, lead_resp.get_json()
    lead_id = (lead_resp.get_json() or {}).get("lead", {}).get("id")
    assert lead_id

    r = c.post(f"/v1/integrations/voice/outreach_lead/{lead_id}", json={})
    assert r.status_code == 200, r.get_json()
    j = r.get_json() or {}
    assert j.get("status") == "queued"
    assert j.get("provider") in {"tata", "tata_mock"}


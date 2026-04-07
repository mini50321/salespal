from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest

from app.social import get_provider


def test_social_providers_x_youtube():
    assert get_provider("twitter_x").name == "twitter_x"
    assert get_provider("youtube").name == "youtube"


def test_get_provider_rejects_unknown():
    with pytest.raises(ValueError):
        get_provider("unknown_network_xyz")


def test_parallel_outreach_route_requires_admin(tmp_path: Path):
    os.environ["STORE_BACKEND"] = "json"
    os.environ["LEAD_STORE_PATH"] = str(tmp_path / "lead_store.json")
    os.environ["JOB_STORE_PATH"] = str(tmp_path / "job_store.json")
    os.environ["POST_STORE_PATH"] = str(tmp_path / "post_store.json")
    os.environ["CONVERSATION_STORE_PATH"] = str(tmp_path / "conversation_store.json")
    os.environ["PUBLIC_RATE_LIMIT_ENABLED"] = "0"
    os.environ["ADMIN_API_KEY"] = "secret-admin"
    os.environ["VOICE_DEFAULT_BRAND_ID"] = "brand1"
    os.environ["VOICE_STT_PROVIDER"] = "mock"
    os.environ["VOICE_TTS_PROVIDER"] = "mock"
    os.environ["WHATSAPP_PHONE_NUMBER_ID"] = "x"
    os.environ["WHATSAPP_ACCESS_TOKEN"] = "y"

    main = importlib.import_module("app.main")
    importlib.reload(main)
    c = main.app.test_client()

    lead_resp = c.post(
        "/v1/marketing/leads",
        json={
            "brand_id": "brand1",
            "source": "web",
            "name": "P Lead",
            "phone": "+91 9888877777",
            "message": "hi",
        },
    )
    assert lead_resp.status_code == 200
    lead_id = (lead_resp.get_json() or {}).get("lead", {}).get("id")
    r = c.post(f"/v1/integrations/sales/parallel_outreach/{lead_id}", json={"timezone": "Asia/Kolkata"})
    assert r.status_code == 401

    r2 = c.post(
        f"/v1/integrations/sales/parallel_outreach/{lead_id}",
        json={"timezone": "Asia/Kolkata"},
        headers={"X-Admin-Api-Key": "secret-admin"},
    )
    assert r2.status_code == 200, r2.get_json()
    j = r2.get_json() or {}
    assert j.get("status") == "started"

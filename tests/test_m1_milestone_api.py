from __future__ import annotations

import importlib
import os
from pathlib import Path


def _client(tmp_path: Path):
    os.environ["STORE_BACKEND"] = "json"
    os.environ["LEAD_STORE_PATH"] = str(tmp_path / "lead_store.json")
    os.environ["JOB_STORE_PATH"] = str(tmp_path / "job_store.json")
    os.environ["POST_STORE_PATH"] = str(tmp_path / "post_store.json")
    os.environ["CONVERSATION_STORE_PATH"] = str(tmp_path / "conversation_store.json")
    os.environ["M1_OPS_STORE_PATH"] = str(tmp_path / "m1_ops_store.json")
    os.environ["PUBLIC_RATE_LIMIT_ENABLED"] = "0"
    main = importlib.import_module("app.main")
    importlib.reload(main)
    return main.app.test_client()


def test_m1_social_and_dashboard(tmp_path: Path):
    c = _client(tmp_path)
    r = c.post(
        "/v1/marketing/ops/social/connect",
        json={"brand_id": "demo", "platform": "instagram", "account_handle": "@demo_brand"},
    )
    assert r.status_code == 200, r.get_json()
    j = r.get_json() or {}
    assert (j.get("connection") or {}).get("platform") == "instagram"

    r2 = c.get("/v1/marketing/ops/dashboard?brand_id=demo")
    assert r2.status_code == 200, r2.get_json()
    d = r2.get_json() or {}
    assert d.get("brand_id") == "demo"
    assert "digital_presence_score" in d


def test_m1_lead_capture_requires_ad_campaign(tmp_path: Path):
    c = _client(tmp_path)
    r = c.post("/v1/marketing/ops/ads/lead_capture", json={"brand_id": "demo", "campaign_id": "last"})
    assert r.status_code == 409, r.get_json()


def test_m1_lead_manual_history(tmp_path: Path):
    c = _client(tmp_path)
    lead = c.post(
        "/v1/marketing/leads",
        json={"brand_id": "demo", "source": "web", "name": "A", "phone": "+919999999999"},
    ).get_json()["lead"]["id"]

    n = c.post(f"/v1/marketing/ops/leads/{lead}/note", json={"note": "follow soon"})
    assert n.status_code == 200, n.get_json()
    m = c.post(f"/v1/marketing/ops/leads/{lead}/mark", json={"temperature": "warm"})
    assert m.status_code == 200, m.get_json()
    h = c.get(f"/v1/marketing/ops/leads/{lead}/history")
    assert h.status_code == 200, h.get_json()
    jj = h.get_json() or {}
    assert len(((jj.get("history") or {}).get("notes") or [])) == 1

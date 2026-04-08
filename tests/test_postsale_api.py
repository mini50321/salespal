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
    os.environ["SALESPAL360_STORE_PATH"] = str(tmp_path / "salespal360_store.json")
    os.environ["SALE_OPS_STORE_PATH"] = str(tmp_path / "sale_ops_store.json")
    os.environ["POSTSALE_OPS_STORE_PATH"] = str(tmp_path / "postsale_ops_store.json")
    os.environ["PUBLIC_RATE_LIMIT_ENABLED"] = "0"
    os.environ["ADMIN_API_KEY"] = "test-admin"
    main = importlib.import_module("app.main")
    importlib.reload(main)
    return main.app.test_client()


def test_postsale_start_payment_and_timeline(tmp_path: Path):
    c = _client(tmp_path)
    h = {"X-Admin-Api-Key": "test-admin"}
    lead_id = c.post(
        "/v1/marketing/leads",
        json={"brand_id": "demo", "source": "web", "name": "PSLead", "phone": "+919990001119"},
    ).get_json()["lead"]["id"]

    s = c.post("/v1/postsale/start", json={"brand_id": "demo", "lead_id": lead_id}, headers=h)
    assert s.status_code == 200, s.get_json()
    p = c.post("/v1/postsale/payment_status", json={"brand_id": "demo", "lead_id": lead_id, "payment_status": "pending"}, headers=h)
    assert p.status_code == 200, p.get_json()
    a = c.post("/v1/postsale/action", json={"brand_id": "demo", "lead_id": lead_id, "action": "send_payment_reminder"}, headers=h)
    assert a.status_code == 200, a.get_json()
    t = c.get(f"/v1/postsale/timeline?brand_id=demo&lead_id={lead_id}", headers=h)
    assert t.status_code == 200, t.get_json()
    assert len((t.get_json() or {}).get("events") or []) >= 2


def test_postsale_score_dashboard_learning(tmp_path: Path):
    c = _client(tmp_path)
    h = {"X-Admin-Api-Key": "test-admin"}
    lead_id = c.post(
        "/v1/marketing/leads",
        json={"brand_id": "demo", "source": "web", "name": "PSLead2", "phone": "+919990001120"},
    ).get_json()["lead"]["id"]
    c.post("/v1/postsale/start", json={"brand_id": "demo", "lead_id": lead_id}, headers=h)
    c.post("/v1/postsale/action", json={"brand_id": "demo", "lead_id": lead_id, "action": "load_requirements"}, headers=h)
    c.post("/v1/postsale/action", json={"brand_id": "demo", "lead_id": lead_id, "action": "request_documents"}, headers=h)
    c.post("/v1/postsale/action", json={"brand_id": "demo", "lead_id": lead_id, "action": "receive_document"}, headers=h)
    c.post("/v1/postsale/action", json={"brand_id": "demo", "lead_id": lead_id, "action": "validate_document"}, headers=h)
    c.post("/v1/postsale/action", json={"brand_id": "demo", "lead_id": lead_id, "action": "all_requirements_done"}, headers=h)
    c.post("/v1/postsale/action", json={"brand_id": "demo", "lead_id": lead_id, "action": "issue_remaining_no"}, headers=h)
    c.post("/v1/postsale/action", json={"brand_id": "demo", "lead_id": lead_id, "action": "proceed"}, headers=h)
    c.post("/v1/postsale/action", json={"brand_id": "demo", "lead_id": lead_id, "action": "ask_rating_1_10"}, headers=h)
    s = c.post("/v1/postsale/score", json={"brand_id": "demo", "lead_id": lead_id, "score": 9, "resolved": True}, headers=h)
    assert s.status_code == 200, s.get_json()
    d = c.get("/v1/postsale/dashboard?brand_id=demo", headers=h)
    assert d.status_code == 200, d.get_json()
    l = c.post("/v1/postsale/learning", json={"brand_id": "demo"}, headers=h)
    assert l.status_code == 200, l.get_json()

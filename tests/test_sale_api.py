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
    os.environ["PUBLIC_RATE_LIMIT_ENABLED"] = "0"
    os.environ["ADMIN_API_KEY"] = "test-admin"
    main = importlib.import_module("app.main")
    importlib.reload(main)
    return main.app.test_client()


def test_sale_flow_and_dashboard(tmp_path: Path):
    c = _client(tmp_path)
    h = {"X-Admin-Api-Key": "test-admin"}
    lead_id = c.post(
        "/v1/marketing/leads",
        json={"brand_id": "demo", "source": "web", "name": "SLead", "phone": "+919990001111"},
    ).get_json()["lead"]["id"]
    s = c.post(
        "/v1/sale/start",
        json={"brand_id": "demo", "lead_id": lead_id, "timezone": "Asia/Kolkata", "language": "hing"},
        headers=h,
    )
    assert s.status_code == 200, s.get_json()
    q = c.post(
        "/v1/sale/qualification",
        json={"brand_id": "demo", "lead_id": lead_id, "lead_type": "hot", "need_budget_timeline": "need,budget,timeline"},
        headers=h,
    )
    assert q.status_code == 200, q.get_json()
    sc = c.post("/v1/sale/score", json={"brand_id": "demo", "lead_id": lead_id, "score": 9, "resolved": True}, headers=h)
    assert sc.status_code == 200, sc.get_json()
    d = c.get("/v1/sale/dashboard?brand_id=demo", headers=h)
    assert d.status_code == 200, d.get_json()
    dj = d.get_json() or {}
    assert (dj.get("lead_type_breakdown") or {}).get("hot", 0) >= 1


def test_sale_timeline_and_learning(tmp_path: Path):
    c = _client(tmp_path)
    h = {"X-Admin-Api-Key": "test-admin"}
    lead_id = c.post(
        "/v1/marketing/leads",
        json={"brand_id": "demo", "source": "web", "name": "SLead2", "phone": "+919990001112"},
    ).get_json()["lead"]["id"]
    c.post("/v1/sale/start", json={"brand_id": "demo", "lead_id": lead_id}, headers=h)
    c.post("/v1/sale/call_result", json={"brand_id": "demo", "lead_id": lead_id, "result": "no_answer"}, headers=h)
    wr = c.post("/v1/sale/wa_reply", json={"brand_id": "demo", "lead_id": lead_id, "reply": "yes"}, headers=h)
    assert wr.status_code == 200, wr.get_json()
    assert (wr.get_json() or {}).get("followup_days") == []
    assert ((wr.get_json() or {}).get("session") or {}).get("status") == "conversation"
    wr_no = c.post("/v1/sale/wa_reply", json={"brand_id": "demo", "lead_id": lead_id, "reply": "no"}, headers=h)
    assert wr_no.status_code == 200, wr_no.get_json()
    assert (wr_no.get_json() or {}).get("followup_days") == [0, 1, 3, 5, 7]
    a = c.post(
        "/v1/sale/action",
        json={"brand_id": "demo", "lead_id": lead_id, "action": "warm_whatsapp_d1_d3_d5"},
        headers=h,
    )
    assert a.status_code == 200, a.get_json()
    t = c.get(f"/v1/sale/timeline?brand_id=demo&lead_id={lead_id}", headers=h)
    assert t.status_code == 200, t.get_json()
    tj = t.get_json() or {}
    assert len(tj.get("events") or []) >= 2
    l = c.post("/v1/sale/learning", json={"brand_id": "demo"}, headers=h)
    assert l.status_code == 200, l.get_json()
    assert "recommended_action" in ((l.get_json() or {}).get("learning_loop") or {})

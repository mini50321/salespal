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
    os.environ["PUBLIC_RATE_LIMIT_ENABLED"] = "0"
    os.environ["ADMIN_API_KEY"] = "test-admin"
    main = importlib.import_module("app.main")
    importlib.reload(main)
    return main.app.test_client()


def test_salespal360_case_timeline_and_analytics(tmp_path: Path):
    c = _client(tmp_path)
    h = {"X-Admin-Api-Key": "test-admin"}
    lead_id = c.post(
        "/v1/marketing/leads",
        json={"brand_id": "demo", "source": "web", "name": "Lead", "phone": "+919999000111"},
    ).get_json()["lead"]["id"]
    r = c.post(
        "/v1/salespal360/case/upsert",
        json={"brand_id": "demo", "lead_id": lead_id, "stage": "sales", "status": "open", "owner": "ai"},
        headers=h,
    )
    assert r.status_code == 200, r.get_json()
    t = c.get(f"/v1/salespal360/timeline?brand_id=demo&lead_id={lead_id}", headers=h)
    assert t.status_code == 200, t.get_json()
    tj = t.get_json() or {}
    assert (tj.get("case") or {}).get("stage") == "sales"
    a = c.get("/v1/salespal360/analytics?brand_id=demo", headers=h)
    assert a.status_code == 200, a.get_json()
    aj = a.get_json() or {}
    assert "analytics" in aj and "stage_breakdown" in (aj.get("analytics") or {})


def test_salespal360_console_page_loads(tmp_path: Path):
    c = _client(tmp_path)
    r = c.get("/salespal360")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    assert "SalesPal 360" in html
    assert "Overview" in html and "Pipeline" in html
    assert "/v1/salespal360/sales/dashboard" in html


def test_salespal360_m2_dashboard_and_features(tmp_path: Path):
    c = _client(tmp_path)
    h = {"X-Admin-Api-Key": "test-admin"}
    fs = c.get("/v1/salespal360/features/status", headers=h)
    assert fs.status_code == 200, fs.get_json()
    assert "features" in (fs.get_json() or {})
    d = c.get("/v1/salespal360/sales/dashboard?brand_id=demo", headers=h)
    assert d.status_code == 200, d.get_json()
    dj = d.get_json() or {}
    assert "sales_dashboard" in dj and "customer_crm_snapshot" in dj


def test_salespal360_support_and_memory(tmp_path: Path):
    c = _client(tmp_path)
    h = {"X-Admin-Api-Key": "test-admin"}
    lead_id = c.post(
        "/v1/marketing/leads",
        json={"brand_id": "demo", "source": "web", "name": "Lead2", "phone": "+919999000222"},
    ).get_json()["lead"]["id"]
    o = c.post(
        "/v1/salespal360/support/open",
        json={"brand_id": "demo", "lead_id": lead_id, "channel": "whatsapp", "complaint": "need help"},
        headers=h,
    )
    assert o.status_code == 200, o.get_json()
    ticket_id = (o.get_json() or {}).get("ticket", {}).get("id")
    u = c.post(
        "/v1/salespal360/support/update",
        json={"ticket_id": ticket_id, "status": "resolved", "resolution": "done", "escalation_level": "ai"},
        headers=h,
    )
    assert u.status_code == 200, u.get_json()
    m = c.post(
        "/v1/salespal360/memory/merge",
        json={"brand_id": "demo", "lead_id": lead_id, "patch": {"conversation_memory": {"last": "help"}}},
        headers=h,
    )
    assert m.status_code == 200, m.get_json()
    g = c.get(f"/v1/salespal360/memory/get?brand_id=demo&lead_id={lead_id}", headers=h)
    assert g.status_code == 200, g.get_json()
    gj = g.get_json() or {}
    assert "conversation_memory" in (gj.get("memory") or {})

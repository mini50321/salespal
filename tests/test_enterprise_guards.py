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

    main = importlib.import_module("app.main")
    importlib.reload(main)
    return main.app.test_client()


def test_admin_routes_require_api_key_when_enabled(tmp_path: Path):
    os.environ["ADMIN_API_KEY"] = "secret-admin"
    c = _build_client(tmp_path)

    r = c.post("/v1/integrations/zoho/push_leads", json={"limit": 1})
    assert r.status_code == 401
    assert (r.get_json() or {}).get("error") == "admin api key required"

    r2 = c.post(
        "/v1/integrations/zoho/push_leads",
        json={"limit": 1},
        headers={"X-Admin-Api-Key": "secret-admin"},
    )
    # authorized to pass API-key guard; downstream may still reject due to config
    assert r2.status_code != 401


def test_public_rate_limit_for_copy(tmp_path: Path):
    os.environ["PUBLIC_RATE_LIMIT_ENABLED"] = "1"
    os.environ["PUBLIC_RATE_LIMIT_WINDOW_SECONDS"] = "60"
    os.environ["COPY_RATE_LIMIT_PER_WINDOW"] = "2"
    c = _build_client(tmp_path)

    p = {"content_type": "email", "brand_name": "ACME", "context": "short test"}
    assert c.post("/v1/marketing/copy", json=p).status_code == 200
    assert c.post("/v1/marketing/copy", json=p).status_code == 200
    r3 = c.post("/v1/marketing/copy", json=p)
    assert r3.status_code == 429
    assert "Retry-After" in r3.headers


def test_leads_still_accept_with_idempotency_no_contact(tmp_path: Path):
    os.environ["PUBLIC_RATE_LIMIT_ENABLED"] = "0"
    os.environ["ADMIN_API_KEY"] = ""
    c = _build_client(tmp_path)

    r = c.post(
        "/v1/marketing/leads",
        json={
            "brand_id": "brand1",
            "name": "No Contact",
            "idempotency_key": "submission-1",
        },
    )
    assert r.status_code == 200, r.get_json()
    data = r.get_json() or {}
    assert data.get("created") is True


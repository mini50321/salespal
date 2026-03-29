from __future__ import annotations

import logging
import os

from .logging_config import configure_logging
from .gcp_bootstrap import maybe_load_secrets

configure_logging()
maybe_load_secrets()

from flask import Flask, jsonify, request
from datetime import datetime, timedelta, timezone
from typing import Any

from .generator import Generator
from .persistence import build_stores
from .settings import settings
from .lead_ingest import (
    load_field_map,
    normalize_ingest_body,
    sanitize_idempotency_key,
    verify_ingest_auth,
)
from .scheduler_auth import verify_scheduler_internal_request
from .social import get_provider
from .zoho import ZohoClient, map_lead_to_zoho, zoho_first_row_outcome


app = Flask(__name__)
store, posts, leads = build_stores(settings)
gen = Generator()
zoho = ZohoClient()

log = logging.getLogger(__name__)


@app.before_request
def _log_request():
    if (
        request.path in ("/healthz", "/readyz")
        or request.path.endswith("/webhook")
        or "/cron/" in request.path
        or request.path.endswith("/dispatch")
    ):
        return None
    log.info(
        "request",
        extra={
            "gcp_extra": {
                "httpRequest": {
                    "requestMethod": request.method,
                    "requestUrl": request.path,
                }
            }
        },
    )
    return None


def _err(status: int, message: str):
    return jsonify({"error": message}), status


def _auto_sync_lead(lead) -> None:
    if (os.getenv("LEADS_AUTO_SYNC_ZOHO") or "").lower() not in ("1", "true", "yes"):
        return
    if not zoho.ready():
        return
    if lead.status != "captured":
        return
    _push_lead_core(lead.id)


def _push_lead_core(lead_id: str) -> dict[str, Any]:
    l = leads.get(lead_id)
    if not l:
        return {"status": "skipped", "reason": "not_found"}
    if l.status != "captured":
        return {"status": "skipped", "reason": "not_captured", "lead_status": l.status}
    payload = map_lead_to_zoho(l)
    resp = zoho.create_lead(payload)
    ok, rid, err = zoho_first_row_outcome(resp)
    if ok:
        patch: dict[str, Any] = {"zoho_create_response": resp}
        if rid:
            patch["zcrm_id"] = rid
        leads.merge_raw(l.id, patch)
        leads.set_status(l.id, "pushed")
        return {"status": "pushed", "zcrm_id": rid, "response": resp}
    return {"status": "failed", "error": err, "response": resp}


def _batch_push_zoho_leads(brand_id: str | None, limit: int) -> dict[str, Any]:
    captured = leads.list(brand_id=brand_id, status="captured")
    captured = captured[:limit]
    pushed = 0
    failed = 0
    skipped = 0
    results: list[dict[str, Any]] = []
    for l in captured:
        try:
            out = _push_lead_core(l.id)
            st = out.get("status")
            if st == "pushed":
                pushed += 1
            elif st == "failed":
                failed += 1
            else:
                skipped += 1
            results.append({"lead_id": l.id, **out})
        except Exception as e:
            failed += 1
            results.append({"lead_id": l.id, "status": "failed", "error": str(e)})
    return {
        "attempted": len(captured),
        "pushed": pushed,
        "failed": failed,
        "skipped": skipped,
        "results": results,
    }


@app.get("/healthz")
def healthz():
    return jsonify(
        {
            "status": "ok",
            "service": os.getenv("K_SERVICE"),
            "revision": os.getenv("K_REVISION"),
            "configuration": os.getenv("K_CONFIGURATION"),
        }
    )


@app.get("/readyz")
def readyz():
    b = (settings.store_backend or "json").strip().lower()
    if b == "firestore":
        try:
            from .firestore_stores import firestore_ready_check

            firestore_ready_check(settings)
        except Exception as e:
            log.exception("readyz")
            return jsonify({"status": "not_ready", "error": str(e)}), 503
    if (os.getenv("ZOHO_READY_CHECK") or "").lower() in ("1", "true", "yes"):
        if zoho.ready():
            try:
                zoho.ping()
            except Exception as e:
                log.exception("readyz")
                return jsonify({"status": "not_ready", "error": f"zoho: {e}"}), 503
    return jsonify({"status": "ready", "store": settings.store_backend})


@app.post("/v1/marketing/assets")
def create_asset():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    asset_type = str(body.get("asset_type") or "").strip()
    prompt = str(body.get("prompt") or "").strip()
    n = int(body.get("n") or 3)
    require_approval = bool(body.get("require_approval", True))

    if not brand_id or len(brand_id) > 64:
        return _err(400, "invalid brand_id")
    if asset_type not in ("image", "carousel", "video"):
        return _err(400, "invalid asset_type")
    if not prompt or len(prompt) > 4000:
        return _err(400, "invalid prompt")
    if n < 1 or n > 10:
        return _err(400, "invalid n")

    job = store.create(brand_id, asset_type, prompt, require_approval)
    try:
        out = gen.generate(asset_type, prompt, n)
        job.output = out.payload
        job.status = "awaiting_approval" if require_approval else "approved"
        store.update(job)
    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        store.update(job)

    return jsonify(job.__dict__)


@app.get("/v1/marketing/assets")
def list_assets():
    brand_id = request.args.get("brand_id")
    jobs = store.list(brand_id=brand_id)
    return jsonify([j.__dict__ for j in jobs])


@app.get("/v1/marketing/assets/<job_id>")
def get_asset(job_id: str):
    job = store.get(job_id)
    if not job:
        return _err(404, "not found")
    return jsonify(job.__dict__)


@app.post("/v1/marketing/assets/<job_id>/approve")
def approve(job_id: str):
    job = store.get(job_id)
    if not job:
        return _err(404, "not found")
    if job.status not in ("awaiting_approval", "approved"):
        return _err(409, "invalid status")
    job.status = "approved"
    store.update(job)
    return jsonify(job.__dict__)


@app.post("/v1/marketing/assets/<job_id>/reject")
def reject(job_id: str):
    job = store.get(job_id)
    if not job:
        return _err(404, "not found")
    if job.status not in ("awaiting_approval", "rejected"):
        return _err(409, "invalid status")
    job.status = "rejected"
    store.update(job)
    return jsonify(job.__dict__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@app.post("/v1/marketing/posts")
def create_post():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    provider = str(body.get("provider") or "mock").strip()
    channel = str(body.get("channel") or "").strip()
    asset_job_id = str(body.get("asset_job_id") or "").strip()
    caption = str(body.get("caption") or "").strip()
    scheduled_at = str(body.get("scheduled_at") or "").strip()
    max_attempts = int(body.get("max_attempts") or 5)

    if not brand_id or len(brand_id) > 64:
        return _err(400, "invalid brand_id")
    if not channel or len(channel) > 64:
        return _err(400, "invalid channel")
    if not asset_job_id:
        return _err(400, "invalid asset_job_id")
    if not scheduled_at:
        scheduled_at = _utcnow().isoformat()
    try:
        datetime.fromisoformat(scheduled_at.replace("Z", "+00:00"))
    except Exception:
        return _err(400, "invalid scheduled_at")
    if max_attempts < 1 or max_attempts > 20:
        return _err(400, "invalid max_attempts")

    aj = store.get(asset_job_id)
    if not aj:
        return _err(404, "asset job not found")
    if aj.status != "approved":
        return _err(409, "asset job must be approved")
    if not isinstance(aj.output, dict):
        return _err(409, "asset output missing")

    try:
        get_provider(provider)
    except Exception:
        return _err(400, "invalid provider")

    pj = posts.create(
        brand_id=brand_id,
        provider=provider,
        channel=channel,
        asset_job_id=asset_job_id,
        caption=caption,
        scheduled_at=scheduled_at,
        max_attempts=max_attempts,
    )
    return jsonify(pj.__dict__)


@app.get("/v1/marketing/posts")
def list_posts():
    brand_id = request.args.get("brand_id")
    return jsonify([p.__dict__ for p in posts.list(brand_id=brand_id)])


@app.get("/v1/marketing/posts/<post_id>")
def get_post(post_id: str):
    p = posts.get(post_id)
    if not p:
        return _err(404, "not found")
    return jsonify(p.__dict__)


@app.post("/v1/marketing/posts/<post_id>/cancel")
def cancel_post(post_id: str):
    p = posts.get(post_id)
    if not p:
        return _err(404, "not found")
    if p.status == "posted":
        return _err(409, "already posted")
    p.status = "cancelled"
    posts.update(p)
    return jsonify(p.__dict__)


@app.post("/v1/marketing/posts/dispatch")
def dispatch_posts():
    if not verify_scheduler_internal_request(request):
        return _err(401, "unauthorized")
    now_iso = _utcnow().isoformat()
    due = posts.due(now_iso)
    processed = 0
    posted = 0
    failed = 0

    for p in due:
        processed += 1
        aj = store.get(p.asset_job_id)
        if not aj or aj.status != "approved" or not isinstance(aj.output, dict):
            p.attempt_count += 1
            p.status = "failed"
            p.error = "asset not available"
            backoff = min(3600, 2 ** min(10, p.attempt_count))
            p.next_attempt_at = (_utcnow() + timedelta(seconds=backoff)).isoformat()
            posts.update(p)
            failed += 1
            continue

        try:
            p.status = "posting"
            posts.update(p)
            provider = get_provider(p.provider)
            r = provider.post(channel=p.channel, caption=p.caption, asset_payload=aj.output, brand_id=p.brand_id)
            p.provider_post_id = r.provider_post_id
            p.provider_response = r.response
            p.status = "posted"
            posts.update(p)
            posted += 1
        except Exception as e:
            p.attempt_count += 1
            p.status = "failed"
            p.error = str(e)
            backoff = min(3600, 2 ** min(10, p.attempt_count))
            p.next_attempt_at = (_utcnow() + timedelta(seconds=backoff)).isoformat()
            posts.update(p)
            failed += 1

    return jsonify({"processed": processed, "posted": posted, "failed": failed, "now": now_iso})


@app.post("/v1/marketing/leads")
def capture_lead():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    source = str(body.get("source") or "web").strip()
    name = body.get("name")
    email = body.get("email")
    phone = body.get("phone")
    company = body.get("company")
    message = body.get("message")
    utm = body.get("utm")
    raw = body.get("raw") or body

    if not brand_id or len(brand_id) > 64:
        return _err(400, "invalid brand_id")
    if len(source) > 64:
        return _err(400, "invalid source")
    if utm is not None and not isinstance(utm, dict):
        return _err(400, "invalid utm")
    if not isinstance(raw, dict):
        raw = {"value": raw}

    idem = sanitize_idempotency_key(request.headers.get("Idempotency-Key"))

    lead, created = leads.upsert(
        brand_id=brand_id,
        source=source,
        name=str(name) if name is not None else None,
        email=str(email) if email is not None else None,
        phone=str(phone) if phone is not None else None,
        company=str(company) if company is not None else None,
        message=str(message) if message is not None else None,
        utm=utm,
        raw=raw,
        idempotency_key=idem,
    )

    _auto_sync_lead(lead)

    return jsonify({"created": created, "lead": lead.__dict__})


@app.post("/v1/marketing/leads/webhook")
def lead_webhook():
    if not verify_ingest_auth(request):
        return _err(401, "unauthorized")
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    body = normalize_ingest_body(body, load_field_map())
    brand_id = str(body.get("brand_id") or "").strip()
    source = str(body.get("source") or "webhook").strip()
    name = body.get("name")
    email = body.get("email")
    phone = body.get("phone")
    company = body.get("company")
    message = body.get("message")
    utm = body.get("utm")
    raw = body.get("raw") if isinstance(body.get("raw"), dict) else body

    if not brand_id or len(brand_id) > 64:
        return _err(400, "invalid brand_id")
    if len(source) > 64:
        return _err(400, "invalid source")
    if utm is not None and not isinstance(utm, dict):
        return _err(400, "invalid utm")
    if not isinstance(raw, dict):
        raw = {"payload": raw}

    idem = sanitize_idempotency_key(
        request.headers.get("Idempotency-Key") or (raw.get("idempotency_key") if isinstance(raw, dict) else None)
    )

    lead, created = leads.upsert(
        brand_id=brand_id,
        source=source,
        name=str(name) if name is not None else None,
        email=str(email) if email is not None else None,
        phone=str(phone) if phone is not None else None,
        company=str(company) if company is not None else None,
        message=str(message) if message is not None else None,
        utm=utm,
        raw=raw,
        idempotency_key=idem,
    )

    _auto_sync_lead(lead)

    return jsonify({"created": created, "lead": lead.__dict__})


@app.get("/v1/marketing/leads")
def list_leads():
    brand_id = request.args.get("brand_id")
    status = request.args.get("status")
    return jsonify([l.__dict__ for l in leads.list(brand_id=brand_id, status=status)])


@app.get("/v1/marketing/leads/<lead_id>")
def get_lead(lead_id: str):
    l = leads.get(lead_id)
    if not l:
        return _err(404, "not found")
    return jsonify(l.__dict__)


@app.post("/v1/marketing/leads/<lead_id>/mark_pushed")
def mark_lead_pushed(lead_id: str):
    l = leads.set_status(lead_id, "pushed")
    if not l:
        return _err(404, "not found")
    return jsonify(l.__dict__)


@app.post("/v1/integrations/zoho/push_lead/<lead_id>")
def push_lead_to_zoho(lead_id: str):
    if not zoho.ready():
        return _err(409, "zoho not configured")
    try:
        out = _push_lead_core(lead_id)
    except Exception as e:
        return jsonify({"lead_id": lead_id, "status": "failed", "error": str(e)}), 502
    if out.get("status") == "failed":
        return jsonify({"lead_id": lead_id, **out}), 502
    if out.get("status") == "skipped":
        return jsonify({"lead_id": lead_id, **out}), 409
    return jsonify({"lead_id": lead_id, **out})


@app.post("/v1/integrations/zoho/push_leads")
def push_leads_to_zoho():
    if not zoho.ready():
        return _err(409, "zoho not configured")

    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip() or None
    limit = int(body.get("limit") or 25)

    if limit < 1 or limit > 100:
        return _err(400, "invalid limit")

    return jsonify(_batch_push_zoho_leads(brand_id, limit))


@app.post("/v1/cron/zoho_push_leads")
def zoho_cron_push_leads():
    if not verify_scheduler_internal_request(request):
        return _err(401, "unauthorized")
    if not zoho.ready():
        return _err(409, "zoho not configured")
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip() or None
    limit = int(body.get("limit") or os.getenv("ZOHO_CRON_LIMIT") or 50)
    if limit < 1 or limit > 100:
        return _err(400, "invalid limit")
    return jsonify(_batch_push_zoho_leads(brand_id, limit))

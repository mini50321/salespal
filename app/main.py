from __future__ import annotations

import base64
import html
import json
import logging
import os
import queue
import threading
import time
import traceback
import urllib.request
import uuid

from .logging_config import configure_logging
from .gcp_bootstrap import maybe_load_secrets

configure_logging()
maybe_load_secrets()

from flask import Flask, Response, jsonify, redirect, request
from datetime import datetime, timedelta, timezone
from typing import Any

from .creative_assets import (
    apply_logo_asset_to_prompts,
    apply_production_plan_to_prompts,
    apply_script_audio_to_video_plan,
    derive_autonomous_campaign_params,
    derive_imagen_prompts_from_brief,
    derive_production_plan_from_brief,
    derive_script_audio_spec,
    derive_video_plan_from_brief,
    merge_stage5_video_options,
    stage5_asset_manifest,
)
from .campaign_qa import run_stage6_campaign_qa, stage6_pending_placeholder
from .creative_brief import describe_logo_image, fetch_website_form_hints, generate_creative_brief
from .generator import Generator
from .conversation_engine import conversation_to_qualification_dict, process_user_message
from .persistence import build_conversation_store, build_stores
from .settings import settings
from .lead_ingest import (
    load_field_map,
    normalize_ingest_body,
    sanitize_idempotency_key,
    verify_ingest_auth,
)
from .scheduler_auth import verify_scheduler_internal_request
from .security import PublicRateLimiter, verify_admin_api_key
from .social import get_provider
from .whatsapp_cloud import (
    default_locale,
    iter_inbound_text_messages,
    resolve_brand_id,
    send_text as whatsapp_send_text,
    verify_signature as whatsapp_verify_signature,
    verify_subscription as whatsapp_verify_subscription,
)
from .whatsapp_outreach import batch_whatsapp_outreach, outreach_one_lead
from .voice_outreach import batch_voice_outreach, outreach_one_lead as voice_outreach_one_lead
from .parallel_outreach import start_parallel_outreach
from .whatsapp_nurture import process_due_nurtures
from .cold_campaign_engine import enroll_cold_lead, process_due_cold_campaigns
from .m1_ops import M1OpsStore, allocate_budget, suggest_platforms
from .postsale_ops import PostSaleOpsStore
from .sale_ops import SaleOpsStore
from .salespal360_ops import SalesPal360Store
from .sales_pipeline import merge_classification
from .voice_stt import transcribe_audio_base64, transcribe_audio_url
from .voice_tata import call_outbound as tata_call_outbound, parse_event as parse_voice_tata_event, verify_webhook as verify_voice_tata_webhook
from .voice_tts import synthesize as voice_synthesize
from .conversation_store import Conversation
from .zoho import (
    ZohoClient,
    build_qualification_update_payload,
    map_lead_to_zoho,
    zoho_first_row_outcome,
)
from . import public_chat as public_chat_api
from . import marketing_leads_http
from .marketing_copy import generate_marketing_copy
from .lead_store import make_dedupe_key


app = Flask(__name__)
store, posts, leads = build_stores(settings)
conversations = build_conversation_store(settings)
gen = Generator()
zoho = ZohoClient()
m1ops = M1OpsStore(os.getenv("M1_OPS_STORE_PATH") or "./m1_ops_store.json")
sp360 = SalesPal360Store(os.getenv("SALESPAL360_STORE_PATH") or "./salespal360_store.json")
saleops = SaleOpsStore(os.getenv("SALE_OPS_STORE_PATH") or "./sale_ops_store.json")
postsaleops = PostSaleOpsStore(os.getenv("POSTSALE_OPS_STORE_PATH") or "./postsale_ops_store.json")

log = logging.getLogger(__name__)
rate_limiter = PublicRateLimiter()
_RENDER_QUEUE: queue.Queue[str] = queue.Queue()
_RENDER_LOCK = threading.Lock()
_RENDER_JOBS: dict[str, dict[str, Any]] = {}

_ADMIN_GUARDED_PREFIXES = (
    "/v1/integrations/zoho/",
    "/v1/integrations/whatsapp/",
    "/v1/integrations/voice/",
    "/v1/integrations/sales/",
    "/v1/marketing/ops/",
    "/v1/salespal360/",
    "/v1/sale/",
    "/v1/postsale/",
)
_ADMIN_GUARDED_EXACT = {
    "/v1/marketing/posts/dispatch",
}
_PUBLIC_RATE_LIMITED = {
    "/v1/marketing/leads",
    "/v1/marketing/copy",
}

_VIDEO_LANGUAGE_LABELS: dict[str, str] = {
    "auto": "Auto",
    "en": "English",
    "hi": "Hindi",
    "hinglish": "Hinglish",
    "ta": "Tamil",
    "te": "Telugu",
    "ml": "Malayalam",
    "kn": "Kannada",
    "mr": "Marathi",
    "bn": "Bengali",
    "gu": "Gujarati",
    "pa": "Punjabi",
    "ur": "Urdu",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "ar": "Arabic",
    "pt": "Portuguese",
    "id": "Indonesian",
}


def _normalize_video_language(raw: Any) -> str:
    v = str(raw or "auto").strip().lower() or "auto"
    if v not in _VIDEO_LANGUAGE_LABELS:
        allowed = ", ".join(sorted(_VIDEO_LANGUAGE_LABELS.keys()))
        raise ValueError(f"video_language must be one of: {allowed}")
    return v


def _service_index_payload() -> dict[str, Any]:
    return {
        "service": "salespal-api",
        "status": "ok",
        "demo_ui": "/demo",
        "api_json": "/api",
        "routes": {
            "api_json": "/api",
            "healthz": "/healthz",
            "readyz": "/readyz",
            "whatsapp_webhook": "/v1/webhooks/whatsapp",
            "voice_tata_webhook": "/v1/webhooks/voice/tata",
            "public_chat_start": "/v1/public/chat/start",
            "public_chat_message": "/v1/public/chat/message",
            "creative_brief": "/v1/marketing/creative-brief",
            "fetch_website_hints": "/v1/marketing/fetch-website-hints",
            "creative_assets": "/v1/marketing/creative-assets",
            "marketing_campaign": "/v1/marketing/campaign",
            "marketing_campaign_execute": "/v1/marketing/campaign/execute",
            "marketing_campaign_execute_async": "/v1/marketing/campaign/execute_async",
            "marketing_render_job_get": "/v1/marketing/render_jobs/<job_id>",
            "marketing_render_job_list": "/v1/marketing/render_jobs",
            "marketing_copy": "/v1/marketing/copy",
            "marketing_leads": "/v1/marketing/leads",
            "marketing_lead_form_embed": "/v1/marketing/embed/lead-form",
            "voice_outreach_lead": "/v1/integrations/voice/outreach_lead/<lead_id>",
            "voice_outreach_leads": "/v1/integrations/voice/outreach_leads",
            "voice_call_now": "/v1/integrations/voice/call_now",
            "voice_outreach_cron": "/v1/cron/voice_outreach",
            "parallel_outreach": "/v1/integrations/sales/parallel_outreach/<lead_id>",
            "sales_classify": "/v1/integrations/sales/leads/<lead_id>/classify",
            "cold_enroll": "/v1/integrations/sales/leads/<lead_id>/cold_enroll",
            "cron_whatsapp_nurture": "/v1/cron/whatsapp_nurture",
            "cron_cold_campaign": "/v1/cron/cold_campaign",
            "marketing_console": "/marketing",
            "marketing_dashboard": "/v1/marketing/ops/dashboard",
            "marketing_optimization_loop": "/v1/marketing/ops/optimization/loop",
            "salespal360_console": "/salespal360",
            "salespal360_analytics": "/v1/salespal360/analytics",
            "salespal360_sales_dashboard": "/v1/salespal360/sales/dashboard",
            "salespal360_m2_features": "/v1/salespal360/features/status",
            "sale_console": "/sale",
            "postsale_console": "/post-sale",
        },
    }


def _index_browser_html() -> str:
    data = _service_index_payload()
    routes = data["routes"]
    items = "".join(
        "<li><code>{0}</code> — <code>{1}</code></li>".format(html.escape(k), html.escape(str(v)))
        for k, v in sorted(routes.items())
    )
    if not _demo_ui_enabled():
        demo_note = (
            "<p class=\"muted\"><strong>Demo UI is off.</strong> Set environment variable "
            "<code>DEMO_UI_ENABLED=1</code> and restart (local script does this by default).</p>"
        )
    elif (os.getenv("DEMO_UI_KEY") or "").strip():
        demo_note = (
            "<p class=\"muted\">Demo requires <code>?key=…</code> on <code>/demo</code> "
            "(same value as <code>DEMO_UI_KEY</code>).</p>"
        )
    else:
        demo_note = ""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SalesPal API</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 40rem;
      margin: 2rem auto; padding: 0 1rem; color: #0f172a; line-height: 1.45; }}
    a {{ color: #0369a1; }}
    code {{ background: #f1f5f9; padding: 0.12rem 0.35rem; border-radius: 4px; font-size: 0.88em; }}
    .muted {{ color: #64748b; font-size: 0.9rem; }}
    ul {{ padding-left: 1.2rem; }}
    li {{ margin: 0.25rem 0; }}
  </style>
</head>
<body>
  <h1 style="font-size:1.35rem">SalesPal API</h1>
  <p>Service is up.</p>
  <p style="margin:1.25rem 0"><a href="/demo" style="display:inline-block;background:#0369a1;color:#fff;padding:0.6rem 1.2rem;border-radius:10px;text-decoration:none;font-weight:600">Open campaign demo</a></p>
  {demo_note}
  <p class="muted">Machine-readable: <a href="/api">/api</a> · <a href="/?format=json">/?format=json</a> · <a href="/healthz">/healthz</a></p>
  <h2 style="font-size:1rem;margin-top:1.5rem">Routes</h2>
  <ul>{items}</ul>
</body>
</html>"""


def _request_likely_from_browser_tab() -> bool:
    """
    True when we should send the user to /demo instead of JSON.
    curl/scripts use Accept: */* or application/json — no 'text/html' substring.
    Real browsers send text/html in Accept (and often Sec-Fetch-Dest: document).
    """
    if (request.headers.get("Sec-Fetch-Dest") or "").lower() == "document":
        return True
    accept = (request.headers.get("Accept") or "").lower()
    return "text/html" in accept


@app.get("/api")
def api_discovery():
    """Always JSON — use this URL for scripts and monitoring (no Accept header quirks)."""
    return jsonify(_service_index_payload())


@app.get("/")
def index():
    data = _service_index_payload()
    if (request.args.get("format") or "").lower() == "json":
        return jsonify(data)
    # Demo on, no key: browsers hitting / go straight to the UI (local + Cloud Run).
    if (
        _demo_ui_enabled()
        and not (os.getenv("DEMO_UI_KEY") or "").strip()
        and _request_likely_from_browser_tab()
    ):
        return redirect("/demo", code=302)
    if request.accept_mimetypes.best_match(["application/json", "text/html"]) == "text/html":
        return Response(_index_browser_html(), mimetype="text/html; charset=utf-8")
    return jsonify(data)


@app.after_request
def _public_chat_cors(resp):
    for k, v in public_chat_api.cors_header_items(request).items():
        resp.headers[k] = v
    for k, v in marketing_leads_http.lead_capture_cors_headers(request).items():
        resp.headers[k] = v
    return resp


@app.before_request
def _log_request():
    if (
        request.path in ("/healthz", "/readyz")
        or request.path.endswith("/webhook")
        or "/webhooks/" in request.path
        or "/cron/" in request.path
        or request.path.startswith("/v1/public/chat")
        or request.path.endswith("/dispatch")
        or (request.path == "/v1/marketing/leads" and request.method == "OPTIONS")
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


@app.before_request
def _enforce_security_guards():
    path = request.path

    # Optional admin API key guard for integration/admin operations.
    if (
        path in _ADMIN_GUARDED_EXACT or any(path.startswith(pfx) for pfx in _ADMIN_GUARDED_PREFIXES)
    ) and not verify_admin_api_key(request):
        return _err(401, "admin api key required")

    # Basic per-IP throttling for public endpoints.
    if request.method == "POST" and path in _PUBLIC_RATE_LIMITED:
        ok, retry_after = rate_limiter.check(request)
        if not ok:
            resp, status = _err(429, "rate limit exceeded")
            if retry_after is not None:
                resp.headers["Retry-After"] = str(retry_after)
            return resp, status

    return None


def _err(status: int, message: str):
    return jsonify({"error": message}), status


_MAX_CAMPAIGN_ON_SCREEN_LINES = 20


def _parse_campaign_on_screen_lines(raw: Any) -> list[str]:
    out: list[str] = []
    if isinstance(raw, list):
        for x in raw:
            s = str(x).strip()
            if s:
                out.append(s)
    elif isinstance(raw, str):
        for line in raw.splitlines():
            s = line.strip()
            if s:
                out.append(s)
    return out[:_MAX_CAMPAIGN_ON_SCREEN_LINES]


_ALLOWED_LOGO_IMAGE_MIMES = frozenset({"image/png", "image/jpeg", "image/webp"})


def _parse_logo_image_from_body(body: dict[str, Any]) -> tuple[bytes | None, str | None, str | None]:
    """Returns (raw_bytes, mime_type, error_message). Empty input → (None, None, None)."""
    raw = body.get("logo_image_base64")
    if raw is None or raw == "":
        return None, None, None
    if not isinstance(raw, str) or not raw.strip():
        return None, None, "logo_image_base64 must be a non-empty string"
    s = raw.strip()
    mime = "image/png"
    if s.startswith("data:"):
        try:
            head, b64part = s.split(",", 1)
            semi = head.find(";")
            mime = (head[5:semi] if semi > 5 else head[5:]).strip().lower()
            s = b64part
        except ValueError:
            return None, None, "invalid logo_image_base64 data URL"
    else:
        declared = body.get("logo_image_mime_type")
        if isinstance(declared, str) and declared.strip():
            mime = declared.strip().lower()
    if mime not in _ALLOWED_LOGO_IMAGE_MIMES:
        return None, None, "logo_image_mime_type must be image/png, image/jpeg, or image/webp"
    try:
        data = base64.b64decode(s, validate=False)
    except Exception:
        return None, None, "logo_image_base64 is not valid base64"
    max_b = int(os.getenv("CAMPAIGN_LOGO_IMAGE_MAX_BYTES") or str(2_500_000))
    if len(data) > max_b:
        return None, None, f"logo image too large (max {max_b} bytes)"
    if len(data) < 32:
        return None, None, "logo image data too small"
    return data, mime, None


def _normalize_primary_market(raw: Any) -> str:
    default = (os.getenv("CAMPAIGN_DEFAULT_PRIMARY_MARKET") or "India").strip() or "India"
    if raw is None:
        return default
    s = str(raw).strip()
    return s if s else default


def _primary_market_is_india(pm: str) -> bool:
    s = (pm or "").strip().lower()
    if not s:
        return False
    if s in ("in", "ind", "india", "bharat"):
        return True
    return "india" in s or "indian" in s or "south asia" in s


_INDIA_CASTING_DIRECTION = (
    "Indian and South Asian adult professionals (mixed gender, credible ages ~28–55) in contemporary Indian metro corporate "
    "or tier-1 business settings; wardrobe and context must read as India-paid-social authentic — not Western-only default casting."
)
_INDIA_MARKETING_LOCALIZATION = (
    "India digital ads (Meta/LinkedIn/YouTube): ground messaging in credible local buyer segments; Indian English VO is acceptable; "
    "subtle corporate music bed; avoid all-foreign team tropes when primary buyers are Indian."
)


def _demo_ui_enabled() -> bool:
    return (os.getenv("DEMO_UI_ENABLED") or "").lower() in ("1", "true", "yes")


def _demo_ui_authorized() -> bool:
    # If DEMO_UI_KEY is set, require it via query (?key=) or header (X-Demo-Key).
    expected = (os.getenv("DEMO_UI_KEY") or "").strip()
    if not expected:
        return True
    got = (request.args.get("key") or request.headers.get("X-Demo-Key") or "").strip()
    return bool(got) and got == expected


def _demo_allowed_media_gs_uri(gs: str) -> bool:
    """Only serve objects from configured media buckets (demo key is not a full auth boundary)."""
    for env_name in ("META_MEDIA_BUCKET", "ASSET_MEDIA_BUCKET"):
        b = (os.getenv(env_name) or "").strip()
        if b and (gs == f"gs://{b}" or gs.startswith(f"gs://{b}/")):
            return True
    return False


@app.get("/demo/asset-media")
def demo_asset_media():
    """Stream GCS bytes through the app so <img>/<video> previews work reliably (redirect + signed URL often breaks)."""
    if not _demo_ui_enabled():
        return _err(404, "not found")
    if not _demo_ui_authorized():
        return _err(401, "unauthorized")
    gs = (request.args.get("gs") or "").strip()
    if not gs.startswith("gs://"):
        return _err(400, "invalid gs")
    if not _demo_allowed_media_gs_uri(gs):
        log.warning("demo asset-media: rejected gs uri outside configured buckets")
        return _err(403, "gs uri not allowed")
    try:
        from google.cloud import storage

        from .social_meta import _parse_gs_uri

        bucket_name, blob_name = _parse_gs_uri(gs)
        blob = storage.Client().bucket(bucket_name).blob(blob_name)
        if not blob.exists():
            return _err(404, "object not found")
        data = blob.download_as_bytes()
        ct = (blob.content_type or "application/octet-stream").strip()
        return Response(
            data,
            mimetype=ct,
            headers={"Cache-Control": "private, max-age=300"},
        )
    except Exception as e:
        log.exception("demo asset-media proxy")
        return _err(502, str(e))


@app.get("/demo")
def demo_ui():
    if not _demo_ui_enabled():
        return _err(404, "not found")
    if not _demo_ui_authorized():
        return _err(401, "unauthorized")

    html = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>SalesPal — Demo console</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700&display=swap" rel="stylesheet" />
    <style>
      :root {
        --bg: #eef2f6;
        --surface: #ffffff;
        --text: #0f172a;
        --muted: #64748b;
        --border: #e2e8f0;
        --accent: #0c4a6e;
        --accent-light: #0284c7;
        --success: #047857;
        --radius: 14px;
        --shadow: 0 1px 2px rgba(15, 23, 42, 0.05), 0 12px 40px rgba(15, 23, 42, 0.08);
        --font: "DM Sans", system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: var(--font);
        color: var(--text);
        background: var(--bg);
        min-height: 100vh;
        line-height: 1.5;
      }
      .topbar {
        background: linear-gradient(135deg, #0c4a6e 0%, #082f49 45%, #0e7490 100%);
        color: #f8fafc;
        padding: 0 1.5rem;
        border-bottom: 1px solid rgba(255,255,255,0.12);
      }
      .topbar-inner {
        max-width: 1180px;
        margin: 0 auto;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 1rem;
        min-height: 4rem;
        flex-wrap: wrap;
      }
      .brand {
        display: flex;
        align-items: center;
        gap: 0.875rem;
      }
      .brand-mark {
        width: 40px; height: 40px;
        border-radius: 10px;
        background: rgba(255,255,255,0.15);
        display: grid;
        place-items: center;
        font-weight: 700;
        font-size: 0.95rem;
        letter-spacing: -0.02em;
      }
      .brand h1 {
        margin: 0;
        font-size: 1.125rem;
        font-weight: 600;
        letter-spacing: -0.02em;
      }
      .brand p {
        margin: 0;
        font-size: 0.8125rem;
        opacity: 0.85;
        font-weight: 400;
      }
      .badge {
        font-size: 0.6875rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        padding: 0.35rem 0.65rem;
        border-radius: 999px;
        background: rgba(255,255,255,0.18);
        border: 1px solid rgba(255,255,255,0.25);
        white-space: nowrap;
      }
      .shell {
        max-width: 1180px;
        margin: 0 auto;
        padding: 1.75rem 1.5rem 3rem;
      }
      .hero {
        margin-bottom: 1.5rem;
      }
      .hero h2 {
        margin: 0 0 0.35rem;
        font-size: 1.375rem;
        font-weight: 600;
        letter-spacing: -0.02em;
      }
      .hero p {
        margin: 0;
        color: var(--muted);
        font-size: 0.9375rem;
        max-width: 52rem;
      }
      .layout {
        display: grid;
        grid-template-columns: 1fr minmax(280px, 380px);
        gap: 1.5rem;
        align-items: start;
      }
      @media (max-width: 960px) {
        .layout { grid-template-columns: 1fr; }
        .panel-out { position: static !important; }
      }
      @media (max-height: 520px) and (orientation: landscape) {
        .container { padding: 0.85rem 0.9rem 1.2rem; }
        .topbar { padding: 0 1rem; }
        .card { padding: 0.9rem 1rem; }
        .row2 { grid-template-columns: 1fr; }
        pre#out { max-height: 40vh; }
        .asset-preview-body img,
        .asset-preview-body video { max-height: 40vh; }
      }
      .card {
        background: var(--surface);
        border-radius: var(--radius);
        border: 1px solid var(--border);
        box-shadow: var(--shadow);
        padding: 1.25rem 1.35rem;
        margin-bottom: 1.25rem;
      }
      .card:last-child { margin-bottom: 0; }
      .card-h {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 0.75rem;
        margin-bottom: 1rem;
      }
      .card-h h3 {
        margin: 0;
        font-size: 1rem;
        font-weight: 600;
        letter-spacing: -0.01em;
      }
      .card-h span {
        font-size: 0.75rem;
        color: var(--muted);
        font-weight: 500;
      }
      label {
        display: block;
        font-size: 0.6875rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: var(--muted);
        margin: 0.75rem 0 0.35rem;
      }
      label:first-of-type { margin-top: 0; }
      input, textarea, select {
        width: 100%;
        padding: 0.65rem 0.85rem;
        border: 1px solid var(--border);
        border-radius: 10px;
        font: inherit;
        font-size: 0.9375rem;
        color: var(--text);
        background: #fff;
        transition: border-color 0.15s, box-shadow 0.15s;
      }
      input:focus, textarea:focus, select:focus {
        outline: none;
        border-color: var(--accent-light);
        box-shadow: 0 0 0 3px rgba(2, 132, 199, 0.15);
      }
      textarea { min-height: 88px; resize: vertical; }
      .row2 {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.875rem;
      }
      @media (max-width: 520px) { .row2 { grid-template-columns: 1fr; } }
      .btn-row {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        margin-top: 1rem;
      }
      button {
        font-family: inherit;
        font-size: 0.875rem;
        font-weight: 600;
        padding: 0.55rem 1rem;
        border-radius: 10px;
        cursor: pointer;
        border: none;
        transition: transform 0.08s, opacity 0.15s, box-shadow 0.15s;
      }
      button:active { transform: scale(0.98); }
      button:disabled { opacity: 0.55; cursor: not-allowed; transform: none; }
      .btn-primary {
        background: linear-gradient(180deg, #0369a1 0%, var(--accent) 100%);
        color: #fff;
        box-shadow: 0 2px 8px rgba(12, 74, 110, 0.28);
      }
      .btn-primary:hover:not(:disabled) {
        box-shadow: 0 4px 14px rgba(12, 74, 110, 0.35);
      }
      .btn-secondary {
        background: var(--surface);
        color: var(--text);
        border: 1px solid var(--border);
      }
      .btn-secondary:hover:not(:disabled) {
        background: #f8fafc;
      }
      .hint {
        font-size: 0.75rem;
        color: var(--muted);
        margin: 0.65rem 0 0;
      }
      .hint code {
        font-size: 0.7rem;
        background: #f1f5f9;
        padding: 0.12rem 0.35rem;
        border-radius: 4px;
      }
      .panel-out {
        position: sticky;
        top: 1rem;
      }
      .panel-out .card {
        min-height: 320px;
        display: flex;
        flex-direction: column;
      }
      pre#out {
        flex: 1;
        margin: 0;
        margin-top: 0.75rem;
        white-space: pre-wrap;
        word-break: break-word;
        background: #0f172a;
        color: #e2e8f0;
        padding: 1rem;
        border-radius: 10px;
        font-size: 0.78rem;
        line-height: 1.45;
        overflow: auto;
        max-height: min(58vh, 520px);
        font-family: ui-monospace, "Cascadia Code", "SF Mono", Menlo, monospace;
      }
      .footer {
        margin-top: 2rem;
        text-align: center;
        font-size: 0.75rem;
        color: var(--muted);
      }
      .status-line {
        font-size: 0.8125rem;
        color: var(--muted);
        min-height: 1.25rem;
      }
      .asset-preview-wrap {
        margin-top: 0.75rem;
        padding-top: 0.75rem;
        border-top: 1px solid var(--border);
      }
      .asset-preview-h {
        font-size: 0.6875rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: var(--muted);
        margin-bottom: 0.5rem;
      }
      .asset-preview-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(132px, 1fr));
        gap: 0.5rem;
      }
      .asset-preview-body img,
      .asset-preview-body video {
        width: 100%;
        max-height: min(42vh, 320px);
        object-fit: contain;
        border-radius: 10px;
        border: 1px solid var(--border);
        background: #f8fafc;
      }
      .asset-preview-empty {
        font-size: 0.8125rem;
        color: var(--muted);
        margin: 0;
      }
      .asset-preview-empty code {
        font-size: 0.75rem;
        background: #f1f5f9;
        padding: 0.1rem 0.3rem;
        border-radius: 4px;
      }
      pre#out.gen-out-pending {
        opacity: 0.6;
      }
      .gen-loading-root {
        margin: 0.75rem 0 0;
        border-radius: 10px;
        border: 1px solid var(--border);
        background: linear-gradient(145deg, #f8fafc 0%, #f1f5f9 100%);
        box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06);
        overflow: hidden;
      }
      .gen-loading-root[hidden] {
        display: none !important;
      }
      .gen-loading-row {
        display: flex;
        align-items: flex-start;
        gap: 1rem;
        padding: 1rem 1.1rem 0.85rem;
      }
      .gen-loading-spinner {
        width: 2.35rem;
        height: 2.35rem;
        border-radius: 50%;
        border: 3px solid #e2e8f0;
        border-top-color: var(--accent);
        animation: gen-spin 0.88s linear infinite;
        flex-shrink: 0;
        margin-top: 0.1rem;
      }
      @keyframes gen-spin {
        to { transform: rotate(360deg); }
      }
      .gen-loading-text {
        flex: 1;
        min-width: 0;
      }
      .gen-loading-title {
        font-weight: 700;
        font-size: 0.9rem;
        color: var(--text);
        letter-spacing: -0.01em;
      }
      .gen-loading-detail {
        font-size: 0.8125rem;
        color: var(--muted);
        margin-top: 0.35rem;
        line-height: 1.45;
      }
      .gen-loading-meta {
        flex-shrink: 0;
        font-variant-numeric: tabular-nums;
        font-size: 0.78rem;
        font-weight: 700;
        color: var(--accent);
        padding-top: 0.15rem;
      }
      .gen-loading-track {
        height: 3px;
        background: #e2e8f0;
        margin: 0 1.1rem 1rem;
        border-radius: 3px;
        overflow: hidden;
      }
      .gen-loading-track::after {
        content: "";
        display: block;
        height: 100%;
        width: 42%;
        background: linear-gradient(90deg, var(--accent), #38bdf8);
        border-radius: 3px;
        animation: gen-indeterminate 1.25s ease-in-out infinite;
      }
      @keyframes gen-indeterminate {
        0% { transform: translateX(-100%); }
        100% { transform: translateX(320%); }
      }
      .studio-card .card-h h3 { font-size: 1.05rem; }
      .studio-lead {
        margin: 0 0 1rem;
        font-size: 0.9rem;
        color: var(--muted);
        line-height: 1.5;
      }
      .source-pills {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        margin-bottom: 0.75rem;
      }
      .source-pills label {
        margin: 0;
        cursor: pointer;
      }
      .source-pills input { position: absolute; opacity: 0; pointer-events: none; }
      .source-pills span {
        display: block;
        padding: 0.45rem 0.9rem;
        border-radius: 999px;
        border: 1px solid var(--border);
        font-size: 0.8125rem;
        font-weight: 600;
        color: var(--muted);
        background: #f8fafc;
        transition: border-color 0.15s, color 0.15s, background 0.15s;
      }
      .source-pills input:focus-visible + span {
        outline: 2px solid var(--accent-light);
        outline-offset: 2px;
      }
      .source-pills input:checked + span {
        border-color: var(--accent-light);
        color: var(--accent);
        background: #e0f2fe;
      }
      .studio-aspect-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.65rem;
        margin-top: 0.35rem;
      }
      .studio-aspect-grid label { margin: 0; cursor: pointer; }
      .studio-aspect-grid input { position: absolute; opacity: 0; }
      .aspect-tile {
        display: block;
        padding: 0.85rem 1rem;
        border-radius: 12px;
        border: 2px solid var(--border);
        background: #f8fafc;
        transition: border-color 0.15s, box-shadow 0.15s;
      }
      .aspect-tile strong { display: block; font-size: 0.9rem; color: var(--text); }
      .aspect-tile small { font-size: 0.75rem; color: var(--muted); }
      .studio-aspect-grid input:checked + .aspect-tile {
        border-color: var(--accent-light);
        box-shadow: 0 0 0 3px rgba(2, 132, 199, 0.12);
        background: #fff;
      }
      .chk-row {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin-top: 1rem;
        font-size: 0.875rem;
        color: var(--text);
      }
      .chk-row input { width: auto; margin: 0; }
      .studio-toggle-card {
        margin-top: 0.85rem;
        padding: 0.9rem 1rem;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
      }
      label.studio-toggle-label {
        display: flex;
        align-items: flex-start;
        gap: 0.75rem;
        margin: 0;
        cursor: pointer;
        text-transform: none;
        letter-spacing: normal;
        font-weight: 400;
        color: var(--text);
      }
      .studio-toggle-input {
        width: 1.125rem;
        height: 1.125rem;
        min-width: 1.125rem;
        margin: 0.2rem 0 0 0;
        flex-shrink: 0;
        accent-color: var(--accent);
        cursor: pointer;
      }
      .studio-toggle-body {
        display: flex;
        flex-direction: column;
        gap: 0.3rem;
        min-width: 0;
        flex: 1;
      }
      .studio-toggle-title {
        font-size: 0.9375rem;
        font-weight: 600;
        color: var(--text);
        letter-spacing: -0.015em;
        line-height: 1.35;
      }
      .studio-toggle-desc {
        font-size: 0.8125rem;
        font-weight: 400;
        color: var(--muted);
        line-height: 1.5;
        max-width: 42rem;
      }
      .visually-hidden {
        position: absolute;
        width: 1px;
        height: 1px;
        padding: 0;
        margin: -1px;
        overflow: hidden;
        clip: rect(0, 0, 0, 0);
        white-space: nowrap;
        border: 0;
      }
      .studio-fetch-row {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        align-items: stretch;
        margin-top: 0.35rem;
      }
      .studio-fetch-row input[type="text"],
      .studio-fetch-row input[type="url"] {
        flex: 1;
        min-width: 12rem;
      }
      .studio-advanced-hint {
        margin-top: 0.75rem;
        font-size: 0.8125rem;
        color: var(--muted);
      }
      .studio-advanced-hint summary {
        cursor: pointer;
        font-weight: 600;
        color: var(--text);
        list-style: none;
      }
      .studio-advanced-hint summary::-webkit-details-marker { display: none; }
      .studio-advanced-hint[open] summary { margin-bottom: 0.5rem; }
      .studio-advanced-hint .hint { margin-top: 0; }
      .btn-generate-campaign {
        margin-top: 1.1rem;
        width: 100%;
        padding: 0.75rem 1.25rem;
        font-size: 0.95rem;
        border-radius: 12px;
      }
      .campaign-plan-note {
        font-size: 0.8125rem;
        color: var(--muted);
        margin-bottom: 0.75rem;
        padding: 0.65rem 0.75rem;
        background: #f1f5f9;
        border-radius: 10px;
        line-height: 1.45;
      }
      .campaign-job-block {
        margin-top: 0.85rem;
        padding-top: 0.85rem;
        border-top: 1px solid var(--border);
      }
      .campaign-job-h {
        font-size: 0.6875rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--accent);
        margin-bottom: 0.45rem;
      }
      .api-out-details {
        margin-top: 0.85rem;
        border-radius: 10px;
        border: 1px solid var(--border);
        background: #0f172a;
        overflow: hidden;
      }
      .api-out-details summary {
        cursor: pointer;
        list-style: none;
        padding: 0.55rem 0.75rem;
        font-size: 0.8125rem;
        font-weight: 600;
        color: #e2e8f0;
        background: #1e293b;
        display: flex;
        align-items: center;
        gap: 0.45rem;
      }
      .api-out-details summary::-webkit-details-marker { display: none; }
      .api-out-details summary::before {
        content: "▸";
        font-size: 0.65rem;
        opacity: 0.8;
      }
      .api-out-details[open] summary::before { content: "▾"; }
      .api-out-badge {
        font-size: 0.65rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #94a3b8;
        margin-left: auto;
      }
      pre#out.api-out-pre {
        margin: 0;
        max-height: min(38vh, 320px);
        overflow: auto;
        padding: 0.75rem;
        font-size: 0.75rem;
        line-height: 1.45;
        background: #0f172a;
        color: #e2e8f0;
        border: none;
        border-radius: 0;
      }
      .campaign-job-err {
        padding: 0.6rem 0.75rem;
        margin-top: 0.5rem;
        border-radius: 8px;
        background: #fef2f2;
        border: 1px solid #fecaca;
        color: #991b1b;
        font-size: 0.8125rem;
        line-height: 1.45;
      }
    </style>
  </head>
  <body>
    <header class="topbar">
      <div class="topbar-inner">
        <div class="brand">
          <div class="brand-mark">SP</div>
          <div>
            <h1>SalesPal</h1>
            <p>Marketing studio · Lead tools</p>
          </div>
        </div>
        <span class="badge">Preview · Vertex AI</span>
      </div>
    </header>

    <div class="shell">
      <div class="hero">
        <h2>AI ad studio</h2>
        <p>
          Add your website, PDF, or brief notes only. The system analyzes the content, infers audience and angles, and produces image, carousel, and long-form video creatives automatically — no prompt writing.
        </p>
      </div>

      <div class="layout">
        <div class="main-col">
          <div class="card">
            <div class="card-h">
              <div>
                <h3>System status</h3>
                <span>Health &amp; readiness probes</span>
              </div>
            </div>
            <div class="btn-row">
              <button type="button" class="btn-secondary" id="btn-health" onclick="runHealth()">Check health</button>
              <button type="button" class="btn-secondary" id="btn-ready" onclick="runReady()">Check database ready</button>
            </div>
            <p class="hint">Calls <code>GET /_healthz</code> and <code>GET /readyz</code>.</p>
            <div class="status-line" id="status-hint"></div>
          </div>

          <div class="card">
            <div class="card-h">
              <div>
                <h3>Lead capture</h3>
                <span>Marketing ingestion · Firestore</span>
              </div>
            </div>
            <div class="row2">
              <div>
                <label for="lead_brand">Brand ID</label>
                <input id="lead_brand" value="demo" autocomplete="off" />
              </div>
              <div>
                <label for="lead_source">Source</label>
                <input id="lead_source" value="demo_ui" autocomplete="off" />
              </div>
            </div>
            <div class="row2">
              <div>
                <label for="lead_name">Name</label>
                <input id="lead_name" value="Test Lead" autocomplete="name" />
              </div>
              <div>
                <label for="lead_phone">Phone</label>
                <input id="lead_phone" value="9999999999" autocomplete="tel" />
              </div>
            </div>
            <label for="lead_message">Message</label>
            <textarea id="lead_message">Interested in SalesPal demo</textarea>
            <div class="btn-row">
              <button type="button" class="btn-primary" id="btn-lead" onclick="createLead()">Create lead</button>
              <button type="button" class="btn-secondary" id="btn-list" onclick="listLeads()">List leads</button>
            </div>
            <label for="last_lead_id">Last lead ID</label>
            <input id="last_lead_id" placeholder="Populated after create" autocomplete="off" />
            <div class="btn-row">
              <button type="button" class="btn-primary" id="btn-zoho" onclick="pushZoho()">Push to Zoho CRM</button>
            </div>
            <p class="hint">Zoho requires OAuth credentials on Cloud Run. Errors appear in the response panel.</p>
          </div>

          <div class="card studio-card">
            <div class="card-h">
              <div>
                <h3>Generate full campaign</h3>
                <span>Add one source — AI picks formats</span>
              </div>
            </div>
            <p class="studio-lead">Text, link, or PDF below. Optional brand fields help the AI; leave them blank if you prefer.</p>
            <label>What are you adding?</label>
            <div class="source-pills" id="studio_source_pills">
              <label><input type="radio" name="studio_source" value="text" checked /><span>Brief text</span></label>
              <label><input type="radio" name="studio_source" value="url" /><span>Website URL</span></label>
              <label><input type="radio" name="studio_source" value="pdf" /><span>PDF document</span></label>
            </div>
            <label>Brand (optional)</label>
            <label for="studio_fetch_url" class="visually-hidden">Page URL to auto-fill brand fields</label>
            <div class="studio-fetch-row" id="studio-fetch-anchor">
              <input id="studio_fetch_url" type="text" inputmode="url" placeholder="Paste URL to auto-fill brand →" autocomplete="url" aria-label="URL to fetch brand hints from" />
              <button type="button" class="btn-secondary" id="btn-fetch-website" onclick="fetchWebsiteHints()">Fetch</button>
            </div>
            <label for="studio_brand_name">Brand name</label>
            <input id="studio_brand_name" type="text" placeholder="Company or product name" autocomplete="organization" />
            <label for="studio_brand_tagline">Tagline</label>
            <input id="studio_brand_tagline" type="text" placeholder="Optional slogan" autocomplete="off" />
            <label for="studio_logo_text">Logo text</label>
            <input id="studio_logo_text" type="text" placeholder="Short text on screen (e.g. ACME)" autocomplete="off" />
            <label for="studio_logo_image">Logo file</label>
            <input id="studio_logo_image" type="file" accept="image/png,image/jpeg,image/jpg,image/webp,.png,.jpg,.jpeg,.webp" />
            <label for="studio_primary_market">Market</label>
            <input id="studio_primary_market" type="text" value="India" placeholder="Country / region" autocomplete="off" />
            <label for="studio_video_language">Video language</label>
            <select id="studio_video_language">
              <option value="auto" selected>Auto (brief-driven)</option>
              <option value="en">English</option>
              <option value="hi">Hindi</option>
              <option value="hinglish">Hinglish</option>
              <option value="ta">Tamil</option>
              <option value="te">Telugu</option>
              <option value="ml">Malayalam</option>
              <option value="kn">Kannada</option>
              <option value="mr">Marathi</option>
              <option value="bn">Bengali</option>
              <option value="gu">Gujarati</option>
              <option value="pa">Punjabi</option>
              <option value="ur">Urdu</option>
              <option value="es">Spanish</option>
              <option value="fr">French</option>
              <option value="de">German</option>
              <option value="ar">Arabic</option>
              <option value="pt">Portuguese</option>
              <option value="id">Indonesian</option>
            </select>
            <label for="studio_objective">Objective</label>
            <input id="studio_objective" type="text" placeholder="e.g. Leads, awareness, installs" autocomplete="off" />
            <label for="studio_on_screen_lines">Exact lines on video (optional)</label>
            <textarea id="studio_on_screen_lines" style="min-height:72px" placeholder="One line per row — copied exactly on video" autocomplete="off"></textarea>
            <div class="studio-toggle-card">
              <label class="studio-toggle-label" for="studio_logo_persistent">
                <input class="studio-toggle-input" id="studio_logo_persistent" type="checkbox" checked />
                <span class="studio-toggle-body">
                  <span class="studio-toggle-title">Show logo on every video clip</span>
                </span>
              </label>
            </div>
            <label for="studio_text" id="studio_text_label">Your brief or notes</label>
            <textarea id="studio_text" style="min-height:120px" placeholder="Product, audience, benefits, tone — or one URL if &quot;Website URL&quot; is selected" autocomplete="off"></textarea>
            <label for="studio_pdf" id="studio_pdf_label" hidden>PDF file</label>
            <input type="file" id="studio_pdf" accept="application/pdf,.pdf" hidden />
            <label>Video shape</label>
            <div class="studio-aspect-grid">
              <label>
                <input type="radio" name="studio_aspect" value="9:16" />
                <span class="aspect-tile"><strong>Vertical 9:16</strong><small>Stories, Reels, Shorts</small></span>
              </label>
              <label>
                <input type="radio" name="studio_aspect" value="1:1" />
                <span class="aspect-tile"><strong>Square 1:1</strong><small>Feed, brand posts</small></span>
              </label>
              <label>
                <input type="radio" name="studio_aspect" value="16:9" checked />
                <span class="aspect-tile"><strong>Landscape 16:9</strong><small>YouTube, web, display</small></span>
              </label>
            </div>
            <label>What to create</label>
            <div class="chk-row" style="flex-direction:column;align-items:flex-start;gap:0.5rem">
              <div class="chk-row" style="margin-top:0">
                <input type="checkbox" id="studio_gen_image" checked />
                <label for="studio_gen_image" style="margin:0;text-transform:none;font-weight:500;letter-spacing:normal">Image ad</label>
              </div>
              <div class="chk-row" style="margin-top:0;gap:0.6rem;align-items:center">
                <label for="studio_image_n" style="margin:0;text-transform:none;font-weight:500;letter-spacing:normal;min-width:9rem"># images</label>
                <input id="studio_image_n" type="number" min="1" max="10" step="1" value="1" style="width:6.5rem" />
              </div>
              <div class="chk-row" style="margin-top:0">
                <input type="checkbox" id="studio_gen_carousel" checked />
                <label for="studio_gen_carousel" style="margin:0;text-transform:none;font-weight:500;letter-spacing:normal">Carousel</label>
              </div>
              <div class="chk-row" style="margin-top:0;gap:0.6rem;align-items:center">
                <label for="studio_carousel_n" style="margin:0;text-transform:none;font-weight:500;letter-spacing:normal;min-width:9rem"># carousel slides</label>
                <input id="studio_carousel_n" type="number" min="3" max="10" step="1" value="5" style="width:6.5rem" />
              </div>
              <div class="chk-row" style="margin-top:0">
                <input type="checkbox" id="studio_gen_video" checked />
                <label for="studio_gen_video" style="margin:0;text-transform:none;font-weight:500;letter-spacing:normal">Long video (slow — keep page open)</label>
              </div>
              <div class="chk-row" style="margin-top:0;gap:0.6rem;align-items:center">
                <label for="studio_video_minutes" style="margin:0;text-transform:none;font-weight:500;letter-spacing:normal;min-width:9rem">Video length</label>
                <select id="studio_video_minutes" style="width:10.5rem">
                  <option value="0.5">0.5 min (30s)</option>
                  <option value="1" selected>1 min (60s)</option>
                  <option value="2">2 min (120s)</option>
                  <option value="3">3 min (180s)</option>
                  <option value="5">5 min (300s)</option>
                  <option value="10">10 min (600s)</option>
                </select>
              </div>
            </div>
            <p class="hint" id="studio_credit_hint" style="margin-top:0.65rem">Estimated credits: —</p>
            <button type="button" class="btn-primary btn-generate-campaign" id="btn-campaign" onclick="runAutonomousCampaign()">Generate campaign</button>
            <details class="studio-advanced-hint">
              <summary>API &amp; video notes</summary>
              <p class="hint">Uses <code>POST /v1/marketing/campaign</code> then <code>/execute</code>. Video needs bucket config on the service. Lines under “Exact lines on video” are <strong>burned in with FFmpeg</strong> (Veo cannot typeset reliably). Override: <code>VERTEX_VIDEO_BURN_IN_CAPTIONS=0</code>.</p>
            </details>
          </div>
        </div>

        <aside class="panel-out">
          <div class="card">
            <div class="card-h">
              <div>
                <h3>Results</h3>
                <span>Brief, plan &amp; previews</span>
              </div>
            </div>
            <p class="hint" style="margin:0">Plans, QA, and video/image previews show above. Expand <strong>Full API response</strong> for raw JSON (opens automatically if a job failed).</p>
            <div id="gen-loading-root" class="gen-loading-root" hidden aria-live="polite" aria-busy="false">
              <div class="gen-loading-row">
                <div class="gen-loading-spinner" aria-hidden="true"></div>
                <div class="gen-loading-text">
                  <div class="gen-loading-title" id="gen-loading-title">Generating</div>
                  <div class="gen-loading-detail" id="gen-loading-detail"></div>
                </div>
                <div class="gen-loading-meta"><span id="gen-loading-elapsed">0s</span></div>
              </div>
              <div class="gen-loading-track" aria-hidden="true"></div>
            </div>
            <div id="asset-preview-wrap" class="asset-preview-wrap" hidden>
              <div class="asset-preview-h" id="asset-preview-title">Preview</div>
              <div id="asset-preview-body" class="asset-preview-body"></div>
            </div>
            <details id="out-details" class="api-out-details">
              <summary class="api-out-summary">Full API response <span class="api-out-badge">JSON</span></summary>
              <pre id="out" class="api-out-pre">Select an action to view the response.</pre>
            </details>
          </div>
        </aside>
      </div>

      <p class="footer">
        SalesPal demo console — not for production end users. Configure secrets only via GCP / Secret Manager.
      </p>
    </div>

    <script>
      const outEl = document.getElementById('out');
      const outDetailsEl = document.getElementById('out-details');
      const hintEl = document.getElementById('status-hint');
      const assetPreviewWrap = document.getElementById('asset-preview-wrap');
      const assetPreviewBody = document.getElementById('asset-preview-body');
      const demoKey = new URLSearchParams(location.search).get('key') || '';

      function isAssetJob(obj) {
        return obj && typeof obj === 'object' && !Array.isArray(obj) &&
          ['image', 'carousel', 'video'].indexOf(obj.asset_type) !== -1;
      }
      function clearAssetPreview() {
        if (!assetPreviewWrap || !assetPreviewBody) return;
        assetPreviewWrap.hidden = true;
        assetPreviewBody.innerHTML = '';
      }
      function dataUrlFromImageB64(b64) {
        if (!b64 || typeof b64 !== 'string') return '';
        var s = b64.replace(/\\s/g, '');
        if (!s) return '';
        var mime = 'image/png';
        if (s.indexOf('/9j/') === 0) mime = 'image/jpeg';
        else if (s.indexOf('iVBORw') === 0) mime = 'image/png';
        else if (s.indexOf('R0lGOD') === 0) mime = 'image/gif';
        else if (s.indexOf('UklGR') === 0) mime = 'image/webp';
        return 'data:' + mime + ';base64,' + s;
      }
      function mediaProxyUrl(gsUri) {
        if (!gsUri) return '';
        var u = '/demo/asset-media?gs=' + encodeURIComponent(gsUri);
        if (demoKey) u += '&key=' + encodeURIComponent(demoKey);
        return u;
      }
      function buildPreviewPartsFromOutput(out) {
        if (!out || typeof out !== 'object') return [];
        var parts = [];
        var b64 = out.image_base64;
        var uri = out.image_gcs_uri;
        var imgs = out.images_base64;
        var uris = out.images_gcs_uris;
        var videos = out.videos;
        if (typeof b64 === 'string' && b64.trim()) {
          var du = dataUrlFromImageB64(b64);
          if (du) parts.push('<img alt="Generated image" src="' + du.replace(/"/g, '&quot;') + '" />');
        } else if (typeof uri === 'string' && uri.indexOf('gs://') === 0) {
          parts.push('<img alt="Generated image" src="' + mediaProxyUrl(uri).replace(/"/g, '&quot;') + '" />');
        }
        if (Array.isArray(imgs) && imgs.length) {
          var cells = imgs.map(function (x) {
            var d = dataUrlFromImageB64(x);
            return d ? '<img alt="Carousel slide" src="' + d.replace(/"/g, '&quot;') + '" />' : '';
          }).filter(Boolean);
          if (cells.length) parts.push('<div class="asset-preview-grid">' + cells.join('') + '</div>');
        } else if (Array.isArray(uris) && uris.length) {
          var uriCells = uris.map(function (g) {
            if (typeof g === 'string' && g.indexOf('gs://') === 0)
              return '<img alt="Carousel slide" src="' + mediaProxyUrl(g).replace(/"/g, '&quot;') + '" />';
            return '';
          }).filter(Boolean);
          if (uriCells.length) parts.push('<div class="asset-preview-grid">' + uriCells.join('') + '</div>');
        }
        if (Array.isArray(videos) && videos.length) {
          for (var i = 0; i < videos.length; i++) {
            var v = videos[i];
            if (!v || typeof v !== 'object') continue;
            var gcs = (v.gcs_uri || '').trim();
            var vb64 = v.bytes_base64;
            var mt = (v.mime_type || 'video/mp4').trim() || 'video/mp4';
            if (gcs && gcs.indexOf('gs://') === 0) {
              parts.push('<video controls playsinline preload="metadata" src="' +
                mediaProxyUrl(gcs).replace(/"/g, '&quot;') + '"></video>');
            } else if (typeof vb64 === 'string' && vb64.trim()) {
              var raw = vb64.replace(/\\s/g, '');
              var safeMt = mt.replace(/"/g, '');
              parts.push('<video controls playsinline preload="metadata" src="data:' + safeMt + ';base64,' + raw + '"></video>');
            }
          }
        }
        if (out.output_omitted) {
          parts.push('<p class="asset-preview-empty">Stored copy omitted media (size limit). Ensure <code>META_MEDIA_BUCKET</code> is set so images land in GCS, or use base64 in the response when running without offload.</p>');
        }
        return parts;
      }
      function escapeHtml(t) {
        if (t == null) return '';
        return String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
      }
      function renderAssetPreview(job) {
        if (!assetPreviewWrap || !assetPreviewBody) return;
        var titleEl = document.getElementById('asset-preview-title');
        if (titleEl) titleEl.textContent = 'Preview';
        var parts = buildPreviewPartsFromOutput(job && job.output);
        if (!parts.length) {
          clearAssetPreview();
          return;
        }
        assetPreviewBody.innerHTML = parts.join('');
        assetPreviewWrap.hidden = false;
      }
      function renderCampaignPreview(data) {
        if (!assetPreviewWrap || !assetPreviewBody) return;
        var titleEl = document.getElementById('asset-preview-title');
        if (titleEl) titleEl.textContent = 'Campaign preview';
        var plan = data.prompts_used && data.prompts_used.autonomous_plan;
        var prod = data.prompts_used && data.prompts_used.production_plan;
        var sa = data.prompts_used && data.prompts_used.script_audio;
        var chunks = [];
        if (plan && plan.planning_rationale) {
          chunks.push('<div class="campaign-plan-note"><strong>AI plan</strong> · ' + escapeHtml(plan.planning_rationale) + '</div>');
        }
        if (prod && prod.one_line_summary) {
          chunks.push('<div class="campaign-plan-note"><strong>Production plan</strong> · ' + escapeHtml(prod.one_line_summary) + '</div>');
        }
        if (sa && sa.one_line_audio_summary) {
          chunks.push('<div class="campaign-plan-note"><strong>Script &amp; audio</strong> · ' + escapeHtml(sa.one_line_audio_summary) + '</div>');
        }
        if (data.stage5 && data.stage5.jobs && data.stage5.jobs.length) {
          var st = data.stage5.status ? String(data.stage5.status) : '';
          var parts = data.stage5.jobs.map(function (j) {
            if (!j || !j.asset_type) return '';
            if (j.count) return j.asset_type + ' ×' + j.count;
            if (j.panels) return j.asset_type + ' ×' + j.panels;
            if (j.veo_segments) return j.asset_type + ' (' + j.veo_segments + ' clips)';
            return j.asset_type;
          }).filter(Boolean);
          var line = '<strong>Stage 5</strong> · ' + parts.join(', ') + (st ? ' · ' + st : '');
          chunks.push('<div class="campaign-plan-note">' + line + '</div>');
        }
        if (data.stage6) {
          var s6 = data.stage6;
          if (s6.overall_status) {
            var s6sum = (s6.llm_review && s6.llm_review.client_summary) ? s6.llm_review.client_summary : '';
            var s6line = '<strong>Stage 6 QA</strong> · ' + escapeHtml(String(s6.overall_status));
            if (s6sum) s6line += ' · ' + escapeHtml(s6sum);
            if (s6.issues && s6.issues.length) s6line += ' (' + s6.issues.length + ' issue(s))';
            chunks.push('<div class="campaign-plan-note">' + s6line + '</div>');
          } else if (s6.message) {
            chunks.push('<div class="campaign-plan-note"><strong>Stage 6</strong> · ' + escapeHtml(String(s6.message)) + '</div>');
          }
        }
        (data.jobs || []).forEach(function (job) {
          if (!isAssetJob(job)) return;
          var label = String(job.asset_type || 'asset').toUpperCase();
          var inner = buildPreviewPartsFromOutput(job.output).join('');
          var err = job.status === 'failed' && job.error
            ? '<div class="campaign-job-err" role="alert">' + escapeHtml(job.error) + '</div>'
            : '';
          if (inner || err) chunks.push('<div class="campaign-job-block"><div class="campaign-job-h">' + label + '</div>' + inner + err + '</div>');
        });
        if (!chunks.length) {
          clearAssetPreview();
          return;
        }
        assetPreviewBody.innerHTML = chunks.join('');
        assetPreviewWrap.hidden = false;
      }
      function show(obj) {
        outEl.textContent = (typeof obj === 'string') ? obj : JSON.stringify(obj, null, 2);
        var isCampaign = obj && typeof obj === 'object' && obj.brief && Array.isArray(obj.jobs);
        if (outDetailsEl) {
          outDetailsEl.open = !isCampaign;
        }
        if (isCampaign) {
          var hasFail = (obj.jobs || []).some(function (j) { return j && j.status === 'failed'; });
          if (outDetailsEl) {
            outDetailsEl.open = !!hasFail;
          }
          renderCampaignPreview(obj);
          return;
        }
        if (isAssetJob(obj)) renderAssetPreview(obj);
        else if (obj && typeof obj === 'object' && Array.isArray(obj.jobs) && obj.jobs.length) {
          var pick = null;
          var k = 0;
          for (k = 0; k < obj.jobs.length; k++) {
            if (isAssetJob(obj.jobs[k]) && obj.jobs[k].asset_type === 'video' && obj.jobs[k].output) {
              pick = obj.jobs[k];
              break;
            }
          }
          if (!pick) {
            for (var i = obj.jobs.length - 1; i >= 0; i--) {
              if (isAssetJob(obj.jobs[i]) && obj.jobs[i].output) { pick = obj.jobs[i]; break; }
            }
          }
          if (!pick) {
            for (var j = obj.jobs.length - 1; j >= 0; j--) {
              if (isAssetJob(obj.jobs[j])) { pick = obj.jobs[j]; break; }
            }
          }
          if (pick) renderAssetPreview(pick);
          else clearAssetPreview();
        } else clearAssetPreview();
      }
      function setBusy(id, busy) {
        const el = document.getElementById(id);
        if (el) el.disabled = !!busy;
      }
      var genLoadingTimer = null;
      var genLoadingStart = 0;
      function formatGenElapsed(ms) {
        var s = Math.floor(ms / 1000);
        var m = Math.floor(s / 60);
        s = s % 60;
        if (m > 0) return m + ':' + (s < 10 ? '0' : '') + s;
        return s + 's';
      }
      function startAssetGenerationLoading(title, detail, preservePreview) {
        var root = document.getElementById('gen-loading-root');
        var titleEl = document.getElementById('gen-loading-title');
        var detailEl = document.getElementById('gen-loading-detail');
        var elapsedEl = document.getElementById('gen-loading-elapsed');
        if (!root || !titleEl || !detailEl || !elapsedEl) return;
        titleEl.textContent = title;
        detailEl.textContent = detail || '';
        root.hidden = false;
        root.setAttribute('aria-busy', 'true');
        outEl.classList.add('gen-out-pending');
        outEl.textContent = 'Response JSON will appear here when the request finishes.';
        if (!preservePreview) clearAssetPreview();
        genLoadingStart = Date.now();
        if (genLoadingTimer) clearInterval(genLoadingTimer);
        elapsedEl.textContent = '0s';
        genLoadingTimer = setInterval(function () {
          elapsedEl.textContent = formatGenElapsed(Date.now() - genLoadingStart);
        }, 1000);
        hintEl.textContent = 'Generating…';
      }
      function stopAssetGenerationLoading() {
        if (genLoadingTimer) {
          clearInterval(genLoadingTimer);
          genLoadingTimer = null;
        }
        var root = document.getElementById('gen-loading-root');
        if (root) {
          root.hidden = true;
          root.setAttribute('aria-busy', 'false');
        }
        outEl.classList.remove('gen-out-pending');
      }
      async function callGet(path) {
        hintEl.textContent = 'Requesting ' + path + '…';
        const r = await fetch(path, { method: 'GET' });
        const t = await r.text();
        hintEl.textContent = r.ok ? 'OK · HTTP ' + r.status : 'Error · HTTP ' + r.status;
        try { show(JSON.parse(t)); } catch { show(t); }
      }
      function runHealth() { setBusy('btn-health', true); callGet('/_healthz').finally(() => setBusy('btn-health', false)); }
      function runReady() { setBusy('btn-ready', true); callGet('/readyz').finally(() => setBusy('btn-ready', false)); }
      async function callJson(path, body) {
        const r = await fetch(path, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const t = await r.text();
        let data;
        try { data = JSON.parse(t); } catch { data = { raw: t }; }
        if (!r.ok) data._http = { status: r.status };
        hintEl.textContent = r.ok ? 'OK · HTTP ' + r.status : 'Error · HTTP ' + r.status;
        return data;
      }
      async function createLead() {
        setBusy('btn-lead', true);
        try {
        const body = {
          brand_id: document.getElementById('lead_brand').value.trim(),
          source: document.getElementById('lead_source').value.trim(),
          name: document.getElementById('lead_name').value.trim(),
          phone: document.getElementById('lead_phone').value.trim(),
          message: document.getElementById('lead_message').value,
          raw: { demo_ui: true },
        };
        const data = await callJson('/v1/marketing/leads', body);
        show(data);
        const id = data && data.lead && data.lead.id ? String(data.lead.id) : '';
        if (id) document.getElementById('last_lead_id').value = id;
        } finally { setBusy('btn-lead', false); }
      }
      async function listLeads() {
        setBusy('btn-list', true);
        hintEl.textContent = 'Loading leads…';
        try {
        const brand = document.getElementById('lead_brand').value.trim();
        const r = await fetch('/v1/marketing/leads?brand_id=' + encodeURIComponent(brand), { method: 'GET' });
        const t = await r.text();
        hintEl.textContent = r.ok ? 'OK · HTTP ' + r.status : 'Error · HTTP ' + r.status;
        try { show(JSON.parse(t)); } catch { show(t); }
        } finally { setBusy('btn-list', false); }
      }
      async function pushZoho() {
        const id = document.getElementById('last_lead_id').value.trim();
        if (!id) { show('Create a lead first, then push to Zoho.'); hintEl.textContent = ''; return; }
        setBusy('btn-zoho', true);
        try {
        const data = await callJson('/v1/integrations/zoho/push_lead/' + encodeURIComponent(id), {});
        show(data);
        } finally { setBusy('btn-zoho', false); }
      }
      async function fetchWebsiteHints() {
        var fetchIn = document.getElementById('studio_fetch_url');
        var ta = document.getElementById('studio_text');
        var bn = document.getElementById('studio_brand_name');
        var raw = fetchIn && (fetchIn.value || '').trim();
        if (!raw && bn) {
          var bv = (bn.value || '').trim();
          var bvl = bv.toLowerCase();
          if (bvl.indexOf('http://') === 0 || bvl.indexOf('https://') === 0) raw = bv;
        }
        if (!raw && studioSourceType() === 'url' && ta) {
          var tv = (ta.value || '').trim();
          var firstLine = tv;
          var ix = firstLine.indexOf("\\n");
          if (ix >= 0) firstLine = firstLine.slice(0, ix);
          ix = firstLine.indexOf("\\r");
          if (ix >= 0) firstLine = firstLine.slice(0, ix);
          var fll = firstLine.toLowerCase();
          if (fll.indexOf('http://') === 0 || fll.indexOf('https://') === 0) raw = firstLine.trim();
        }
        if (!raw) {
          show('Enter a URL in the fetch field, paste a URL in Brand name, or choose Website URL and put the URL in the source field.');
          hintEl.textContent = '';
          return;
        }
        setBusy('btn-fetch-website', true);
        hintEl.textContent = 'Reading website…';
        try {
          var data = await callJson('/v1/marketing/fetch-website-hints', { url: raw });
          if (data._http) {
            show(data);
            return;
          }
          if (bn && data.brand_name) bn.value = data.brand_name;
          var bt = document.getElementById('studio_brand_tagline');
          if (bt && data.brand_tagline) bt.value = data.brand_tagline;
          var lt = document.getElementById('studio_logo_text');
          if (lt && data.logo_text) lt.value = data.logo_text;
          var ob = document.getElementById('studio_objective');
          if (ob && data.objective) ob.value = data.objective;
          if (data.campaign_summary && ta) {
            var st = studioSourceType();
            if (st === 'url') {
              var urlPill = document.querySelector('input[name="studio_source"][value="url"]');
              if (urlPill) urlPill.checked = true;
              toggleStudioSource();
              ta.value = (data.url || raw).trim();
            } else {
              var cur = (ta.value || '').trim();
              if (!cur) ta.value = data.campaign_summary;
              else ta.value = cur + "\\n\\n" + data.campaign_summary;
            }
          } else if (studioSourceType() === 'url' && ta && (data.url || raw)) {
            var urlPill2 = document.querySelector('input[name="studio_source"][value="url"]');
            if (urlPill2) urlPill2.checked = true;
            toggleStudioSource();
            ta.value = (data.url || raw).trim();
          }
          if (fetchIn && !(fetchIn.value || '').trim()) fetchIn.value = (data.url || raw).trim();
          show(data);
          hintEl.textContent = 'Filled from website · HTTP 200';
        } catch (e) {
          show(String(e));
          hintEl.textContent = '';
        } finally {
          setBusy('btn-fetch-website', false);
        }
      }
      function studioSourceType() {
        var el = document.querySelector('input[name="studio_source"]:checked');
        return el ? el.value : 'text';
      }
      function toggleStudioSource() {
        var st = studioSourceType();
        var ta = document.getElementById('studio_text');
        var lab = document.getElementById('studio_text_label');
        var pdfL = document.getElementById('studio_pdf_label');
        var pdfI = document.getElementById('studio_pdf');
        if (!ta || !lab) return;
        if (st === 'pdf') {
          ta.hidden = true;
          lab.hidden = true;
          if (pdfL) pdfL.hidden = false;
          if (pdfI) pdfI.hidden = false;
        } else {
          ta.hidden = false;
          lab.hidden = false;
          if (pdfL) pdfL.hidden = true;
          if (pdfI) pdfI.hidden = true;
        }
        lab.textContent = st === 'url' ? 'Website URL' : 'Your brief or notes';
      }

      function clampInt(v, minV, maxV, fallback) {
        var n = parseInt(String(v || ''), 10);
        if (isNaN(n)) n = fallback;
        if (typeof minV === 'number') n = Math.max(minV, n);
        if (typeof maxV === 'number') n = Math.min(maxV, n);
        return n;
      }
      function parseMinutes(v, fallback) {
        var n = parseFloat(String(v || ''));
        if (!isFinite(n) || n <= 0) return fallback;
        return n;
      }
      function estimatedCredits(genImage, imageN, genCarousel, carouselN, genVideo, videoSeconds, clipSeconds) {
        // Heuristic estimate only; real billing depends on provider.
        var c = 0;
        if (genImage) c += Math.max(1, imageN || 1);
        if (genCarousel) c += Math.max(1, carouselN || 3);
        if (genVideo) {
          var clips = Math.max(1, Math.ceil((videoSeconds || 60) / Math.max(1, clipSeconds || 6)));
          c += clips * 5; // assume video clips are more expensive than a single image
        }
        return c;
      }
      function updateCreditHint() {
        var el = document.getElementById('studio_credit_hint');
        if (!el) return;
        var gImg = document.getElementById('studio_gen_image');
        var gCar = document.getElementById('studio_gen_carousel');
        var gVid = document.getElementById('studio_gen_video');
        var genImage = gImg ? gImg.checked : true;
        var genCarousel = gCar ? gCar.checked : true;
        var genVideo = gVid ? gVid.checked : true;
        var imageN = clampInt(document.getElementById('studio_image_n') && document.getElementById('studio_image_n').value, 1, 10, 1);
        var carouselN = clampInt(document.getElementById('studio_carousel_n') && document.getElementById('studio_carousel_n').value, 3, 10, 5);
        var minutes = parseMinutes(document.getElementById('studio_video_minutes') && document.getElementById('studio_video_minutes').value, 1);
        var videoSeconds = Math.round(minutes * 60);
        var clipSeconds = 6;
        var credits = estimatedCredits(genImage, imageN, genCarousel, carouselN, genVideo, videoSeconds, clipSeconds);
        var parts = [];
        if (genImage) parts.push(imageN + ' image' + (imageN === 1 ? '' : 's'));
        if (genCarousel) parts.push(carouselN + ' slide' + (carouselN === 1 ? '' : 's'));
        if (genVideo) parts.push(videoSeconds + 's video');
        el.textContent = 'Estimated credits: ' + credits + (parts.length ? ' · ' + parts.join(' + ') : '');
      }
      async function runAutonomousCampaign() {
        setBusy('btn-campaign', true);
        var aspectEl = document.querySelector('input[name="studio_aspect"]:checked');
        var aspect = aspectEl ? aspectEl.value : '16:9';
        var gImg = document.getElementById('studio_gen_image');
        var gCar = document.getElementById('studio_gen_carousel');
        var gVid = document.getElementById('studio_gen_video');
        var genImage = gImg ? gImg.checked : true;
        var genCarousel = gCar ? gCar.checked : true;
        var genVideo = gVid ? gVid.checked : true;
        if (!genImage && !genCarousel && !genVideo) {
          show('Choose at least one output: image, carousel, or video.');
          hintEl.textContent = '';
          setBusy('btn-campaign', false);
          return;
        }
        var st = studioSourceType();
        var imageN = clampInt(document.getElementById('studio_image_n') && document.getElementById('studio_image_n').value, 1, 10, 1);
        var carouselN = clampInt(document.getElementById('studio_carousel_n') && document.getElementById('studio_carousel_n').value, 3, 10, 5);
        var minutes = parseMinutes(document.getElementById('studio_video_minutes') && document.getElementById('studio_video_minutes').value, 1);
        var videoTotalSeconds = Math.round(minutes * 60);
        var ta = document.getElementById('studio_text');
        var studioVal = ta ? ta.value.trim() : '';
        if (st === 'pdf') {
          var pdfFile = document.getElementById('studio_pdf').files[0];
          if (!pdfFile) {
            show('Choose a PDF file.');
            hintEl.textContent = '';
            setBusy('btn-campaign', false);
            return;
          }
        } else if (st === 'url' && !studioVal) {
          show('Enter a website URL.');
          hintEl.textContent = '';
          setBusy('btn-campaign', false);
          return;
        } else if (st === 'text' && !studioVal) {
          show('Enter your brief or notes.');
          hintEl.textContent = '';
          setBusy('btn-campaign', false);
          return;
        }
        var parts = [];
        if (genImage) parts.push('image');
        if (genCarousel) parts.push('carousel');
        if (genVideo) parts.push('video');
        startAssetGenerationLoading(
          'Planning your campaign',
          'Analyzing your content and building the AI plan…',
          false
        );
        try {
          var brand = document.getElementById('lead_brand').value.trim() || 'demo';
          var body = {
            brand_id: brand,
            source_type: st,
            video_aspect_ratio: aspect,
            require_approval: false,
            generate_image: genImage,
            generate_carousel: genCarousel,
            generate_video: genVideo,
            plan_only: true,
            image_n: imageN,
            carousel_n: carouselN,
            video_total_seconds: videoTotalSeconds,
            video_clip_seconds: 6,
          };
          var bn = document.getElementById('studio_brand_name');
          var bt = document.getElementById('studio_brand_tagline');
          var lt = document.getElementById('studio_logo_text');
          var ob = document.getElementById('studio_objective');
          if (bn && (bn.value || '').trim()) body.brand_name = (bn.value || '').trim();
          if (bt && (bt.value || '').trim()) body.brand_tagline = (bt.value || '').trim();
          if (lt && (lt.value || '').trim()) body.logo_text = (lt.value || '').trim();
          if (ob && (ob.value || '').trim()) body.objective = (ob.value || '').trim();
          var pmEl = document.getElementById('studio_primary_market');
          if (pmEl && (pmEl.value || '').trim()) body.primary_market = (pmEl.value || '').trim();
          var vl = document.getElementById('studio_video_language');
          if (vl && (vl.value || '').trim()) body.video_language = (vl.value || '').trim();
          var osl = document.getElementById('studio_on_screen_lines');
          var lpp = document.getElementById('studio_logo_persistent');
          if (osl && (osl.value || '').trim()) body.on_screen_lines = (osl.value || '').trim();
          body.logo_persistent = lpp ? !!lpp.checked : true;
          var lf = document.getElementById('studio_logo_image');
          if (lf && lf.files && lf.files[0]) {
            var logoDataUrl = await new Promise(function (resolve, reject) {
              var lr = new FileReader();
              lr.onload = function () { resolve(lr.result); };
              lr.onerror = reject;
              lr.readAsDataURL(lf.files[0]);
            });
            if (typeof logoDataUrl === 'string') {
              body.logo_image_base64 = logoDataUrl;
              var ldSemi = logoDataUrl.indexOf(';');
              var ldComma = logoDataUrl.indexOf(',');
              if (ldSemi > 5 && ldComma > ldSemi) {
                body.logo_image_mime_type = logoDataUrl.slice(5, ldSemi);
              }
            }
          }
          if (st === 'pdf') {
            var f = document.getElementById('studio_pdf').files[0];
            var b64 = await new Promise(function (resolve, reject) {
              var r = new FileReader();
              r.onload = function () {
                var s = r.result;
                var i = s.indexOf(',');
                resolve(i >= 0 ? s.slice(i + 1) : s);
              };
              r.onerror = reject;
              r.readAsDataURL(f);
            });
            body.pdf_base64 = b64;
          } else if (st === 'url') {
            body.url = studioVal;
          } else {
            body.text = studioVal;
          }
          var planData = await callJson('/v1/marketing/campaign', body);
          stopAssetGenerationLoading();
          if (planData._http) {
            show(planData);
            return;
          }
          show(planData);
          startAssetGenerationLoading(
            'Generating assets',
            'Creating: ' + parts.join(', ') + '. ' +
            (genVideo ? 'Video can take many minutes — keep this page open.' : ''),
            true
          );
          var execBody = {
            brand_id: planData.brand_id || brand,
            video_aspect_ratio: aspect,
            require_approval: false,
            generate_image: genImage,
            generate_carousel: genCarousel,
            generate_video: genVideo,
            prompts_used: planData.prompts_used,
          };
          if (planData.brief) execBody.brief = planData.brief;
          var exec = await callJson('/v1/marketing/campaign/execute', execBody);
          stopAssetGenerationLoading();
          if (exec._http) {
            outEl.textContent = JSON.stringify(exec, null, 2);
            hintEl.textContent = 'Error · HTTP ' + exec._http.status;
            return;
          }
          var merged = Object.assign({}, planData, { jobs: exec.jobs || [], plan_only: false });
          if (exec.stage5) merged.stage5 = exec.stage5;
          if (exec.stage6) merged.stage6 = exec.stage6;
          show(merged);
        } catch (e) {
          stopAssetGenerationLoading();
          show(String(e));
          hintEl.textContent = '';
        } finally {
          stopAssetGenerationLoading();
          setBusy('btn-campaign', false);
        }
      }
      document.querySelectorAll('input[name="studio_source"]').forEach(function (el) {
        el.addEventListener('change', toggleStudioSource);
      });
      ['studio_gen_image','studio_gen_carousel','studio_gen_video','studio_image_n','studio_carousel_n','studio_video_minutes'].forEach(function (id) {
        var el = document.getElementById(id);
        if (!el) return;
        el.addEventListener('change', updateCreditHint);
        el.addEventListener('input', updateCreditHint);
      });
      toggleStudioSource();
      updateCreditHint();
    </script>
  </body>
</html>"""
    return Response(html, mimetype="text/html; charset=utf-8")


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


def _sync_qualification_to_zoho(lead_id: str, conv: Conversation) -> None:
    """When B/L/T qualification completes, update the Zoho Lead (pipeline / custom fields)."""
    if conv.state != "complete":
        return
    if (os.getenv("ZOHO_SYNC_QUALIFICATION") or "1").lower() in ("0", "false", "no"):
        return
    if not zoho.ready():
        return
    qual = conversation_to_qualification_dict(conv)
    payload = build_qualification_update_payload(qual)
    if not payload:
        return
    lead = leads.get(lead_id)
    if not lead:
        return
    raw = lead.raw if isinstance(lead.raw, dict) else {}
    if raw.get("qualification_synced_for_conv") == conv.id:
        return
    zcrm_id = str(raw.get("zcrm_id") or "").strip()
    if not zcrm_id:
        if lead.status == "captured":
            try:
                out = _push_lead_core(lead_id)
            except Exception:
                log.exception("zoho push before qualification update")
                return
            if out.get("status") != "pushed":
                log.warning("qualification zoho sync: could not create lead first: %s", out)
                return
            lead = leads.get(lead_id)
            if not lead:
                return
            raw = lead.raw if isinstance(lead.raw, dict) else {}
            zcrm_id = str(raw.get("zcrm_id") or "").strip()
        if not zcrm_id:
            log.warning("qualification zoho sync: missing zcrm_id for lead %s", lead_id)
            return
    try:
        resp = zoho.update_lead(zcrm_id, payload)
        ok, _, err = zoho_first_row_outcome(resp)
        if ok:
            leads.merge_raw(
                lead_id,
                {
                    "qualification_synced_for_conv": conv.id,
                    "zoho_qualification_synced_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        else:
            log.warning("qualification zoho update failed: %s", err)
    except Exception:
        log.exception("qualification zoho update")


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


@app.get("/_healthz")
def healthz_alias():
    return healthz()


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


@app.post("/v1/marketing/creative-brief")
def creative_brief_route():
    """Step 1: ingest URL / text / PDF and return a structured marketing brief (Vertex Gemini JSON)."""
    body = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "demo").strip()
    source_type = str(body.get("source_type") or "").strip().lower()
    brand_hint = str(body.get("brand_hint") or "").strip() or None
    ad_constraints = body.get("ad_constraints") if isinstance(body.get("ad_constraints"), dict) else None

    if not brand_id or len(brand_id) > 64:
        return _err(400, "invalid brand_id")
    if source_type not in ("text", "url", "pdf", "pdf_base64"):
        return _err(400, "invalid source_type (use text, url, or pdf)")

    text_v = body.get("text")
    url_v = body.get("url")
    pdf_b64 = body.get("pdf_base64")
    if source_type == "text" and (not isinstance(text_v, str) or not text_v.strip()):
        return _err(400, "text required for source_type text")
    if source_type == "url" and (not isinstance(url_v, str) or not url_v.strip()):
        return _err(400, "url required for source_type url")
    if source_type in ("pdf", "pdf_base64") and (not isinstance(pdf_b64, str) or not pdf_b64.strip()):
        return _err(400, "pdf_base64 required for source_type pdf")
    try:
        ac_in = dict(ad_constraints) if isinstance(ad_constraints, dict) else {}
        ac_in["primary_market"] = _normalize_primary_market(
            body.get("primary_market") or ac_in.get("primary_market")
        )
        out = generate_creative_brief(
            source_type=source_type,
            text=str(text_v).strip() if isinstance(text_v, str) else None,
            url=str(url_v).strip() if isinstance(url_v, str) else None,
            pdf_base64=str(pdf_b64).strip() if isinstance(pdf_b64, str) else None,
            brand_hint=brand_hint,
            ad_constraints=ac_in,
        )
        out["brand_id"] = brand_id
        return jsonify(out)
    except ValueError as e:
        return _err(400, str(e))
    except RuntimeError as e:
        return _err(503, str(e))
    except Exception as e:
        log.exception("creative brief route")
        return _err(500, str(e))


@app.post("/v1/marketing/fetch-website-hints")
def fetch_website_hints_route():
    """Suggest Studio form fields by fetching public page text and calling Vertex Gemini."""
    body = request.get_json(force=True, silent=True) or {}
    url = str(body.get("url") or "").strip()
    if not url:
        return _err(400, "url required")
    try:
        return jsonify(fetch_website_form_hints(url))
    except ValueError as e:
        return _err(400, str(e))
    except RuntimeError as e:
        return _err(503, str(e))
    except Exception as e:
        log.exception("fetch website hints")
        return _err(500, str(e))


@app.post("/v1/marketing/copy")
def marketing_copy_route():
    """Generate short marketing text (captions, email, headlines, landing snippet) via Vertex or mock."""
    body = request.get_json(force=True, silent=True) or {}
    content_type = str(body.get("content_type") or "").strip()
    context = body.get("context")
    brief = body.get("brief") if isinstance(body.get("brief"), dict) else None
    brand_name = str(body.get("brand_name") or "").strip() or None
    tone = str(body.get("tone") or "").strip() or None
    locale = str(body.get("locale") or "").strip() or None
    try:
        out = generate_marketing_copy(
            content_type=content_type,
            context=str(context).strip() if isinstance(context, str) else None,
            brief=brief,
            brand_name=brand_name,
            tone=tone,
            locale=locale,
        )
        return jsonify(out)
    except ValueError as e:
        return _err(400, str(e))
    except Exception as e:
        log.exception("marketing copy route")
        return _err(500, str(e))


@app.post("/v1/marketing/campaign")
def marketing_campaign_route():
    """
    One-shot autonomous pipeline: source → structured brief → AI-chosen carousel/video params →
    image + carousel + optional long stitched video. User only picks source, aspect ratio, and whether to include video.
    """
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "demo").strip()
    source_type = str(body.get("source_type") or "").strip().lower()
    brand_hint = str(body.get("brand_hint") or "").strip() or None
    audience_hint = str(body.get("audience_hint") or "").strip()
    if audience_hint:
        brand_hint = f"{brand_hint} | Target audience: {audience_hint}" if brand_hint else f"Target audience: {audience_hint}"
    brand_name = str(body.get("brand_name") or "").strip() or None
    brand_tagline = str(body.get("brand_tagline") or "").strip() or None
    logo_text = str(body.get("logo_text") or "").strip() or None
    objective = str(body.get("objective") or "").strip() or None
    copy_locked_lines = _parse_campaign_on_screen_lines(body.get("on_screen_lines"))
    primary_market = _normalize_primary_market(body.get("primary_market"))
    try:
        video_language = _normalize_video_language(body.get("video_language"))
    except ValueError as e:
        return _err(400, str(e))
    logo_persistent = True if body.get("logo_persistent") is None else bool(body.get("logo_persistent"))
    video_aspect_ratio = str(body.get("video_aspect_ratio") or "16:9").strip()
    if video_aspect_ratio not in ("9:16", "16:9", "1:1"):
        return _err(400, "video_aspect_ratio must be 9:16, 1:1, or 16:9")
    gen_image = True if body.get("generate_image") is None else bool(body.get("generate_image"))
    gen_carousel = True if body.get("generate_carousel") is None else bool(body.get("generate_carousel"))
    gen_video = body.get("generate_video")
    if gen_video is None:
        gen_video = True if body.get("include_video") is None else bool(body.get("include_video"))
    else:
        gen_video = bool(gen_video)
    if not gen_image and not gen_carousel and not gen_video:
        return _err(400, "select at least one of generate_image, generate_carousel, generate_video")
    if gen_video and video_aspect_ratio == "1:1":
        return _err(400, "video_aspect_ratio 1:1 is not supported for video; use 9:16 or 16:9 (1:1 is fine for image/carousel)")
    require_approval = bool(body.get("require_approval", False))
    base_opts = body.get("options") if isinstance(body.get("options"), dict) else None

    if not brand_id or len(brand_id) > 64:
        return _err(400, "invalid brand_id")
    if source_type not in ("text", "url", "pdf", "pdf_base64"):
        return _err(400, "invalid source_type (use text, url, or pdf)")

    text_v = body.get("text")
    url_v = body.get("url")
    pdf_b64 = body.get("pdf_base64")
    if source_type == "text" and (not isinstance(text_v, str) or not text_v.strip()):
        return _err(400, "text required for source_type text")
    if source_type == "url" and (not isinstance(url_v, str) or not url_v.strip()):
        return _err(400, "url required for source_type url")
    if source_type in ("pdf", "pdf_base64") and (not isinstance(pdf_b64, str) or not pdf_b64.strip()):
        return _err(400, "pdf_base64 required for source_type pdf")

    logo_bytes, logo_mime, logo_err = _parse_logo_image_from_body(body)
    if logo_err:
        return _err(400, logo_err)
    logo_ctx: dict[str, Any] = {}
    if logo_bytes:
        try:
            logo_ctx = describe_logo_image(image_bytes=logo_bytes, mime_type=logo_mime or "image/png")
        except Exception:
            log.exception("campaign logo image vision")
            logo_ctx = {}

    try:
        ad_constraints: dict[str, Any] = {
            "brand_name": brand_name,
            "brand_tagline": brand_tagline,
            "logo_text": (logo_text or brand_name or brand_id) if brand_id else logo_text,
            "objective": objective,
            "must_include_logo": True,
            "must_sound_like_ad": True,
            "logo_persistent": logo_persistent,
            "on_screen_lines_verbatim": copy_locked_lines,
            "logo_image_supplied": bool(logo_bytes),
            "primary_market": primary_market,
            "video_language": video_language,
        }
        lv = str(logo_ctx.get("logo_visual_description") or "").strip()
        if lv:
            ad_constraints["logo_visual_description"] = lv
        llt = str(logo_ctx.get("logo_lettering_text") or "").strip()
        if llt:
            ad_constraints["logo_lettering_text"] = llt
        dc = logo_ctx.get("dominant_colors")
        if isinstance(dc, list) and dc:
            ad_constraints["logo_dominant_colors"] = [
                str(x).strip() for x in dc[:5] if str(x).strip()
            ]
        brief_out = generate_creative_brief(
            source_type=source_type,
            text=str(text_v).strip() if isinstance(text_v, str) else None,
            url=str(url_v).strip() if isinstance(url_v, str) else None,
            pdf_base64=str(pdf_b64).strip() if isinstance(pdf_b64, str) else None,
            brand_hint=brand_hint,
            ad_constraints=ad_constraints,
        )
    except ValueError as e:
        return _err(400, str(e))
    except RuntimeError as e:
        return _err(503, str(e))
    except Exception as e:
        log.exception("marketing campaign brief")
        return _err(500, str(e))

    brief = brief_out.get("brief")
    if not isinstance(brief, dict) or not brief:
        return _err(500, "brief generation returned no brief object")

    arq = brief.get("ad_requirements")
    if not isinstance(arq, dict):
        arq = {}
        brief["ad_requirements"] = arq
    arq["on_screen_lines_verbatim"] = list(copy_locked_lines)
    arq["logo_persistent"] = bool(logo_persistent)
    arq["logo_image_supplied"] = bool(logo_bytes)
    arq["primary_market"] = primary_market
    arq["video_language"] = video_language
    if _primary_market_is_india(primary_market):
        cd_brief = str(arq.get("casting_direction") or "").strip()
        arq["casting_direction"] = (
            (_INDIA_CASTING_DIRECTION + (" " + cd_brief if cd_brief else "")).strip()
        )
        ml_brief = str(arq.get("marketing_localization_notes") or "").strip()
        arq["marketing_localization_notes"] = (
            (_INDIA_MARKETING_LOCALIZATION + (" " + ml_brief if ml_brief else "")).strip()
        )
    if ad_constraints.get("logo_visual_description"):
        arq["logo_visual_description"] = str(ad_constraints["logo_visual_description"]).strip()
    if ad_constraints.get("logo_lettering_text"):
        arq["logo_lettering_text"] = str(ad_constraints["logo_lettering_text"]).strip()
    elif logo_text:
        arq["logo_lettering_text"] = logo_text
    if isinstance(ad_constraints.get("logo_dominant_colors"), list):
        arq["logo_dominant_colors"] = list(ad_constraints["logo_dominant_colors"])

    ltv = str(arq.get("logo_lettering_text") or "").strip()
    logo_label_for_plan = (
        (logo_text or ltv or brand_name or (brand_id if brand_id != "demo" else "") or "").strip() or None
    )

    auto: dict[str, Any] | None = None
    prompts_imagen: dict[str, Any] | None = None
    video_plan: dict[str, Any] | None = None
    production_plan: dict[str, Any] | None = None
    script_audio: dict[str, Any] | None = None
    try:
        need_imagen = gen_image or gen_carousel
        need_video_plan = gen_video
        if need_imagen or need_video_plan:
            auto = derive_autonomous_campaign_params(brief)

            try:
                image_n = int(body.get("image_n") or 1)
            except (TypeError, ValueError):
                return _err(400, "image_n must be an integer")
            image_n = max(1, min(10, image_n))

            try:
                carousel_n = int(body.get("carousel_n") or 0)
            except (TypeError, ValueError):
                return _err(400, "carousel_n must be an integer")
            if carousel_n <= 0:
                carousel_n = int(auto.get("carousel_panel_count") or 4)
            carousel_n = max(3, min(10, carousel_n))

            try:
                v_total = int(body.get("video_total_seconds") or 0)
            except (TypeError, ValueError):
                return _err(400, "video_total_seconds must be an integer")
            if v_total <= 0:
                v_total = int(auto.get("video_total_seconds") or 32)
            v_total = max(6, min(600, v_total))

            try:
                v_clip = int(body.get("video_clip_seconds") or 0)
            except (TypeError, ValueError):
                return _err(400, "video_clip_seconds must be an integer")
            if v_clip <= 0:
                v_clip = int(auto.get("video_clip_seconds") or 8)
            v_clip = max(2, min(30, v_clip))

            auto = dict(auto)
            auto["carousel_panel_count"] = carousel_n
            auto["video_total_seconds"] = v_total
            auto["video_clip_seconds"] = v_clip
            auto["image_n"] = image_n

            production_plan = derive_production_plan_from_brief(
                brief,
                carousel_n=carousel_n,
                total_seconds=v_total,
                clip_seconds=v_clip,
                image_n=image_n,
                gen_image=gen_image,
                gen_carousel=gen_carousel,
                gen_video=gen_video,
                copy_locked_lines=copy_locked_lines,
                logo_persistent=logo_persistent,
                logo_label=logo_label_for_plan,
            )
            if need_imagen:
                prompts_imagen = derive_imagen_prompts_from_brief(brief, carousel_n=carousel_n)
            if need_video_plan:
                video_plan = derive_video_plan_from_brief(brief, total_seconds=v_total, clip_seconds=v_clip)
                if isinstance(video_plan, dict) and video_language != "auto":
                    lang_label = _VIDEO_LANGUAGE_LABELS.get(video_language, video_language)
                    lang_note = (
                        f"Language requirement: all spoken voiceover and on-screen text should be in {lang_label} ({video_language}). "
                        "Keep wording natural for native speakers and do not mix unrelated languages."
                    )
                    vp = str(video_plan.get("video_prompt") or "").strip()
                    vc = str(video_plan.get("video_continuity_text") or "").strip()
                    video_plan["video_prompt"] = (f"{vp}\n\n{lang_note}" if vp else lang_note).strip()
                    video_plan["video_continuity_text"] = (f"{vc}\n\n{lang_note}" if vc else lang_note).strip()
            apply_production_plan_to_prompts(
                production_plan,
                prompts_imagen=prompts_imagen if need_imagen else None,
                video_plan=video_plan if need_video_plan else None,
            )
            if need_video_plan and video_plan is not None and production_plan is not None:
                script_audio = derive_script_audio_spec(brief, production_plan, video_plan)
                apply_script_audio_to_video_plan(video_plan, script_audio)
    except ValueError as e:
        return _err(400, str(e))
    except RuntimeError as e:
        return _err(503, str(e))
    except Exception as e:
        log.exception("marketing campaign planning")
        return _err(500, str(e))

    img_opts: dict[str, Any] = dict(base_opts) if base_opts else {}
    img_opts["image_aspect_ratio"] = video_aspect_ratio

    prompts_used: dict[str, Any] = {}
    prompts_used["copy_locked_lines"] = list(copy_locked_lines)
    prompts_used["logo_persistent"] = bool(logo_persistent)
    prompts_used["primary_market"] = primary_market
    if auto is not None:
        prompts_used["autonomous_plan"] = auto
    if production_plan is not None:
        prompts_used["production_plan"] = production_plan
    if script_audio is not None:
        prompts_used["script_audio"] = script_audio
    if prompts_imagen:
        prompts_used.update(prompts_imagen)
    if auto is not None and isinstance(auto, dict) and auto.get("image_n") is not None:
        prompts_used["image_n"] = int(auto.get("image_n") or 1)
    if video_plan is not None:
        prompts_used["video"] = video_plan

    prompts_used["logo_image_supplied"] = bool(logo_bytes)
    if logo_bytes:
        prompts_used["logo_image_base64"] = base64.b64encode(logo_bytes).decode("ascii")
        prompts_used["logo_image_mime_type"] = logo_mime
    prompts_used["logo_visual_description"] = str(arq.get("logo_visual_description") or "").strip()
    prompts_used["logo_lettering_text"] = str(arq.get("logo_lettering_text") or "").strip()
    if isinstance(arq.get("logo_dominant_colors"), list):
        prompts_used["logo_dominant_colors"] = list(arq["logo_dominant_colors"])
    apply_logo_asset_to_prompts(prompts_used)

    if gen_image and not prompts_imagen:
        return _err(500, "image generation requested but imagen planning did not run")
    if gen_carousel and not prompts_imagen:
        return _err(500, "carousel generation requested but imagen planning did not run")
    if gen_video and video_plan is None:
        return _err(500, "video generation requested but video planning did not run")

    stage5_pending = stage5_asset_manifest(
        gen_image=gen_image,
        gen_carousel=gen_carousel,
        gen_video=gen_video,
        prompts_used=prompts_used,
        status="pending_execute",
    )

    if bool(body.get("plan_only")):
        return jsonify(
            {
                "brand_id": brand_id,
                "brief": brief,
                "brief_meta": brief_out.get("meta"),
                "video_aspect_ratio": video_aspect_ratio,
                "generate_image": gen_image,
                "generate_carousel": gen_carousel,
                "generate_video": gen_video,
                "include_video": gen_video,
                "prompts_used": prompts_used,
                "jobs": [],
                "plan_only": True,
                "stage5": stage5_pending,
                "stage6": stage6_pending_placeholder(),
            }
        )

    async_render = False if body.get("async_render") is None else bool(body.get("async_render"))
    if async_render and gen_video:
        row = _render_job_enqueue(
            {
                "brand_id": brand_id,
                "video_aspect_ratio": video_aspect_ratio,
                "generate_image": gen_image,
                "generate_carousel": gen_carousel,
                "generate_video": gen_video,
                "require_approval": require_approval,
                "options": base_opts,
                "prompts_used": prompts_used,
                "brief": brief,
                "notify_webhook_url": str(body.get("notify_webhook_url") or "").strip() or None,
                "notify_whatsapp_to": str(body.get("notify_whatsapp_to") or "").strip() or None,
            }
        )
        return jsonify(
            {
                "brand_id": brand_id,
                "brief": brief,
                "brief_meta": brief_out.get("meta"),
                "video_aspect_ratio": video_aspect_ratio,
                "generate_image": gen_image,
                "generate_carousel": gen_carousel,
                "generate_video": gen_video,
                "include_video": gen_video,
                "prompts_used": prompts_used,
                "jobs": [],
                "queued": True,
                "render_job": row,
                "status_url": f"/v1/marketing/render_jobs/{row['job_id']}",
                "plan_only": False,
                "stage5": stage5_pending,
                "stage6": stage6_pending_placeholder(),
            }
        )

    try:
        jobs = _run_campaign_asset_jobs(
            brand_id,
            prompts_used=prompts_used,
            gen_image=gen_image,
            gen_carousel=gen_carousel,
            gen_video=gen_video,
            video_aspect_ratio=video_aspect_ratio,
            require_approval=require_approval,
            base_opts=base_opts,
        )
    except ValueError as e:
        return _err(400, str(e))

    stage6 = run_stage6_campaign_qa(
        brief=brief,
        prompts_used=prompts_used,
        jobs=jobs,
        gen_image=gen_image,
        gen_carousel=gen_carousel,
        gen_video=gen_video,
    )
    return jsonify(
        {
            "brand_id": brand_id,
            "brief": brief,
            "brief_meta": brief_out.get("meta"),
            "video_aspect_ratio": video_aspect_ratio,
            "generate_image": gen_image,
            "generate_carousel": gen_carousel,
            "generate_video": gen_video,
            "include_video": gen_video,
            "prompts_used": prompts_used,
            "jobs": jobs,
            "plan_only": False,
            "stage5": stage5_asset_manifest(
                gen_image=gen_image,
                gen_carousel=gen_carousel,
                gen_video=gen_video,
                prompts_used=prompts_used,
                status="completed",
            ),
            "stage6": stage6,
        }
    )


@app.post("/v1/marketing/campaign/execute")
def marketing_campaign_execute_route():
    """Run asset generation from a `prompts_used` object returned by `POST /v1/marketing/campaign` with plan_only=true."""
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "demo").strip()
    video_aspect_ratio = str(body.get("video_aspect_ratio") or "16:9").strip()
    if video_aspect_ratio not in ("9:16", "16:9", "1:1"):
        return _err(400, "video_aspect_ratio must be 9:16, 1:1, or 16:9")
    gen_image = True if body.get("generate_image") is None else bool(body.get("generate_image"))
    gen_carousel = True if body.get("generate_carousel") is None else bool(body.get("generate_carousel"))
    gen_video = body.get("generate_video")
    if gen_video is None:
        gen_video = True if body.get("include_video") is None else bool(body.get("include_video"))
    else:
        gen_video = bool(gen_video)
    if not gen_image and not gen_carousel and not gen_video:
        return _err(400, "select at least one of generate_image, generate_carousel, generate_video")
    if gen_video and video_aspect_ratio == "1:1":
        return _err(400, "video_aspect_ratio 1:1 is not supported for video; use 9:16 or 16:9 (1:1 is fine for image/carousel)")
    require_approval = bool(body.get("require_approval", False))
    base_opts = body.get("options") if isinstance(body.get("options"), dict) else None
    prompts_used = body.get("prompts_used")
    if not isinstance(prompts_used, dict):
        return _err(400, "prompts_used object required")
    async_render = False if body.get("async_render") is None else bool(body.get("async_render"))
    notify_webhook_url = str(body.get("notify_webhook_url") or "").strip() or None
    notify_whatsapp_to = str(body.get("notify_whatsapp_to") or "").strip() or None

    if not brand_id or len(brand_id) > 64:
        return _err(400, "invalid brand_id")
    if gen_video:
        try:
            req_total = body.get("video_total_seconds")
            vp_total, _ = _validate_video_plan_or_raise(prompts_used)
            if req_total is not None:
                try:
                    req_total_i = int(req_total)
                except (TypeError, ValueError):
                    return _err(400, "video_total_seconds must be an integer")
                if req_total_i != vp_total:
                    return _err(400, f"requested video_total_seconds={req_total_i} but planned total_seconds={vp_total}; regenerate plan before execute")
        except ValueError as e:
            return _err(400, str(e))

    if async_render:
        row = _render_job_enqueue(
            {
                "brand_id": brand_id,
                "video_aspect_ratio": video_aspect_ratio,
                "generate_image": gen_image,
                "generate_carousel": gen_carousel,
                "generate_video": gen_video,
                "require_approval": require_approval,
                "options": base_opts,
                "prompts_used": prompts_used,
                "brief": body.get("brief") if isinstance(body.get("brief"), dict) else None,
                "notify_webhook_url": notify_webhook_url,
                "notify_whatsapp_to": notify_whatsapp_to,
            }
        )
        return jsonify(
            {
                "brand_id": brand_id,
                "queued": True,
                "render_job": row,
                "status_url": f"/v1/marketing/render_jobs/{row['job_id']}",
            }
        )

    try:
        jobs = _run_campaign_asset_jobs(
            brand_id,
            prompts_used=prompts_used,
            gen_image=gen_image,
            gen_carousel=gen_carousel,
            gen_video=gen_video,
            video_aspect_ratio=video_aspect_ratio,
            require_approval=require_approval,
            base_opts=base_opts,
        )
    except ValueError as e:
        return _err(400, str(e))
    except Exception as e:
        log.exception("marketing campaign execute")
        return _err(500, str(e))

    brief_exec = body.get("brief") if isinstance(body.get("brief"), dict) else None
    stage6_ex = run_stage6_campaign_qa(
        brief=brief_exec,
        prompts_used=prompts_used,
        jobs=jobs,
        gen_image=gen_image,
        gen_carousel=gen_carousel,
        gen_video=gen_video,
    )
    return jsonify(
        {
            "brand_id": brand_id,
            "video_aspect_ratio": video_aspect_ratio,
            "generate_image": gen_image,
            "generate_carousel": gen_carousel,
            "generate_video": gen_video,
            "include_video": gen_video,
            "jobs": jobs,
            "stage5": stage5_asset_manifest(
                gen_image=gen_image,
                gen_carousel=gen_carousel,
                gen_video=gen_video,
                prompts_used=prompts_used,
                status="completed",
            ),
            "stage6": stage6_ex,
        }
    )


@app.post("/v1/marketing/campaign/execute_async")
def marketing_campaign_execute_async_route():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    body["async_render"] = True
    # Reuse validation and queue behavior from execute route.
    with app.test_request_context(
        path="/v1/marketing/campaign/execute",
        method="POST",
        json=body,
        headers={k: v for k, v in request.headers.items()},
    ):
        return marketing_campaign_execute_route()


@app.get("/v1/marketing/render_jobs/<job_id>")
def marketing_render_job_get(job_id: str):
    with _RENDER_LOCK:
        row = _RENDER_JOBS.get(job_id)
        if not row:
            return _err(404, "render job not found")
        return jsonify(_render_job_public(row))


@app.get("/v1/marketing/render_jobs")
def marketing_render_job_list():
    brand_id = str(request.args.get("brand_id") or "").strip()
    limit_raw = str(request.args.get("limit") or "50").strip()
    try:
        limit = max(1, min(200, int(limit_raw)))
    except ValueError:
        return _err(400, "limit must be an integer")
    with _RENDER_LOCK:
        rows = list(_RENDER_JOBS.values())
    rows = sorted(rows, key=lambda x: str(x.get("created_at") or ""), reverse=True)
    if brand_id:
        rows = [r for r in rows if str(((r.get("payload") or {}).get("brand_id") or "")).strip() == brand_id]
    return jsonify({"items": [_render_job_public(r) for r in rows[:limit]], "count": min(len(rows), limit)})


def _run_asset_generation(
    brand_id: str,
    asset_type: str,
    prompt: str,
    n: int,
    require_approval: bool,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a store job, run the generator, persist output (Firestore-safe when configured)."""
    job = store.create(brand_id, asset_type, prompt, require_approval)
    try:
        out = gen.generate(asset_type, prompt, n, options=options)
        full_output = out.payload
        job.status = "awaiting_approval" if require_approval else "approved"
        if (settings.store_backend or "").strip().lower() == "firestore":
            from .firestore_stores import prepare_asset_output_for_firestore

            job.output = prepare_asset_output_for_firestore(settings, full_output)
        else:
            job.output = full_output
        store.update(job)
    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        store.update(job)

    return job.__dict__


def _strip_logo_prompt_directives(text: str) -> str:
    lines = [ln for ln in (text or "").splitlines() if "LOGO_ASSET:" not in ln and "LOGO_CONTINUITY:" not in ln]
    return "\n".join(lines).strip()


def _validate_video_plan_or_raise(prompts_used: dict[str, Any]) -> tuple[int, int]:
    vp = prompts_used.get("video")
    if not isinstance(vp, dict):
        raise ValueError("prompts_used missing video plan")
    try:
        total = int(vp.get("total_seconds") or 0)
        clip = int(vp.get("clip_seconds") or 0)
    except (TypeError, ValueError):
        raise ValueError("video plan total_seconds/clip_seconds must be integers") from None
    if total < 8 or total > 600:
        raise ValueError("video plan total_seconds must be 8..600")
    if clip not in {4, 6, 8}:
        raise ValueError("video plan clip_seconds must be one of 4, 6, 8")
    segs = (total + clip - 1) // clip
    max_segs = int((os.getenv("VERTEX_VIDEO_MAX_SEGMENTS") or "30").strip() or "30")
    if segs > max_segs:
        raise ValueError(f"video plan requires {segs} clips; max allowed is {max_segs}")
    return total, clip


def _http_post_json(url: str, payload: dict[str, Any], timeout_sec: int = 20) -> tuple[bool, str]:
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=max(3, min(90, timeout_sec))) as resp:
            code = int(getattr(resp, "status", 200) or 200)
            return (200 <= code < 300), f"http_{code}"
    except Exception as e:
        return False, str(e)


def _render_job_enqueue(payload: dict[str, Any]) -> dict[str, Any]:
    job_id = "rj_" + uuid.uuid4().hex[:20]
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "job_id": job_id,
        "status": "queued",
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "finished_at": None,
        "error": None,
        "payload": payload,
        "result": None,
    }
    with _RENDER_LOCK:
        _RENDER_JOBS[job_id] = row
    _RENDER_QUEUE.put(job_id)
    return {k: v for k, v in row.items() if k != "payload"}


def _render_job_public(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "job_id": job.get("job_id"),
        "status": job.get("status"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "error": job.get("error"),
        "result": job.get("result"),
    }


def _notify_render_done(payload: dict[str, Any], result: dict[str, Any], ok: bool) -> None:
    url = str(payload.get("notify_webhook_url") or "").strip()
    wa_to = str(payload.get("notify_whatsapp_to") or "").strip()
    if url:
        _http_post_json(
            url,
            {
                "type": "marketing_render_completed",
                "ok": ok,
                "brand_id": payload.get("brand_id"),
                "job_id": payload.get("job_id"),
                "result": result if ok else None,
                "error": None if ok else result.get("error"),
            },
            timeout_sec=20,
        )
    if wa_to:
        try:
            if ok:
                ms = int(result.get("tat_ms") or 0)
                dur = result.get("video_actual_duration_seconds")
                txt = (
                    f"Render completed. Job {payload.get('job_id')} is ready."
                    f" TAT {round(ms/1000,1)}s."
                    + (f" Video duration {dur}s." if dur is not None else "")
                )
            else:
                txt = f"Render failed. Job {payload.get('job_id')}. Error: {str(result.get('error') or 'unknown')[:500]}"
            whatsapp_send_text(wa_to, txt)
        except Exception:
            log.exception("render job whatsapp notify")


def _render_worker_loop() -> None:
    while True:
        job_id = _RENDER_QUEUE.get()
        with _RENDER_LOCK:
            row = _RENDER_JOBS.get(job_id)
            if not row:
                _RENDER_QUEUE.task_done()
                continue
            row["status"] = "running"
            row["started_at"] = datetime.now(timezone.utc).isoformat()
            row["updated_at"] = row["started_at"]
            payload = dict(row.get("payload") or {})
            payload["job_id"] = job_id
            row["payload"] = payload
        t0 = time.perf_counter()
        try:
            jobs = _run_campaign_asset_jobs(
                payload["brand_id"],
                prompts_used=payload["prompts_used"],
                gen_image=bool(payload["generate_image"]),
                gen_carousel=bool(payload["generate_carousel"]),
                gen_video=bool(payload["generate_video"]),
                video_aspect_ratio=str(payload["video_aspect_ratio"]),
                require_approval=bool(payload.get("require_approval", False)),
                base_opts=payload.get("options") if isinstance(payload.get("options"), dict) else None,
            )
            brief_exec = payload.get("brief") if isinstance(payload.get("brief"), dict) else None
            stage6 = run_stage6_campaign_qa(
                brief=brief_exec,
                prompts_used=payload["prompts_used"],
                jobs=jobs,
                gen_image=bool(payload["generate_image"]),
                gen_carousel=bool(payload["generate_carousel"]),
                gen_video=bool(payload["generate_video"]),
            )
            video_actual = None
            video_requested = None
            for j in jobs:
                if str(j.get("asset_type") or "").strip() == "video":
                    vp = (payload.get("prompts_used") or {}).get("video")
                    if isinstance(vp, dict):
                        try:
                            video_requested = int(vp.get("total_seconds") or 0)
                        except (TypeError, ValueError):
                            video_requested = None
                    out = j.get("output")
                    if isinstance(out, dict) and out.get("actual_duration_seconds") is not None:
                        video_actual = out.get("actual_duration_seconds")
                    break
            duration_ok = None
            if video_requested is not None and video_actual is not None:
                duration_ok = abs(float(video_actual) - float(video_requested)) <= max(
                    1.0, float((os.getenv("VERTEX_VIDEO_DURATION_TOLERANCE_SEC") or "3.0").strip() or "3.0")
                )
            result = {
                "brand_id": payload["brand_id"],
                "jobs": jobs,
                "stage5": stage5_asset_manifest(
                    gen_image=bool(payload["generate_image"]),
                    gen_carousel=bool(payload["generate_carousel"]),
                    gen_video=bool(payload["generate_video"]),
                    prompts_used=payload["prompts_used"],
                    status="completed",
                ),
                "stage6": stage6,
                "tat_ms": int((time.perf_counter() - t0) * 1000),
                "video_requested_total_seconds": video_requested,
                "video_actual_duration_seconds": video_actual,
                "qa": {
                    "duration_match": duration_ok,
                    "logo_hard_overlay_expected": bool((payload.get("prompts_used") or {}).get("logo_image_base64")),
                    "audio_codec_target": "aac 48kHz stereo 192k",
                },
            }
            with _RENDER_LOCK:
                row2 = _RENDER_JOBS.get(job_id)
                if row2:
                    row2["status"] = "completed"
                    row2["updated_at"] = datetime.now(timezone.utc).isoformat()
                    row2["finished_at"] = row2["updated_at"]
                    row2["result"] = result
            _notify_render_done(payload, result, True)
        except Exception as e:
            err = str(e)
            result = {"error": err, "trace": traceback.format_exc()[:4000], "tat_ms": int((time.perf_counter() - t0) * 1000)}
            with _RENDER_LOCK:
                row2 = _RENDER_JOBS.get(job_id)
                if row2:
                    row2["status"] = "failed"
                    row2["error"] = err
                    row2["updated_at"] = datetime.now(timezone.utc).isoformat()
                    row2["finished_at"] = row2["updated_at"]
                    row2["result"] = result
            _notify_render_done(payload, result, False)
        finally:
            _RENDER_QUEUE.task_done()


_render_worker = threading.Thread(target=_render_worker_loop, name="campaign-render-worker", daemon=True)
_render_worker.start()


def _run_campaign_asset_jobs(
    brand_id: str,
    *,
    prompts_used: dict[str, Any],
    gen_image: bool,
    gen_carousel: bool,
    gen_video: bool,
    video_aspect_ratio: str,
    require_approval: bool,
    base_opts: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Run Imagen/Veo jobs from a prior `prompts_used` payload (plan phase)."""
    img_opts: dict[str, Any] = dict(base_opts) if base_opts else {}
    img_opts["image_aspect_ratio"] = video_aspect_ratio
    jobs: list[dict[str, Any]] = []

    if gen_image:
        image_prompt = str(prompts_used.get("image_prompt") or "").strip()
        if not image_prompt:
            raise ValueError("prompts_used missing image_prompt")
        image_n = int(prompts_used.get("image_n") or 1)
        image_n = max(1, min(10, image_n))
        jobs.append(_run_asset_generation(brand_id, "image", image_prompt, image_n, require_approval, img_opts))

    if gen_carousel:
        carousel_n_eff = int(prompts_used.get("carousel_n") or 4)
        carousel_n_eff = max(3, min(10, carousel_n_eff))
        carousel_prompt = str(prompts_used.get("carousel_prompt") or "").strip()
        if not carousel_prompt:
            raise ValueError("prompts_used missing carousel_prompt")
        jobs.append(
            _run_asset_generation(brand_id, "carousel", carousel_prompt, carousel_n_eff, require_approval, img_opts)
        )

    if gen_video:
        _validate_video_plan_or_raise(prompts_used)
        video_plan = prompts_used.get("video")
        if not isinstance(video_plan, dict):
            raise ValueError("prompts_used missing video plan")
        vid_opts: dict[str, Any] = dict(base_opts) if base_opts else {}
        vid_opts = merge_stage5_video_options(prompts_used, vid_opts)
        vid_opts["video_total_seconds"] = int(video_plan["total_seconds"])
        vid_opts["video_clip_seconds"] = int(video_plan["clip_seconds"])
        vid_opts["video_aspect_ratio"] = video_aspect_ratio
        cont = video_plan.get("video_continuity_text")
        if isinstance(cont, str) and cont.strip():
            vid_opts["video_continuity_text"] = cont.strip()
        sb = video_plan.get("video_storyboard")
        if isinstance(sb, list):
            vid_opts["video_storyboard"] = sb
        vov = video_plan.get("video_segment_overlays")
        if isinstance(vov, list) and any(str(x).strip() for x in vov):
            vid_opts["video_segment_overlays"] = [str(x) for x in vov]
        logo_b64 = str(prompts_used.get("logo_image_base64") or "").strip()
        logo_mt = str(prompts_used.get("logo_image_mime_type") or "").strip()
        if logo_b64:
            vid_opts["video_logo_image_base64"] = logo_b64
            if logo_mt:
                vid_opts["video_logo_image_mime_type"] = logo_mt
        v_prompt = str(video_plan.get("video_prompt") or "").strip()
        v_prompt = _strip_logo_prompt_directives(v_prompt)
        if not v_prompt:
            raise ValueError("prompts_used missing video_prompt")
        jobs.append(_run_asset_generation(brand_id, "video", v_prompt, 1, require_approval, vid_opts))

    return jobs


@app.post("/v1/marketing/creative-assets")
def creative_assets_route():
    """Steps 2–3: from Step 1 `brief`, derive prompts and generate image, carousel, and/or video."""
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "demo").strip()
    brief = body.get("brief")
    asset_types_raw = body.get("asset_types")
    if not isinstance(asset_types_raw, list) or not asset_types_raw:
        asset_types = ["image", "carousel"]
    else:
        asset_types = [str(x).strip().lower() for x in asset_types_raw if str(x).strip()]
    carousel_n = int(body.get("carousel_n") or 3)
    require_approval = bool(body.get("require_approval", False))
    options = body.get("options") if isinstance(body.get("options"), dict) else None

    if not brand_id or len(brand_id) > 64:
        return _err(400, "invalid brand_id")
    if not isinstance(brief, dict) or not brief:
        return _err(400, "brief object required (use output.brief from Step 1)")
    allowed = {"image", "carousel", "video"}
    if not set(asset_types).issubset(allowed):
        return _err(400, "asset_types must be a subset of image, carousel, video")

    needs_image = "image" in asset_types
    needs_carousel = "carousel" in asset_types
    needs_video = "video" in asset_types

    if needs_carousel and (carousel_n < 1 or carousel_n > 10):
        return _err(400, "carousel_n must be 1–10")

    prompts_used: dict[str, Any] = {}
    try:
        if needs_image or needs_carousel:
            prompts_meta = derive_imagen_prompts_from_brief(brief, carousel_n=carousel_n)
            prompts_used.update(prompts_meta)
        if needs_video:
            try:
                v_total = int(body.get("video_total_seconds") or 24)
            except (TypeError, ValueError):
                return _err(400, "video_total_seconds must be an integer")
            try:
                v_clip = int(body.get("video_clip_seconds") or 8)
            except (TypeError, ValueError):
                return _err(400, "video_clip_seconds must be an integer")
            video_plan = derive_video_plan_from_brief(brief, total_seconds=v_total, clip_seconds=v_clip)
            prompts_used["video"] = video_plan
    except ValueError as e:
        return _err(400, str(e))
    except RuntimeError as e:
        return _err(503, str(e))
    except Exception as e:
        log.exception("creative assets derive prompts")
        return _err(500, str(e))

    jobs: list[dict[str, Any]] = []

    if needs_image:
        image_prompt = str(prompts_used.get("image_prompt") or "").strip()
        if not image_prompt:
            return _err(500, "missing image_prompt after planning")
        jobs.append(_run_asset_generation(brand_id, "image", image_prompt, 1, require_approval, options))
    if needs_carousel:
        carousel_n_eff = int(prompts_used.get("carousel_n") or carousel_n)
        carousel_n_eff = max(1, min(10, carousel_n_eff))
        carousel_prompt = str(prompts_used.get("carousel_prompt") or "").strip()
        if not carousel_prompt:
            return _err(500, "missing carousel_prompt after planning")
        jobs.append(
            _run_asset_generation(brand_id, "carousel", carousel_prompt, carousel_n_eff, require_approval, options)
        )
    if needs_video:
        vp = prompts_used.get("video")
        if not isinstance(vp, dict):
            return _err(500, "missing video plan after planning")
        vid_opts: dict[str, Any] = dict(options) if options else {}
        vid_opts["video_total_seconds"] = int(vp["total_seconds"])
        vid_opts["video_clip_seconds"] = int(vp["clip_seconds"])
        cont = vp.get("video_continuity_text")
        if isinstance(cont, str) and cont.strip():
            vid_opts["video_continuity_text"] = cont.strip()
        sb = vp.get("video_storyboard")
        if isinstance(sb, list):
            vid_opts["video_storyboard"] = sb
        vov = vp.get("video_segment_overlays")
        if isinstance(vov, list) and any(str(x).strip() for x in vov):
            vid_opts["video_segment_overlays"] = [str(x) for x in vov]
        v_prompt = str(vp.get("video_prompt") or "").strip()
        if not v_prompt:
            return _err(500, "missing video_prompt after planning")
        jobs.append(_run_asset_generation(brand_id, "video", v_prompt, 1, require_approval, vid_opts))

    return jsonify({"brand_id": brand_id, "prompts_used": prompts_used, "jobs": jobs})


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

    options = body.get("options") if isinstance(body.get("options"), dict) else None
    return jsonify(_run_asset_generation(brand_id, asset_type, prompt, n, require_approval, options))


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


@app.get("/v1/marketing/embed/lead-form")
def embed_lead_form():
    """Hosted HTML form for same-origin lead capture landing pages on the API host."""
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id or len(brand_id) > 64:
        return _err(400, "brand_id query parameter required")
    source = str(request.args.get("source") or "embed_form").strip()
    if len(source) > 64:
        return _err(400, "invalid source")
    page = marketing_leads_http.lead_form_embed_page(brand_id=brand_id, source=source)
    return Response(page, mimetype="text/html; charset=utf-8")


@app.route("/v1/marketing/leads", methods=["POST", "OPTIONS"])
def capture_lead():
    if request.method == "OPTIONS":
        return marketing_leads_http.lead_capture_options_response(request)

    parsed, parse_err = marketing_leads_http.parse_lead_request_body(request)
    if parse_err or parsed is None:
        return _err(415, parse_err or "invalid body")
    body = parsed

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

    hdr_idem = request.headers.get("Idempotency-Key")
    body_idem = body.get("idempotency_key")
    idem_src = (hdr_idem or "").strip() or (
        str(body_idem).strip() if body_idem is not None and str(body_idem).strip() else None
    )
    idem = sanitize_idempotency_key(idem_src)

    em = str(email) if email is not None else None
    ph = str(phone) if phone is not None else None
    if not make_dedupe_key(brand_id, em, ph) and not idem:
        return _err(
            400,
            "valid email or phone required (or send Idempotency-Key / idempotency_key for tracked submissions)",
        )

    lead, created = leads.upsert(
        brand_id=brand_id,
        source=source,
        name=str(name) if name is not None else None,
        email=em,
        phone=ph,
        company=str(company) if company is not None else None,
        message=str(message) if message is not None else None,
        utm=utm,
        raw=raw,
        idempotency_key=idem,
    )

    _auto_sync_lead(lead)

    ct = (request.content_type or "").split(";")[0].strip().lower()
    redir = marketing_leads_http.safe_form_redirect_url(str(body.get("redirect_url") or body.get("redirect") or ""))
    if redir and ct in ("application/x-www-form-urlencoded", "multipart/form-data"):
        return redirect(redir, code=303)

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

    hdr_idem = request.headers.get("Idempotency-Key")
    raw_idem = raw.get("idempotency_key") if isinstance(raw, dict) else None
    idem_src = (hdr_idem or "").strip() or (
        str(raw_idem).strip() if raw_idem is not None and str(raw_idem).strip() else None
    )
    idem = sanitize_idempotency_key(idem_src)

    em = str(email) if email is not None else None
    ph = str(phone) if phone is not None else None
    if not make_dedupe_key(brand_id, em, ph) and not idem:
        return _err(
            400,
            "valid email or phone required (or send Idempotency-Key / idempotency_key in payload)",
        )

    lead, created = leads.upsert(
        brand_id=brand_id,
        source=source,
        name=str(name) if name is not None else None,
        email=em,
        phone=ph,
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


@app.post("/v1/conversations")
def create_conversation():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    lead_id = str(body.get("lead_id") or "").strip()
    brand_id = str(body.get("brand_id") or "").strip()
    channel = str(body.get("channel") or "web").strip()
    locale = body.get("locale")
    metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else None
    require_lead = bool(body.get("require_lead", True))

    if not lead_id or not brand_id or len(brand_id) > 64:
        return _err(400, "invalid lead_id or brand_id")
    if require_lead:
        ld = leads.get(lead_id)
        if not ld or ld.brand_id != brand_id:
            return _err(404, "lead not found for brand")

    conv = conversations.create(
        lead_id=lead_id,
        brand_id=brand_id,
        channel=channel,
        locale=str(locale) if locale is not None else None,
        metadata=metadata,
    )
    return jsonify(
        {
            "conversation": conv.__dict__,
            "assistant_reply": conv.turns[-1].get("content") if conv.turns else "",
            "qualification": conversation_to_qualification_dict(conv),
        }
    )


@app.get("/v1/conversations/<cid>")
def get_conversation(cid: str):
    conv = conversations.get(cid)
    if not conv:
        return _err(404, "not found")
    return jsonify(
        {"conversation": conv.__dict__, "qualification": conversation_to_qualification_dict(conv)}
    )


@app.get("/v1/conversations/<cid>/qualification")
def get_conversation_qualification(cid: str):
    conv = conversations.get(cid)
    if not conv:
        return _err(404, "not found")
    return jsonify(conversation_to_qualification_dict(conv))


@app.get("/v1/conversations")
def list_conversation_for_lead():
    lead_id = (request.args.get("lead_id") or "").strip()
    brand_id = (request.args.get("brand_id") or "").strip()
    if not lead_id or not brand_id:
        return _err(400, "lead_id and brand_id required")
    conv = conversations.get_latest_for_lead(brand_id, lead_id)
    if not conv:
        return _err(404, "not found")
    return jsonify(
        {"conversation": conv.__dict__, "qualification": conversation_to_qualification_dict(conv)}
    )


@app.post("/v1/conversations/<cid>/messages")
def post_conversation_message(cid: str):
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    text = body.get("text")
    if text is None or str(text).strip() == "":
        return _err(400, "text required")
    conv = conversations.get(cid)
    if not conv:
        return _err(404, "not found")
    conv2, reply = process_user_message(conv, str(text))
    conversations.update(conv2)
    _sync_qualification_to_zoho(conv2.lead_id, conv2)
    return jsonify(
        {
            "conversation": conv2.__dict__,
            "assistant_reply": reply,
            "qualification": conversation_to_qualification_dict(conv2),
        }
    )


@app.route("/v1/public/chat/start", methods=["POST", "OPTIONS"])
def public_chat_start():
    if request.method == "OPTIONS":
        return Response("", 204)
    if not public_chat_api.is_enabled():
        return _err(404, "not found")
    if not public_chat_api.verify_api_key(request):
        return _err(401, "unauthorized")
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    payload, code = public_chat_api.start_session(
        leads,
        conversations,
        body,
        auto_sync_lead=_auto_sync_lead,
    )
    if code >= 400:
        return jsonify(payload), code
    return jsonify(payload)


@app.route("/v1/public/chat/message", methods=["POST", "OPTIONS"])
def public_chat_message():
    if request.method == "OPTIONS":
        return Response("", 204)
    if not public_chat_api.is_enabled():
        return _err(404, "not found")
    if not public_chat_api.verify_api_key(request):
        return _err(401, "unauthorized")
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    cid = str(body.get("conversation_id") or "").strip()
    text = body.get("text")
    if not cid:
        return _err(400, "conversation_id required")
    if text is None or str(text).strip() == "":
        return _err(400, "text required")
    payload, code = public_chat_api.post_message(
        conversations,
        cid,
        str(text),
        sync_qualification_to_zoho=_sync_qualification_to_zoho,
    )
    if code >= 400:
        return jsonify(payload), code
    return jsonify(payload)


def _handle_whatsapp_message_item(item: dict[str, str]) -> None:
    wa_from = item["wa_from"]
    text = item["body"]
    pnid = item["phone_number_id"]
    mid = item.get("message_id") or ""
    brand_id = resolve_brand_id(pnid)
    if not brand_id:
        raise RuntimeError(
            "set WHATSAPP_DEFAULT_BRAND_ID or WHATSAPP_NUMBER_BRAND_MAP for this phone_number_id"
        )
    loc = default_locale()
    lead = leads.find_by_brand_and_phone(brand_id, wa_from)
    if not lead:
        lead, _ = leads.upsert(
            brand_id=brand_id,
            source="whatsapp",
            name=None,
            email=None,
            phone=wa_from,
            company=None,
            message=text[:2000],
            utm=None,
            raw={"wa_id": wa_from, "last_wa_message_id": mid},
            idempotency_key=None,
        )
    lead = leads.get(lead.id)
    if not lead:
        return
    if mid and (lead.raw or {}).get("last_processed_wa_id") == mid:
        return
    leads.merge_raw(
        lead.id,
        {
            "wa_id": wa_from,
            "last_wa_message_id": mid,
            "last_wa_body_preview": text[:240],
        },
    )
    lead = leads.get(lead.id)
    if not lead:
        return
    _auto_sync_lead(lead)
    conv = conversations.get_latest_for_lead(brand_id, lead.id)
    if conv is None or conv.state == "complete":
        conv = conversations.create(
            lead_id=lead.id,
            brand_id=brand_id,
            channel="whatsapp",
            locale=loc,
            metadata={"wa_id": wa_from},
        )
        opening = ""
        if conv.turns:
            opening = str(conv.turns[-1].get("content") or "")
        if opening:
            whatsapp_send_text(wa_from, opening)
    conv_out, reply = process_user_message(conv, text)
    conversations.update(conv_out)
    _sync_qualification_to_zoho(lead.id, conv_out)
    whatsapp_send_text(wa_from, reply)
    if mid:
        leads.merge_raw(lead.id, {"last_processed_wa_id": mid})


@app.route("/v1/webhooks/whatsapp", methods=["GET", "POST"])
def whatsapp_webhook():
    if request.method == "GET":
        exp = (os.getenv("WHATSAPP_VERIFY_TOKEN") or "").strip()
        ch = whatsapp_verify_subscription(
            request.args.get("hub.mode"),
            request.args.get("hub.verify_token"),
            request.args.get("hub.challenge"),
            exp,
        )
        if ch is None:
            return "Forbidden", 403
        return Response(ch, mimetype="text/plain")
    raw = request.get_data()
    sec = (os.getenv("WHATSAPP_APP_SECRET") or "").strip()
    if sec:
        if not whatsapp_verify_signature(raw, request.headers.get("X-Hub-Signature-256"), sec):
            return _err(401, "invalid signature")
    payload = request.get_json(force=True, silent=True) or {}
    items = iter_inbound_text_messages(payload)
    errs: list[str] = []
    for it in items:
        try:
            _handle_whatsapp_message_item(it)
        except Exception as e:
            log.exception("whatsapp")
            errs.append(str(e))
    return jsonify({"received": True, "processed": len(items), "errors": errs})


def _voice_default_brand_id() -> str:
    return (os.getenv("VOICE_DEFAULT_BRAND_ID") or "").strip()


def _handle_tata_voice_event(payload: dict[str, Any]) -> dict[str, Any]:
    ev = parse_voice_tata_event(payload)
    brand_id = (ev.get("brand_id") or _voice_default_brand_id() or "").strip()
    from_phone = str(ev.get("from_phone") or "").strip()
    event_id = str(ev.get("event_id") or "").strip()
    if not brand_id:
        raise RuntimeError("set VOICE_DEFAULT_BRAND_ID or include brand_id in Tata webhook payload")
    if not from_phone:
        raise RuntimeError("missing caller phone in Tata webhook payload")

    lead = leads.find_by_brand_and_phone(brand_id, from_phone)
    if not lead:
        lead, _ = leads.upsert(
            brand_id=brand_id,
            source="voice",
            name=None,
            email=None,
            phone=from_phone,
            company=None,
            message=None,
            utm=None,
            raw={"voice_from": from_phone, "last_voice_event_id": event_id},
            idempotency_key=None,
        )
    lead = leads.get(lead.id)
    if not lead:
        return {"processed": False, "reason": "lead_not_found"}

    raw = lead.raw if isinstance(lead.raw, dict) else {}
    if event_id and str(raw.get("last_processed_voice_event_id") or "") == event_id:
        return {"processed": False, "reason": "duplicate_event", "lead_id": lead.id}

    text = str(ev.get("transcript") or "").strip()
    stt_meta: dict[str, Any] = {}
    if not text:
        audio_b64 = ev.get("audio_base64")
        audio_mime = ev.get("audio_mime_type")
        audio_url = ev.get("audio_url")
        if isinstance(audio_b64, str) and audio_b64.strip():
            text, stt_meta = transcribe_audio_base64(audio_b64, audio_mime if isinstance(audio_mime, str) else None)
        elif isinstance(audio_url, str) and audio_url.strip():
            text, stt_meta = transcribe_audio_url(audio_url)
    if not text:
        return {"processed": False, "reason": "no_transcript", "lead_id": lead.id}

    leads.merge_raw(
        lead.id,
        {
            "voice_from": from_phone,
            "last_voice_event_id": event_id,
            "last_voice_text_preview": text[:240],
            "last_voice_stt_meta": stt_meta,
        },
    )
    lead = leads.get(lead.id)
    if not lead:
        return {"processed": False, "reason": "lead_missing_after_patch"}

    _auto_sync_lead(lead)
    conv = conversations.get_latest_for_lead(brand_id, lead.id)
    if conv is None or conv.state == "complete":
        conv = conversations.create(
            lead_id=lead.id,
            brand_id=brand_id,
            channel="voice",
            locale=str(ev.get("locale") or "").strip() or None,
            metadata={"voice_from": from_phone, "voice_call_id": ev.get("call_id")},
        )
    conv_out, reply = process_user_message(conv, text)
    conversations.update(conv_out)
    _sync_qualification_to_zoho(lead.id, conv_out)
    tts = voice_synthesize(reply)
    if event_id:
        leads.merge_raw(lead.id, {"last_processed_voice_event_id": event_id, "last_voice_tts": tts})
    return {
        "processed": True,
        "lead_id": lead.id,
        "conversation_id": conv_out.id,
        "state": conv_out.state,
        "assistant_reply": reply,
        "tts": tts,
    }


@app.post("/v1/webhooks/voice/tata")
def voice_tata_webhook():
    if not verify_voice_tata_webhook(request):
        return _err(401, "invalid signature")
    payload = request.get_json(force=True, silent=True) or {}
    if not isinstance(payload, dict):
        return _err(400, "invalid payload")
    try:
        out = _handle_tata_voice_event(payload)
    except ValueError as e:
        return _err(400, str(e))
    except Exception as e:
        log.exception("voice tata webhook")
        return _err(500, str(e))
    return jsonify(out)


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


def _whatsapp_send_configured() -> bool:
    return bool(
        (os.getenv("WHATSAPP_PHONE_NUMBER_ID") or "").strip()
        and (os.getenv("WHATSAPP_ACCESS_TOKEN") or "").strip()
    )


@app.post("/v1/integrations/voice/outreach_lead/<lead_id>")
def integrations_voice_outreach_lead(lead_id: str):
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    ignore = bool(body.get("ignore_outreach_marker"))
    out = voice_outreach_one_lead(
        leads,
        conversations,
        lead_id,
        auto_sync_lead=_auto_sync_lead,
        ignore_outreach_marker=ignore,
    )
    if out.get("status") == "failed":
        return jsonify(out), 502
    return jsonify(out)


@app.post("/v1/integrations/voice/outreach_leads")
def integrations_voice_outreach_leads():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip() or None
    limit = int(body.get("limit") or 25)
    ignore = bool(body.get("ignore_outreach_marker"))
    if limit < 1 or limit > 200:
        return _err(400, "invalid limit")
    batch = batch_voice_outreach(
        leads,
        conversations,
        brand_id=brand_id,
        limit=limit,
        auto_sync_lead=_auto_sync_lead,
        ignore_outreach_marker=ignore,
    )
    if batch.get("error"):
        return _err(400, str(batch["error"]))
    return jsonify(batch)


@app.post("/v1/integrations/voice/call_now")
def integrations_voice_call_now():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    phone = str(body.get("phone") or "").strip()
    name = str(body.get("name") or "").strip() or None
    locale = str(body.get("locale") or os.getenv("VOICE_DEFAULT_LOCALE") or "hing").strip().lower()
    if locale not in {"hing", "hi", "en"}:
        return _err(400, "locale must be one of: hing, hi, en")
    if not brand_id:
        return _err(400, "brand_id required")

    lead = leads.get(lead_id) if lead_id else None
    if lead is None and phone:
        existing = leads.find_by_brand_and_phone(brand_id, phone)
        if existing is not None:
            lead = existing
            lead_id = existing.id
        else:
            try:
                lead, _ = leads.upsert(
                    brand_id=brand_id,
                    source="sale_voice_console",
                    name=name,
                    email=None,
                    phone=phone,
                    company=None,
                    message="Created from /sale voice console call_now",
                    utm=None,
                    raw={"voice_manual_call_requested_at": datetime.now(timezone.utc).isoformat(), "sales_language": locale},
                    idempotency_key=None,
                )
                lead_id = lead.id
            except Exception as e:
                return _err(400, f"could not create lead from phone: {e}")
    if lead is None:
        return _err(400, "lead_id not found. Provide a valid lead_id or provide phone to auto-create a lead.")

    try:
        conv = conversations.create(
            lead_id=lead.id,
            brand_id=brand_id,
            channel="voice",
            locale=locale,
            metadata={"voice_manual": True},
        )
        opening = str(conv.turns[0].get("content") or "").strip() if conv.turns else ""
        if not opening:
            opening = "Hello, this is SalesPal calling to understand your requirements."
        tts_payload = voice_synthesize(opening)
        call_resp = tata_call_outbound(to_phone=str(lead.phone or phone), brand_id=brand_id, text=opening, tts_payload=tts_payload)
        leads.merge_raw(
            lead.id,
            {
                "voice_manual_call_requested_at": datetime.now(timezone.utc).isoformat(),
                "voice_manual_conversation_id": conv.id,
                "voice_last_call_response": call_resp,
            },
        )
    except Exception as e:
        log.exception("voice call now")
        return _err(502, str(e))

    return jsonify(
        {
            "status": "queued",
            "brand_id": brand_id,
            "lead_id": lead.id,
            "conversation_id": conv.id,
            "locale": locale,
            "provider": str(call_resp.get("provider") or "tata"),
            "call_response": call_resp,
        }
    )


@app.post("/v1/integrations/whatsapp/outreach_lead/<lead_id>")
def integrations_whatsapp_outreach_lead(lead_id: str):
    if not _whatsapp_send_configured():
        return _err(409, "whatsapp send not configured")
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    ignore = bool(body.get("ignore_outreach_marker"))
    out = outreach_one_lead(
        leads,
        conversations,
        lead_id,
        auto_sync_lead=_auto_sync_lead,
        ignore_outreach_marker=ignore,
    )
    if out.get("status") == "failed":
        return jsonify(out), 502
    return jsonify(out)


@app.post("/v1/integrations/whatsapp/outreach_leads")
def integrations_whatsapp_outreach_leads():
    if not _whatsapp_send_configured():
        return _err(409, "whatsapp send not configured")
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip() or None
    limit = int(body.get("limit") or 25)
    ignore = bool(body.get("ignore_outreach_marker"))
    if limit < 1 or limit > 200:
        return _err(400, "invalid limit")
    batch = batch_whatsapp_outreach(
        leads,
        conversations,
        brand_id=brand_id,
        limit=limit,
        auto_sync_lead=_auto_sync_lead,
        ignore_outreach_marker=ignore,
    )
    if batch.get("error"):
        return _err(400, str(batch["error"]))
    return jsonify(batch)


@app.post("/v1/cron/whatsapp_outreach")
def cron_whatsapp_outreach():
    if not verify_scheduler_internal_request(request):
        return _err(401, "unauthorized")
    if not _whatsapp_send_configured():
        return _err(409, "whatsapp send not configured")
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip() or None
    limit = int(body.get("limit") or os.getenv("WHATSAPP_OUTREACH_CRON_LIMIT") or 25)
    ignore = bool(body.get("ignore_outreach_marker"))
    if limit < 1 or limit > 200:
        return _err(400, "invalid limit")
    batch = batch_whatsapp_outreach(
        leads,
        conversations,
        brand_id=brand_id,
        limit=limit,
        auto_sync_lead=_auto_sync_lead,
        ignore_outreach_marker=ignore,
    )
    if batch.get("error"):
        return _err(400, str(batch["error"]))
    return jsonify(batch)


@app.post("/v1/cron/voice_outreach")
def cron_voice_outreach():
    if not verify_scheduler_internal_request(request):
        return _err(401, "unauthorized")
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip() or None
    limit = int(body.get("limit") or os.getenv("VOICE_OUTREACH_CRON_LIMIT") or 25)
    ignore = bool(body.get("ignore_outreach_marker"))
    if limit < 1 or limit > 200:
        return _err(400, "invalid limit")
    batch = batch_voice_outreach(
        leads,
        conversations,
        brand_id=brand_id,
        limit=limit,
        auto_sync_lead=_auto_sync_lead,
        ignore_outreach_marker=ignore,
        retry_only=True,
    )
    if batch.get("error"):
        return _err(400, str(batch["error"]))
    return jsonify(batch)


@app.post("/v1/integrations/sales/parallel_outreach/<lead_id>")
def integrations_sales_parallel_outreach(lead_id: str):
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    tz = body.get("timezone") or body.get("tz")
    out = start_parallel_outreach(
        leads,
        conversations,
        lead_id,
        tz_name=str(tz).strip() if tz else None,
        auto_sync_lead=_auto_sync_lead,
    )
    if out.get("status") == "error":
        code = 404 if out.get("reason") == "not_found" else 400
        return jsonify(out), code
    return jsonify(out)


@app.post("/v1/integrations/sales/leads/<lead_id>/classify")
def integrations_sales_classify(lead_id: str):
    lead = leads.get(lead_id)
    if not lead:
        return _err(404, "not found")
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    before = dict(lead.raw or {})
    after = merge_classification(
        before,
        user_type=body.get("user_type"),
        lead_temperature=body.get("lead_temperature"),
        escalation=body.get("escalation"),
        feedback_score=body.get("feedback_score"),
    )
    patch = {k: after[k] for k in after if after.get(k) != before.get(k)}
    if patch:
        leads.merge_raw(lead_id, patch)
    keys = ("sales_user_type", "sales_lead_temperature", "sales_escalation", "sales_feedback_score")
    return jsonify({"lead_id": lead_id, "sales": {k: after.get(k) for k in keys if k in after}})


@app.post("/v1/integrations/sales/leads/<lead_id>/cold_enroll")
def integrations_sales_cold_enroll(lead_id: str):
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    tz = body.get("timezone") or body.get("tz")
    out = enroll_cold_lead(leads, lead_id, str(tz).strip() if tz else None)
    if out.get("status") == "error":
        return jsonify(out), 404
    return jsonify(out)


@app.post("/v1/cron/whatsapp_nurture")
def cron_whatsapp_nurture():
    if not verify_scheduler_internal_request(request):
        return _err(401, "unauthorized")
    if not _whatsapp_send_configured():
        return _err(409, "whatsapp send not configured")
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip() or None
    limit = int(body.get("limit") or os.getenv("WHATSAPP_NURTURE_CRON_LIMIT") or 25)
    if limit < 1 or limit > 200:
        return _err(400, "invalid limit")
    return jsonify(
        process_due_nurtures(
            leads,
            brand_id=brand_id,
            limit=limit,
            auto_sync_lead=_auto_sync_lead,
        )
    )


@app.post("/v1/cron/cold_campaign")
def cron_cold_campaign():
    if not verify_scheduler_internal_request(request):
        return _err(401, "unauthorized")
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip() or None
    limit = int(body.get("limit") or os.getenv("COLD_CAMPAIGN_CRON_LIMIT") or 25)
    if limit < 1 or limit > 200:
        return _err(400, "invalid limit")
    return jsonify(
        process_due_cold_campaigns(
            leads,
            brand_id=brand_id,
            limit=limit,
            auto_sync_lead=_auto_sync_lead,
        )
    )


def _append_lead_raw_event(lead_id: str, key: str, event: dict[str, Any]) -> dict[str, Any] | None:
    lead = leads.get(lead_id)
    if not lead:
        return None
    raw = dict(lead.raw or {}) if isinstance(getattr(lead, "raw", None), dict) else {}
    items = list(raw.get(key) or [])
    items.append(event)
    raw[key] = items[-100:]
    leads.merge_raw(lead_id, {key: raw[key]})
    return raw


@app.get("/marketing")
def marketing_console():
    # Milestone 1 now uses the demo console.
    return demo_ui()


@app.post("/v1/marketing/ops/social/connect")
def m1_social_connect():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        item = m1ops.connect_social(
            brand_id=str(body.get("brand_id") or "").strip(),
            platform=str(body.get("platform") or "").strip(),
            account_handle=str(body.get("account_handle") or "").strip(),
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"connection": item.__dict__})


@app.get("/v1/marketing/ops/social/connections")
def m1_social_connections():
    brand_id = str(request.args.get("brand_id") or "").strip()
    try:
        out = [x.__dict__ for x in m1ops.list_social(brand_id=brand_id)]
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"items": out})


@app.post("/v1/marketing/ops/social/platform_suggestion")
def m1_social_platform_suggestion():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "invalid brand_id")
    ranked = suggest_platforms(
        objective=str(body.get("objective") or "").strip(),
        audience=str(body.get("audience") or "").strip(),
    )
    return jsonify({"brand_id": brand_id, "suggested_platforms": ranked})


@app.post("/v1/marketing/ops/social/post_schedule")
def m1_social_post_schedule():
    """Diagram: Post Now / Schedule / Auto Publish."""
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        job = m1ops.schedule_or_post_social(
            brand_id=str(body.get("brand_id") or "").strip(),
            platform=str(body.get("platform") or "").strip(),
            mode=str(body.get("mode") or "post_now").strip(),
            schedule_at=str(body.get("schedule_at") or "").strip() or None,
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"job": job.__dict__})


@app.get("/v1/marketing/ops/social/jobs")
def m1_social_jobs():
    brand_id = str(request.args.get("brand_id") or "").strip()
    try:
        items = [x.__dict__ for x in m1ops.list_social_jobs(brand_id=brand_id)]
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"brand_id": brand_id, "items": items})


@app.post("/v1/marketing/ops/social/engagement")
def m1_social_engagement():
    """Diagram: Engagement Tracking (increments brand engagement counter)."""
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        m = m1ops.record_engagement(brand_id=str(body.get("brand_id") or "").strip(), delta=int(body.get("delta") or 1))
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"metrics": m.__dict__})


@app.post("/v1/marketing/ops/ads/lead_capture")
def m1_ads_lead_capture():
    """Diagram: Ads Engine → Lead Capture → Lead Management (records capture event; create lead via /v1/marketing/leads)."""
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    campaign_id = str(body.get("campaign_id") or "").strip()
    if not campaign_id or campaign_id.lower() in ("last", "latest", "recent"):
        ads = m1ops.list_ads(brand_id=brand_id)
        campaign_id = ads[0].id if ads else ""
    if not campaign_id:
        return _err(409, "no ad campaign for this brand; use Ad Launch first")
    note = str(body.get("note") or "lead_capture").strip() or "lead_capture"
    ev = {
        "type": "ad_lead_capture",
        "campaign_id": campaign_id,
        "note": note[:500],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    return jsonify({"brand_id": brand_id, "lead_capture": ev, "hint": "POST /v1/marketing/leads to persist a full lead record."})


@app.post("/v1/marketing/ops/leads/<lead_id>/send_to_owner")
def m1_lead_send_to_owner(lead_id: str):
    """Diagram: Send Lead to Owner (WhatsApp)."""
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    l = leads.get(lead_id)
    if not l:
        return _err(404, "not found")
    msg = str(body.get("message") or "New marketing lead — check dashboard.").strip()[:2000]
    at = datetime.now(timezone.utc).isoformat()
    leads.merge_raw(lead_id, {"m1_sent_to_owner_at": at, "m1_owner_message_preview": msg[:240]})
    ev = {"action": "send_to_owner", "created_at": at, "message": msg}
    _append_lead_raw_event(lead_id, "m1_manual_events", ev)
    return jsonify({"lead_id": lead_id, "sent": True, "whatsapp_stub": not bool((os.getenv("WHATSAPP_ACCESS_TOKEN") or "").strip()), "event": ev})


@app.post("/v1/marketing/ops/leads/<lead_id>/manual_followup")
def m1_lead_manual_followup(lead_id: str):
    """Diagram: Manual Follow-up → Call / WhatsApp → Update Status."""
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    channel = str(body.get("channel") or "").strip().lower()
    status = str(body.get("status") or "").strip().lower()
    if channel not in {"call", "whatsapp"}:
        return _err(400, "channel must be call or whatsapp")
    if not status:
        return _err(400, "status required")
    l = leads.get(lead_id)
    if not l:
        return _err(404, "not found")
    at = datetime.now(timezone.utc).isoformat()
    ev = {"channel": channel, "status": status, "created_at": at}
    _append_lead_raw_event(lead_id, "m1_manual_events", ev)
    leads.merge_raw(lead_id, {"m1_last_manual_followup": ev})
    return jsonify({"lead_id": lead_id, "followup": ev})


@app.post("/v1/marketing/ops/leads/<lead_id>/send_to_sales")
def m1_lead_send_to_sales(lead_id: str):
    """Diagram: Send to Sales Engine (Optional)."""
    l = leads.get(lead_id)
    if not l:
        return _err(404, "not found")
    at = datetime.now(timezone.utc).isoformat()
    leads.merge_raw(lead_id, {"m1_sent_to_sales_engine": True, "m1_sent_to_sales_at": at})
    ev = {"action": "send_to_sales_engine", "created_at": at}
    _append_lead_raw_event(lead_id, "m1_manual_events", ev)
    return jsonify({"lead_id": lead_id, "sent_to_sales_engine": True, "event": ev})


def _marketing_dashboard_payload(brand_id: str) -> dict[str, Any]:
    ll = leads.list(brand_id=brand_id)
    pp = posts.list(brand_id=brand_id)
    ads = m1ops.list_ads(brand_id=brand_id)
    bcs = m1ops.list_broadcasts(brand_id=brand_id)

    hot = warm = cold = 0
    for l in ll:
        raw = l.raw if isinstance(getattr(l, "raw", None), dict) else {}
        t = str(raw.get("sales_lead_temperature") or "").lower()
        if t == "hot":
            hot += 1
        elif t == "warm":
            warm += 1
        elif t == "cold":
            cold += 1
    posted = len([p for p in pp if p.status == "posted"])
    failed = len([p for p in pp if p.status == "failed"])
    engagement_score = min(100, posted * 7 + len(ads) * 5 + sum(x.responses_count for x in bcs))
    digital_presence_score = min(100, posted * 4 + len(m1ops.list_social(brand_id=brand_id)) * 10 + len(ads) * 6)
    m = m1ops.get_brand_metrics(brand_id=brand_id)
    engagement_score = min(100, engagement_score + min(30, m.engagement_events))
    digital_presence_score = min(100, digital_presence_score + min(15, m.posts_published * 2))
    reschedule = (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat()
    return {
        "brand_id": brand_id,
        "campaign_performance": {"posts_total": len(pp), "posted": posted, "failed": failed, "ads": len(ads)},
        "lead_insights": {"total": len(ll), "hot": hot, "warm": warm, "cold": cold},
        "engagement_metrics": {"engagement_score": engagement_score, "broadcast_responses": sum(x.responses_count for x in bcs)},
        "digital_presence_score": digital_presence_score,
        "reschedule_next_action": {"at_utc": reschedule, "hint": "Review lead follow-ups and refresh top creative."},
    }


@app.get("/v1/marketing/ops/optimization/loop")
def m1_optimization_loop():
    """Diagram: Dashboard → Digital Presence Score → AI Optimization Loop → AI Content Creation."""
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        dj = _marketing_dashboard_payload(brand_id)
    except ValueError as e:
        return _err(400, str(e))
    dps = int(dj.get("digital_presence_score") or 0)
    recommendations: list[str] = []
    if dps < 40:
        recommendations.append("Increase posting frequency and connect one more social account.")
        recommendations.append("Launch a small retargeting ad with lead-form objective.")
    elif dps < 70:
        recommendations.append("A/B test carousel + short video; schedule posts at peak local hours.")
    else:
        recommendations.append("Scale winning creative; add weekly broadcast to re-engage cold leads.")
    return jsonify(
        {
            "brand_id": brand_id,
            "digital_presence_score": dps,
            "dashboard_snapshot": dj,
            "ai_optimization_loop": {
                "recommendations": recommendations,
                "feed_back_to": "ai_content_creation",
                "suggested_next_brief_focus": "stronger CTA + proof points based on last campaign performance",
            },
        }
    )


@app.post("/v1/marketing/ops/ads/budget_allocation")
def m1_ads_budget_allocation():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "invalid brand_id")
    try:
        total = float(body.get("budget_total") or 0)
        alloc = allocate_budget(total, str(body.get("objective") or ""))
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"brand_id": brand_id, "budget_total": total, "allocation": alloc})


@app.post("/v1/marketing/ops/ads/launch")
def m1_ads_launch():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        total = float(body.get("budget_total") or 0)
        allocation = allocate_budget(total, str(body.get("objective") or ""))
        ad = m1ops.create_ad_campaign(
            brand_id=str(body.get("brand_id") or "").strip(),
            name=str(body.get("name") or "Campaign").strip(),
            objective=str(body.get("objective") or "awareness").strip(),
            budget_total=total,
            allocation=allocation,
        )
        ad = m1ops.update_ad_status(ad.id, "launched") or ad
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"campaign": ad.__dict__})


@app.post("/v1/marketing/ops/ads/optimize")
def m1_ads_optimize():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "invalid brand_id")
    ads = m1ops.list_ads(brand_id=brand_id)
    recommendations: list[str] = []
    if not ads:
        recommendations.append("Launch at least one campaign to collect optimization signals.")
    else:
        recommendations.append("Shift 10-15% spend from low CTR channels to high engagement channels.")
        recommendations.append("Refresh creatives every 7 days for ad fatigue control.")
        recommendations.append("Retarget warm leads via WhatsApp/email within 24h.")
    return jsonify({"brand_id": brand_id, "recommendations": recommendations, "campaign_count": len(ads)})


@app.post("/v1/marketing/ops/broadcast/campaign")
def m1_broadcast_campaign():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        item = m1ops.create_broadcast(
            brand_id=str(body.get("brand_id") or "").strip(),
            channel=str(body.get("channel") or "").strip(),
            audience=body.get("audience") if isinstance(body.get("audience"), dict) else {},
            schedule_at=str(body.get("schedule_at") or "").strip(),
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"campaign": item.__dict__})


@app.post("/v1/marketing/ops/broadcast/response")
def m1_broadcast_response():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    campaign_id = str(body.get("campaign_id") or "").strip()
    if not campaign_id:
        return _err(400, "invalid campaign_id")
    c = m1ops.add_broadcast_responses(campaign_id, int(body.get("count") or 1))
    if not c:
        return _err(404, "not found")
    return jsonify({"campaign": c.__dict__})


@app.post("/v1/marketing/ops/leads/<lead_id>/note")
def m1_lead_note(lead_id: str):
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    note = str(body.get("note") or "").strip()
    if not note:
        return _err(400, "note required")
    ev = {"note": note[:1000], "created_at": datetime.now(timezone.utc).isoformat()}
    raw = _append_lead_raw_event(lead_id, "m1_lead_notes", ev)
    if raw is None:
        return _err(404, "not found")
    return jsonify({"lead_id": lead_id, "note": ev})


@app.post("/v1/marketing/ops/leads/<lead_id>/followup")
def m1_lead_followup(lead_id: str):
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    at = str(body.get("follow_up_at") or "").strip()
    if not at:
        return _err(400, "follow_up_at required")
    ev = {"follow_up_at": at, "created_at": datetime.now(timezone.utc).isoformat()}
    raw = _append_lead_raw_event(lead_id, "m1_followups", ev)
    if raw is None:
        return _err(404, "not found")
    leads.merge_raw(lead_id, {"m1_next_followup_at": at})
    return jsonify({"lead_id": lead_id, "follow_up": ev})


@app.post("/v1/marketing/ops/leads/<lead_id>/mark")
def m1_lead_mark(lead_id: str):
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    t = str(body.get("temperature") or "").strip().lower()
    if t not in {"hot", "warm", "cold"}:
        return _err(400, "temperature must be hot/warm/cold")
    l = leads.get(lead_id)
    if not l:
        return _err(404, "not found")
    leads.merge_raw(lead_id, {"sales_lead_temperature": t})
    ev = {"temperature": t, "created_at": datetime.now(timezone.utc).isoformat()}
    _append_lead_raw_event(lead_id, "m1_temp_history", ev)
    return jsonify({"lead_id": lead_id, "temperature": t})


@app.get("/v1/marketing/ops/leads/<lead_id>/history")
def m1_lead_history(lead_id: str):
    l = leads.get(lead_id)
    if not l:
        return _err(404, "not found")
    raw = l.raw if isinstance(getattr(l, "raw", None), dict) else {}
    return jsonify(
        {
            "lead_id": lead_id,
            "lead": l.__dict__,
            "history": {
                "notes": list(raw.get("m1_lead_notes") or []),
                "followups": list(raw.get("m1_followups") or []),
                "temperature": list(raw.get("m1_temp_history") or []),
                "manual_events": list(raw.get("m1_manual_events") or []),
            },
        }
    )


@app.get("/v1/marketing/ops/dashboard")
def marketing_dashboard():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        payload = _marketing_dashboard_payload(brand_id)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify(payload)


@app.post("/v1/marketing/ops/optimization/reschedule")
def m1_optimization_reschedule():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    # Heuristic next action recommendation loop.
    ll = leads.list(brand_id=brand_id)
    hot = 0
    for l in ll:
        raw = l.raw if isinstance(getattr(l, "raw", None), dict) else {}
        if str(raw.get("sales_lead_temperature") or "").lower() == "hot":
            hot += 1
    if hot:
        action = "Prioritize call + WhatsApp follow-up for hot leads within 2 hours."
    else:
        action = "Run retargeting ad + broadcast campaign and rescore leads tomorrow."
    when = (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat()
    return jsonify({"brand_id": brand_id, "next_action": action, "reschedule_at_utc": when})


def _salespal360_console_html() -> str:
    """Inline SalesPal 360 web console markup."""
    return '<!doctype html>\n<html lang="en">\n<head>\n  <meta charset="utf-8" />\n  <meta name="viewport" content="width=device-width, initial-scale=1" />\n  <title>SalesPal 360</title>\n  <style>\n    :root {\n      --bg: #f8fafc;\n      --surface: #ffffff;\n      --border: #e2e8f0;\n      --text: #0f172a;\n      --muted: #64748b;\n      --primary: #2563eb;\n      --primary-hover: #1d4ed8;\n      --accent: #0f766e;\n      --radius: 10px;\n      --shadow: 0 1px 3px rgba(15, 23, 42, 0.06);\n    }\n    * { box-sizing: border-box; }\n    body {\n      margin: 0;\n      font-family: "Inter", system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;\n      background: var(--bg);\n      color: var(--text);\n      font-size: 14px;\n      line-height: 1.5;\n      -webkit-font-smoothing: antialiased;\n    }\n    .shell { min-height: 100vh; display: flex; flex-direction: column; }\n    header.app-head {\n      background: var(--surface);\n      border-bottom: 1px solid var(--border);\n      padding: 16px 20px 12px;\n    }\n    header.app-head h1 {\n      margin: 0;\n      font-size: 1.25rem;\n      font-weight: 700;\n      letter-spacing: -0.02em;\n    }\n    header.app-head p {\n      margin: 4px 0 0;\n      font-size: 13px;\n      color: var(--muted);\n      max-width: 52ch;\n    }\n    .credentials {\n      display: grid;\n      grid-template-columns: repeat(4, minmax(0, 1fr));\n      gap: 12px;\n      padding: 12px 20px;\n      background: var(--surface);\n      border-bottom: 1px solid var(--border);\n    }\n    @media (max-width: 900px) { .credentials { grid-template-columns: 1fr 1fr; } }\n    @media (max-width: 480px) { .credentials { grid-template-columns: 1fr; } }\n    label {\n      display: block;\n      font-size: 11px;\n      font-weight: 600;\n      color: var(--muted);\n      margin-bottom: 4px;\n    }\n    input, select, textarea {\n      width: 100%;\n      padding: 8px 10px;\n      border: 1px solid var(--border);\n      border-radius: 8px;\n      font: inherit;\n      background: #fff;\n    }\n    input:focus, select:focus, textarea:focus {\n      outline: none;\n      border-color: var(--primary);\n      box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.15);\n    }\n    textarea { min-height: 72px; resize: vertical; font-size: 13px; }\n    nav.section-nav {\n      position: sticky;\n      top: 0;\n      z-index: 10;\n      display: flex;\n      flex-wrap: wrap;\n      gap: 4px;\n      padding: 10px 20px;\n      background: rgba(248, 250, 252, 0.92);\n      backdrop-filter: blur(8px);\n      border-bottom: 1px solid var(--border);\n    }\n    nav.section-nav a {\n      text-decoration: none;\n      color: var(--muted);\n      font-size: 12px;\n      font-weight: 600;\n      padding: 6px 12px;\n      border-radius: 999px;\n      border: 1px solid transparent;\n    }\n    nav.section-nav a:hover { color: var(--text); background: #fff; border-color: var(--border); }\n    .body-grid {\n      flex: 1;\n      display: grid;\n      grid-template-columns: 1fr 340px;\n      gap: 0;\n      align-items: start;\n      max-width: 1600px;\n      margin: 0 auto;\n      width: 100%;\n    }\n    @media (max-width: 1100px) {\n      .body-grid { grid-template-columns: 1fr; }\n      aside.response-col { position: relative; top: 0; max-height: 50vh; }\n    }\n    main.content { padding: 20px; display: flex; flex-direction: column; gap: 20px; }\n    aside.response-col {\n      position: sticky;\n      top: 48px;\n      align-self: start;\n      padding: 20px 20px 20px 0;\n      border-left: 1px solid var(--border);\n      background: var(--bg);\n      min-height: 200px;\n    }\n    @media (max-width: 1100px) {\n      aside.response-col { border-left: 0; border-top: 1px solid var(--border); padding: 16px 20px; }\n    }\n    .panel {\n      background: var(--surface);\n      border: 1px solid var(--border);\n      border-radius: var(--radius);\n      box-shadow: var(--shadow);\n      padding: 18px 20px;\n    }\n    .panel h2 {\n      margin: 0 0 4px;\n      font-size: 15px;\n      font-weight: 700;\n    }\n    .panel .lede {\n      margin: 0 0 16px;\n      font-size: 13px;\n      color: var(--muted);\n    }\n    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }\n    @media (max-width: 640px) { .grid-2 { grid-template-columns: 1fr; } }\n    .grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }\n    @media (max-width: 960px) { .grid-3 { grid-template-columns: 1fr; } }\n    .actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }\n    .actions:first-child { margin-top: 0; }\n    button {\n      font: inherit;\n      font-size: 12px;\n      font-weight: 600;\n      padding: 8px 14px;\n      border-radius: 8px;\n      border: none;\n      cursor: pointer;\n      background: var(--primary);\n      color: #fff;\n    }\n    button:hover { background: var(--primary-hover); }\n    button.secondary { background: #475569; }\n    button.secondary:hover { background: #334155; }\n    button.ghost { background: #fff; color: var(--text); border: 1px solid var(--border); }\n    button.ghost:hover { background: #f8fafc; }\n    button.teal { background: var(--accent); }\n    button.teal:hover { filter: brightness(0.95); }\n    button:disabled { opacity: 0.5; cursor: not-allowed; }\n    .response-label {\n      font-size: 11px;\n      font-weight: 700;\n      text-transform: uppercase;\n      letter-spacing: 0.06em;\n      color: var(--muted);\n      margin-bottom: 8px;\n    }\n    pre#out {\n      margin: 0;\n      padding: 14px;\n      background: #0f172a;\n      color: #e2e8f0;\n      border-radius: var(--radius);\n      font-size: 11px;\n      line-height: 1.45;\n      white-space: pre-wrap;\n      word-break: break-word;\n      max-height: min(75vh, 720px);\n      overflow: auto;\n    }\n    .divider { height: 1px; background: var(--border); margin: 16px 0; }\n    .subhead { font-size: 12px; font-weight: 700; color: var(--muted); margin: 16px 0 8px; text-transform: uppercase; letter-spacing: 0.04em; }\n    .subhead:first-child { margin-top: 0; }\n  </style>\n</head>\n<body>\n  <div class="shell">\n    <header class="app-head">\n      <h1>SalesPal 360</h1>\n      <p>Unified sales operations: pipeline, marketing handoff, post-sale, support, and reporting. Set a brand and lead for customer-scoped actions; use an admin API key where your deployment requires it.</p>\n    </header>\n\n    <div class="credentials">\n      <div><label for="brand">Brand</label><input id="brand" value="demo" autocomplete="off" /></div>\n      <div><label for="lead">Lead</label><input id="lead" placeholder="Required for lead-specific actions" autocomplete="off" /></div>\n      <div><label for="adminkey">Admin API key</label><input id="adminkey" placeholder="If required by server" autocomplete="off" /></div>\n      <div><label for="tasktitle">Default task title</label><input id="tasktitle" placeholder="e.g. Follow up call" /></div>\n    </div>\n\n    <nav class="section-nav" aria-label="Sections">\n      <a href="#overview">Overview</a>\n      <a href="#pipeline">Pipeline</a>\n      <a href="#marketing">Marketing</a>\n      <a href="#postsale">Post-sale</a>\n      <a href="#support">Support</a>\n      <a href="#library">Library</a>\n      <a href="#reports">Reports</a>\n      <a href="#administration">Administration</a>\n    </nav>\n\n    <div class="body-grid">\n      <main class="content">\n        <section class="panel" id="overview">\n          <h2>Overview</h2>\n          <p class="lede">Snapshot metrics, forecast, analytics, and recommended next steps.</p>\n          <div class="actions">\n            <button type="button" onclick="salesDash()">Sales dashboard</button>\n            <button type="button" class="secondary" onclick="forecast()">Forecast</button>\n            <button type="button" class="secondary" onclick="analytics()">Analytics</button>\n            <button type="button" class="teal" onclick="learning()">Recommended next action</button>\n            <button type="button" class="ghost" onclick="featStatus()">Integration status</button>\n          </div>\n        </section>\n\n        <section class="panel" id="pipeline">\n          <h2>Pipeline &amp; deals</h2>\n          <p class="lede">Move opportunities through stages, qualify, schedule work, and review history.</p>\n          <div class="grid-2">\n            <div><label for="stage">Stage</label>\n              <select id="stage"><option>marketing</option><option selected>sales</option><option>postsale</option><option>support</option></select></div>\n            <div><label for="status">Status</label><input id="status" value="open" /></div>\n            <div><label for="owner">Owner</label>\n              <select id="owner"><option>ai</option><option>senior_ai</option><option>human</option></select></div>\n            <div><label for="qual">Qualification</label>\n              <select id="qual"><option>hot</option><option>warm</option><option>cold</option></select></div>\n          </div>\n          <div class="subhead">Sales motions</div>\n          <div class="actions">\n            <button type="button" onclick="requireLead(caseUpsert)">Save case</button>\n            <button type="button" class="secondary" onclick="requireLead(timelineOnly)">Timeline</button>\n            <button type="button" onclick="requireLead(salesQualify)">Qualify</button>\n            <button type="button" class="secondary" onclick="requireLead(salesFollow)">Schedule follow-up</button>\n            <button type="button" onclick="requireLead(salesEscalate)">Escalate</button>\n            <button type="button" class="ghost" onclick="logSalesAct()">Log automation event</button>\n          </div>\n          <div class="divider"></div>\n          <div class="grid-2">\n            <div><label for="meeting">Meeting status</label>\n              <select id="meeting"><option>scheduled</option><option>reschedule</option><option>no_show</option><option>completed</option></select></div>\n            <div><label for="follow">Follow-up time (UTC)</label><input id="follow" placeholder="2026-04-08T12:00:00+00:00" /></div>\n          </div>\n          <div class="actions">\n            <button type="button" class="teal" onclick="requireLead(salesMeeting)">Apply meeting update</button>\n            <button type="button" class="ghost" onclick="requireLead(scheduleMeeting)">Mark scheduled</button>\n            <button type="button" class="ghost" onclick="requireLead(markReschedule)">Mark reschedule / no-show</button>\n            <button type="button" class="ghost" onclick="requireLead(completeVisit)">Complete visit</button>\n          </div>\n          <div class="actions" style="margin-top:8px">\n            <button type="button" onclick="tasksList()">List tasks</button>\n            <button type="button" class="secondary" onclick="taskAdd()">Create task</button>\n          </div>\n        </section>\n\n        <section class="panel" id="marketing">\n          <h2>Marketing handoff</h2>\n          <p class="lede">Leads, campaigns, and optimization. Uses marketing APIs for this brand.</p>\n          <div class="subhead">Leads &amp; performance</div>\n          <div class="actions">\n            <button type="button" onclick="mktLeads()">List leads</button>\n            <button type="button" class="secondary" onclick="repLeads()">Lead analytics (SalesPal)</button>\n            <button type="button" onclick="mktOptLoop()">Optimization loop</button>\n          </div>\n          <div class="subhead">Channels</div>\n          <div class="actions">\n            <button type="button" onclick="mktCopy()">Generate copy</button>\n            <button type="button" class="secondary" onclick="mktSocialConn()">Social connections</button>\n            <button type="button" class="secondary" onclick="mktSocialJobs()">Social jobs</button>\n            <button type="button" onclick="mktAdsLeadCap()">Ads lead capture</button>\n            <button type="button" class="ghost" onclick="mktBroadcast()">Broadcast campaign</button>\n          </div>\n          <div class="subhead">Website</div>\n          <div class="actions">\n            <button type="button" class="ghost" onclick="mktFetchHints()">Fetch website hints</button>\n          </div>\n        </section>\n\n        <section class="panel" id="postsale">\n          <h2>Post-sale</h2>\n          <p class="lede">Payments, satisfaction, and moving the record into post-sale.</p>\n          <div class="actions">\n            <button type="button" class="secondary" onclick="requireLead(caseUpsertPost)">Set stage: post-sale</button>\n          </div>\n          <div class="grid-2" style="margin-top:12px">\n            <div><label for="pay">Payment verification</label>\n              <select id="pay"><option>partial_verified</option><option>fully_verified</option></select></div>\n            <div><label for="fb">Satisfaction (1–10)</label><input id="fb" type="number" min="1" max="10" value="8" /></div>\n          </div>\n          <div class="actions">\n            <button type="button" class="teal" onclick="requireLead(postClose)">Record payment &amp; close</button>\n            <button type="button" onclick="requireLead(postFeedback)">Submit satisfaction</button>\n          </div>\n        </section>\n\n        <section class="panel" id="support">\n          <h2>Support</h2>\n          <p class="lede">Open and update support tickets for the current lead.</p>\n          <div class="grid-2">\n            <div><label for="sch">Channel</label>\n              <select id="sch"><option>whatsapp</option><option>email</option><option>sms</option></select></div>\n            <div><label for="ticket">Ticket ID</label><input id="ticket" placeholder="Filled after open" /></div>\n          </div>\n          <label for="complaint">Details</label>\n          <textarea id="complaint"></textarea>\n          <div class="grid-2" style="margin-top:8px">\n            <div><label for="tstatus">Ticket status</label>\n              <select id="tstatus"><option>open</option><option>resolved</option><option>closed</option></select></div>\n            <div><label for="esc">Escalation</label>\n              <select id="esc"><option>ai</option><option>human</option></select></div>\n          </div>\n          <div class="actions">\n            <button type="button" onclick="requireLead(supportOpen)">Open ticket</button>\n            <button type="button" class="secondary" onclick="supportUpdate()">Update ticket</button>\n          </div>\n        </section>\n\n        <section class="panel" id="library">\n          <h2>Library &amp; memory</h2>\n          <p class="lede">Documents, e-sign requests, and per-lead memory.</p>\n          <div class="actions">\n            <button type="button" onclick="docAdd()">Register document</button>\n            <button type="button" class="secondary" onclick="docList()">List documents</button>\n            <button type="button" onclick="requireLead(esigAdd)">Request e-signature</button>\n            <button type="button" class="secondary" onclick="esigList()">E-sign requests</button>\n          </div>\n          <label for="mem">Memory (JSON)</label>\n          <textarea id="mem">{"conversation_memory":{"topic":"pricing"}}</textarea>\n          <div class="actions">\n            <button type="button" class="teal" onclick="requireLead(memoryMerge)">Save memory</button>\n            <button type="button" class="ghost" onclick="requireLead(memoryGet)">Load memory</button>\n          </div>\n        </section>\n\n        <section class="panel" id="reports">\n          <h2>Reports</h2>\n          <p class="lede">Sales activity, lead sources, and team performance.</p>\n          <div class="actions">\n            <button type="button" onclick="repSales()">Sales activity</button>\n            <button type="button" class="secondary" onclick="repLeads()">Leads &amp; sources</button>\n            <button type="button" onclick="repPerf()">Performance</button>\n          </div>\n        </section>\n\n        <section class="panel" id="administration">\n          <h2>Platform &amp; administration</h2>\n          <p class="lede">Tenant settings, CRM configuration, and operational visibility.</p>\n          <div class="grid-3">\n            <div>\n              <div class="subhead" style="margin-top:0">Platform</div>\n              <div class="actions">\n                <button type="button" onclick="platSum()">Summary</button>\n                <button type="button" class="secondary" onclick="platPatch()">Save settings</button>\n              </div>\n              <label for="platpatch">Settings (JSON)</label>\n              <textarea id="platpatch">{"timezone":"Asia/Kolkata"}</textarea>\n            </div>\n            <div>\n              <div class="subhead" style="margin-top:0">CRM</div>\n              <div class="actions">\n                <button type="button" onclick="crmGet()">Load config</button>\n                <button type="button" class="secondary" onclick="crmPatch()">Save config</button>\n              </div>\n              <label for="crmpatch">CRM config (JSON)</label>\n              <textarea id="crmpatch">{"pipeline_stages":["new","qualified","won"]}</textarea>\n            </div>\n            <div>\n              <div class="subhead" style="margin-top:0">Admin</div>\n              <div class="actions">\n                <button type="button" onclick="adminSum()">Dashboard</button>\n                <button type="button" class="secondary" onclick="adminAct()">Activity log</button>\n                <button type="button" class="ghost" onclick="adminHealth()">Service health</button>\n              </div>\n            </div>\n          </div>\n        </section>\n      </main>\n\n      <aside class="response-col">\n        <div class="response-label">Response</div>\n        <pre id="out">Ready.</pre>\n      </aside>\n    </div>\n  </div>\n\n  <script>\n(function () {\n  var out = document.getElementById("out");\n  function hdr() {\n    var h = { "Content-Type": "application/json" };\n    var k = (document.getElementById("adminkey").value || "").trim();\n    if (k) h["X-Admin-Api-Key"] = k;\n    return h;\n  }\n  function api(path, method, body) {\n    method = method || "GET";\n    return fetch(path, {\n      method: method,\n      headers: hdr(),\n      body: body ? JSON.stringify(body) : null\n    }).then(function (r) {\n      return r.json().catch(function () { return { _parse: "non-json" }; }).then(function (j) {\n        if (!r.ok) j._http = { status: r.status };\n        out.textContent = JSON.stringify(j, null, 2);\n        return j;\n      });\n    });\n  }\n  function brand() { return (document.getElementById("brand").value || "").trim(); }\n  function lead() { return (document.getElementById("lead").value || "").trim(); }\n  function requireLead(fn) {\n    if (!lead()) {\n      out.textContent = JSON.stringify({ error: "Set a lead ID for this action." }, null, 2);\n      return;\n    }\n    fn();\n  }\n  window.api = api;\n\n  window.featStatus = function () { return api("/v1/salespal360/features/status"); };\n  window.salesDash = function () { return api("/v1/salespal360/sales/dashboard?brand_id=" + encodeURIComponent(brand())); };\n  window.repSales = function () { return api("/v1/salespal360/reports/sales?brand_id=" + encodeURIComponent(brand())); };\n  window.repLeads = function () { return api("/v1/salespal360/reports/leads?brand_id=" + encodeURIComponent(brand())); };\n  window.repPerf = function () { return api("/v1/salespal360/reports/performance?brand_id=" + encodeURIComponent(brand())); };\n  window.forecast = function () { return api("/v1/salespal360/forecast?brand_id=" + encodeURIComponent(brand())); };\n  window.tasksList = function () { return api("/v1/salespal360/tasks?brand_id=" + encodeURIComponent(brand())); };\n  window.taskAdd = function () {\n    var t = (document.getElementById("tasktitle").value || "Follow up").trim();\n    return api("/v1/salespal360/tasks", "POST", { brand_id: brand(), title: t, lead_id: lead() || null, due_at: null });\n  };\n  window.logSalesAct = function () {\n    return api("/v1/salespal360/admin/activity", "POST", {\n      brand_id: brand(),\n      actor: "console",\n      action: "sales_automation",\n      meta: { lead_id: lead() || null }\n    });\n  };\n  window.platSum = function () { return api("/v1/salespal360/platform/summary?brand_id=" + encodeURIComponent(brand())); };\n  window.platPatch = function () {\n    var p = {};\n    try { p = JSON.parse(document.getElementById("platpatch").value || "{}"); } catch (e) {}\n    return api("/v1/salespal360/platform/settings", "POST", { brand_id: brand(), patch: p });\n  };\n  window.adminSum = function () { return api("/v1/salespal360/admin/summary?brand_id=" + encodeURIComponent(brand())); };\n  window.adminAct = function () { return api("/v1/salespal360/admin/activity?brand_id=" + encodeURIComponent(brand())); };\n  window.adminHealth = function () { return api("/v1/salespal360/admin/health"); };\n  window.crmGet = function () { return api("/v1/salespal360/crm/config?brand_id=" + encodeURIComponent(brand())); };\n  window.crmPatch = function () {\n    var p = {};\n    try { p = JSON.parse(document.getElementById("crmpatch").value || "{}"); } catch (e) {}\n    return api("/v1/salespal360/crm/config", "POST", { brand_id: brand(), patch: p });\n  };\n  window.caseUpsert = function () {\n    return api("/v1/salespal360/case/upsert", "POST", {\n      brand_id: brand(),\n      lead_id: lead(),\n      stage: document.getElementById("stage").value,\n      status: document.getElementById("status").value,\n      owner: document.getElementById("owner").value\n    });\n  };\n  window.caseUpsertPost = function () {\n    return api("/v1/salespal360/case/upsert", "POST", {\n      brand_id: brand(),\n      lead_id: lead(),\n      stage: "postsale",\n      status: document.getElementById("status").value,\n      owner: document.getElementById("owner").value\n    });\n  };\n  window.timelineOnly = function () {\n    return api("/v1/salespal360/timeline?brand_id=" + encodeURIComponent(brand()) + "&lead_id=" + encodeURIComponent(lead()));\n  };\n  window.salesQualify = function () {\n    return api("/v1/salespal360/sales/qualify", "POST", {\n      brand_id: brand(),\n      lead_id: lead(),\n      qualification: document.getElementById("qual").value,\n      owner: document.getElementById("owner").value\n    });\n  };\n  window.salesFollow = function () {\n    return api("/v1/salespal360/sales/followup", "POST", {\n      brand_id: brand(),\n      lead_id: lead(),\n      follow_up_at: document.getElementById("follow").value || null\n    });\n  };\n  window.salesMeeting = function () {\n    return api("/v1/salespal360/sales/meeting", "POST", {\n      brand_id: brand(),\n      lead_id: lead(),\n      meeting_status: document.getElementById("meeting").value\n    });\n  };\n  window.salesEscalate = function () {\n    return api("/v1/salespal360/sales/escalate", "POST", {\n      brand_id: brand(),\n      lead_id: lead(),\n      owner: document.getElementById("owner").value\n    });\n  };\n  window.scheduleMeeting = function () {\n    document.getElementById("meeting").value = "scheduled";\n    return salesMeeting();\n  };\n  window.markReschedule = function () {\n    document.getElementById("meeting").value = "reschedule";\n    return salesMeeting();\n  };\n  window.completeVisit = function () {\n    document.getElementById("meeting").value = "completed";\n    return api("/v1/salespal360/admin/activity", "POST", {\n      brand_id: brand(),\n      actor: "console",\n      action: "visit_completed",\n      meta: { lead_id: lead() }\n    }).then(function () { return salesMeeting(); });\n  };\n  window.postClose = function () {\n    return api("/v1/salespal360/postsale/close", "POST", {\n      brand_id: brand(),\n      lead_id: lead(),\n      payment_status: document.getElementById("pay").value\n    });\n  };\n  window.postFeedback = function () {\n    return api("/v1/salespal360/postsale/feedback", "POST", {\n      brand_id: brand(),\n      lead_id: lead(),\n      score: Number(document.getElementById("fb").value || 0)\n    });\n  };\n  window.supportOpen = function () {\n    return api("/v1/salespal360/support/open", "POST", {\n      brand_id: brand(),\n      lead_id: lead(),\n      channel: document.getElementById("sch").value,\n      complaint: document.getElementById("complaint").value\n    }).then(function (j) {\n      if (j.ticket && j.ticket.id) document.getElementById("ticket").value = j.ticket.id;\n      return j;\n    });\n  };\n  window.supportUpdate = function () {\n    return api("/v1/salespal360/support/update", "POST", {\n      ticket_id: document.getElementById("ticket").value,\n      status: document.getElementById("tstatus").value,\n      resolution: document.getElementById("complaint").value,\n      escalation_level: document.getElementById("esc").value\n    });\n  };\n  window.memoryMerge = function () {\n    var patch = {};\n    try { patch = JSON.parse(document.getElementById("mem").value || "{}"); } catch (e) {}\n    return api("/v1/salespal360/memory/merge", "POST", { brand_id: brand(), lead_id: lead(), patch: patch });\n  };\n  window.memoryGet = function () {\n    return api("/v1/salespal360/memory/get?brand_id=" + encodeURIComponent(brand()) + "&lead_id=" + encodeURIComponent(lead()));\n  };\n  window.docAdd = function () {\n    return api("/v1/salespal360/documents", "POST", {\n      brand_id: brand(),\n      name: "Proposal.pdf",\n      kind: "pdf",\n      lead_id: lead() || null\n    });\n  };\n  window.docList = function () { return api("/v1/salespal360/documents?brand_id=" + encodeURIComponent(brand())); };\n  window.esigAdd = function () {\n    return api("/v1/salespal360/esign", "POST", {\n      brand_id: brand(),\n      lead_id: lead(),\n      document_name: "Order form"\n    });\n  };\n  window.esigList = function () { return api("/v1/salespal360/esign?brand_id=" + encodeURIComponent(brand())); };\n  window.analytics = function () { return api("/v1/salespal360/analytics?brand_id=" + encodeURIComponent(brand())); };\n  window.learning = function () { return api("/v1/salespal360/learning/next_action", "POST", { brand_id: brand() }); };\n\n  window.mktLeads = function () { return api("/v1/marketing/leads?brand_id=" + encodeURIComponent(brand())); };\n  window.mktOptLoop = function () { return api("/v1/marketing/ops/optimization/loop?brand_id=" + encodeURIComponent(brand())); };\n  window.mktSocialConn = function () { return api("/v1/marketing/ops/social/connections?brand_id=" + encodeURIComponent(brand())); };\n  window.mktSocialJobs = function () { return api("/v1/marketing/ops/social/jobs?brand_id=" + encodeURIComponent(brand())); };\n  window.mktAdsLeadCap = function () {\n    return api("/v1/marketing/ops/ads/lead_capture", "POST", { brand_id: brand(), campaign_id: "latest" });\n  };\n  window.mktBroadcast = function () {\n    return api("/v1/marketing/ops/broadcast/campaign", "POST", {\n      brand_id: brand(),\n      channel: "sms",\n      audience: {},\n      schedule_at: ""\n    });\n  };\n  window.mktCopy = function () {\n    return api("/v1/marketing/copy", "POST", {\n      content_type: "social_caption",\n      context: "Brand promotion",\n      tone: "professional",\n      brand_name: brand()\n    });\n  };\n  window.mktFetchHints = function () {\n    return api("/v1/marketing/fetch-website-hints", "POST", { url: "https://example.com" });\n  };\n})();\n  </script>\n</body>\n</html>\n'

@app.get("/salespal360")
def salespal360_console():
    return Response(_salespal360_console_html(), mimetype="text/html; charset=utf-8")


@app.post("/v1/salespal360/case/upsert")
def salespal360_case_upsert():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        case = sp360.upsert_case(
            brand_id=str(body.get("brand_id") or "").strip(),
            lead_id=str(body.get("lead_id") or "").strip(),
            stage=str(body.get("stage") or "sales").strip(),
            status=str(body.get("status") or "open").strip(),
            owner=str(body.get("owner") or "ai").strip(),
            score=int(body.get("score") or 50),
        )
        sp360.add_event(
            brand_id=case.brand_id,
            lead_id=case.lead_id,
            engine=case.stage,
            event_type="case_upsert",
            payload={"status": case.status, "owner": case.owner, "score": case.score},
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"case": case.__dict__})


@app.get("/v1/salespal360/timeline")
def salespal360_timeline():
    brand_id = str(request.args.get("brand_id") or "").strip()
    lead_id = str(request.args.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    try:
        case = sp360.get_case(brand_id=brand_id, lead_id=lead_id)
        events = [e.__dict__ for e in sp360.list_events(brand_id=brand_id, lead_id=lead_id)]
        mem = sp360.get_memory(brand_id=brand_id, lead_id=lead_id)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"case": case.__dict__ if case else None, "events": events, "memory": mem})


@app.post("/v1/salespal360/sales/qualify")
def salespal360_sales_qualify():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    q = str(body.get("qualification") or "").strip().lower()
    if q not in {"hot", "warm", "cold"}:
        return _err(400, "qualification must be hot/warm/cold")
    score = {"hot": 85, "warm": 60, "cold": 30}[q]
    try:
        case = sp360.upsert_case(
            brand_id=str(body.get("brand_id") or "").strip(),
            lead_id=str(body.get("lead_id") or "").strip(),
            stage="sales",
            status="qualified",
            owner=str(body.get("owner") or "ai").strip(),
            score=score,
        )
        sp360.add_event(
            brand_id=case.brand_id,
            lead_id=case.lead_id,
            engine="sales",
            event_type="qualification",
            payload={"qualification": q, "score": score},
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"case": case.__dict__, "qualification": q})


@app.post("/v1/salespal360/sales/followup")
def salespal360_sales_followup():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    follow = str(body.get("follow_up_at") or "").strip() or (
        datetime.now(timezone.utc) + timedelta(hours=24)
    ).isoformat()
    try:
        case = sp360.upsert_case(
            brand_id=brand_id, lead_id=lead_id, stage="sales", status="followup_due", owner="ai"
        )
        sp360.add_event(
            brand_id=brand_id,
            lead_id=lead_id,
            engine="sales",
            event_type="followup_scheduled",
            payload={"follow_up_at": follow},
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"case": case.__dict__, "follow_up_at": follow})


@app.post("/v1/salespal360/sales/meeting")
def salespal360_sales_meeting():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    ms = str(body.get("meeting_status") or "").strip().lower()
    if ms not in {"scheduled", "reschedule", "no_show", "completed"}:
        return _err(400, "invalid meeting_status")
    status = "meeting_" + ms
    try:
        case = sp360.upsert_case(
            brand_id=str(body.get("brand_id") or "").strip(),
            lead_id=str(body.get("lead_id") or "").strip(),
            stage="sales",
            status=status,
            owner="ai",
        )
        sp360.add_event(
            brand_id=case.brand_id,
            lead_id=case.lead_id,
            engine="sales",
            event_type="meeting",
            payload={"meeting_status": ms},
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"case": case.__dict__, "meeting_status": ms})


@app.post("/v1/salespal360/sales/escalate")
def salespal360_sales_escalate():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    owner = str(body.get("owner") or "").strip().lower() or "human"
    if owner not in {"senior_ai", "human", "owner"}:
        return _err(400, "owner must be senior_ai/human/owner")
    try:
        case = sp360.upsert_case(
            brand_id=str(body.get("brand_id") or "").strip(),
            lead_id=str(body.get("lead_id") or "").strip(),
            stage="sales",
            status="escalated",
            owner=owner,
        )
        sp360.add_event(
            brand_id=case.brand_id,
            lead_id=case.lead_id,
            engine="sales",
            event_type="escalation",
            payload={"owner": owner},
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"case": case.__dict__})


@app.post("/v1/salespal360/postsale/close")
def salespal360_postsale_close():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    pay = str(body.get("payment_status") or "").strip().lower()
    if pay not in {"partial_verified", "fully_verified"}:
        return _err(400, "payment_status must be partial_verified/fully_verified")
    try:
        case = sp360.upsert_case(
            brand_id=str(body.get("brand_id") or "").strip(),
            lead_id=str(body.get("lead_id") or "").strip(),
            stage="postsale",
            status="deal_closed",
            owner="ai",
            score=90 if pay == "fully_verified" else 75,
        )
        sp360.add_event(
            brand_id=case.brand_id,
            lead_id=case.lead_id,
            engine="postsale",
            event_type="deal_close_payment",
            payload={"payment_status": pay},
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"case": case.__dict__, "payment_status": pay})


@app.post("/v1/salespal360/postsale/feedback")
def salespal360_postsale_feedback():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        score = int(body.get("score") or 0)
    except (TypeError, ValueError):
        return _err(400, "score must be integer")
    if score < 1 or score > 10:
        return _err(400, "score must be between 1 and 10")
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    ev_type = "feedback_positive" if score >= 8 else ("feedback_neutral" if score >= 5 else "feedback_negative")
    try:
        case = sp360.upsert_case(
            brand_id=brand_id,
            lead_id=lead_id,
            stage="postsale",
            status="feedback_captured",
            owner="ai",
            score=min(100, score * 10),
        )
        sp360.add_event(
            brand_id=brand_id,
            lead_id=lead_id,
            engine="postsale",
            event_type=ev_type,
            payload={"score": score},
        )
    except ValueError as e:
        return _err(400, str(e))
    next_action = "referral_testimonial" if score >= 8 else ("upsell_retention" if score >= 5 else "owner_alert")
    return jsonify({"case": case.__dict__, "score": score, "next_action": next_action})


@app.post("/v1/salespal360/support/open")
def salespal360_support_open():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        t = sp360.create_ticket(
            brand_id=str(body.get("brand_id") or "").strip(),
            lead_id=str(body.get("lead_id") or "").strip(),
            channel=str(body.get("channel") or "").strip(),
            complaint_text=str(body.get("complaint") or "").strip(),
        )
        sp360.upsert_case(
            brand_id=t.brand_id,
            lead_id=t.lead_id,
            stage="support",
            status="ticket_open",
            owner="ai",
            score=50,
        )
        sp360.add_event(
            brand_id=t.brand_id,
            lead_id=t.lead_id,
            engine="support",
            event_type="ticket_open",
            payload={"ticket_id": t.id, "complaint_id": t.complaint_id},
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"ticket": t.__dict__})


@app.post("/v1/salespal360/support/update")
def salespal360_support_update():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    tid = str(body.get("ticket_id") or "").strip()
    if not tid:
        return _err(400, "ticket_id required")
    t = sp360.update_ticket(
        tid,
        status=str(body.get("status") or "").strip() or None,
        resolution=str(body.get("resolution") or "").strip() or None,
        escalation_level=str(body.get("escalation_level") or "").strip() or None,
    )
    if not t:
        return _err(404, "not found")
    sp360.add_event(
        brand_id=t.brand_id,
        lead_id=t.lead_id,
        engine="support",
        event_type="ticket_update",
        payload={"ticket_id": t.id, "status": t.status, "escalation_level": t.escalation_level},
    )
    return jsonify({"ticket": t.__dict__})


@app.post("/v1/salespal360/memory/merge")
def salespal360_memory_merge():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    patch = body.get("patch") if isinstance(body.get("patch"), dict) else {}
    try:
        m = sp360.merge_memory(
            brand_id=str(body.get("brand_id") or "").strip(),
            lead_id=str(body.get("lead_id") or "").strip(),
            patch=patch,
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"memory": m})


@app.get("/v1/salespal360/memory/get")
def salespal360_memory_get():
    brand_id = str(request.args.get("brand_id") or "").strip()
    lead_id = str(request.args.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    try:
        m = sp360.get_memory(brand_id=brand_id, lead_id=lead_id)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"memory": m})


@app.get("/v1/salespal360/analytics")
def salespal360_analytics():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        a = sp360.analytics(brand_id=brand_id)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"brand_id": brand_id, "analytics": a})


@app.post("/v1/salespal360/learning/next_action")
def salespal360_learning_next_action():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        out = sp360.learning_next_action(brand_id=brand_id)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"brand_id": brand_id, "learning": out})


def _sp360_lead_snapshot(brand_id: str) -> dict[str, Any]:
    ll = leads.list(brand_id=brand_id)
    by_source: dict[str, int] = {}
    temps = {"hot": 0, "warm": 0, "cold": 0, "unknown": 0}
    for x in ll:
        by_source[str(getattr(x, "source", None) or "unknown")] = by_source.get(str(getattr(x, "source", None) or "unknown"), 0) + 1
        raw = x.raw if isinstance(getattr(x, "raw", None), dict) else {}
        t = str(raw.get("sales_lead_temperature") or "").lower()
        if t in temps:
            temps[t] += 1
        else:
            temps["unknown"] += 1
    return {"leads_total": len(ll), "lead_source_tracking": by_source, "mark_temperature": temps}


@app.get("/v1/salespal360/sales/dashboard")
def salespal360_sales_dashboard():
    """M2 Salespal: sales dashboard + lead management snapshot."""
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        core = sp360.m2_core_metrics(brand_id=brand_id)
        snap = _sp360_lead_snapshot(brand_id)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"brand_id": brand_id, "sales_dashboard": core, "customer_crm_snapshot": snap})


@app.get("/v1/salespal360/reports/sales")
def salespal360_report_sales():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    ev = [e.__dict__ for e in sp360.events.values() if e.brand_id == brand_id and e.engine in {"sales", "marketing", "postsale"}]
    ev.sort(key=lambda x: x["created_at"], reverse=True)
    return jsonify({"brand_id": brand_id, "sales_reports": ev[:200]})


@app.get("/v1/salespal360/reports/leads")
def salespal360_report_leads():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    return jsonify({"brand_id": brand_id, **_sp360_lead_snapshot(brand_id)})


@app.get("/v1/salespal360/reports/performance")
def salespal360_report_performance():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        a = sp360.analytics(brand_id=brand_id)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify(
        {
            "brand_id": brand_id,
            "performance_reports": a,
            "daily_reports_proxy": [x.__dict__ for x in sp360.list_activity(brand_id=brand_id, limit=20)],
        }
    )


@app.get("/v1/salespal360/platform/summary")
def salespal360_platform_summary():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        s = sp360.get_platform_settings(brand_id=brand_id)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify(
        {
            "brand_id": brand_id,
            "user_management_stub": {"max_users": 50, "active_users_estimate": 3},
            "role_based_access": s.get("roles") or ["admin", "agent", "viewer"],
            "system_settings": s,
            "integration_api": {"status": "ok", "base_path": "/v1"},
            "security_compliance": {"dnd_respect": True, "consent_log_stub": True},
        }
    )


@app.post("/v1/salespal360/platform/settings")
def salespal360_platform_settings():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    patch = body.get("patch") if isinstance(body.get("patch"), dict) else {}
    try:
        out = sp360.patch_platform_settings(brand_id=brand_id, patch=patch)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"brand_id": brand_id, "settings": out})


@app.get("/v1/salespal360/admin/summary")
def salespal360_admin_summary():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        logs = [x.__dict__ for x in sp360.list_activity(brand_id=brand_id, limit=50)]
        crm = sp360.get_crm_config(brand_id=brand_id)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"brand_id": brand_id, "admin_dashboard": {"recent_activity": logs}, "crm_configuration": crm})


@app.get("/v1/salespal360/admin/activity")
def salespal360_admin_activity_list():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        items = [x.__dict__ for x in sp360.list_activity(brand_id=brand_id)]
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"brand_id": brand_id, "user_activity_logs": items})


@app.post("/v1/salespal360/admin/activity")
def salespal360_admin_activity_log():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        e = sp360.log_activity(
            brand_id=str(body.get("brand_id") or "").strip(),
            actor=str(body.get("actor") or "admin").strip(),
            action=str(body.get("action") or "manual").strip(),
            meta=body.get("meta") if isinstance(body.get("meta"), dict) else {},
        )
    except ValueError as err:
        return _err(400, str(err))
    return jsonify({"entry": e.__dict__})


@app.get("/v1/salespal360/admin/health")
def salespal360_admin_health():
    return jsonify({"system_health": {"store": "ok", "api": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}})


@app.get("/v1/salespal360/crm/config")
def salespal360_crm_config_get():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        c = sp360.get_crm_config(brand_id=brand_id)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"brand_id": brand_id, "crm_config": c})


@app.post("/v1/salespal360/crm/config")
def salespal360_crm_config_post():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    patch = body.get("patch") if isinstance(body.get("patch"), dict) else {}
    try:
        c = sp360.patch_crm_config(brand_id=brand_id, patch=patch)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"brand_id": brand_id, "crm_config": c})


@app.post("/v1/salespal360/tasks")
def salespal360_tasks_create():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        t = sp360.create_task(
            brand_id=str(body.get("brand_id") or "").strip(),
            title=str(body.get("title") or "").strip(),
            lead_id=str(body.get("lead_id") or "").strip() or None,
            due_at=str(body.get("due_at") or "").strip() or None,
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"task": t.__dict__})


@app.get("/v1/salespal360/tasks")
def salespal360_tasks_list():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        items = [x.__dict__ for x in sp360.list_tasks(brand_id=brand_id)]
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"brand_id": brand_id, "tasks": items})


@app.post("/v1/salespal360/documents")
def salespal360_documents_create():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        d = sp360.register_document(
            brand_id=str(body.get("brand_id") or "").strip(),
            name=str(body.get("name") or "").strip(),
            kind=str(body.get("kind") or "file").strip(),
            lead_id=str(body.get("lead_id") or "").strip() or None,
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"document": d.__dict__})


@app.get("/v1/salespal360/documents")
def salespal360_documents_list():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        items = [x.__dict__ for x in sp360.documents.values() if x.brand_id == brand_id]
    except Exception:
        items = []
    items.sort(key=lambda x: x["created_at"], reverse=True)
    return jsonify({"brand_id": brand_id, "documents": items})


@app.post("/v1/salespal360/esign")
def salespal360_esign_create():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        r = sp360.request_esign(
            brand_id=str(body.get("brand_id") or "").strip(),
            lead_id=str(body.get("lead_id") or "").strip(),
            document_name=str(body.get("document_name") or "contract").strip(),
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"esign": r.__dict__})


@app.get("/v1/salespal360/esign")
def salespal360_esign_list():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    items = [x.__dict__ for x in sp360.esign_requests.values() if x.brand_id == brand_id]
    items.sort(key=lambda x: x["created_at"], reverse=True)
    return jsonify({"brand_id": brand_id, "esign_requests": items})


@app.get("/v1/salespal360/forecast")
def salespal360_forecast():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    try:
        f = sp360.sales_forecast_stub(brand_id=brand_id)
    except ValueError as e:
        return _err(400, str(e))
    return jsonify(f)


@app.get("/v1/salespal360/features/status")
def salespal360_features_status():
    """M2 checklist: what is wired (stubs vs live)."""
    return jsonify(
        {
            "milestone": "M2 SalesPal 360",
            "features": {
                "lead_source_tracking": {"implemented": True, "endpoint": "GET /v1/salespal360/reports/leads"},
                "sales_pipeline_management": {"implemented": True, "endpoint": "GET /v1/salespal360/crm/config + case upsert"},
                "task_reminder_system": {"implemented": True, "endpoint": "GET/POST /v1/salespal360/tasks"},
                "sales_forecasting": {"implemented": True, "endpoint": "GET /v1/salespal360/forecast"},
                "email_sms_integration": {"implemented": "stub", "note": "Use /v1/marketing/ops and webhooks"},
                "document_storage": {"implemented": True, "endpoint": "GET/POST /v1/salespal360/documents"},
                "social_media_integration": {"implemented": "stub", "note": "Marketing M1 social ops"},
                "api_integration_layer": {"implemented": True, "endpoint": "GET /v1/salespal360/platform/summary"},
                "e_signature": {"implemented": True, "endpoint": "GET/POST /v1/salespal360/esign"},
            },
        }
    )


def _sale_next_action_from_score(score: int) -> dict[str, str]:
    if score >= 8:
        return {"bucket": "positive", "action": "ask_referral"}
    if score >= 5:
        return {"bucket": "neutral", "action": "soft_referral"}
    return {"bucket": "negative", "action": "ai_resolve_first"}


_SALE_ALLOWED_BY_STATUS: dict[str, set[str]] = {
    "started": {
        "retry_2hr_then_630pm_then_nextday",
        "retry_next_slot",
        "stop_wrong_number",
        "conversation_open",
        "wa_followup_d0_d1_d3_d5_d7",
    },
    "call_no_answer": {"retry_2hr_then_630pm_then_nextday"},
    "call_busy": {"retry_next_slot"},
    "call_connected": {"conversation_open"},
    "wa_followup_day0_1_3_5_7": {
        "conversation_open",
        "set_user_type_genuine",
        "capture_need_budget_timeline",
        "lead_type_hot",
        "lead_type_warm",
        "lead_type_cold",
        "warm_whatsapp_d1_d3_d5",
        "cold_weekly_campaign_broadcast",
    },
    "conversation": {"set_user_type_abusive", "set_user_type_timepass", "set_user_type_genuine"},
    "exit_block": set(),
    "short_exit": set(),
    "qualification": {"capture_need_budget_timeline"},
    "capture_need_budget_timeline": {"lead_type_hot", "lead_type_warm", "lead_type_cold"},
    "lead_type_hot": {"notify_owner_immediately", "push_call_or_meeting_same_day", "visit_or_meeting", "warm_whatsapp_d1_d3_d5"},
    "push_call_or_meeting_same_day": {"schedule_datetime", "send_location_or_link", "visit_or_meeting"},
    "warm_whatsapp_d1_d3_d5": {"warm_reminder_1d_sameday_1hr"},
    "lead_type_warm": {"warm_whatsapp_d1_d3_d5", "warm_call_next_day_11_or_630", "warm_convert_to_meeting"},
    "lead_type_cold": {"cold_weekly_campaign_broadcast"},
    "visit_or_meeting": {"schedule_datetime", "send_location_or_link", "warm_reminder_1d_sameday_1hr", "visit_status_done", "visit_status_no_show"},
    "warm_convert_to_meeting": {"visit_status_done", "visit_status_no_show"},
    "visit_status_done": {"proceed_after_visit_done"},
    "visit_status_no_show": {"reschedule_next_day"},
    "proceed_after_visit_done": {"need_escalation_yes", "need_escalation_critical"},
    "need_escalation_yes": {"senior_ai_call_male_voice"},
    "need_escalation_critical": {"human_intervention"},
    "senior_ai_call_male_voice": {"outcome"},
    "human_intervention": {"outcome"},
    "outcome": {"notify_owner"},
    "notify_owner": {"ask_rating_1_to_10"},
    "ask_rating_1_to_10": {"score_1_4", "score_5_7", "score_8_10"},
    "score_8_10": {"positive", "ask_referral"},
    "score_5_7": {"neutral", "soft_referral"},
    "score_1_4": {"negative", "ai_resolve_first"},
    "ai_resolve_first": {"resolved_yes", "resolved_no"},
    "resolved_no": {"owner_alert"},
    "resolved_yes": {"update_dashboard"},
    "owner_alert": set(),
    "ask_referral": {"update_dashboard"},
    "soft_referral": {"update_dashboard"},
    "update_dashboard": {"morning_plan_today", "evening_report_today", "learning_loop"},
    "learning_loop": {"sales_system"},
    "sales_system": set(),
}


def _sale_next_allowed(status: str) -> list[str]:
    return sorted(_SALE_ALLOWED_BY_STATUS.get(status, set()))


def _sale_set_status(brand_id: str, lead_id: str, status: str, *, stage: str = "flow", payload: dict[str, Any] | None = None, next_action_at: str | None = None):
    s = saleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage=stage, status=status, next_action_at=next_action_at)
    e = saleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="diagram_action", payload={"status": status, **(payload or {})})
    return s, e


@app.get("/sale")
def sale_console():
    html = """<!doctype html><html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Sales Operations Console</title><style>
  :root{--bg:#f1f5f9;--panel:#ffffff;--line:#d9e1ea;--text:#0f172a;--muted:#64748b;--accent:#0f4c81;--ok:#0f766e;--warn:#9f1239}
  *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,Arial,sans-serif}
  .wrap{max-width:1360px;margin:0 auto;padding:16px}
  .hero{background:linear-gradient(135deg,#0f4c81,#155e75);color:#fff;padding:14px 16px;border-radius:12px;margin-bottom:12px}
  .layout{display:grid;grid-template-columns:2.3fr 1fr;gap:12px}@media(max-width:1100px){.layout{grid-template-columns:1fr}}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:12px;margin-bottom:12px}
  .flow{position:relative}
  .flow:before{content:"";position:absolute;left:16px;top:8px;bottom:8px;width:3px;background:#e2e8f0;border-radius:3px}
  .step{position:relative;padding-left:34px}
  .step:before{content:"";position:absolute;left:10px;top:18px;width:14px;height:14px;border-radius:999px;background:#fff;border:3px solid #94a3b8}
  .step.active:before{border-color:#0f4c81;background:#dbeafe}
  .step.done:before{border-color:#0f766e;background:#ccfbf1}
  h2{margin:0 0 8px;font-size:17px} h3{margin:0 0 8px;font-size:14px}
  label{display:block;font-size:12px;font-weight:700;color:var(--muted);margin:7px 0 3px;text-transform:uppercase;letter-spacing:.03em}
  input,select,textarea{width:100%;padding:8px 10px;border:1px solid var(--line);border-radius:8px;background:#fff;font:inherit}
  textarea{min-height:66px;resize:vertical}
  .row{display:grid;grid-template-columns:1fr 1fr;gap:10px}@media(max-width:780px){.row{grid-template-columns:1fr}}
  .btns{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
  button{border:0;border-radius:8px;padding:8px 10px;background:var(--accent);color:#fff;font-weight:700;cursor:pointer}
  button.alt{background:#1f2937} button.good{background:var(--ok)} button.warn{background:var(--warn)}
  button.ghost{background:#475569} button[disabled]{opacity:.45;cursor:not-allowed}
  .chips{display:flex;flex-wrap:wrap;gap:6px}
  .chip{font-size:11px;padding:4px 8px;border-radius:999px;background:#e2e8f0;color:#0f172a;font-weight:700}
  .hint{font-size:12px;color:var(--muted)} .tiny{font-size:11px;color:#475569}
  pre{margin:0;white-space:pre-wrap;word-break:break-word;background:#0f172a;color:#e2e8f0;border-radius:10px;padding:10px;max-height:70vh;overflow:auto;font-size:12px}
  .state{display:grid;grid-template-columns:1fr;gap:8px}
</style></head><body><div class="wrap">
  <div class="hero"><strong>Sales Operations Console</strong><div class="hint" style="color:#dbeafe">Complete lead-to-outcome operations aligned with Milestone 3 flow.</div></div>
  <div class="layout"><main class="flow">
    <section class="card step active" id="s-intake"><h2>Lead Intake, Locale Setup, Parallel Outreach</h2>
      <div class="row"><div><label>Brand Id</label><input id="brand" value="demo"/></div><div><label>Lead Id</label><input id="lead" placeholder="lead id"/></div></div>
      <div class="row"><div><label>Admin API Key</label><input id="adminkey" placeholder="X-Admin-Api-Key"/></div><div><label>Timezone</label><input id="tz" value="Asia/Kolkata"/></div></div>
      <div class="row"><div><label>Default Language</label><select id="lang"><option value="hinglish" selected>hinglish</option><option value="hi">hi</option><option value="en">en</option></select></div><div><label>Auto Language Switch</label><select id="autosw"><option value="true" selected>true</option><option value="false">false</option></select></div></div>
      <div class="chips"><span class="chip">Call Active 9AM-9PM</span><span class="chip">WhatsApp Active 24x7</span></div>
      <div class="btns"><button onclick="startParallel()">Start Call + WhatsApp Together</button><button class="ghost" onclick="timeline()">Refresh Timeline</button></div>
    </section>

    <section class="card step" id="s-voice"><h2>Direct Bot Call (Voice Provider)</h2>
      <div class="row"><div><label>Phone Number (required if lead not found)</label><input id="voice_phone" placeholder="+91XXXXXXXXXX"/></div><div><label>Contact Name (optional)</label><input id="voice_name" placeholder="Customer name"/></div></div>
      <div class="row"><div><label>Call Locale</label><select id="voice_locale"><option value="hing" selected>hing</option><option value="hi">hi</option><option value="en">en</option></select></div><div><label>Action</label><button onclick="callNow()">Call Bot Now</button></div></div>
      <div class="tiny">Uses Tata voice integration via <code>/v1/integrations/voice/call_now</code>. If lead_id is empty, system can create/find lead by phone.</div>
    </section>

    <section class="card step" id="s-channel"><h2>Channel Outcomes (Call + WhatsApp)</h2>
      <div class="row"><div><label>Call Result</label><select id="cr"><option>no_answer</option><option>busy</option><option>wrong_number</option><option>connected</option></select></div><div><label>WhatsApp Reply (No -> Follow-up, Yes -> Conversation)</label><select id="wr"><option>no</option><option>yes</option></select></div></div>
      <div class="btns">
        <button onclick="callResult()">Apply Call Result</button>
        <button class="alt" onclick="waReply()">Apply WhatsApp Reply</button>
      </div>
      <div class="tiny">Branch is applied automatically by call result and WhatsApp reply above.</div>
    </section>

    <section class="card step" id="s-qual"><h2>User Classification and Qualification</h2>
      <div class="row"><div><label>User Type</label><select id="ut"><option>genuine</option><option>timepass</option><option>abusive</option></select></div><div><label>Lead Type</label><select id="lt"><option>hot</option><option>warm</option><option>cold</option></select></div></div>
      <label>Capture Need / Budget / Timeline</label><textarea id="nbt">Need: demo\\nBudget: 50000\\nTimeline: 7 days</textarea>
      <div class="btns"><button onclick="userType()">Set User Type</button><button onclick="qualify()">Set Qualification + Lead Type</button></div>
      <div class="tiny">User type and lead type are captured via the two apply buttons above.</div>
    </section>

    <section class="card step" id="s-flow"><h2>Hot, Warm, and Cold Lead Workflows</h2>
      <div class="row"><div><label>Schedule Date Time</label><input id="mt" type="datetime-local"/></div><div><label>Location / Link</label><input id="mlink" placeholder="https://meet.google.com/..."/></div></div>
      <div class="row"><div><label>Flow Action (Hot/Warm/Cold)</label><select id="flow_action">
        <option value="notify_owner_immediately">Notify Owner Immediately</option>
        <option value="push_call_or_meeting_same_day">Push Call or Meeting Same Day</option>
        <option value="visit_or_meeting">Visit or Meeting</option>
        <option value="schedule_datetime">Schedule Date Time</option>
        <option value="send_location_or_link">Send Location or Link</option>
        <option value="warm_whatsapp_d1_d3_d5">WhatsApp Day1 Day3 Day5</option>
        <option value="warm_reminder_1d_sameday_1hr">Reminder 1 day / same day / 1hr</option>
        <option value="warm_call_next_day_11_or_630">Call next day 11AM or 6:30PM</option>
        <option value="warm_convert_to_meeting">Convert to Meeting</option>
        <option value="cold_weekly_campaign_broadcast">Weekly Campaign Broadcast</option>
      </select></div><div><label>Apply</label><button onclick="runSelectedAction('flow_action')">Apply Flow Action</button></div></div>
    </section>

    <section class="card step" id="s-escalate"><h2>Visit Status, Escalation, and Outcome</h2>
      <div class="row"><div><label>Visit/Escalation Action</label><select id="escalation_action">
        <option value="visit_status_done">Visit Status Done</option>
        <option value="proceed_after_visit_done">Proceed</option>
        <option value="visit_status_no_show">Visit Status No Show</option>
        <option value="reschedule_next_day">Reschedule Next Day</option>
        <option value="need_escalation_yes">Need Escalation Yes</option>
        <option value="senior_ai_call_male_voice">Senior AI Call Male Voice</option>
        <option value="need_escalation_critical">Need Escalation Critical</option>
        <option value="human_intervention">Human Intervention</option>
        <option value="outcome">Outcome</option>
        <option value="notify_owner">Notify Owner</option>
        <option value="ask_rating_1_to_10">Ask Rating 1 to 10</option>
      </select></div><div><label>Apply</label><button onclick="runSelectedAction('escalation_action')">Apply Visit/Escalation</button></div></div>
    </section>

    <section class="card step" id="s-score"><h2>Feedback Scoring, Resolution, and Reporting</h2>
      <div class="row"><div><label>Score (1-10)</label><input id="score" type="number" min="1" max="10" value="8"/></div><div><label>Resolved?</label><select id="res"><option>yes</option><option>no</option></select></div></div>
      <div class="btns"><button class="good" onclick="score()">Submit Score</button></div>
      <div class="row"><div><label>Score/Reporting Action</label><select id="score_action">
        <option value="score_1_4">Score 1-4 Negative</option>
        <option value="ai_resolve_first">AI Resolve First</option>
        <option value="resolved_no">Resolved No</option>
        <option value="owner_alert">Owner Alert</option>
        <option value="resolved_yes">Resolved Yes</option>
        <option value="update_dashboard">Update Dashboard</option>
        <option value="score_5_7">Score 5-7 Neutral</option>
        <option value="soft_referral">Soft Referral</option>
        <option value="score_8_10">Score 8-10 Positive</option>
        <option value="ask_referral">Ask Referral</option>
        <option value="morning_plan_today">Morning Plan Today</option>
        <option value="evening_report_today">Evening Report Today</option>
        <option value="learning_loop">Learning Loop</option>
        <option value="sales_system">Sales System</option>
      </select></div><div><label>Apply</label><button onclick="runSelectedAction('score_action')">Apply Score/Reporting Action</button></div></div>
    </section>
  </main><aside>
    <section class="card"><h3>Current Flow State</h3>
      <div class="state">
        <div><span class="tiny">Current status</span><div id="currentStatus" class="chip">unknown</div></div>
        <div><span class="tiny">Next allowed actions</span><div id="nextAllowed" class="chips"></div></div>
      </div>
    </section>
    <section class="card"><h3>Analytics + Utilities</h3>
      <div class="btns"><button onclick="dash()">Dashboard API</button><button class="alt" onclick="dayPlan()">Day Plan API</button><button class="alt" onclick="learn()">Learning API</button></div>
    </section>
    <section class="card"><h3>API Response</h3><pre id="out">Ready.</pre></section>
  </aside></div></div>
<script>
const out=document.getElementById('out');
const currentStatus=document.getElementById('currentStatus');
const nextAllowed=document.getElementById('nextAllowed');
let current='started';
let allowed=[];

function headers(){const h={'Content-Type':'application/json'};const k=(document.getElementById('adminkey').value||'').trim();if(k)h['X-Admin-Api-Key']=k;return h;}
function brand(){return (document.getElementById('brand').value||'').trim();}
function lead(){return (document.getElementById('lead').value||'').trim();}
function renderAllowed(){
  currentStatus.textContent=current||'unknown';
  nextAllowed.innerHTML='';
  for(const a of allowed){const s=document.createElement('span');s.className='chip';s.textContent=a;nextAllowed.appendChild(s);}
  document.querySelectorAll('[data-a]').forEach(btn=>{btn.disabled=allowed.length>0 && !allowed.includes(btn.dataset.a);});
}
function consumeState(j){
  if(j && j.session && j.session.status){current=j.session.status;}
  if(j && Array.isArray(j.next_allowed_actions)){allowed=j.next_allowed_actions;}
  renderAllowed();
}
async function api(p,m='GET',b=null){
  const r=await fetch(p,{method:m,headers:headers(),body:b?JSON.stringify(b):null});
  const j=await r.json().catch(()=>({raw:'non-json'}));
  if(!r.ok)j._http={status:r.status};
  out.textContent=JSON.stringify(j,null,2);
  consumeState(j);
  return j;
}
async function runAction(kind){await api('/v1/sale/action','POST',{brand_id:brand(),lead_id:lead(),action:kind,meeting_time_local:(document.getElementById('mt').value||''),location_link:(document.getElementById('mlink').value||'')});}
async function runSelectedAction(selId){const k=(document.getElementById(selId).value||'').trim();if(!k)return;await runAction(k);}
async function startParallel(){const j=await api('/v1/sale/start','POST',{brand_id:brand(),lead_id:lead(),timezone:document.getElementById('tz').value,language:document.getElementById('lang').value,auto_language_switch:(document.getElementById('autosw').value==='true')});if(j&&j.session&&j.session.status){current=j.session.status;allowed=['retry_2hr_then_630pm_then_nextday','retry_next_slot','stop_wrong_number','conversation_open','wa_followup_d0_d1_d3_d5_d7'];renderAllowed();}}
async function callResult(){const j=await api('/v1/sale/call_result','POST',{brand_id:brand(),lead_id:lead(),result:document.getElementById('cr').value});if(j&&j.session&&j.session.status){current=j.session.status;allowed=[];renderAllowed();}}
async function waReply(){const j=await api('/v1/sale/wa_reply','POST',{brand_id:brand(),lead_id:lead(),reply:document.getElementById('wr').value});if(j&&j.session&&j.session.status){current=j.session.status;allowed=[];renderAllowed();}}
async function userType(){const j=await api('/v1/sale/user_type','POST',{brand_id:brand(),lead_id:lead(),user_type:document.getElementById('ut').value});if(j&&j.session&&j.session.status){current=j.session.status;allowed=[];renderAllowed();}}
async function qualify(){const j=await api('/v1/sale/qualification','POST',{brand_id:brand(),lead_id:lead(),lead_type:document.getElementById('lt').value,need_budget_timeline:document.getElementById('nbt').value});if(j&&j.session&&j.session.status){current=j.session.status;allowed=[];renderAllowed();}}
async function score(){await api('/v1/sale/score','POST',{brand_id:brand(),lead_id:lead(),score:Number(document.getElementById('score').value||0),resolved:(document.getElementById('res').value==='yes')});}
async function callNow(){
  const j=await api('/v1/integrations/voice/call_now','POST',{
    brand_id:brand(),
    lead_id:lead(),
    phone:(document.getElementById('voice_phone').value||'').trim(),
    name:(document.getElementById('voice_name').value||'').trim(),
    locale:(document.getElementById('voice_locale').value||'hing').trim()
  });
  if(j&&j.lead_id){document.getElementById('lead').value=j.lead_id;}
}
async function dash(){await api('/v1/sale/dashboard?brand_id='+encodeURIComponent(brand()));}
async function learn(){await api('/v1/sale/learning','POST',{brand_id:brand()});}
async function timeline(){
  const j=await api('/v1/sale/timeline?brand_id='+encodeURIComponent(brand())+'&lead_id='+encodeURIComponent(lead()));
  if(j&&j.session&&j.session.status){current=j.session.status;}
  allowed=[];renderAllowed();
}
async function dayPlan(){await api('/v1/sale/day_plan','POST',{brand_id:brand()});}
renderAllowed();
</script></body></html>"""
    return Response(html, mimetype="text/html; charset=utf-8")


@app.post("/v1/sale/start")
def sale_start():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    tz = str(body.get("timezone") or "Asia/Kolkata").strip()
    lang = str(body.get("language") or "hinglish").strip().lower()
    auto = bool(body.get("auto_language_switch", True))
    try:
        s = saleops.upsert_session(
            brand_id=brand_id, lead_id=lead_id, timezone=tz, language=lang, auto_language_switch=auto, stage="init", status="started"
        )
        saleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="start", payload={"timezone": tz, "language": lang})
    except ValueError as e:
        return _err(400, str(e))
    # Best-effort start parallel outreach if lead exists in core store.
    if leads.get(lead_id):
        try:
            start_parallel_outreach(leads, conversations, lead_id, tz_name=tz, auto_sync_lead=_auto_sync_lead)
        except Exception:
            pass
    leads.merge_raw(lead_id, {"sales_timezone": tz, "sales_language": lang, "sales_auto_language_switch": auto})
    return jsonify({"session": s.__dict__})


@app.post("/v1/sale/call_result")
def sale_call_result():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    result = str(body.get("result") or "").strip().lower()
    if result not in {"no_answer", "busy", "wrong_number", "connected"}:
        return _err(400, "invalid result")
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    next_action = None
    flow_steps: list[str] = []
    status = "call_" + result
    if result == "no_answer":
        now = datetime.now(timezone.utc)
        next_action = (now + timedelta(hours=2)).isoformat()
        flow_steps = [
            "Retry after 2 hours",
            "If still pending, retry at 18:30 local",
            "If still pending, retry next day same slot",
        ]
    elif result == "busy":
        next_action = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        flow_steps = ["Retry in next available slot"]
    elif result == "wrong_number":
        status = "stop_wrong_number"
        flow_steps = ["Stop outreach for this contact"]
    else:
        flow_steps = ["Open conversation path"]
    try:
        s = saleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage="call", status=status, next_action_at=next_action)
        saleops.add_event(
            brand_id=brand_id,
            lead_id=lead_id,
            event_type="call_result",
            payload={"result": result, "next_action_at": next_action, "flow_steps": flow_steps},
        )
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"session": s.__dict__, "result": result, "next_action_at": next_action, "flow_steps": flow_steps})


@app.post("/v1/sale/wa_reply")
def sale_wa_reply():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    reply = str(body.get("reply") or "").strip().lower()
    if reply not in {"yes", "no"}:
        return _err(400, "reply must be yes/no")
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    # Diagram mapping:
    # - Yes -> conversation
    # - No -> WA follow-up sequence (Day0/1/3/5/7)
    status = "conversation" if reply == "yes" else "wa_followup_day0_1_3_5_7"
    seq = [0, 1, 3, 5, 7] if reply == "no" else []
    try:
        s = saleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage="whatsapp", status=status)
        saleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="wa_reply", payload={"reply": reply, "followup_days": seq})
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"session": s.__dict__, "reply": reply, "followup_days": seq})


@app.post("/v1/sale/user_type")
def sale_user_type():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    u = str(body.get("user_type") or "").strip().lower()
    if u not in {"abusive", "timepass", "genuine"}:
        return _err(400, "invalid user_type")
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    status = "qualification"
    if u == "abusive":
        status = "exit_block"
    elif u == "timepass":
        status = "short_exit"
    try:
        s = saleops.upsert_session(brand_id=brand_id, lead_id=lead_id, user_type=u, stage="qualification", status=status)
        saleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="user_type", payload={"user_type": u})
    except ValueError as e:
        return _err(400, str(e))
    leads.merge_raw(lead_id, {"sales_user_type": u})
    return jsonify({"session": s.__dict__})


@app.post("/v1/sale/qualification")
def sale_qualification():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    lt = str(body.get("lead_type") or "").strip().lower()
    if lt not in {"hot", "warm", "cold"}:
        return _err(400, "lead_type must be hot/warm/cold")
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    nbt = str(body.get("need_budget_timeline") or "").strip()
    status = "lead_type_" + lt
    try:
        s = saleops.upsert_session(brand_id=brand_id, lead_id=lead_id, lead_type=lt, stage="qualification", status=status)
        saleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="lead_type", payload={"lead_type": lt, "need_budget_timeline": nbt})
    except ValueError as e:
        return _err(400, str(e))
    leads.merge_raw(lead_id, {"sales_lead_temperature": lt, "sales_need_budget_timeline": nbt})
    return jsonify({"session": s.__dict__, "lead_type": lt, "status": status})


@app.post("/v1/sale/meeting")
def sale_meeting():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    ms = str(body.get("meeting_status") or "").strip().lower()
    if ms not in {"scheduled", "done", "no_show", "reschedule"}:
        return _err(400, "invalid meeting_status")
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    status = "visit_" + ms
    if ms == "no_show":
        next_at = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    else:
        next_at = None
    try:
        s = saleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage="meeting", status=status, next_action_at=next_at)
        saleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="visit_status", payload={"meeting_status": ms, "next_action_at": next_at})
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"session": s.__dict__, "meeting_status": ms, "next_action_at": next_at})


@app.post("/v1/sale/escalation")
def sale_escalation():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    level = str(body.get("level") or "").strip().lower()
    if level not in {"none", "senior_ai", "human", "critical"}:
        return _err(400, "invalid level")
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    status = "escalation_" + level
    try:
        s = saleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage="escalation", status=status)
        saleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="escalation", payload={"level": level})
    except ValueError as e:
        return _err(400, str(e))
    leads.merge_raw(lead_id, {"sales_escalation": level})
    return jsonify({"session": s.__dict__, "level": level})


@app.post("/v1/sale/score")
def sale_score():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        score = int(body.get("score") or 0)
    except (TypeError, ValueError):
        return _err(400, "score must be integer")
    if score < 1 or score > 10:
        return _err(400, "score must be 1..10")
    resolved = bool(body.get("resolved", True))
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    plan = _sale_next_action_from_score(score)
    # Keep score endpoint focused on score branching only; resolution branching
    # is explicitly handled by action transitions (resolved_yes / resolved_no).
    try:
        s = saleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage="score", status=plan["action"], score=score)
        saleops.add_event(
            brand_id=brand_id,
            lead_id=lead_id,
            event_type="score",
            payload={"score": score, "resolved_hint": resolved, "bucket": plan["bucket"], "action": plan["action"]},
        )
    except ValueError as e:
        return _err(400, str(e))
    leads.merge_raw(lead_id, {"sales_feedback_score": score})
    return jsonify({"session": s.__dict__, "score_bucket": plan["bucket"], "next_action": plan["action"], "next_allowed_actions": _sale_next_allowed(plan["action"])})


@app.post("/v1/sale/action")
def sale_action():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    action = str(body.get("action") or "").strip().lower()
    meeting_time_local = str(body.get("meeting_time_local") or "").strip()
    location_link = str(body.get("location_link") or "").strip()
    all_actions = {a for v in _SALE_ALLOWED_BY_STATUS.values() for a in v}
    if action not in all_actions:
        return _err(400, "invalid action")

    cur = saleops.get_session(brand_id=brand_id, lead_id=lead_id)
    current_status = cur.status if cur else "started"
    allowed_next = _sale_next_allowed(current_status)
    if action not in allowed_next:
        return _err(
            409,
            f"invalid transition from {current_status} -> {action}; allowed next: {', '.join(allowed_next) if allowed_next else '(none)'}",
        )

    stage = "action"
    status = action
    payload: dict[str, Any] = {}
    next_action_at = None
    if action == "retry_2hr_then_630pm_then_nextday":
        stage = "call"
        payload = {"steps": ["retry_2h", "retry_18_30_local", "retry_next_day_same_slot"]}
        next_action_at = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    elif action == "retry_next_slot":
        stage = "call"
        next_action_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    elif action == "stop_wrong_number":
        stage = "call"
        status = "stop_wrong_number"
    elif action == "conversation_open":
        stage = "whatsapp"
    elif action == "wa_followup_d0_d1_d3_d5_d7":
        stage = "whatsapp"
        payload = {"followup_days": [0, 1, 3, 5, 7]}
    elif action in {"set_user_type_abusive", "set_user_type_timepass", "set_user_type_genuine"}:
        stage = "qualification"
        if action == "set_user_type_abusive":
            status = "exit_block"
            leads.merge_raw(lead_id, {"sales_user_type": "abusive"})
        elif action == "set_user_type_timepass":
            status = "short_exit"
            leads.merge_raw(lead_id, {"sales_user_type": "timepass"})
        else:
            status = "qualification"
            leads.merge_raw(lead_id, {"sales_user_type": "genuine"})
    elif action == "capture_need_budget_timeline":
        stage = "qualification"
        status = "capture_need_budget_timeline"
    elif action in {"lead_type_hot", "lead_type_warm", "lead_type_cold"}:
        stage = "qualification"
        status = action
        leads.merge_raw(lead_id, {"sales_lead_temperature": action.replace("lead_type_", "")})
    elif action in {"notify_owner_immediately", "notify_owner", "owner_alert"}:
        stage = "owner"
    elif action in {"push_call_or_meeting_same_day", "visit_or_meeting", "warm_convert_to_meeting"}:
        stage = "meeting"
    elif action == "schedule_datetime":
        stage = "meeting"
        status = "schedule_datetime"
        payload = {"meeting_time_local": meeting_time_local}
    elif action == "send_location_or_link":
        stage = "meeting"
        status = "send_location_or_link"
        payload = {"location_link": location_link}
    elif action == "warm_whatsapp_d1_d3_d5":
        stage = "followup"
        payload = {"followup_days": [1, 3, 5]}
    elif action == "warm_call_next_day_11_or_630":
        stage = "followup"
        payload = {"next_day_slots_local": ["11:00", "18:30"]}
        next_action_at = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    elif action == "warm_reminder_1d_sameday_1hr":
        stage = "followup"
        payload = {"reminders": ["1_day", "same_day", "1_hour_before"]}
    elif action == "cold_weekly_campaign_broadcast":
        stage = "campaign"
        payload = {"cadence": "weekly"}
    elif action in {"visit_status_done", "visit_status_no_show"}:
        stage = "meeting"
        status = action
    elif action == "reschedule_next_day":
        stage = "meeting"
        next_action_at = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    elif action in {"proceed_after_visit_done", "need_escalation_yes", "need_escalation_critical", "senior_ai_call_male_voice", "human_intervention", "outcome"}:
        stage = "escalation"
    elif action == "ask_rating_1_to_10":
        stage = "score"
    elif action in {"score_1_4", "score_5_7", "score_8_10"}:
        stage = "score"
    elif action in {"positive", "neutral", "negative"}:
        stage = "score"
    elif action in {"resolved_yes", "resolved_no"}:
        stage = "score"
    elif action in {"ask_referral", "soft_referral", "ai_resolve_first"}:
        stage = "score"
    elif action == "update_dashboard":
        stage = "dashboard"
    elif action in {"morning_plan_today", "evening_report_today", "learning_loop", "sales_system"}:
        stage = "dashboard"

    try:
        s = saleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage=stage, status=status, next_action_at=next_action_at)
        e = saleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="diagram_action", payload={"action": action, **payload})
    except ValueError as e:
        return _err(400, str(e))
    return jsonify(
        {
            "session": s.__dict__,
            "event": e.__dict__,
            "action": action,
            "next_action_at": next_action_at,
            "next_allowed_actions": _sale_next_allowed(status),
        }
    )


@app.get("/v1/sale/timeline")
def sale_timeline():
    brand_id = str(request.args.get("brand_id") or "").strip()
    lead_id = str(request.args.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    try:
        s = saleops.get_session(brand_id=brand_id, lead_id=lead_id)
        ev = [e.__dict__ for e in saleops.list_events(brand_id=brand_id, lead_id=lead_id)]
    except ValueError as e:
        return _err(400, str(e))
    return jsonify({"session": s.__dict__ if s else None, "events": ev})


@app.get("/v1/sale/dashboard")
def sale_dashboard():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    ss = [s for s in saleops.sessions.values() if s.brand_id == brand_id]
    ev = [e for e in saleops.events.values() if e.brand_id == brand_id]
    by_lead_type = {"hot": 0, "warm": 0, "cold": 0, "unknown": 0}
    by_status: dict[str, int] = {}
    for s in ss:
        by_lead_type[s.lead_type if s.lead_type in by_lead_type else "unknown"] += 1
        by_status[s.status] = by_status.get(s.status, 0) + 1
    avg_score = round(sum((s.score or 0) for s in ss) / len([s for s in ss if s.score is not None]), 2) if any(
        s.score is not None for s in ss
    ) else None
    return jsonify({"brand_id": brand_id, "sessions_total": len(ss), "events_total": len(ev), "lead_type_breakdown": by_lead_type, "status_breakdown": by_status, "average_score": avg_score})


@app.post("/v1/sale/day_plan")
def sale_day_plan():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    now = datetime.now(timezone.utc)
    morning = [s.lead_id for s in saleops.sessions.values() if s.brand_id == brand_id and s.lead_type == "hot"][:20]
    evening_statuses = {
        "warm_call_next_day_11_or_630",
        "reschedule_next_day",
        "visit_no_show",
        "visit_status_no_show",
        "owner_alert",
    }
    evening = [s.lead_id for s in saleops.sessions.values() if s.brand_id == brand_id and s.status in evening_statuses][:20]
    return jsonify(
        {
            "brand_id": brand_id,
            "morning_plan_today": {"generated_at": now.isoformat(), "lead_ids": morning},
            "evening_report_today": {"generated_at": now.isoformat(), "lead_ids": evening},
        }
    )


@app.post("/v1/sale/learning")
def sale_learning():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    ss = [s for s in saleops.sessions.values() if s.brand_id == brand_id]
    hot = len([s for s in ss if s.lead_type == "hot"])
    unresolved_neg = len([s for s in ss if s.status == "owner_alert"])
    if unresolved_neg:
        act = "Increase senior AI/human intervention for unresolved negative cases."
    elif hot:
        act = "Prioritize same-day meetings for hot leads and push referral asks after score >= 8."
    else:
        act = "Increase nurture cadence and qualify warm leads faster."
    return jsonify({"brand_id": brand_id, "learning_loop": {"recommended_action": act, "generated_at": datetime.now(timezone.utc).isoformat()}})


_POSTSALE_ALLOWED_BY_STATUS: dict[str, set[str]] = {
    "started": {"load_requirements"},
    "load_requirements": {"set_payment_pending", "set_payment_partial", "request_documents"},
    "set_payment_pending": {"send_payment_reminder"},
    "send_payment_reminder": {"retry_d0_d3_d5_d7"},
    "retry_d0_d3_d5_d7": {"send_payment_reminder", "set_payment_partial"},
    "set_payment_partial": {"ask_payment_proof"},
    "ask_payment_proof": {"receive_proof"},
    "receive_proof": {"send_to_owner_verification"},
    "send_to_owner_verification": {"owner_whatsapp_verify_payment"},
    "owner_whatsapp_verify_payment": {"owner_confirm_yes", "owner_confirm_no"},
    "owner_confirm_yes": {"send_second_confirmation"},
    "send_second_confirmation": {"confirm_again_yes", "confirm_again_no"},
    "confirm_again_yes": {"update_payment"},
    "confirm_again_no": {"cancel_update"},
    "owner_confirm_no": {"cancel_update"},
    "cancel_update": {"set_payment_partial"},
    "request_documents": {"doc_followup_d0_d2_d4", "receive_document"},
    "doc_followup_d0_d2_d4": {"request_documents", "receive_document"},
    "receive_document": {"validate_document"},
    "validate_document": {"all_requirements_done"},
    "update_payment": {"all_requirements_done"},
    "all_requirements_done": {"issue_remaining_yes", "issue_remaining_no"},
    "issue_remaining_yes": {"ai_try_resolve"},
    "ai_try_resolve": {"resolved_yes", "resolved_no"},
    "resolved_yes": {"proceed"},
    "resolved_no": {"owner_intervention"},
    "owner_intervention": {"proceed"},
    "issue_remaining_no": {"proceed"},
    "proceed": {"ask_rating_1_10"},
    "ask_rating_1_10": {"score_8_10", "score_5_7", "score_1_4"},
    "score_8_10": {"save_testimonial", "ask_referral"},
    "score_5_7": {"ask_improvement", "soft_referral"},
    "score_1_4": {"ai_resolve_first"},
    "ai_resolve_first": {"negative_resolved_yes", "negative_resolved_no"},
    "negative_resolved_yes": {"close"},
    "negative_resolved_no": {"owner_alert"},
    "save_testimonial": {"update_dashboard"},
    "ask_referral": {"update_dashboard"},
    "ask_improvement": {"update_dashboard"},
    "soft_referral": {"update_dashboard"},
    "close": {"update_dashboard"},
    "owner_alert": {"update_dashboard"},
    "update_dashboard": {"morning_plan", "evening_report", "learning_loop"},
    "morning_plan": set(),
    "evening_report": set(),
    "learning_loop": {"load_requirements", "post_sale_system"},
    "post_sale_system": set(),
}


def _postsale_next_allowed(status: str) -> list[str]:
    return sorted(_POSTSALE_ALLOWED_BY_STATUS.get(status, set()))


def _parse_bool_field(body: dict[str, Any], key: str, *, required: bool = False) -> bool | None:
    if key not in body or body.get(key) is None:
        if required:
            raise ValueError(f"{key} is required")
        return None
    value = body.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"true", "1", "yes", "y"}:
            return True
        if raw in {"false", "0", "no", "n"}:
            return False
    raise ValueError(f"{key} must be boolean")


def _postsale_apply_actions(brand_id: str, lead_id: str, actions: list[str], *, event_type: str, payload: dict[str, Any], stage: str) -> Any:
    cur = postsaleops.get_session(brand_id=brand_id, lead_id=lead_id)
    current_status = cur.status if cur else "started"
    initial_status = current_status
    next_action_at = None
    for action in actions:
        allowed = _postsale_next_allowed(current_status)
        if action not in allowed:
            raise ValueError(f"invalid transition from {current_status} -> {action}; allowed next: {', '.join(allowed) if allowed else '(none)'}")
        current_status = action
        if action in {"retry_d0_d3_d5_d7", "doc_followup_d0_d2_d4", "reschedule_next_day"}:
            next_action_at = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()

    s = postsaleops.upsert_session(
        brand_id=brand_id,
        lead_id=lead_id,
        stage=stage,
        status=current_status,
        next_action_at=next_action_at,
    )
    postsaleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type=event_type, payload=payload | {"actions": actions, "final_status": current_status})
    return s, {
        "initial_status": initial_status,
        "actions_applied": actions,
        "final_status": current_status,
        "stage": stage,
    }


def _postsale_ok(session: Any, *, event: Any | None = None, audit: dict[str, Any] | None = None):
    payload: dict[str, Any] = {
        "session": session.__dict__,
        "next_allowed_actions": _postsale_next_allowed(session.status),
    }
    if event is not None:
        payload["event"] = event.__dict__
    if audit is not None:
        payload["audit"] = audit
    return jsonify(payload)


def _postsale_data(data: dict[str, Any], *, audit: dict[str, Any] | None = None):
    payload: dict[str, Any] = {"data": data}
    if audit is not None:
        payload["audit"] = audit
    return jsonify(payload)


@app.get("/post-sale")
def postsale_console():
    html = """<!doctype html><html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Post Sale Engine</title><style>
  :root{--bg:#f1f5f9;--panel:#ffffff;--line:#d9e1ea;--text:#0f172a;--muted:#64748b;--accent:#0f4c81}
  *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,Arial,sans-serif}
  .wrap{max-width:1320px;margin:0 auto;padding:16px}.hero{background:linear-gradient(135deg,#0f4c81,#155e75);color:#fff;padding:14px 16px;border-radius:12px;margin-bottom:12px}
  .grid{display:grid;grid-template-columns:2fr 1fr;gap:12px}@media(max-width:1100px){.grid{grid-template-columns:1fr}}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:12px;margin-bottom:12px}
  .row{display:grid;grid-template-columns:1fr 1fr;gap:10px}@media(max-width:780px){.row{grid-template-columns:1fr}}
  label{display:block;font-size:11px;font-weight:700;color:var(--muted);margin:7px 0 3px;text-transform:uppercase}
  input,select,textarea{width:100%;padding:8px 10px;border:1px solid var(--line);border-radius:8px;background:#fff;font:inherit}
  .btns{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px} button{border:0;border-radius:8px;padding:8px 10px;background:var(--accent);color:#fff;font-weight:700;cursor:pointer}
  .chip{font-size:11px;padding:4px 8px;border-radius:999px;background:#e2e8f0;font-weight:700;margin-right:6px;display:inline-block}
  pre{margin:0;white-space:pre-wrap;word-break:break-word;background:#0f172a;color:#e2e8f0;border-radius:10px;padding:10px;max-height:70vh;overflow:auto;font-size:12px}
</style></head><body><div class="wrap">
  <div class="hero"><strong>Post Sale Engine</strong><div style="font-size:12px;color:#dbeafe">Condition-based flow: backend computes transitions from your inputs.</div></div>
  <div class="grid"><main>
    <section class="card"><h3>Start</h3>
      <div class="row"><div><label>Brand Id</label><input id="brand" value="demo"/></div><div><label>Lead Id</label><input id="lead" placeholder="lead id"/></div></div>
      <div class="row"><div><label>Admin API Key</label><input id="adminkey" placeholder="X-Admin-Api-Key"/></div><div><label>Timezone</label><input id="tz" value="Asia/Kolkata"/></div></div>
      <div class="row"><div><label>Language</label><select id="lang"><option value="hinglish" selected>hinglish</option><option value="hi">hi</option><option value="en">en</option></select></div><div><label>Auto Language Switch</label><select id="autosw"><option value="true" selected>true</option><option value="false">false</option></select></div></div>
      <div class="btns"><button onclick="start()">Start + Load Requirements</button><button onclick="timeline()">Timeline</button></div>
    </section>
    <section class="card"><h3>Payment Flow</h3>
      <div class="row"><div><label>Payment Status</label><select id="pay"><option value="pending">pending</option><option value="partial">partial</option></select></div><div><label>Owner Confirm?</label><select id="owner_confirm"><option value="">n/a</option><option value="yes">yes</option><option value="no">no</option></select></div></div>
      <div class="row"><div><label>Confirm Again?</label><select id="confirm_again"><option value="">n/a</option><option value="yes">yes</option><option value="no">no</option></select></div><div><label>Note</label><input id="pay_note" placeholder="optional"/></div></div>
      <div class="btns"><button onclick="paymentFlow()">Process Payment Flow</button></div>
    </section>
    <section class="card"><h3>Document + Issue Flow</h3>
      <div class="row"><div><label>Document Status</label><select id="doc"><option value="pending">pending</option><option value="received">received</option></select></div><div><label>Requirements Done?</label><select id="requirements_done"><option value="yes">yes</option><option value="no">no</option></select></div></div>
      <div class="row"><div><label>Issue Remaining?</label><select id="issue"><option value="no">no</option><option value="yes">yes</option></select></div><div><label>AI Resolved? (if issue=yes)</label><select id="ai_resolved"><option value="">n/a</option><option value="yes">yes</option><option value="no">no</option></select></div></div>
      <div class="btns"><button onclick="documentFlow()">Process Document Flow</button><button onclick="issueFlow()">Process Issue Flow</button></div>
    </section>
    <section class="card"><h3>Feedback + Final</h3>
      <div class="row"><div><label>Rating 1-10</label><input id="score" type="number" min="1" max="10" value="8"/></div><div><label>Negative Resolved? (for 1-4 branch)</label><select id="resolved"><option value="yes">yes</option><option value="no">no</option></select></div></div>
      <div class="btns"><button onclick="feedbackFlow()">Process Feedback Flow</button><button onclick="finalizeFlow()">Finalize (Dashboard + Plans + Learning)</button><button onclick="dashboard()">Dashboard</button><button onclick="dayPlan()">Day Plan</button><button onclick="learning()">Learning</button></div>
    </section>
  </main><aside>
    <section class="card"><h3>Current state</h3><div><span class="chip" id="st">unknown</span></div><div id="na"></div></section>
    <section class="card"><h3>API response</h3><pre id="out">Ready.</pre></section>
  </aside></div>
<script>
const out=document.getElementById('out');const st=document.getElementById('st');const na=document.getElementById('na');
function headers(){const h={'Content-Type':'application/json'};const k=(document.getElementById('adminkey').value||'').trim();if(k)h['X-Admin-Api-Key']=k;return h;}
function brand(){return (document.getElementById('brand').value||'').trim();} function lead(){return (document.getElementById('lead').value||'').trim();}
function consume(j){if(j&&j.session&&j.session.status){st.textContent=j.session.status;} if(j&&Array.isArray(j.next_allowed_actions)){na.innerHTML=j.next_allowed_actions.map(x=>'<span class="chip">'+x+'</span>').join(' ');}}
async function api(p,m='GET',b=null){const r=await fetch(p,{method:m,headers:headers(),body:b?JSON.stringify(b):null});const j=await r.json().catch(()=>({raw:'non-json'}));if(!r.ok)j._http={status:r.status};out.textContent=JSON.stringify(j,null,2);consume(j);return j;}
async function start(){await api('/v1/postsale/start','POST',{brand_id:brand(),lead_id:lead(),timezone:document.getElementById('tz').value,language:document.getElementById('lang').value,auto_language_switch:document.getElementById('autosw').value==='true'});await api('/v1/postsale/action','POST',{brand_id:brand(),lead_id:lead(),action:'load_requirements'});}
async function paymentFlow(){await api('/v1/postsale/flow/payment','POST',{brand_id:brand(),lead_id:lead(),payment_status:document.getElementById('pay').value,owner_confirm:document.getElementById('owner_confirm').value||null,confirm_again:document.getElementById('confirm_again').value||null,note:document.getElementById('pay_note').value||''});}
async function documentFlow(){await api('/v1/postsale/flow/document','POST',{brand_id:brand(),lead_id:lead(),document_status:document.getElementById('doc').value});}
async function issueFlow(){const ai=document.getElementById('ai_resolved').value;await api('/v1/postsale/flow/issue','POST',{brand_id:brand(),lead_id:lead(),requirements_done:document.getElementById('requirements_done').value==='yes',issue_remaining:document.getElementById('issue').value==='yes',ai_resolved:ai===''?null:(ai==='yes')});}
async function feedbackFlow(){await api('/v1/postsale/flow/feedback','POST',{brand_id:brand(),lead_id:lead(),score:Number(document.getElementById('score').value||0),negative_resolved:document.getElementById('resolved').value==='yes'});}
async function finalizeFlow(){await api('/v1/postsale/flow/finalize','POST',{brand_id:brand(),lead_id:lead()});}
async function timeline(){await api('/v1/postsale/timeline?brand_id='+encodeURIComponent(brand())+'&lead_id='+encodeURIComponent(lead()));}
async function dashboard(){await api('/v1/postsale/dashboard?brand_id='+encodeURIComponent(brand()));}
async function dayPlan(){await api('/v1/postsale/day_plan','POST',{brand_id:brand()});}
async function learning(){await api('/v1/postsale/learning','POST',{brand_id:brand()});}
</script></body></html>"""
    return Response(html, mimetype="text/html; charset=utf-8")


@app.post("/v1/postsale/start")
def postsale_start():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    tz = str(body.get("timezone") or "Asia/Kolkata").strip()
    lang = str(body.get("language") or "hinglish").strip().lower()
    try:
        auto_parsed = _parse_bool_field(body, "auto_language_switch", required=False)
    except ValueError as e:
        return _err(400, str(e))
    auto = True if auto_parsed is None else auto_parsed
    try:
        s = postsaleops.upsert_session(
            brand_id=brand_id,
            lead_id=lead_id,
            timezone=tz,
            language=lang,
            auto_language_switch=auto,
            stage="init",
            status="started",
            payment_status="pending",
            document_status="pending",
        )
        postsaleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="start", payload={"timezone": tz, "language": lang})
    except ValueError as e:
        return _err(400, str(e))
    leads.merge_raw(lead_id, {"postsale_timezone": tz, "postsale_language": lang, "postsale_auto_language_switch": auto})
    return _postsale_ok(s, audit={"initial_status": "started", "actions_applied": ["start"], "final_status": s.status, "stage": "init"})


@app.post("/v1/postsale/payment_status")
def postsale_payment_status():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    ps = str(body.get("payment_status") or "").strip().lower()
    if ps not in {"pending", "partial"}:
        return _err(400, "payment_status must be pending/partial")
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    action = "set_payment_pending" if ps == "pending" else "set_payment_partial"
    try:
        s = postsaleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage="payment", status=action, payment_status=ps)
        postsaleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="payment_status", payload={"payment_status": ps})
    except ValueError as e:
        return _err(400, str(e))
    return _postsale_ok(s, audit={"initial_status": "n/a", "actions_applied": [action], "final_status": s.status, "stage": "payment"})


@app.post("/v1/postsale/document_status")
def postsale_document_status():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    ds = str(body.get("document_status") or "").strip().lower()
    if ds not in {"pending", "received"}:
        return _err(400, "document_status must be pending/received")
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    action = "request_documents" if ds == "pending" else "receive_document"
    try:
        s = postsaleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage="document", status=action, document_status=ds)
        postsaleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="document_status", payload={"document_status": ds})
    except ValueError as e:
        return _err(400, str(e))
    return _postsale_ok(s, audit={"initial_status": "n/a", "actions_applied": [action], "final_status": s.status, "stage": "document"})


@app.post("/v1/postsale/issue_status")
def postsale_issue_status():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    try:
        issue_remaining_parsed = _parse_bool_field(body, "issue_remaining", required=False)
    except ValueError as e:
        return _err(400, str(e))
    issue_remaining = False if issue_remaining_parsed is None else issue_remaining_parsed
    action = "issue_remaining_yes" if issue_remaining else "issue_remaining_no"
    try:
        s = postsaleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage="issue", status=action)
        postsaleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="issue_status", payload={"issue_remaining": issue_remaining})
    except ValueError as e:
        return _err(400, str(e))
    return _postsale_ok(s, audit={"initial_status": "n/a", "actions_applied": [action], "final_status": s.status, "stage": "issue"})


@app.post("/v1/postsale/score")
def postsale_score():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    try:
        score = int(body.get("score") or 0)
    except (TypeError, ValueError):
        return _err(400, "score must be integer")
    if score < 1 or score > 10:
        return _err(400, "score must be 1..10")
    try:
        resolved_parsed = _parse_bool_field(body, "resolved", required=False)
    except ValueError as e:
        return _err(400, str(e))
    resolved = True if resolved_parsed is None else resolved_parsed
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    if score >= 8:
        status = "score_8_10"
    elif score >= 5:
        status = "score_5_7"
    else:
        status = "score_1_4"
    try:
        s = postsaleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage="score", status=status, rating=score)
        postsaleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="score", payload={"score": score, "resolved": resolved, "status": status})
    except ValueError as e:
        return _err(400, str(e))
    return _postsale_ok(s, audit={"initial_status": "n/a", "actions_applied": [status], "final_status": s.status, "stage": "score"})


@app.post("/v1/postsale/flow/payment")
def postsale_flow_payment():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    ps = str(body.get("payment_status") or "").strip().lower()
    owner_confirm = str(body.get("owner_confirm") or "").strip().lower()
    confirm_again = str(body.get("confirm_again") or "").strip().lower()
    note = str(body.get("note") or "").strip()
    if ps not in {"pending", "partial"}:
        return _err(400, "payment_status must be pending/partial")
    if owner_confirm not in {"", "yes", "no"}:
        return _err(400, "owner_confirm must be yes/no when provided")
    if confirm_again not in {"", "yes", "no"}:
        return _err(400, "confirm_again must be yes/no when provided")
    if ps == "pending" and (owner_confirm or confirm_again):
        return _err(400, "owner_confirm/confirm_again are not valid when payment_status=pending")
    if owner_confirm == "no" and confirm_again:
        return _err(400, "confirm_again is only valid when owner_confirm=yes")

    try:
        if ps == "pending":
            s, audit = _postsale_apply_actions(
                brand_id=brand_id,
                lead_id=lead_id,
                actions=["set_payment_pending", "send_payment_reminder", "retry_d0_d3_d5_d7"],
                event_type="payment_flow",
                payload={"payment_status": ps, "path": "reminder_retry_loop", "note": note},
                stage="payment",
            )
            s = postsaleops.upsert_session(brand_id=brand_id, lead_id=lead_id, payment_status="pending")
            return _postsale_ok(s, audit=audit)

        actions = ["set_payment_partial", "ask_payment_proof", "receive_proof", "send_to_owner_verification", "owner_whatsapp_verify_payment"]
        if owner_confirm in {"yes", "no"}:
            actions.append("owner_confirm_yes" if owner_confirm == "yes" else "owner_confirm_no")
            if owner_confirm == "yes":
                actions.append("send_second_confirmation")
                if confirm_again in {"yes", "no"}:
                    actions.append("confirm_again_yes" if confirm_again == "yes" else "confirm_again_no")
                    actions.append("update_payment" if confirm_again == "yes" else "cancel_update")
            else:
                actions.append("cancel_update")
        s, audit = _postsale_apply_actions(
            brand_id=brand_id,
            lead_id=lead_id,
            actions=actions,
            event_type="payment_flow",
            payload={"payment_status": ps, "owner_confirm": owner_confirm or None, "confirm_again": confirm_again or None, "note": note},
            stage="payment",
        )
        s = postsaleops.upsert_session(brand_id=brand_id, lead_id=lead_id, payment_status="partial")
    except ValueError as e:
        return _err(400, str(e))
    return _postsale_ok(s, audit=audit)


@app.post("/v1/postsale/flow/document")
def postsale_flow_document():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    ds = str(body.get("document_status") or "").strip().lower()
    if ds not in {"pending", "received"}:
        return _err(400, "document_status must be pending/received")
    try:
        actions = ["request_documents", "doc_followup_d0_d2_d4"] if ds == "pending" else ["receive_document", "validate_document"]
        s, audit = _postsale_apply_actions(
            brand_id=brand_id,
            lead_id=lead_id,
            actions=actions,
            event_type="document_flow",
            payload={"document_status": ds},
            stage="document",
        )
        s = postsaleops.upsert_session(brand_id=brand_id, lead_id=lead_id, document_status=ds)
    except ValueError as e:
        return _err(400, str(e))
    return _postsale_ok(s, audit=audit)


@app.post("/v1/postsale/flow/issue")
def postsale_flow_issue():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    try:
        requirements_done = bool(_parse_bool_field(body, "requirements_done", required=True))
        issue_remaining = bool(_parse_bool_field(body, "issue_remaining", required=True))
        ai_resolved = _parse_bool_field(body, "ai_resolved", required=False)
    except ValueError as e:
        return _err(400, str(e))

    if not requirements_done:
        return _err(400, "requirements_done must be true before issue flow")

    if not issue_remaining:
        actions = ["all_requirements_done", "issue_remaining_no", "proceed"]
    elif ai_resolved is None:
        actions = ["all_requirements_done", "issue_remaining_yes", "ai_try_resolve"]
    else:
        actions = ["all_requirements_done", "issue_remaining_yes", "ai_try_resolve", "resolved_yes" if ai_resolved else "resolved_no"]
        actions.append("proceed" if ai_resolved else "owner_intervention")

    try:
        s, audit = _postsale_apply_actions(
            brand_id=brand_id,
            lead_id=lead_id,
            actions=actions,
            event_type="issue_flow",
            payload={"requirements_done": requirements_done, "issue_remaining": issue_remaining, "ai_resolved": ai_resolved},
            stage="issue",
        )
    except ValueError as e:
        return _err(400, str(e))
    return _postsale_ok(s, audit=audit)


@app.post("/v1/postsale/flow/feedback")
def postsale_flow_feedback():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    try:
        score = int(body.get("score") or 0)
    except (TypeError, ValueError):
        return _err(400, "score must be integer")
    if score < 1 or score > 10:
        return _err(400, "score must be 1..10")
    try:
        negative_resolved_parsed = _parse_bool_field(body, "negative_resolved", required=False)
    except ValueError as e:
        return _err(400, str(e))
    negative_resolved = True if negative_resolved_parsed is None else negative_resolved_parsed

    if score >= 8:
        actions = ["ask_rating_1_10", "score_8_10", "ask_referral"]
    elif score >= 5:
        actions = ["ask_rating_1_10", "score_5_7", "soft_referral"]
    else:
        actions = ["ask_rating_1_10", "score_1_4", "ai_resolve_first"]
        actions.append("negative_resolved_yes" if negative_resolved else "negative_resolved_no")
        actions.append("close" if negative_resolved else "owner_alert")

    try:
        s, audit = _postsale_apply_actions(
            brand_id=brand_id,
            lead_id=lead_id,
            actions=actions,
            event_type="feedback_flow",
            payload={"score": score, "negative_resolved": negative_resolved},
            stage="feedback",
        )
        s = postsaleops.upsert_session(brand_id=brand_id, lead_id=lead_id, rating=score)
    except ValueError as e:
        return _err(400, str(e))
    return _postsale_ok(s, audit=audit)


@app.post("/v1/postsale/flow/finalize")
def postsale_flow_finalize():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    try:
        s, audit = _postsale_apply_actions(
            brand_id=brand_id,
            lead_id=lead_id,
            actions=["update_dashboard", "learning_loop"],
            event_type="finalize_flow",
            payload={},
            stage="reporting",
        )
    except ValueError as e:
        return _err(400, str(e))
    return _postsale_ok(s, audit=audit)


@app.post("/v1/postsale/action")
def postsale_action():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    lead_id = str(body.get("lead_id") or "").strip()
    action = str(body.get("action") or "").strip().lower()
    all_actions = {a for v in _POSTSALE_ALLOWED_BY_STATUS.values() for a in v}
    if action not in all_actions:
        return _err(400, "invalid action")
    cur = postsaleops.get_session(brand_id=brand_id, lead_id=lead_id)
    current_status = cur.status if cur else "started"
    allowed = _postsale_next_allowed(current_status)
    if action not in allowed:
        return _err(409, f"invalid transition from {current_status} -> {action}; allowed next: {', '.join(allowed) if allowed else '(none)'}")
    stage = "flow"
    next_action_at = None
    if action in {"retry_d0_d3_d5_d7", "doc_followup_d0_d2_d4", "reschedule_next_day"}:
        next_action_at = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    if action in {"set_payment_pending", "send_payment_reminder", "ask_payment_proof", "receive_proof", "send_to_owner_verification", "owner_whatsapp_verify_payment"}:
        stage = "payment"
    elif action in {"request_documents", "doc_followup_d0_d2_d4", "receive_document", "validate_document"}:
        stage = "document"
    elif action in {"issue_remaining_yes", "issue_remaining_no", "ai_try_resolve", "owner_intervention"}:
        stage = "issue"
    elif action.startswith("score_") or action in {"ask_rating_1_10", "save_testimonial", "ask_referral", "ask_improvement", "soft_referral", "ai_resolve_first", "negative_resolved_yes", "negative_resolved_no"}:
        stage = "feedback"
    elif action in {"update_dashboard", "morning_plan", "evening_report", "learning_loop", "post_sale_system"}:
        stage = "reporting"
    try:
        s = postsaleops.upsert_session(brand_id=brand_id, lead_id=lead_id, stage=stage, status=action, next_action_at=next_action_at)
        e = postsaleops.add_event(brand_id=brand_id, lead_id=lead_id, event_type="diagram_action", payload={"action": action})
    except ValueError as e:
        return _err(400, str(e))
    return _postsale_ok(
        s,
        event=e,
        audit={"initial_status": current_status, "actions_applied": [action], "final_status": s.status, "stage": stage},
    )


@app.get("/v1/postsale/timeline")
def postsale_timeline():
    brand_id = str(request.args.get("brand_id") or "").strip()
    lead_id = str(request.args.get("lead_id") or "").strip()
    if not brand_id or not lead_id:
        return _err(400, "brand_id and lead_id required")
    try:
        s = postsaleops.get_session(brand_id=brand_id, lead_id=lead_id)
        ev = [e.__dict__ for e in postsaleops.list_events(brand_id=brand_id, lead_id=lead_id)]
    except ValueError as e:
        return _err(400, str(e))
    return _postsale_data(
        {"session": s.__dict__ if s else None, "events": ev},
        audit={"endpoint": "timeline", "events_count": len(ev)},
    )


@app.get("/v1/postsale/dashboard")
def postsale_dashboard():
    brand_id = str(request.args.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    ss = [s for s in postsaleops.sessions.values() if s.brand_id == brand_id]
    ev = [e for e in postsaleops.events.values() if e.brand_id == brand_id]
    by_status: dict[str, int] = {}
    for s in ss:
        by_status[s.status] = by_status.get(s.status, 0) + 1
    avg_rating = round(sum((s.rating or 0) for s in ss) / len([s for s in ss if s.rating is not None]), 2) if any(s.rating is not None for s in ss) else None
    return _postsale_data(
        {
            "brand_id": brand_id,
            "sessions_total": len(ss),
            "events_total": len(ev),
            "status_breakdown": by_status,
            "average_rating": avg_rating,
        },
        audit={"endpoint": "dashboard", "sessions_count": len(ss), "events_count": len(ev)},
    )


@app.post("/v1/postsale/day_plan")
def postsale_day_plan():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    now = datetime.now(timezone.utc)
    morning = [s.lead_id for s in postsaleops.sessions.values() if s.brand_id == brand_id and s.status in {"set_payment_pending", "send_payment_reminder", "request_documents"}][:20]
    evening = [s.lead_id for s in postsaleops.sessions.values() if s.brand_id == brand_id and s.status in {"owner_alert", "negative_resolved_no", "doc_followup_d0_d2_d4"}][:20]
    return _postsale_data(
        {
            "brand_id": brand_id,
            "morning_plan": {"generated_at": now.isoformat(), "lead_ids": morning},
            "evening_report": {"generated_at": now.isoformat(), "lead_ids": evening},
        },
        audit={"endpoint": "day_plan", "morning_count": len(morning), "evening_count": len(evening)},
    )


@app.post("/v1/postsale/learning")
def postsale_learning():
    body: dict[str, Any] = request.get_json(force=True, silent=True) or {}
    brand_id = str(body.get("brand_id") or "").strip()
    if not brand_id:
        return _err(400, "brand_id required")
    ss = [s for s in postsaleops.sessions.values() if s.brand_id == brand_id]
    pending_payments = len([s for s in ss if s.status in {"set_payment_pending", "send_payment_reminder"}])
    owner_alerts = len([s for s in ss if s.status in {"owner_alert", "negative_resolved_no"}])
    if owner_alerts:
        act = "Increase owner intervention speed for unresolved negative post-sale issues."
    elif pending_payments:
        act = "Prioritize payment reminders and proof verification in first half of day."
    else:
        act = "Scale testimonial + referral requests for satisfied customers."
    return _postsale_data(
        {
            "brand_id": brand_id,
            "learning_loop": {"recommended_action": act, "generated_at": datetime.now(timezone.utc).isoformat()},
        },
        audit={"endpoint": "learning", "pending_payments": pending_payments, "owner_alerts": owner_alerts},
    )

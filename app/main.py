from __future__ import annotations

import base64
import html
import logging
import os

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
from .conversation_store import Conversation
from .zoho import (
    ZohoClient,
    build_qualification_update_payload,
    map_lead_to_zoho,
    zoho_first_row_outcome,
)
from . import public_chat as public_chat_api


app = Flask(__name__)
store, posts, leads = build_stores(settings)
conversations = build_conversation_store(settings)
gen = Generator()
zoho = ZohoClient()

log = logging.getLogger(__name__)


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
            "public_chat_start": "/v1/public/chat/start",
            "public_chat_message": "/v1/public/chat/message",
            "creative_brief": "/v1/marketing/creative-brief",
            "fetch_website_hints": "/v1/marketing/fetch-website-hints",
            "creative_assets": "/v1/marketing/creative-assets",
            "marketing_campaign": "/v1/marketing/campaign",
            "marketing_campaign_execute": "/v1/marketing/campaign/execute",
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
    <title>SalesPal — Milestone 1 · Demo console</title>
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
    brand_name = str(body.get("brand_name") or "").strip() or None
    brand_tagline = str(body.get("brand_tagline") or "").strip() or None
    logo_text = str(body.get("logo_text") or "").strip() or None
    objective = str(body.get("objective") or "").strip() or None
    copy_locked_lines = _parse_campaign_on_screen_lines(body.get("on_screen_lines"))
    primary_market = _normalize_primary_market(body.get("primary_market"))
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

    if not brand_id or len(brand_id) > 64:
        return _err(400, "invalid brand_id")

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
        v_prompt = str(video_plan.get("video_prompt") or "").strip()
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

from __future__ import annotations

import logging
import os

from .logging_config import configure_logging
from .gcp_bootstrap import maybe_load_secrets

configure_logging()
maybe_load_secrets()

from flask import Flask, Response, jsonify, request
from datetime import datetime, timedelta, timezone
from typing import Any

from .creative_assets import derive_imagen_prompts_from_brief, derive_video_plan_from_brief
from .creative_brief import generate_creative_brief
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


@app.get("/")
def index():
    return jsonify(
        {
            "service": "salespal-api",
            "status": "ok",
            "routes": {
                "healthz": "/healthz",
                "readyz": "/readyz",
                "whatsapp_webhook": "/v1/webhooks/whatsapp",
                "public_chat_start": "/v1/public/chat/start",
                "public_chat_message": "/v1/public/chat/message",
                "creative_brief": "/v1/marketing/creative-brief",
                "creative_assets": "/v1/marketing/creative-assets",
            },
        }
    )


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
    </style>
  </head>
  <body>
    <header class="topbar">
      <div class="topbar-inner">
        <div class="brand">
          <div class="brand-mark">SP</div>
          <div>
            <h1>SalesPal</h1>
            <p>Milestone 1 · Marketing &amp; lead pipeline demo</p>
          </div>
        </div>
        <span class="badge">Internal UAT console</span>
      </div>
    </header>

    <div class="shell">
      <div class="hero">
        <h2>Live API demo</h2>
        <p>
          Use this console to validate Cloud Run deployment, Firestore lead capture, Zoho CRM push, and Vertex-backed asset generation —
          in one place for stakeholder review.
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

          <div class="card">
            <div class="card-h">
              <div>
                <h3>Step 1 · Creative brief</h3>
                <span>Website · PDF · text → plan JSON</span>
              </div>
            </div>
            <div class="row2">
              <div>
                <label for="brief_source">Source type</label>
                <select id="brief_source">
                  <option value="text" selected>Brief text</option>
                  <option value="url">Website URL</option>
                  <option value="pdf">PDF file</option>
                </select>
              </div>
              <div>
                <label for="brief_brand_hint">Brand hint (optional)</label>
                <input id="brief_brand_hint" placeholder="e.g. India enterprise B2B" autocomplete="off" />
              </div>
            </div>
            <label for="brief_text" id="brief_text_label">Paste brief text or website URL</label>
            <textarea id="brief_text" placeholder="Paste product copy, positioning notes, or a full website URL when you selected Website."></textarea>
            <label for="brief_pdf" id="brief_pdf_label" hidden>PDF file</label>
            <input type="file" id="brief_pdf" accept="application/pdf,.pdf" hidden />
            <div class="btn-row">
              <button type="button" class="btn-primary" id="btn-brief" onclick="createCreativeBrief()">Analyze &amp; build brief</button>
            </div>
            <div class="row2">
              <div>
                <label for="brief_carousel_n">Step 2 · Carousel panels</label>
                <input id="brief_carousel_n" value="3" inputmode="numeric" />
              </div>
              <div style="display:flex;align-items:flex-end">
                <button type="button" class="btn-secondary" id="btn-brief2" onclick="generateCreativeAssetsFromBrief()">Generate image &amp; carousel from last brief</button>
              </div>
            </div>
            <div class="row2">
              <div>
                <label for="brief_video_total">Step 3 · Video total (seconds)</label>
                <input id="brief_video_total" value="24" inputmode="numeric" />
              </div>
              <div>
                <label for="brief_video_clip">Clip length (Veo fast)</label>
                <select id="brief_video_clip">
                  <option value="4">4</option>
                  <option value="6">6</option>
                  <option value="8" selected>8</option>
                </select>
              </div>
              <div style="display:flex;align-items:flex-end">
                <button type="button" class="btn-secondary" id="btn-brief3" onclick="generateCreativeVideoFromBrief()">Generate video from last brief</button>
              </div>
            </div>
            <p class="hint">Steps 2–3 use <code>/v1/marketing/creative-assets</code> (planners + Imagen / stitched Veo). Step 3 needs <code>META_MEDIA_BUCKET</code> and a long HTTP timeout. Run Step 1 first.</p>
          </div>

          <div class="card">
            <div class="card-h">
              <div>
                <h3>Vertex marketing asset</h3>
                <span>Image · carousel · video</span>
              </div>
            </div>
            <div class="row2">
              <div>
                <label for="asset_type">Asset type</label>
                <select id="asset_type">
                  <option value="image" selected>Image</option>
                  <option value="carousel">Carousel</option>
                  <option value="video">Video</option>
                </select>
              </div>
              <div>
                <label for="asset_n">Count (n)</label>
                <input id="asset_n" value="1" inputmode="numeric" />
              </div>
            </div>
            <div class="row2">
              <div>
                <label for="video_total_seconds">Video total seconds</label>
                <input id="video_total_seconds" value="8" inputmode="numeric" />
              </div>
              <div>
                <label for="video_clip_seconds">Video clip seconds</label>
                <select id="video_clip_seconds">
                  <option value="4">4</option>
                  <option value="6">6</option>
                  <option value="8" selected>8</option>
                </select>
              </div>
            </div>
            <label for="video_continuity_text">Video continuity (optional)</label>
            <textarea id="video_continuity_text" placeholder="Example: Same character (orange tabby cat), same setting (modern living room), same camera style (handheld, shallow depth of field), same lighting (warm), consistent outfit/props. Keep brand colors: #0A66C2 and #111827."></textarea>
            <label for="video_storyboard">Storyboard (optional, one line per 8s scene)</label>
            <textarea id="video_storyboard" placeholder="Scene 1: Establish cat in living room.\nScene 2: Cat interacts with phone showing SalesPal.\nScene 3: CTA screen with logo and tagline."></textarea>
            <label for="asset_prompt">Prompt</label>
            <textarea id="asset_prompt">Create a premium B2B marketing visual for SalesPal 360 (India, enterprise tone).</textarea>
            <div class="btn-row">
              <button type="button" class="btn-primary" id="btn-asset" onclick="createAsset()">Generate asset</button>
            </div>
            <p class="hint">Set <code>GENERATOR_BACKEND=vertex</code> for live Vertex output. For carousel/video with Firestore, set <code>META_MEDIA_BUCKET</code> (bucket name only). <strong>Video</strong> via Veo often takes <strong>several minutes</strong> — keep the page open; Cloud Run and Gunicorn must use a long request timeout (3600s in deploy scripts).</p>
          </div>
        </div>

        <aside class="panel-out">
          <div class="card">
            <div class="card-h">
              <div>
                <h3>API response</h3>
                <span>Latest result</span>
              </div>
            </div>
            <p class="hint" style="margin:0">JSON from the backend. Share this panel during screen recordings.</p>
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
              <div class="asset-preview-h">Generated preview</div>
              <div id="asset-preview-body" class="asset-preview-body"></div>
            </div>
            <pre id="out">Select an action to view the response.</pre>
          </div>
        </aside>
      </div>

      <p class="footer">
        SalesPal demo console — not for production end users. Configure secrets only via GCP / Secret Manager.
      </p>
    </div>

    <script>
      const outEl = document.getElementById('out');
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
      function renderAssetPreview(job) {
        if (!assetPreviewWrap || !assetPreviewBody) return;
        var out = job && job.output;
        if (!out || typeof out !== 'object') {
          clearAssetPreview();
          return;
        }
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

        if (!parts.length) {
          clearAssetPreview();
          return;
        }
        assetPreviewBody.innerHTML = parts.join('');
        assetPreviewWrap.hidden = false;
      }
      var lastCreativeBrief = null;
      function show(obj) {
        outEl.textContent = (typeof obj === 'string') ? obj : JSON.stringify(obj, null, 2);
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
      function startAssetGenerationLoading(title, detail) {
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
        clearAssetPreview();
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
      function toggleBriefSource() {
        var st = document.getElementById('brief_source').value;
        var ta = document.getElementById('brief_text');
        var lab = document.getElementById('brief_text_label');
        var pdfL = document.getElementById('brief_pdf_label');
        var pdfI = document.getElementById('brief_pdf');
        if (st === 'pdf') {
          ta.hidden = true;
          lab.hidden = true;
          pdfL.hidden = false;
          pdfI.hidden = false;
        } else {
          ta.hidden = false;
          lab.hidden = false;
          pdfL.hidden = true;
          pdfI.hidden = true;
        }
        lab.textContent = st === 'url' ? 'Website URL (https://…)' : 'Paste brief text or notes';
      }
      async function createCreativeBrief() {
        setBusy('btn-brief', true);
        try {
          var st = document.getElementById('brief_source').value;
          var brand = document.getElementById('lead_brand').value.trim() || 'demo';
          var hint = (document.getElementById('brief_brand_hint').value || '').trim();
          var body = { brand_id: brand, source_type: st };
          if (hint) body.brand_hint = hint;
          if (st === 'pdf') {
            var f = document.getElementById('brief_pdf').files[0];
            if (!f) { show('Choose a PDF file.'); hintEl.textContent = ''; return; }
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
            body.url = document.getElementById('brief_text').value.trim();
          } else {
            body.text = document.getElementById('brief_text').value.trim();
          }
          var data = await callJson('/v1/marketing/creative-brief', body);
          if (data && data.brief && !data._http) lastCreativeBrief = data.brief;
          show(data);
        } finally {
          setBusy('btn-brief', false);
        }
      }
      async function generateCreativeAssetsFromBrief() {
        if (!lastCreativeBrief) {
          show('Run Step 1 successfully first (no brief in this tab yet).');
          hintEl.textContent = '';
          return;
        }
        setBusy('btn-brief2', true);
        startAssetGenerationLoading(
          'Creating image & carousel',
          'Gemini is drafting prompts from your brief, then Imagen will generate the still and all carousel panels. This may take a few minutes.'
        );
        try {
          var cn = parseInt(document.getElementById('brief_carousel_n').value || '3', 10);
          if (!isFinite(cn) || cn < 1) cn = 3;
          var body = {
            brand_id: document.getElementById('lead_brand').value.trim() || 'demo',
            brief: lastCreativeBrief,
            asset_types: ['image', 'carousel'],
            carousel_n: cn,
            require_approval: false,
          };
          var data = await callJson('/v1/marketing/creative-assets', body);
          show(data);
        } finally {
          stopAssetGenerationLoading();
          setBusy('btn-brief2', false);
        }
      }
      async function generateCreativeVideoFromBrief() {
        if (!lastCreativeBrief) {
          show('Run Step 1 successfully first (no brief in this tab yet).');
          hintEl.textContent = '';
          return;
        }
        setBusy('btn-brief3', true);
        startAssetGenerationLoading(
          'Creating video from brief',
          'Gemini is planning the storyboard; then Vertex Veo renders each clip and stitches the MP4. Long runs often take several minutes—keep this tab open and wait for HTTP completion.'
        );
        try {
          var total = parseInt(document.getElementById('brief_video_total').value || '24', 10);
          var clip = parseInt(document.getElementById('brief_video_clip').value || '8', 10);
          var body = {
            brand_id: document.getElementById('lead_brand').value.trim() || 'demo',
            brief: lastCreativeBrief,
            asset_types: ['video'],
            video_total_seconds: isFinite(total) && total >= 4 ? total : 24,
            video_clip_seconds: isFinite(clip) && clip > 0 ? clip : 8,
            require_approval: false,
          };
          var data = await callJson('/v1/marketing/creative-assets', body);
          show(data);
        } finally {
          stopAssetGenerationLoading();
          setBusy('btn-brief3', false);
        }
      }
      document.getElementById('brief_source').addEventListener('change', toggleBriefSource);
      toggleBriefSource();
      async function createAsset() {
        setBusy('btn-asset', true);
        var at = document.getElementById('asset_type').value;
        var nAsk = parseInt(document.getElementById('asset_n').value || '1', 10);
        var loadTitle = 'Generating image';
        var loadDetail = 'Vertex Imagen is rendering your creative. Usually under a minute.';
        if (at === 'carousel') {
          loadTitle = 'Generating carousel';
          var np = isFinite(nAsk) && nAsk > 0 ? nAsk : 1;
          loadDetail = 'Imagen is creating ' + np + ' consistent panel(s). Larger batches take longer.';
        } else if (at === 'video') {
          loadTitle = 'Generating video';
          loadDetail = 'Vertex Veo is producing clip(s) and stitching when needed. Expect several minutes for long or multi-segment videos—do not close this tab.';
        }
        startAssetGenerationLoading(loadTitle, loadDetail);
        try {
        const body = {
          brand_id: document.getElementById('lead_brand').value.trim(),
          asset_type: document.getElementById('asset_type').value,
          prompt: document.getElementById('asset_prompt').value,
          n: parseInt(document.getElementById('asset_n').value || '1', 10),
          require_approval: false,
          options: {},
        };
        if (body.asset_type === 'video') {
          const total = parseInt(document.getElementById('video_total_seconds').value || '8', 10);
          const clip = parseInt(document.getElementById('video_clip_seconds').value || '8', 10);
          const cont = (document.getElementById('video_continuity_text').value || '').trim();
          const sbRaw = (document.getElementById('video_storyboard').value || '').trim();
          // If total > 8, backend will generate multiple short clips and stitch into one MP4.
          body.options.video_total_seconds = isFinite(total) ? total : 8;
          body.options.video_clip_seconds = isFinite(clip) ? clip : 8;
          if (cont) body.options.video_continuity_text = cont;
          if (sbRaw) {
            const lines = sbRaw.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
            if (lines.length) body.options.video_storyboard = lines;
          }
        } else {
          delete body.options;
        }
        const data = await callJson('/v1/marketing/assets', body);
        show(data);
        } finally {
          stopAssetGenerationLoading();
          setBusy('btn-asset', false);
        }
      }
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
        out = generate_creative_brief(
            source_type=source_type,
            text=str(text_v).strip() if isinstance(text_v, str) else None,
            url=str(url_v).strip() if isinstance(url_v, str) else None,
            pdf_base64=str(pdf_b64).strip() if isinstance(pdf_b64, str) else None,
            brand_hint=brand_hint,
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

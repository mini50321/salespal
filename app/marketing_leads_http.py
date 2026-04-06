"""
HTTP helpers for Milestone 1 lead capture: browser forms, CORS, optional redirect after POST.
"""

from __future__ import annotations

import html
import os
import re
from typing import Any

from flask import Request, Response


def _lead_cors_origins() -> set[str]:
    raw = (os.getenv("MARKETING_LEAD_CORS_ORIGINS") or "").strip()
    return {x.strip().rstrip("/") for x in raw.split(",") if x.strip()}


def lead_capture_cors_headers(req: Request) -> dict[str, str]:
    """Headers to merge on POST responses when Origin is allowed."""
    origin = (req.headers.get("Origin") or "").strip().rstrip("/")
    if not origin or origin not in _lead_cors_origins():
        return {}
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Idempotency-Key",
        "Access-Control-Max-Age": "86400",
    }


def lead_capture_options_response(req: Request) -> Response:
    origin = (req.headers.get("Origin") or "").strip().rstrip("/")
    if not origin or origin not in _lead_cors_origins():
        return Response("", 403)
    resp = Response("", 204)
    for k, v in lead_capture_cors_headers(req).items():
        resp.headers[k] = v
    return resp


def parse_lead_request_body(req: Request) -> tuple[dict[str, Any] | None, str | None]:
    """
    Parse JSON or form body into a dict suitable for lead field extraction.
    Returns (body, error_message).
    """
    ct = (req.content_type or "").split(";")[0].strip().lower()
    if ct == "application/json" or req.is_json:
        data = req.get_json(force=True, silent=True)
        if data is None:
            return None, "invalid JSON body"
        if not isinstance(data, dict):
            return None, "JSON body must be an object"
        return data, None
    if ct in ("application/x-www-form-urlencoded", "multipart/form-data"):
        fd = req.form.to_dict(flat=True)
        out: dict[str, Any] = dict(fd)
        utm: dict[str, str] = {}
        for k in list(out.keys()):
            if k.startswith("utm_") and isinstance(out[k], str):
                utm[k[4:]] = out.pop(k)  # type: ignore[arg-type]
        if utm:
            out["utm"] = utm
        return out, None
    return None, "Content-Type must be application/json, application/x-www-form-urlencoded, or multipart/form-data"


def safe_form_redirect_url(url: str) -> str | None:
    """Only same-origin relative paths to avoid open redirects."""
    u = (url or "").strip()
    if not u.startswith("/") or len(u) > 2048:
        return None
    if "\n" in u or "\r" in u or "//" in u:
        return None
    if not re.match(r"^/[A-Za-z0-9_./?#&=%+~!*'(),;:@-]*$", u):
        return None
    return u


def lead_form_embed_page(*, brand_id: str, source: str) -> str:
    """Minimal self-contained form (same-origin POST)."""
    b = html.escape(brand_id, quote=True)
    s = html.escape(source, quote=True)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Contact — SalesPal</title>
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 22rem; margin: 2rem auto; padding: 0 1rem; }}
    label {{ display: block; margin-top: 0.75rem; font-size: 0.85rem; color: #334155; }}
    input, textarea {{ width: 100%; box-sizing: border-box; margin-top: 0.25rem; padding: 0.45rem 0.5rem; }}
    button {{ margin-top: 1rem; padding: 0.55rem 1rem; background: #0369a1; color: #fff; border: none; border-radius: 8px; font-weight: 600; cursor: pointer; }}
    .hint {{ font-size: 0.8rem; color: #64748b; margin-top: 1rem; }}
  </style>
</head>
<body>
  <h1 style="font-size:1.1rem">Get in touch</h1>
  <form method="post" action="/v1/marketing/leads" enctype="application/x-www-form-urlencoded">
    <input type="hidden" name="brand_id" value="{b}" />
    <input type="hidden" name="source" value="{s}" />
    <label>Name <input name="name" type="text" autocomplete="name" /></label>
    <label>Email <input name="email" type="email" autocomplete="email" /></label>
    <label>Phone <input name="phone" type="tel" autocomplete="tel" /></label>
    <label>Company <input name="company" type="text" autocomplete="organization" /></label>
    <label>Message <textarea name="message" rows="3"></textarea></label>
    <label>utm_source <input name="utm_source" type="text" /></label>
    <label>utm_campaign <input name="utm_campaign" type="text" /></label>
    <button type="submit">Submit</button>
  </form>
  <p class="hint">Requires a valid email or phone. Submits to this API (same origin).</p>
</body>
</html>"""

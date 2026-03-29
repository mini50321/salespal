from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from typing import Any

from flask import Request


def sanitize_idempotency_key(raw: str | None) -> str | None:
    if raw is None:
        return None
    s = raw.strip()
    if not s or len(s) > 256:
        return None
    if not re.match(r"^[a-zA-Z0-9._@-]+$", s):
        return None
    return s


def load_field_map() -> dict[str, str]:
    raw = (os.getenv("LEAD_INGEST_FIELD_MAP") or "").strip()
    if not raw:
        return {}
    try:
        m = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(m, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in m.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out


def normalize_ingest_body(body: dict[str, Any], field_map: dict[str, str]) -> dict[str, Any]:
    if not field_map:
        return dict(body)
    out: dict[str, Any] = {}
    for src_key, dst_key in field_map.items():
        if src_key in body:
            out[dst_key] = body[src_key]
    for k, v in body.items():
        if k not in out and k not in field_map:
            out[k] = v
    return out


def verify_ingest_auth(req: Request) -> bool:
    hmac_secret = (os.getenv("LEAD_WEBHOOK_HMAC_SECRET") or "").strip()
    if hmac_secret:
        sig = (req.headers.get("X-Signature") or req.headers.get("X-Hub-Signature-256") or "").strip()
        if not sig.startswith("sha256="):
            return False
        got = sig[7:].strip()
        body = req.get_data()
        want = hmac.new(hmac_secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(got, want)
    plain = (os.getenv("LEAD_WEBHOOK_SECRET") or "").strip()
    if plain:
        return hmac.compare_digest(
            (req.headers.get("X-Lead-Webhook-Secret") or "").strip(),
            plain,
        )
    return True


def idempotency_doc_id(brand_id: str, key: str) -> str:
    h = hashlib.sha256(f"{brand_id}\n{key}".encode("utf-8")).hexdigest()
    return f"idem_{h}"

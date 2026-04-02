from __future__ import annotations

import base64 as b64mod
import json
import logging
import os
import re
from html import unescape
from typing import Any
from urllib.parse import urlparse

import requests

from .settings import settings

log = logging.getLogger(__name__)

_MODEL: Any = None
_INIT_KEY: tuple[str, str, str] | None = None

MAX_URL_BYTES = int(os.getenv("CREATIVE_BRIEF_MAX_URL_BYTES") or str(2 * 1024 * 1024))
MAX_TEXT_CHARS = int(os.getenv("CREATIVE_BRIEF_MAX_TEXT_CHARS") or "48000")


def _get_brief_model():
    global _MODEL, _INIT_KEY
    project = (settings.gcp_project_id or "").strip()
    if not project:
        return None
    region = (os.getenv("VERTEX_BRIEF_REGION") or settings.gcp_region or "us-central1").strip()
    model_name = (os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.5-flash").strip()
    key = (project, region, model_name)
    if _MODEL is not None and _INIT_KEY == key:
        return _MODEL
    import vertexai
    from vertexai.generative_models import GenerativeModel

    vertexai.init(project=project, location=region)
    sys_txt = (
        "You are a senior performance marketing strategist. "
        "You analyze business source material and output structured JSON only—no markdown, no commentary."
    )
    _MODEL = GenerativeModel(model_name, system_instruction=sys_txt)
    _INIT_KEY = key
    return _MODEL


def _strip_html(html: str) -> str:
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _fetch_url_text(url: str) -> str:
    u = (url or "").strip()
    p = urlparse(u)
    if p.scheme not in ("http", "https") or not p.netloc:
        raise ValueError("url must be http(s) with a host")
    headers = {"User-Agent": "SalesPalCreativeBrief/1.0 (+https://salespal.biz)"}
    with requests.get(u, headers=headers, timeout=25, stream=True) as r:
        r.raise_for_status()
        charset = r.encoding or "utf-8"
        raw = bytearray()
        for chunk in r.iter_content(chunk_size=65536):
            if not chunk:
                continue
            raw.extend(chunk)
            if len(raw) > MAX_URL_BYTES:
                break
    try:
        text = raw.decode(charset, errors="replace")
    except LookupError:
        text = raw.decode("utf-8", errors="replace")
    return _strip_html(text)


def _pdf_bytes_to_text(data: bytes) -> str:
    try:
        from pypdf import PdfReader
    except ImportError as e:
        raise RuntimeError("PDF support requires pypdf (install dependency)") from e
    import io

    r = PdfReader(io.BytesIO(data))
    parts: list[str] = []
    for page in r.pages:
        t = page.extract_text()
        if t:
            parts.append(t)
    return re.sub(r"\s+", " ", "\n".join(parts)).strip()


def _truncate(s: str, max_chars: int = MAX_TEXT_CHARS) -> str:
    s = (s or "").strip()
    if len(s) <= max_chars:
        return s
    return s[:max_chars] + "\n\n[…truncated…]"


def _build_prompt(*, source_label: str, body: str, brand_hint: str | None) -> str:
    bh = (brand_hint or "").strip()
    brand_line = f"\nOptional brand context from user: {bh}\n" if bh else ""
    return f"""Analyze the following {source_label} and produce ONE JSON object with these keys:
- company_summary (string, 2-4 sentences)
- target_audience (string)
- demographics (object with keys: regions, age_ranges, segments — each string or list of short strings; use best effort if unknown)
- pain_points (array of strings)
- value_propositions (array of strings)
- marketing_angles (array of objects with keys: angle, rationale)
- tone_and_voice (string)
- key_messages (array of strings, max 5)
- suggested_ctas (array of strings, max 5)
- visual_direction (string: colours, imagery, style guidance for designers / image models)
- creative_formats_recommended (array of strings, e.g. "carousel", "static image", "short video", "long video storyboard")
- confidence (string: "high"|"medium"|"low")
- assumptions (array of strings: what you assumed because data was missing)

Rules:
- Ground answers in the source material; label gaps in assumptions.
- No prompt templates for end users—this JSON is consumed by an automated ad pipeline.{brand_line}

SOURCE BEGIN
{body}
SOURCE END

Output valid JSON only."""

def generate_creative_brief(
    *,
    source_type: str,
    text: str | None = None,
    url: str | None = None,
    pdf_base64: str | None = None,
    brand_hint: str | None = None,
) -> dict[str, Any]:
    st = (source_type or "").strip().lower()
    if st == "text":
        body = (text or "").strip()
        label = "brief text"
    elif st == "url":
        body = _fetch_url_text(url or "")
        label = f"website page text (fetched from {url})"
    elif st in ("pdf", "pdf_base64"):
        raw_b64 = (pdf_base64 or "").strip()
        if not raw_b64:
            raise ValueError("pdf_base64 required for pdf source")
        data = b64mod.b64decode(raw_b64, validate=False)
        max_pdf = int(os.getenv("CREATIVE_BRIEF_MAX_PDF_BYTES") or str(12 * 1024 * 1024))
        if len(data) > max_pdf:
            raise ValueError(f"pdf too large (max {max_pdf} bytes)")
        body = _pdf_bytes_to_text(data)
        label = "PDF document text"
    else:
        raise ValueError("source_type must be text, url, or pdf")

    if not body:
        raise ValueError("no extractable content from source")

    body = _truncate(body)
    model = _get_brief_model()
    if model is None:
        raise RuntimeError("GCP_PROJECT_ID is required for creative brief (Vertex Gemini)")

    prompt = _build_prompt(source_label=label, body=body, brand_hint=brand_hint)

    model_id = (os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.5-flash").strip()

    def _call_json_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_BRIEF_MAX_TOKENS") or "2048").strip() or "2048"),
            temperature=float((os.getenv("VERTEX_BRIEF_TEMPERATURE") or "0.35").strip() or "0.35"),
            response_mime_type="application/json",
        )
        resp = model.generate_content(prompt, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from model")
        parts = resp.candidates[0].content.parts
        raw_out = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        data = json.loads(raw_out)
        if not isinstance(data, dict):
            raise RuntimeError("model returned non-object JSON")
        return data

    def _call_text_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_BRIEF_MAX_TOKENS") or "2048").strip() or "2048"),
            temperature=float((os.getenv("VERTEX_BRIEF_TEMPERATURE") or "0.35").strip() or "0.35"),
        )
        resp = model.generate_content(prompt, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from model")
        parts = resp.candidates[0].content.parts
        raw_out = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        m = re.search(r"\{[\s\S]*\}", raw_out)
        if not m:
            raise RuntimeError("model returned no JSON object")
        data = json.loads(m.group(0))
        if not isinstance(data, dict):
            raise RuntimeError("model returned non-object JSON")
        return data

    try:
        try:
            brief_obj = _call_json_mode()
        except Exception:
            log.warning("creative brief: JSON mode failed, retrying as plain text parse")
            brief_obj = _call_text_mode()
        return {
            "brief": brief_obj,
            "meta": {
                "source_type": st,
                "source_chars": len(body),
                "model": model_id,
            },
        }
    except json.JSONDecodeError:
        log.exception("creative brief JSON parse")
        raise RuntimeError("model returned invalid JSON") from None
    except Exception:
        log.exception("creative brief generation")
        raise

"""
Standalone marketing copy generation (short text: captions, email, headlines).
Complements creative_brief / campaign asset pipelines for Milestone 1.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from .settings import settings

log = logging.getLogger(__name__)

_MODEL: Any = None
_INIT_KEY: tuple[str, str, str] | None = None

CONTENT_TYPES = frozenset({"social_caption", "email", "ad_headlines", "landing_snippet"})


def _get_copy_model():
    global _MODEL, _INIT_KEY
    project = (settings.gcp_project_id or "").strip()
    if not project:
        return None
    region = (os.getenv("VERTEX_COPY_REGION") or settings.gcp_region or "us-central1").strip()
    model_name = (os.getenv("VERTEX_COPY_MODEL") or "gemini-2.5-flash").strip()
    key = (project, region, model_name)
    if _MODEL is not None and _INIT_KEY == key:
        return _MODEL
    import vertexai
    from vertexai.generative_models import GenerativeModel

    vertexai.init(project=project, location=region)
    sys_txt = (
        "You are a senior B2C/B2B performance copywriter. "
        "Output valid JSON only—no markdown, no commentary."
    )
    _MODEL = GenerativeModel(model_name, system_instruction=sys_txt)
    _INIT_KEY = key
    return _MODEL


def _parse_json_obj(text: str) -> dict[str, Any]:
    s = (text or "").strip()
    decoder = json.JSONDecoder()
    for i, c in enumerate(s):
        if c != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(s, i)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    raise ValueError("model did not return a JSON object")


def _mock_copy(content_type: str, context: str, brand_name: str | None) -> dict[str, Any]:
    bn = (brand_name or "Your brand").strip() or "Your brand"
    ctx = (context or "").strip()[:200]
    tail = f" — {ctx}" if ctx else ""
    if content_type == "social_caption":
        return {
            "primary_caption": f"Discover what {bn} can do for you.{tail}",
            "alt_captions": [f"Ready to level up with {bn}?{tail}", f"{bn}: built for real results.{tail}"],
            "hashtags": ["#growth", "#innovation", "#brand"],
        }
    if content_type == "email":
        return {
            "subject": f"{bn}: quick question",
            "preheader": "A short note we thought you'd find useful",
            "body_plain": f"Hi,\n\nWe wanted to reach out about {bn}.{tail}\n\nBest,\nThe team",
        }
    if content_type == "ad_headlines":
        return {
            "headlines": [f"{bn} — see the difference", f"Why teams choose {bn}", f"{bn}, simplified"],
            "short_descriptions": [f"Learn more about {bn} today.", "Book a quick call with our team."],
            "cta_options": ["Learn more", "Get started", "Book a demo"],
        }
    return {
        "headline": f"{bn} moves you forward",
        "subheadline": "Clear value, fast onboarding, human support when you need it.",
        "bullet_points": ["Trusted by growing teams", "Simple pricing", "Fast setup"],
        "cta": "Get started",
    }


def generate_marketing_copy(
    *,
    content_type: str,
    context: str | None = None,
    brief: dict[str, Any] | None = None,
    brand_name: str | None = None,
    tone: str | None = None,
    locale: str | None = None,
) -> dict[str, Any]:
    ct = (content_type or "").strip().lower()
    if ct not in CONTENT_TYPES:
        raise ValueError(f"content_type must be one of: {', '.join(sorted(CONTENT_TYPES))}")

    backend = (os.getenv("MARKETING_COPY_BACKEND") or "auto").strip().lower()
    if backend == "mock":
        return {"content_type": ct, "backend": "mock", "copy": _mock_copy(ct, context or "", brand_name)}

    brief_txt = ""
    if isinstance(brief, dict) and brief:
        try:
            brief_txt = json.dumps(brief, ensure_ascii=False, indent=2)[:12000]
        except Exception:
            brief_txt = str(brief)[:12000]

    ctx = (context or "").strip()[:8000]
    bn = (brand_name or "").strip()
    tn = (tone or "").strip()
    loc = (locale or "en").strip() or "en"

    model = _get_copy_model()
    if model is None or backend == "mock":
        return {"content_type": ct, "backend": "mock", "copy": _mock_copy(ct, ctx, bn or None)}

    schema_hints = {
        "social_caption": """{
  "primary_caption": "string (under 2200 chars, platform-agnostic)",
  "alt_captions": ["string", "string"],
  "hashtags": ["string", ...] (max 8, no spaces inside tags)
}""",
        "email": """{
  "subject": "string (under 120 chars)",
  "preheader": "string (under 140 chars)",
  "body_plain": "string (plain text email body, concise)"
}""",
        "ad_headlines": """{
  "headlines": ["string", ...] (3-5, each under 40 chars),
  "short_descriptions": ["string", ...] (2-3, each under 90 chars),
  "cta_options": ["string", ...] (2-4 short CTA labels)
}""",
        "landing_snippet": """{
  "headline": "string",
  "subheadline": "string",
  "bullet_points": ["string", ...] (3-5),
  "cta": "string"
}""",
    }

    user_prompt = f"""Locale: {loc}
Brand name (if any): {bn or "(not specified)"}
Tone guidance: {tn or "professional, clear, trustworthy"}
Optional structured brief (JSON):\n{brief_txt or "(none)"}

Freeform context from the marketer:\n{ctx or "(none)"}

Produce copy for content_type={ct!r}.
Return ONE JSON object exactly matching this shape:
{schema_hints[ct]}
"""

    from vertexai.generative_models import GenerationConfig

    cfg = GenerationConfig(
        max_output_tokens=2048,
        temperature=0.35,
        response_mime_type="application/json",
    )
    try:
        resp = model.generate_content(user_prompt, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from copy model")
        parts = resp.candidates[0].content.parts
        raw_out = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        copy_obj = _parse_json_obj(raw_out)
    except Exception:
        log.exception("generate_marketing_copy vertex")
        copy_obj = _mock_copy(ct, ctx, bn or None)
        return {"content_type": ct, "backend": "vertex_fallback_mock", "copy": copy_obj}

    return {"content_type": ct, "backend": "vertex", "copy": copy_obj}

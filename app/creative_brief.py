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


def _parse_model_json_object(raw_out: str) -> dict[str, Any]:
    """
    Parse one JSON object from model output: handles markdown fences, extra prose,
    and a few common Gemini glitches (trailing commas). Uses raw_decode to find a
    valid object substring when the model wraps or truncates oddly.
    """
    s = (raw_out or "").strip()
    if not s:
        raise json.JSONDecodeError("empty model output", "", 0)
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.I)
        s = re.sub(r"\s*```\s*$", "", s).strip()

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

    fixed = re.sub(r",\s*([}\]])", r"\1", s)
    for i, c in enumerate(fixed):
        if c != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(fixed, i)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue

    raise json.JSONDecodeError("Could not parse a JSON object from model output", s[:500], 0)


def describe_logo_image(*, image_bytes: bytes, mime_type: str = "image/png") -> dict[str, Any]:
    """
    Analyze a client-supplied logo bitmap via Gemini vision; returned strings feed the text-only Imagen/Veo path.
    """
    if not image_bytes or len(image_bytes) < 32:
        return {}
    model = _get_brief_model()
    if model is None:
        return {}

    from vertexai.generative_models import GenerationConfig, Part

    mt = (mime_type or "image/png").strip().lower()
    if mt not in ("image/png", "image/jpeg", "image/webp"):
        mt = "image/png"
    prompt = """The attached image is a brand logo or logomark supplied by the client.
Return ONE JSON object with keys:
- logo_visual_description (string): 2-4 sentences for ad art direction (palette, shapes, style; icon vs wordmark vs combination). Factual only.
- logo_has_lettering (boolean)
- logo_lettering_text (string): exact visible text in the logo if any; otherwise ""
- dominant_colors (array of strings, max 5): short color names

Output valid JSON only."""
    part = Part.from_data(data=image_bytes, mime_type=mt)
    max_tok = int((os.getenv("LOGO_VISION_MAX_TOKENS") or "1024").strip() or "1024")

    def _json_call() -> dict[str, Any]:
        cfg = GenerationConfig(
            max_output_tokens=max_tok,
            temperature=0.2,
            response_mime_type="application/json",
        )
        resp = model.generate_content([prompt, part], generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from logo vision model")
        parts = resp.candidates[0].content.parts
        raw_out = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        return _parse_model_json_object(raw_out)

    def _text_call() -> dict[str, Any]:
        cfg = GenerationConfig(max_output_tokens=max_tok, temperature=0.2)
        resp = model.generate_content([prompt, part], generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from logo vision model")
        parts = resp.candidates[0].content.parts
        raw_out = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        return _parse_model_json_object(raw_out)

    try:
        try:
            out = _json_call()
        except (RuntimeError, json.JSONDecodeError, ValueError):
            out = _text_call()
        if not isinstance(out, dict):
            return {}
        return out
    except Exception:
        log.exception("describe_logo_image")
        return {}


def _build_prompt(
    *,
    source_label: str,
    body: str,
    brand_hint: str | None,
    ad_constraints: dict[str, Any] | None,
) -> str:
    bh = (brand_hint or "").strip()
    brand_line = f"\nOptional brand context from user: {bh}\n" if bh else ""

    # Stage 2: force an "ad-ready" brief (tone, CTA, logo, overlays, audio/music direction).
    ac = ad_constraints or {}
    brand_name = str(ac.get("brand_name") or "").strip()
    brand_tagline = str(ac.get("brand_tagline") or "").strip()
    logo_text = str(ac.get("logo_text") or "").strip()
    objective = str(ac.get("objective") or "").strip()
    must_include_logo = bool(ac.get("must_include_logo", True))
    must_sound_like_ad = bool(ac.get("must_sound_like_ad", True))
    logo_persistent = bool(ac.get("logo_persistent", False))
    logo_image_supplied = bool(ac.get("logo_image_supplied", False))
    logo_vis = str(ac.get("logo_visual_description") or "").strip()
    logo_letters_ac = str(ac.get("logo_lettering_text") or "").strip()
    dc_raw = ac.get("logo_dominant_colors")
    dc_list: list[str] = []
    if isinstance(dc_raw, list):
        for x in dc_raw[:5]:
            t = str(x).strip()
            if t:
                dc_list.append(t)
    logo_asset_lines = ""
    if logo_image_supplied or logo_vis or logo_letters_ac or dc_list:
        logo_asset_lines = (
            "\nLOGO IMAGE (client uploaded bitmap — match this identity in all visuals; Imagen/Veo use text only, so treat the lines below as literal art direction):\n"
            f"- logo_image_supplied: {str(logo_image_supplied).lower()}\n"
        )
        if logo_vis:
            logo_asset_lines += f"- logo_visual_description: {logo_vis}\n"
        if logo_letters_ac:
            logo_asset_lines += f"- logo_lettering_text (exact): {logo_letters_ac}\n"
        if dc_list:
            logo_asset_lines += "- logo_dominant_colors: " + ", ".join(dc_list) + "\n"
    os_lines_raw = ac.get("on_screen_lines_verbatim")
    os_lines: list[str] = []
    if isinstance(os_lines_raw, list):
        for x in os_lines_raw:
            t = str(x).strip()
            if t:
                os_lines.append(t)
    verbatim_block = (
        "\n- on_screen_lines_verbatim: (none — you may propose short overlays)\n"
        if not os_lines
        else "\n- on_screen_lines_verbatim (CLIENT-LOCKED — copy into ad_requirements EXACTLY; same spelling/punctuation, do NOT paraphrase or fix typos):\n"
        + "\n".join(f"  {i + 1}. {line}" for i, line in enumerate(os_lines))
        + "\n"
    )

    primary_market = str(ac.get("primary_market") or "").strip()
    market_block = ""
    if primary_market:
        market_block = (
            f"\n- primary_market (media buy + creative geography): {primary_market}\n"
            "  Act as a performance marketing strategist: research realistic audience demographics for THIS product/service IN that market "
            "(regions or cities, age ranges, income or firm-size bands, languages, B2B vs B2C norms, category adoption). "
            "Reflect those findings in demographics, target_audience, visual_direction, tone_and_voice, and key_messages.\n"
            "  CASTING (critical for conversion): People shown in recommended creative (roles described for video/stills) must look like they "
            f"belong to {primary_market} buyers — do not default to generic Western-only office casting when the market is India or South Asia. "
            "For India specifically: Indian and South Asian professionals as default heroes (credible mixed gender, varied ages, contemporary metro/tier-1 corporate wardrobe and settings); "
            "modern Indian business context unless the source explicitly calls for something else.\n"
        )

    constraints_line = (
        "\nAD CONSTRAINTS (must follow):\n"
        f"- brand_name: {brand_name or '(not provided)'}\n"
        f"- brand_tagline: {brand_tagline or '(not provided)'}\n"
        f"- logo_text: {logo_text or '(not provided)'}\n"
        f"- objective: {objective or '(not provided)'}\n"
        f"- must_include_logo: {str(must_include_logo).lower()}\n"
        f"- must_sound_like_ad: {str(must_sound_like_ad).lower()}\n"
        f"- logo_persistent: {str(logo_persistent).lower()}  # if true, logo visible entire runtime (not only end card)\n"
        f"{verbatim_block}"
        f"{logo_asset_lines}"
        f"{market_block}"
        "- Make it feel like a professional ad campaign (clear hook, benefits, proof, CTA).\n"
        "- On-screen copy should be short, high-contrast, and safe for overlay (avoid dense paragraphs).\n"
        "- Provide explicit audio & music direction (tempo/BPM, vibe, mixing notes) so edits can stay in rhythm — for Indian campaigns, "
        "prefer contemporary corporate bed suitable for Meta/LinkedIn/YouTube India (subtle, not dominant; VO intelligible).\n"
    )

    return f"""Analyze the following {source_label} and produce ONE JSON object with these keys:
- company_summary (string, 2-4 sentences)
- target_audience (string)
- demographics (object with keys: regions, age_ranges, segments — each string or list of short strings; anchor to primary_market in ad_requirements when set, e.g. Indian metros/tiers, languages, B2B titles; use best effort if unknown)
- pain_points (array of strings)
- value_propositions (array of strings)
- marketing_angles (array of objects with keys: angle, rationale)
- tone_and_voice (string)
- key_messages (array of strings, max 5)
- suggested_ctas (array of strings, max 5)
- visual_direction (string: colours, imagery, style guidance for designers / image models; must align with primary_market casting and settings when set)
- ad_requirements (object with keys:
  - brand_name (string or null)
  - brand_tagline (string or null)
  - logo_text (string or null)  # use when no logo file is available; short, e.g. "ACME"
  - objective (string or null)  # e.g. "lead gen", "website traffic", "app installs"
  - must_include_logo (boolean)
  - must_sound_like_ad (boolean)
  - primary_market (string or null)  # e.g. "India" — drives demographics research + casting
  - casting_direction (string or null)  # who appears on camera; must match primary_market
  - marketing_localization_notes (string or null)  # channels, cultural nuances, rupee vs dollar B2C, Indian English VO, etc.
  - logo_persistent (boolean)  # true = logo/bug visible for whole video, all clips
  - on_screen_lines_verbatim (array of strings)  # echo client lines exactly; empty if none supplied
  - logo_image_supplied (boolean)  # true if client uploaded logo artwork
  - logo_visual_description (string or null)  # from uploaded logo analysis; null if no upload
  - logo_lettering_text (string or null)  # exact letters visible in uploaded logo; null if none/icon-only
  - logo_dominant_colors (array of strings or null)  # from upload analysis
  - hook (string, 1 sentence)
  - campaign_story_arc (array of short strings)  # e.g. ["Hook", "Problem", "Solution", "Proof", "CTA"]
  - on_screen_text_rules (array of strings)  # overlay guidance (length, contrast, safe areas)
  - audio_direction (string)  # voiceover style + clarity/mixing notes
  - music_direction (string)  # genre + BPM/tempo + rhythm notes
)
- creative_formats_recommended (array of strings, e.g. "carousel", "static image", "short video", "long video storyboard")
- confidence (string: "high"|"medium"|"low")
- assumptions (array of strings: what you assumed because data was missing)

Rules:
- Ground answers in the source material; label gaps in assumptions.
- No prompt templates for end users—this JSON is consumed by an automated ad pipeline.{brand_line}{constraints_line}

SOURCE BEGIN
{body}
SOURCE END

Output valid JSON only."""


def fetch_website_form_hints(url: str) -> dict[str, Any]:
    """
    Fetch a URL and use Vertex to suggest Studio form fields (brand name, tagline, etc.).
    """
    u = (url or "").strip()
    if not u:
        raise ValueError("url required")
    if not u.startswith(("http://", "https://")):
        u = "https://" + u

    page = _fetch_url_text(u)
    if not page:
        raise ValueError("could not fetch page or no extractable text")
    page = _truncate(page)

    model = _get_brief_model()
    if model is None:
        raise RuntimeError("GCP_PROJECT_ID is required for website hints (Vertex Gemini)")

    prompt = f"""You help fill a marketing campaign form from a website landing page.

URL: {u}

From SOURCE below, output JSON with ONLY these keys (use null if unknown):
- brand_name (string|null): customer-facing company/product name; prefer visible branding over raw domain.
- brand_tagline (string|null): visible slogan or subhead; else a very short value prop, or null.
- logo_text (string|null): short text for on-screen logo bug (letters/spaces, max ~24 chars, often uppercase style); null if unclear.
- objective (string|null): one short phrase e.g. lead generation, brand awareness, sales, website traffic, app installs.
- campaign_summary (string|null): 2-4 sentences: what to advertise and main hooks from the page (for brief / objective context).

SOURCE BEGIN
{page}
SOURCE END

Output valid JSON only."""

    from vertexai.generative_models import GenerationConfig

    _hint_tokens = int((os.getenv("VERTEX_WEBSITE_HINTS_MAX_TOKENS") or "2048").strip() or "2048")
    model_id = (os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.5-flash").strip()
    cfg = GenerationConfig(
        max_output_tokens=_hint_tokens,
        temperature=float((os.getenv("VERTEX_BRIEF_TEMPERATURE") or "0.35").strip() or "0.35"),
        response_mime_type="application/json",
    )
    resp = model.generate_content(prompt, generation_config=cfg)
    if not resp.candidates:
        raise RuntimeError("no candidates from model")
    parts = resp.candidates[0].content.parts
    raw_out = "".join(getattr(p, "text", "") or "" for p in parts).strip()
    hints = _parse_model_json_object(raw_out)

    def _s(key: str) -> str | None:
        v = hints.get(key)
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    return {
        "url": u,
        "brand_name": _s("brand_name"),
        "brand_tagline": _s("brand_tagline"),
        "logo_text": _s("logo_text"),
        "objective": _s("objective"),
        "campaign_summary": _s("campaign_summary"),
        "meta": {"source_chars": len(page), "model": model_id},
    }


def generate_creative_brief(
    *,
    source_type: str,
    text: str | None = None,
    url: str | None = None,
    pdf_base64: str | None = None,
    brand_hint: str | None = None,
    ad_constraints: dict[str, Any] | None = None,
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

    prompt = _build_prompt(source_label=label, body=body, brand_hint=brand_hint, ad_constraints=ad_constraints)

    model_id = (os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.5-flash").strip()
    # Large schema (incl. ad_requirements); 2048 often truncates mid-JSON → invalid JSON / 503.
    _brief_max_tokens = int((os.getenv("VERTEX_BRIEF_MAX_TOKENS") or "8192").strip() or "8192")

    def _call_json_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=_brief_max_tokens,
            temperature=float((os.getenv("VERTEX_BRIEF_TEMPERATURE") or "0.35").strip() or "0.35"),
            response_mime_type="application/json",
        )
        resp = model.generate_content(prompt, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from model")
        parts = resp.candidates[0].content.parts
        raw_out = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        return _parse_model_json_object(raw_out)

    def _call_text_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=_brief_max_tokens,
            temperature=float((os.getenv("VERTEX_BRIEF_TEMPERATURE") or "0.35").strip() or "0.35"),
        )
        resp = model.generate_content(prompt, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from model")
        parts = resp.candidates[0].content.parts
        raw_out = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        return _parse_model_json_object(raw_out)

    try:
        try:
            brief_obj = _call_json_mode()
        except (RuntimeError, json.JSONDecodeError, ValueError) as e:
            log.warning("creative brief: JSON mode failed (%s); retrying as plain text parse", e)
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

from __future__ import annotations

import json
import logging
import os
import re
import ast
from typing import Any

from .settings import settings

log = logging.getLogger(__name__)

_PLANNER: Any = None
_PLANNER_KEY: tuple[str, str, str] | None = None
_VIDEO_PLANNER: Any = None
_VIDEO_PLANNER_KEY: tuple[str, str, str] | None = None


def _get_prompt_planner():
    global _PLANNER, _PLANNER_KEY
    project = (settings.gcp_project_id or "").strip()
    if not project:
        return None
    region = (os.getenv("VERTEX_ASSETS_REGION") or os.getenv("VERTEX_BRIEF_REGION") or settings.gcp_region or "us-central1").strip()
    model_name = (os.getenv("VERTEX_ASSETS_MODEL") or os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.5-flash").strip()
    key = (project, region, model_name)
    if _PLANNER is not None and _PLANNER_KEY == key:
        return _PLANNER
    import vertexai
    from vertexai.generative_models import GenerativeModel

    vertexai.init(project=project, location=region)
    sys_txt = (
        "You are a creative director for paid social ads. "
        "You convert structured marketing briefs into precise text prompts for image generation models. "
        "Output JSON only—no markdown, no commentary."
    )
    _PLANNER = GenerativeModel(model_name, system_instruction=sys_txt)
    _PLANNER_KEY = key
    return _PLANNER


def _get_video_planner():
    global _VIDEO_PLANNER, _VIDEO_PLANNER_KEY
    project = (settings.gcp_project_id or "").strip()
    if not project:
        return None
    region = (os.getenv("VERTEX_ASSETS_REGION") or os.getenv("VERTEX_BRIEF_REGION") or settings.gcp_region or "us-central1").strip()
    model_name = (os.getenv("VERTEX_VIDEO_PLAN_MODEL") or os.getenv("VERTEX_ASSETS_MODEL") or os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.5-flash").strip()
    key = (project, region, model_name)
    if _VIDEO_PLANNER is not None and _VIDEO_PLANNER_KEY == key:
        return _VIDEO_PLANNER
    import vertexai
    from vertexai.generative_models import GenerativeModel

    vertexai.init(project=project, location=region)
    sys_txt = (
        "You are a creative director for short paid-social video (multi-clip stitch). "
        "You turn marketing briefs into one hero prompt for a video model, cross-clip continuity notes, "
        "and one storyboard beat per clip. Output JSON only—no markdown, no commentary."
    )
    _VIDEO_PLANNER = GenerativeModel(model_name, system_instruction=sys_txt)
    _VIDEO_PLANNER_KEY = key
    return _VIDEO_PLANNER


_ZW_SPACE_RE = re.compile(r"[\u200b-\u200d\ufeff]")


def _parse_model_json_object(raw: str) -> dict[str, Any]:
    """Best-effort parse for model JSON-ish outputs."""
    txt = (raw or "").strip()
    if not txt:
        raise json.JSONDecodeError("empty", txt, 0)

    # 1) strict JSON first
    try:
        data = json.loads(txt)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass

    # 2) extract first object and retry strict JSON
    m = re.search(r"\{[\s\S]*\}", txt)
    candidate = m.group(0) if m else txt
    try:
        data = json.loads(candidate)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass

    # 3) strip markdown/code fences + trailing commas + JSON booleans/null for literal_eval
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", candidate, flags=re.IGNORECASE | re.MULTILINE).strip()
    cleaned = re.sub(r",(\s*[}\]])", r"\1", cleaned)  # trailing commas
    cleaned = re.sub(r"\btrue\b", "True", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bfalse\b", "False", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bnull\b", "None", cleaned, flags=re.IGNORECASE)
    try:
        data = ast.literal_eval(cleaned)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    raise json.JSONDecodeError("invalid json object", txt, 0)


def _derive_brief_subject(brief: dict[str, Any]) -> str:
    """Extract a short campaign subject from brief with robust fallbacks."""
    candidates: list[str] = []
    for k in (
        "headline",
        "summary",
        "product_name",
        "brand_name",
        "business_name",
        "campaign_goal",
        "objective",
        "context",
        "source_text",
    ):
        v = brief.get(k)
        if isinstance(v, str) and v.strip():
            candidates.append(v.strip())
    if not candidates:
        # Last resort: compact brief blob and use first meaningful slice.
        raw = json.dumps(brief, ensure_ascii=False, default=str)
        raw = re.sub(r"\s+", " ", raw).strip()
        return (raw[:80] + "...") if len(raw) > 80 else (raw or "campaign")
    best = candidates[0]
    return best[:80]


def _fallback_production_plan(
    *,
    brief: dict[str, Any],
    segs: int,
    cn: int,
    total: int,
    clip: int,
    gen_video: bool,
    gen_carousel: bool,
    gen_image: bool,
    locked: list[str],
    logo_persistent: bool,
    lbl: str,
    model_id: str,
) -> dict[str, Any]:
    """Deterministic fallback so campaign generation never fails on planner JSON."""
    subject = _derive_brief_subject(brief)
    arc = ["Hook", "Problem", "Solution", "Proof", "CTA"]
    beats: list[dict[str, Any]] = []
    if gen_video and segs > 0:
        roles = ["hook", "problem", "solution", "proof", "cta"]
        for i in range(segs):
            role = roles[min(i, len(roles) - 1)]
            on_screen = locked[min(i, len(locked) - 1)] if locked else ""
            beats.append(
                {
                    "clip_index": i + 1,
                    "role": role,
                    "storyboard_action": f"{role.title()} scene for {subject} in a clean professional setting.",
                    "on_screen_text": _normalize_overlay_string(on_screen, 120),
                    "logo_treatment": "Persistent corner logo bug." if logo_persistent else "Subtle logo placement.",
                    "voiceover_line": f"{subject}: {role} message.",
                    "transition": "Clean cut",
                    "music_cue": "Corporate upbeat pulse",
                }
            )
    panels: list[dict[str, Any]] = []
    if gen_carousel and cn > 0:
        for i in range(cn):
            panels.append(
                {
                    "panel_index": i + 1,
                    "headline": f"{subject[:40]}".strip() or f"Panel {i + 1}",
                    "subline": f"Key benefit {i + 1}",
                    "visual_focus": f"Professional visual centered on {subject}.",
                    "logo_rule": "Consistent logo placement on all panels.",
                }
            )
    return {
        "one_line_summary": f"Production plan for {subject}.",
        "campaign_arc": arc,
        "video_beats": beats,
        "carousel_panels": panels,
        "still_image_notes": f"Create clean, brand-safe still imagery for {subject}." if gen_image else "",
        "continuity_addon": "Maintain consistent style, talent, lighting, and brand tone.",
        "hero_prompt_addon": f"Professional commercial style focused on {subject}.",
        "carousel_prompt_addon": f"Each panel should advance one clear benefit of {subject}.",
        "image_prompt_addon": f"High-quality product-focused visual for {subject}.",
        "audio_mix_notes": "Clear voiceover over light corporate music; prioritize speech clarity." if gen_video else "",
        "music_direction": "Modern corporate, medium tempo, positive." if gen_video else "",
        "production_plan_model": model_id + ":fallback",
        "segments": segs if gen_video else 0,
        "clip_seconds": clip if gen_video else 0,
        "total_seconds": total if gen_video else 0,
        "copy_locked_lines": list(locked),
        "logo_persistent": bool(logo_persistent),
        "logo_label": lbl or None,
    }


_TYPOGRAPHY_CONTINUITY = (
    "TYPOGRAPHY: At most ONE caption line per clip, placed only bottom-center OR a single lower-third band — "
    "never the same headline on left and right, never mirrored or duplicated side banners, never split one line into two matching titles. "
    "Monitors, wall displays, phones, laptops, whiteboards, and documents must show abstract glowing charts or heavily blurred UI — "
    "zero readable letters, numbers, or words in the environment except that single caption."
)


def _build_video_segment_overlays(production_plan: dict[str, Any]) -> list[str]:
    """One caption string per video segment from copy lock or production beats (may contain empty strings)."""
    try:
        segs = int(production_plan.get("segments") or 0)
    except (TypeError, ValueError):
        segs = 0
    if segs < 1:
        return []
    locked = production_plan.get("copy_locked_lines")
    if isinstance(locked, list) and any(str(x).strip() for x in locked):
        lines = [_normalize_overlay_string(str(x), 160) for x in locked if str(x).strip()]
        if not lines:
            return []
        n = len(lines)
        return [lines[min(i, n - 1)] for i in range(segs)]
    vb = production_plan.get("video_beats")
    if not isinstance(vb, list):
        return []
    out: list[str] = []
    for i in range(segs):
        if i < len(vb) and isinstance(vb[i], dict):
            out.append(_normalize_overlay_string(str(vb[i].get("on_screen_text") or ""), 160))
        else:
            out.append("")
    return out


def _ad_requirements_dict(brief: dict[str, Any]) -> dict[str, Any]:
    ar = brief.get("ad_requirements")
    return ar if isinstance(ar, dict) else {}


def _primary_market_label(brief: dict[str, Any]) -> str:
    return str(_ad_requirements_dict(brief).get("primary_market") or "").strip()


def _brief_targets_india(brief: dict[str, Any]) -> bool:
    pm = _primary_market_label(brief).lower()
    if not pm:
        return False
    if pm in ("in", "ind", "india", "bharat"):
        return True
    return "india" in pm or "indian" in pm or "south asia" in pm


def _localization_casting_clause(brief: dict[str, Any]) -> str:
    ar = _ad_requirements_dict(brief)
    pm = str(ar.get("primary_market") or "").strip()
    cast = str(ar.get("casting_direction") or "").strip()
    loc = str(ar.get("marketing_localization_notes") or "").strip()
    if not pm and not cast and not loc:
        return ""
    parts: list[str] = []
    if pm:
        parts.append(f"Primary market for media, messaging, and on-screen casting: {pm}.")
    if cast:
        parts.append(f"Casting (mandatory): {cast}")
    if loc:
        parts.append(f"Localization: {loc}")
    parts.append(
        "All featured people in generated visuals must match the demographics and casting above; storyboard beats are visual-only but must show "
        "credible talent for this market — not a default Western-only cast when the market is India or South Asia."
    )
    return "\n".join(parts)


def _normalize_overlay_string(s: str, max_len: int = 120) -> str:
    t = _ZW_SPACE_RE.sub("", (s or "").strip())
    t = re.sub(r"\s+", " ", t)
    return t[:max_len] if max_len > 0 else t


def _snap_veo_clip_seconds(seconds: int) -> int:
    s = int(seconds)
    if s <= 4:
        return 4
    if s <= 6:
        return 6
    return 8


def derive_video_plan_from_brief(
    brief: dict[str, Any],
    *,
    total_seconds: int,
    clip_seconds: int,
) -> dict[str, Any]:
    """
    Step 3: brief → hero prompt + continuity + storyboard lines (one per stitched segment).
    """
    if not isinstance(brief, dict) or not brief:
        raise ValueError("brief must be a non-empty object")

    model = _get_video_planner()
    if model is None:
        raise RuntimeError("GCP_PROJECT_ID is required for video planning (Vertex Gemini)")

    total = max(4, int(total_seconds))
    max_total = int(os.getenv("VERTEX_VIDEO_MAX_TOTAL_SECONDS") or "180")
    if total > max_total:
        raise ValueError(f"video_total_seconds must be ≤ {max_total} (VERTEX_VIDEO_MAX_TOTAL_SECONDS)")
    clip = _snap_veo_clip_seconds(clip_seconds)
    segs = (total + clip - 1) // clip
    max_seg = int(os.getenv("VERTEX_VIDEO_MAX_SEGMENTS") or "30")
    segs = max(1, min(segs, max_seg))

    brief_json = json.dumps(brief, ensure_ascii=False, default=str)
    if len(brief_json) > 32000:
        brief_json = brief_json[:32000] + "\n…"

    loc_extra = _localization_casting_clause(brief)
    loc_pre = (
        f"CLIENT MARKET & CASTING (mandatory — apply to continuity and implied subjects):\n{loc_extra}\n\n"
        if loc_extra
        else ""
    )

    user = f"""{loc_pre}You are given a marketing brief JSON. We will generate a video as {segs} separate clips of ~{clip}s each (stitched into one ad, total ~{total}s).

Produce ONE JSON object with keys:
- video_prompt (string): single premium hero creative direction for the whole piece (product, mood, audience, setting). No URLs. Keep under 1200 characters.
- video_continuity_text (string): constraints to keep across ALL clips (same talent style or product focus, lighting, color palette, camera language, brand tone). Pull from visual_direction and tone_and_voice. Under 800 characters.
- video_storyboard (array of exactly {segs} strings): for clip 1..{segs}, VISUAL action and cinematography only (who, where, what we see, camera). Do NOT include on-screen text, supers, captions, or dialogue in these strings — captions are handled separately.

Brief JSON:
{brief_json}

Output valid JSON only. video_storyboard must have exactly {segs} strings."""

    model_id = (os.getenv("VERTEX_VIDEO_PLAN_MODEL") or os.getenv("VERTEX_ASSETS_MODEL") or os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.5-flash").strip()

    def _json_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_VIDEO_PLAN_MAX_TOKENS") or os.getenv("VERTEX_ASSETS_MAX_TOKENS") or "4096").strip() or "4096"),
            temperature=float((os.getenv("VERTEX_VIDEO_PLAN_TEMPERATURE") or "0.45").strip() or "0.45"),
            response_mime_type="application/json",
        )
        resp = model.generate_content(user, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from video planner model")
        parts = resp.candidates[0].content.parts
        raw = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError("video planner returned non-object JSON")
        return data

    def _text_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_VIDEO_PLAN_MAX_TOKENS") or os.getenv("VERTEX_ASSETS_MAX_TOKENS") or "4096").strip() or "4096"),
            temperature=float((os.getenv("VERTEX_VIDEO_PLAN_TEMPERATURE") or "0.45").strip() or "0.45"),
        )
        resp = model.generate_content(user, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from video planner model")
        parts = resp.candidates[0].content.parts
        raw = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        m = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            raise RuntimeError("video planner returned no JSON object")
        data = json.loads(m.group(0))
        if not isinstance(data, dict):
            raise RuntimeError("video planner returned non-object JSON")
        return data

    try:
        try:
            data = _json_mode()
        except Exception:
            log.warning("creative assets video: JSON mode failed, retrying text parse")
            data = _text_mode()
    except json.JSONDecodeError:
        log.exception("creative assets video JSON parse")
        raise RuntimeError("video planner returned invalid JSON") from None

    vp = str(data.get("video_prompt") or "").strip()
    cont = str(data.get("video_continuity_text") or "").strip()
    raw_sb = data.get("video_storyboard")
    sb_list: list[str] = []
    if isinstance(raw_sb, list):
        sb_list = [str(x).strip() for x in raw_sb if x is not None and str(x).strip()]
    if not vp:
        raise RuntimeError("video planner missing video_prompt")
    if len(sb_list) != segs:
        if len(sb_list) > segs:
            sb_list = sb_list[:segs]
        else:
            while len(sb_list) < segs:
                sb_list.append(f"Continue the narrative; segment {len(sb_list) + 1} of {segs}.")

    if len(vp) > 4000:
        vp = vp[:4000]
    if len(cont) > 4000:
        cont = cont[:4000]

    return {
        "video_prompt": vp,
        "video_continuity_text": cont,
        "video_storyboard": sb_list,
        "segments": segs,
        "clip_seconds": clip,
        "total_seconds": total,
        "video_planner_model": model_id,
    }


def derive_imagen_prompts_from_brief(brief: dict[str, Any], *, carousel_n: int) -> dict[str, str]:
    """
    Turn Step 1 `brief` into Imagen-ready prompts for one still and one N-panel carousel request.
    """
    if not isinstance(brief, dict) or not brief:
        raise ValueError("brief must be a non-empty object")

    cn = max(1, min(8, int(carousel_n)))
    model = _get_prompt_planner()
    if model is None:
        raise RuntimeError("GCP_PROJECT_ID is required for creative assets (Vertex Gemini)")

    brief_json = json.dumps(brief, ensure_ascii=False, default=str)
    if len(brief_json) > 32000:
        brief_json = brief_json[:32000] + "\n…"

    loc_extra = _localization_casting_clause(brief)
    ind_add = ""
    if _brief_targets_india(brief):
        ind_add = (
            " For India-market campaigns: main subjects must be Indian or South Asian adults in professional Indian business settings; "
            "avoid all-Western or obvious foreign-stock-looking casts unless the brief explicitly requires global teams."
        )
    loc_pre = (
        f"CLIENT MARKET & CASTING (mandatory — both prompts must comply):\n{loc_extra}\n\n"
        if loc_extra
        else ""
    )

    user = f"""{loc_pre}You are given a marketing brief JSON (from an automated analysis step). Produce ONE JSON object with keys:
- image_prompt (string): a single detailed text-to-image prompt for ONE premium static ad (social feed). Use company_summary, value_propositions, key_messages, visual_direction, tone_and_voice, demographics, and ad_requirements.casting_direction. If appropriate, mention one short on-image headline derived from key_messages (max ~6 words). No URLs.{ind_add}
- carousel_prompt (string): ONE prompt instructing an image model to generate exactly {cn} distinct but visually consistent carousel panels for Instagram/LinkedIn. Number them "Panel 1:" … "Panel {cn}:" with clear scene/angle/prop changes while keeping brand look from visual_direction. Casting must match primary_market.{ind_add}
- negative_prompt (string): comma-separated things to avoid (e.g. extra fingers, watermark, cluttered text, low resolution).

Brief JSON:
{brief_json}

Output valid JSON only."""

    model_id = (os.getenv("VERTEX_ASSETS_MODEL") or os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.5-flash").strip()

    def _json_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_ASSETS_MAX_TOKENS") or "2048").strip() or "2048"),
            temperature=float((os.getenv("VERTEX_ASSETS_TEMPERATURE") or "0.45").strip() or "0.45"),
            response_mime_type="application/json",
        )
        resp = model.generate_content(user, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from planner model")
        parts = resp.candidates[0].content.parts
        raw = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError("planner returned non-object JSON")
        return data

    def _text_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_ASSETS_MAX_TOKENS") or "2048").strip() or "2048"),
            temperature=float((os.getenv("VERTEX_ASSETS_TEMPERATURE") or "0.45").strip() or "0.45"),
        )
        resp = model.generate_content(user, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from planner model")
        parts = resp.candidates[0].content.parts
        raw = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        m = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            raise RuntimeError("planner returned no JSON object")
        data = json.loads(m.group(0))
        if not isinstance(data, dict):
            raise RuntimeError("planner returned non-object JSON")
        return data

    try:
        try:
            data = _json_mode()
        except Exception:
            log.warning("creative assets: JSON mode failed, retrying text parse")
            data = _text_mode()
    except json.JSONDecodeError:
        log.exception("creative assets JSON parse")
        raise RuntimeError("planner returned invalid JSON") from None

    ip = str(data.get("image_prompt") or "").strip()
    cp = str(data.get("carousel_prompt") or "").strip()
    neg = str(data.get("negative_prompt") or "").strip()
    if not ip or not cp:
        raise RuntimeError("planner missing image_prompt or carousel_prompt")

    if neg:
        ip = f"{ip}\nAvoid: {neg}"
        cp = f"{cp}\nAvoid: {neg}"

    if _brief_targets_india(brief):
        cast_line = (
            " Casting: feature Indian/South Asian professional adults (mixed gender, credible ages) in contemporary Indian corporate or metro business context; "
            "not Western-only extras."
        )
        neg_india = "Western-only office cast with no South Asian representation, generic foreign stock-photo look for an India-market ad"
        ip = (ip + cast_line).strip()
        cp = (cp + cast_line).strip()
        ip = f"{ip}\nAvoid: {neg_india}"
        cp = f"{cp}\nAvoid: {neg_india}"

    if len(ip) > 4000:
        ip = ip[:4000]
    if len(cp) > 4000:
        cp = cp[:4000]

    return {
        "image_prompt": ip,
        "carousel_prompt": cp,
        "carousel_n": cn,
        "planner_model": model_id,
    }


def derive_autonomous_campaign_params(brief: dict[str, Any]) -> dict[str, Any]:
    """
    Decide carousel depth and long-form video stitch parameters from the brief alone (no user prompts).
    """
    if not isinstance(brief, dict) or not brief:
        raise ValueError("brief must be a non-empty object")

    model = _get_prompt_planner()
    if model is None:
        raise RuntimeError("GCP_PROJECT_ID is required for autonomous campaign planning")

    max_total = int(os.getenv("VERTEX_VIDEO_MAX_TOTAL_SECONDS") or "180")
    brief_json = json.dumps(brief, ensure_ascii=False, default=str)
    if len(brief_json) > 32000:
        brief_json = brief_json[:32000] + "\n…"

    user = f"""You are planning an automated paid-social campaign from a structured marketing brief JSON.

Produce ONE JSON object with keys:
- carousel_panel_count (integer): how many distinct carousel panels to generate (3–7). Base on story complexity and key_messages count.
- video_total_seconds (integer): target total length for a STITCHED multi-clip ad (16–{min(120, max_total)}). Prefer 24–48 for B2B; longer only if the brief clearly needs a story arc. Must be ≤ {max_total}.
- video_clip_seconds (integer): per-clip length for Veo fast; MUST be exactly 4, 6, or 8.
- planning_rationale (string): one short sentence for stakeholders (non-technical).

Brief JSON:
{brief_json}

Output valid JSON only."""

    model_id = (os.getenv("VERTEX_ASSETS_MODEL") or os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.5-flash").strip()

    def _json_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_ASSETS_MAX_TOKENS") or "2048").strip() or "2048"),
            temperature=float((os.getenv("VERTEX_ASSETS_TEMPERATURE") or "0.4").strip() or "0.4"),
            response_mime_type="application/json",
        )
        resp = model.generate_content(user, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from autonomous planner")
        parts = resp.candidates[0].content.parts
        raw = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError("autonomous planner returned non-object JSON")
        return data

    def _text_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_ASSETS_MAX_TOKENS") or "2048").strip() or "2048"),
            temperature=float((os.getenv("VERTEX_ASSETS_TEMPERATURE") or "0.4").strip() or "0.4"),
        )
        resp = model.generate_content(user, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from autonomous planner")
        parts = resp.candidates[0].content.parts
        raw = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        m = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            raise RuntimeError("autonomous planner returned no JSON object")
        data = json.loads(m.group(0))
        if not isinstance(data, dict):
            raise RuntimeError("autonomous planner returned non-object JSON")
        return data

    try:
        try:
            data = _json_mode()
        except Exception:
            log.warning("autonomous campaign: JSON mode failed, retrying text parse")
            data = _text_mode()
    except json.JSONDecodeError:
        log.exception("autonomous campaign JSON parse")
        raise RuntimeError("autonomous planner returned invalid JSON") from None

    try:
        cn = int(data.get("carousel_panel_count") or 4)
    except (TypeError, ValueError):
        cn = 4
    cn = max(3, min(7, cn))

    try:
        vt = int(data.get("video_total_seconds") or 32)
    except (TypeError, ValueError):
        vt = 32
    vt = max(16, min(max_total, vt))

    try:
        vclip = int(data.get("video_clip_seconds") or 8)
    except (TypeError, ValueError):
        vclip = 8
    vclip = _snap_veo_clip_seconds(vclip)

    rationale = str(data.get("planning_rationale") or "").strip() or "Planned from brief content and recommended formats."

    return {
        "carousel_panel_count": cn,
        "video_total_seconds": vt,
        "video_clip_seconds": vclip,
        "planning_rationale": rationale[:500],
        "planner_model": model_id,
    }


def derive_production_plan_from_brief(
    brief: dict[str, Any],
    *,
    carousel_n: int,
    total_seconds: int,
    clip_seconds: int,
    image_n: int,
    gen_image: bool = True,
    gen_carousel: bool = True,
    gen_video: bool = True,
    copy_locked_lines: list[str] | None = None,
    logo_persistent: bool = False,
    logo_label: str | None = None,
) -> dict[str, Any]:
    """
    Stage 3: turn brief (+ Stage 2 ad_requirements) into a detailed JSON production plan:
    story arc, per-clip beats, overlays, logo, transitions, audio/music, and prompt add-ons for generators.
    """
    if not isinstance(brief, dict) or not brief:
        raise ValueError("brief must be a non-empty object")

    model = _get_prompt_planner()
    if model is None:
        raise RuntimeError("GCP_PROJECT_ID is required for production planning (Vertex Gemini)")

    segs = 0
    total = 0
    clip = 8
    if gen_video:
        total = max(4, int(total_seconds))
        max_total = int(os.getenv("VERTEX_VIDEO_MAX_TOTAL_SECONDS") or "180")
        if total > max_total:
            raise ValueError(f"video_total_seconds must be ≤ {max_total}")
        clip = _snap_veo_clip_seconds(clip_seconds)
        segs = (total + clip - 1) // clip
        max_seg = int(os.getenv("VERTEX_VIDEO_MAX_SEGMENTS") or "30")
        segs = max(1, min(segs, max_seg))

    cn = max(0, min(10, int(carousel_n)))
    if gen_carousel and cn < 3:
        cn = 3
    if not gen_carousel:
        cn = 0

    in_images = max(0, min(10, int(image_n)))
    if gen_image and in_images < 1:
        in_images = 1
    if not gen_image:
        in_images = 0

    brief_json = json.dumps(brief, ensure_ascii=False, default=str)
    if len(brief_json) > 32000:
        brief_json = brief_json[:32000] + "\n…"

    video_clause = (
        f"Stitched video: ~{total}s total, ~{clip}s per clip, exactly {segs} clips."
        if gen_video
        else "No stitched video in this request (video_beats must be an empty array)."
    )
    carousel_clause = f"Carousel: {cn} panels." if gen_carousel else "No carousel (carousel_panels must be an empty array)."
    image_clause = f"Still image batch: {in_images} distinct still(s)." if gen_image else "No still images (still_image_notes and image_prompt_addon may be empty strings)."

    locked = [str(x).strip() for x in (copy_locked_lines or []) if str(x).strip()]
    lbl = (logo_label or "").strip()
    lock_clause = ""
    if locked:
        lock_clause = (
            "\nCLIENT COPY LOCK (non-negotiable):\n"
            "- These on-screen supers MUST be used verbatim in video_beats.on_screen_text — exact spelling and punctuation, no paraphrase, no substitute phrases (e.g. do not swap for 'data silos' if the line says something else).\n"
            "- One line per clip in order; if there are fewer lines than clips, repeat the LAST line for remaining clips.\n"
            + "\n".join(f"  {i + 1}. {_normalize_overlay_string(line, 200)}" for i, line in enumerate(locked))
            + "\n"
        )
    logo_clause = ""
    if logo_persistent:
        logo_clause = (
            "\nLOGO LOCK:\n"
            f"- logo_persistent=true: every video beat must show the brand logo as a small readable corner bug for the full clip duration (not only end card). Use label: {lbl or '(brand mark from brief)'}\n"
        )

    user = f"""You are a senior commercial director. The client needs a PRODUCTION PLAN for paid social (professional ad quality — not a vlog).

Inputs:
- {video_clause}
- {carousel_clause}
- {image_clause}
{lock_clause}{logo_clause}
Use the marketing brief JSON (includes ad_requirements / audio / music hints when present). Output ONE JSON object with keys:

- one_line_summary (string): single stakeholder-facing sentence (what this production delivers).
- campaign_arc (array of strings): explicit beats, e.g. Hook → Problem → Solution → Proof → CTA (5 items typical).
- video_beats (array of exactly {segs} objects), each object:
  - clip_index (integer, 1-based)
  - role (string): e.g. hook, problem, solution, proof, cta
  - storyboard_action (string): what we see (under 200 chars)
  - on_screen_text (string): max 6 words, title case or sentence case, high-contrast overlay style
  - logo_treatment (string): where/when logo appears (e.g. end card, lower-third), must match brief ad_requirements
  - voiceover_line (string): VO line for this clip (short, ad-speak)
  - transition (string): cut/dissolve/match-cut to next
  - music_cue (string): bar/phrase or BPM note aligned to this clip (keep rhythm with cuts)
- carousel_panels (array of exactly {cn} objects), each:
  - panel_index (integer, 1-based)
  - headline (max 8 words)
  - subline (optional, max 14 words)
  - visual_focus (string)
  - logo_rule (string)
- still_image_notes (string): direction for still(s); empty if no stills requested.
- continuity_addon (string): video continuity addendum; empty if no video.
- hero_prompt_addon (string): extra hero direction for video; empty if no video.
- carousel_prompt_addon (string): extra direction for carousel; empty if no carousel.
- image_prompt_addon (string): extra direction for stills; empty if no stills.
- audio_mix_notes (string): VO vs music balance, sidechain, avoid mud (under 400 chars). Empty if no video.
- music_direction (string): genre, tempo/BPM, rhythm (under 300 chars). Still useful for brand feel even without video.

Rules:
- If brief.ad_requirements exists, follow must_include_logo and on_screen_text_rules, primary_market, casting_direction, and marketing_localization_notes.
- Casting: storyboard_action and visual_focus must show people and locations credible for brief.ad_requirements.primary_market (for India: Indian/South Asian professionals, modern Indian business settings — not default Western-only talent when the market is India).
- Overlays: short, readable; never dense paragraphs on screen. storyboard_action must never instruct duplicate captions (e.g. same line on left and right).
- Environments: describe office screens/monitors/phones as abstract charts or blurred UI — not legible text — so video models do not generate garbled lettering on props.
- Sound: professional ad (VO + music in rhythm); call out sync points if useful; for India-market ads favor subtle corporate score and clear VO (Indian English neutral broadcast acceptable when primary_market is India).

Brief JSON:
{brief_json}

Output valid JSON only. Arrays must have exact lengths: video_beats={segs}, carousel_panels={cn}."""

    model_id = (os.getenv("VERTEX_PRODUCTION_PLAN_MODEL") or os.getenv("VERTEX_ASSETS_MODEL") or os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.5-flash").strip()

    def _json_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_PRODUCTION_PLAN_MAX_TOKENS") or "8192").strip() or "8192"),
            temperature=float((os.getenv("VERTEX_PRODUCTION_PLAN_TEMPERATURE") or "0.35").strip() or "0.35"),
            response_mime_type="application/json",
        )
        resp = model.generate_content(user, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from production planner")
        parts = resp.candidates[0].content.parts
        raw = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        data = _parse_model_json_object(raw)
        if not isinstance(data, dict):
            raise RuntimeError("production planner returned non-object JSON")
        return data

    def _text_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_PRODUCTION_PLAN_MAX_TOKENS") or "8192").strip() or "8192"),
            temperature=float((os.getenv("VERTEX_PRODUCTION_PLAN_TEMPERATURE") or "0.35").strip() or "0.35"),
        )
        resp = model.generate_content(user, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from production planner")
        parts = resp.candidates[0].content.parts
        raw = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        data = _parse_model_json_object(raw)
        if not isinstance(data, dict):
            raise RuntimeError("production planner returned non-object JSON")
        return data

    try:
        try:
            data = _json_mode()
        except Exception:
            log.warning("production plan: JSON mode failed, retrying text parse")
            data = _text_mode()
    except json.JSONDecodeError:
        log.exception("production plan JSON parse")
        return _fallback_production_plan(
            brief=brief,
            segs=segs,
            cn=cn,
            total=total,
            clip=clip,
            gen_video=gen_video,
            gen_carousel=gen_carousel,
            gen_image=gen_image,
            locked=locked,
            logo_persistent=logo_persistent,
            lbl=lbl,
            model_id=model_id,
        )

    beats: list[dict[str, Any]] = []
    if segs > 0:
        vb = data.get("video_beats")
        if isinstance(vb, list):
            for x in vb:
                if isinstance(x, dict):
                    beats.append(x)
        if len(beats) > segs:
            beats = beats[:segs]
        while len(beats) < segs:
            beats.append(
                {
                    "clip_index": len(beats) + 1,
                    "role": "progression",
                    "storyboard_action": f"Advance campaign story; clip {len(beats) + 1} of {segs}.",
                    "on_screen_text": "",
                    "logo_treatment": "End card or subtle lower-third per brief.",
                    "voiceover_line": "",
                    "transition": "Clean cut",
                    "music_cue": "On beat",
                }
            )

        persistent_logo = (
            f"Small clean brand logo bug ({lbl or 'from brief'}) in lower-right safe area, fully readable for entire "
            "clip; do not remove mid-clip; not only end card."
            if logo_persistent
            else ""
        )
        if locked:
            n = len(locked)
            for i, b in enumerate(beats):
                if not isinstance(b, dict):
                    continue
                b["on_screen_text"] = _normalize_overlay_string(locked[min(i, n - 1)])
        if logo_persistent and persistent_logo:
            for b in beats:
                if isinstance(b, dict):
                    b["logo_treatment"] = persistent_logo

    panels: list[dict[str, Any]] = []
    if cn > 0:
        cp = data.get("carousel_panels")
        if isinstance(cp, list):
            for x in cp:
                if isinstance(x, dict):
                    panels.append(x)
        if len(panels) > cn:
            panels = panels[:cn]
        while len(panels) < cn:
            panels.append(
                {
                    "panel_index": len(panels) + 1,
                    "headline": f"Panel {len(panels) + 1}",
                    "subline": "",
                    "visual_focus": "On-brand product or benefit shot.",
                    "logo_rule": "Consistent placement per brief.",
                }
            )

    def _s(key: str, cap: int, default: str = "") -> str:
        t = str(data.get(key) or default).strip()
        return t[:cap] if cap else t

    out: dict[str, Any] = {
        "one_line_summary": _s("one_line_summary", 400, "Production plan from brief."),
        "campaign_arc": data.get("campaign_arc") if isinstance(data.get("campaign_arc"), list) else [],
        "video_beats": beats,
        "carousel_panels": panels,
        "still_image_notes": _s("still_image_notes", 1200),
        "continuity_addon": _s("continuity_addon", 800),
        "hero_prompt_addon": _s("hero_prompt_addon", 600),
        "carousel_prompt_addon": _s("carousel_prompt_addon", 600),
        "image_prompt_addon": _s("image_prompt_addon", 600),
        "audio_mix_notes": _s("audio_mix_notes", 500),
        "music_direction": _s("music_direction", 400),
        "production_plan_model": model_id,
        "segments": segs,
        "clip_seconds": clip if gen_video else 0,
        "total_seconds": total if gen_video else 0,
        "copy_locked_lines": list(locked),
        "logo_persistent": bool(logo_persistent),
        "logo_label": lbl or None,
    }
    return out


def apply_production_plan_to_prompts(
    production_plan: dict[str, Any],
    *,
    prompts_imagen: dict[str, Any] | None,
    video_plan: dict[str, Any] | None,
) -> None:
    """Merge Stage 3 add-ons into model prompts (mutates dicts in place)."""
    if not production_plan:
        return

    def _add(base: str, addon: str, label: str) -> str:
        a = (addon or "").strip()
        if not a:
            return base
        return ((base or "").rstrip() + f"\n\n{label}: " + a).strip()

    ip_add = str(production_plan.get("image_prompt_addon") or "").strip()
    cp_add = str(production_plan.get("carousel_prompt_addon") or "").strip()
    hp_add = str(production_plan.get("hero_prompt_addon") or "").strip()
    cont_add = str(production_plan.get("continuity_addon") or "").strip()
    audio = str(production_plan.get("audio_mix_notes") or "").strip()
    music = str(production_plan.get("music_direction") or "").strip()
    still_notes = str(production_plan.get("still_image_notes") or "").strip()

    if prompts_imagen:
        if ip_add:
            prompts_imagen["image_prompt"] = _add(str(prompts_imagen.get("image_prompt") or ""), ip_add, "PRODUCTION_PLAN")
        elif still_notes:
            prompts_imagen["image_prompt"] = _add(str(prompts_imagen.get("image_prompt") or ""), still_notes, "STILL_NOTES")
        if cp_add:
            prompts_imagen["carousel_prompt"] = _add(str(prompts_imagen.get("carousel_prompt") or ""), cp_add, "PRODUCTION_PLAN")

    if video_plan:
        if hp_add:
            video_plan["video_prompt"] = _add(str(video_plan.get("video_prompt") or ""), hp_add, "PRODUCTION_PLAN")
        if cont_add:
            video_plan["video_continuity_text"] = _add(str(video_plan.get("video_continuity_text") or ""), cont_add, "PRODUCTION_PLAN_CONTINUITY")
        extra_lines: list[str] = []
        if audio:
            extra_lines.append("Audio mix: " + audio)
        if music:
            extra_lines.append("Music: " + music)
        if extra_lines:
            video_plan["video_continuity_text"] = _add(
                str(video_plan.get("video_continuity_text") or ""),
                " ".join(extra_lines),
                "PRODUCTION_PLAN_AUDIO",
            )

        # Storyboard = visual beats only. Do not append caption text here — it caused duplicate/mirrored supers
        # in Veo; captions go in video_segment_overlays → per-segment SEGMENT_CAPTION in vertex_video.
        vb = production_plan.get("video_beats")
        sb = video_plan.get("video_storyboard")
        if isinstance(vb, list) and isinstance(sb, list) and len(vb) == len(sb):
            merged: list[str] = []
            for i, line in enumerate(sb):
                beat = vb[i] if i < len(vb) and isinstance(vb[i], dict) else {}
                action = str(beat.get("storyboard_action") or "").strip()
                merged.append((action or str(line).strip())[:500])
            video_plan["video_storyboard"] = merged

        overlays = _build_video_segment_overlays(production_plan)
        if overlays and any(str(x).strip() for x in overlays):
            video_plan["video_segment_overlays"] = overlays

        video_plan["video_continuity_text"] = _add(
            str(video_plan.get("video_continuity_text") or ""),
            _TYPOGRAPHY_CONTINUITY,
            "TYPOGRAPHY_RULES",
        )

        locked_lines = production_plan.get("copy_locked_lines")
        if isinstance(locked_lines, list) and any(str(x).strip() for x in locked_lines):
            video_plan["video_continuity_text"] = _add(
                str(video_plan.get("video_continuity_text") or ""),
                "VERBATIM_ORDER: Captions must match the SEGMENT_CAPTION block in each clip prompt exactly "
                "(same spelling and punctuation). Do not paraphrase; do not add extra taglines.",
                "VERBATIM_ORDER",
            )
        if production_plan.get("logo_persistent"):
            ll = str(production_plan.get("logo_label") or "").strip()
            video_plan["video_continuity_text"] = _add(
                str(video_plan.get("video_continuity_text") or ""),
                f"LOGO_CONTINUITY: keep brand logo ({ll or 'brand mark'}) visible as a small corner bug for 100% of "
                "runtime in every segment — never logo-only on end card.",
                "LOGO_CONTINUITY",
            )


def apply_logo_asset_to_prompts(prompts_used: dict[str, Any]) -> None:
    """Append uploaded-logo art direction to Imagen and Veo prompts (mutates prompts_used in place)."""
    if not prompts_used:
        return
    desc = str(prompts_used.get("logo_visual_description") or "").strip()
    letters = str(prompts_used.get("logo_lettering_text") or "").strip()
    supplied = bool(prompts_used.get("logo_image_supplied"))
    if not supplied and not desc and not letters:
        return
    chunks: list[str] = []
    if supplied:
        chunks.append(
            "The client supplied an official logo image file; reproduce this brand identity faithfully "
            "(colors, mark style, and any described lettering)."
        )
    if letters:
        chunks.append("Exact logo lettering to preserve on-device: " + letters)
    if desc:
        chunks.append("Logo visual reference (from uploaded bitmap): " + desc)
    block = "\n".join(chunks)

    def _tail(base: str, label: str) -> str:
        return ((base or "").rstrip() + f"\n\n{label}: " + block).strip()

    ip = prompts_used.get("image_prompt")
    if isinstance(ip, str) and ip.strip():
        prompts_used["image_prompt"] = _tail(ip, "LOGO_ASSET")
    cp = prompts_used.get("carousel_prompt")
    if isinstance(cp, str) and cp.strip():
        prompts_used["carousel_prompt"] = _tail(cp, "LOGO_ASSET")
    vp = prompts_used.get("video")
    if isinstance(vp, dict):
        vpt = str(vp.get("video_prompt") or "")
        if vpt.strip():
            vp["video_prompt"] = _tail(vpt, "LOGO_ASSET")
        vc = str(vp.get("video_continuity_text") or "")
        vp["video_continuity_text"] = _tail(vc, "LOGO_ASSET") if vc.strip() else ("LOGO_ASSET: " + block).strip()


def derive_script_audio_spec(
    brief: dict[str, Any],
    production_plan: dict[str, Any],
    video_plan: dict[str, Any],
) -> dict[str, Any]:
    """
    Stage 4: full ad VO script, performance direction, music spec, and per-clip timing map
    (aligned to stitched clips) so audio can follow picture cuts and rhythm.
    """
    if not isinstance(brief, dict) or not brief:
        raise ValueError("brief must be a non-empty object")
    if not isinstance(video_plan, dict) or not video_plan:
        raise ValueError("video_plan required for script/audio spec")

    model = _get_video_planner()
    if model is None:
        raise RuntimeError("GCP_PROJECT_ID is required for script/audio planning (Vertex Gemini)")

    try:
        segs = int(video_plan.get("segments") or 0)
    except (TypeError, ValueError):
        segs = 0
    if segs < 1:
        raise ValueError("video_plan.segments must be >= 1")

    try:
        clip_s = int(video_plan.get("clip_seconds") or 8)
    except (TypeError, ValueError):
        clip_s = 8
    clip_s = _snap_veo_clip_seconds(clip_s)

    try:
        total_s = int(video_plan.get("total_seconds") or segs * clip_s)
    except (TypeError, ValueError):
        total_s = segs * clip_s

    brief_json = json.dumps(brief, ensure_ascii=False, default=str)
    if len(brief_json) > 24000:
        brief_json = brief_json[:24000] + "\n…"

    pp_json = json.dumps(production_plan, ensure_ascii=False, default=str)
    if len(pp_json) > 24000:
        pp_json = pp_json[:24000] + "\n…"

    vp_trim = {
        "video_prompt": video_plan.get("video_prompt"),
        "video_continuity_text": video_plan.get("video_continuity_text"),
        "video_storyboard": video_plan.get("video_storyboard"),
        "segments": segs,
        "clip_seconds": clip_s,
        "total_seconds": total_s,
    }
    vp_json = json.dumps(vp_trim, ensure_ascii=False, default=str)
    ar = _ad_requirements_dict(brief)
    video_lang = str(ar.get("video_language") or "auto").strip().lower() or "auto"
    if video_lang == "auto":
        lang_clause = "Language: infer from brief and primary_market; use a single coherent language per ad."
    else:
        lang_clause = (
            f"Language requirement: voiceover and spoken phrasing must be in {video_lang}. "
            "Do not switch to unrelated languages."
        )

    user = f"""You are a broadcast copy chief + sound designer for paid social ads.

We have:
- {segs} stitched clips of ~{clip_s}s each (~{total_s}s total).
- A marketing brief JSON, a production_plan JSON (beats, music notes), and the current video_plan (hero prompt, continuity, storyboard lines).

Produce ONE JSON object with keys:

- one_line_audio_summary (string): one sentence: how VO + music should feel (professional ad, not vlog).
- ad_script_full (string): complete voiceover as one script with exact line breaks; include markers like [CLIP 1] … [CLIP {segs}] so each clip has its lines. Persuasive, clear CTA. Under 2500 chars.
- voiceover_direction (object with keys):
  - tone (string)
  - pacing (string)  # e.g. "measured 140 wpm on benefits, faster on CTA"
  - energy_curve (string)  # low→build→peak at CTA
  - do_list (array of strings, max 5)
  - dont_list (array of strings, max 5)  # e.g. no mumbling, no long lists
- music_spec (object with keys):
  - genre (string)
  - bpm (integer or string)  # target tempo
  - meter (string)  # e.g. 4/4
  - arrangement_arc (string)  # intro / lift / bridge / outro for ad length
  - edit_sync_rules (string)  # downbeats on cuts; avoid off-beat chorus hits on dialogue
- timing_map (array of exactly {segs} objects), each:
  - clip_index (integer, 1..{segs})
  - t_start_sec (number)  # approximate cumulative start
  - t_end_sec (number)
  - vo_lines (string)  # lines for this clip only (no stage directions)
  - music_hit (string)  # what happens musically at clip start/end (e.g. downbeat, lift, drop)
  - ducking_note (string)  # VO vs music (sidechain / -6dB under VO)
- sfx_optional (array of strings, max 5): subtle UI whoosh / logo sting — or empty

Rules:
- Align timing_map to ~{clip_s}s per clip; t_end_sec - t_start_sec ≈ {clip_s} (last clip may be shorter if total not divisible).
- Music and VO must work in rhythm with cuts; call out sync explicitly.
- If brief.ad_requirements exists, honor brand voice, primary_market, marketing_localization_notes, and CTA.
- {lang_clause}
- When primary_market is India (or brief says so): prefer clear Indian English VO (neutral professional), natural code-mix only if brief implies it; subtle India-appropriate corporate music bed — keep music supportive, not distracting (client-grade mix).
- Sound like a premium ad: confident, concise, not cheesy.

Brief JSON:
{brief_json}

Production plan JSON:
{pp_json}

Video plan JSON:
{vp_json}

Output valid JSON only. timing_map must have exactly {segs} items."""

    model_id = (
        os.getenv("VERTEX_SCRIPT_AUDIO_MODEL")
        or os.getenv("VERTEX_VIDEO_PLAN_MODEL")
        or os.getenv("VERTEX_ASSETS_MODEL")
        or os.getenv("VERTEX_BRIEF_MODEL")
        or "gemini-2.5-flash"
    ).strip()

    def _json_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_SCRIPT_AUDIO_MAX_TOKENS") or "8192").strip() or "8192"),
            temperature=float((os.getenv("VERTEX_SCRIPT_AUDIO_TEMPERATURE") or "0.38").strip() or "0.38"),
            response_mime_type="application/json",
        )
        resp = model.generate_content(user, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from script/audio planner")
        parts = resp.candidates[0].content.parts
        raw = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError("script/audio planner returned non-object JSON")
        return data

    def _text_mode() -> dict[str, Any]:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_SCRIPT_AUDIO_MAX_TOKENS") or "8192").strip() or "8192"),
            temperature=float((os.getenv("VERTEX_SCRIPT_AUDIO_TEMPERATURE") or "0.38").strip() or "0.38"),
        )
        resp = model.generate_content(user, generation_config=cfg)
        if not resp.candidates:
            raise RuntimeError("no candidates from script/audio planner")
        parts = resp.candidates[0].content.parts
        raw = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        m = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            raise RuntimeError("script/audio planner returned no JSON object")
        data = json.loads(m.group(0))
        if not isinstance(data, dict):
            raise RuntimeError("script/audio planner returned non-object JSON")
        return data

    try:
        try:
            data = _json_mode()
        except Exception:
            log.warning("script/audio spec: JSON mode failed, retrying text parse")
            data = _text_mode()
    except json.JSONDecodeError:
        log.exception("script/audio spec JSON parse")
        raise RuntimeError("script/audio planner returned invalid JSON") from None

    tm = data.get("timing_map")
    rows: list[dict[str, Any]] = []
    if isinstance(tm, list):
        for x in tm:
            if isinstance(x, dict):
                rows.append(x)
    if len(rows) > segs:
        rows = rows[:segs]
    while len(rows) < segs:
        rows.append(
            {
                "clip_index": len(rows) + 1,
                "t_start_sec": round(len(rows) * clip_s, 2),
                "t_end_sec": round((len(rows) + 1) * clip_s, 2),
                "vo_lines": "",
                "music_hit": "Downbeat on cut",
                "ducking_note": "Duck music under VO (−6dB).",
            }
        )

    def _s(key: str, cap: int, default: str = "") -> str:
        t = str(data.get(key) or default).strip()
        return t[:cap] if cap else t

    vd = data.get("voiceover_direction") if isinstance(data.get("voiceover_direction"), dict) else {}
    ms = data.get("music_spec") if isinstance(data.get("music_spec"), dict) else {}
    sfx = data.get("sfx_optional") if isinstance(data.get("sfx_optional"), list) else []

    return {
        "one_line_audio_summary": _s("one_line_audio_summary", 400, "Professional VO + music locked to edits."),
        "ad_script_full": _s("ad_script_full", 3000),
        "language": video_lang,
        "voiceover_direction": vd,
        "music_spec": ms,
        "timing_map": rows,
        "sfx_optional": [str(x) for x in sfx[:5]],
        "script_audio_model": model_id,
        "segments": segs,
        "clip_seconds": clip_s,
        "total_seconds": total_s,
    }


def apply_script_audio_to_video_plan(video_plan: dict[str, Any], script_audio: dict[str, Any]) -> None:
    """Merge Stage 4 script/audio into video prompts (mutates video_plan)."""
    if not video_plan or not script_audio:
        return

    def _add(base: str, addon: str, label: str) -> str:
        a = (addon or "").strip()
        if not a:
            return base
        return ((base or "").rstrip() + f"\n\n{label}: " + a).strip()

    summary = str(script_audio.get("one_line_audio_summary") or "").strip()
    full_script = str(script_audio.get("ad_script_full") or "").strip()
    vd = script_audio.get("voiceover_direction")
    ms = script_audio.get("music_spec")

    vo_bits: list[str] = []
    if isinstance(vd, dict):
        t = str(vd.get("tone") or "").strip()
        p = str(vd.get("pacing") or "").strip()
        e = str(vd.get("energy_curve") or "").strip()
        if t:
            vo_bits.append("VO tone: " + t)
        if p:
            vo_bits.append("Pacing: " + p)
        if e:
            vo_bits.append("Energy: " + e)

    mu_bits: list[str] = []
    if isinstance(ms, dict):
        g = str(ms.get("genre") or "").strip()
        b = str(ms.get("bpm") or "").strip()
        er = str(ms.get("edit_sync_rules") or "").strip()
        if g:
            mu_bits.append("Music genre: " + g)
        if b:
            mu_bits.append("Target BPM: " + b)
        if er:
            mu_bits.append("Edit sync: " + er)

    continuity_blob = ""
    if summary:
        continuity_blob += summary + "\n"
    if vo_bits:
        continuity_blob += "\n".join(vo_bits) + "\n"
    if mu_bits:
        continuity_blob += "\n".join(mu_bits) + "\n"

    tm = script_audio.get("timing_map")
    if isinstance(tm, list) and tm:
        lines: list[str] = []
        for row in tm:
            if not isinstance(row, dict):
                continue
            ci = row.get("clip_index")
            vo = str(row.get("vo_lines") or "").strip()
            mh = str(row.get("music_hit") or "").strip()
            dk = str(row.get("ducking_note") or "").strip()
            chunk = f"Clip {ci}: VO: {vo}" if vo else f"Clip {ci}:"
            if mh:
                chunk += f" | Music: {mh}"
            if dk:
                chunk += f" | Mix: {dk}"
            lines.append(chunk[:500])
        if lines:
            continuity_blob += "Per-clip audio map:\n" + "\n".join(lines)

    if continuity_blob.strip():
        video_plan["video_continuity_text"] = _add(
            str(video_plan.get("video_continuity_text") or ""),
            continuity_blob.strip(),
            "STAGE4_SCRIPT_AUDIO",
        )

    if full_script:
        video_plan["video_prompt"] = _add(
            str(video_plan.get("video_prompt") or ""),
            "Full VO script (read naturally; align captions/overlays to phrases):\n" + full_script[:2000],
            "STAGE4_FULL_SCRIPT",
        )

    sb = video_plan.get("video_storyboard")
    if isinstance(sb, list) and isinstance(tm, list) and len(sb) == len(tm):
        merged_sb: list[str] = []
        for i, line in enumerate(sb):
            r = tm[i] if i < len(tm) and isinstance(tm[i], dict) else {}
            vo = str(r.get("vo_lines") or "").strip()
            merged_sb.append((str(line).strip() + (" | VO: " + vo if vo else ""))[:500])
        video_plan["video_storyboard"] = merged_sb


_STAGE5_VIDEO_NEG_EXTRA = (
    "amateur vlog feel, shaky handheld, unreadable tiny text overlays, paragraph-length on-screen text, "
    "cluttered graphics, chaotic music edits, off-beat cuts, muddy audio mix, low resolution, stock-footage mismatch, "
    "misspelled supers, garbled or corrupted on-screen text, random Unicode overlay artifacts, substituting "
    "different wording for requested supers, duplicate matching captions on left and right, symmetrical mirrored text, "
    "twin title cards, readable letters on monitors and phone screens, dashboard UI with legible words"
)


def merge_stage5_video_options(
    prompts_used: dict[str, Any],
    vid_opts: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Stage 5: finalize Vertex / Veo video request options — extended negative prompt and sensible generateAudio.
    Merges env VERTEX_VIDEO_NEGATIVE_PROMPT, campaign-quality guardrails, and optional caller vid_opts.
    """
    o = dict(vid_opts) if vid_opts else {}
    neg_parts: list[str] = []
    env_neg = (os.getenv("VERTEX_VIDEO_NEGATIVE_PROMPT") or "").strip()
    if env_neg:
        neg_parts.append(env_neg)
    neg_parts.append(_STAGE5_VIDEO_NEG_EXTRA)
    pp = prompts_used.get("production_plan") if isinstance(prompts_used.get("production_plan"), dict) else None
    if pp and str(pp.get("audio_mix_notes") or "").strip():
        neg_parts.append("speech buried under music, clipping, unintelligible VO")
    if pp and isinstance(pp.get("copy_locked_lines"), list) and any(str(x).strip() for x in pp["copy_locked_lines"]):
        neg_parts.append("wrong on-screen copy, paraphrased supers, typo overlays, gibberish captions")
    pm = str(prompts_used.get("primary_market") or "").strip()
    if _brief_targets_india({"ad_requirements": {"primary_market": pm}}):
        neg_parts.append(
            "Western-only corporate cast inappropriate for Indian paid social, visibly all-non-Indian team for India-market lead gen"
        )
    existing = str(o.get("video_negative_prompt") or "").strip()
    if existing:
        neg_parts.append(existing)
    o["video_negative_prompt"] = ", ".join(neg_parts)[:4000]

    if o.get("video_generate_audio") is None:
        gen_raw = (os.getenv("VERTEX_VIDEO_GENERATE_AUDIO") or "1").strip().lower()
        has_script = isinstance(prompts_used.get("script_audio"), dict)
        if gen_raw in ("0", "false", "no"):
            o["video_generate_audio"] = False
        elif has_script or gen_raw in ("1", "true", "yes", ""):
            o["video_generate_audio"] = True
    return o


def stage5_asset_manifest(
    *,
    gen_image: bool,
    gen_carousel: bool,
    gen_video: bool,
    prompts_used: dict[str, Any],
    status: str,
) -> dict[str, Any]:
    """Human-readable Stage 5 summary for API clients (what Imagen/Veo will or did run)."""
    jobs: list[dict[str, Any]] = []
    if gen_image:
        n = int(prompts_used.get("image_n") or 1)
        n = max(1, min(10, n))
        jobs.append({"asset_type": "image", "count": n, "backend": "vertex_imagen"})
    if gen_carousel:
        n = int(prompts_used.get("carousel_n") or 4)
        n = max(3, min(10, n))
        jobs.append({"asset_type": "carousel", "panels": n, "backend": "vertex_imagen"})
    if gen_video:
        vp = prompts_used.get("video")
        segs = 0
        clip_s = 0
        total_s = 0
        if isinstance(vp, dict):
            try:
                segs = int(vp.get("segments") or 0)
            except (TypeError, ValueError):
                segs = 0
            try:
                clip_s = int(vp.get("clip_seconds") or 0)
            except (TypeError, ValueError):
                clip_s = 0
            try:
                total_s = int(vp.get("total_seconds") or 0)
            except (TypeError, ValueError):
                total_s = 0
        jobs.append(
            {
                "asset_type": "video",
                "stitched_outputs": 1,
                "veo_segments": segs,
                "clip_seconds": clip_s,
                "total_seconds": total_s,
                "backend": "vertex_veo_stitched",
            }
        )
    return {
        "stage": 5,
        "name": "Asset generation",
        "description": "Runs Imagen (stills/carousel) and stitched Veo video using prompts enriched by Stages 3–4.",
        "status": status,
        "jobs": jobs,
    }

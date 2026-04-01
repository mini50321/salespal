from __future__ import annotations

import json
import logging
import os
import re
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
    model_name = (os.getenv("VERTEX_ASSETS_MODEL") or os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.0-flash-001").strip()
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
    model_name = (os.getenv("VERTEX_VIDEO_PLAN_MODEL") or os.getenv("VERTEX_ASSETS_MODEL") or os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.0-flash-001").strip()
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

    user = f"""You are given a marketing brief JSON. We will generate a video as {segs} separate clips of ~{clip}s each (stitched into one ad, total ~{total}s).

Produce ONE JSON object with keys:
- video_prompt (string): single premium hero creative direction for the whole piece (product, mood, audience, setting). No URLs. Keep under 1200 characters.
- video_continuity_text (string): constraints to keep across ALL clips (same talent style or product focus, lighting, color palette, camera language, brand tone). Pull from visual_direction and tone_and_voice. Under 800 characters.
- video_storyboard (array of exactly {segs} strings): for clip 1..{segs}, each string is the key beat/action for that clip (establish → benefit → proof/shot → CTA for typical arcs). Each line under 200 characters.

Brief JSON:
{brief_json}

Output valid JSON only. video_storyboard must have exactly {segs} strings."""

    model_id = (os.getenv("VERTEX_VIDEO_PLAN_MODEL") or os.getenv("VERTEX_ASSETS_MODEL") or os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.0-flash-001").strip()

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

    user = f"""You are given a marketing brief JSON (from an automated analysis step). Produce ONE JSON object with keys:
- image_prompt (string): a single detailed text-to-image prompt for ONE premium static ad (social feed). Use company_summary, value_propositions, key_messages, visual_direction, tone_and_voice. If appropriate, mention one short on-image headline derived from key_messages (max ~6 words). No URLs.
- carousel_prompt (string): ONE prompt instructing an image model to generate exactly {cn} distinct but visually consistent carousel panels for Instagram/LinkedIn. Number them "Panel 1:" … "Panel {cn}:" with clear scene/angle/prop changes while keeping brand look from visual_direction.
- negative_prompt (string): comma-separated things to avoid (e.g. extra fingers, watermark, cluttered text, low resolution).

Brief JSON:
{brief_json}

Output valid JSON only."""

    model_id = (os.getenv("VERTEX_ASSETS_MODEL") or os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.0-flash-001").strip()

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

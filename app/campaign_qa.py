from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

from .settings import settings

log = logging.getLogger(__name__)

_QA_MODEL: Any = None
_QA_KEY: tuple[str, str, str] | None = None


def _get_qa_model():
    global _QA_MODEL, _QA_KEY
    project = (settings.gcp_project_id or "").strip()
    if not project:
        return None
    region = (os.getenv("VERTEX_QA_REGION") or os.getenv("VERTEX_ASSETS_REGION") or settings.gcp_region or "us-central1").strip()
    model_name = (os.getenv("VERTEX_QA_MODEL") or os.getenv("VERTEX_BRIEF_MODEL") or "gemini-2.5-flash").strip()
    key = (project, region, model_name)
    if _QA_MODEL is not None and _QA_KEY == key:
        return _QA_MODEL
    import vertexai
    from vertexai.generative_models import GenerativeModel

    vertexai.init(project=project, location=region)
    sys_txt = (
        "You are a senior creative QA lead for paid social ads. "
        "You review delivery metadata (not raw pixels) and output concise JSON only."
    )
    _QA_MODEL = GenerativeModel(model_name, system_instruction=sys_txt)
    _QA_KEY = key
    return _QA_MODEL


def _jobs_outline(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        o = j.get("output")
        o = o if isinstance(o, dict) else {}
        imgs = o.get("images_base64")
        uris = o.get("images_gcs_uris")
        vids = o.get("videos")
        car_n = 0
        if isinstance(imgs, list):
            car_n = len(imgs)
        elif isinstance(uris, list):
            car_n = len(uris)
        out.append(
            {
                "asset_type": j.get("asset_type"),
                "status": j.get("status"),
                "error": j.get("error"),
                "has_still": bool(o.get("image_base64") or o.get("image_gcs_uri")),
                "carousel_images": car_n,
                "video_clips": len(vids) if isinstance(vids, list) else 0,
            }
        )
    return out


def _heuristic_qa(
    brief: dict[str, Any] | None,
    prompts_used: dict[str, Any],
    jobs: list[dict[str, Any]],
    *,
    gen_image: bool,
    gen_carousel: bool,
    gen_video: bool,
) -> tuple[list[dict[str, Any]], list[str], str]:
    checks: list[dict[str, Any]] = []
    issues: list[str] = []
    worst = "pass"

    def job_for(at: str) -> dict[str, Any] | None:
        for j in jobs:
            if isinstance(j, dict) and j.get("asset_type") == at:
                return j
        return None

    if gen_image:
        j = job_for("image")
        if not j:
            checks.append({"id": "image_job", "label": "Image job exists", "ok": False, "detail": "missing"})
            issues.append("Image was requested but no image job was returned.")
            worst = "fail"
        else:
            st = str(j.get("status") or "")
            ok = st != "failed"
            checks.append({"id": "image_job", "label": "Image job status", "ok": ok, "detail": st or "unknown"})
            if not ok:
                issues.append(f"Image job failed: {j.get('error') or 'error'}")
                worst = "fail"
            else:
                o = j.get("output") if isinstance(j.get("output"), dict) else {}
                has_out = bool(o.get("image_base64") or o.get("image_gcs_uri"))
                checks.append({"id": "image_output", "label": "Image output present", "ok": has_out, "detail": "base64 or GCS"})
                if not has_out:
                    issues.append("Image job has no image_base64 / image_gcs_uri in output.")
                    worst = "fail" if worst == "pass" else worst

    if gen_carousel:
        j = job_for("carousel")
        if not j:
            checks.append({"id": "carousel_job", "label": "Carousel job exists", "ok": False, "detail": "missing"})
            issues.append("Carousel was requested but no carousel job was returned.")
            worst = "fail"
        else:
            st = str(j.get("status") or "")
            ok = st != "failed"
            checks.append({"id": "carousel_job", "label": "Carousel job status", "ok": ok, "detail": st or "unknown"})
            if not ok:
                issues.append(f"Carousel job failed: {j.get('error') or 'error'}")
                worst = "fail"
            else:
                o = j.get("output") if isinstance(j.get("output"), dict) else {}
                imgs = o.get("images_base64") if isinstance(o.get("images_base64"), list) else []
                uris = o.get("images_gcs_uris") if isinstance(o.get("images_gcs_uris"), list) else []
                n = max(len(imgs), len(uris))
                want = int(prompts_used.get("carousel_n") or 3)
                want = max(3, min(10, want))
                has_enough = n >= want or (n > 0 and o.get("output_omitted"))
                checks.append(
                    {
                        "id": "carousel_count",
                        "label": "Carousel panel count",
                        "ok": has_enough,
                        "detail": f"got {n}, expected ~{want}",
                    }
                )
                if not has_enough and n < want:
                    issues.append(f"Carousel returned {n} images; expected about {want}.")
                    worst = "warn" if worst == "pass" else worst

    if gen_video:
        j = job_for("video")
        vp = prompts_used.get("video") if isinstance(prompts_used.get("video"), dict) else {}
        want_segs = int(vp.get("segments") or 0)
        if not j:
            checks.append({"id": "video_job", "label": "Video job exists", "ok": False, "detail": "missing"})
            issues.append("Video was requested but no video job was returned.")
            worst = "fail"
        else:
            st = str(j.get("status") or "")
            ok = st != "failed"
            checks.append({"id": "video_job", "label": "Video job status", "ok": ok, "detail": st or "unknown"})
            if not ok:
                issues.append(f"Video job failed: {j.get('error') or 'error'}")
                worst = "fail"
            else:
                o = j.get("output") if isinstance(j.get("output"), dict) else {}
                vids = o.get("videos") if isinstance(o.get("videos"), list) else []
                has_vid = any(
                    isinstance(v, dict) and (v.get("gcs_uri") or v.get("bytes_base64")) for v in vids
                )
                checks.append({"id": "video_output", "label": "Video output present", "ok": has_vid, "detail": f"{len(vids)} item(s)"})
                if not has_vid:
                    issues.append("Video job output has no playable video payload.")
                    worst = "fail"

        sa = prompts_used.get("script_audio") if isinstance(prompts_used.get("script_audio"), dict) else None
        if sa and want_segs > 0:
            tm = sa.get("timing_map") if isinstance(sa.get("timing_map"), list) else []
            match = len(tm) == want_segs
            checks.append(
                {
                    "id": "script_timing_segments",
                    "label": "Script timing_map vs video segments",
                    "ok": match,
                    "detail": f"timing {len(tm)} vs segments {want_segs}",
                }
            )
            if not match:
                issues.append("Stage 4 timing_map length does not match planned video segments (review script/audio stage).")
                worst = "warn" if worst == "pass" else worst

    if brief and isinstance(brief.get("ad_requirements"), dict):
        ar = brief["ad_requirements"]
        must_logo = bool(ar.get("must_include_logo", False))
        if must_logo and gen_video:
            vp_text = ""
            vpl = prompts_used.get("video") if isinstance(prompts_used.get("video"), dict) else {}
            vp_text = str(vpl.get("video_continuity_text") or "") + str(vpl.get("video_prompt") or "")
            logo_guess = re.search(
                r"\blogo\b|logo_text|end card|lower third|brand mark",
                vp_text,
                re.I,
            )
            checks.append(
                {
                    "id": "logo_in_prompts",
                    "label": "Logo called out in video prompts (proxy)",
                    "ok": bool(logo_guess),
                    "detail": "continuity/prompt mention",
                }
            )
            if not logo_guess:
                issues.append("Brief requires logo but video prompt/continuity may not emphasize logo placement (verify visually).")
                worst = "warn" if worst == "pass" else worst

    return checks, issues, worst


def _llm_qa_review(
    brief: dict[str, Any] | None,
    prompts_used: dict[str, Any],
    outline: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if (os.getenv("VERTEX_STAGE6_LLM") or "1").strip().lower() in ("0", "false", "no"):
        return None
    model = _get_qa_model()
    if model is None:
        return None

    brief_json = json.dumps(brief or {}, ensure_ascii=False, default=str)
    if len(brief_json) > 12000:
        brief_json = brief_json[:12000] + "\n…"
    pu_trim = {
        "autonomous_plan": (prompts_used or {}).get("autonomous_plan"),
        "one_line_summary": ((prompts_used or {}).get("production_plan") or {}).get("one_line_summary")
        if isinstance((prompts_used or {}).get("production_plan"), dict)
        else None,
        "script_audio_summary": ((prompts_used or {}).get("script_audio") or {}).get("one_line_audio_summary")
        if isinstance((prompts_used or {}).get("script_audio"), dict)
        else None,
    }
    outline_json = json.dumps(outline, ensure_ascii=False, default=str)

    user = f"""Given brief JSON (may be partial), planning summaries, and asset job outline after generation:

1) Does this DELIVERY PLAN still look like a professional paid-social ad (not a random short)?
2) List up to 5 concrete follow-ups for the team (e.g. re-check VO/music sync, shorten supers, verify logo end card) — based on metadata only.
3) Rate confidence high/medium/low.

Output ONE JSON object:
- sounds_like_ad (string: high|medium|low)
- client_summary (string, max 400 chars): plain English for a client
- follow_ups (array of strings, max 5)
- iteration_hints (array of objects with keys: area, suggestion; area one of video|carousel|image|audio|copy)

Brief JSON:
{brief_json}

Planning summary:
{json.dumps(pu_trim, ensure_ascii=False)}

Job outline:
{outline_json}
"""

    model_id = (os.getenv("VERTEX_QA_MODEL") or "gemini-2.5-flash").strip()
    try:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_QA_MAX_TOKENS") or "2048").strip() or "2048"),
            temperature=float((os.getenv("VERTEX_QA_TEMPERATURE") or "0.35").strip() or "0.35"),
            response_mime_type="application/json",
        )
        resp = model.generate_content(user, generation_config=cfg)
        if not resp.candidates:
            return None
        parts = resp.candidates[0].content.parts
        raw = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        data = json.loads(raw)
        if not isinstance(data, dict):
            return None
        data["qa_model"] = model_id
        return data
    except Exception:
        log.exception("stage6 LLM QA")
        return None


def run_stage6_campaign_qa(
    *,
    brief: dict[str, Any] | None,
    prompts_used: dict[str, Any],
    jobs: list[dict[str, Any]],
    gen_image: bool,
    gen_carousel: bool,
    gen_video: bool,
) -> dict[str, Any]:
    """
    Stage 6: QA checklist on job outputs + optional Gemini narrative for client follow-ups.
    Does not inspect raw pixels/audio (metadata-only + heuristics).
    """
    checks, issues, worst = _heuristic_qa(
        brief,
        prompts_used,
        jobs,
        gen_image=gen_image,
        gen_carousel=gen_carousel,
        gen_video=gen_video,
    )
    outline = _jobs_outline(jobs)
    llm = _llm_qa_review(brief, prompts_used, outline)

    recommended: list[dict[str, Any]] = []
    for iss in issues:
        target = "campaign"
        if "Image" in iss:
            target = "image"
        elif "Carousel" in iss:
            target = "carousel"
        elif "Video" in iss or "video" in iss:
            target = "video"
        recommended.append({"action": "investigate", "target": target, "detail": iss})

    if llm and isinstance(llm.get("iteration_hints"), list):
        for h in llm["iteration_hints"][:5]:
            if isinstance(h, dict):
                recommended.append(
                    {
                        "action": "iterate",
                        "target": str(h.get("area") or "campaign"),
                        "detail": str(h.get("suggestion") or ""),
                    }
                )

    overall = worst
    if llm:
        sla = str(llm.get("sounds_like_ad") or "").strip().lower()
        if sla == "medium" and overall == "pass":
            overall = "warn"
        elif sla == "low":
            overall = "warn" if overall == "pass" else overall

    return {
        "stage": 6,
        "name": "QA & iteration",
        "description": "Validates asset jobs and suggests follow-ups; rerun execute with updated prompts to iterate.",
        "overall_status": overall,
        "checks": checks,
        "issues": issues,
        "jobs_outline": outline,
        "llm_review": llm,
        "recommended_actions": recommended[:12],
    }


def stage6_pending_placeholder() -> dict[str, Any]:
    return {
        "stage": 6,
        "name": "QA & iteration",
        "status": "pending_execute",
        "message": "QA runs automatically after asset jobs complete (full campaign or /campaign/execute).",
    }

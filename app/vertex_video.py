from __future__ import annotations

import os
import time
from typing import Any

import google.auth
from google.auth.transport.requests import AuthorizedSession


def _is_veo3_family(model_id: str) -> bool:
    m = model_id.lower()
    return "veo-3" in m or "veo-3.1" in m


def _session() -> AuthorizedSession:
    creds, _ = google.auth.default(scopes=("https://www.googleapis.com/auth/cloud-platform",))
    return AuthorizedSession(creds)


def _base_v1(project: str, location: str) -> str:
    loc = location.strip()
    return f"https://{loc}-aiplatform.googleapis.com/v1/projects/{project}/locations/{loc}"


def generate_videos_veo(
    project_id: str,
    location: str,
    model_id: str,
    prompt: str,
    sample_count: int,
) -> dict[str, Any]:
    model_id = model_id.strip()
    duration = int(os.getenv("VERTEX_VIDEO_DURATION_SECONDS") or "8")
    aspect = (os.getenv("VERTEX_VIDEO_ASPECT_RATIO") or "16:9").strip()
    storage_uri = (os.getenv("VERTEX_VIDEO_OUTPUT_GCS_URI") or "").strip() or None
    neg = (os.getenv("VERTEX_VIDEO_NEGATIVE_PROMPT") or "").strip() or None
    resolution = (os.getenv("VERTEX_VIDEO_RESOLUTION") or "").strip() or None
    poll_interval = float(os.getenv("VERTEX_VIDEO_POLL_INTERVAL_SEC") or "10")
    max_wait = float(os.getenv("VERTEX_VIDEO_POLL_MAX_SEC") or "600")
    gen_audio_raw = os.getenv("VERTEX_VIDEO_GENERATE_AUDIO")
    if gen_audio_raw is None or gen_audio_raw == "":
        generate_audio = True
    else:
        generate_audio = gen_audio_raw.lower() in ("1", "true", "yes")

    sc = max(1, min(4, int(sample_count)))
    params: dict[str, Any] = {
        "durationSeconds": duration,
        "aspectRatio": aspect,
        "sampleCount": sc,
    }
    if storage_uri:
        params["storageUri"] = storage_uri
    if neg:
        params["negativePrompt"] = neg
    if _is_veo3_family(model_id):
        params["generateAudio"] = generate_audio
        if resolution:
            params["resolution"] = resolution
    person_gen = (os.getenv("VERTEX_VIDEO_PERSON_GENERATION") or "").strip()
    if person_gen:
        params["personGeneration"] = person_gen

    body: dict[str, Any] = {"instances": [{"prompt": prompt}], "parameters": params}

    session = _session()
    base = _base_v1(project_id, location)
    start_url = f"{base}/publishers/google/models/{model_id}:predictLongRunning"
    r = session.post(start_url, json=body, timeout=120)
    if r.status_code >= 400:
        raise RuntimeError(f"veo predictLongRunning failed: {r.status_code} {r.text}")
    op = r.json()
    op_name = op.get("name")
    if not op_name:
        raise RuntimeError(f"veo missing operation name: {op}")

    fetch_url = f"{base}/publishers/google/models/{model_id}:fetchPredictOperation"
    deadline = time.monotonic() + max_wait
    while time.monotonic() < deadline:
        pr = session.post(fetch_url, json={"operationName": op_name}, timeout=120)
        if pr.status_code >= 400:
            raise RuntimeError(f"veo fetchPredictOperation failed: {pr.status_code} {pr.text}")
        data = pr.json()
        if data.get("error"):
            raise RuntimeError(str(data["error"]))
        if data.get("done"):
            resp = data.get("response") or {}
            videos_out: list[dict[str, Any]] = []
            for v in resp.get("videos") or []:
                item: dict[str, Any] = {"mime_type": v.get("mimeType") or "video/mp4"}
                if v.get("gcsUri"):
                    item["gcs_uri"] = v["gcsUri"]
                if v.get("bytesBase64Encoded"):
                    item["bytes_base64"] = v["bytesBase64Encoded"]
                videos_out.append(item)
            return {
                "prompt": prompt,
                "model": model_id,
                "rai_media_filtered_count": resp.get("raiMediaFilteredCount"),
                "videos": videos_out,
            }
        time.sleep(poll_interval)

    raise RuntimeError("veo video generation timed out")

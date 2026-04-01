from __future__ import annotations

import base64
import os
import subprocess
import tempfile
import time
from typing import Any

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage


def _is_veo3_family(model_id: str) -> bool:
    m = model_id.lower()
    return "veo-3" in m or "veo-3.1" in m


def _session() -> AuthorizedSession:
    creds, _ = google.auth.default(scopes=("https://www.googleapis.com/auth/cloud-platform",))
    return AuthorizedSession(creds)


def _base_v1(project: str, location: str) -> str:
    loc = location.strip()
    return f"https://{loc}-aiplatform.googleapis.com/v1/projects/{project}/locations/{loc}"


def _parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    u = (gs_uri or "").strip()
    if not u.startswith("gs://"):
        raise ValueError("invalid gcs uri")
    rest = u[5:]
    i = rest.find("/")
    if i < 0:
        raise ValueError("invalid gcs uri")
    return rest[:i], rest[i + 1 :]


def _download_video_to_file(client: storage.Client, item: dict[str, Any], path: str) -> None:
    gcs = str(item.get("gcs_uri") or "").strip()
    b64 = item.get("bytes_base64")
    if gcs:
        b, name = _parse_gs_uri(gcs)
        blob = client.bucket(b).blob(name)
        blob.download_to_filename(path)
        return
    if isinstance(b64, str) and b64.strip():
        raw = base64.b64decode(b64.strip())
        with open(path, "wb") as f:
            f.write(raw)
        return
    raise RuntimeError("video item empty (missing gcs_uri/bytes_base64)")


def _concat_mp4_ffmpeg(paths: list[str], out_path: str) -> None:
    # Prefer re-encode for robustness (clips may differ).
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", suffix=".txt") as tf:
        for p in paths:
            # ffmpeg concat file quoting: wrap in single quotes and escape existing quotes.
            safe = p.replace("'", "'\\''")
            tf.write(f"file '{safe}'\n")
        list_path = tf.name
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            list_path,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            out_path,
        ]
        subprocess.run(cmd, check=True)
    finally:
        try:
            os.unlink(list_path)
        except OSError:
            pass


def _upload_file_to_gcs(client: storage.Client, bucket_name: str, object_name: str, path: str) -> str:
    blob = client.bucket(bucket_name).blob(object_name)
    blob.upload_from_filename(path, content_type="video/mp4")
    return f"gs://{bucket_name}/{object_name}"


def _enforce_veo_fast_duration(seconds: int) -> int:
    # Veo 3 fast supports only 4/6/8 seconds.
    if seconds in (4, 6, 8):
        return seconds
    return 8 if seconds > 6 else 6 if seconds > 4 else 4


def generate_long_video_stitched(
    *,
    project_id: str,
    location: str,
    model_id: str,
    prompt: str,
    total_seconds: int,
    clip_seconds: int,
    aspect_ratio: str | None = None,
    resolution: str | None = None,
    negative_prompt: str | None = None,
    storage_uri: str | None = None,
    poll_interval_sec: float | None = None,
    poll_max_sec: float | None = None,
    generate_audio: bool | None = None,
    continuity_text: str | None = None,
    storyboard: list[str] | None = None,
) -> dict[str, Any]:
    """
    Build a long video by generating multiple short clips and stitching them.
    Requires META_MEDIA_BUCKET (or ASSET_MEDIA_BUCKET) to store the stitched MP4.
    """
    total = max(1, int(total_seconds))
    clip = max(1, int(clip_seconds))
    if "fast" in (model_id or "").lower():
        clip = _enforce_veo_fast_duration(clip)
    max_total = int(os.getenv("VERTEX_VIDEO_MAX_TOTAL_SECONDS") or "180")
    if total > max_total:
        raise RuntimeError(f"requested total video length {total}s exceeds max {max_total}s (VERTEX_VIDEO_MAX_TOTAL_SECONDS)")

    bucket_name = (os.getenv("META_MEDIA_BUCKET") or os.getenv("ASSET_MEDIA_BUCKET") or "").strip()
    if not bucket_name:
        raise RuntimeError("META_MEDIA_BUCKET (or ASSET_MEDIA_BUCKET) is required for stitched long videos")

    segs = (total + clip - 1) // clip
    segs = max(1, min(segs, int(os.getenv("VERTEX_VIDEO_MAX_SEGMENTS") or "30")))
    client = storage.Client()

    cont = (continuity_text or "").strip()
    sb = [str(x).strip() for x in (storyboard or []) if isinstance(x, (str, int, float)) and str(x).strip()]
    clip_items: list[dict[str, Any]] = []
    segment_prompts: list[str] = []
    for i in range(segs):
        if sb and i < len(sb):
            seg_desc = sb[i]
        elif sb:
            seg_desc = sb[-1]
        else:
            seg_desc = (
                "Opening scene establishing shot."
                if i == 0
                else "Closing scene with clear call-to-action."
                if i == segs - 1
                else "Continuation of the same scene and story."
            )
        seg_prompt = prompt
        if cont:
            seg_prompt = f"{seg_prompt}\n\nContinuity constraints (keep consistent across all segments):\n{cont}"
        if segs > 1:
            seg_prompt = f"{seg_prompt}\n\nScene {i+1} of {segs}: {seg_desc}"
        segment_prompts.append(seg_prompt)
        out = generate_videos_veo(
            project_id=project_id,
            location=location,
            model_id=model_id,
            prompt=seg_prompt,
            sample_count=1,
            duration_seconds=clip,
            aspect_ratio=aspect_ratio,
            resolution=resolution,
            negative_prompt=negative_prompt,
            storage_uri=storage_uri,
            poll_interval_sec=poll_interval_sec,
            poll_max_sec=poll_max_sec,
            generate_audio=generate_audio,
        )
        vids = out.get("videos")
        if not isinstance(vids, list) or not vids:
            raise RuntimeError("veo returned no video segments")
        v0 = vids[0]
        if not isinstance(v0, dict):
            raise RuntimeError("veo returned invalid video item")
        clip_items.append(v0)

    with tempfile.TemporaryDirectory(prefix="salespal_veo_") as td:
        clip_paths: list[str] = []
        for idx, item in enumerate(clip_items):
            p = os.path.join(td, f"clip_{idx:03d}.mp4")
            _download_video_to_file(client, item, p)
            clip_paths.append(p)
        stitched_path = os.path.join(td, "stitched.mp4")
        _concat_mp4_ffmpeg(clip_paths, stitched_path)
        obj = f"salespal-assets/stitched_{int(time.time())}_{segs}x{clip}s.mp4"
        stitched_gs = _upload_file_to_gcs(client, bucket_name, obj, stitched_path)

    return {
        "prompt": prompt,
        "model": model_id,
        "stitched": True,
        "total_seconds": total,
        "clip_seconds": clip,
        "segments": segs,
        "segment_prompts": segment_prompts,
        "videos": [{"mime_type": "video/mp4", "gcs_uri": stitched_gs}],
    }


def generate_videos_veo(
    project_id: str,
    location: str,
    model_id: str,
    prompt: str,
    sample_count: int,
    duration_seconds: int | None = None,
    aspect_ratio: str | None = None,
    resolution: str | None = None,
    negative_prompt: str | None = None,
    storage_uri: str | None = None,
    poll_interval_sec: float | None = None,
    poll_max_sec: float | None = None,
    generate_audio: bool | None = None,
) -> dict[str, Any]:
    model_id = model_id.strip()
    duration = int(duration_seconds if duration_seconds is not None else (os.getenv("VERTEX_VIDEO_DURATION_SECONDS") or "8"))
    aspect = (aspect_ratio or os.getenv("VERTEX_VIDEO_ASPECT_RATIO") or "16:9").strip()
    storage_uri = (storage_uri or os.getenv("VERTEX_VIDEO_OUTPUT_GCS_URI") or "").strip() or None
    neg = (negative_prompt or os.getenv("VERTEX_VIDEO_NEGATIVE_PROMPT") or "").strip() or None
    resolution = (resolution or os.getenv("VERTEX_VIDEO_RESOLUTION") or "").strip() or None
    poll_interval = float(poll_interval_sec if poll_interval_sec is not None else (os.getenv("VERTEX_VIDEO_POLL_INTERVAL_SEC") or "10"))
    max_wait = float(poll_max_sec if poll_max_sec is not None else (os.getenv("VERTEX_VIDEO_POLL_MAX_SEC") or "600"))
    if generate_audio is None:
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

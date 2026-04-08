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


def _default_caption_font_path() -> str:
    for p in (
        (os.getenv("VERTEX_VIDEO_CAPTION_FONT") or "").strip(),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    ):
        if p and os.path.isfile(p):
            return p
    return ""


def _burn_caption_on_video(src: str, dst: str, caption: str) -> None:
    """
    Pixel-accurate caption overlay (exact spelling). Veo cannot render reliable text; we burn in after generation.
    """
    cap = (caption or "").strip()
    if not cap:
        import shutil

        shutil.copyfile(src, dst)
        return
    fontsize = int((os.getenv("VERTEX_VIDEO_CAPTION_FONTSIZE") or "38").strip() or "38")
    if len(cap) > 48:
        fontsize = max(22, fontsize - min(14, (len(cap) - 48) // 6))
    margin = int((os.getenv("VERTEX_VIDEO_CAPTION_MARGIN_BOTTOM") or "68").strip() or "68")
    font = _default_caption_font_path()
    font_ff = font.replace("\\", "/") if font else ""
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, suffix=".txt", newline="\n") as tf:
        tf.write(cap)
        if not cap.endswith("\n"):
            tf.write("\n")
        text_path = tf.name
    text_ff = text_path.replace("\\", "/")
    try:
        if font_ff:
            vf = (
                f"drawtext=fontfile={font_ff}:textfile={text_ff}:fontsize={fontsize}:"
                f"fontcolor=white:borderw=2:bordercolor=black@0.85:"
                f"x=(w-text_w)/2:y=h-text_h-{margin}:"
                f"box=1:boxcolor=black@0.55:boxborderw=14:line_spacing=10"
            )
        else:
            vf = (
                f"drawtext=textfile={text_ff}:fontsize={fontsize}:"
                f"fontcolor=white:borderw=2:bordercolor=black@0.85:"
                f"x=(w-text_w)/2:y=h-text_h-{margin}:"
                f"box=1:boxcolor=black@0.55:boxborderw=14:line_spacing=10"
            )
        cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", src, "-vf", vf, "-codec:a", "copy", dst]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError((r.stderr or r.stdout or "ffmpeg drawtext failed").strip())
    finally:
        try:
            os.unlink(text_path)
        except OSError:
            pass


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
            "-ar",
            "48000",
            "-ac",
            "2",
            "-b:a",
            "192k",
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


def _ffprobe_duration_seconds(path: str) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or r.stdout or "ffprobe failed").strip())
    raw = (r.stdout or "").strip()
    try:
        return float(raw)
    except ValueError as e:
        raise RuntimeError(f"invalid ffprobe duration output: {raw}") from e


def _overlay_logo_on_video(src: str, dst: str, logo_path: str) -> None:
    margin = int((os.getenv("VERTEX_VIDEO_LOGO_MARGIN") or "28").strip() or "28")
    width_pct = float((os.getenv("VERTEX_VIDEO_LOGO_WIDTH_PCT") or "0.14").strip() or "0.14")
    width_pct = min(0.35, max(0.06, width_pct))
    alpha = float((os.getenv("VERTEX_VIDEO_LOGO_ALPHA") or "0.95").strip() or "0.95")
    alpha = min(1.0, max(0.1, alpha))
    pos = (os.getenv("VERTEX_VIDEO_LOGO_POSITION") or "top-right").strip().lower()
    if pos == "top-left":
        x_expr = f"{margin}"
        y_expr = f"{margin}"
    elif pos == "bottom-left":
        x_expr = f"{margin}"
        y_expr = f"H-h-{margin}"
    elif pos == "bottom-right":
        x_expr = f"W-w-{margin}"
        y_expr = f"H-h-{margin}"
    else:
        x_expr = f"W-w-{margin}"
        y_expr = f"{margin}"
    vf = (
        f"[1:v]format=rgba,colorchannelmixer=aa={alpha},"
        f"scale=iw*min(1\\,W*{width_pct}/iw):-1[logo];"
        f"[0:v][logo]overlay={x_expr}:{y_expr}:format=auto"
    )
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        src,
        "-i",
        logo_path,
        "-filter_complex",
        vf,
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
        "-ar",
        "48000",
        "-ac",
        "2",
        "-b:a",
        "192k",
        dst,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or r.stdout or "ffmpeg logo overlay failed").strip())


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
    segment_captions: list[str] | None = None,
    logo_image_base64: str | None = None,
    logo_image_mime_type: str | None = None,
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
    caps_raw = segment_captions or []
    caps: list[str] = []
    for x in caps_raw:
        if isinstance(x, (str, int, float)):
            caps.append(str(x))
        else:
            caps.append("")
    burn_in = os.getenv("VERTEX_VIDEO_BURN_IN_CAPTIONS", "1").strip().lower() not in ("0", "false", "no")
    burn_in = burn_in and bool(any(str(x).strip() for x in caps))
    if burn_in:
        cont = (
            "CRITICAL — NO ON-SCREEN TYPOGRAPHY IN GENERATED PIXELS: Do not draw any readable words, captions, titles, "
            "logotypes, or UI labels in the video. Keep the lower third visually empty or a soft unfocused gradient only. "
            "Monitors and phones must stay abstract/blurred with no legible characters. Exact captions are composited "
            "after generation.\n\n"
            + cont
        )
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
        cap_i = caps[i].strip() if i < len(caps) else ""
        if cap_i and not burn_in:
            safe = cap_i.replace("\\", " ").replace('"', "'")[:500]
            seg_prompt = (
                f'{seg_prompt}\n\nSEGMENT_CAPTION (this clip only — one placement, bottom-center or lower-third; '
                f'no duplicate side titles; no other readable text in-scene). Verbatim caption text: "{safe}"'
            )
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
            p_raw = os.path.join(td, f"clip_{idx:03d}.mp4")
            _download_video_to_file(client, item, p_raw)
            cap_i = caps[idx].strip() if idx < len(caps) else ""
            if burn_in and cap_i:
                p_cap = os.path.join(td, f"clip_{idx:03d}_captioned.mp4")
                _burn_caption_on_video(p_raw, p_cap, cap_i)
                clip_paths.append(p_cap)
            else:
                clip_paths.append(p_raw)
        stitched_path = os.path.join(td, "stitched.mp4")
        _concat_mp4_ffmpeg(clip_paths, stitched_path)
        final_path = stitched_path
        if isinstance(logo_image_base64, str) and logo_image_base64.strip():
            raw_logo = logo_image_base64.strip()
            if raw_logo.startswith("data:"):
                try:
                    _, raw_logo = raw_logo.split(",", 1)
                except ValueError:
                    raw_logo = ""
            if raw_logo:
                logo_ext = ".png"
                mt = (logo_image_mime_type or "").strip().lower()
                if "jpeg" in mt or "jpg" in mt:
                    logo_ext = ".jpg"
                logo_path = os.path.join(td, "logo" + logo_ext)
                try:
                    with open(logo_path, "wb") as lf:
                        lf.write(base64.b64decode(raw_logo, validate=True))
                    overlaid = os.path.join(td, "stitched_logo.mp4")
                    _overlay_logo_on_video(stitched_path, overlaid, logo_path)
                    final_path = overlaid
                except Exception:
                    # Do not fail the whole pipeline if logo decode/overlay fails.
                    final_path = stitched_path
        dur_actual = _ffprobe_duration_seconds(final_path)
        tol = float((os.getenv("VERTEX_VIDEO_DURATION_TOLERANCE_SEC") or "3.0").strip() or "3.0")
        if abs(dur_actual - float(total)) > max(1.0, tol):
            raise RuntimeError(
                f"stitched video duration mismatch: requested={total}s actual={dur_actual:.2f}s"
            )
        obj = f"salespal-assets/stitched_{int(time.time())}_{segs}x{clip}s.mp4"
        stitched_gs = _upload_file_to_gcs(client, bucket_name, obj, final_path)

    return {
        "prompt": prompt,
        "model": model_id,
        "stitched": True,
        "total_seconds": total,
        "clip_seconds": clip,
        "segments": segs,
        "requested_total_seconds": total,
        "actual_duration_seconds": round(dur_actual, 2),
        "segment_prompts": segment_prompts,
        "caption_burn_in": burn_in,
        "logo_overlay_applied": bool(logo_image_base64 and str(logo_image_base64).strip()),
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
    _rai_retry_attempted: bool = False,
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
                if item.get("gcs_uri") or item.get("bytes_base64"):
                    videos_out.append(item)
            rai_n = resp.get("raiMediaFilteredCount")
            if not videos_out:
                retry_enabled = (os.getenv("VERTEX_VIDEO_RAI_RETRY_ENABLED") or "1").strip().lower() not in ("0", "false", "no")
                if retry_enabled and not _rai_retry_attempted:
                    retry_prompt = (
                        "Single adult subject in a clean professional office setting, neutral and brand-safe commercial tone. "
                        "No sensitive content, no crowd scene, no risky framing. "
                        + (prompt or "")[:700]
                    )
                    extra_neg = "violence, blood, weapon, nudity, explicit content, hate symbols, injury"
                    merged_neg = ((neg or "").strip() + ", " + extra_neg).strip(", ").strip()
                    return generate_videos_veo(
                        project_id=project_id,
                        location=location,
                        model_id=model_id,
                        prompt=retry_prompt,
                        sample_count=1,
                        duration_seconds=min(max(4, duration), 6),
                        aspect_ratio=aspect,
                        resolution=resolution,
                        negative_prompt=merged_neg,
                        storage_uri=storage_uri,
                        poll_interval_sec=poll_interval,
                        poll_max_sec=max_wait,
                        generate_audio=generate_audio,
                        _rai_retry_attempted=True,
                    )
                hint = (
                    " Vertex often returns an empty list when every sample was removed by safety/RAI filters "
                    "(see raiMediaFilteredCount). Retry with a shorter segment prompt, simpler scene, fewer people, "
                    "or adjust VERTEX_VIDEO_PERSON_GENERATION / negative prompt per your GCP policy."
                )
                rai_bit = f"raiMediaFilteredCount={rai_n}. " if rai_n is not None else ""
                raise RuntimeError("veo returned no video segments — " + rai_bit + hint.strip())
            return {
                "prompt": prompt,
                "model": model_id,
                "rai_media_filtered_count": rai_n,
                "videos": videos_out,
            }
        time.sleep(poll_interval)

    raise RuntimeError("veo video generation timed out")

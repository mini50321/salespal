from __future__ import annotations

from dataclasses import dataclass
import base64
import os
from typing import Any

from .settings import settings


@dataclass
class GeneratedAsset:
    asset_type: str
    payload: dict[str, Any]


def _mock_png_base64() -> str:
    raw = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/w8AAoMBgQd1Y+oAAAAASUVORK5CYII="
    )
    return base64.b64encode(raw).decode("ascii")


def _image_gen_kwargs() -> dict[str, Any]:
    kw: dict[str, Any] = {}
    ar = (os.getenv("VERTEX_IMAGE_ASPECT_RATIO") or "").strip()
    if ar:
        kw["aspect_ratio"] = ar
    neg = (os.getenv("VERTEX_IMAGE_NEGATIVE_PROMPT") or "").strip()
    if neg:
        kw["negative_prompt"] = neg
    lang = (os.getenv("VERTEX_IMAGE_LANGUAGE") or "").strip()
    if lang:
        kw["language"] = lang
    gs = (os.getenv("VERTEX_IMAGE_GUIDANCE_SCALE") or "").strip()
    if gs:
        try:
            kw["guidance_scale"] = float(gs)
        except ValueError:
            pass
    gcs = (os.getenv("VERTEX_IMAGE_OUTPUT_GCS_URI") or "").strip()
    if gcs:
        kw["output_gcs_uri"] = gcs
    sf = (os.getenv("VERTEX_IMAGE_SAFETY_FILTER_LEVEL") or "").strip()
    if sf:
        kw["safety_filter_level"] = sf
    pg = (os.getenv("VERTEX_IMAGE_PERSON_GENERATION") or "").strip()
    if pg:
        kw["person_generation"] = pg
    wm = (os.getenv("VERTEX_IMAGE_ADD_WATERMARK") or "").strip().lower()
    if wm in ("0", "false", "no"):
        kw["add_watermark"] = False
    elif wm in ("1", "true", "yes"):
        kw["add_watermark"] = True
    return kw


class Generator:
    def generate(self, asset_type: str, prompt: str, n: int, options: dict[str, Any] | None = None) -> GeneratedAsset:
        if settings.generator_backend == "vertex":
            return self._generate_vertex(asset_type, prompt, n, options=options)
        return self._generate_mock(asset_type, prompt, n)

    def _generate_mock(self, asset_type: str, prompt: str, n: int) -> GeneratedAsset:
        if asset_type == "video":
            return GeneratedAsset(
                asset_type="video",
                payload={"prompt": prompt, "model": "mock", "videos": [], "status": "mock"},
            )
        if asset_type == "carousel":
            return GeneratedAsset(
                asset_type="carousel",
                payload={"images_base64": [_mock_png_base64() for _ in range(max(1, n))], "prompt": prompt},
            )
        return GeneratedAsset(asset_type="image", payload={"image_base64": _mock_png_base64(), "prompt": prompt})

    def _generate_vertex(
        self, asset_type: str, prompt: str, n: int, *, options: dict[str, Any] | None = None
    ) -> GeneratedAsset:
        project = settings.gcp_project_id
        region = settings.gcp_region
        if not project:
            raise RuntimeError("GCP_PROJECT_ID is required for vertex backend")

        if asset_type in ("image", "carousel"):
            import vertexai

            vertexai.init(project=project, location=region)
            from vertexai.preview.vision_models import ImageGenerationModel

            # imagegeneration@006 is retired; see Google migration guide for Vertex AI image models.
            model_name = os.getenv("VERTEX_IMAGE_MODEL", "imagen-3.0-generate-002")
            model = ImageGenerationModel.from_pretrained(model_name)
            # Imagen requests typically allow a small batch (often max 4 per call).
            max_per = int((os.getenv("VERTEX_IMAGE_MAX_PER_REQUEST") or "4").strip())
            max_per = max(1, min(8, max_per))
            raw_n = n if asset_type == "carousel" else 1
            num = max(1, min(max_per, raw_n))
            img_kw = _image_gen_kwargs()
            out = model.generate_images(prompt=prompt, number_of_images=num, **img_kw)
            imgs = []
            for img in out.images:
                b = img._image_bytes
                imgs.append(base64.b64encode(b).decode("ascii"))
            if asset_type == "carousel":
                return GeneratedAsset(asset_type="carousel", payload={"images_base64": imgs, "prompt": prompt})
            return GeneratedAsset(asset_type="image", payload={"image_base64": imgs[0], "prompt": prompt})

        if asset_type == "video":
            from .vertex_video import generate_long_video_stitched, generate_videos_veo

            vid_region = (os.getenv("VERTEX_VIDEO_REGION") or region or "").strip()
            if not vid_region:
                raise RuntimeError("GCP_REGION or VERTEX_VIDEO_REGION is required for veo")
            opt = options or {}
            model_name = str(opt.get("video_model") or os.getenv("VERTEX_VIDEO_MODEL") or "veo-3.0-fast-generate-001").strip()
            # Long video request: stitch multiple short clips into one MP4.
            total_seconds = opt.get("video_total_seconds")
            clip_seconds = opt.get("video_clip_seconds")
            duration_seconds = opt.get("video_duration_seconds")
            aspect_ratio = opt.get("video_aspect_ratio")
            resolution = opt.get("video_resolution")
            negative_prompt = opt.get("video_negative_prompt")
            storage_uri = opt.get("video_output_gcs_uri")
            generate_audio = opt.get("video_generate_audio")

            if isinstance(total_seconds, (int, float)) and int(total_seconds) > 0:
                clip = int(clip_seconds) if isinstance(clip_seconds, (int, float)) and int(clip_seconds) > 0 else 8
                continuity_text = opt.get("video_continuity_text")
                storyboard = opt.get("video_storyboard")
                sb_list: list[str] | None = None
                if isinstance(storyboard, list):
                    sb_list = [str(x) for x in storyboard if x is not None and str(x).strip()]
                payload = generate_long_video_stitched(
                    project_id=project,
                    location=vid_region,
                    model_id=model_name,
                    prompt=prompt,
                    total_seconds=int(total_seconds),
                    clip_seconds=clip,
                    aspect_ratio=str(aspect_ratio) if isinstance(aspect_ratio, str) and aspect_ratio.strip() else None,
                    resolution=str(resolution) if isinstance(resolution, str) and resolution.strip() else None,
                    negative_prompt=str(negative_prompt) if isinstance(negative_prompt, str) and negative_prompt.strip() else None,
                    storage_uri=str(storage_uri) if isinstance(storage_uri, str) and storage_uri.strip() else None,
                    generate_audio=bool(generate_audio) if isinstance(generate_audio, bool) else None,
                    continuity_text=str(continuity_text) if isinstance(continuity_text, str) and continuity_text.strip() else None,
                    storyboard=sb_list,
                )
            else:
                ds = int(duration_seconds) if isinstance(duration_seconds, (int, float)) and int(duration_seconds) > 0 else None
                payload = generate_videos_veo(
                    project_id=project,
                    location=vid_region,
                    model_id=model_name,
                    prompt=prompt,
                    sample_count=n,
                    duration_seconds=ds,
                    aspect_ratio=str(aspect_ratio) if isinstance(aspect_ratio, str) and aspect_ratio.strip() else None,
                    resolution=str(resolution) if isinstance(resolution, str) and resolution.strip() else None,
                    negative_prompt=str(negative_prompt) if isinstance(negative_prompt, str) and negative_prompt.strip() else None,
                    storage_uri=str(storage_uri) if isinstance(storage_uri, str) and storage_uri.strip() else None,
                    generate_audio=bool(generate_audio) if isinstance(generate_audio, bool) else None,
                )
            return GeneratedAsset(asset_type="video", payload=payload)

        raise ValueError("invalid asset_type")

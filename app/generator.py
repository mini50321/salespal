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
    def generate(self, asset_type: str, prompt: str, n: int) -> GeneratedAsset:
        if settings.generator_backend == "vertex":
            return self._generate_vertex(asset_type, prompt, n)
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

    def _generate_vertex(self, asset_type: str, prompt: str, n: int) -> GeneratedAsset:
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
            from .vertex_video import generate_videos_veo

            vid_region = (os.getenv("VERTEX_VIDEO_REGION") or region or "").strip()
            if not vid_region:
                raise RuntimeError("GCP_REGION or VERTEX_VIDEO_REGION is required for veo")
            model_name = (os.getenv("VERTEX_VIDEO_MODEL") or "veo-3.0-fast-generate-001").strip()
            payload = generate_videos_veo(
                project_id=project,
                location=vid_region,
                model_id=model_name,
                prompt=prompt,
                sample_count=n,
            )
            return GeneratedAsset(asset_type="video", payload=payload)

        raise ValueError("invalid asset_type")

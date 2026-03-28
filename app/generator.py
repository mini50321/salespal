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


class Generator:
    def generate(self, asset_type: str, prompt: str, n: int) -> GeneratedAsset:
        if settings.generator_backend == "vertex":
            return self._generate_vertex(asset_type, prompt, n)
        return self._generate_mock(asset_type, prompt, n)

    def _generate_mock(self, asset_type: str, prompt: str, n: int) -> GeneratedAsset:
        if asset_type == "video":
            return GeneratedAsset(asset_type="video", payload={"status": "not_ready", "prompt": prompt})
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

        import vertexai

        vertexai.init(project=project, location=region)

        if asset_type in ("image", "carousel"):
            from vertexai.preview.vision_models import ImageGenerationModel

            model_name = os.getenv("VERTEX_IMAGE_MODEL", "imagegeneration@006")
            model = ImageGenerationModel.from_pretrained(model_name)
            out = model.generate_images(
                prompt=prompt,
                number_of_images=max(1, n if asset_type == "carousel" else 1),
            )
            imgs = []
            for img in out.images:
                b = img._image_bytes
                imgs.append(base64.b64encode(b).decode("ascii"))
            if asset_type == "carousel":
                return GeneratedAsset(asset_type="carousel", payload={"images_base64": imgs, "prompt": prompt})
            return GeneratedAsset(asset_type="image", payload={"image_base64": imgs[0], "prompt": prompt})

        if asset_type == "video":
            return GeneratedAsset(asset_type="video", payload={"status": "not_ready", "prompt": prompt})

        raise ValueError("invalid asset_type")

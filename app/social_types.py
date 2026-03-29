from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class PostResult:
    provider_post_id: str
    response: dict[str, Any]


class SocialProvider:
    name: str

    def post(
        self,
        channel: str,
        caption: str,
        asset_payload: dict[str, Any],
        brand_id: str,
    ) -> PostResult:
        raise NotImplementedError()

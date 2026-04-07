"""X (Twitter) — stub provider; wire X API v2 when credentials are available."""

from __future__ import annotations

from typing import Any

from .social_types import PostResult, SocialProvider


class TwitterXProvider(SocialProvider):
    name = "twitter_x"

    def post(
        self,
        channel: str,
        caption: str,
        asset_payload: dict[str, Any],
        brand_id: str,
    ) -> PostResult:
        return PostResult(
            provider_post_id=f"{brand_id}-x-{channel}-stub",
            response={
                "provider": "twitter_x",
                "channel": channel,
                "caption_preview": caption[:200],
                "stub": True,
            },
        )

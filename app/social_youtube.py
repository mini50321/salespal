"""YouTube — stub provider; wire YouTube Data API upload when OAuth is configured."""

from __future__ import annotations

from typing import Any

from .social_types import PostResult, SocialProvider


class YouTubeProvider(SocialProvider):
    name = "youtube"

    def post(
        self,
        channel: str,
        caption: str,
        asset_payload: dict[str, Any],
        brand_id: str,
    ) -> PostResult:
        return PostResult(
            provider_post_id=f"{brand_id}-yt-{channel}-stub",
            response={
                "provider": "youtube",
                "channel": channel,
                "caption_preview": caption[:200],
                "stub": True,
            },
        )

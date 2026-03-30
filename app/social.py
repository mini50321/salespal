from __future__ import annotations

from typing import Any

from .social_linkedin import LinkedInProvider
from .social_meta import MetaFacebookProvider, MetaInstagramProvider
from .social_types import PostResult, SocialProvider


class MockProvider(SocialProvider):
    name = "mock"

    def post(self, channel: str, caption: str, asset_payload: dict[str, Any], brand_id: str) -> PostResult:
        provider_post_id = f"{brand_id}-{channel}-mock"
        return PostResult(
            provider_post_id=provider_post_id,
            response={
                "provider": "mock",
                "channel": channel,
                "caption": caption,
                "asset_keys": sorted(list(asset_payload.keys())),
            },
        )


def get_provider(provider_name: str) -> SocialProvider:
    n = (provider_name or "").strip().lower()
    if n == "mock":
        return MockProvider()
    if n == "meta_facebook":
        return MetaFacebookProvider()
    if n == "meta_instagram":
        return MetaInstagramProvider()
    if n == "linkedin":
        return LinkedInProvider()
    raise ValueError("unsupported provider")

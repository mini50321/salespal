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
    if provider_name == "mock":
        return MockProvider()
    raise ValueError("unsupported provider")


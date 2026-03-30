from __future__ import annotations

import os
from typing import Any

import requests

from .social_meta import _images_from_payload, _video_from_payload
from .social_types import PostResult, SocialProvider


def _api_version() -> str:
    return (os.getenv("LINKEDIN_API_VERSION") or "202411").strip()


def _headers_json(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
        "LinkedIn-Version": _api_version(),
        "X-Restli-Protocol-Version": "2.0.0",
    }


def _register_upload(token: str, author_urn: str) -> tuple[str, str, dict[str, str]]:
    url = "https://api.linkedin.com/v2/assets?action=registerUpload"
    body = {
        "registerUploadRequest": {
            "recipes": ["urn:li:digitalmediaRecipe:feedshare-image"],
            "owner": author_urn,
            "serviceRelationships": [
                {
                    "relationshipType": "OWNER",
                    "identifier": "urn:li:userGeneratedContent",
                }
            ],
        }
    }
    r = requests.post(url, json=body, headers=_headers_json(token), timeout=120)
    j = r.json() if r.text else {}
    if r.status_code >= 400:
        raise RuntimeError(f"linkedin registerUpload {r.status_code}: {j or r.text}")
    val = j.get("value") or {}
    mech = val.get("uploadMechanism") or {}
    up = mech.get("com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest") or {}
    upload_url = up.get("uploadUrl")
    asset = val.get("asset")
    if not upload_url or not asset:
        raise RuntimeError(f"linkedin registerUpload missing fields: {j}")
    raw_h = up.get("headers") or {}
    put_h: dict[str, str] = {}
    if isinstance(raw_h, dict):
        for k, v in raw_h.items():
            if isinstance(v, list) and v:
                put_h[str(k)] = str(v[0])
            elif isinstance(v, str):
                put_h[str(k)] = v
    return str(upload_url), str(asset), put_h


def _put_image(upload_url: str, data: bytes, content_type: str, extra_headers: dict[str, str]) -> None:
    h = {"Content-Type": content_type}
    h.update(extra_headers)
    r = requests.put(upload_url, data=data, headers=h, timeout=300)
    if r.status_code >= 400:
        raise RuntimeError(f"linkedin image upload {r.status_code}: {r.text}")


def _create_ugc_image_post(
    token: str,
    author_urn: str,
    caption: str,
    asset_urn: str,
) -> dict[str, Any]:
    url = "https://api.linkedin.com/v2/ugcPosts"
    body = {
        "author": author_urn,
        "lifecycleState": "PUBLISHED",
        "specificContent": {
            "com.linkedin.ugc.ShareContent": {
                "shareCommentary": {"text": caption or " "},
                "shareMediaCategory": "IMAGE",
                "media": [
                    {
                        "status": "READY",
                        "media": asset_urn,
                    }
                ],
            }
        },
        "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"},
    }
    r = requests.post(url, json=body, headers=_headers_json(token), timeout=120)
    try:
        j = r.json()
    except Exception:
        j = {"raw": r.text}
    if r.status_code >= 400:
        raise RuntimeError(f"linkedin ugcPosts {r.status_code}: {j}")
    return j if isinstance(j, dict) else {"data": j}


class LinkedInProvider(SocialProvider):
    name = "linkedin"

    def post(
        self,
        channel: str,
        caption: str,
        asset_payload: dict[str, Any],
        brand_id: str,
    ) -> PostResult:
        token = (os.getenv("LINKEDIN_ACCESS_TOKEN") or "").strip()
        author = (os.getenv("LINKEDIN_AUTHOR_URN") or "").strip()
        if not token or not author:
            raise RuntimeError("LINKEDIN_ACCESS_TOKEN and LINKEDIN_AUTHOR_URN are required")
        if not author.startswith("urn:li:"):
            raise RuntimeError("LINKEDIN_AUTHOR_URN must be urn:li:person:... or urn:li:organization:...")

        gcs_v, bytes_v, _mime_v = _video_from_payload(asset_payload)
        if gcs_v or bytes_v:
            raise RuntimeError("linkedin provider supports image and carousel images only, not video yet")

        imgs = _images_from_payload(asset_payload)
        if not imgs:
            raise ValueError("linkedin post requires image_base64 or images_base64 in asset payload")

        total = len(imgs)
        data, ctype = imgs[0]
        if not ctype.startswith("image/"):
            ctype = "image/jpeg"

        upload_url, asset_urn, put_headers = _register_upload(token, author)
        _put_image(upload_url, data, ctype, put_headers)
        resp = _create_ugc_image_post(token, author, caption, asset_urn)

        post_id = ""
        if isinstance(resp.get("id"), str):
            post_id = resp["id"]
        lid = str(resp.get("id") or resp.get("urn") or "")
        if not post_id and lid:
            post_id = lid
        if not post_id:
            post_id = f"{brand_id}-linkedin"

        return PostResult(
            provider_post_id=post_id,
            response={
                "provider": "linkedin",
                "linkedin_response": resp,
                "images_posted": 1,
                "images_in_payload": total,
            },
        )

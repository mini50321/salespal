from __future__ import annotations

import base64
import os
import uuid
from datetime import timedelta
from typing import Any

import requests

from .social_types import PostResult, SocialProvider


def _graph_version() -> str:
    return (os.getenv("META_GRAPH_API_VERSION") or "v21.0").strip().strip("/")


def _page_token() -> str:
    return (os.getenv("META_PAGE_ACCESS_TOKEN") or os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN") or "").strip()


def _page_id() -> str:
    return (os.getenv("META_PAGE_ID") or os.getenv("FACEBOOK_PAGE_ID") or "").strip()


def _ig_user_id() -> str:
    return (os.getenv("META_IG_USER_ID") or os.getenv("INSTAGRAM_BUSINESS_ACCOUNT_ID") or "").strip()


def _media_bucket() -> str | None:
    b = (os.getenv("META_MEDIA_BUCKET") or "").strip()
    return b or None


def _guess_mime_from_bytes(data: bytes) -> str:
    if len(data) >= 8 and data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if len(data) >= 3 and data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    return "application/octet-stream"


def _decode_b64(data_b64: str) -> bytes:
    return base64.b64decode(data_b64.strip())


def _images_from_payload(asset_payload: dict[str, Any]) -> list[tuple[bytes, str]]:
    out: list[tuple[bytes, str]] = []
    if asset_payload.get("image_base64"):
        raw = _decode_b64(str(asset_payload["image_base64"]))
        out.append((raw, _guess_mime_from_bytes(raw)))
    imgs = asset_payload.get("images_base64")
    if isinstance(imgs, list):
        for item in imgs:
            if isinstance(item, str) and item.strip():
                raw = _decode_b64(item)
                out.append((raw, _guess_mime_from_bytes(raw)))
    return out


def _video_from_payload(asset_payload: dict[str, Any]) -> tuple[str | None, bytes | None, str]:
    vids = asset_payload.get("videos")
    if not isinstance(vids, list) or not vids:
        return None, None, "video/mp4"
    v0 = vids[0] if isinstance(vids[0], dict) else {}
    gcs = (v0.get("gcs_uri") or "").strip() if isinstance(v0, dict) else ""
    b64 = v0.get("bytes_base64") if isinstance(v0, dict) else None
    mime = (v0.get("mime_type") or "video/mp4") if isinstance(v0, dict) else "video/mp4"
    if gcs:
        return gcs, None, str(mime)
    if isinstance(b64, str) and b64.strip():
        return None, _decode_b64(b64), str(mime)
    return None, None, "video/mp4"


def _parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    u = gs_uri.strip()
    if not u.startswith("gs://"):
        raise ValueError("invalid gcs uri")
    rest = u[5:]
    i = rest.find("/")
    if i < 0:
        raise ValueError("invalid gcs uri")
    return rest[:i], rest[i + 1 :]


def _signed_gcs_get_url(gs_uri: str, minutes: int = 120) -> str:
    from google.cloud import storage

    bucket_name, blob_name = _parse_gs_uri(gs_uri)
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_name)
    return blob.generate_signed_url(
        expiration=timedelta(minutes=minutes),
        method="GET",
        version="v4",
    )


def _upload_temp_signed_url(data: bytes, content_type: str) -> str:
    bucket = _media_bucket()
    if not bucket:
        raise RuntimeError("META_MEDIA_BUCKET is required to publish this asset to Instagram")
    from google.cloud import storage

    ext = ".jpg" if "jpeg" in content_type else ".png" if "png" in content_type else ".bin"
    name = f"meta-ig/{uuid.uuid4().hex}{ext}"
    client = storage.Client()
    b = client.bucket(bucket)
    blob = b.blob(name)
    blob.upload_from_string(data, content_type=content_type)
    return blob.generate_signed_url(
        expiration=timedelta(minutes=120),
        method="GET",
        version="v4",
    )


def _public_image_url(asset_payload: dict[str, Any]) -> str:
    u = (asset_payload.get("public_image_url") or asset_payload.get("image_url") or "").strip()
    if u:
        return u
    imgs = _images_from_payload(asset_payload)
    if not imgs:
        raise ValueError("no image in asset payload")
    data, mime = imgs[0]
    vids = asset_payload.get("videos")
    if vids:
        raise ValueError("use meta_facebook for video or add public_image_url")
    gcs = (asset_payload.get("image_gcs_uri") or "").strip()
    if gcs.startswith("gs://"):
        return _signed_gcs_get_url(gcs)
    return _upload_temp_signed_url(data, mime)


def _graph_post(url: str, data: dict[str, Any] | None = None, files: dict | None = None) -> dict[str, Any]:
    r = requests.post(url, data=data, files=files, timeout=300)
    try:
        j = r.json()
    except Exception:
        j = {"raw": r.text}
    if r.status_code >= 400:
        raise RuntimeError(f"meta graph error {r.status_code}: {j}")
    if isinstance(j, dict) and j.get("error"):
        raise RuntimeError(str(j["error"]))
    return j if isinstance(j, dict) else {"data": j}


def _graph_post_params(url: str, params: dict[str, Any]) -> dict[str, Any]:
    r = requests.post(url, params=params, timeout=300)
    j = r.json()
    if r.status_code >= 400:
        raise RuntimeError(f"meta graph error {r.status_code}: {j}")
    if isinstance(j, dict) and j.get("error"):
        raise RuntimeError(str(j["error"]))
    return j if isinstance(j, dict) else {"data": j}


class MetaFacebookProvider(SocialProvider):
    name = "meta_facebook"

    def post(
        self,
        channel: str,
        caption: str,
        asset_payload: dict[str, Any],
        brand_id: str,
    ) -> PostResult:
        token = _page_token()
        page = _page_id()
        if not token or not page:
            raise RuntimeError("META_PAGE_ACCESS_TOKEN and META_PAGE_ID are required")
        ver = _graph_version()
        base = f"https://graph.facebook.com/{ver}"

        gcs_v, bytes_v, mime_v = _video_from_payload(asset_payload)
        if gcs_v or bytes_v:
            return self._post_video(base, page, token, caption, gcs_v, bytes_v, mime_v)

        imgs = _images_from_payload(asset_payload)
        if not imgs:
            raise ValueError("facebook post requires image, carousel, or video payload")

        if len(imgs) == 1:
            data, ct = imgs[0]
            ext = "jpg" if "jpeg" in ct else "png" if "png" in ct else "bin"
            url = f"{base}/{page}/photos"
            files = {"source": (f"upload.{ext}", data, ct)}
            form: dict[str, Any] = {"access_token": token, "published": "true"}
            if caption:
                form["message"] = caption
            out = _graph_post(url, data=form, files=files)
            pid = str(out.get("id") or out.get("post_id") or "")
            return PostResult(provider_post_id=pid or "unknown", response=out)

        media_ids: list[str] = []
        for data, ct in imgs:
            ext = "jpg" if "jpeg" in ct else "png" if "png" in ct else "bin"
            url = f"{base}/{page}/photos"
            files = {"source": (f"upload.{ext}", data, ct)}
            form = {"access_token": token, "published": "false"}
            out = _graph_post(url, data=form, files=files)
            mid = str(out.get("id") or "")
            if not mid:
                raise RuntimeError(f"facebook photo upload missing id: {out}")
            media_ids.append(mid)

        url = f"{base}/{page}/feed"
        payload: dict[str, Any] = {"access_token": token}
        if caption:
            payload["message"] = caption
        for i, mid in enumerate(media_ids):
            payload[f"attached_media[{i}][media_fbid]"] = mid
        out = _graph_post(url, data=payload)
        pid = str(out.get("id") or out.get("post_id") or "")
        return PostResult(provider_post_id=pid or "unknown", response=out)

    def _post_video(
        self,
        base: str,
        page: str,
        token: str,
        caption: str,
        gcs_uri: str | None,
        raw: bytes | None,
        mime: str,
    ) -> PostResult:
        url = f"{base}/{page}/videos"
        if gcs_uri:
            file_url = _signed_gcs_get_url(gcs_uri)
            data: dict[str, Any] = {"access_token": token, "file_url": file_url}
            if caption:
                data["description"] = caption
            out = _graph_post(url, data=data)
        elif raw:
            ext = "mp4" if "mp4" in mime else "mov" if "mov" in mime else "mp4"
            files = {"source": (f"upload.{ext}", raw, mime or "video/mp4")}
            form: dict[str, Any] = {"access_token": token}
            if caption:
                form["description"] = caption
            out = _graph_post(url, data=form, files=files)
        else:
            raise ValueError("video payload empty")
        pid = str(out.get("id") or "")
        return PostResult(provider_post_id=pid or "unknown", response=out)


class MetaInstagramProvider(SocialProvider):
    name = "meta_instagram"

    def post(
        self,
        channel: str,
        caption: str,
        asset_payload: dict[str, Any],
        brand_id: str,
    ) -> PostResult:
        token = _page_token()
        ig = _ig_user_id()
        if not token or not ig:
            raise RuntimeError("META_PAGE_ACCESS_TOKEN and META_IG_USER_ID are required")
        ver = _graph_version()
        base = f"https://graph.facebook.com/{ver}"

        vids = asset_payload.get("videos")
        if isinstance(vids, list) and vids:
            return self._post_reel(base, ig, token, caption, asset_payload)

        imgs = _images_from_payload(asset_payload)
        if len(imgs) > 1:
            return self._post_carousel(base, ig, token, caption, asset_payload)

        image_url = _public_image_url(asset_payload)
        create_url = f"{base}/{ig}/media"
        params: dict[str, Any] = {
            "access_token": token,
            "image_url": image_url,
        }
        if caption:
            params["caption"] = caption
        cr = _graph_post_params(create_url, params)
        cid = str(cr.get("id") or "")
        if not cid:
            raise RuntimeError(f"instagram media create failed: {cr}")

        pub_url = f"{base}/{ig}/media_publish"
        pub = _graph_post_params(pub_url, {"access_token": token, "creation_id": cid})
        pid = str(pub.get("id") or "")
        return PostResult(provider_post_id=pid or cid, response={"create": cr, "publish": pub})

    def _post_carousel(
        self,
        base: str,
        ig: str,
        token: str,
        caption: str,
        asset_payload: dict[str, Any],
    ) -> PostResult:
        imgs = _images_from_payload(asset_payload)
        if len(imgs) < 2:
            raise ValueError("carousel requires at least two images")
        child_ids: list[str] = []
        create_url = f"{base}/{ig}/media"
        for data, mime in imgs:
            ext = "jpg" if "jpeg" in mime else "png" if "png" in mime else "bin"
            url = _upload_temp_signed_url(data, mime)
            params = {"access_token": token, "image_url": url, "is_carousel_item": "true"}
            cr = _graph_post_params(create_url, params)
            cid = str(cr.get("id") or "")
            if not cid:
                raise RuntimeError(f"instagram carousel child failed: {cr}")
            child_ids.append(cid)

        params2: dict[str, Any] = {
            "access_token": token,
            "media_type": "CAROUSEL",
            "children": ",".join(child_ids),
        }
        if caption:
            params2["caption"] = caption
        cr2 = _graph_post_params(create_url, params2)
        top_id = str(cr2.get("id") or "")
        if not top_id:
            raise RuntimeError(f"instagram carousel container failed: {cr2}")
        pub_url = f"{base}/{ig}/media_publish"
        pub = _graph_post_params(pub_url, {"access_token": token, "creation_id": top_id})
        pid = str(pub.get("id") or "")
        return PostResult(provider_post_id=pid or top_id, response={"container": cr2, "publish": pub})

    def _post_reel(
        self,
        base: str,
        ig: str,
        token: str,
        caption: str,
        asset_payload: dict[str, Any],
    ) -> PostResult:
        gcs_v, bytes_v, mime_v = _video_from_payload(asset_payload)
        if gcs_v:
            video_url = _signed_gcs_get_url(gcs_v)
        elif bytes_v:
            if not _media_bucket():
                raise RuntimeError("META_MEDIA_BUCKET is required for Instagram video from bytes")
            from google.cloud import storage

            ext = "mp4" if "mp4" in mime_v else "mov"
            name = f"meta-ig/{uuid.uuid4().hex}.{ext}"
            b = storage.Client().bucket(_media_bucket())
            blob = b.blob(name)
            blob.upload_from_string(bytes_v, content_type=mime_v or "video/mp4")
            video_url = blob.generate_signed_url(
                expiration=timedelta(minutes=120),
                method="GET",
                version="v4",
            )
        else:
            raise ValueError("instagram reel requires video gcs_uri or bytes_base64")

        create_url = f"{base}/{ig}/media"
        params: dict[str, Any] = {
            "access_token": token,
            "media_type": "REELS",
            "video_url": video_url,
        }
        if caption:
            params["caption"] = caption
        cr = _graph_post_params(create_url, params)
        cid = str(cr.get("id") or "")
        if not cid:
            raise RuntimeError(f"instagram reel create failed: {cr}")
        pub_url = f"{base}/{ig}/media_publish"
        pub = _graph_post_params(pub_url, {"access_token": token, "creation_id": cid})
        pid = str(pub.get("id") or "")
        return PostResult(provider_post_id=pid or cid, response={"create": cr, "publish": pub})

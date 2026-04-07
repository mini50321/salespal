from __future__ import annotations

import base64
import logging
import os
from typing import Any

import requests

log = logging.getLogger(__name__)


def _provider() -> str:
    return (os.getenv("VOICE_STT_PROVIDER") or "deepgram").strip().lower()


def _stt_timeout_seconds() -> int:
    raw = (os.getenv("VOICE_STT_TIMEOUT_SECONDS") or "45").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 45
    return max(5, min(300, n))


def _safe_fetch_audio(url: str) -> tuple[bytes, str]:
    r = requests.get(url, timeout=_stt_timeout_seconds())
    if r.status_code >= 400:
        raise RuntimeError(f"audio fetch failed {r.status_code}")
    ct = (r.headers.get("Content-Type") or "audio/mpeg").split(";")[0].strip().lower()
    return r.content, ct


def _deepgram_transcribe(audio_bytes: bytes, mime_type: str) -> str:
    key = (os.getenv("DEEPGRAM_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("DEEPGRAM_API_KEY required for deepgram STT")
    model = (os.getenv("DEEPGRAM_STT_MODEL") or "nova-2").strip()
    lang = (os.getenv("VOICE_STT_LANGUAGE") or "en").strip()
    url = "https://api.deepgram.com/v1/listen"
    params = {
        "model": model,
        "language": lang,
        "smart_format": "true",
        "punctuate": "true",
        "diarize": "false",
    }
    h = {"Authorization": f"Token {key}", "Content-Type": mime_type or "audio/mpeg"}
    r = requests.post(url, params=params, headers=h, data=audio_bytes, timeout=_stt_timeout_seconds())
    if r.status_code >= 400:
        raise RuntimeError(f"deepgram STT failed {r.status_code}: {r.text[:400]}")
    j = r.json()
    try:
        return str(j["results"]["channels"][0]["alternatives"][0]["transcript"]).strip()
    except Exception as e:
        raise RuntimeError(f"deepgram transcript parse failed: {e}") from e


def _mock_transcribe(audio_bytes: bytes, mime_type: str) -> str:
    # Deterministic fallback for local/dev usage where STT keys are unavailable.
    _ = (audio_bytes, mime_type)
    return (os.getenv("VOICE_STT_MOCK_TEXT") or "").strip() or "hi"


def transcribe_audio_bytes(audio_bytes: bytes, mime_type: str) -> tuple[str, dict[str, Any]]:
    p = _provider()
    if not audio_bytes:
        return "", {"provider": p, "status": "empty_audio"}
    if p == "mock":
        return _mock_transcribe(audio_bytes, mime_type), {"provider": p}
    if p == "deepgram":
        txt = _deepgram_transcribe(audio_bytes, mime_type)
        return txt, {"provider": p}
    if p == "gemini":
        # Placeholder: can be upgraded to Vertex Gemini audio APIs without changing callers.
        return _mock_transcribe(audio_bytes, mime_type), {"provider": "gemini_fallback_mock"}
    return _mock_transcribe(audio_bytes, mime_type), {"provider": "unknown_fallback_mock"}


def transcribe_audio_url(audio_url: str) -> tuple[str, dict[str, Any]]:
    if not audio_url:
        return "", {"provider": _provider(), "status": "no_audio_url"}
    b, ct = _safe_fetch_audio(audio_url)
    txt, meta = transcribe_audio_bytes(b, ct)
    meta2 = dict(meta)
    meta2["audio_url"] = audio_url
    return txt, meta2


def transcribe_audio_base64(audio_b64: str, mime_type: str | None) -> tuple[str, dict[str, Any]]:
    if not audio_b64:
        return "", {"provider": _provider(), "status": "no_audio_base64"}
    raw = audio_b64.strip()
    if raw.startswith("data:"):
        try:
            head, raw = raw.split(",", 1)
            if ";" in head:
                mime_type = (head[5 : head.index(";")] or mime_type or "audio/mpeg").strip()
        except ValueError as e:
            raise ValueError("invalid data URL audio_base64") from e
    try:
        b = base64.b64decode(raw, validate=True)
    except Exception as e:
        raise ValueError("invalid base64 in audio_base64") from e
    return transcribe_audio_bytes(b, (mime_type or "audio/mpeg").strip())


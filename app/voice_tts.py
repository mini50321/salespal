from __future__ import annotations

import base64
import logging
import os
from typing import Any

import requests

log = logging.getLogger(__name__)


def _provider() -> str:
    return (os.getenv("VOICE_TTS_PROVIDER") or "elevenlabs").strip().lower()


def _timeout_seconds() -> int:
    raw = (os.getenv("VOICE_TTS_TIMEOUT_SECONDS") or "45").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 45
    return max(5, min(300, n))


def _elevenlabs_tts(text: str) -> dict[str, Any]:
    key = (os.getenv("ELEVENLABS_API_KEY") or "").strip()
    voice_id = (os.getenv("ELEVENLABS_VOICE_ID") or "").strip()
    if not key or not voice_id:
        raise RuntimeError("ELEVENLABS_API_KEY and ELEVENLABS_VOICE_ID required")
    model_id = (os.getenv("ELEVENLABS_MODEL_ID") or "eleven_turbo_v2_5").strip()
    fmt = (os.getenv("ELEVENLABS_OUTPUT_FORMAT") or "mp3_22050_32").strip()
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    body = {
        "text": text[:3000],
        "model_id": model_id,
        "output_format": fmt,
        "voice_settings": {
            "stability": float((os.getenv("ELEVENLABS_STABILITY") or "0.4").strip()),
            "similarity_boost": float((os.getenv("ELEVENLABS_SIMILARITY_BOOST") or "0.75").strip()),
        },
    }
    headers = {"xi-api-key": key, "Content-Type": "application/json"}
    r = requests.post(url, json=body, headers=headers, timeout=_timeout_seconds())
    if r.status_code >= 400:
        raise RuntimeError(f"elevenlabs TTS failed {r.status_code}: {r.text[:400]}")
    ct = (r.headers.get("Content-Type") or "audio/mpeg").split(";")[0].strip().lower()
    return {
        "provider": "elevenlabs",
        "mime_type": ct,
        "audio_base64": base64.b64encode(r.content).decode("ascii"),
        "text": text,
    }


def synthesize(text: str) -> dict[str, Any]:
    t = (text or "").strip()
    if not t:
        return {"provider": _provider(), "status": "empty_text", "text": ""}
    p = _provider()
    if p == "mock":
        return {"provider": "mock", "status": "text_only", "text": t}
    if p == "elevenlabs":
        try:
            return _elevenlabs_tts(t)
        except Exception:
            log.exception("voice tts elevenlabs")
            return {"provider": "elevenlabs_fallback_text", "status": "text_only", "text": t}
    if p == "gcp":
        # Placeholder: easy swap-in for Google Cloud TTS in a future step.
        return {"provider": "gcp_fallback_text", "status": "text_only", "text": t}
    return {"provider": "unknown_fallback_text", "status": "text_only", "text": t}


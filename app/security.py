from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hmac
import os
from threading import Lock
from typing import Deque

from flask import Request


def _safe_int_env(name: str, default: int, *, lo: int, hi: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        n = int(raw)
    except ValueError:
        return default
    return max(lo, min(hi, n))


def _window_seconds() -> int:
    return _safe_int_env("PUBLIC_RATE_LIMIT_WINDOW_SECONDS", 60, lo=10, hi=3600)


def _limit_for_path(path: str) -> int:
    if path == "/v1/marketing/leads":
        return _safe_int_env("LEADS_RATE_LIMIT_PER_WINDOW", 30, lo=1, hi=5000)
    if path == "/v1/marketing/copy":
        return _safe_int_env("COPY_RATE_LIMIT_PER_WINDOW", 60, lo=1, hi=5000)
    return _safe_int_env("PUBLIC_RATE_LIMIT_PER_WINDOW", 60, lo=1, hi=5000)


def _enabled() -> bool:
    return (os.getenv("PUBLIC_RATE_LIMIT_ENABLED") or "1").strip().lower() not in ("0", "false", "no")


def _trusted_proxy_hops() -> int:
    return _safe_int_env("TRUSTED_PROXY_HOPS", 0, lo=0, hi=10)


def client_ip(req: Request) -> str:
    hops = _trusted_proxy_hops()
    if hops > 0:
        xff = (req.headers.get("X-Forwarded-For") or "").strip()
        if xff:
            parts = [x.strip() for x in xff.split(",") if x.strip()]
            if parts:
                idx = max(0, len(parts) - hops - 1)
                return parts[idx]
    return (req.remote_addr or "unknown").strip() or "unknown"


@dataclass
class _Bucket:
    hits: Deque[datetime]


class PublicRateLimiter:
    """
    In-memory per-instance limiter, good as a first-line throttle.
    For multi-instance strict limits, add Cloud Armor/API Gateway.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._store: dict[str, _Bucket] = {}

    def check(self, req: Request) -> tuple[bool, int | None]:
        if not _enabled():
            return True, None
        key = f"{req.path}|{client_ip(req)}"
        now = datetime.now(timezone.utc)
        win = timedelta(seconds=_window_seconds())
        limit = _limit_for_path(req.path)
        with self._lock:
            bucket = self._store.get(key)
            if bucket is None:
                bucket = _Bucket(hits=deque())
                self._store[key] = bucket
            hits = bucket.hits
            cutoff = now - win
            while hits and hits[0] < cutoff:
                hits.popleft()
            if len(hits) >= limit:
                retry = int((_window_seconds() - (now - hits[0]).total_seconds()) if hits else _window_seconds())
                return False, max(1, retry)
            hits.append(now)
            return True, None


def _extract_bearer(req: Request) -> str:
    auth = (req.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


def verify_admin_api_key(req: Request) -> bool:
    exp = (os.getenv("ADMIN_API_KEY") or "").strip()
    if not exp:
        return True
    got = (req.headers.get("X-Admin-Api-Key") or "").strip() or _extract_bearer(req)
    return bool(got) and hmac.compare_digest(got, exp)


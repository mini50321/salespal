from __future__ import annotations

import hashlib
import hmac
import os

from flask import Request


def verify_scheduler_internal_request(req: Request) -> bool:
    hmac_secret = (os.getenv("SCHEDULER_HMAC_SECRET") or os.getenv("DISPATCH_HMAC_SECRET") or "").strip()
    if hmac_secret:
        sig = (req.headers.get("X-Signature") or "").strip()
        if not sig.startswith("sha256="):
            return False
        got = sig[7:].strip()
        body = req.get_data()
        want = hmac.new(hmac_secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(got, want)
    plain = (
        os.getenv("SCHEDULER_SECRET")
        or os.getenv("DISPATCH_SECRET")
        or os.getenv("INTERNAL_SCHEDULER_SECRET")
        or ""
    ).strip()
    if plain:
        a = (req.headers.get("X-Scheduler-Secret") or req.headers.get("X-Dispatch-Secret") or "").strip()
        if a and hmac.compare_digest(a, plain):
            return True
        auth = req.headers.get("Authorization") or ""
        if auth.startswith("Bearer "):
            tok = auth[7:].strip()
            return bool(tok) and hmac.compare_digest(tok, plain)
        return False
    return True

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_brand(brand_id: str) -> str:
    b = (brand_id or "").strip()
    if not b or len(b) > 64:
        raise ValueError("invalid brand_id")
    return b


def _safe_lead(lead_id: str) -> str:
    l = (lead_id or "").strip()
    if not l or len(l) > 128:
        raise ValueError("invalid lead_id")
    return l


@dataclass
class SaleSession:
    id: str
    brand_id: str
    lead_id: str
    timezone: str
    language: str
    auto_language_switch: bool
    user_type: str
    lead_type: str
    stage: str
    status: str
    next_action_at: str | None
    score: int | None
    created_at: str
    updated_at: str


@dataclass
class SaleEvent:
    id: str
    brand_id: str
    lead_id: str
    event_type: str
    payload: dict[str, Any]
    created_at: str


class SaleOpsStore:
    def __init__(self, path: str):
        self.path = path
        self.sessions: dict[str, SaleSession] = {}
        self.events: dict[str, SaleEvent] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        self.sessions = {k: SaleSession(**v) for k, v in (raw.get("sessions") or {}).items()}
        self.events = {k: SaleEvent(**v) for k, v in (raw.get("events") or {}).items()}

    def _save(self) -> None:
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "sessions": {k: asdict(v) for k, v in self.sessions.items()},
                    "events": {k: asdict(v) for k, v in self.events.items()},
                },
                f,
                ensure_ascii=False,
            )
        os.replace(tmp, self.path)

    def get_session(self, *, brand_id: str, lead_id: str) -> SaleSession | None:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        for s in self.sessions.values():
            if s.brand_id == b and s.lead_id == l:
                return s
        return None

    def upsert_session(
        self,
        *,
        brand_id: str,
        lead_id: str,
        timezone: str | None = None,
        language: str | None = None,
        auto_language_switch: bool | None = None,
        user_type: str | None = None,
        lead_type: str | None = None,
        stage: str | None = None,
        status: str | None = None,
        next_action_at: str | None = None,
        score: int | None = None,
    ) -> SaleSession:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        cur = self.get_session(brand_id=b, lead_id=l)
        now = _utc_iso()
        if cur:
            if timezone is not None:
                cur.timezone = timezone
            if language is not None:
                cur.language = language
            if auto_language_switch is not None:
                cur.auto_language_switch = bool(auto_language_switch)
            if user_type is not None:
                cur.user_type = user_type
            if lead_type is not None:
                cur.lead_type = lead_type
            if stage is not None:
                cur.stage = stage
            if status is not None:
                cur.status = status
            if next_action_at is not None:
                cur.next_action_at = next_action_at
            if score is not None:
                cur.score = max(0, min(10, int(score)))
            cur.updated_at = now
            self.sessions[cur.id] = cur
            self._save()
            return cur
        s = SaleSession(
            id=uuid.uuid4().hex,
            brand_id=b,
            lead_id=l,
            timezone=timezone or "Asia/Kolkata",
            language=language or "hinglish",
            auto_language_switch=bool(True if auto_language_switch is None else auto_language_switch),
            user_type=user_type or "unknown",
            lead_type=lead_type or "unknown",
            stage=stage or "init",
            status=status or "open",
            next_action_at=next_action_at,
            score=max(0, min(10, int(score))) if score is not None else None,
            created_at=now,
            updated_at=now,
        )
        self.sessions[s.id] = s
        self._save()
        return s

    def add_event(self, *, brand_id: str, lead_id: str, event_type: str, payload: dict[str, Any] | None = None) -> SaleEvent:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        e = SaleEvent(
            id=uuid.uuid4().hex,
            brand_id=b,
            lead_id=l,
            event_type=(event_type or "").strip().lower()[:64] or "event",
            payload=payload if isinstance(payload, dict) else {},
            created_at=_utc_iso(),
        )
        self.events[e.id] = e
        self._save()
        return e

    def list_events(self, *, brand_id: str, lead_id: str) -> list[SaleEvent]:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        out = [e for e in self.events.values() if e.brand_id == b and e.lead_id == l]
        out.sort(key=lambda x: x.created_at)
        return out


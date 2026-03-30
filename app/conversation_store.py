from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import os
import uuid
from typing import Any

_MAX_TURNS = 120


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_locale(raw: str | None) -> str:
    s = (raw or "en").strip().lower()
    if s.startswith("hi"):
        return "hi"
    if s.startswith("en"):
        return "en"
    return "en"


@dataclass
class Conversation:
    id: str
    lead_id: str
    brand_id: str
    channel: str
    locale: str
    state: str
    slots: dict[str, Any]
    turns: list[dict[str, Any]]
    metadata: dict[str, Any]
    created_at: str
    updated_at: str


class JsonConversationStore:
    def __init__(self, path: str):
        self.path = path
        self._mem: dict[str, Conversation] = {}
        self._latest: dict[str, str] = {}
        self._load()

    def _latest_key(self, brand_id: str, lead_id: str) -> str:
        return f"{brand_id}|{lead_id}"

    def _load(self) -> None:
        if not os.path.exists(self.path):
            self._mem = {}
            self._latest = {}
            return
        with open(self.path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        self._mem = {k: self._conv_from_dict(v) for k, v in raw.get("conversations", {}).items()}
        self._latest = {k: v for k, v in raw.get("latest", {}).items() if isinstance(k, str) and isinstance(v, str)}

    def _save(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "conversations": {k: self._conv_to_dict(v) for k, v in self._mem.items()},
                    "latest": self._latest,
                },
                f,
                ensure_ascii=False,
            )
        os.replace(tmp, self.path)

    @staticmethod
    def _conv_to_dict(c: Conversation) -> dict[str, Any]:
        return asdict(c)

    @staticmethod
    def _conv_from_dict(d: dict[str, Any]) -> Conversation:
        return Conversation(
            id=d["id"],
            lead_id=d["lead_id"],
            brand_id=d["brand_id"],
            channel=d["channel"],
            locale=d["locale"],
            state=d["state"],
            slots=d.get("slots") or {},
            turns=list(d.get("turns") or []),
            metadata=d.get("metadata") or {},
            created_at=d["created_at"],
            updated_at=d["updated_at"],
        )

    def create(
        self,
        lead_id: str,
        brand_id: str,
        channel: str,
        locale: str | None,
        metadata: dict[str, Any] | None,
    ) -> Conversation:
        lc = _norm_locale(locale)
        ch = (channel or "web").strip().lower()
        if ch not in ("web", "whatsapp", "voice", "sms", "email", "rcs"):
            ch = "web"
        cid = uuid.uuid4().hex
        now = _utcnow()
        from .conversation_engine import opening_turn

        ot = opening_turn(lc, ch)
        t0 = dict(ot["turn"])
        t0["created_at"] = now
        conv = Conversation(
            id=cid,
            lead_id=lead_id,
            brand_id=brand_id,
            channel=ch,
            locale=lc,
            state=ot["state"],
            slots={},
            turns=[t0],
            metadata=dict(metadata) if isinstance(metadata, dict) else {},
            created_at=now,
            updated_at=now,
        )
        self._mem[cid] = conv
        self._latest[self._latest_key(brand_id, lead_id)] = cid
        self._save()
        return conv

    def get(self, conversation_id: str) -> Conversation | None:
        return self._mem.get(conversation_id)

    def get_latest_for_lead(self, brand_id: str, lead_id: str) -> Conversation | None:
        lk = self._latest_key(brand_id, lead_id)
        cid = self._latest.get(lk)
        if cid and cid in self._mem:
            return self._mem[cid]
        for c in self._mem.values():
            if c.brand_id == brand_id and c.lead_id == lead_id:
                return c
        return None

    def update(self, conv: Conversation) -> None:
        conv.updated_at = _utcnow()
        if len(conv.turns) > _MAX_TURNS:
            conv.turns = conv.turns[-_MAX_TURNS :]
        self._mem[conv.id] = conv
        self._latest[self._latest_key(conv.brand_id, conv.lead_id)] = conv.id
        self._save()

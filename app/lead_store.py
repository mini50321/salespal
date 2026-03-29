from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
import re
import uuid
from typing import Any


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_email(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip().lower()
    if not s or "@" not in s:
        return None
    return s


def _norm_phone(s: str | None) -> str | None:
    if not s:
        return None
    digits = re.sub(r"\D+", "", s)
    if len(digits) < 8:
        return None
    return digits


def make_dedupe_key(brand_id: str, email: str | None, phone: str | None) -> str | None:
    e = _norm_email(email)
    p = _norm_phone(phone)
    if e:
        return f"{brand_id}|email|{e}"
    if p:
        return f"{brand_id}|phone|{p}"
    return None


@dataclass
class Lead:
    id: str
    brand_id: str
    source: str
    name: str | None
    email: str | None
    phone: str | None
    company: str | None
    message: str | None
    utm: dict[str, Any] | None
    raw: dict[str, Any]
    status: str
    created_at: str
    updated_at: str
    dedupe_key: str | None = None


class LeadStore:
    def __init__(self, path: str):
        self.path = path
        self._mem: dict[str, Lead] = {}
        self._index: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            self._mem = {}
            self._index = {}
            return
        with open(self.path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        self._mem = {k: Lead(**v) for k, v in raw.get("leads", {}).items()}
        self._index = {k: v for k, v in raw.get("index", {}).items()}

    def _save(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {"leads": {k: asdict(v) for k, v in self._mem.items()}, "index": self._index},
                f,
                ensure_ascii=False,
            )
        os.replace(tmp, self.path)

    def upsert(
        self,
        brand_id: str,
        source: str,
        name: str | None,
        email: str | None,
        phone: str | None,
        company: str | None,
        message: str | None,
        utm: dict[str, Any] | None,
        raw: dict[str, Any],
    ) -> tuple[Lead, bool]:
        dedupe_key = make_dedupe_key(brand_id, email, phone)
        existing_id = self._index.get(dedupe_key) if dedupe_key else None
        if existing_id and existing_id in self._mem:
            lead = self._mem[existing_id]
            lead.source = source or lead.source
            lead.name = name or lead.name
            lead.email = _norm_email(email) or lead.email
            lead.phone = _norm_phone(phone) or lead.phone
            lead.company = company or lead.company
            lead.message = message or lead.message
            lead.utm = utm or lead.utm
            lead.raw = raw or lead.raw
            lead.updated_at = _utcnow()
            self._mem[lead.id] = lead
            self._save()
            return lead, False

        lead_id = uuid.uuid4().hex
        now = _utcnow()
        lead = Lead(
            id=lead_id,
            brand_id=brand_id,
            source=source,
            name=name.strip() if isinstance(name, str) and name.strip() else None,
            email=_norm_email(email),
            phone=_norm_phone(phone),
            company=company.strip() if isinstance(company, str) and company.strip() else None,
            message=message.strip() if isinstance(message, str) and message.strip() else None,
            utm=utm,
            raw=raw,
            status="captured",
            created_at=now,
            updated_at=now,
            dedupe_key=dedupe_key,
        )
        self._mem[lead_id] = lead
        if dedupe_key:
            self._index[dedupe_key] = lead_id
        self._save()
        return lead, True

    def get(self, lead_id: str) -> Lead | None:
        return self._mem.get(lead_id)

    def list(self, brand_id: str | None = None, status: str | None = None) -> list[Lead]:
        leads = list(self._mem.values())
        if brand_id:
            leads = [l for l in leads if l.brand_id == brand_id]
        if status:
            leads = [l for l in leads if l.status == status]
        leads.sort(key=lambda l: l.created_at, reverse=True)
        return leads

    def set_status(self, lead_id: str, status: str) -> Lead | None:
        lead = self._mem.get(lead_id)
        if not lead:
            return None
        lead.status = status
        lead.updated_at = _utcnow()
        self._mem[lead_id] = lead
        self._save()
        return lead

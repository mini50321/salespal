from __future__ import annotations

import os
import json
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
    lid = (lead_id or "").strip()
    if not lid or len(lid) > 128:
        raise ValueError("invalid lead_id")
    return lid


@dataclass
class LifecycleCase:
    id: str
    brand_id: str
    lead_id: str
    stage: str
    status: str
    owner: str
    score: int
    created_at: str
    updated_at: str


@dataclass
class TimelineEvent:
    id: str
    brand_id: str
    lead_id: str
    engine: str
    event_type: str
    payload: dict[str, Any]
    created_at: str


@dataclass
class SupportTicket:
    id: str
    brand_id: str
    lead_id: str
    channel: str
    complaint_id: str
    status: str
    resolution: str | None
    escalation_level: str
    created_at: str
    updated_at: str


@dataclass
class Sp360Task:
    """Task & reminder (M2 feature checklist)."""

    id: str
    brand_id: str
    lead_id: str | None
    title: str
    due_at: str | None
    status: str
    created_at: str


@dataclass
class ActivityLogEntry:
    """User / admin activity (M2 Admin)."""

    id: str
    brand_id: str
    actor: str
    action: str
    meta: dict[str, Any]
    created_at: str


@dataclass
class DocumentRef:
    """Document storage stub (M2)."""

    id: str
    brand_id: str
    lead_id: str | None
    name: str
    kind: str
    created_at: str


@dataclass
class EsignRequest:
    """E-signature stub (M2)."""

    id: str
    brand_id: str
    lead_id: str
    document_name: str
    status: str
    created_at: str


class SalesPal360Store:
    def __init__(self, path: str):
        self.path = path
        self.cases: dict[str, LifecycleCase] = {}
        self.events: dict[str, TimelineEvent] = {}
        self.tickets: dict[str, SupportTicket] = {}
        self.memory: dict[str, dict[str, Any]] = {}
        self.tasks: dict[str, Sp360Task] = {}
        self.activity_logs: dict[str, ActivityLogEntry] = {}
        self.documents: dict[str, DocumentRef] = {}
        self.esign_requests: dict[str, EsignRequest] = {}
        self.platform_settings: dict[str, dict[str, Any]] = {}
        self.crm_config: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        self.cases = {k: LifecycleCase(**v) for k, v in (raw.get("cases") or {}).items()}
        self.events = {k: TimelineEvent(**v) for k, v in (raw.get("events") or {}).items()}
        self.tickets = {k: SupportTicket(**v) for k, v in (raw.get("tickets") or {}).items()}
        self.memory = {
            k: (v if isinstance(v, dict) else {}) for k, v in (raw.get("memory") or {}).items()
        }
        self.tasks = {k: Sp360Task(**v) for k, v in (raw.get("tasks") or {}).items()}
        self.activity_logs = {k: ActivityLogEntry(**v) for k, v in (raw.get("activity_logs") or {}).items()}
        self.documents = {k: DocumentRef(**v) for k, v in (raw.get("documents") or {}).items()}
        self.esign_requests = {k: EsignRequest(**v) for k, v in (raw.get("esign_requests") or {}).items()}
        self.platform_settings = {
            k: (v if isinstance(v, dict) else {}) for k, v in (raw.get("platform_settings") or {}).items()
        }
        self.crm_config = {k: (v if isinstance(v, dict) else {}) for k, v in (raw.get("crm_config") or {}).items()}

    def _save(self) -> None:
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "cases": {k: asdict(v) for k, v in self.cases.items()},
                    "events": {k: asdict(v) for k, v in self.events.items()},
                    "tickets": {k: asdict(v) for k, v in self.tickets.items()},
                    "memory": self.memory,
                    "tasks": {k: asdict(v) for k, v in self.tasks.items()},
                    "activity_logs": {k: asdict(v) for k, v in self.activity_logs.items()},
                    "documents": {k: asdict(v) for k, v in self.documents.items()},
                    "esign_requests": {k: asdict(v) for k, v in self.esign_requests.items()},
                    "platform_settings": self.platform_settings,
                    "crm_config": self.crm_config,
                },
                f,
                ensure_ascii=False,
            )
        os.replace(tmp, self.path)

    def upsert_case(
        self,
        *,
        brand_id: str,
        lead_id: str,
        stage: str,
        status: str,
        owner: str,
        score: int | None = None,
    ) -> LifecycleCase:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        stage_v = (stage or "").strip().lower()
        if stage_v not in {"marketing", "sales", "postsale", "support"}:
            raise ValueError("invalid stage")
        st = (status or "").strip().lower() or "open"
        owner_v = (owner or "").strip() or "ai"
        existing = None
        for c in self.cases.values():
            if c.brand_id == b and c.lead_id == l:
                existing = c
                break
        now = _utc_iso()
        if existing:
            existing.stage = stage_v
            existing.status = st
            existing.owner = owner_v
            if isinstance(score, int):
                existing.score = max(0, min(100, score))
            existing.updated_at = now
            self.cases[existing.id] = existing
            self._save()
            return existing
        c = LifecycleCase(
            id=uuid.uuid4().hex,
            brand_id=b,
            lead_id=l,
            stage=stage_v,
            status=st,
            owner=owner_v,
            score=max(0, min(100, int(score if isinstance(score, int) else 50))),
            created_at=now,
            updated_at=now,
        )
        self.cases[c.id] = c
        self._save()
        return c

    def get_case(self, *, brand_id: str, lead_id: str) -> LifecycleCase | None:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        for c in self.cases.values():
            if c.brand_id == b and c.lead_id == l:
                return c
        return None

    def add_event(
        self,
        *,
        brand_id: str,
        lead_id: str,
        engine: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> TimelineEvent:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        e = TimelineEvent(
            id=uuid.uuid4().hex,
            brand_id=b,
            lead_id=l,
            engine=(engine or "").strip().lower()[:32] or "salespal360",
            event_type=(event_type or "").strip().lower()[:64] or "event",
            payload=payload if isinstance(payload, dict) else {},
            created_at=_utc_iso(),
        )
        self.events[e.id] = e
        self._save()
        return e

    def list_events(self, *, brand_id: str, lead_id: str) -> list[TimelineEvent]:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        out = [e for e in self.events.values() if e.brand_id == b and e.lead_id == l]
        out.sort(key=lambda x: x.created_at)
        return out

    def create_ticket(
        self,
        *,
        brand_id: str,
        lead_id: str,
        channel: str,
        complaint_text: str,
    ) -> SupportTicket:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        now = _utc_iso()
        tid = uuid.uuid4().hex
        t = SupportTicket(
            id=tid,
            brand_id=b,
            lead_id=l,
            channel=(channel or "").strip().lower()[:24] or "whatsapp",
            complaint_id=f"CMP-{tid[:8].upper()}",
            status="open",
            resolution=(complaint_text or "").strip()[:1000] or None,
            escalation_level="ai",
            created_at=now,
            updated_at=now,
        )
        self.tickets[t.id] = t
        self._save()
        return t

    def update_ticket(
        self,
        ticket_id: str,
        *,
        status: str | None = None,
        resolution: str | None = None,
        escalation_level: str | None = None,
    ) -> SupportTicket | None:
        t = self.tickets.get(ticket_id)
        if not t:
            return None
        if status is not None:
            t.status = (status or "").strip().lower()[:24] or t.status
        if resolution is not None:
            t.resolution = (resolution or "").strip()[:1000] or None
        if escalation_level is not None:
            t.escalation_level = (escalation_level or "").strip().lower()[:24] or t.escalation_level
        t.updated_at = _utc_iso()
        self.tickets[t.id] = t
        self._save()
        return t

    def list_tickets(self, *, brand_id: str) -> list[SupportTicket]:
        b = _safe_brand(brand_id)
        out = [t for t in self.tickets.values() if t.brand_id == b]
        out.sort(key=lambda x: x.created_at, reverse=True)
        return out

    def merge_memory(self, *, brand_id: str, lead_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        key = f"{b}:{l}"
        base = dict(self.memory.get(key) or {})
        base.update(patch if isinstance(patch, dict) else {})
        base["updated_at"] = _utc_iso()
        self.memory[key] = base
        self._save()
        return base

    def get_memory(self, *, brand_id: str, lead_id: str) -> dict[str, Any]:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        return dict(self.memory.get(f"{b}:{l}") or {})

    def analytics(self, *, brand_id: str) -> dict[str, Any]:
        b = _safe_brand(brand_id)
        cs = [c for c in self.cases.values() if c.brand_id == b]
        ts = [t for t in self.tickets.values() if t.brand_id == b]
        ev = [e for e in self.events.values() if e.brand_id == b]
        by_stage = {"marketing": 0, "sales": 0, "postsale": 0, "support": 0}
        for c in cs:
            by_stage[c.stage] = by_stage.get(c.stage, 0) + 1
        conversions = len([c for c in cs if c.stage in {"postsale", "support"}])
        closed_tickets = len([t for t in ts if t.status in {"closed", "resolved"}])
        positive_feedback = len(
            [e for e in ev if e.event_type in {"feedback_positive", "testimonial", "referral"}]
        )
        return {
            "cases_total": len(cs),
            "stage_breakdown": by_stage,
            "revenue_conversion_proxy": round((conversions / len(cs) * 100), 2) if cs else 0.0,
            "support_tickets_total": len(ts),
            "support_closed": closed_tickets,
            "positive_feedback_signals": positive_feedback,
            "events_total": len(ev),
        }

    def learning_next_action(self, *, brand_id: str) -> dict[str, Any]:
        a = self.analytics(brand_id=brand_id)
        if a["support_tickets_total"] and a["support_closed"] / max(1, a["support_tickets_total"]) < 0.6:
            action = "Increase human takeover for support escalations and tighten resolution SLA."
        elif a["revenue_conversion_proxy"] < 25:
            action = "Strengthen qualification + follow-up cadence and prioritize warm leads."
        else:
            action = "Scale top performing channels and push referral/testimonial loop."
        return {"recommended_action": action, "generated_at": _utc_iso()}

    # --- Milestone 2 module surfaces (diagram: Salespal / Platform / Admin + checklist) ---

    def log_activity(self, *, brand_id: str, actor: str, action: str, meta: dict[str, Any] | None = None) -> ActivityLogEntry:
        b = _safe_brand(brand_id)
        e = ActivityLogEntry(
            id=uuid.uuid4().hex,
            brand_id=b,
            actor=(actor or "system").strip()[:64] or "system",
            action=(action or "event").strip()[:128] or "event",
            meta=meta if isinstance(meta, dict) else {},
            created_at=_utc_iso(),
        )
        self.activity_logs[e.id] = e
        self._save()
        return e

    def list_activity(self, *, brand_id: str, limit: int = 100) -> list[ActivityLogEntry]:
        b = _safe_brand(brand_id)
        out = [x for x in self.activity_logs.values() if x.brand_id == b]
        out.sort(key=lambda x: x.created_at, reverse=True)
        return out[: max(1, min(500, limit))]

    def create_task(
        self, *, brand_id: str, title: str, lead_id: str | None = None, due_at: str | None = None
    ) -> Sp360Task:
        b = _safe_brand(brand_id)
        if lead_id:
            _safe_lead(lead_id)
        t = Sp360Task(
            id=uuid.uuid4().hex,
            brand_id=b,
            lead_id=lead_id,
            title=(title or "").strip()[:500] or "task",
            due_at=(due_at or "").strip() or None,
            status="open",
            created_at=_utc_iso(),
        )
        self.tasks[t.id] = t
        self._save()
        return t

    def list_tasks(self, *, brand_id: str) -> list[Sp360Task]:
        b = _safe_brand(brand_id)
        out = [x for x in self.tasks.values() if x.brand_id == b]
        out.sort(key=lambda x: x.created_at, reverse=True)
        return out

    def register_document(
        self, *, brand_id: str, name: str, kind: str = "file", lead_id: str | None = None
    ) -> DocumentRef:
        b = _safe_brand(brand_id)
        if lead_id:
            _safe_lead(lead_id)
        d = DocumentRef(
            id=uuid.uuid4().hex,
            brand_id=b,
            lead_id=lead_id,
            name=(name or "").strip()[:240] or "document",
            kind=(kind or "file").strip()[:32],
            created_at=_utc_iso(),
        )
        self.documents[d.id] = d
        self._save()
        return d

    def request_esign(self, *, brand_id: str, lead_id: str, document_name: str) -> EsignRequest:
        b = _safe_brand(brand_id)
        l = _safe_lead(lead_id)
        r = EsignRequest(
            id=uuid.uuid4().hex,
            brand_id=b,
            lead_id=l,
            document_name=(document_name or "").strip()[:240] or "contract",
            status="pending",
            created_at=_utc_iso(),
        )
        self.esign_requests[r.id] = r
        self._save()
        return r

    def get_platform_settings(self, *, brand_id: str) -> dict[str, Any]:
        b = _safe_brand(brand_id)
        return dict(self.platform_settings.get(b) or {"roles_default": ["admin", "agent", "viewer"]})

    def patch_platform_settings(self, *, brand_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        b = _safe_brand(brand_id)
        cur = dict(self.platform_settings.get(b) or {})
        cur.update(patch if isinstance(patch, dict) else {})
        cur["updated_at"] = _utc_iso()
        self.platform_settings[b] = cur
        self._save()
        return cur

    def get_crm_config(self, *, brand_id: str) -> dict[str, Any]:
        b = _safe_brand(brand_id)
        return dict(self.crm_config.get(b) or {"pipeline_stages": ["new", "qualified", "proposal", "won", "lost"]})

    def patch_crm_config(self, *, brand_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        b = _safe_brand(brand_id)
        cur = dict(self.crm_config.get(b) or {})
        cur.update(patch if isinstance(patch, dict) else {})
        cur["updated_at"] = _utc_iso()
        self.crm_config[b] = cur
        self._save()
        return cur

    def m2_core_metrics(self, *, brand_id: str) -> dict[str, Any]:
        b = _safe_brand(brand_id)
        a = self.analytics(brand_id=b)
        tasks_open = len([t for t in self.tasks.values() if t.brand_id == b and t.status == "open"])
        docs_n = len([d for d in self.documents.values() if d.brand_id == b])
        esign_n = len([e for e in self.esign_requests.values() if e.brand_id == b])
        return {
            **a,
            "tasks_open": tasks_open,
            "documents_total": docs_n,
            "esign_requests_total": esign_n,
            "sales_automation_events": len([e for e in self.events.values() if e.brand_id == b]),
        }

    def sales_forecast_stub(self, *, brand_id: str) -> dict[str, Any]:
        """Heuristic forecast from case stages (stub)."""
        b = _safe_brand(brand_id)
        cs = [c for c in self.cases.values() if c.brand_id == b]
        won_proxy = len([c for c in cs if c.stage == "postsale"])
        pipeline = len([c for c in cs if c.stage == "sales"])
        return {
            "brand_id": b,
            "forecast_note": "stub: based on lifecycle cases only",
            "weighted_pipeline_proxy": pipeline * 10 + won_proxy * 25,
            "cases_in_sales_stage": pipeline,
            "cases_in_postsale": won_proxy,
        }


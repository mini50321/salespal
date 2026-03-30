from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import uuid
from typing import Any

from google.cloud import firestore

from .conversation_store import Conversation, _norm_locale as _conv_norm_locale
from .lead_ingest import idempotency_doc_id
from .lead_store import Lead, _norm_email, _norm_phone, _utcnow as _lead_utcnow, make_dedupe_key
from .post_store import PostJob, _parse_dt, _utcnow as _post_utcnow
from .settings import Settings
from .store import AssetJob, _utcnow as _job_utcnow


def _fs_client(settings: Settings) -> firestore.Client:
    project = (settings.gcp_project_id or "").strip()
    if not project:
        raise RuntimeError("GCP_PROJECT_ID required for Firestore stores")
    db_id = (settings.firestore_database_id or "").strip()
    if db_id and db_id.lower() != "(default)":
        return firestore.Client(project=project, database=db_id)
    return firestore.Client(project=project)


def _job_from_doc(d: dict[str, Any]) -> AssetJob:
    return AssetJob(
        id=d["id"],
        brand_id=d["brand_id"],
        asset_type=d["asset_type"],
        prompt=d["prompt"],
        status=d["status"],
        created_at=d["created_at"],
        updated_at=d["updated_at"],
        require_approval=bool(d["require_approval"]),
        output=d.get("output"),
        error=d.get("error"),
    )


def _post_from_doc(d: dict[str, Any]) -> PostJob:
    return PostJob(
        id=d["id"],
        brand_id=d["brand_id"],
        provider=d["provider"],
        channel=d["channel"],
        asset_job_id=d["asset_job_id"],
        caption=d["caption"],
        scheduled_at=d["scheduled_at"],
        status=d["status"],
        attempt_count=int(d["attempt_count"]),
        max_attempts=int(d["max_attempts"]),
        next_attempt_at=d["next_attempt_at"],
        created_at=d["created_at"],
        updated_at=d["updated_at"],
        provider_post_id=d.get("provider_post_id"),
        provider_response=d.get("provider_response"),
        error=d.get("error"),
    )


def _lead_from_doc(d: dict[str, Any]) -> Lead:
    return Lead(
        id=d["id"],
        brand_id=d["brand_id"],
        source=d["source"],
        name=d.get("name"),
        email=d.get("email"),
        phone=d.get("phone"),
        company=d.get("company"),
        message=d.get("message"),
        utm=d.get("utm"),
        raw=d.get("raw") or {},
        status=d["status"],
        created_at=d["created_at"],
        updated_at=d["updated_at"],
        dedupe_key=d.get("dedupe_key"),
    )


class FirestoreJobStore:
    def __init__(self, settings: Settings):
        self._db = _fs_client(settings)
        self._col = self._db.collection(settings.firestore_collection_jobs)

    def create(
        self,
        brand_id: str,
        asset_type: str,
        prompt: str,
        require_approval: bool,
    ) -> AssetJob:
        job_id = uuid.uuid4().hex
        now = _job_utcnow()
        job = AssetJob(
            id=job_id,
            brand_id=brand_id,
            asset_type=asset_type,
            prompt=prompt,
            status="pending",
            created_at=now,
            updated_at=now,
            require_approval=require_approval,
        )
        self._col.document(job_id).set(asdict(job))
        return job

    def get(self, job_id: str) -> AssetJob | None:
        snap = self._col.document(job_id).get()
        if not snap.exists:
            return None
        d = snap.to_dict()
        if not d:
            return None
        return _job_from_doc(d)

    def update(self, job: AssetJob) -> None:
        job.updated_at = _job_utcnow()
        self._col.document(job.id).set(asdict(job))

    def list(self, brand_id: str | None = None) -> list[AssetJob]:
        if brand_id:
            stream = self._col.where("brand_id", "==", brand_id).stream()
        else:
            stream = self._col.stream()
        jobs = [_job_from_doc(doc.to_dict() or {}) for doc in stream if doc.to_dict()]
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs


class FirestorePostStore:
    def __init__(self, settings: Settings):
        self._db = _fs_client(settings)
        self._col = self._db.collection(settings.firestore_collection_posts)

    def create(
        self,
        brand_id: str,
        provider: str,
        channel: str,
        asset_job_id: str,
        caption: str,
        scheduled_at: str,
        max_attempts: int,
    ) -> PostJob:
        post_id = uuid.uuid4().hex
        now = _post_utcnow()
        job = PostJob(
            id=post_id,
            brand_id=brand_id,
            provider=provider,
            channel=channel,
            asset_job_id=asset_job_id,
            caption=caption,
            scheduled_at=scheduled_at,
            status="scheduled",
            attempt_count=0,
            max_attempts=max_attempts,
            next_attempt_at=scheduled_at,
            created_at=now,
            updated_at=now,
        )
        self._col.document(post_id).set(asdict(job))
        return job

    def get(self, post_id: str) -> PostJob | None:
        snap = self._col.document(post_id).get()
        if not snap.exists:
            return None
        d = snap.to_dict()
        if not d:
            return None
        return _post_from_doc(d)

    def update(self, job: PostJob) -> None:
        job.updated_at = _post_utcnow()
        self._col.document(job.id).set(asdict(job))

    def list(self, brand_id: str | None = None) -> list[PostJob]:
        if brand_id:
            stream = self._col.where("brand_id", "==", brand_id).stream()
        else:
            stream = self._col.stream()
        jobs = [_post_from_doc(doc.to_dict() or {}) for doc in stream if doc.to_dict()]
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs

    def due(self, now_iso: str) -> list[PostJob]:
        now = _parse_dt(now_iso)
        stream = self._col.where("status", "in", ["scheduled", "posting", "failed"]).stream()
        out: list[PostJob] = []
        for doc in stream:
            d = doc.to_dict()
            if not d:
                continue
            j = _post_from_doc(d)
            if j.attempt_count >= j.max_attempts:
                continue
            if _parse_dt(j.next_attempt_at) <= now:
                out.append(j)
        out.sort(key=lambda j: j.next_attempt_at)
        return out


class FirestoreLeadStore:
    def __init__(self, settings: Settings):
        self._db = _fs_client(settings)
        self._leads = self._db.collection(settings.firestore_collection_leads)
        self._dedupe = self._db.collection(settings.firestore_collection_lead_dedupe)
        self._idem = self._db.collection(settings.firestore_collection_lead_idem)

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
        idempotency_key: str | None = None,
    ) -> tuple[Lead, bool]:
        dedupe_key = make_dedupe_key(brand_id, email, phone)
        leads_col = self._leads
        dedupe_col = self._dedupe
        idem_col = self._idem

        @firestore.transactional
        def _run(transaction) -> tuple[Lead, bool]:
            if idempotency_key:
                idref = idem_col.document(idempotency_doc_id(brand_id, idempotency_key))
                ids = idref.get(transaction=transaction)
                if ids.exists:
                    idd = ids.to_dict() or {}
                    lid = str(idd.get("lead_id") or "")
                    if lid:
                        lref = leads_col.document(lid)
                        ls = lref.get(transaction=transaction)
                        if ls.exists:
                            return _lead_from_doc(ls.to_dict() or {}), False

            if dedupe_key:
                iref = dedupe_col.document(dedupe_key)
                idx = iref.get(transaction=transaction)
                if idx.exists:
                    data = idx.to_dict() or {}
                    lead_id = str(data.get("lead_id") or "")
                    if lead_id:
                        lref = leads_col.document(lead_id)
                        ls = lref.get(transaction=transaction)
                        if ls.exists:
                            lead = _lead_from_doc(ls.to_dict() or {})
                            lead.source = source or lead.source
                            lead.name = name or lead.name
                            lead.email = _norm_email(email) or lead.email
                            lead.phone = _norm_phone(phone) or lead.phone
                            lead.company = company or lead.company
                            lead.message = message or lead.message
                            lead.utm = utm or lead.utm
                            lead.raw = raw or lead.raw
                            lead.updated_at = _lead_utcnow()
                            transaction.set(lref, asdict(lead))
                            if idempotency_key:
                                transaction.set(
                                    idem_col.document(idempotency_doc_id(brand_id, idempotency_key)),
                                    {"lead_id": lead.id, "brand_id": brand_id},
                                )
                            return lead, False

            lead_id = uuid.uuid4().hex
            now = _lead_utcnow()
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
            transaction.set(leads_col.document(lead_id), asdict(lead))
            if dedupe_key:
                transaction.set(dedupe_col.document(dedupe_key), {"lead_id": lead_id})
            if idempotency_key:
                transaction.set(
                    idem_col.document(idempotency_doc_id(brand_id, idempotency_key)),
                    {"lead_id": lead_id, "brand_id": brand_id},
                )
            return lead, True

        return _run(self._db.transaction())

    def get(self, lead_id: str) -> Lead | None:
        snap = self._leads.document(lead_id).get()
        if not snap.exists:
            return None
        d = snap.to_dict()
        if not d:
            return None
        return _lead_from_doc(d)

    def find_by_brand_and_phone(self, brand_id: str, phone: str | None) -> Lead | None:
        p = _norm_phone(phone)
        if not p:
            return None
        for doc in self._leads.where("brand_id", "==", brand_id).stream():
            d = doc.to_dict()
            if not d:
                continue
            lead = _lead_from_doc(d)
            if lead.phone == p:
                return lead
        return None

    def list(self, brand_id: str | None = None, status: str | None = None) -> list[Lead]:
        if brand_id is not None:
            stream = self._leads.where("brand_id", "==", brand_id).stream()
        elif status is not None:
            stream = self._leads.where("status", "==", status).stream()
        else:
            stream = self._leads.stream()
        leads = [_lead_from_doc(doc.to_dict() or {}) for doc in stream if doc.to_dict()]
        if brand_id is not None and status is not None:
            leads = [l for l in leads if l.status == status]
        leads.sort(key=lambda l: l.created_at, reverse=True)
        return leads

    def set_status(self, lead_id: str, status: str) -> Lead | None:
        ref = self._leads.document(lead_id)
        snap = ref.get()
        if not snap.exists:
            return None
        lead = _lead_from_doc(snap.to_dict() or {})
        lead.status = status
        lead.updated_at = _lead_utcnow()
        ref.set(asdict(lead))
        return lead

    def merge_raw(self, lead_id: str, patch: dict[str, Any]) -> Lead | None:
        ref = self._leads.document(lead_id)
        snap = ref.get()
        if not snap.exists:
            return None
        lead = _lead_from_doc(snap.to_dict() or {})
        base = dict(lead.raw) if isinstance(lead.raw, dict) else {}
        base.update(patch)
        lead.raw = base
        lead.updated_at = _lead_utcnow()
        ref.set(asdict(lead))
        return lead


def _conv_doc_to_model(d: dict[str, Any]) -> Conversation:
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


def _conv_latest_id(brand_id: str, lead_id: str) -> str:
    return hashlib.sha256(f"{brand_id}\n{lead_id}".encode()).hexdigest()


class FirestoreConversationStore:
    def __init__(self, settings: Settings):
        self._db = _fs_client(settings)
        self._col = self._db.collection(settings.firestore_collection_conversations)
        self._latest = self._db.collection(settings.firestore_collection_conv_latest)
        self._max_turns = 120

    def create(
        self,
        lead_id: str,
        brand_id: str,
        channel: str,
        locale: str | None,
        metadata: dict[str, Any] | None,
    ) -> Conversation:
        lc = _conv_norm_locale(locale)
        ch = (channel or "web").strip().lower()
        if ch not in ("web", "whatsapp", "voice", "sms", "email", "rcs"):
            ch = "web"
        cid = uuid.uuid4().hex
        now = _job_utcnow()
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
        self._col.document(cid).set(asdict(conv))
        self._latest.document(_conv_latest_id(brand_id, lead_id)).set(
            {"conversation_id": cid, "brand_id": brand_id, "lead_id": lead_id, "updated_at": now}
        )
        return conv

    def get(self, conversation_id: str) -> Conversation | None:
        snap = self._col.document(conversation_id).get()
        if not snap.exists:
            return None
        d = snap.to_dict()
        if not d:
            return None
        return _conv_doc_to_model(d)

    def get_latest_for_lead(self, brand_id: str, lead_id: str) -> Conversation | None:
        snap = self._latest.document(_conv_latest_id(brand_id, lead_id)).get()
        if not snap.exists:
            return None
        cid = (snap.to_dict() or {}).get("conversation_id")
        if not cid:
            return None
        return self.get(str(cid))

    def update(self, conv: Conversation) -> None:
        conv.updated_at = _job_utcnow()
        if len(conv.turns) > self._max_turns:
            conv.turns = conv.turns[-self._max_turns :]
        self._col.document(conv.id).set(asdict(conv))
        self._latest.document(_conv_latest_id(conv.brand_id, conv.lead_id)).set(
            {
                "conversation_id": conv.id,
                "brand_id": conv.brand_id,
                "lead_id": conv.lead_id,
                "updated_at": conv.updated_at,
            }
        )


def firestore_ready_check(settings: Settings) -> None:
    db = _fs_client(settings)
    col = db.collection(settings.firestore_collection_jobs)
    next(col.limit(1).stream(), None)

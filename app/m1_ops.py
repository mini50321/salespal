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


@dataclass
class SocialConnection:
    id: str
    brand_id: str
    platform: str
    account_handle: str
    connected_at: str
    status: str


@dataclass
class AdCampaign:
    id: str
    brand_id: str
    name: str
    objective: str
    budget_total: float
    allocation: dict[str, float]
    status: str
    created_at: str
    updated_at: str
    notes: str | None = None


@dataclass
class BroadcastCampaign:
    id: str
    brand_id: str
    channel: str
    audience: dict[str, Any]
    schedule_at: str
    status: str
    created_at: str
    updated_at: str
    responses_count: int = 0


@dataclass
class SocialPostJob:
    """Post Now / Schedule / Auto-publish pipeline (stub persistence)."""

    id: str
    brand_id: str
    platform: str
    mode: str  # post_now | schedule | auto_publish
    schedule_at: str | None
    status: str  # queued | posted | scheduled
    created_at: str


@dataclass
class BrandM1Metrics:
    """Engagement tracking + loop counters per brand."""

    brand_id: str
    engagement_events: int = 0
    posts_published: int = 0
    last_engagement_at: str | None = None


class M1OpsStore:
    def __init__(self, path: str):
        self.path = path
        self.social_connections: dict[str, SocialConnection] = {}
        self.ad_campaigns: dict[str, AdCampaign] = {}
        self.broadcast_campaigns: dict[str, BroadcastCampaign] = {}
        self.social_post_jobs: dict[str, SocialPostJob] = {}
        self.brand_metrics: dict[str, BrandM1Metrics] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        self.social_connections = {
            k: SocialConnection(**v) for k, v in (raw.get("social_connections") or {}).items()
        }
        self.ad_campaigns = {k: AdCampaign(**v) for k, v in (raw.get("ad_campaigns") or {}).items()}
        self.broadcast_campaigns = {
            k: BroadcastCampaign(**v) for k, v in (raw.get("broadcast_campaigns") or {}).items()
        }
        self.social_post_jobs = {k: SocialPostJob(**v) for k, v in (raw.get("social_post_jobs") or {}).items()}
        self.brand_metrics = {k: BrandM1Metrics(**v) for k, v in (raw.get("brand_metrics") or {}).items()}

    def _save(self) -> None:
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "social_connections": {k: asdict(v) for k, v in self.social_connections.items()},
                    "ad_campaigns": {k: asdict(v) for k, v in self.ad_campaigns.items()},
                    "broadcast_campaigns": {k: asdict(v) for k, v in self.broadcast_campaigns.items()},
                    "social_post_jobs": {k: asdict(v) for k, v in self.social_post_jobs.items()},
                    "brand_metrics": {k: asdict(v) for k, v in self.brand_metrics.items()},
                },
                f,
                ensure_ascii=False,
            )
        os.replace(tmp, self.path)

    def connect_social(self, *, brand_id: str, platform: str, account_handle: str) -> SocialConnection:
        b = _safe_brand(brand_id)
        p = (platform or "").strip().lower()
        if p not in {"instagram", "facebook", "linkedin", "x", "youtube"}:
            raise ValueError("invalid platform")
        ah = (account_handle or "").strip()
        if not ah or len(ah) > 128:
            raise ValueError("invalid account_handle")
        item = SocialConnection(
            id=uuid.uuid4().hex,
            brand_id=b,
            platform=p,
            account_handle=ah,
            connected_at=_utc_iso(),
            status="connected",
        )
        self.social_connections[item.id] = item
        self._save()
        return item

    def list_social(self, *, brand_id: str) -> list[SocialConnection]:
        b = _safe_brand(brand_id)
        out = [x for x in self.social_connections.values() if x.brand_id == b]
        out.sort(key=lambda x: x.connected_at, reverse=True)
        return out

    def _metrics(self, brand_id: str) -> BrandM1Metrics:
        b = _safe_brand(brand_id)
        if b not in self.brand_metrics:
            self.brand_metrics[b] = BrandM1Metrics(brand_id=b)
        return self.brand_metrics[b]

    def get_brand_metrics(self, *, brand_id: str) -> BrandM1Metrics:
        return self._metrics(brand_id)

    def record_engagement(self, *, brand_id: str, delta: int = 1) -> BrandM1Metrics:
        m = self._metrics(brand_id)
        m.engagement_events = max(0, m.engagement_events + int(delta))
        m.last_engagement_at = _utc_iso()
        self.brand_metrics[m.brand_id] = m
        self._save()
        return m

    def schedule_or_post_social(
        self,
        *,
        brand_id: str,
        platform: str,
        mode: str,
        schedule_at: str | None = None,
    ) -> SocialPostJob:
        b = _safe_brand(brand_id)
        p = (platform or "").strip().lower()
        if p not in {"instagram", "facebook", "linkedin", "x", "youtube"}:
            raise ValueError("invalid platform")
        mo = (mode or "").strip().lower()
        if mo not in {"post_now", "schedule", "auto_publish"}:
            raise ValueError("mode must be post_now|schedule|auto_publish")
        now = _utc_iso()
        if mo == "schedule" and not (schedule_at or "").strip():
            raise ValueError("schedule_at required for schedule mode")
        status = "scheduled" if mo == "schedule" else "posted" if mo == "post_now" else "queued"
        job = SocialPostJob(
            id=uuid.uuid4().hex,
            brand_id=b,
            platform=p,
            mode=mo,
            schedule_at=(schedule_at or "").strip() or None,
            status=status,
            created_at=now,
        )
        self.social_post_jobs[job.id] = job
        m = self._metrics(b)
        if status == "posted":
            m.posts_published += 1
        self.brand_metrics[m.brand_id] = m
        self._save()
        return job

    def list_social_jobs(self, *, brand_id: str) -> list[SocialPostJob]:
        b = _safe_brand(brand_id)
        out = [x for x in self.social_post_jobs.values() if x.brand_id == b]
        out.sort(key=lambda x: x.created_at, reverse=True)
        return out

    def create_ad_campaign(
        self,
        *,
        brand_id: str,
        name: str,
        objective: str,
        budget_total: float,
        allocation: dict[str, float],
        notes: str | None = None,
    ) -> AdCampaign:
        b = _safe_brand(brand_id)
        nm = (name or "").strip() or "Campaign"
        obj = (objective or "").strip() or "awareness"
        if budget_total <= 0:
            raise ValueError("invalid budget_total")
        now = _utc_iso()
        item = AdCampaign(
            id=uuid.uuid4().hex,
            brand_id=b,
            name=nm[:128],
            objective=obj[:64],
            budget_total=float(budget_total),
            allocation=allocation,
            status="draft",
            created_at=now,
            updated_at=now,
            notes=(notes or "").strip()[:800] or None,
        )
        self.ad_campaigns[item.id] = item
        self._save()
        return item

    def update_ad_status(self, campaign_id: str, status: str) -> AdCampaign | None:
        x = self.ad_campaigns.get(campaign_id)
        if not x:
            return None
        x.status = status
        x.updated_at = _utc_iso()
        self.ad_campaigns[campaign_id] = x
        self._save()
        return x

    def list_ads(self, *, brand_id: str) -> list[AdCampaign]:
        b = _safe_brand(brand_id)
        out = [x for x in self.ad_campaigns.values() if x.brand_id == b]
        out.sort(key=lambda x: x.created_at, reverse=True)
        return out

    def create_broadcast(
        self,
        *,
        brand_id: str,
        channel: str,
        audience: dict[str, Any],
        schedule_at: str,
    ) -> BroadcastCampaign:
        b = _safe_brand(brand_id)
        ch = (channel or "").strip().lower()
        if ch not in {"whatsapp", "sms", "email", "rcs"}:
            raise ValueError("invalid channel")
        if not schedule_at:
            schedule_at = _utc_iso()
        now = _utc_iso()
        item = BroadcastCampaign(
            id=uuid.uuid4().hex,
            brand_id=b,
            channel=ch,
            audience=audience if isinstance(audience, dict) else {},
            schedule_at=schedule_at,
            status="scheduled",
            created_at=now,
            updated_at=now,
        )
        self.broadcast_campaigns[item.id] = item
        self._save()
        return item

    def add_broadcast_responses(self, campaign_id: str, count: int) -> BroadcastCampaign | None:
        x = self.broadcast_campaigns.get(campaign_id)
        if not x:
            return None
        x.responses_count = max(0, int(x.responses_count) + max(0, int(count)))
        x.updated_at = _utc_iso()
        self.broadcast_campaigns[campaign_id] = x
        self._save()
        return x

    def list_broadcasts(self, *, brand_id: str) -> list[BroadcastCampaign]:
        b = _safe_brand(brand_id)
        out = [x for x in self.broadcast_campaigns.values() if x.brand_id == b]
        out.sort(key=lambda x: x.created_at, reverse=True)
        return out


def suggest_platforms(*, objective: str, audience: str) -> list[str]:
    o = (objective or "").strip().lower()
    a = (audience or "").strip().lower()
    ranked = ["facebook", "instagram", "youtube", "linkedin", "x"]
    if "b2b" in a or "professional" in a or "enterprise" in o:
        ranked = ["linkedin", "youtube", "x", "facebook", "instagram"]
    elif "local" in o or "whatsapp" in o:
        ranked = ["facebook", "instagram", "youtube", "x", "linkedin"]
    return ranked


def allocate_budget(total: float, objective: str) -> dict[str, float]:
    if total <= 0:
        raise ValueError("invalid total budget")
    o = (objective or "").strip().lower()
    if "lead" in o:
        mix = {"facebook": 0.35, "instagram": 0.25, "linkedin": 0.2, "youtube": 0.15, "x": 0.05}
    elif "awareness" in o:
        mix = {"youtube": 0.35, "instagram": 0.25, "facebook": 0.2, "x": 0.1, "linkedin": 0.1}
    else:
        mix = {"facebook": 0.3, "instagram": 0.25, "youtube": 0.2, "linkedin": 0.15, "x": 0.1}
    out = {k: round(total * v, 2) for k, v in mix.items()}
    rem = round(total - sum(out.values()), 2)
    if rem:
        out["facebook"] = round(out.get("facebook", 0) + rem, 2)
    return out

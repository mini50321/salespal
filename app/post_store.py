from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
import uuid
from typing import Any


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


@dataclass
class PostJob:
    id: str
    brand_id: str
    provider: str
    channel: str
    asset_job_id: str
    caption: str
    scheduled_at: str
    status: str
    attempt_count: int
    max_attempts: int
    next_attempt_at: str
    created_at: str
    updated_at: str
    provider_post_id: str | None = None
    provider_response: dict[str, Any] | None = None
    error: str | None = None


class PostStore:
    def __init__(self, path: str):
        self.path = path
        self._mem: dict[str, PostJob] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            self._mem = {}
            return
        with open(self.path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        self._mem = {k: PostJob(**v) for k, v in raw.items()}

    def _save(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({k: asdict(v) for k, v in self._mem.items()}, f, ensure_ascii=False)
        os.replace(tmp, self.path)

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
        now = _utcnow()
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
        self._mem[post_id] = job
        self._save()
        return job

    def get(self, post_id: str) -> PostJob | None:
        return self._mem.get(post_id)

    def update(self, job: PostJob) -> None:
        job.updated_at = _utcnow()
        self._mem[job.id] = job
        self._save()

    def list(self, brand_id: str | None = None) -> list[PostJob]:
        jobs = list(self._mem.values())
        if brand_id:
            jobs = [j for j in jobs if j.brand_id == brand_id]
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs

    def due(self, now_iso: str) -> list[PostJob]:
        now = _parse_dt(now_iso)
        out = []
        for j in self._mem.values():
            if j.status in ("posted", "cancelled"):
                continue
            if j.attempt_count >= j.max_attempts:
                continue
            if _parse_dt(j.next_attempt_at) <= now:
                out.append(j)
        out.sort(key=lambda j: j.next_attempt_at)
        return out


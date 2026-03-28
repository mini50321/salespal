from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
import uuid
from typing import Any


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class AssetJob:
    id: str
    brand_id: str
    asset_type: str
    prompt: str
    status: str
    created_at: str
    updated_at: str
    require_approval: bool
    output: dict[str, Any] | None = None
    error: str | None = None


class JobStore:
    def __init__(self, path: str):
        self.path = path
        self._mem: dict[str, AssetJob] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            self._mem = {}
            return
        with open(self.path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        self._mem = {k: AssetJob(**v) for k, v in raw.items()}

    def _save(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({k: asdict(v) for k, v in self._mem.items()}, f, ensure_ascii=False)
        os.replace(tmp, self.path)

    def create(
        self,
        brand_id: str,
        asset_type: str,
        prompt: str,
        require_approval: bool,
    ) -> AssetJob:
        job_id = uuid.uuid4().hex
        now = _utcnow()
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
        self._mem[job_id] = job
        self._save()
        return job

    def get(self, job_id: str) -> AssetJob | None:
        return self._mem.get(job_id)

    def update(self, job: AssetJob) -> None:
        job.updated_at = _utcnow()
        self._mem[job.id] = job
        self._save()

    def list(self, brand_id: str | None = None) -> list[AssetJob]:
        jobs = list(self._mem.values())
        if brand_id:
            jobs = [j for j in jobs if j.brand_id == brand_id]
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs

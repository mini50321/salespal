from __future__ import annotations

import json
import logging
import os

log = logging.getLogger(__name__)


def maybe_load_secrets() -> None:
    flag = (os.getenv("LOAD_SECRETS_FROM_SECRET_MANAGER") or "").lower()
    if flag not in ("1", "true", "yes"):
        return
    project = (os.getenv("GCP_PROJECT_ID") or "").strip()
    if not project:
        log.warning("LOAD_SECRETS_FROM_SECRET_MANAGER set but GCP_PROJECT_ID missing")
        return
    raw = os.getenv("SECRET_MANAGER_ENV_MAP") or "{}"
    try:
        mapping = json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning("SECRET_MANAGER_ENV_MAP invalid json: %s", e)
        return
    if not isinstance(mapping, dict) or not mapping:
        return
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    for env_key, secret_id in mapping.items():
        if not isinstance(env_key, str) or not isinstance(secret_id, str):
            continue
        sid = secret_id.strip()
        if not sid:
            continue
        name = f"projects/{project}/secrets/{sid}/versions/latest"
        try:
            resp = client.access_secret_version(request={"name": name})
            val = resp.payload.data.decode("utf-8")
            os.environ[env_key] = val.rstrip("\n\r")
        except Exception as e:
            log.error("secret_manager access failed for %s: %s", env_key, e)
            raise

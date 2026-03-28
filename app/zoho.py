from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
import requests

from .settings import settings


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _dc() -> str:
    return (settings.zoho_dc or "IN").strip().upper()


def _accounts_base() -> str:
    dc = _dc()
    if dc == "COM":
        return "https://accounts.zoho.com"
    if dc == "EU":
        return "https://accounts.zoho.eu"
    if dc == "IN":
        return "https://accounts.zoho.in"
    if dc == "AU":
        return "https://accounts.zoho.com.au"
    if dc == "JP":
        return "https://accounts.zoho.jp"
    return "https://accounts.zoho.in"


def _api_base() -> str:
    dc = _dc()
    if dc == "COM":
        return "https://www.zohoapis.com"
    if dc == "EU":
        return "https://www.zohoapis.eu"
    if dc == "IN":
        return "https://www.zohoapis.in"
    if dc == "AU":
        return "https://www.zohoapis.com.au"
    if dc == "JP":
        return "https://www.zohoapis.jp"
    return "https://www.zohoapis.in"


@dataclass
class Token:
    access_token: str
    expires_at: datetime


class ZohoClient:
    def __init__(self):
        self._token: Token | None = None

    def ready(self) -> bool:
        return bool(settings.zoho_client_id and settings.zoho_client_secret and settings.zoho_refresh_token)

    def _get_token(self) -> str:
        if self._token and self._token.expires_at > _now() + timedelta(seconds=20):
            return self._token.access_token

        if not self.ready():
            raise RuntimeError("zoho credentials missing")

        url = f"{_accounts_base()}/oauth/v2/token"
        resp = requests.post(
            url,
            data={
                "refresh_token": settings.zoho_refresh_token,
                "client_id": settings.zoho_client_id,
                "client_secret": settings.zoho_client_secret,
                "grant_type": "refresh_token",
            },
            timeout=30,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"zoho token error: {resp.status_code} {resp.text}")

        data = resp.json()
        access = data.get("access_token")
        if not access:
            raise RuntimeError("zoho token missing access_token")
        expires_in = int(data.get("expires_in", 3600))
        self._token = Token(access_token=access, expires_at=_now() + timedelta(seconds=expires_in))
        return access

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Zoho-oauthtoken {self._get_token()}"}

    def create_lead(self, lead_payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{_api_base()}/crm/v2/Leads"
        resp = requests.post(url, json={"data": [lead_payload]}, headers=self._headers(), timeout=30)
        if resp.status_code >= 400:
            raise RuntimeError(f"zoho create lead error: {resp.status_code} {resp.text}")
        return resp.json()


def map_lead_to_zoho(lead: Any) -> dict[str, Any]:
    name = (lead.name or "").strip() if getattr(lead, "name", None) else ""
    company = (lead.company or "").strip() if getattr(lead, "company", None) else ""
    last_name = name or company or "Lead"

    payload: dict[str, Any] = {
        "Last_Name": last_name,
        "Company": company or "Unknown",
    }

    email = getattr(lead, "email", None)
    phone = getattr(lead, "phone", None)
    message = getattr(lead, "message", None)
    source = getattr(lead, "source", None)

    if email:
        payload["Email"] = email
    if phone:
        payload["Phone"] = phone
    if message:
        payload["Description"] = message
    if source:
        payload["Lead_Source"] = source

    brand_id = getattr(lead, "brand_id", None)
    if brand_id:
        payload["Description"] = (payload.get("Description", "") + f"\nbrand_id={brand_id}").strip()

    raw = getattr(lead, "raw", None)
    if isinstance(raw, dict) and isinstance(raw.get("zoho"), dict):
        for k, v in raw["zoho"].items():
            payload[k] = v

    owner_id = settings.zoho_owner_id
    if owner_id:
        payload["Owner"] = {"id": owner_id}

    return payload


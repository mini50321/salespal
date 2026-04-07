"""
SalesPal sales flow metadata stored on lead.raw:
- user_type: abusive | timepass | genuine | unknown
- lead_temperature: hot | warm | cold | unknown
- escalation: none | senior_ai | human | resolved
- feedback_score: 1-10 optional
"""

from __future__ import annotations

from typing import Any

VALID_USER_TYPES = frozenset({"abusive", "timepass", "genuine", "unknown"})
VALID_TEMPERATURES = frozenset({"hot", "warm", "cold", "unknown"})


def merge_classification(
    raw: dict[str, Any],
    *,
    user_type: str | None = None,
    lead_temperature: str | None = None,
    escalation: str | None = None,
    feedback_score: int | None = None,
) -> dict[str, Any]:
    out = dict(raw)
    if user_type is not None:
        u = user_type.strip().lower()
        if u in VALID_USER_TYPES:
            out["sales_user_type"] = u
    if lead_temperature is not None:
        t = lead_temperature.strip().lower()
        if t in VALID_TEMPERATURES:
            out["sales_lead_temperature"] = t
    if escalation is not None:
        out["sales_escalation"] = str(escalation)[:64]
    if feedback_score is not None:
        try:
            fs = int(feedback_score)
            if 1 <= fs <= 10:
                out["sales_feedback_score"] = fs
        except (TypeError, ValueError):
            pass
    return out


def feedback_branch(score: int | None) -> str:
    if score is None:
        return "unknown"
    if score >= 8:
        return "positive_referral"
    if score >= 5:
        return "neutral_soft_referral"
    return "negative_ai_resolve"

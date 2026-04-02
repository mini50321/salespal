from __future__ import annotations

import json
import logging
import os
from typing import Any

from .settings import settings

log = logging.getLogger(__name__)

_MODEL: Any = None
_INIT_KEY: tuple[str, str, str] | None = None


def _reply_backend_vertex() -> bool:
    return (getattr(settings, "conversation_reply_backend", "") or "rules").strip().lower() == "vertex"


def _get_model():
    global _MODEL, _INIT_KEY
    project = (settings.gcp_project_id or "").strip()
    region = (settings.gcp_region or "asia-south1").strip()
    if not project:
        return None
    model_name = (os.getenv("VERTEX_CHAT_MODEL") or "gemini-2.5-flash").strip()
    key = (project, region, model_name)
    if _MODEL is not None and _INIT_KEY == key:
        return _MODEL
    try:
        import vertexai
        from vertexai.generative_models import GenerativeModel

        vertexai.init(project=project, location=region)
        sys_txt = (
            "You rewrite assistant messages for a B2B sales qualification bot named SalesPal. "
            "Output only the final message text for the user—no quotes, no markdown, no prefixes. "
            "Preserve the template's intent: ask exactly what it asks, add no extra questions, "
            "no unrelated offers, no specific price promises."
        )
        _MODEL = GenerativeModel(model_name, system_instruction=sys_txt)
        _INIT_KEY = key
        return _MODEL
    except Exception:
        log.exception("vertex chat model init")
        return None


def polish_assistant_reply(
    *,
    locale: str,
    template_reply: str,
    user_message: str | None,
    conversation_state: str,
    filled_slots: dict[str, Any],
) -> str | None:
    """
    Paraphrase a rule-based template in natural language (en / hi / …) using Vertex Gemini.
    Returns None to fall back to the template on misconfig or errors.
    """
    if not _reply_backend_vertex():
        return None
    t = (template_reply or "").strip()
    if not t:
        return None
    model = _get_model()
    if model is None:
        return None

    um = (user_message or "").strip() or "(no user message yet — this is an opening line)"
    slots_json = json.dumps(filled_slots or {}, ensure_ascii=False)
    loc = (locale or "en").strip().lower()
    prompt = (
        f"Target locale: {loc}. If locale starts with 'hi', write in natural Hindi (Devanagari). "
        f"If 'en', write in clear English.\n"
        f"Rewrite briefly (at most 3 short sentences).\n\n"
        f"Template to paraphrase faithfully:\n{t}\n\n"
        f"User's last message:\n{um}\n\n"
        f"Slots collected so far (JSON): {slots_json}\n"
        f"Bot state after this turn: {conversation_state}\n"
    )

    try:
        from vertexai.generative_models import GenerationConfig

        cfg = GenerationConfig(
            max_output_tokens=int((os.getenv("VERTEX_CHAT_MAX_TOKENS") or "256").strip() or "256"),
            temperature=float((os.getenv("VERTEX_CHAT_TEMPERATURE") or "0.35").strip() or "0.35"),
        )
        resp = model.generate_content(prompt, generation_config=cfg)
        if not resp.candidates:
            return None
        parts = resp.candidates[0].content.parts
        if not parts:
            return None
        out = "".join(getattr(p, "text", "") or "" for p in parts).strip()
        return out if out else None
    except Exception:
        log.exception("vertex chat generate")
        return None

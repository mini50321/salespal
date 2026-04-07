from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .conversation_store import Conversation
from .settings import settings


MESSAGES: dict[str, dict[str, str]] = {
    "hing": {
        "welcome": "Namaste! Team aapki help ke liye kuch short sawal pooch rahi hai.",
        "ask_budget": "Aap roughly kaunsa budget soch rahe ho is project ke liye?",
        "ask_location": "Aap kaunse city / region se ho?",
        "ask_timeline": "Aap kitne time mein decision lena chahte ho?",
        "done": "Dhanyavaad! Team jaldi next steps ke saath connect karegi.",
        "already_done": "Ye details pehle hi save ho chuki hain. Zarurat ho to team call karegi.",
        "budget_clarify": "Please budget ek amount ya range mein likho — jaise 5 lakh ya 50000 INR.",
    },
    "en": {
        "welcome": "Hello. I will ask a few short questions to help our team assist you.",
        "ask_budget": "What budget range are you considering for this?",
        "ask_location": "Which city or region should we use?",
        "ask_timeline": "What is your expected timeline to move forward?",
        "done": "Thank you. Our team will follow up with the next steps shortly.",
        "already_done": "This qualification is already complete. Our team will reach out if needed.",
        "budget_clarify": "Please share your budget as an amount or range (for example: 5 lakh or 50000 INR).",
    },
    "hi": {
        "welcome": "नमस्ते। हमारी टीम आपकी मदद के लिए कुछ छोटे सवाल पूछेगी।",
        "ask_budget": "आप किस बजट रेंज के बारे में सोच रहे हैं?",
        "ask_location": "आप किस शहर या क्षेत्र में हैं?",
        "ask_timeline": "आप किस समय सीमा में आगे बढ़ना चाहते हैं?",
        "done": "धन्यवाद। हमारी टीम जल्द ही अगले कदम के साथ संपर्क करेगी।",
        "already_done": "यह जानकारी पहले ही पूरी हो चुकी है। जरूरत होगी तो टीम संपर्क करेगी।",
        "budget_clarify": "कृपया अपना बजट राशि या रेंज में लिखें (जैसे: 5 लाख या 50000 INR)।",
    },
}


def _is_greeting(msg: str) -> bool:
    t = msg.lower().strip().rstrip("!.")
    if not t:
        return True
    if t in {"hi", "hello", "hey", "namaste", "namaskar", "hlo", "hii", "hiii"}:
        return True
    if t in {"नमस्ते", "नमस्कार"}:
        return True
    return len(t) <= 2 and t.isalpha()


def _t(locale: str, key: str) -> str:
    pack = MESSAGES.get(locale) or MESSAGES["hing"] or MESSAGES["en"]
    return pack.get(key) or MESSAGES["en"][key]


def suggest_locale_from_message(msg: str, *, default: str = "hing") -> str:
    """
    Light heuristic: Devanagari-heavy → hi, Latin-heavy → en, else Hinglish default.
    """
    s = (msg or "").strip()
    if not s:
        return default if default in MESSAGES else "hing"
    dev = sum(1 for c in s if "\u0900" <= c <= "\u097f")
    lat = sum(1 for c in s if ("a" <= c.lower() <= "z"))
    if dev >= 3 and dev >= lat:
        return "hi"
    if lat >= 10 and lat > dev * 2:
        return "en"
    return "hing" if "hing" in MESSAGES else default


def _polish_reply(
    *,
    locale: str,
    template_reply: str,
    user_message: str | None,
    conversation_state: str,
    filled_slots: dict[str, Any],
) -> str:
    if (settings.conversation_reply_backend or "rules").strip().lower() != "vertex":
        return template_reply
    try:
        from .vertex_chat import polish_assistant_reply

        out = polish_assistant_reply(
            locale=locale,
            template_reply=template_reply,
            user_message=user_message,
            conversation_state=conversation_state,
            filled_slots=filled_slots,
        )
        if out and out.strip():
            return out.strip()
    except Exception:
        pass
    return template_reply


def opening_turn(locale: str, channel: str) -> dict[str, Any]:
    lc = locale if locale in MESSAGES else ("hing" if "hing" in MESSAGES else "en")
    text = f"{_t(lc, 'welcome')} {_t(lc, 'ask_budget')}"
    text = _polish_reply(
        locale=lc,
        template_reply=text,
        user_message=None,
        conversation_state="ask_budget",
        filled_slots={},
    )
    return {
        "state": "ask_budget",
        "turn": {"role": "assistant", "content": text, "channel": channel},
    }


def process_user_message(conv: Conversation, text: str) -> tuple[Conversation, str]:
    msg = (text or "").strip()
    if conv.locale in ("hing", "en", "hi") and msg and conv.state != "complete":
        sl = suggest_locale_from_message(msg, default=conv.locale if conv.locale in MESSAGES else "hing")
        if sl in MESSAGES and sl != conv.locale:
            conv.locale = sl
    lc = conv.locale if conv.locale in MESSAGES else "hing"

    if not msg:
        reply = _t(conv.locale, "ask_budget") if conv.state != "complete" else _t(conv.locale, "already_done")
        reply = _polish_reply(
            locale=lc,
            template_reply=reply,
            user_message=None,
            conversation_state=conv.state,
            filled_slots=dict(conv.slots),
        )
        return conv, reply

    if conv.state == "complete":
        reply = _t(lc, "already_done")
        reply = _polish_reply(
            locale=lc,
            template_reply=reply,
            user_message=msg,
            conversation_state=conv.state,
            filled_slots=dict(conv.slots),
        )
        return conv, reply

    turns = list(conv.turns)
    ts_u = datetime.now(timezone.utc).isoformat()
    turns.append({"role": "user", "content": msg, "channel": conv.channel, "created_at": ts_u})

    if conv.state == "ask_budget":
        if _is_greeting(msg):
            reply = _polish_reply(
                locale=lc,
                template_reply=_t(lc, "budget_clarify"),
                user_message=msg,
                conversation_state="ask_budget",
                filled_slots=dict(conv.slots),
            )
            ts_a = datetime.now(timezone.utc).isoformat()
            turns.append({"role": "assistant", "content": reply, "channel": conv.channel, "created_at": ts_a})
            conv.turns = turns
            return conv, reply
        conv.slots = dict(conv.slots)
        conv.slots["budget"] = msg
        conv.state = "ask_location"
        reply = _polish_reply(
            locale=lc,
            template_reply=_t(lc, "ask_location"),
            user_message=msg,
            conversation_state=conv.state,
            filled_slots=dict(conv.slots),
        )
    elif conv.state == "ask_location":
        conv.slots = dict(conv.slots)
        conv.slots["location"] = msg
        conv.state = "ask_timeline"
        reply = _polish_reply(
            locale=lc,
            template_reply=_t(lc, "ask_timeline"),
            user_message=msg,
            conversation_state=conv.state,
            filled_slots=dict(conv.slots),
        )
    elif conv.state == "ask_timeline":
        conv.slots = dict(conv.slots)
        conv.slots["timeline"] = msg
        conv.state = "complete"
        reply = _polish_reply(
            locale=lc,
            template_reply=_t(lc, "done"),
            user_message=msg,
            conversation_state=conv.state,
            filled_slots=dict(conv.slots),
        )
    else:
        conv.state = "ask_budget"
        reply = _polish_reply(
            locale=lc,
            template_reply=_t(lc, "ask_budget"),
            user_message=msg,
            conversation_state=conv.state,
            filled_slots=dict(conv.slots),
        )

    ts_a = datetime.now(timezone.utc).isoformat()
    turns.append({"role": "assistant", "content": reply, "channel": conv.channel, "created_at": ts_a})
    conv.turns = turns
    return conv, reply


def conversation_to_qualification_dict(conv: Conversation) -> dict[str, Any]:
    return {
        "lead_id": conv.lead_id,
        "brand_id": conv.brand_id,
        "channel": conv.channel,
        "locale": conv.locale,
        "state": conv.state,
        "budget": conv.slots.get("budget"),
        "location": conv.slots.get("location"),
        "timeline": conv.slots.get("timeline"),
        "complete": conv.state == "complete",
    }

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .conversation_store import Conversation


MESSAGES: dict[str, dict[str, str]] = {
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
    pack = MESSAGES.get(locale) or MESSAGES["en"]
    return pack.get(key) or MESSAGES["en"][key]


def opening_turn(locale: str, channel: str) -> dict[str, Any]:
    lc = locale if locale in MESSAGES else "en"
    text = f"{_t(lc, 'welcome')} {_t(lc, 'ask_budget')}"
    return {
        "state": "ask_budget",
        "turn": {"role": "assistant", "content": text, "channel": channel},
    }


def process_user_message(conv: Conversation, text: str) -> tuple[Conversation, str]:
    msg = (text or "").strip()
    if not msg:
        return conv, _t(conv.locale, "ask_budget") if conv.state != "complete" else _t(conv.locale, "already_done")

    lc = conv.locale if conv.locale in MESSAGES else "en"

    if conv.state == "complete":
        return conv, _t(lc, "already_done")

    turns = list(conv.turns)
    ts_u = datetime.now(timezone.utc).isoformat()
    turns.append({"role": "user", "content": msg, "channel": conv.channel, "created_at": ts_u})

    if conv.state == "ask_budget":
        if _is_greeting(msg):
            reply = _t(lc, "budget_clarify")
            ts_a = datetime.now(timezone.utc).isoformat()
            turns.append({"role": "assistant", "content": reply, "channel": conv.channel, "created_at": ts_a})
            conv.turns = turns
            return conv, reply
        conv.slots = dict(conv.slots)
        conv.slots["budget"] = msg
        conv.state = "ask_location"
        reply = _t(lc, "ask_location")
    elif conv.state == "ask_location":
        conv.slots = dict(conv.slots)
        conv.slots["location"] = msg
        conv.state = "ask_timeline"
        reply = _t(lc, "ask_timeline")
    elif conv.state == "ask_timeline":
        conv.slots = dict(conv.slots)
        conv.slots["timeline"] = msg
        conv.state = "complete"
        reply = _t(lc, "done")
    else:
        conv.state = "ask_budget"
        reply = _t(lc, "ask_budget")

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

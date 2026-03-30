from __future__ import annotations

from .settings import Settings


def build_stores(settings: Settings):
    b = (settings.store_backend or "json").strip().lower()
    if b == "firestore":
        from .firestore_stores import FirestoreJobStore, FirestoreLeadStore, FirestorePostStore

        return (
            FirestoreJobStore(settings),
            FirestorePostStore(settings),
            FirestoreLeadStore(settings),
        )
    from .lead_store import LeadStore
    from .post_store import PostStore
    from .store import JobStore

    return (
        JobStore(settings.job_store_path),
        PostStore(settings.post_store_path),
        LeadStore(settings.lead_store_path),
    )


def build_conversation_store(settings: Settings):
    b = (settings.store_backend or "json").strip().lower()
    if b == "firestore":
        from .firestore_stores import FirestoreConversationStore

        return FirestoreConversationStore(settings)
    from .conversation_store import JsonConversationStore

    return JsonConversationStore(settings.conversation_store_path)

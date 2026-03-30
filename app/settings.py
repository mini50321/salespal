from pydantic import BaseModel
import os

_fs_prefix = ((os.getenv("FIRESTORE_COLLECTION_PREFIX") or "salespal").strip().rstrip("_") or "salespal")


class Settings(BaseModel):
    gcp_project_id: str | None = os.getenv("GCP_PROJECT_ID")
    gcp_region: str = os.getenv("GCP_REGION", "asia-south1")
    generator_backend: str = os.getenv("GENERATOR_BACKEND", "mock")
    store_backend: str = os.getenv("STORE_BACKEND", "json")
    firestore_database_id: str | None = (os.getenv("FIRESTORE_DATABASE") or "").strip() or None
    firestore_collection_jobs: str = os.getenv("FIRESTORE_COLLECTION_JOBS") or f"{_fs_prefix}_jobs"
    firestore_collection_posts: str = os.getenv("FIRESTORE_COLLECTION_POSTS") or f"{_fs_prefix}_posts"
    firestore_collection_leads: str = os.getenv("FIRESTORE_COLLECTION_LEADS") or f"{_fs_prefix}_leads"
    firestore_collection_lead_dedupe: str = (
        os.getenv("FIRESTORE_COLLECTION_LEAD_DEDUPE") or f"{_fs_prefix}_lead_dedupe"
    )
    firestore_collection_lead_idem: str = (
        os.getenv("FIRESTORE_COLLECTION_LEAD_IDEM") or f"{_fs_prefix}_lead_idem"
    )
    firestore_collection_conversations: str = (
        os.getenv("FIRESTORE_COLLECTION_CONVERSATIONS") or f"{_fs_prefix}_conversations"
    )
    firestore_collection_conv_latest: str = (
        os.getenv("FIRESTORE_COLLECTION_CONV_LATEST") or f"{_fs_prefix}_conv_latest"
    )
    conversation_store_path: str = os.getenv("CONVERSATION_STORE_PATH", "conversation_store.json")
    job_store_path: str = os.getenv("JOB_STORE_PATH", "job_store.json")
    post_store_path: str = os.getenv("POST_STORE_PATH", "post_store.json")
    lead_store_path: str = os.getenv("LEAD_STORE_PATH", "lead_store.json")
    zoho_dc: str = os.getenv("ZOHO_DC", "IN")
    zoho_client_id: str | None = os.getenv("ZOHO_CLIENT_ID")
    zoho_client_secret: str | None = os.getenv("ZOHO_CLIENT_SECRET")
    zoho_refresh_token: str | None = os.getenv("ZOHO_REFRESH_TOKEN")
    zoho_owner_id: str | None = os.getenv("ZOHO_OWNER_ID")


settings = Settings()

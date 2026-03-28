from pydantic import BaseModel
import os


class Settings(BaseModel):
    gcp_project_id: str | None = os.getenv("GCP_PROJECT_ID")
    gcp_region: str = os.getenv("GCP_REGION", "asia-south1")
    generator_backend: str = os.getenv("GENERATOR_BACKEND", "mock")
    job_store_path: str = os.getenv("JOB_STORE_PATH", "job_store.json")
    post_store_path: str = os.getenv("POST_STORE_PATH", "post_store.json")
    lead_store_path: str = os.getenv("LEAD_STORE_PATH", "lead_store.json")
    zoho_dc: str = os.getenv("ZOHO_DC", "IN")
    zoho_client_id: str | None = os.getenv("ZOHO_CLIENT_ID")
    zoho_client_secret: str | None = os.getenv("ZOHO_CLIENT_SECRET")
    zoho_refresh_token: str | None = os.getenv("ZOHO_REFRESH_TOKEN")
    zoho_owner_id: str | None = os.getenv("ZOHO_OWNER_ID")


settings = Settings()

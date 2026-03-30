# SalesPal (WhatsApp + Zoho + Vertex) — Cloud Run Deploy

This repo is already set up to run on **Google Cloud Run**.

## Quick deploy (Windows / PowerShell)

1) Open PowerShell in the repo root:

```powershell
cd "D:\my project\whatsapp dev"
```

2) Login to Google Cloud:

```powershell
gcloud auth login
```

3) Deploy (builds container + deploys to Cloud Run):

```powershell
.\scripts\deploy-cloudrun.ps1 -ProjectId "YOUR_PROJECT_ID" -ServiceName "salespal-api" -Region "asia-south1"
```

It will print the **Cloud Run URL** at the end.

## After deploy (configure integrations)

### Vertex (Marketing assets + optional chat polish)

Set these env vars on the Cloud Run service:

- `GENERATOR_BACKEND=vertex` (for image/carousel/video generation)
- `CONVERSATION_REPLY_BACKEND=vertex` (optional: paraphrase bot replies via Gemini)
- `GCP_PROJECT_ID` and `GCP_REGION` (already set by deploy script)

### Firestore (recommended for production)

Set:

- `STORE_BACKEND=firestore`

### Zoho (lead push + qualification sync)

Use:

```powershell
.\scripts\step1-6-set-zoho-secrets.ps1 `
  -ProjectId "YOUR_PROJECT_ID" `
  -Region "asia-south1" `
  -ServiceName "salespal-api" `
  -ZohoDc "IN" `
  -ZohoClientId "..." `
  -ZohoClientSecret "..." `
  -ZohoRefreshToken "..."
```

### WhatsApp

Set on Cloud Run:

- `WHATSAPP_VERIFY_TOKEN`
- `WHATSAPP_ACCESS_TOKEN`
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_APP_SECRET` (optional but recommended)
- `WHATSAPP_DEFAULT_BRAND_ID` and/or `WHATSAPP_NUMBER_BRAND_MAP`

Webhook path:

- `GET/POST /v1/webhooks/whatsapp`

### Public web chat (embed on salespal.in)

Set on Cloud Run:

- `PUBLIC_CHAT_ENABLED=1`
- `PUBLIC_CHAT_BRAND_IDS=brand1,brand2`
- `PUBLIC_CHAT_CORS_ORIGINS=https://www.salespal.in,https://salespal.in`
- `PUBLIC_CHAT_API_KEY=...` (recommended)

Routes:

- `POST /v1/public/chat/start`
- `POST /v1/public/chat/message`

## Notes

- Do **not** paste passwords/tokens in chat. Prefer Secret Manager or `gcloud run services update --set-env-vars ...`.
- If you use the JSON file stores locally on Windows, you can hit file-lock errors. For production use `STORE_BACKEND=firestore`.


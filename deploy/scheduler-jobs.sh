#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
REGION="${REGION:-asia-south1}"
SCHEDULER_LOCATION="${SCHEDULER_LOCATION:-asia-south1}"
SERVICE_URL="${SERVICE_URL:?set SERVICE_URL to Cloud Run HTTPS URL}"
SCHEDULER_SECRET="${SCHEDULER_SECRET:?set SCHEDULER_SECRET same as Cloud Run env}"
ADMIN_API_KEY="${ADMIN_API_KEY:?set ADMIN_API_KEY same as Cloud Run env}"

DISPATCH_SCHEDULE="${DISPATCH_SCHEDULE:-*/5 * * * *}"
ZOHO_CRON_SCHEDULE="${ZOHO_CRON_SCHEDULE:-*/10 * * * *}"
WA_OUTREACH_SCHEDULE="${WA_OUTREACH_SCHEDULE:-*/15 * * * *}"
VOICE_OUTREACH_SCHEDULE="${VOICE_OUTREACH_SCHEDULE:-*/15 * * * *}"
WA_NURTURE_SCHEDULE="${WA_NURTURE_SCHEDULE:-*/15 * * * *}"
COLD_CAMPAIGN_SCHEDULE="${COLD_CAMPAIGN_SCHEDULE:-*/30 * * * *}"
DISPATCH_JOB="${DISPATCH_JOB:-salespal-dispatch-posts}"
ZOHO_JOB="${ZOHO_JOB:-salespal-cron-zoho-push}"
WA_OUTREACH_JOB="${WA_OUTREACH_JOB:-salespal-cron-whatsapp-outreach}"
VOICE_OUTREACH_JOB="${VOICE_OUTREACH_JOB:-salespal-cron-voice-outreach}"
WA_NURTURE_JOB="${WA_NURTURE_JOB:-salespal-cron-whatsapp-nurture}"
COLD_CAMPAIGN_JOB="${COLD_CAMPAIGN_JOB:-salespal-cron-cold-campaign}"

gcloud config set project "${PROJECT_ID}"

gcloud scheduler jobs delete "${DISPATCH_JOB}" --location="${SCHEDULER_LOCATION}" --quiet 2>/dev/null || true
gcloud scheduler jobs create http "${DISPATCH_JOB}" \
  --location="${SCHEDULER_LOCATION}" \
  --schedule="${DISPATCH_SCHEDULE}" \
  --uri="${SERVICE_URL}/v1/marketing/posts/dispatch" \
  --http-method=POST \
  --headers="Content-Type=application/json,X-Scheduler-Secret=${SCHEDULER_SECRET},X-Admin-Api-Key=${ADMIN_API_KEY}" \
  --message-body="{}" \
  --attempt-deadline=540s \
  --time-zone="${CRON_TZ:-UTC}"

gcloud scheduler jobs delete "${ZOHO_JOB}" --location="${SCHEDULER_LOCATION}" --quiet 2>/dev/null || true
gcloud scheduler jobs create http "${ZOHO_JOB}" \
  --location="${SCHEDULER_LOCATION}" \
  --schedule="${ZOHO_CRON_SCHEDULE}" \
  --uri="${SERVICE_URL}/v1/cron/zoho_push_leads" \
  --http-method=POST \
  --headers="Content-Type=application/json,X-Scheduler-Secret=${SCHEDULER_SECRET},X-Admin-Api-Key=${ADMIN_API_KEY}" \
  --message-body="{\"limit\":50}" \
  --attempt-deadline=540s \
  --time-zone="${CRON_TZ:-UTC}"

gcloud scheduler jobs delete "${WA_OUTREACH_JOB}" --location="${SCHEDULER_LOCATION}" --quiet 2>/dev/null || true
gcloud scheduler jobs create http "${WA_OUTREACH_JOB}" \
  --location="${SCHEDULER_LOCATION}" \
  --schedule="${WA_OUTREACH_SCHEDULE}" \
  --uri="${SERVICE_URL}/v1/cron/whatsapp_outreach" \
  --http-method=POST \
  --headers="Content-Type=application/json,X-Scheduler-Secret=${SCHEDULER_SECRET},X-Admin-Api-Key=${ADMIN_API_KEY}" \
  --message-body="{\"limit\":25}" \
  --attempt-deadline=540s \
  --time-zone="${CRON_TZ:-UTC}"

gcloud scheduler jobs delete "${VOICE_OUTREACH_JOB}" --location="${SCHEDULER_LOCATION}" --quiet 2>/dev/null || true
gcloud scheduler jobs create http "${VOICE_OUTREACH_JOB}" \
  --location="${SCHEDULER_LOCATION}" \
  --schedule="${VOICE_OUTREACH_SCHEDULE}" \
  --uri="${SERVICE_URL}/v1/cron/voice_outreach" \
  --http-method=POST \
  --headers="Content-Type=application/json,X-Scheduler-Secret=${SCHEDULER_SECRET},X-Admin-Api-Key=${ADMIN_API_KEY}" \
  --message-body="{\"limit\":25}" \
  --attempt-deadline=540s \
  --time-zone="${CRON_TZ:-UTC}"

gcloud scheduler jobs delete "${WA_NURTURE_JOB}" --location="${SCHEDULER_LOCATION}" --quiet 2>/dev/null || true
gcloud scheduler jobs create http "${WA_NURTURE_JOB}" \
  --location="${SCHEDULER_LOCATION}" \
  --schedule="${WA_NURTURE_SCHEDULE}" \
  --uri="${SERVICE_URL}/v1/cron/whatsapp_nurture" \
  --http-method=POST \
  --headers="Content-Type=application/json,X-Scheduler-Secret=${SCHEDULER_SECRET},X-Admin-Api-Key=${ADMIN_API_KEY}" \
  --message-body="{\"limit\":25}" \
  --attempt-deadline=540s \
  --time-zone="${CRON_TZ:-UTC}"

gcloud scheduler jobs delete "${COLD_CAMPAIGN_JOB}" --location="${SCHEDULER_LOCATION}" --quiet 2>/dev/null || true
gcloud scheduler jobs create http "${COLD_CAMPAIGN_JOB}" \
  --location="${SCHEDULER_LOCATION}" \
  --schedule="${COLD_CAMPAIGN_SCHEDULE}" \
  --uri="${SERVICE_URL}/v1/cron/cold_campaign" \
  --http-method=POST \
  --headers="Content-Type=application/json,X-Scheduler-Secret=${SCHEDULER_SECRET},X-Admin-Api-Key=${ADMIN_API_KEY}" \
  --message-body="{\"limit\":25}" \
  --attempt-deadline=540s \
  --time-zone="${CRON_TZ:-UTC}"

echo "Created jobs ${DISPATCH_JOB}, ${ZOHO_JOB}, ${WA_OUTREACH_JOB}, ${VOICE_OUTREACH_JOB}, ${WA_NURTURE_JOB}, and ${COLD_CAMPAIGN_JOB}. Ensure Cloud Run has SCHEDULER_SECRET (or DISPATCH_SECRET) and ADMIN_API_KEY set to the same values used here."

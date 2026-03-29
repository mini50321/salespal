#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
REGION="${REGION:-asia-south1}"
SCHEDULER_LOCATION="${SCHEDULER_LOCATION:-asia-south1}"
SERVICE_URL="${SERVICE_URL:?set SERVICE_URL to Cloud Run HTTPS URL}"
SCHEDULER_SECRET="${SCHEDULER_SECRET:?set SCHEDULER_SECRET same as Cloud Run env}"

DISPATCH_SCHEDULE="${DISPATCH_SCHEDULE:-*/5 * * * *}"
ZOHO_CRON_SCHEDULE="${ZOHO_CRON_SCHEDULE:-*/10 * * * *}"
DISPATCH_JOB="${DISPATCH_JOB:-salespal-dispatch-posts}"
ZOHO_JOB="${ZOHO_JOB:-salespal-cron-zoho-push}"

gcloud config set project "${PROJECT_ID}"

gcloud scheduler jobs delete "${DISPATCH_JOB}" --location="${SCHEDULER_LOCATION}" --quiet 2>/dev/null || true
gcloud scheduler jobs create http "${DISPATCH_JOB}" \
  --location="${SCHEDULER_LOCATION}" \
  --schedule="${DISPATCH_SCHEDULE}" \
  --uri="${SERVICE_URL}/v1/marketing/posts/dispatch" \
  --http-method=POST \
  --headers="Content-Type=application/json,X-Scheduler-Secret=${SCHEDULER_SECRET}" \
  --message-body="{}" \
  --attempt-deadline=540s \
  --time-zone="${CRON_TZ:-UTC}"

gcloud scheduler jobs delete "${ZOHO_JOB}" --location="${SCHEDULER_LOCATION}" --quiet 2>/dev/null || true
gcloud scheduler jobs create http "${ZOHO_JOB}" \
  --location="${SCHEDULER_LOCATION}" \
  --schedule="${ZOHO_CRON_SCHEDULE}" \
  --uri="${SERVICE_URL}/v1/cron/zoho_push_leads" \
  --http-method=POST \
  --headers="Content-Type=application/json,X-Scheduler-Secret=${SCHEDULER_SECRET}" \
  --message-body="{\"limit\":50}" \
  --attempt-deadline=540s \
  --time-zone="${CRON_TZ:-UTC}"

echo "Created jobs ${DISPATCH_JOB} and ${ZOHO_JOB}. Ensure Cloud Run has SCHEDULER_SECRET or DISPATCH_SECRET set to the same value."

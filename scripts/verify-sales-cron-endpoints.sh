#!/usr/bin/env bash
# Post-deploy: verify scheduler-authenticated sales cron routes respond (401 without secret).
set -euo pipefail
BASE_URL="${SERVICE_URL:?set SERVICE_URL to Cloud Run base URL, no trailing slash}"
for path in \
  /v1/cron/voice_outreach \
  /v1/cron/whatsapp_nurture \
  /v1/cron/cold_campaign; do
  code="$(curl -s -o /dev/null -w "%{http_code}" -X POST "${BASE_URL}${path}" -H "Content-Type: application/json" -d '{"limit":1}')"
  if [[ "$code" != "401" ]]; then
    echo "FAIL ${path} expected 401 without scheduler auth, got ${code}"
    exit 1
  fi
  echo "OK ${path} -> ${code}"
done
echo "All cron endpoints require scheduler auth (401 as expected)."

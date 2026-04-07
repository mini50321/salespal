$ErrorActionPreference = "Stop"
if (-not $env:PROJECT_ID) { throw "Set PROJECT_ID" }
if (-not $env:SERVICE_URL) { throw "Set SERVICE_URL (Cloud Run HTTPS URL)" }
if (-not $env:SCHEDULER_SECRET) { throw "Set SCHEDULER_SECRET (match Cloud Run env)" }
if (-not $env:ADMIN_API_KEY) { throw "Set ADMIN_API_KEY (match Cloud Run env)" }
$Region = if ($env:REGION) { $env:REGION } else { "asia-south1" }
$SchedLoc = if ($env:SCHEDULER_LOCATION) { $env:SCHEDULER_LOCATION } else { $Region }
$Tz = if ($env:CRON_TZ) { $env:CRON_TZ } else { "UTC" }
$DispSched = if ($env:DISPATCH_SCHEDULE) { $env:DISPATCH_SCHEDULE } else { "*/5 * * * *" }
$ZohoSched = if ($env:ZOHO_CRON_SCHEDULE) { $env:ZOHO_CRON_SCHEDULE } else { "*/10 * * * *" }
$WaSched = if ($env:WA_OUTREACH_SCHEDULE) { $env:WA_OUTREACH_SCHEDULE } else { "*/15 * * * *" }
$VoiceSched = if ($env:VOICE_OUTREACH_SCHEDULE) { $env:VOICE_OUTREACH_SCHEDULE } else { "*/15 * * * *" }
$DispJob = if ($env:DISPATCH_JOB) { $env:DISPATCH_JOB } else { "salespal-dispatch-posts" }
$ZohoJob = if ($env:ZOHO_JOB) { $env:ZOHO_JOB } else { "salespal-cron-zoho-push" }
$WaJob = if ($env:WA_OUTREACH_JOB) { $env:WA_OUTREACH_JOB } else { "salespal-cron-whatsapp-outreach" }
$VoiceJob = if ($env:VOICE_OUTREACH_JOB) { $env:VOICE_OUTREACH_JOB } else { "salespal-cron-voice-outreach" }
$WaNurtureSched = if ($env:WA_NURTURE_SCHEDULE) { $env:WA_NURTURE_SCHEDULE } else { "*/15 * * * *" }
$ColdSched = if ($env:COLD_CAMPAIGN_SCHEDULE) { $env:COLD_CAMPAIGN_SCHEDULE } else { "*/30 * * * *" }
$WaNurtureJob = if ($env:WA_NURTURE_JOB) { $env:WA_NURTURE_JOB } else { "salespal-cron-whatsapp-nurture" }
$ColdJob = if ($env:COLD_CAMPAIGN_JOB) { $env:COLD_CAMPAIGN_JOB } else { "salespal-cron-cold-campaign" }

gcloud config set project $env:PROJECT_ID

gcloud scheduler jobs delete $DispJob --location=$SchedLoc --quiet 2>$null
gcloud scheduler jobs create http $DispJob `
  --location=$SchedLoc `
  --schedule=$DispSched `
  --uri="$($env:SERVICE_URL)/v1/marketing/posts/dispatch" `
  --http-method=POST `
  --headers="Content-Type=application/json,X-Scheduler-Secret=$($env:SCHEDULER_SECRET),X-Admin-Api-Key=$($env:ADMIN_API_KEY)" `
  --message-body="{}" `
  --attempt-deadline=540s `
  --time-zone=$Tz

gcloud scheduler jobs delete $ZohoJob --location=$SchedLoc --quiet 2>$null
gcloud scheduler jobs create http $ZohoJob `
  --location=$SchedLoc `
  --schedule=$ZohoSched `
  --uri="$($env:SERVICE_URL)/v1/cron/zoho_push_leads" `
  --http-method=POST `
  --headers="Content-Type=application/json,X-Scheduler-Secret=$($env:SCHEDULER_SECRET),X-Admin-Api-Key=$($env:ADMIN_API_KEY)" `
  --message-body='{"limit":50}' `
  --attempt-deadline=540s `
  --time-zone=$Tz

gcloud scheduler jobs delete $WaJob --location=$SchedLoc --quiet 2>$null
gcloud scheduler jobs create http $WaJob `
  --location=$SchedLoc `
  --schedule=$WaSched `
  --uri="$($env:SERVICE_URL)/v1/cron/whatsapp_outreach" `
  --http-method=POST `
  --headers="Content-Type=application/json,X-Scheduler-Secret=$($env:SCHEDULER_SECRET),X-Admin-Api-Key=$($env:ADMIN_API_KEY)" `
  --message-body='{"limit":25}' `
  --attempt-deadline=540s `
  --time-zone=$Tz

gcloud scheduler jobs delete $VoiceJob --location=$SchedLoc --quiet 2>$null
gcloud scheduler jobs create http $VoiceJob `
  --location=$SchedLoc `
  --schedule=$VoiceSched `
  --uri="$($env:SERVICE_URL)/v1/cron/voice_outreach" `
  --http-method=POST `
  --headers="Content-Type=application/json,X-Scheduler-Secret=$($env:SCHEDULER_SECRET),X-Admin-Api-Key=$($env:ADMIN_API_KEY)" `
  --message-body='{"limit":25}' `
  --attempt-deadline=540s `
  --time-zone=$Tz

gcloud scheduler jobs delete $WaNurtureJob --location=$SchedLoc --quiet 2>$null
gcloud scheduler jobs create http $WaNurtureJob `
  --location=$SchedLoc `
  --schedule=$WaNurtureSched `
  --uri="$($env:SERVICE_URL)/v1/cron/whatsapp_nurture" `
  --http-method=POST `
  --headers="Content-Type=application/json,X-Scheduler-Secret=$($env:SCHEDULER_SECRET),X-Admin-Api-Key=$($env:ADMIN_API_KEY)" `
  --message-body='{"limit":25}' `
  --attempt-deadline=540s `
  --time-zone=$Tz

gcloud scheduler jobs delete $ColdJob --location=$SchedLoc --quiet 2>$null
gcloud scheduler jobs create http $ColdJob `
  --location=$SchedLoc `
  --schedule=$ColdSched `
  --uri="$($env:SERVICE_URL)/v1/cron/cold_campaign" `
  --http-method=POST `
  --headers="Content-Type=application/json,X-Scheduler-Secret=$($env:SCHEDULER_SECRET),X-Admin-Api-Key=$($env:ADMIN_API_KEY)" `
  --message-body='{"limit":25}' `
  --attempt-deadline=540s `
  --time-zone=$Tz

Write-Host "Done. Set SCHEDULER_SECRET (or DISPATCH_SECRET) and ADMIN_API_KEY on Cloud Run to match. Jobs: $DispJob, $ZohoJob, $WaJob, $VoiceJob, $WaNurtureJob, $ColdJob"

$ErrorActionPreference = "Stop"
if (-not $env:PROJECT_ID) { throw "Set PROJECT_ID" }
if (-not $env:SERVICE_URL) { throw "Set SERVICE_URL (Cloud Run HTTPS URL)" }
if (-not $env:SCHEDULER_SECRET) { throw "Set SCHEDULER_SECRET (match Cloud Run env)" }
$Region = if ($env:REGION) { $env:REGION } else { "asia-south1" }
$SchedLoc = if ($env:SCHEDULER_LOCATION) { $env:SCHEDULER_LOCATION } else { $Region }
$Tz = if ($env:CRON_TZ) { $env:CRON_TZ } else { "UTC" }
$DispSched = if ($env:DISPATCH_SCHEDULE) { $env:DISPATCH_SCHEDULE } else { "*/5 * * * *" }
$ZohoSched = if ($env:ZOHO_CRON_SCHEDULE) { $env:ZOHO_CRON_SCHEDULE } else { "*/10 * * * *" }
$DispJob = if ($env:DISPATCH_JOB) { $env:DISPATCH_JOB } else { "salespal-dispatch-posts" }
$ZohoJob = if ($env:ZOHO_JOB) { $env:ZOHO_JOB } else { "salespal-cron-zoho-push" }

gcloud config set project $env:PROJECT_ID

gcloud scheduler jobs delete $DispJob --location=$SchedLoc --quiet 2>$null
gcloud scheduler jobs create http $DispJob `
  --location=$SchedLoc `
  --schedule=$DispSched `
  --uri="$($env:SERVICE_URL)/v1/marketing/posts/dispatch" `
  --http-method=POST `
  --headers="Content-Type=application/json,X-Scheduler-Secret=$($env:SCHEDULER_SECRET)" `
  --message-body="{}" `
  --attempt-deadline=540s `
  --time-zone=$Tz

gcloud scheduler jobs delete $ZohoJob --location=$SchedLoc --quiet 2>$null
gcloud scheduler jobs create http $ZohoJob `
  --location=$SchedLoc `
  --schedule=$ZohoSched `
  --uri="$($env:SERVICE_URL)/v1/cron/zoho_push_leads" `
  --http-method=POST `
  --headers="Content-Type=application/json,X-Scheduler-Secret=$($env:SCHEDULER_SECRET)" `
  --message-body='{"limit":50}' `
  --attempt-deadline=540s `
  --time-zone=$Tz

Write-Host "Done. Set SCHEDULER_SECRET (or DISPATCH_SECRET) on Cloud Run to match."

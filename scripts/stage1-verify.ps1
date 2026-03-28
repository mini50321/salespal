param(
  [Parameter(Mandatory = $true)][string]$ProjectId
)

$ErrorActionPreference = "Stop"

$requiredApis = @(
  "serviceusage.googleapis.com",
  "secretmanager.googleapis.com",
  "aiplatform.googleapis.com",
  "run.googleapis.com",
  "cloudbuild.googleapis.com",
  "artifactregistry.googleapis.com",
  "iam.googleapis.com",
  "logging.googleapis.com",
  "monitoring.googleapis.com"
)

$requiredSecrets = @(
  "zo_crm_client_secret",
  "zo_crm_refresh_token",
  "whatsapp_access_token",
  "whatsapp_app_secret",
  "whatsapp_verify_token",
  "tata_voice_api_key",
  "smtp_password",
  "sms_api_key"
)

$enabledApis = gcloud services list --enabled --project $ProjectId --format="value(config.name)"
$apiMissing = @()
foreach ($api in $requiredApis) {
  if ($enabledApis -notcontains $api) {
    $apiMissing += $api
  }
}

$secretNames = gcloud secrets list --project $ProjectId --format="value(name)"
$secretMissing = @()
foreach ($secret in $requiredSecrets) {
  if ($secretNames -notcontains $secret) {
    $secretMissing += $secret
  }
}

$sa = "salespal-app@${ProjectId}.iam.gserviceaccount.com"
$saCheck = gcloud iam service-accounts list --project $ProjectId --filter="email=$sa" --format="value(email)"

if (-not $saCheck) {
  Write-Host "Missing service account: $sa"
  exit 1
}

if ($apiMissing.Count -gt 0) {
  Write-Host "Missing APIs:"
  $apiMissing | ForEach-Object { Write-Host $_ }
  exit 1
}

if ($secretMissing.Count -gt 0) {
  Write-Host "Missing secrets:"
  $secretMissing | ForEach-Object { Write-Host $_ }
  exit 1
}

Write-Host "Stage 1 verification passed"

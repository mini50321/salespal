param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [string]$Region = "asia-south1"
)

$ErrorActionPreference = "Stop"

$apis = @(
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

$secrets = @(
  "zo_crm_client_secret",
  "zo_crm_refresh_token",
  "whatsapp_access_token",
  "whatsapp_app_secret",
  "whatsapp_verify_token",
  "tata_voice_api_key",
  "smtp_password",
  "sms_api_key"
)

gcloud auth list | Out-Null
gcloud config set project $ProjectId | Out-Null
gcloud config set compute/region $Region | Out-Null

foreach ($api in $apis) {
  gcloud services enable $api --project $ProjectId | Out-Null
}

gcloud iam service-accounts create salespal-app --display-name "SalesPal application" --project $ProjectId 2>$null

$sa = "salespal-app@${ProjectId}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$sa" --role roles/aiplatform.user | Out-Null
gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$sa" --role roles/secretmanager.secretAccessor | Out-Null

foreach ($secret in $secrets) {
  gcloud secrets create $secret --replication-policy="automatic" --project $ProjectId 2>$null
}

Write-Host "Stage 1 setup completed"
Write-Host "Project: $ProjectId"
Write-Host "Region: $Region"
Write-Host "Service Account: $sa"

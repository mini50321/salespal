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

gcloud config set project $ProjectId
gcloud config set compute/region $Region

foreach ($api in $apis) {
  gcloud services enable $api --project $ProjectId
}

gcloud iam service-accounts create salespal-app `
  --display-name "SalesPal application" `
  --project $ProjectId 2>$null

$sa = "salespal-app@${ProjectId}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $ProjectId `
  --member "serviceAccount:$sa" `
  --role roles/aiplatform.user

gcloud projects add-iam-policy-binding $ProjectId `
  --member "serviceAccount:$sa" `
  --role roles/secretmanager.secretAccessor

Write-Host "Done. Service account: $sa"

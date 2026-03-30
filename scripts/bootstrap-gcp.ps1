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

# gcloud may print progress to stderr; don't treat as terminating.
$ErrorActionPreference = "Continue"

gcloud config set project $ProjectId
if ($LASTEXITCODE -ne 0) { throw "gcloud config set project failed" }
gcloud config set compute/region $Region
if ($LASTEXITCODE -ne 0) { throw "gcloud config set compute/region failed" }

foreach ($api in $apis) {
  gcloud services enable $api --project $ProjectId
  if ($LASTEXITCODE -ne 0) { throw "failed to enable api: $api" }
}

$sa = "salespal-app@${ProjectId}.iam.gserviceaccount.com"
 $pn = (gcloud projects describe $ProjectId --format="value(projectNumber)" 2>$null).Trim()
 $cloudBuildSa = ""
 if ($pn) {
   $cloudBuildSa = "$pn@cloudbuild.gserviceaccount.com"
 }
 $computeDefaultSa = ""
 if ($pn) {
   $computeDefaultSa = "$pn-compute@developer.gserviceaccount.com"
 }

$exists = $false
try {
  gcloud iam service-accounts describe $sa --project $ProjectId 1>$null 2>$null
  $exists = $true
} catch {
  $exists = $false
}

if (-not $exists) {
  gcloud iam service-accounts create salespal-app `
    --display-name "SalesPal application" `
    --project $ProjectId | Out-Null
}

gcloud projects add-iam-policy-binding $ProjectId `
  --member "serviceAccount:$sa" `
  --role roles/aiplatform.user

gcloud projects add-iam-policy-binding $ProjectId `
  --member "serviceAccount:$sa" `
  --role roles/secretmanager.secretAccessor

if ($cloudBuildSa) {
  gcloud projects add-iam-policy-binding $ProjectId `
    --member "serviceAccount:$cloudBuildSa" `
    --role roles/artifactregistry.writer
  gcloud projects add-iam-policy-binding $ProjectId `
    --member "serviceAccount:$cloudBuildSa" `
    --role roles/logging.logWriter
}

if ($computeDefaultSa) {
  gcloud projects add-iam-policy-binding $ProjectId `
    --member "serviceAccount:$computeDefaultSa" `
    --role roles/artifactregistry.writer
  gcloud projects add-iam-policy-binding $ProjectId `
    --member "serviceAccount:$computeDefaultSa" `
    --role roles/logging.logWriter
}

Write-Host "Done. Service account: $sa"

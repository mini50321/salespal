param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [Parameter(Mandatory = $true)][string]$ServiceName,
  [string]$Region = "asia-south1",
  [string]$Repo = "salespal",
  [string]$ImageName = "marketing-api",
  [string]$Tag = "v1",
  [string]$AllowUnauthenticated = "true"
)

$ErrorActionPreference = "Stop"

gcloud config set project $ProjectId | Out-Null
gcloud config set run/region $Region | Out-Null

$repoCheck = gcloud artifacts repositories list --project $ProjectId --location $Region --format="value(name)" | Where-Object { $_ -eq $Repo }
if (-not $repoCheck) {
  gcloud artifacts repositories create $Repo --repository-format=docker --location $Region --project $ProjectId | Out-Null
}

$image = "$Region-docker.pkg.dev/$ProjectId/$Repo/$ImageName`:$Tag"

gcloud builds submit --project $ProjectId --tag $image | Out-Null

$sa = "salespal-app@${ProjectId}.iam.gserviceaccount.com"

$env = @(
  "GCP_PROJECT_ID=$ProjectId",
  "GCP_REGION=$Region"
)

gcloud run deploy $ServiceName `
  --project $ProjectId `
  --region $Region `
  --image $image `
  --service-account $sa `
  --set-env-vars ($env -join ",") `
  --port 8080 `
  --min-instances 0 `
  --max-instances 10 `
  --cpu 1 `
  --memory 512Mi `
  --timeout 60 `
  --concurrency 40 | Out-Null

if ($AllowUnauthenticated -eq "true") {
  gcloud run services add-iam-policy-binding $ServiceName `
    --project $ProjectId `
    --region $Region `
    --member "allUsers" `
    --role "roles/run.invoker" | Out-Null
}

$url = gcloud run services describe $ServiceName --project $ProjectId --region $Region --format="value(status.url)"
Write-Host "Deployed"
Write-Host "URL: $url"
Write-Host "Image: $image"

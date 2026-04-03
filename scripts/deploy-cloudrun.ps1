param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [Parameter(Mandatory = $true)][string]$ServiceName,
  [string]$Region = "asia-south1",
  [string]$StoreBackend = "firestore",
  [string]$GeneratorBackend = "vertex",
  [string]$ConversationReplyBackend = "rules",
  [string]$AllowUnauthenticated = "true",
  # Optional: GCS bucket name (no gs://) for Vertex image/carousel/video offload; required for reliable Firestore jobs.
  [string]$MediaBucket = ""
)

$ErrorActionPreference = "Stop"

Write-Host "Using project=$ProjectId region=$Region service=$ServiceName"

if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
  throw "gcloud not found. Install Google Cloud SDK first."
}

# gcloud can emit progress on stderr; don't let that abort the script.
$ErrorActionPreference = "Continue"

& $PSScriptRoot\bootstrap-gcp.ps1 -ProjectId $ProjectId -Region $Region | Out-Null
if ($LASTEXITCODE -ne 0) { throw "bootstrap-gcp failed" }

& $PSScriptRoot\step1-6-deploy-cloudrun.ps1 `
  -ProjectId $ProjectId `
  -ServiceName $ServiceName `
  -Region $Region `
  -Repo "salespal" `
  -ImageName "salespal-api" `
  -Tag ("build-" + (Get-Date -Format "yyyyMMdd-HHmmss")) `
  -AllowUnauthenticated $AllowUnauthenticated | Out-Null
if ($LASTEXITCODE -ne 0) { throw "deploy step failed" }

$envVars = @(
  "GCP_PROJECT_ID=$ProjectId",
  "GCP_REGION=$Region",
  "VERTEX_VIDEO_REGION=$Region",
  "STORE_BACKEND=$StoreBackend",
  "GENERATOR_BACKEND=$GeneratorBackend",
  "CONVERSATION_REPLY_BACKEND=$ConversationReplyBackend",
  "DEMO_UI_ENABLED=1"
)
if ($MediaBucket.Trim()) {
  $envVars += "META_MEDIA_BUCKET=$($MediaBucket.Trim())"
}

gcloud run services update $ServiceName `
  --project $ProjectId `
  --region $Region `
  --update-env-vars ($envVars -join ",") | Out-Null
if ($LASTEXITCODE -ne 0) { throw "failed to update Cloud Run env vars" }

$url = gcloud run services describe $ServiceName --project $ProjectId --region $Region --format="value(status.url)"
Write-Host "Deployed URL: $url"

try {
  & $PSScriptRoot\step1-6-verify-cloudrun.ps1 -Url $url | Out-Null
  Write-Host "Verify: OK"
} catch {
  Write-Host "Verify: skipped/failed (service may still be starting)"
  Write-Host $_
}


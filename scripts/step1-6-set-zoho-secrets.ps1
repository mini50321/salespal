param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [Parameter(Mandatory = $true)][string]$Region,
  [Parameter(Mandatory = $true)][string]$ServiceName,
  [Parameter(Mandatory = $true)][string]$ZohoDc,
  [Parameter(Mandatory = $true)][string]$ZohoClientId,
  [Parameter(Mandatory = $true)][string]$ZohoClientSecret,
  [Parameter(Mandatory = $true)][string]$ZohoRefreshToken,
  [string]$ZohoOwnerId = ""
)

$ErrorActionPreference = "Stop"

gcloud config set project $ProjectId | Out-Null
gcloud config set run/region $Region | Out-Null

$env = @(
  "ZOHO_DC=$ZohoDc",
  "ZOHO_CLIENT_ID=$ZohoClientId",
  "ZOHO_CLIENT_SECRET=$ZohoClientSecret",
  "ZOHO_REFRESH_TOKEN=$ZohoRefreshToken"
)

if ($ZohoOwnerId) {
  $env += "ZOHO_OWNER_ID=$ZohoOwnerId"
}

# Use --update-env-vars so GCP_PROJECT_ID, GENERATOR_BACKEND, DEMO_UI_*, buckets, etc. are not wiped.
gcloud run services update $ServiceName `
  --project $ProjectId `
  --region $Region `
  --update-env-vars ($env -join ",") | Out-Null

Write-Host "Updated Zoho env vars on Cloud Run service (merged; other env vars preserved)"

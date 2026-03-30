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

# gcloud sometimes writes progress to stderr; don't treat that as a terminating PowerShell error.
$ErrorActionPreference = "Continue"

gcloud config set project $ProjectId | Out-Null
if ($LASTEXITCODE -ne 0) { throw "gcloud config set project failed" }
gcloud config set run/region $Region | Out-Null
if ($LASTEXITCODE -ne 0) { throw "gcloud config set run/region failed" }

$repoCheck = gcloud artifacts repositories list --project $ProjectId --location $Region --format="value(name)" | Where-Object { $_ -eq $Repo }
if (-not $repoCheck) {
  gcloud artifacts repositories create $Repo --repository-format=docker --location $Region --project $ProjectId | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "failed to create Artifact Registry repo '$Repo' in $Region" }
}

$image = "$Region-docker.pkg.dev/$ProjectId/$Repo/$ImageName`:$Tag"

function Invoke-GcloudBuildSubmitWithRetry {
  param(
    [Parameter(Mandatory = $true)][string]$ProjectId,
    [Parameter(Mandatory = $true)][string]$Image
  )

  $maxAttempts = 5
  for ($i = 1; $i -le $maxAttempts; $i++) {
    $out = & gcloud builds submit --project $ProjectId --tag $Image 2>&1
    $code = $LASTEXITCODE
    if ($code -eq 0) {
      return @{ ok = $true; out = $out }
    }

    $s = ($out | Out-String)
    $isTransientSsl =
      ($s -match "SSL" -and $s -match "EOF") -or
      ($s -match "SSL\\s+error") -or
      ($s -match "UNEXPECTED_EOF_WHILE_READING") -or
      ($s -match "TLSV1_ALERT") -or
      ($s -match "Connection.*reset") -or
      ($s -match "timed out") -or
      ($s -match "Temporary failure") -or
      ($s -match "503")

    if (-not $isTransientSsl -or $i -eq $maxAttempts) {
      return @{ ok = $false; out = $out }
    }

    $sleep = [Math]::Min(60, (5 * [Math]::Pow(2, $i - 1)))
    Write-Host "Cloud Build submit failed (likely network/TLS). Retrying in $sleep seconds... (attempt $i/$maxAttempts)"
    Start-Sleep -Seconds $sleep
  }
}

$res = Invoke-GcloudBuildSubmitWithRetry -ProjectId $ProjectId -Image $image
if (-not $res.ok) {
  Write-Host "Cloud Build failed while building/pushing image:"
  Write-Host $image
  Write-Host ""
  Write-Host $res.out
  Write-Host ""
  Write-Host "If you see SSL/TLS EOF errors, try: disable VPN/proxy/SSL-inspection antivirus and retry."
  Write-Host "Next: open the build logs URL above (or run: gcloud builds list --limit 5)."
  exit 1
}

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
  --concurrency 40 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
  Write-Host "Cloud Run deploy failed for image:"
  Write-Host $image
  exit 1
}

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

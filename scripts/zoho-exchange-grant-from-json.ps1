<#
.SYNOPSIS
  Exchange a Zoho OAuth authorization code (from API Console / grant JSON) for tokens.

.DESCRIPTION
  Reads client_id, client_secret, and code from a JSON file (same shape the client sends).
  POSTs to accounts.zoho.* with proper URL encoding. Prints refresh_token on success.

  The redirect_uri MUST match exactly what is registered in Zoho for this client (often http://localhost).

.PARAMETER JsonPath
  Path to JSON file containing at least: client_id, client_secret, code

.PARAMETER RedirectUri
  Must match Zoho app redirect URI (default http://localhost)

.PARAMETER AccountsHost
  Default https://accounts.zoho.in (India). Use accounts.zoho.com / .eu etc. if needed.
#>
param(
  [Parameter(Mandatory = $true)][string]$JsonPath,
  [string]$RedirectUri = "http://localhost",
  [string]$AccountsHost = "https://accounts.zoho.in"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $JsonPath)) {
  throw "File not found: $JsonPath"
}

$raw = Get-Content -LiteralPath $JsonPath -Raw -Encoding UTF8
$j = $raw | ConvertFrom-Json

$clientId = [string]$j.client_id
$clientSecret = [string]$j.client_secret
$code = [string]$j.code

if (-not $clientId -or -not $clientSecret -or -not $code) {
  throw "JSON must include client_id, client_secret, and code"
}

function Encode-Form([string]$s) {
  return [System.Uri]::EscapeDataString($s)
}

$body = @(
  "grant_type=authorization_code",
  "client_id=$(Encode-Form $clientId)",
  "client_secret=$(Encode-Form $clientSecret)",
  "redirect_uri=$(Encode-Form $RedirectUri)",
  "code=$(Encode-Form $code)"
) -join "&"

$uri = "$AccountsHost/oauth/v2/token"
Write-Host "POST $uri" -ForegroundColor DarkGray
Write-Host "redirect_uri=$RedirectUri" -ForegroundColor DarkGray

try {
  $resp = Invoke-RestMethod -Method Post -Uri $uri -ContentType "application/x-www-form-urlencoded" -Body $body
} catch {
  Write-Host "Request failed: $_" -ForegroundColor Red
  throw
}

if ($resp.error) {
  Write-Host "Zoho error: $($resp.error)" -ForegroundColor Red
  if ($resp.error_description) { Write-Host $resp.error_description }
  if ($resp.error -eq "invalid_client") {
    Write-Host ""
    Write-Host "invalid_client usually means client_id or client_secret does not match Zoho API Console." -ForegroundColor Yellow
    Write-Host "Re-copy both from the console (watch for confusing characters: I vs 1, O vs 0, L vs I)." -ForegroundColor Yellow
  }
  exit 1
}

$rt = $resp.refresh_token
$at = $resp.access_token
if (-not $rt) {
  Write-Host "No refresh_token in response. Full response:" -ForegroundColor Yellow
  $resp | ConvertTo-Json -Depth 5
  Write-Host ""
  Write-Host "If you used the wrong redirect_uri or an expired code, exchange will not return refresh_token." -ForegroundColor Yellow
  exit 1
}

Write-Host ""
Write-Host "SUCCESS. Save this refresh token securely (do not commit to git):" -ForegroundColor Green
Write-Host $rt
Write-Host ""
Write-Host "Next (merge into Cloud Run without wiping other env vars):" -ForegroundColor Cyan
Write-Host "  .\scripts\step1-6-set-zoho-secrets.ps1 ``"
Write-Host "    -ProjectId YOUR_PROJECT ``"
Write-Host "    -Region us-central1 ``"
Write-Host "    -ServiceName salespal-api-us ``"
Write-Host "    -ZohoDc IN ``"
Write-Host "    -ZohoClientId `"$clientId`" ``"
Write-Host "    -ZohoClientSecret `"<paste secret from Zoho console>`" ``"
Write-Host "    -ZohoRefreshToken `"$rt`""

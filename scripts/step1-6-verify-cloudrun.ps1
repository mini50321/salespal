param(
  [Parameter(Mandatory = $true)][string]$Url
)

$ErrorActionPreference = "Stop"

$health = Invoke-RestMethod -Method Get -Uri ($Url.TrimEnd("/") + "/_healthz")
if ($health.status -ne "ok") { throw "health failed" }

$assets = Invoke-RestMethod -Method Get -Uri ($Url.TrimEnd("/") + "/v1/marketing/assets")
$posts = Invoke-RestMethod -Method Get -Uri ($Url.TrimEnd("/") + "/v1/marketing/posts")
$leads = Invoke-RestMethod -Method Get -Uri ($Url.TrimEnd("/") + "/v1/marketing/leads")

Write-Host "OK"
Write-Host ("assets_count=" + ($assets | Measure-Object).Count)
Write-Host ("posts_count=" + ($posts | Measure-Object).Count)
Write-Host ("leads_count=" + ($leads | Measure-Object).Count)

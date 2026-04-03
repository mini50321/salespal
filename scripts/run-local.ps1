param(
  [int]$Port = 8080
)

$ErrorActionPreference = "Stop"

Write-Host "Starting local dev server"
Write-Host "  Open in browser: http://localhost:$Port/  → redirects to /demo"
Write-Host "  Demo UI direct: http://localhost:$Port/demo"
Write-Host "  JSON (no browser): http://localhost:$Port/api"
Write-Host "Defaults: STORE_BACKEND=json, GENERATOR_BACKEND=mock, DEMO_UI_ENABLED=1"

$env:PORT = "$Port"
$env:STORE_BACKEND = $env:STORE_BACKEND -as [string]
if (-not $env:STORE_BACKEND) { $env:STORE_BACKEND = "json" }

$env:GENERATOR_BACKEND = $env:GENERATOR_BACKEND -as [string]
if (-not $env:GENERATOR_BACKEND) { $env:GENERATOR_BACKEND = "mock" }

$env:CONVERSATION_REPLY_BACKEND = $env:CONVERSATION_REPLY_BACKEND -as [string]
if (-not $env:CONVERSATION_REPLY_BACKEND) { $env:CONVERSATION_REPLY_BACKEND = "rules" }

$env:DEMO_UI_ENABLED = $env:DEMO_UI_ENABLED -as [string]
if (-not $env:DEMO_UI_ENABLED) { $env:DEMO_UI_ENABLED = "1" }

Write-Host "Note: auto-reload is disabled on Windows (Werkzeug reloader can crash with WinError 10038)."
Write-Host "If you edit UI in app/main.py, stop (Ctrl+C) and rerun this script."

python -m flask --app app.main:app run --host 0.0.0.0 --port $Port


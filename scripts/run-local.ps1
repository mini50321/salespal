param(
  [int]$Port = 8080
)

$ErrorActionPreference = "Stop"

Write-Host "Starting local dev server on http://localhost:$Port/"
Write-Host "Defaults: STORE_BACKEND=json, GENERATOR_BACKEND=mock (safe for UI iteration)"

$env:PORT = "$Port"
$env:STORE_BACKEND = $env:STORE_BACKEND -as [string]
if (-not $env:STORE_BACKEND) { $env:STORE_BACKEND = "json" }

$env:GENERATOR_BACKEND = $env:GENERATOR_BACKEND -as [string]
if (-not $env:GENERATOR_BACKEND) { $env:GENERATOR_BACKEND = "mock" }

$env:CONVERSATION_REPLY_BACKEND = $env:CONVERSATION_REPLY_BACKEND -as [string]
if (-not $env:CONVERSATION_REPLY_BACKEND) { $env:CONVERSATION_REPLY_BACKEND = "rules" }

Write-Host "Note: auto-reload is disabled on Windows (Werkzeug reloader can crash with WinError 10038)."
Write-Host "If you edit UI in app/main.py, stop (Ctrl+C) and rerun this script."

python -m flask --app app.main:app run --host 0.0.0.0 --port $Port


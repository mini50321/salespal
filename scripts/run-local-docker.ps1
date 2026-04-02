param(
  [int]$Port = 8080,
  [string]$Image = "salespal-api-local:dev"
)

$ErrorActionPreference = "Stop"

Write-Host "Building Docker image: $Image"
docker build -t $Image .
if ($LASTEXITCODE -ne 0) { throw "docker build failed" }

Write-Host "Running container on http://localhost:$Port/ (Ctrl+C to stop)"
docker run --rm -p "${Port}:8080" `
  -e "PORT=8080" `
  -e "STORE_BACKEND=json" `
  -e "GENERATOR_BACKEND=mock" `
  -e "CONVERSATION_REPLY_BACKEND=rules" `
  $Image


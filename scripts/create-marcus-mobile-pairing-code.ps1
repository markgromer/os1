param(
  [string]$BaseUrl = "https://task-tracker-5wsa.onrender.com"
)

$ErrorActionPreference = "Stop"
$tokenPath = Join-Path $env:APPDATA "M.A.R.C.U.S.\mobile-live-admin-token.txt"
if (-not (Test-Path $tokenPath)) {
  throw "Marcus mobile admin token not found at $tokenPath."
}

$adminToken = (Get-Content -Raw $tokenPath).Trim()
if (-not $adminToken) {
  throw "Marcus mobile admin token is empty."
}

$result = Invoke-RestMethod -Method Post `
  -Uri "$($BaseUrl.TrimEnd('/'))/api/auth/pairing-code" `
  -Headers @{ Authorization = "Bearer $adminToken" } `
  -ContentType "application/json" `
  -Body "{}" `
  -TimeoutSec 30

[pscustomobject]@{
  Code = $result.code
  ExpiresAt = [DateTimeOffset]::FromUnixTimeMilliseconds([long]$result.expiresAt).ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss zzz")
  MobileUrl = "$($BaseUrl.TrimEnd('/'))/mobile.html"
}

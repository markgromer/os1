param(
  [int]$Port = 4178
)

$ErrorActionPreference = "Stop"

$outputDir = Join-Path (Get-Location) "output"
if (-not (Test-Path $outputDir)) {
  New-Item -ItemType Directory -Path $outputDir | Out-Null
}

$stdoutLog = Join-Path $outputDir "marcus-cloudflared.out.log"
$stderrLog = Join-Path $outputDir "marcus-cloudflared.err.log"

$npx = (Get-Command npx.cmd -ErrorAction SilentlyContinue).Source
if (-not $npx) {
  $npx = (Get-Command npx -ErrorAction Stop).Source
}

Start-Process `
  -FilePath $npx `
  -ArgumentList @("--yes", "cloudflared", "tunnel", "--url", "http://127.0.0.1:$Port") `
  -WorkingDirectory (Get-Location) `
  -WindowStyle Hidden `
  -RedirectStandardOutput $stdoutLog `
  -RedirectStandardError $stderrLog

$url = ""
for ($i = 0; $i -lt 20; $i++) {
  Start-Sleep -Seconds 1
  $content = @()
  if (Test-Path $stdoutLog) { $content += Get-Content $stdoutLog }
  if (Test-Path $stderrLog) { $content += Get-Content $stderrLog }
  $match = [regex]::Match(($content -join "`n"), "https://[a-zA-Z0-9-]+\.trycloudflare\.com")
  if ($match.Success) {
    $url = $match.Value
    break
  }
}

if (-not $url) {
  throw "Cloudflare tunnel URL was not emitted. Check $stdoutLog and $stderrLog."
}

$status = $null
for ($i = 0; $i -lt 30; $i++) {
  try {
    $status = (Invoke-WebRequest -UseBasicParsing -Uri "$url/mobile.html" -TimeoutSec 20).StatusCode
    break
  } catch {
    Start-Sleep -Seconds 2
  }
}

if (-not $status) {
  throw "Cloudflare tunnel URL was created but /mobile.html did not become reachable: $url"
}

[pscustomobject]@{
  TunnelUrl = $url
  MobileUrl = "$url/mobile.html"
  MobileStatus = $status
  StdoutLog = $stdoutLog
  StderrLog = $stderrLog
} | ConvertTo-Json -Depth 3

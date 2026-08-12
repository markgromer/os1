param(
  [int]$Port = 4178,
  [string]$HostName = "0.0.0.0"
)

$ErrorActionPreference = "Stop"

function Get-WranglerToken {
  $configPath = Join-Path $env:APPDATA "xdg.config\.wrangler\config\default.toml"
  if (-not (Test-Path $configPath)) {
    throw "Wrangler config not found. Run `npx wrangler login` first."
  }

  $text = Get-Content -Raw $configPath
  $oauth = [regex]::Match($text, "oauth_token\s*=\s*`"([^`"]+)`"")
  if ($oauth.Success) { return $oauth.Groups[1].Value }

  $api = [regex]::Match($text, "api_token\s*=\s*`"([^`"]+)`"")
  if ($api.Success) { return $api.Groups[1].Value }

  throw "No Wrangler token found in $configPath."
}

function Get-GitHubToken {
  $token = (& gh auth token).Trim()
  if (-not $token) {
    throw "GitHub CLI did not return a token. Run `gh auth login` first."
  }
  return $token
}

function Get-CloudflareAccountAndZone($Token) {
  $headers = @{ Authorization = "Bearer $Token" }
  $accounts = Invoke-RestMethod -Method Get -Uri "https://api.cloudflare.com/client/v4/accounts" -Headers $headers -TimeoutSec 20
  $account = @($accounts.result)[0]
  if (-not $account.id) { throw "No Cloudflare account returned for Wrangler token." }

  $zones = Invoke-RestMethod -Method Get -Uri "https://api.cloudflare.com/client/v4/zones?per_page=50" -Headers $headers -TimeoutSec 20
  $zone = @($zones.result | Where-Object { $_.account.id -eq $account.id } | Select-Object -First 1)

  return @{
    AccountId = $account.id
    ZoneId = $zone.id
    ZoneName = $zone.name
  }
}

$githubToken = Get-GitHubToken
$cloudflareToken = Get-WranglerToken
$cloudflare = Get-CloudflareAccountAndZone $cloudflareToken
$adminToken = "marcus-live-" + ([guid]::NewGuid().ToString("N"))

$env:PORT = [string]$Port
$env:MARCUS_HOST = $HostName
$env:NODE_ENV = "development"
$env:ADMIN_TOKEN = $adminToken
$env:GITHUB_TOKEN = $githubToken
$env:GITHUB_OWNER = "markgromer"
$env:CLOUDFLARE_API_TOKEN = $cloudflareToken
$env:CLOUDFLARE_ACCOUNT_ID = $cloudflare.AccountId
if ($cloudflare.ZoneId) {
  $env:CLOUDFLARE_DEFAULT_ZONE_ID = $cloudflare.ZoneId
}
$env:MARCUS_CODEX_GITHUB_ACTIONS_ENABLED = "true"
$env:MARCUS_CODEX_GITHUB_TOKEN = $githubToken
$env:MARCUS_CODEX_RUNNER_REPO = "markgromer/Reggie"

Start-Process -FilePath node -ArgumentList "server.js" -WorkingDirectory (Get-Location) -WindowStyle Hidden
Start-Sleep -Seconds 4

$health = Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:$Port/api/marcus/operator-health" -Headers @{ Authorization = "Bearer $adminToken" } -TimeoutSec 10
$mobileStatus = (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$Port/mobile.html" -TimeoutSec 10).StatusCode
$lanIps = Get-NetIPAddress -AddressFamily IPv4 |
  Where-Object { $_.IPAddress -notlike "127.*" -and $_.PrefixOrigin -ne "WellKnown" } |
  Select-Object -First 5 -ExpandProperty IPAddress

[pscustomobject]@{
  Port = $Port
  AdminToken = $adminToken
  LocalUrl = "http://127.0.0.1:$Port/mobile.html"
  LanUrls = @($lanIps | ForEach-Object { "http://$($_):$Port/mobile.html" })
  MobileStatus = $mobileStatus
  GitHubConnected = $health.capabilities.github.backendTokenConfigured
  CloudflareConnected = $health.capabilities.cloudflare.backendTokenConfigured
  CloudflareAccountConfigured = $health.capabilities.cloudflare.accountIdConfigured
  CloudflareZoneConfigured = $health.capabilities.cloudflare.defaultZoneConfigured
  CloudflareZoneName = $cloudflare.ZoneName
  CodexMode = $health.capabilities.projectOperator.mode
  CodexProvider = $health.capabilities.projectOperator.provider
  CanStartCodex = $health.capabilities.projectOperator.canStartCodexDirectly
  RemainingBlockers = $health.blockers
} | ConvertTo-Json -Depth 5

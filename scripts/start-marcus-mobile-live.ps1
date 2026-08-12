param(
  [int]$Port = 4178,
  [string]$HostName = "0.0.0.0",
  [string]$AdminToken = ""
)

$ErrorActionPreference = "Stop"

function Get-WranglerToken {
  if ($env:CLOUDFLARE_API_TOKEN) {
    return $env:CLOUDFLARE_API_TOKEN.Trim()
  }

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
  if ($env:GITHUB_TOKEN) {
    return $env:GITHUB_TOKEN.Trim()
  }

  $token = (& gh auth token).Trim()
  if (-not $token) {
    throw "GitHub CLI did not return a token. Run `gh auth login` first."
  }
  return $token
}

function Get-StableAdminToken($Override) {
  if ($Override) { return $Override.Trim() }
  if ($env:ADMIN_TOKEN) { return $env:ADMIN_TOKEN.Trim() }

  $dir = Join-Path $env:APPDATA "M.A.R.C.U.S."
  if (-not (Test-Path $dir)) {
    New-Item -ItemType Directory -Path $dir | Out-Null
  }

  $path = Join-Path $dir "mobile-live-admin-token.txt"
  if (Test-Path $path) {
    $existing = (Get-Content -Raw $path).Trim()
    if ($existing) { return $existing }
  }

  $token = "marcus-live-" + ([guid]::NewGuid().ToString("N"))
  Set-Content -Path $path -Value $token -NoNewline
  return $token
}

function Read-JsonFile($Path) {
  if (-not (Test-Path $Path)) { return $null }
  try {
    return Get-Content -Raw $Path | ConvertFrom-Json
  } catch {
    return $null
  }
}

function Read-EnvValue($Path, $Key) {
  if (-not (Test-Path $Path)) { return "" }
  $line = Select-String -LiteralPath $Path -Pattern "^\s*$Key\s*=" -ErrorAction SilentlyContinue | Select-Object -First 1
  if (-not $line) { return "" }
  $raw = $line.Line -replace "^\s*$Key\s*=\s*", ""
  return $raw.Trim().Trim('"').Trim("'")
}

function Get-OpenAiConfig {
  $settingsPaths = @(
    (Join-Path $env:APPDATA "M.A.R.C.U.S.\settings.json"),
    (Join-Path $env:APPDATA "Task Tracker\settings.json")
  )

  $apiKey = if ($env:OPENAI_API_KEY) { $env:OPENAI_API_KEY.Trim() } else { "" }
  $model = if ($env:OPENAI_MODEL) { $env:OPENAI_MODEL.Trim() } else { "" }

  foreach ($path in $settingsPaths) {
    $settings = Read-JsonFile $path
    if (-not $apiKey -and $settings.openaiApiKey) { $apiKey = [string]$settings.openaiApiKey }
    if (-not $model -and $settings.openaiModel) { $model = [string]$settings.openaiModel }
  }

  $fallbackEnv = "C:\Users\markg\OneDrive\Documents\FastFoodSMS\.env"
  if (-not $apiKey) { $apiKey = Read-EnvValue $fallbackEnv "OPENAI_API_KEY" }
  if (-not $model) { $model = Read-EnvValue $fallbackEnv "OPENAI_MODEL" }

  return @{
    ApiKey = $apiKey
    Model = if ($model) { $model } else { "gpt-4.1-mini" }
  }
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
$adminToken = Get-StableAdminToken $AdminToken
$openai = Get-OpenAiConfig

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
if ($openai.ApiKey) {
  $env:OPENAI_API_KEY = $openai.ApiKey
}
if ($openai.Model) {
  $env:OPENAI_MODEL = $openai.Model
}

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
  OpenAiConfigured = $health.capabilities.openai.configured
  OpenAiModel = $health.capabilities.openai.model
  RemainingBlockers = $health.blockers
} | ConvertTo-Json -Depth 5

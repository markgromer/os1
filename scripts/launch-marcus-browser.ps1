param(
  [string[]]$Urls = @(
    "https://mail.google.com/",
    "https://www.skool.com/",
    "https://zoom.us/wc",
    "https://www.youtube.com/",
    "https://www.tiktok.com/",
    "http://127.0.0.1:3030/live-presence.html"
  )
)

$ErrorActionPreference = "Stop"

function Find-Browser {
  $candidates = @(
    "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
    "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
    "$env:LocalAppData\Google\Chrome\Application\chrome.exe"
  )
  foreach ($candidate in $candidates) {
    if ($candidate -and (Test-Path -LiteralPath $candidate)) { return $candidate }
  }
  throw "Chrome was not found. Install Chrome first so Marcus can use a dedicated Chrome window/profile."
}

$browser = Find-Browser
$profileRoot = Join-Path $env:LocalAppData "M.A.R.C.U.S\MarcusBrowserProfile"
New-Item -ItemType Directory -Path $profileRoot -Force | Out-Null
$normalizedUrls = @(
  foreach ($url in $Urls) {
    foreach ($part in ([string]$url -split ',')) {
      $trimmed = $part.Trim()
      if ($trimmed) { $trimmed }
    }
  }
)

$args = @(
  "--user-data-dir=$profileRoot",
  "--profile-directory=Default",
  "--no-first-run",
  "--disable-features=Translate",
  "--new-window"
) + $normalizedUrls

Start-Process -FilePath $browser -ArgumentList $args

Write-Host "Opened Marcus browser profile."
Write-Host "Profile data: $profileRoot"
Write-Host "Use this Chrome window for markgromermarcus@gmail.com, Gmail, Skool, Zoom, YouTube, and TikTok logins. Do not use Mark's personal browser profile for Marcus."

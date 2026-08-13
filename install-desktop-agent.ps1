param(
    [string]$ServerUrl = "https://task-tracker-5wsa.onrender.com",
    [string]$TokenFile = (Join-Path $env:APPDATA "M.A.R.C.U.S\mobile-live-admin-token.txt"),
    [string[]]$AllowedWorkspaceRoots = @(
        (Join-Path $env:USERPROFILE "OneDrive\Documents"),
        (Join-Path $env:USERPROFILE "Documents"),
        (Join-Path $env:USERPROFILE "Downloads")
    ),
    [string]$NewProjectRoot = (Join-Path $env:USERPROFILE "OneDrive\Documents\Marcus Projects"),
    [string[]]$PcAccessRoots = @(),
    [switch]$FullPcAccess,
    [ValidateSet("kiosk", "app")]
    [string]$CodexMonitorMode = "kiosk",
    [switch]$StartNow
)

$ErrorActionPreference = "Stop"
$taskName = "MARCUS-DesktopAgent"
$scriptDir = $PSScriptRoot
$agentPath = Join-Path $scriptDir "desktop-agent.cjs"

if (-not (Test-Path -LiteralPath $agentPath)) {
    throw "desktop-agent.cjs was not found at $agentPath"
}

if (-not (Test-Path -LiteralPath $TokenFile)) {
    throw "Marcus admin token file was not found at $TokenFile"
}

if ([string]::IsNullOrWhiteSpace((Get-Content -LiteralPath $TokenFile -Raw))) {
    throw "Marcus admin token file is empty."
}

$nodePath = (Get-Command node -ErrorAction SilentlyContinue).Source
if (-not $nodePath) {
    throw "node.exe was not found in PATH."
}

$configDir = Join-Path $env:APPDATA "M.A.R.C.U.S"
$configPath = Join-Path $configDir "desktop-agent.json"
$existingConfig = $null
if (Test-Path -LiteralPath $configPath) {
    try { $existingConfig = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json } catch {}
}
$effectiveFullPcAccess = if ($FullPcAccess.IsPresent) { $true } elseif ($null -ne $existingConfig) { [bool]$existingConfig.fullPcAccess } else { $false }
$effectivePcAccessRoots = @($PcAccessRoots | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
if ($effectiveFullPcAccess -and $effectivePcAccessRoots.Count -eq 0) {
    $effectivePcAccessRoots = @("$($env:SystemDrive)\")
}
$effectiveWorkspaceRoots = @($AllowedWorkspaceRoots | Where-Object { -not [string]::IsNullOrWhiteSpace($_) -and (Test-Path -LiteralPath $_) })
if ($effectiveWorkspaceRoots.Count -eq 0) {
    throw "At least one existing workspace root is required."
}
New-Item -ItemType Directory -Path $configDir -Force | Out-Null
[ordered]@{
    version = 1
    fullPcAccess = $effectiveFullPcAccess
    pcAccessRoots = $effectivePcAccessRoots
    allowBroadWorkspaceRoots = $true
    allowedWorkspaceRoots = $effectiveWorkspaceRoots
    newProjectRoot = $NewProjectRoot
    codexMonitorMode = $CodexMonitorMode
    updatedAt = (Get-Date).ToUniversalTime().ToString("o")
} | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $configPath -Encoding UTF8

$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

# The agent reads the token file itself so no credential appears in Task Scheduler
# arguments or the node.exe process command line.
$action = New-ScheduledTaskAction `
    -Execute $nodePath `
    -Argument "`"$agentPath`" `"$ServerUrl`"" `
    -WorkingDirectory $scriptDir

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 0) `
    -MultipleInstances IgnoreNew `
    -RestartCount 20 `
    -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Relays local desktop and workspace context to Marcus production" `
    -RunLevel Limited | Out-Null

if ($StartNow) {
    Start-ScheduledTask -TaskName $taskName
}

[pscustomobject]@{
    TaskName = $taskName
    ServerUrl = $ServerUrl
    TokenSource = $TokenFile
    ConfigPath = $configPath
    FullPcAccess = $effectiveFullPcAccess
    PcAccessRoots = $effectivePcAccessRoots
    AllowedWorkspaceRoots = $effectiveWorkspaceRoots
    Started = [bool]$StartNow
}

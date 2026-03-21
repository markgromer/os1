# ─────────────────────────────────────────────────────────────
# Install M.A.R.C.U.S. Desktop Agent as a Windows startup task
# ─────────────────────────────────────────────────────────────
# Run this ONCE from PowerShell (inside the Task Tracker folder):
#   powershell -ExecutionPolicy Bypass -File install-desktop-agent.ps1
#
# It creates a Windows Scheduled Task that launches the desktop
# agent at login, running hidden in the background. The agent
# relays your active window info to the Render server every 5s.
#
# To uninstall:
#   Unregister-ScheduledTask -TaskName "MARCUS-DesktopAgent" -Confirm:$false
# ─────────────────────────────────────────────────────────────

$ErrorActionPreference = "Stop"

# --- Config ---
$serverUrl = "https://os1-q78n.onrender.com"
$taskName  = "MARCUS-DesktopAgent"
$scriptDir = $PSScriptRoot
$agentPath = Join-Path $scriptDir "desktop-agent.cjs"

if (-not (Test-Path $agentPath)) {
    Write-Host "ERROR: desktop-agent.cjs not found at $agentPath" -ForegroundColor Red
    exit 1
}

# Prompt for the admin token (stored in the task, not visible in plain text in task scheduler)
$token = Read-Host "Enter your ADMIN_TOKEN for the Render server" -AsSecureString
$bstr  = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($token)
$plain = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
[System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)

if (-not $plain) {
    Write-Host "ERROR: Token cannot be empty." -ForegroundColor Red
    exit 1
}

# Find node.exe
$nodePath = (Get-Command node -ErrorAction SilentlyContinue).Source
if (-not $nodePath) {
    Write-Host "ERROR: node.exe not found in PATH." -ForegroundColor Red
    exit 1
}

# Remove existing task if present
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "Removed existing scheduled task." -ForegroundColor Yellow
}

# Create the task
$action  = New-ScheduledTaskAction `
    -Execute $nodePath `
    -Argument "`"$agentPath`" `"$serverUrl`" `"$plain`"" `
    -WorkingDirectory $scriptDir

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 0) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Relays desktop context to M.A.R.C.U.S. on Render" `
    -RunLevel Limited | Out-Null

Write-Host ""
Write-Host "Desktop Agent installed as startup task: $taskName" -ForegroundColor Green
Write-Host "It will start automatically at your next login." -ForegroundColor Green
Write-Host ""
Write-Host "To start it now:  Start-ScheduledTask -TaskName '$taskName'" -ForegroundColor Cyan
Write-Host "To stop it:       Stop-ScheduledTask  -TaskName '$taskName'" -ForegroundColor Cyan
Write-Host "To uninstall:     Unregister-ScheduledTask -TaskName '$taskName' -Confirm:`$false" -ForegroundColor Cyan

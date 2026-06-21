# Job scheduler using Windows Task Scheduler — Run job every 2 hours
#
# Usage (PowerShell as Administrator):
#   .\scripts\schedule_job.ps1 -Action install
#   .\scripts\schedule_job.ps1 -Action list
#   .\scripts\schedule_job.ps1 -Action remove
#   .\scripts\schedule_job.ps1 -Action test

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet('install', 'list', 'remove', 'test', 'help')]
    [string]$Action = 'help'
)

$scriptDir = Split-Path -Parent $PSScriptRoot
$jobScript = Join-Path $scriptDir "scripts\run_job.py"
$logDir = Join-Path $scriptDir "datalake\_logs"
$logFile = Join-Path $logDir "scheduler.log"

# Ensure log directory exists
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

function Install-Job {
    Write-Host "Installing Windows Task Scheduler job (every 2 hours)..."

    # Check if running as administrator
    $currentUser = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentUser)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        Write-Error "ERROR: Must run as Administrator"
        exit 1
    }

    $taskName = "ETL-Pipeline-Job"
    $taskPath = "\Exalt\ETL\"
    $fullTaskName = "$taskPath$taskName"

    # Remove existing task if it exists
    $existingTask = Get-ScheduledTask -TaskPath $taskPath -TaskName $taskName -ErrorAction SilentlyContinue
    if ($existingTask) {
        Write-Host "  Removing existing task..."
        Unregister-ScheduledTask -TaskPath $taskPath -TaskName $taskName -Confirm:$false
    }

    # Create task trigger (every 2 hours, starting at midnight)
    $trigger = New-ScheduledTaskTrigger `
        -At "00:00:00" `
        -RepetitionInterval (New-TimeSpan -Hours 2) `
        -RepetitionDuration (New-TimeSpan -Days 365) `
        -Daily

    # Create action
    $action = New-ScheduledTaskAction `
        -Execute "python" `
        -Argument """$jobScript"" --with-silver-gold" `
        -WorkingDirectory $scriptDir

    # Create task settings
    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -RunOnlyIfNetworkAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -DontStopOnIdleEnd

    # Register task
    Register-ScheduledTask `
        -TaskName $taskName `
        -TaskPath $taskPath `
        -Trigger $trigger `
        -Action $action `
        -Settings $settings `
        -RunLevel Highest | Out-Null

    Write-Host "✓ Task installed: $fullTaskName"
    Write-Host "  Schedule: Every 2 hours (00:00, 02:00, 04:00, ...)"
    Write-Host "  Log: $logFile"
    Write-Host ""
    Write-Host "To view task in Task Scheduler:"
    Write-Host "  taskmgr.exe → Task Scheduler → Exalt → ETL → ETL-Pipeline-Job"
}

function List-Jobs {
    Write-Host "Current scheduled tasks:"
    $taskPath = "\Exalt\ETL\"
    $tasks = Get-ScheduledTask -TaskPath $taskPath -ErrorAction SilentlyContinue
    if ($tasks) {
        $tasks | Format-Table -Property TaskName, @{Name="Status"; Expression={$_.State}} -AutoSize
    } else {
        Write-Host "  No tasks found"
    }
}

function Remove-Job {
    Write-Host "Removing Windows Task Scheduler job..."

    # Check if running as administrator
    $currentUser = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentUser)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        Write-Error "ERROR: Must run as Administrator"
        exit 1
    }

    $taskName = "ETL-Pipeline-Job"
    $taskPath = "\Exalt\ETL\"
    $existingTask = Get-ScheduledTask -TaskPath $taskPath -TaskName $taskName -ErrorAction SilentlyContinue

    if ($existingTask) {
        Unregister-ScheduledTask -TaskPath $taskPath -TaskName $taskName -Confirm:$false
        Write-Host "✓ Task removed"
    } else {
        Write-Host "No task found to remove"
    }
}

function Test-Job {
    Write-Host "Testing job execution..."
    Write-Host ""

    Push-Location $scriptDir
    try {
        & python scripts\run_job.py --with-silver-gold
    } finally {
        Pop-Location
    }
}

function Show-Help {
    $help = @"
Job Scheduler — Étape 7 (Windows)

Usage: .\scripts\schedule_job.ps1 -Action <command>

Commands:
  install   Install Task Scheduler job (every 2 hours)
  list      List scheduled tasks
  remove    Remove scheduled task
  test      Test job execution (1 run)
  help      Show this help

Examples:
  .\scripts\schedule_job.ps1 -Action install   # Install scheduler
  .\scripts\schedule_job.ps1 -Action test      # Test 1 execution
  .\scripts\schedule_job.ps1 -Action list      # See scheduled tasks
  .\scripts\schedule_job.ps1 -Action remove    # Stop scheduler

IMPORTANT: Run as Administrator (right-click PowerShell → Run as administrator)

View in GUI:
  Press Win+R → taskschd.msc → Exalt\ETL\ETL-Pipeline-Job

Log:
  $logFile
"@
    Write-Host $help
}

switch ($Action) {
    'install' {
        Install-Job
    }
    'list' {
        List-Jobs
    }
    'remove' {
        Remove-Job
    }
    'test' {
        Test-Job
    }
    'help' {
        Show-Help
    }
    default {
        Write-Error "Unknown action: $Action"
        Show-Help
        exit 1
    }
}

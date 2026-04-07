# =============================================================
# setup_scheduler.ps1
# Run this ONCE to register the daily refresh task in Windows
# Task Scheduler. After that it runs automatically every day.
# =============================================================

$TASK_NAME  = "MapboxReportRefresh"
$SCRIPT     = "C:\Users\H0B08S2\Documents\puppy_workspace\mapbox_dashboard\refresh_report.ps1"
# Runs at 9 AM daily - adjust to fire after your BQ scheduled query
$RUN_AT     = "09:00"

$action  = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NonInteractive -ExecutionPolicy Bypass -File `"$SCRIPT`""

$trigger = New-ScheduledTaskTrigger -Daily -At $RUN_AT

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

# Remove existing task if present
if (Get-ScheduledTask -TaskName $TASK_NAME -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TASK_NAME -Confirm:$false
    Write-Host "Removed existing task '$TASK_NAME'."
}

Register-ScheduledTask `
    -TaskName $TASK_NAME `
    -Action   $action `
    -Trigger  $trigger `
    -Settings $settings `
    -Description "Daily refresh of Mapbox HTML report from BigQuery -> GitHub Pages"

Write-Host ""
Write-Host "Task '$TASK_NAME' registered! It will run daily at $RUN_AT."
Write-Host "To run it right now:  Start-ScheduledTask -TaskName '$TASK_NAME'"
Write-Host "To view logs:         Get-Content '$((Split-Path $SCRIPT))\refresh.log' -Tail 30"
Write-Host "To remove the task:   Unregister-ScheduledTask -TaskName '$TASK_NAME' -Confirm:`$false"

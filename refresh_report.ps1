# =============================================================
# refresh_report.ps1
# Runs daily via Windows Task Scheduler (must be on Walmart VPN)
# Queries BigQuery, regenerates mapbox_report.html, pushes to
# GitHub Pages so the live link auto-updates.
# =============================================================

$ErrorActionPreference = "Stop"

$REPO_DIR  = "C:\Users\H0B08S2\Documents\puppy_workspace\mapbox_dashboard"
$LOG_FILE  = "$REPO_DIR\refresh.log"
# Token is stored in the git remote URL (set by setup), not here.

function Log($msg) {
    $ts = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
    "[$ts] $msg" | Tee-Object -FilePath $LOG_FILE -Append
}

Log "=== Mapbox report refresh started ==="

try {
    Set-Location $REPO_DIR

    # 1. Activate the project venv if present, otherwise use system python
    $venvPy = Join-Path $REPO_DIR ".venv\Scripts\python.exe"
    $python = if (Test-Path $venvPy) { $venvPy } else { "python" }
    Log "Using Python: $python"

    # 2. Make sure dependencies are installed
    Log "Installing/verifying dependencies..."
    & $python -m pip install -q `
        google-cloud-bigquery db-dtypes pyarrow `
        --index-url https://pypi.ci.artifacts.walmart.com/artifactory/api/pypi/external-pypi/simple `
        --allow-insecure-host pypi.ci.artifacts.walmart.com

    # 3. Regenerate the HTML from BigQuery
    Log "Running generate_report.py..."
    & $python generate_report.py
    if ($LASTEXITCODE -ne 0) { throw "generate_report.py exited with code $LASTEXITCODE" }
    Log "HTML generated successfully."

    # 4. Commit and push if the file changed
    $status = git status --porcelain mapbox_report.html
    if ($status) {
        Log "mapbox_report.html changed — committing and pushing..."
        git add mapbox_report.html
        $date = (Get-Date -Format "yyyy-MM-dd HH:mm UTC")
        git commit -m "chore: auto-refresh mapbox report [$date]"

        # Token is embedded in the 'origin' remote URL - no credentials needed here
        git push origin main
        Log "Pushed to GitHub Pages successfully."
    } else {
        Log "No changes in mapbox_report.html — nothing to push."
    }

} catch {
    Log "ERROR: $_"
    exit 1
}

Log "=== Refresh complete ==="

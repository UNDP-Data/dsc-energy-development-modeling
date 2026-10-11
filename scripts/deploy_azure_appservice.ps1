# Deploy the EDIM API + UI to Azure App Service (undphq220api001).
#
# Builds a staging tree that matches the layout the backend expects at the
# site root (/home/site/wwwroot):
#
#   api_service/                  FastAPI application package
#   migrations/ + alembic.ini     Alembic migrations (run at startup)
#   requirements.txt              API-ONLY deps (from backend/requirements-api.txt)
#   frontend/                     Static UI served at /ui
#   inputs/                       Runtime config, CSVs, generated scenario data
#   model_runtime/edim_model/     Model runtime package + manifests
#   Energy Modelling Scenario Report.docx
#
# IMPORTANT: the solver stack (calliope, pyomo, highspy, xarray, netCDF4) is
# deliberately NOT installed here. Model execution runs on the isolated worker
# VM (Phase 2). Deploying the full backend/requirements.txt to App Service
# would waste build time and memory on dependencies the API never imports.
#
# DEPENDENCY STRATEGY: follow Microsoft's Python ZIP-deployment guide.
# Include requirements.txt at the ZIP root and enable Oryx build automation
# so App Service installs API dependencies in its Linux environment.
# The model solver stack is deliberately excluded; it belongs on the worker VM.
#
# Heavy local-only directories are excluded (venvs, node_modules, caches).
# This script only READS the repository; it never modifies project sources.
#
# Usage:
#   powershell -File scripts/deploy_azure_appservice.ps1
#
# Options:
#   -AppName           (default: undphq220api001)
#   -SkipDeploy        build the zip only (useful for inspection)
#   -AuthMode          (default: local_password; pass test_header to disable login)
#   -AuthAdminEmail    (default: daniele.ciminieri@gmail.com; guaranteed admin account)
#   -AuthSeedPassword  (optional; activates the admin with this password on first boot)
#   -AuthSessionSecret (optional; auto-generated and preserved in app settings otherwise)
#   -AuthEmailConnectionString (optional; ACS CommunicationService connection string for reset e-mails)
#   -AuthEmailSender   (optional; verified sender, e.g. DoNotReply@<guid>.azurecomm.net)
#   -AuthEmailBaseUrl  (optional; public base URL used in reset links; defaults to request origin)

[CmdletBinding()]
param(
    [string]$AppName = "undphq220api001",
    [switch]$SkipDeploy,
    [string]$AuthMode = "local_password",
    [string]$AuthAdminEmail = "daniele.ciminieri@gmail.com",
    [string]$AuthSeedPassword = "",
    [string]$AuthSessionSecret = "",
    [string]$AuthEmailConnectionString = "",
    [string]$AuthEmailSender = "",
    [string]$AuthEmailBaseUrl = ""
)

$ErrorActionPreference = "Stop"
$ResourceGroup = "undphq220rg03"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$WorkDir = Join-Path $RepoRoot "outputs\azure-deploy"
$StageDir = Join-Path $WorkDir "staging"
$ZipPath = Join-Path $WorkDir "edim-api.zip"
Write-Host "Repo root : $RepoRoot"
Write-Host "Staging   : $StageDir"
Write-Host "Zip       : $ZipPath"

if (Test-Path $StageDir) { Remove-Item $StageDir -Recurse -Force }
if (Test-Path $ZipPath) { Remove-Item $ZipPath -Force }
New-Item -ItemType Directory -Path $StageDir -Force | Out-Null

function Copy-Tree {
    param(
        [Parameter(Mandatory = $true)][string]$Source,
        [Parameter(Mandatory = $true)][string]$Destination,
        [string[]]$ExcludeDirs = @()
    )
    if (-not (Test-Path $Source)) {
        Write-Warning "Missing source, skipped: $Source"
        return
    }
    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
    $robocopyArgs = @($Source, $Destination, "/E", "/NFL", "/NDL", "/NJH", "/NJS", "/NP", "/R:1", "/W:1")
    if ($ExcludeDirs.Count -gt 0) { $robocopyArgs += "/XD"; $robocopyArgs += $ExcludeDirs }
    & robocopy @robocopyArgs | Out-Null
    if ($LASTEXITCODE -ge 8) {
        throw "robocopy failed for $Source (exit $LASTEXITCODE)"
    }
    $global:LASTEXITCODE = 0
}

$CommonExcludes = @("__pycache__", "*.pyc", ".pytest_cache", ".mypy_cache")

# API-only requirements (no calliope / pyomo / highspy / xarray / netCDF4).
$ApiRequirements = Join-Path $RepoRoot "backend\requirements-api.txt"
if (-not (Test-Path $ApiRequirements)) {
    throw "Missing backend/requirements-api.txt - refusing to deploy the full solver stack to App Service."
}

Write-Host "`n[1/4] Copying backend application package..."
Copy-Tree -Source (Join-Path $RepoRoot "backend\api_service") -Destination (Join-Path $StageDir "api_service") -ExcludeDirs $CommonExcludes
Copy-Tree -Source (Join-Path $RepoRoot "backend\migrations") -Destination (Join-Path $StageDir "migrations") -ExcludeDirs $CommonExcludes
Copy-Item (Join-Path $RepoRoot "backend\alembic.ini") (Join-Path $StageDir "alembic.ini") -Force
Copy-Item $ApiRequirements (Join-Path $StageDir "requirements.txt") -Force

Write-Host "[2/4] Copying frontend, inputs, and model runtime..."
Copy-Tree -Source (Join-Path $RepoRoot "frontend") -Destination (Join-Path $StageDir "frontend") -ExcludeDirs @("node_modules", "dist", "tests", ".vite")
Copy-Tree -Source (Join-Path $RepoRoot "inputs") -Destination (Join-Path $StageDir "inputs") -ExcludeDirs $CommonExcludes
Copy-Tree -Source (Join-Path $RepoRoot "model_runtime\edim_model") -Destination (Join-Path $StageDir "model_runtime\edim_model") -ExcludeDirs $CommonExcludes

# Calliope CONFIG only, never the data.
#
# The API needs Calliope-Africa-main/overrides.yaml to build the scenario
# catalog served by /api/scenarios (edim_model.core.scenarios reads the
# `scenarios:` keys). Without it that endpoint returns 500 and the UI scenario
# picker is empty.
#
# The `Timeseries/` directory is ~48.5 MB and is needed ONLY to solve a model,
# which happens exclusively on the worker VM. Excluding it keeps the App Service
# package small while still serving the catalog. Everything else here (the YAML
# overrides and the small config folders) is ~0.75 MB.
Copy-Tree `
    -Source (Join-Path $RepoRoot "model_runtime\model_modules\calliope\Calliope-Africa-main") `
    -Destination (Join-Path $StageDir "model_runtime\model_modules\calliope\Calliope-Africa-main") `
    -ExcludeDirs ($CommonExcludes + @("Timeseries"))

$ReportDoc = Join-Path $RepoRoot "Energy Modelling Scenario Report.docx"
if (Test-Path $ReportDoc) {
    Copy-Item $ReportDoc (Join-Path $StageDir "Energy Modelling Scenario Report.docx") -Force
} else {
    Write-Warning "Scenario report docx not found at repo root; skipping."
}

Write-Host "[3/4] Creating zip archive..."

# DO NOT use Compress-Archive here. On Windows PowerShell 5.1 it writes zip
# entries with BACKSLASH separators. Linux does not treat '\' as a path
# separator, so every file extracts as a single flat file whose name contains
# backslashes - no directory tree is created. That produced
# "ModuleNotFoundError: No module named 'api_service'" and rsync
# "failed to stat ... Invalid argument (22)" on the platform.
#
# Build the archive explicitly with forward-slash entry names instead.
function New-ZipFromDirectory {
    param(
        [Parameter(Mandatory = $true)][string]$SourceDir,
        [Parameter(Mandatory = $true)][string]$DestinationZip
    )
    Add-Type -AssemblyName System.IO.Compression | Out-Null
    Add-Type -AssemblyName System.IO.Compression.FileSystem | Out-Null

    if (Test-Path $DestinationZip) { Remove-Item $DestinationZip -Force }

    $source = (Resolve-Path $SourceDir).Path.TrimEnd('\')
    $zip = [System.IO.Compression.ZipFile]::Open(
        $DestinationZip,
        [System.IO.Compression.ZipArchiveMode]::Create
    )
    try {
        $files = Get-ChildItem -Path $source -Recurse -File -Force
        foreach ($file in $files) {
            $relative = $file.FullName.Substring($source.Length + 1).Replace('\', '/')
            [void][System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                $zip,
                $file.FullName,
                $relative,
                [System.IO.Compression.CompressionLevel]::Optimal
            )
        }
    } finally {
        $zip.Dispose()
    }
}

New-ZipFromDirectory -SourceDir $StageDir -DestinationZip $ZipPath

# Sanity check: every entry must use forward slashes and no leading drive letter.
$check = [System.IO.Compression.ZipFile]::OpenRead($ZipPath)
try {
    $bad = @($check.Entries | Where-Object { $_.FullName -match '\\' -or $_.FullName -match '^[A-Za-z]:' })
    if ($bad.Count -gt 0) {
        throw "Zip contains $($bad.Count) entries with Windows path separators, e.g. '$($bad[0].FullName)'"
    }
    Write-Host "      Entries: $($check.Entries.Count) (all forward-slash)"
} finally {
    $check.Dispose()
}

$zipMb = [math]::Round((Get-Item $ZipPath).Length / 1MB, 2)
Write-Host "Zip size: $zipMb MB"

if ($SkipDeploy) {
    Write-Host "`n-SkipDeploy set; stopping before az webapp deploy."
    exit 0
}

Write-Host "[4/4] Deploying to $AppName ($ResourceGroup)..."

# Follow the Microsoft Python ZIP-deploy guide: Oryx installs requirements.
# Remove WEBSITE_RUN_FROM_PACKAGE if an earlier deployment attempt enabled it;
# this deployment uses the normal extracted ZIP layout.
#
# NOTE: this must be a single `delete`. A stray duplicated `... appsettings set`
# line previously sat above it; the trailing backtick continuation joined the
# two into `appsettings set az webapp config appsettings delete ...`, which az
# rejected with "unrecognized arguments" and the delete never ran.
az webapp config appsettings delete `
    --resource-group $ResourceGroup `
    --name $AppName `
    --setting-names WEBSITE_RUN_FROM_PACKAGE `
    --output none

az webapp config appsettings set `
    --resource-group $ResourceGroup `
    --name $AppName `
    --settings "SCM_DO_BUILD_DURING_DEPLOYMENT=true" "WEBSITES_PORT=8000" "EDIM_RUNS_DIR=/home/edim-data/runs" "PYTHONPATH=/home/site/wwwroot:/home/site/wwwroot/model_runtime" `
    --output none

if ($AuthSessionSecret) {
    $sessionSecret = $AuthSessionSecret
} else {
    # NOTE: casting an empty command result via ([string](az ...)).Trim() gives
    # $null in Windows PowerShell 5.1 and crashes .Trim() - assign then test.
    $existingSecret = az webapp config appsettings list `
        --resource-group $ResourceGroup `
        --name $AppName `
        --query "[?name=='EDIM_AUTH_SESSION_SECRET'].value | [0]" `
        --output tsv
    $sessionSecret = if ($existingSecret) { [string]$existingSecret } else { "" }
}
if (-not $sessionSecret) {
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $secretBytes = New-Object byte[] 32
        $rng.GetBytes($secretBytes)
    } finally {
        $rng.Dispose()
    }
    $sessionSecret = [Convert]::ToBase64String($secretBytes)
    Write-Host "Generated EDIM_AUTH_SESSION_SECRET (stored in app settings; pass -AuthSessionSecret to pin it)."
}

az webapp config appsettings set `
    --resource-group $ResourceGroup `
    --name $AppName `
    --settings "EDIM_AUTH_MODE=$AuthMode" "EDIM_AUTH_ADMIN_EMAIL=$AuthAdminEmail" "EDIM_AUTH_SESSION_SECRET=$sessionSecret" `
    --output none

if ($AuthSeedPassword) {
    az webapp config appsettings set `
        --resource-group $ResourceGroup `
        --name $AppName `
        --settings "EDIM_AUTH_SEED_PASSWORD=$AuthSeedPassword" `
        --output none
}

if ($AuthEmailConnectionString -and $AuthEmailSender) {
    az webapp config appsettings set `
        --resource-group $ResourceGroup `
        --name $AppName `
        --settings "EDIM_AUTH_EMAIL_CONNECTION_STRING=$AuthEmailConnectionString" "EDIM_AUTH_EMAIL_SENDER=$AuthEmailSender" "EDIM_AUTH_DEV_TOKEN_DISPLAY=false" `
        --output none
    if ($AuthEmailBaseUrl) {
        az webapp config appsettings set `
            --resource-group $ResourceGroup `
            --name $AppName `
            --settings "EDIM_AUTH_EMAIL_BASE_URL=$AuthEmailBaseUrl" `
            --output none
    }
    Write-Host "Reset e-mails enabled via ACS sender $AuthEmailSender."
}

az webapp config set `
    --resource-group $ResourceGroup `
    --name $AppName `
    --startup-file "python -m uvicorn api_service.main:app --host 0.0.0.0 --port 8000" `
    --output none

az webapp deploy `
    --resource-group $ResourceGroup `
    --name $AppName `
    --src-path $ZipPath `
    --type zip `
    --async true `
    --output none

if ($LASTEXITCODE -ne 0) { throw "az webapp deploy failed (exit $LASTEXITCODE)" }

Write-Host "`nDeployed. App URL: https://$AppName.azurewebsites.net"
Write-Host "UI:  https://$AppName.azurewebsites.net/ui/"
Write-Host "Auth mode : $AuthMode"
Write-Host "Admin user: $AuthAdminEmail"
if ($AuthSeedPassword) {
    Write-Host "Admin password: EDIM_AUTH_SEED_PASSWORD is set; sign in with it and change it after first sign-in."
} else {
    Write-Host "Admin account starts inactive. Open the one-time invite link from the log stream:"
    Write-Host "  az webapp log tail --resource-group $ResourceGroup --name $AppName"
}

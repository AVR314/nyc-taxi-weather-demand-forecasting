[CmdletBinding()]
param(
    [switch]$KeepRunning,
    [switch]$ValidateRestart
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ComposeBase = @("compose", "--env-file", ".env")
$Started = $false

Set-Location $ProjectRoot

function Invoke-Compose {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)

    & docker @ComposeBase @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $($Arguments -join ' ')"
    }
}

function Get-ContainerId {
    param([string]$Service)

    $ContainerId = (& docker @ComposeBase ps --all --quiet $Service | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $ContainerId) {
        throw "No container found for Compose service '$Service'."
    }
    return $ContainerId
}

function Wait-ForHealthyService {
    param(
        [string]$Service,
        [int]$TimeoutSeconds = 180
    )

    $ContainerId = Get-ContainerId $Service
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $State = (& docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $ContainerId | Out-String).Trim()
        if ($LASTEXITCODE -ne 0) {
            throw "Could not inspect Compose service '$Service'."
        }
        if ($State -eq "healthy") {
            return
        }
        if ($State -in @("dead", "exited", "unhealthy")) {
            throw "Compose service '$Service' reached state '$State'."
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $Deadline)

    throw "Timed out waiting for Compose service '$Service' to become healthy."
}

function Wait-ForSuccessfulInit {
    param([int]$TimeoutSeconds = 180)

    $ContainerId = Get-ContainerId "minio-init"
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $State = (& docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' $ContainerId | Out-String).Trim()
        if ($State -eq "exited 0") {
            return
        }
        if ($State -match '^exited ' -and $State -ne "exited 0") {
            throw "MinIO initialization failed with state '$State'."
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $Deadline)

    throw "Timed out waiting for MinIO initialization."
}

function Wait-ForStack {
    Wait-ForHealthyService "minio"
    Wait-ForSuccessfulInit
    Wait-ForHealthyService "spark-master"
    Wait-ForHealthyService "spark-worker"
}

function Invoke-RoundTrip {
    Invoke-Compose exec --no-TTY minio curl --fail --silent http://localhost:9000/minio/health/ready
    Invoke-Compose exec --no-TTY spark-master /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        /opt/project/scripts/smoke_test_spark_minio.py
}

if (-not (Test-Path -LiteralPath ".env")) {
    throw "Missing .env. Copy .env.example to .env and set local MinIO credentials."
}

try {
    Invoke-Compose config --quiet
    $Started = $true
    Invoke-Compose up --detach --build
    Wait-ForStack
    Invoke-RoundTrip

    if ($ValidateRestart) {
        Invoke-Compose restart minio spark-master spark-worker
        Wait-ForHealthyService "minio"
        Wait-ForHealthyService "spark-master"
        Wait-ForHealthyService "spark-worker"
        Invoke-RoundTrip
    }

    Write-Output "INFRASTRUCTURE_SMOKE_OK restart_checked=$($ValidateRestart.IsPresent)"
}
finally {
    if ($Started -and -not $KeepRunning) {
        Invoke-Compose down --remove-orphans
    }
}

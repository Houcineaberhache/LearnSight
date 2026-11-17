<#
.SYNOPSIS
    Lance le pipeline d'ingestion LearnSight (Fatima) sous Windows.

.DESCRIPTION
    1. Crée les topics Kafka s'ils n'existent pas (sauf -SkipTopics).
    2. Lance les producteurs VLE et Évaluations.

.EXAMPLE
    .\scripts\run_ingestion.ps1 -Limit 1000 -Delay 0.01

.EXAMPLE
    .\scripts\run_ingestion.ps1 -DryRun -Limit 50

.EXAMPLE
    .\scripts\run_ingestion.ps1 -SkipTopics
#>
[CmdletBinding()]
param(
    [string]$DataDir = "data/processed",
    [double]$Delay = 0.01,
    [int]$Limit = 0,
    [switch]$DryRun,
    [switch]$SkipTopics,
    [switch]$Validate
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot

try {
    # Résolution de l'interpréteur Python (venv prioritaire)
    $python = Join-Path $projectRoot "venv\Scripts\python.exe"
    if (-not (Test-Path $python)) {
        $python = Join-Path $projectRoot ".venv\Scripts\python.exe"
    }
    if (-not (Test-Path $python)) {
        $python = "python"
    }
    Write-Host "Python : $python"

    if (-not $SkipTopics) {
        Write-Host "Création / vérification des topics Kafka..."
        & $python -m ingestion.producers.kafka_admin --create --list
        if ($LASTEXITCODE -ne 0) {
            throw "Impossible de créer les topics Kafka (code $LASTEXITCODE)."
        }
    }

    $producerArgs = @(
        "-m", "ingestion.producers.run_all_producers",
        "--data-dir", $DataDir,
        "--delay", "$Delay"
    )
    if ($Limit -gt 0) {
        $producerArgs += @("--limit", "$Limit")
    }
    if ($DryRun) {
        $producerArgs += "--dry-run"
    }
    if ($Validate) {
        $producerArgs += "--validate"
    }

    Write-Host "Lancement des producteurs..."
    & $python @producerArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}

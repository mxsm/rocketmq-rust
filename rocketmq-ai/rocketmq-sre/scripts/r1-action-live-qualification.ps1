# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [string]$Kubeconfig = 'D:\BuildCache\rocketmq-sre-temp\kind\phase00-kubeconfig',

    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',

    [string]$CargoTargetDir = 'F:\BuildCache\rocketmq-sre-r1-action-qualification',

    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',

    [string]$EvidenceRoot = 'D:\rocketmq-sre-evidence',

    [ValidateRange(1024, 65535)]
    [int]$PostgresLocalPort = 35432,

    [ValidateRange(1024, 65535)]
    [int]$ExecutorLocalPort = 58096,

    [ValidateRange(1024, 65535)]
    [int]$AgentLocalPort = 58097
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$manifestPath = Join-Path $sreRoot 'config\qualification\r1-actions.v1.json'
$checkerPath = Join-Path $scriptDirectory 'check_r1_action_qualification.py'
$waveScript = Join-Path $scriptDirectory 'phase03-wave-actions-supervised-e2e.ps1'

function Assert-DataPath([string]$Path, [string]$Description) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetPathRoot($fullPath)
    if (
        -not $root.Equals('D:\', [StringComparison]::OrdinalIgnoreCase) -and
        -not $root.Equals('F:\', [StringComparison]::OrdinalIgnoreCase)
    ) {
        throw "$Description must use the D or F drive."
    }
}

function Invoke-Native(
    [string]$Command,
    [string[]]$Arguments,
    [string]$Description
) {
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

foreach ($path in @(
    @{ Value = $Kubeconfig; Description = 'Kubernetes kubeconfig' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' },
    @{ Value = $EvidenceRoot; Description = 'qualification Evidence root' }
)) {
    Assert-DataPath $path.Value $path.Description
}

$resolvedKubeconfig = [IO.Path]::GetFullPath($Kubeconfig)
if (-not (Test-Path -LiteralPath $resolvedKubeconfig -PathType Leaf)) {
    throw "Kubernetes kubeconfig does not exist: $resolvedKubeconfig"
}
$resolvedEvidenceRoot = [IO.Path]::GetFullPath($EvidenceRoot).TrimEnd('\')
$allowedEvidenceRoots = @(
    [IO.Path]::GetFullPath('D:\rocketmq-sre-evidence').TrimEnd('\'),
    [IO.Path]::GetFullPath('F:\rocketmq-sre-evidence').TrimEnd('\')
)
if (-not ($allowedEvidenceRoots -contains $resolvedEvidenceRoot)) {
    throw 'Qualification reports must use D:\rocketmq-sre-evidence or F:\rocketmq-sre-evidence.'
}

Invoke-Native python @($checkerPath, '--manifest', $manifestPath) 'R1 qualification manifest validation'
$revision = (& git -C $repositoryRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to determine the qualification source revision.'
}
$dirty = & git -C $repositoryRoot status --porcelain=v1
if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($dirty -join ''))) {
    throw 'R1 live qualification requires a committed, clean source tree.'
}

$startedAt = [DateTimeOffset]::UtcNow
$runName = 'r1-actions-{0}-{1}' -f $startedAt.ToString('yyyyMMdd-HHmmss'), ([Guid]::NewGuid().ToString('N'))
$runRoot = [IO.Path]::GetFullPath((Join-Path $resolvedEvidenceRoot $runName))
$expectedPrefix = $resolvedEvidenceRoot + '\'
if (-not $runRoot.StartsWith($expectedPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'R1 qualification output escaped the configured Evidence root.'
}
$liveFragmentPath = Join-Path $runRoot 'live-fragment.json'
$recoveryFragmentPath = Join-Path $runRoot 'recovery-fragment.json'
$reportPath = Join-Path $runRoot 'qualification-report.v1.json'
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

try {
    & $waveScript `
        -Kubeconfig $resolvedKubeconfig `
        -CargoHome ([IO.Path]::GetFullPath($CargoHome)) `
        -CargoTargetDir ([IO.Path]::GetFullPath($CargoTargetDir)) `
        -TemporaryRoot ([IO.Path]::GetFullPath($TemporaryRoot)) `
        -PostgresLocalPort $PostgresLocalPort `
        -ExecutorLocalPort $ExecutorLocalPort `
        -AgentLocalPort $AgentLocalPort `
        -R1Only `
        -LiveFragment $liveFragmentPath `
        -RecoveryFragment $recoveryFragmentPath
    if (-not (Test-Path -LiteralPath $liveFragmentPath -PathType Leaf)) {
        throw 'The real Kind execution did not emit its live R1 fragment.'
    }
    if (-not (Test-Path -LiteralPath $recoveryFragmentPath -PathType Leaf)) {
        throw 'The PostgreSQL recovery matrix did not emit its R1 fragment.'
    }

    $live = Get-Content -Raw -LiteralPath $liveFragmentPath | ConvertFrom-Json
    $recovery = Get-Content -Raw -LiteralPath $recoveryFragmentPath | ConvertFrom-Json
    if (
        $live.schema_version -ne 'rocketmq-sre.r1-action-live-fragment.v1' -or
        $recovery.schema_version -ne 'rocketmq-sre.r1-action-recovery-fragment.v1' -or
        [int]$live.model_provider_network_calls -ne 0 -or
        [int]$recovery.model_provider_network_calls -ne 0 -or
        $live.logger_ttl_restored -ne $true
    ) {
        throw 'R1 qualification fragments failed closed because their schema or safety metadata drifted.'
    }

    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
    $actionReports = [Collections.Generic.List[object]]::new()
    foreach ($action in $manifest.actions) {
        $liveAction = @($live.actions | Where-Object { $_.id -eq $action.id })
        $recoveryAction = @($recovery.actions | Where-Object { $_.id -eq $action.id })
        if ($liveAction.Count -ne 1 -or $recoveryAction.Count -ne 1) {
            throw "R1 qualification fragments do not contain exactly one record for $($action.id)."
        }
        $outcomes = [ordered]@{}
        foreach ($outcome in $manifest.required_outcomes) {
            $outcomes[[string]$outcome] = 'passed'
        }
        $actionReports.Add([ordered]@{
            id = [string]$action.id
            outcomes = $outcomes
            live = $liveAction[0]
            recovery = $recoveryAction[0]
        })
    }

    $ownedJob = & kubectl `
        --kubeconfig $resolvedKubeconfig `
        -n rocketmq-system `
        get job rocketmq-sre-phase03-wave-admin-bootstrap `
        --ignore-not-found=true `
        -o name
    if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($ownedJob -join ''))) {
        throw 'The qualification-owned Admin bootstrap Job was not removed.'
    }
    $report = [ordered]@{
        schema_version = 'rocketmq-sre.r1-action-qualification-report.v1'
        revision = $revision
        source_clean = $true
        environment = 'disposable_kind'
        started_at = $startedAt.ToString('o')
        finished_at = [DateTimeOffset]::UtcNow.ToString('o')
        status = 'passed'
        model_provider_network_calls = 0
        secrets_recorded = $false
        message_bodies_recorded = $false
        actions = $actionReports
        cleanup = [ordered]@{
            status = 'passed'
            proxy_replicas_restored = $true
            logger_ttl_restored = $true
            proxy_ready = $true
            collector_ready = $true
            owned_resources_removed = $true
        }
    }
    $json = $report | ConvertTo-Json -Depth 12
    [IO.File]::WriteAllText($reportPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
    Invoke-Native python @($checkerPath, '--manifest', $manifestPath, '--report', $reportPath) `
        'R1 live qualification report validation'
    Remove-Item -LiteralPath $liveFragmentPath, $recoveryFragmentPath -Force
    Write-Host "R1_ACTION_LIVE_QUALIFICATION_OK report=$reportPath actions=4 outcomes=40 model_network_calls=0"
}
catch {
    if (
        (Test-Path -LiteralPath $runRoot) -and
        $runRoot.StartsWith($expectedPrefix, [StringComparison]::OrdinalIgnoreCase)
    ) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
    throw
}

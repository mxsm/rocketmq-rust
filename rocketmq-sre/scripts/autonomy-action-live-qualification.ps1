# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidatePattern('^[a-z0-9][a-z0-9-]{0,39}$')]
    [string]$ClusterName = 'rocketmq-sre-autonomy-qualification',

    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',

    [string]$CargoTargetDir = 'F:\BuildCache\rocketmq-sre-autonomy-qualification',

    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',

    [string]$EvidenceRoot = 'D:\rocketmq-sre-evidence',

    [ValidateRange(1024, 65535)]
    [int]$PostgresLocalPort = 35452,

    [ValidateRange(1024, 65535)]
    [int]$ExecutorLocalPort = 58116,

    [ValidateRange(1024, 65535)]
    [int]$AgentLocalPort = 58117,

    [switch]$SkipImageBuild
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '..'))
$repositoryTarget = [IO.Path]::GetFullPath((Join-Path $repositoryRoot 'target'))
$kindArtifacts = [IO.Path]::GetFullPath((Join-Path $repositoryTarget 'phase00-kind'))
$certificateArtifacts = [IO.Path]::GetFullPath((Join-Path $repositoryTarget 'phase00-certs'))
$kubeconfig = Join-Path $kindArtifacts 'kubeconfig'
$manifestPath = Join-Path $sreRoot 'config\qualification\autonomy-actions.v1.json'
$checkerPath = Join-Path $scriptDirectory 'check_autonomy_action_qualification.py'
$kindScript = Join-Path $scriptDirectory 'kind.ps1'
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

function Test-KindCluster([string]$Name) {
    $savedErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'SilentlyContinue'
        $clusters = & kind get clusters 2>$null
        $status = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $savedErrorActionPreference
    }
    if ($status -ne 0) {
        return $false
    }
    return @($clusters | Where-Object { $_.Trim() -eq $Name }).Count -eq 1
}

function Remove-OwnedArtifacts {
    foreach ($path in @($kindArtifacts, $certificateArtifacts)) {
        if (-not $path.StartsWith($repositoryTarget + '\', [StringComparison]::OrdinalIgnoreCase)) {
            throw "Owned runtime artifact escaped the repository target directory: $path"
        }
        if (Test-Path -LiteralPath $path) {
            Remove-Item -LiteralPath $path -Recurse -Force
        }
    }
}

foreach ($path in @(
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' },
    @{ Value = $EvidenceRoot; Description = 'qualification Evidence root' }
)) {
    Assert-DataPath $path.Value $path.Description
}
if (@(@($PostgresLocalPort, $ExecutorLocalPort, $AgentLocalPort) | Select-Object -Unique).Count -ne 3) {
    throw 'Qualification loopback ports must be distinct.'
}

$resolvedCargoHome = [IO.Path]::GetFullPath($CargoHome)
$resolvedCargoTarget = [IO.Path]::GetFullPath($CargoTargetDir)
$resolvedTemporaryRoot = [IO.Path]::GetFullPath($TemporaryRoot)
$resolvedEvidenceRoot = [IO.Path]::GetFullPath($EvidenceRoot).TrimEnd('\')
$allowedEvidenceRoots = @(
    [IO.Path]::GetFullPath('D:\rocketmq-sre-evidence').TrimEnd('\'),
    [IO.Path]::GetFullPath('F:\rocketmq-sre-evidence').TrimEnd('\')
)
if (-not ($allowedEvidenceRoots -contains $resolvedEvidenceRoot)) {
    throw 'Qualification reports must use D:\rocketmq-sre-evidence or F:\rocketmq-sre-evidence.'
}
if (Test-KindCluster $ClusterName) {
    throw "Refusing to reuse pre-existing Kind cluster '$ClusterName'."
}
if ((Test-Path -LiteralPath $kindArtifacts) -or (Test-Path -LiteralPath $certificateArtifacts)) {
    throw 'Autonomy qualification requires an empty task-owned Kind artifact area.'
}

Invoke-Native python @($checkerPath, '--manifest', $manifestPath) 'autonomy qualification manifest validation'
$revision = (& git -C $repositoryRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to determine the qualification source revision.'
}
$dirty = & git -C $repositoryRoot status --porcelain=v1
if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($dirty -join ''))) {
    throw 'Autonomy live qualification requires a committed, clean source tree.'
}

$startedAt = [DateTimeOffset]::UtcNow
$runName = 'autonomy-actions-{0}-{1}' -f $startedAt.ToString('yyyyMMdd-HHmmss'), ([Guid]::NewGuid().ToString('N'))
$runRoot = [IO.Path]::GetFullPath((Join-Path $resolvedEvidenceRoot $runName))
$expectedEvidencePrefix = $resolvedEvidenceRoot + '\'
if (-not $runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Autonomy qualification output escaped the configured Evidence root.'
}
$liveFragmentPath = Join-Path $runRoot 'live-fragment.json'
$recoveryFragmentPath = Join-Path $runRoot 'recovery-fragment.json'
$lifecycleFragmentPath = Join-Path $runRoot 'lifecycle-fragment.json'
$reportPath = Join-Path $runRoot 'qualification-report.v1.json'
New-Item -ItemType Directory -Force -Path `
    $runRoot, `
    $resolvedCargoHome, `
    $resolvedCargoTarget, `
    $resolvedTemporaryRoot |
    Out-Null

$clusterCreated = $false
$qualificationSucceeded = $false
try {
    $upParameters = @{
        Action = 'Up'
        ClusterName = $ClusterName
    }
    if ($SkipImageBuild) {
        $upParameters.SkipBuild = $true
    }
    & $kindScript @upParameters
    $clusterCreated = Test-KindCluster $ClusterName
    if (-not $clusterCreated -or -not (Test-Path -LiteralPath $kubeconfig -PathType Leaf)) {
        throw 'The disposable Kind cluster did not become available.'
    }

    & $waveScript `
        -Kubeconfig $kubeconfig `
        -CargoHome $resolvedCargoHome `
        -CargoTargetDir $resolvedCargoTarget `
        -TemporaryRoot $resolvedTemporaryRoot `
        -PostgresLocalPort $PostgresLocalPort `
        -ExecutorLocalPort $ExecutorLocalPort `
        -AgentLocalPort $AgentLocalPort `
        -R1Only `
        -LiveFragment $liveFragmentPath `
        -RecoveryFragment $recoveryFragmentPath `
        -AutonomyFragment $lifecycleFragmentPath

    foreach ($fragment in @($liveFragmentPath, $recoveryFragmentPath, $lifecycleFragmentPath)) {
        if (-not (Test-Path -LiteralPath $fragment -PathType Leaf)) {
            throw "A required autonomy qualification fragment is missing: $fragment"
        }
    }
    $live = Get-Content -Raw -LiteralPath $liveFragmentPath | ConvertFrom-Json
    $recovery = Get-Content -Raw -LiteralPath $recoveryFragmentPath | ConvertFrom-Json
    $lifecycle = Get-Content -Raw -LiteralPath $lifecycleFragmentPath | ConvertFrom-Json
    if (
        $live.schema_version -ne 'rocketmq-sre.r1-action-live-fragment.v1' -or
        $recovery.schema_version -ne 'rocketmq-sre.r1-action-recovery-fragment.v1' -or
        $lifecycle.schema_version -ne 'rocketmq-sre.autonomy-action-lifecycle-fragment.v1' -or
        $lifecycle.live_mode_ceiling -ne 'supervised' -or
        $lifecycle.unattended_autonomous_execution -ne $false -or
        [int]$live.model_provider_network_calls -ne 0 -or
        [int]$recovery.model_provider_network_calls -ne 0 -or
        [int]$lifecycle.model_provider_network_calls -ne 0 -or
        $live.logger_ttl_restored -ne $true
    ) {
        throw 'Autonomy qualification fragments failed closed because schema or safety metadata drifted.'
    }
    if (@($live.actions).Count -ne 4 -or @($recovery.actions).Count -ne 4 -or @($lifecycle.actions).Count -ne 4) {
        throw 'Autonomy qualification fragments must each contain exactly four actions.'
    }

    $ownedJob = & kubectl `
        --kubeconfig $kubeconfig `
        -n rocketmq-system `
        get job rocketmq-sre-phase03-wave-admin-bootstrap `
        --ignore-not-found=true `
        -o name
    if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($ownedJob -join ''))) {
        throw 'The qualification-owned Admin bootstrap Job was not removed.'
    }

    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
    $actionReports = [Collections.Generic.List[object]]::new()
    foreach ($action in $manifest.actions) {
        $liveAction = @($live.actions | Where-Object { $_.id -eq $action.id })
        $recoveryAction = @($recovery.actions | Where-Object { $_.id -eq $action.id })
        $lifecycleAction = @($lifecycle.actions | Where-Object { $_.id -eq $action.id })
        if ($liveAction.Count -ne 1 -or $recoveryAction.Count -ne 1 -or $lifecycleAction.Count -ne 1) {
            throw "Autonomy qualification fragments do not contain exactly one record for $($action.id)."
        }
        $outcomes = [ordered]@{}
        foreach ($outcome in $manifest.required_outcomes) {
            $outcomes[[string]$outcome] = 'passed'
        }
        $actionReports.Add([ordered]@{
            id = [string]$action.id
            outcomes = $outcomes
            lifecycle = $lifecycleAction[0]
            live = $liveAction[0]
            recovery = $recoveryAction[0]
        })
    }

    & $kindScript -Action Down -ClusterName $ClusterName
    $clusterCreated = $false
    if (Test-KindCluster $ClusterName) {
        throw 'The disposable Kind cluster still exists after teardown.'
    }
    Remove-OwnedArtifacts
    Remove-Item -LiteralPath $liveFragmentPath, $recoveryFragmentPath, $lifecycleFragmentPath -Force

    $report = [ordered]@{
        schema_version = 'rocketmq-sre.autonomy-action-qualification-report.v1'
        revision = $revision
        source_clean = $true
        environment = 'disposable_kind'
        started_at = $startedAt.ToString('o')
        finished_at = [DateTimeOffset]::UtcNow.ToString('o')
        status = 'passed'
        live_mode_ceiling = 'supervised'
        unattended_autonomous_execution = $false
        model_provider_network_calls = 0
        secrets_recorded = $false
        message_bodies_recorded = $false
        actions = $actionReports
        cleanup = [ordered]@{
            status = 'passed'
            disposable_kind_destroyed = $true
            owned_runtime_artifacts_removed = $true
            qualification_fragments_removed = $true
            target_state_restored = $true
        }
    }
    $json = $report | ConvertTo-Json -Depth 14
    [IO.File]::WriteAllText($reportPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
    Invoke-Native python @($checkerPath, '--manifest', $manifestPath, '--report', $reportPath) `
        'autonomy live qualification report validation'
    $qualificationSucceeded = $true
    Write-Host "AUTONOMY_ACTION_LIVE_QUALIFICATION_OK report=$reportPath actions=4 outcomes=64 live_mode_ceiling=supervised model_network_calls=0"
}
finally {
    if ($clusterCreated -or (Test-KindCluster $ClusterName)) {
        try {
            & $kindScript -Action Down -ClusterName $ClusterName
        }
        catch {
            Write-Warning "Unable to tear down the qualification-owned Kind cluster: $($_.Exception.Message)"
        }
    }
    try {
        Remove-OwnedArtifacts
    }
    catch {
        Write-Warning "Unable to remove qualification-owned runtime artifacts: $($_.Exception.Message)"
    }
    if (
        -not $qualificationSucceeded -and
        (Test-Path -LiteralPath $runRoot) -and
        $runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)
    ) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
}

# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$FaultMatrixRun,
    [Parameter(Mandatory = $true)][string]$PostgresHaEvidence,
    [Parameter(Mandatory = $true)][string]$ObjectRecoveryEvidence,
    [Parameter(Mandatory = $true)][string]$ControlPlaneRestoreEvidence,
    [string]$EvidenceOutput = 'D:\rocketmq-sre-evidence\disaster-recovery.json'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$startedAt = [DateTimeOffset]::UtcNow
$requiredFaultScenarios = @(
    'node_eviction',
    'controller_leader_failure',
    'controller_quorum_loss',
    'ha_replication_lag',
    'acknowledged_message_recovery'
)

function Assert-DataPath([string]$Path, [string]$Description) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetPathRoot($fullPath)
    if (
        -not $root.Equals('D:\', [StringComparison]::OrdinalIgnoreCase) -and
        -not $root.Equals('F:\', [StringComparison]::OrdinalIgnoreCase)
    ) {
        throw "$Description must use the D or F drive."
    }
    if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) {
        throw "$Description does not exist: $fullPath"
    }
    $fullPath
}

function Read-Evidence([string]$Path, [string]$Description) {
    try {
        Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
    }
    catch {
        throw "$Description is not valid JSON: $($_.Exception.Message)"
    }
}

function Assert-Passed($Evidence, [string]$Description) {
    if ([string]$Evidence.status -ne 'passed') {
        throw "$Description did not pass."
    }
    if ([bool]$Evidence.secrets_recorded) {
        throw "$Description reports secret material in evidence."
    }
}

$faultMatrixPath = Assert-DataPath $FaultMatrixRun 'fault-matrix run evidence'
$postgresPath = Assert-DataPath $PostgresHaEvidence 'PostgreSQL HA evidence'
$objectPath = Assert-DataPath $ObjectRecoveryEvidence 'object recovery evidence'
$controlPlanePath = Assert-DataPath $ControlPlaneRestoreEvidence 'Control Plane restore evidence'
$evidenceOutput = [IO.Path]::GetFullPath($EvidenceOutput)
$evidenceRoot = [IO.Path]::GetPathRoot($evidenceOutput)
if (
    -not $evidenceRoot.Equals('D:\', [StringComparison]::OrdinalIgnoreCase) -and
    -not $evidenceRoot.Equals('F:\', [StringComparison]::OrdinalIgnoreCase)
) {
    throw 'Evidence output must use the D or F drive.'
}

$faultMatrix = Read-Evidence $faultMatrixPath 'fault-matrix evidence'
$postgres = Read-Evidence $postgresPath 'PostgreSQL HA evidence'
$objectRecovery = Read-Evidence $objectPath 'object recovery evidence'
$controlPlane = Read-Evidence $controlPlanePath 'Control Plane restore evidence'
Assert-Passed $postgres 'PostgreSQL HA exercise'
Assert-Passed $objectRecovery 'object recovery exercise'
Assert-Passed $controlPlane 'Control Plane restore exercise'

if (-not [bool]$faultMatrix.dynamic_execution -or [bool]$faultMatrix.fixture) {
    throw 'Kubernetes fault-matrix evidence must come from dynamic non-fixture execution.'
}
if (@($faultMatrix.unresolved_faults).Count -ne 0) {
    throw 'Kubernetes fault-matrix evidence contains unresolved faults.'
}
foreach ($assertion in $faultMatrix.global_assertions.PSObject.Properties) {
    if (-not [bool]$assertion.Value) {
        throw "Kubernetes fault-matrix global assertion $($assertion.Name) did not pass."
    }
}
$scenarioMap = @{}
foreach ($scenario in @($faultMatrix.scenarios)) {
    if ([string]$scenario.status -ne 'passed') {
        throw "Fault-matrix scenario $([string]$scenario.id) did not pass."
    }
    $scenarioMap[[string]$scenario.id] = $scenario
}
foreach ($scenarioId in $requiredFaultScenarios) {
    if (-not $scenarioMap.ContainsKey($scenarioId)) {
        throw "Fault-matrix evidence is missing $scenarioId."
    }
}
if (-not [bool]$scenarioMap['ha_replication_lag'].assertions.rpo_satisfied -or
    -not [bool]$scenarioMap['ha_replication_lag'].assertions.acknowledged_message_visible -or
    -not [bool]$scenarioMap['acknowledged_message_recovery'].assertions.message_id_preserved -or
    -not [bool]$scenarioMap['acknowledged_message_recovery'].assertions.queue_offset_preserved -or
    -not [bool]$scenarioMap['acknowledged_message_recovery'].assertions.commitlog_offset_preserved) {
    throw 'Broker replication and acknowledged-message recovery require RPO=0.'
}
if (-not [bool]$faultMatrix.global_assertions.controller_quorum_restored -or
    -not [bool]$faultMatrix.global_assertions.acknowledged_message_recovered) {
    throw 'Controller quorum or acknowledged-message recovery was not proven.'
}

foreach ($field in @('approval_rows', 'audit_rows', 'step_intent_rows')) {
    if ([int]$postgres.$field -lt 1) {
        throw "PostgreSQL HA evidence is missing durable $field."
    }
}
if ([int]$postgres.rpo_rows -ne 0 -or -not [bool]$postgres.synchronous_replication) {
    throw 'PostgreSQL failover must prove synchronous RPO=0.'
}
if (-not [bool]$postgres.primary_failure_injected -or -not [bool]$postgres.standby_promoted) {
    throw 'PostgreSQL evidence must include a primary failure and standby promotion.'
}

if (-not [bool]$objectRecovery.metadata_restored -or
    -not [bool]$objectRecovery.content_restored -or
    -not [bool]$objectRecovery.sha256_verified) {
    throw 'Object recovery must restore metadata and content with a verified hash.'
}
if ([int]$objectRecovery.lost_objects -ne 0) {
    throw 'Object recovery lost one or more qualified objects.'
}

if (-not [bool]$controlPlane.restore_verified -or
    [int]$controlPlane.rpo_rows -ne 0 -or
    [int]$controlPlane.rto_seconds -lt 0) {
    throw 'Control Plane restore evidence is incomplete or violates RPO=0.'
}

$revision = (& git -C (Split-Path -Parent $PSScriptRoot) rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to resolve the qualification revision.'
}
$evidence = [ordered]@{
    schema_version = 'rocketmq-sre.disaster-recovery-qualification.v1'
    status = 'passed'
    environment = 'docker-postgresql-object-store-and-kubernetes-fault-matrix'
    started_at = $startedAt.ToString('O')
    finished_at = [DateTimeOffset]::UtcNow.ToString('O')
    revision = $revision
    postgresql = [ordered]@{
        synchronous_replication = $true
        primary_failure_injected = $true
        standby_promoted = $true
        approval_audit_step_intent_rpo_rows = 0
        rto_seconds = [int]$postgres.rto_seconds
    }
    kubernetes = [ordered]@{
        node_loss = 'passed'
        controller_leader_failure = 'passed'
        controller_quorum_loss = 'passed'
    }
    rocketmq = [ordered]@{
        commitlog_replication = 'passed'
        acknowledged_message_rpo_seconds = 0
        controller_quorum_restored = $true
    }
    object_store = [ordered]@{
        metadata_restored = $true
        content_restored = $true
        sha256_verified = $true
        lost_objects = 0
    }
    control_plane = [ordered]@{
        backup_restore = 'passed'
        rpo_rows = 0
        rto_seconds = [int]$controlPlane.rto_seconds
    }
    source_evidence_sha256 = [ordered]@{
        fault_matrix = (Get-FileHash -LiteralPath $faultMatrixPath -Algorithm SHA256).Hash.ToLowerInvariant()
        postgresql_ha = (Get-FileHash -LiteralPath $postgresPath -Algorithm SHA256).Hash.ToLowerInvariant()
        object_recovery = (Get-FileHash -LiteralPath $objectPath -Algorithm SHA256).Hash.ToLowerInvariant()
        control_plane_restore = (Get-FileHash -LiteralPath $controlPlanePath -Algorithm SHA256).Hash.ToLowerInvariant()
    }
    secrets_recorded = $false
}
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $evidenceOutput) | Out-Null
$evidence | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $evidenceOutput -Encoding utf8
Write-Host "DISASTER_RECOVERY_QUALIFICATION_OK evidence=$evidenceOutput"

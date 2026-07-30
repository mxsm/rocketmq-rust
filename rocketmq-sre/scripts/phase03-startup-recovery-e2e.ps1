# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Kubeconfig,

    [string]$KubectlPath = 'kubectl',

    [string]$Namespace = 'rocketmq-sre',

    [string]$PostgresPod = 'postgres-0',

    [string]$ExecutorDeployment = 'sre-executor',

    [ValidateRange(30, 600)]
    [int]$TimeoutSeconds = 240
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Assert-NonSystemPath([string]$Path, [string]$Description) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    if ([IO.Path]::GetPathRoot($fullPath).Equals('C:\', [StringComparison]::OrdinalIgnoreCase)) {
        throw "$Description must not use the C drive."
    }
}

function Invoke-Kubectl([string[]]$Arguments) {
    $output = & $resolvedKubectl @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl failed with exit code $LASTEXITCODE."
    }
    return @($output)
}

function Invoke-Postgres(
    [string]$Sql,
    [hashtable]$Variables = @{}
) {
    $arguments = @(
        '--kubeconfig', $resolvedKubeconfig,
        '-n', $Namespace,
        'exec', '-i', $PostgresPod,
        '--',
        'psql',
        '-U', 'rocketmq_sre',
        '-d', 'rocketmq_sre',
        '--set=ON_ERROR_STOP=1',
        '-P', 'pager=off',
        '-At'
    )
    foreach ($name in ($Variables.Keys | Sort-Object)) {
        $arguments += "--set=$name=$($Variables[$name])"
    }
    $output = $Sql | & $resolvedKubectl @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "PostgreSQL fixture command failed with exit code $LASTEXITCODE."
    }
    return @($output)
}

function Get-ExecutorPodIdentity {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds([Math]::Min($TimeoutSeconds, 60))
    $readyPods = @()
    do {
        $json = (
            Invoke-Kubectl @(
                '--kubeconfig', $resolvedKubeconfig,
                '-n', $Namespace,
                'get', 'pods',
                '-l', 'app.kubernetes.io/name=rocketmq-sre-executor',
                '-o', 'json'
            )
        ) -join [Environment]::NewLine | ConvertFrom-Json
        $readyPods = @(
            $json.items | Where-Object {
                $deleting = $_.metadata.PSObject.Properties['deletionTimestamp']
                ($null -eq $deleting -or $null -eq $deleting.Value) -and
                $_.status.phase -eq 'Running' -and
                @($_.status.containerStatuses).Count -gt 0 -and
                (@($_.status.containerStatuses | Where-Object { -not $_.ready })).Count -eq 0
            }
        )
        if ($readyPods.Count -eq 1) {
            return [pscustomobject]@{
                Name = [string]$readyPods[0].metadata.name
                Uid = [string]$readyPods[0].metadata.uid
            }
        }
        Start-Sleep -Milliseconds 200
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Expected exactly one Ready non-terminating Executor pod, observed $($readyPods.Count)."
}

Assert-NonSystemPath $Kubeconfig 'Kubernetes kubeconfig'
$resolvedKubeconfig = [IO.Path]::GetFullPath($Kubeconfig)
if (-not (Test-Path -LiteralPath $resolvedKubeconfig -PathType Leaf)) {
    throw "Kubernetes kubeconfig does not exist: $resolvedKubeconfig"
}
$resolvedKubectl = if ([IO.Path]::IsPathRooted($KubectlPath)) {
    [IO.Path]::GetFullPath($KubectlPath)
}
else {
    $command = Get-Command $KubectlPath -ErrorAction Stop
    $command.Source
}
if (-not (Test-Path -LiteralPath $resolvedKubectl -PathType Leaf)) {
    throw "kubectl executable does not exist: $resolvedKubectl"
}

$oldPod = Get-ExecutorPodIdentity
$executionId = [Guid]::NewGuid().ToString()
$correlationId = [Guid]::NewGuid().ToString()
$runtimeStepId = [Guid]::NewGuid().ToString()
$effectId = [Guid]::NewGuid().ToString()
$lockId = [Guid]::NewGuid().ToString()
$fenceNonce = [Guid]::NewGuid().ToString()
$fixtureNonce = [Guid]::NewGuid().ToString('N')
$rootIdempotencyKey = "phase03-startup-recovery-$fixtureNonce"
$intentIdempotencyKey = "$rootIdempotencyKey`:forward:1"
$operationId = "sre-startup-recovery-$fixtureNonce"

$seedSql = @'
\set ON_ERROR_STOP on
BEGIN;

DO $fixture_guard$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM executions
        WHERE action_id = 'observability.logger_level_ttl.v1'
          AND state = 'succeeded'
    ) THEN
        RAISE EXCEPTION 'startup_recovery_source_execution_missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM executor_leases
        WHERE activated_at IS NOT NULL
          AND fence_ack_snapshot IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'startup_recovery_activated_lease_missing';
    END IF;
END
$fixture_guard$;

WITH source AS (
    SELECT execution.*
    FROM executions execution
    WHERE execution.action_id = 'observability.logger_level_ttl.v1'
      AND execution.state = 'succeeded'
    ORDER BY execution.completed_at DESC, execution.id DESC
    LIMIT 1
), source_lease AS (
    SELECT lease.*
    FROM executor_leases lease
    JOIN source ON source.cluster_id = lease.cluster_id
    WHERE lease.activated_at IS NOT NULL
      AND lease.fence_ack_snapshot IS NOT NULL
      AND lease.epoch = (
          SELECT MAX(latest.epoch)
          FROM executor_leases latest
          WHERE latest.cluster_id = source.cluster_id
            AND latest.activated_at IS NOT NULL
            AND latest.fence_ack_snapshot IS NOT NULL
      )
    LIMIT 1
), fixture AS (
    SELECT
        source.*,
        GREATEST(
            source_lease.activated_at + INTERVAL '1 millisecond',
            LEAST(CURRENT_TIMESTAMP, source_lease.expires_at - INTERVAL '1 millisecond')
        ) AS fixture_at
    FROM source
    CROSS JOIN source_lease
)
INSERT INTO executions (
    id,
    tenant_id,
    cluster_id,
    correlation_id,
    plan_id,
    plan_hash,
    resource_key,
    action_id,
    idempotency_key,
    state,
    request_snapshot,
    requested_by,
    started_at,
    completed_at,
    updated_at
)
SELECT
    :'execution_id'::UUID,
    fixture.tenant_id,
    fixture.cluster_id,
    :'correlation_id'::UUID,
    fixture.plan_id,
    fixture.plan_hash,
    fixture.resource_key,
    fixture.action_id,
    :'root_idempotency_key',
    'compensating',
    jsonb_set(
        jsonb_set(
            jsonb_set(
                jsonb_set(
                    fixture.request_snapshot,
                    '{id}',
                    to_jsonb(:'execution_id'::TEXT)
                ),
                '{correlation_id}',
                to_jsonb(:'correlation_id'::TEXT)
            ),
            '{idempotency_key}',
            to_jsonb(:'root_idempotency_key'::TEXT)
        ),
        '{requested_by}',
        to_jsonb('phase03-startup-recovery-e2e'::TEXT)
    ),
    'phase03-startup-recovery-e2e',
    fixture.fixture_at,
    NULL,
    fixture.fixture_at
FROM fixture;

WITH source_execution AS (
    SELECT execution.*
    FROM executions execution
    WHERE execution.action_id = 'observability.logger_level_ttl.v1'
      AND execution.state = 'succeeded'
    ORDER BY execution.completed_at DESC, execution.id DESC
    LIMIT 1
), source_intent AS (
    SELECT step.*
    FROM execution_steps step
    JOIN source_execution ON source_execution.id = step.execution_id
    WHERE step.record_kind = 'intent'
      AND NOT step.compensation
    ORDER BY step.sequence_id
    LIMIT 1
), source_lease AS (
    SELECT lease.*
    FROM executor_leases lease
    JOIN source_execution ON source_execution.cluster_id = lease.cluster_id
    WHERE lease.activated_at IS NOT NULL
      AND lease.fence_ack_snapshot IS NOT NULL
      AND lease.epoch = (
          SELECT MAX(latest.epoch)
          FROM executor_leases latest
          WHERE latest.cluster_id = source_execution.cluster_id
            AND latest.activated_at IS NOT NULL
            AND latest.fence_ack_snapshot IS NOT NULL
      )
    LIMIT 1
), fixture AS (
    SELECT
        source_intent.*,
        source_lease.id AS source_lease_id,
        source_lease.epoch AS source_lease_epoch,
        source_lease.owner AS source_lease_owner,
        GREATEST(
            source_lease.activated_at + INTERVAL '1 millisecond',
            LEAST(CURRENT_TIMESTAMP, source_lease.expires_at - INTERVAL '1 millisecond')
        ) AS fixture_at,
        source_lease.expires_at AS lease_expires_at
    FROM source_intent
    CROSS JOIN source_lease
)
INSERT INTO execution_steps (
    execution_id,
    step_id,
    attempt,
    record_kind,
    lease_id,
    lease_epoch,
    compensation,
    intent_snapshot,
    result_snapshot,
    reason_code,
    occurred_at
)
SELECT
    :'execution_id'::UUID,
    :'runtime_step_id'::UUID,
    1,
    'intent',
    fixture.source_lease_id,
    fixture.source_lease_epoch,
    FALSE,
    fixture.intent_snapshot || jsonb_build_object(
        'execution_id', :'execution_id'::TEXT,
        'step_id', :'runtime_step_id'::TEXT,
        'idempotency_key', :'intent_idempotency_key'::TEXT,
        'intended_at', fixture.fixture_at,
        'fence_grant', (fixture.intent_snapshot -> 'fence_grant') || jsonb_build_object(
            'epoch', fixture.source_lease_epoch,
            'nonce', :'fence_nonce'::TEXT,
            'owner', fixture.source_lease_owner,
            'step_id', :'runtime_step_id'::TEXT,
            'lease_id', fixture.source_lease_id::TEXT,
            'execution_id', :'execution_id'::TEXT,
            'issued_at', fixture.fixture_at,
            'expires_at', fixture.lease_expires_at
        )
    ),
    NULL,
    'startup_recovery_fixture_intent',
    fixture.fixture_at
FROM fixture;

WITH source_execution AS (
    SELECT execution.*
    FROM executions execution
    WHERE execution.action_id = 'observability.logger_level_ttl.v1'
      AND execution.state = 'succeeded'
    ORDER BY execution.completed_at DESC, execution.id DESC
    LIMIT 1
), source_effect AS (
    SELECT effect.*
    FROM execution_agent_effects effect
    JOIN source_execution ON source_execution.id = effect.execution_id
    ORDER BY effect.prepared_at
    LIMIT 1
), new_intent AS (
    SELECT step.*
    FROM execution_steps step
    WHERE step.execution_id = :'execution_id'::UUID
      AND step.record_kind = 'intent'
    LIMIT 1
), source_lease AS (
    SELECT lease.*
    FROM executor_leases lease
    JOIN source_execution ON source_execution.cluster_id = lease.cluster_id
    WHERE lease.activated_at IS NOT NULL
      AND lease.fence_ack_snapshot IS NOT NULL
      AND lease.epoch = (
          SELECT MAX(latest.epoch)
          FROM executor_leases latest
          WHERE latest.cluster_id = source_execution.cluster_id
            AND latest.activated_at IS NOT NULL
            AND latest.fence_ack_snapshot IS NOT NULL
      )
    LIMIT 1
)
INSERT INTO execution_agent_effects (
    id,
    tenant_id,
    cluster_id,
    execution_id,
    step_id,
    lease_id,
    epoch,
    idempotency_key,
    action_id,
    target,
    state,
    request_snapshot,
    operation_id,
    outcome_code,
    sanitized_summary,
    prepared_at,
    dispatched_at,
    confirmed_at,
    updated_at
)
SELECT
    :'effect_id'::UUID,
    source_effect.tenant_id,
    source_effect.cluster_id,
    :'execution_id'::UUID,
    :'runtime_step_id'::UUID,
    source_lease.id,
    source_lease.epoch,
    :'intent_idempotency_key',
    source_effect.action_id,
    source_effect.target,
    'prepared',
    source_effect.request_snapshot || jsonb_build_object(
        'intent', new_intent.intent_snapshot
    ),
    NULL,
    NULL,
    NULL,
    new_intent.occurred_at + INTERVAL '1 millisecond',
    NULL,
    NULL,
    new_intent.occurred_at + INTERVAL '1 millisecond'
FROM source_effect
CROSS JOIN new_intent
CROSS JOIN source_lease;

UPDATE execution_agent_effects
SET state = 'dispatched',
    operation_id = :'operation_id',
    dispatched_at = prepared_at + INTERVAL '1 millisecond',
    updated_at = prepared_at + INTERVAL '1 millisecond'
WHERE id = :'effect_id'::UUID;

UPDATE execution_agent_effects
SET state = 'confirmed',
    outcome_code = 'startup_recovery_fixture_recorded',
    sanitized_summary = 'bounded startup recovery fixture with no live effect',
    confirmed_at = prepared_at + INTERVAL '2 milliseconds',
    updated_at = prepared_at + INTERVAL '2 milliseconds'
WHERE id = :'effect_id'::UUID;

INSERT INTO execution_steps (
    execution_id,
    step_id,
    attempt,
    record_kind,
    lease_id,
    lease_epoch,
    compensation,
    intent_snapshot,
    result_snapshot,
    reason_code,
    occurred_at
)
SELECT
    intent.execution_id,
    intent.step_id,
    intent.attempt,
    'result',
    NULL,
    NULL,
    FALSE,
    NULL,
    jsonb_build_object(
        'step_id', intent.step_id::TEXT,
        'state', 'verifying',
        'agent_result', NULL,
        'verification', NULL,
        'reason_code', 'startup_recovery_fixture_forward_recorded',
        'completed_at', intent.occurred_at + INTERVAL '4 milliseconds'
    ),
    'startup_recovery_fixture_forward_recorded',
    intent.occurred_at + INTERVAL '4 milliseconds'
FROM execution_steps intent
WHERE intent.execution_id = :'execution_id'::UUID
  AND intent.record_kind = 'intent'
  AND NOT intent.compensation;

INSERT INTO resource_locks (
    id,
    tenant_id,
    cluster_id,
    resource_key,
    action_id,
    holder_execution_id,
    acquired_at,
    renewed_at,
    expires_at,
    released_at,
    release_reason
)
SELECT
    :'lock_id'::UUID,
    execution.tenant_id,
    execution.cluster_id,
    execution.resource_key,
    execution.action_id,
    execution.id,
    execution.started_at,
    execution.started_at,
    CURRENT_TIMESTAMP + INTERVAL '10 minutes',
    NULL,
    NULL
FROM executions execution
WHERE execution.id = :'execution_id'::UUID;

COMMIT;

SELECT
    execution.state || '|' ||
    (
        SELECT COUNT(*)::TEXT
        FROM resource_locks lock
        WHERE lock.holder_execution_id = execution.id
          AND lock.released_at IS NULL
    ) || '|' ||
    (
        SELECT COUNT(*)::TEXT
        FROM execution_steps step
        WHERE step.execution_id = execution.id
          AND step.record_kind = 'result'
          AND NOT step.compensation
    )
FROM executions execution
WHERE execution.id = :'execution_id'::UUID;
'@

$variables = @{
    execution_id = $executionId
    correlation_id = $correlationId
    runtime_step_id = $runtimeStepId
    effect_id = $effectId
    lock_id = $lockId
    fence_nonce = $fenceNonce
    root_idempotency_key = $rootIdempotencyKey
    intent_idempotency_key = $intentIdempotencyKey
    operation_id = $operationId
}
$seedResult = (Invoke-Postgres $seedSql $variables | Select-Object -Last 1).Trim()
if ($seedResult -ne 'compensating|1|1') {
    throw "Unexpected startup recovery fixture state: $seedResult"
}

Invoke-Kubectl @(
    '--kubeconfig', $resolvedKubeconfig,
    '-n', $Namespace,
    'rollout', 'restart', "deployment/$ExecutorDeployment"
) | Out-Null
Invoke-Kubectl @(
    '--kubeconfig', $resolvedKubeconfig,
    '-n', $Namespace,
    'rollout', 'status', "deployment/$ExecutorDeployment",
    "--timeout=$($TimeoutSeconds)s"
) | Out-Null

$deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
$terminal = $null
do {
    $terminal = (
        Invoke-Postgres @'
SELECT
    execution.state || '|' ||
    (
        SELECT COUNT(*)::TEXT
        FROM resource_locks lock
        WHERE lock.holder_execution_id = execution.id
          AND lock.released_at IS NULL
    ) || '|' ||
    (
        SELECT COUNT(*)::TEXT
        FROM audit_events audit
        WHERE audit.resource_id = execution.id::TEXT
          AND audit.reason_code = 'recovery_confirmed_forward_effects_absent'
    ) || '|' ||
    (
        SELECT COUNT(*)::TEXT
        FROM execution_steps step
        WHERE step.execution_id = execution.id
          AND step.record_kind = 'result'
          AND NOT step.compensation
    )
FROM executions execution
WHERE execution.id = :'execution_id'::UUID;
'@ @{ execution_id = $executionId } |
            Select-Object -Last 1
    ).Trim()
    if ($terminal -eq 'rolled_back|0|1|1') {
        break
    }
    Start-Sleep -Seconds 2
} while ([DateTimeOffset]::UtcNow -lt $deadline)
if ($terminal -ne 'rolled_back|0|1|1') {
    throw "Executor startup recovery did not converge: $terminal"
}

$newPod = Get-ExecutorPodIdentity
if ($newPod.Uid -eq $oldPod.Uid) {
    throw 'Executor rollout did not replace the Pod UID.'
}
$logs = (
    Invoke-Kubectl @(
        '--kubeconfig', $resolvedKubeconfig,
        '-n', $Namespace,
        'logs', $newPod.Name,
        '--since=10m'
    )
) -join [Environment]::NewLine
if (
    $logs -notmatch 'bounded interrupted-execution recovery sweep completed' -or
    $logs -notmatch '"recovered":1'
) {
    throw 'Executor startup logs do not prove one recovered execution.'
}

Write-Host (
    'PHASE03_STARTUP_RECOVERY_E2E_OK ' +
    "execution_id=$executionId correlation_id=$correlationId " +
    "old_pod_uid=$($oldPod.Uid) new_pod_uid=$($newPod.Uid) " +
    'state=rolled_back unreleased_locks=0 recovery_audit=1 forward_results=1'
)

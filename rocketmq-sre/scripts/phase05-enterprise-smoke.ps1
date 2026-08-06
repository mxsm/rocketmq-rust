# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

[CmdletBinding()]
param(
    [string]$DatabaseUrl = 'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:5432/rocketmq_sre',
    [string]$PostgresContainer = 'rocketmq-rust-ai-sre-phase00-postgres-1',
    [string]$Kubeconfig = 'D:\BuildCache\rocketmq-sre-temp\kind\phase00-kubeconfig',
    [string]$ExpectedContext = 'kubernetes-admin@rocketmq-sre-phase00',
    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',
    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',
    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',
    [string]$EvidenceOutput = 'D:\BuildCache\rocketmq-sre-temp\phase05-enterprise-smoke.json'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$manifestPath = Join-Path $sreRoot 'Cargo.toml'
$restoreScript = Join-Path $scriptDirectory 'phase05-control-plane-restore.ps1'
$drScript = Join-Path $scriptDirectory 'phase05-test-cluster-dr.ps1'
$runDirectory = Join-Path $TemporaryRoot "phase05-enterprise-$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
$restoreEvidence = Join-Path $runDirectory 'control-plane-restore.json'
$drEvidence = Join-Path $runDirectory 'test-cluster-dr.json'
$scaleEvidence = Join-Path $runDirectory 'production-readiness-scale.json'
$policyEvidence = Join-Path $runDirectory 'production-readiness-policy.json'
$precheckEvidence = Join-Path $runDirectory 'production-readiness-precheck.json'

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Description
    )

    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

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

function Invoke-SpaceGuard {
    $targetDriveName = [IO.Path]::GetPathRoot(
        [IO.Path]::GetFullPath($CargoTargetDir)
    ).TrimEnd('\').TrimEnd(':')
    $targetFreeGiB = (Get-PSDrive -Name $targetDriveName).Free / 1GB
    Write-Host "${targetDriveName}_FREE_GIB=$([Math]::Round($targetFreeGiB, 2))"
    if ($targetFreeGiB -lt 15) {
        Invoke-Native cargo @(
            'clean',
            '--manifest-path', $manifestPath,
            '--target-dir', $CargoTargetDir
        ) 'low-space Cargo cleanup'
    }
}

function Invoke-ExactCargoTest(
    [string]$Package,
    [string]$TestName,
    [string]$Capability
) {
    Invoke-SpaceGuard
    $stopwatch = [Diagnostics.Stopwatch]::StartNew()
    $arguments = @(
        'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', $Package,
        $TestName,
        '--',
        '--ignored',
        '--exact'
    )
    $output = & cargo @arguments 2>&1
    $exitCode = $LASTEXITCODE
    $output | Out-Host
    if ($exitCode -ne 0) {
        throw "$Capability failed with exit code $exitCode."
    }
    if (($output -join "`n") -notmatch 'test result: ok\. 1 passed;') {
        throw "$Capability did not execute exactly one test."
    }
    $stopwatch.Stop()
    [ordered]@{
        test = $TestName
        capability = $Capability
        status = 'passed'
        duration_millis = $stopwatch.ElapsedMilliseconds
    }
}

foreach ($path in @(
    @{ Value = $Kubeconfig; Description = 'Kubernetes test kubeconfig' },
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' },
    @{ Value = $EvidenceOutput; Description = 'enterprise evidence output' }
)) {
    Assert-DataPath $path.Value $path.Description
}

$savedEnvironment = @{}
foreach ($name in @(
    'CARGO_HOME',
    'CARGO_TARGET_DIR',
    'TEMP',
    'TMP',
    'KUBECONFIG',
    'ROCKETMQ_SRE_TEST_DATABASE_URL',
    'ROCKETMQ_SRE_PRODUCTION_READINESS_SCALE_REPORT',
    'ROCKETMQ_SRE_PRODUCTION_READINESS_POLICY_REPORT',
    'ROCKETMQ_SRE_PRODUCTION_READINESS_PRECHECK_REPORT'
)) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

try {
    New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null
    $env:CARGO_HOME = [IO.Path]::GetFullPath($CargoHome)
    $env:CARGO_TARGET_DIR = [IO.Path]::GetFullPath($CargoTargetDir)
    $env:TEMP = [IO.Path]::GetFullPath($TemporaryRoot)
    $env:TMP = $env:TEMP
    $env:KUBECONFIG = [IO.Path]::GetFullPath($Kubeconfig)
    $env:ROCKETMQ_SRE_TEST_DATABASE_URL = $DatabaseUrl
    $env:ROCKETMQ_SRE_PRODUCTION_READINESS_SCALE_REPORT = $scaleEvidence
    $env:ROCKETMQ_SRE_PRODUCTION_READINESS_POLICY_REPORT = $policyEvidence
    $env:ROCKETMQ_SRE_PRODUCTION_READINESS_PRECHECK_REPORT = $precheckEvidence

    $tests = @()
    $tests += Invoke-ExactCargoTest `
        'rocketmq-sre-control-plane' `
        'fleet::repository_scale_tests::postgres_fleet_scale_demo_pages_100_clusters_and_applies_backpressure' `
        '100-cluster pagination, quota, backpressure, and worst-health visibility'
    $tests += Invoke-ExactCargoTest `
        'rocketmq-sre-control-plane' `
        'assets::scale_tests::postgres_inventory_profiles_twenty_thousand_assets_and_evidence_queries' `
        '20,000-asset inventory pagination and Evidence query latency'
    $tests += Invoke-ExactCargoTest `
        'rocketmq-sre-control-plane' `
        'supervised_execution::policy::tests::policy_evaluation_latency_profile_is_bounded' `
        'deterministic supervised policy P99 latency'
    $tests += Invoke-ExactCargoTest `
        'rocketmq-sre-execution-agent' `
        'drivers::telemetry_collector_restart_one::tests::execution_precheck_latency_profile_is_bounded' `
        'typed read-only execution precheck P95 latency'
    $tests += Invoke-ExactCargoTest `
        'rocketmq-sre-control-plane' `
        'fleet::repository_tests::postgres_fleet_scope_routing_compliance_and_inspection_are_bounded' `
        'two-region scope, residency, disconnection degradation, and inspection'
    $tests += Invoke-ExactCargoTest `
        'rocketmq-sre-control-plane' `
        'fleet::repository_tests::postgres_current_n_minus_one_and_incompatible_runtime_handshakes_fail_closed' `
        'Connector, Execution Agent, and MCP current/N-1 compatibility'
    $tests += Invoke-ExactCargoTest `
        'rocketmq-sre-control-plane' `
        'release_management::repository_tests::postgres_release_and_integration_records_are_durable_idempotent_and_append_only' `
        'ITSM, ChatOps, Pager outbox idempotency and stale-claim recovery'
    $tests += Invoke-ExactCargoTest `
        'rocketmq-sre-control-plane' `
        'release_management::repository_tests::postgres_enterprise_integration_events_are_signed_scoped_and_idempotent' `
        'CMDB, GitOps, and CI/CD signed event idempotency'
    $tests += Invoke-ExactCargoTest `
        'rocketmq-sre-control-plane' `
        'fleet::releases::repository_tests::postgres_two_region_release_denies_unready_target_and_pauses_on_canary_regression' `
        'two-region canary readiness deny, regression pause, and rollback'

    foreach ($fragment in @($scaleEvidence, $policyEvidence, $precheckEvidence)) {
        if (-not (Test-Path -LiteralPath $fragment -PathType Leaf)) {
            throw "A required production-readiness fragment is missing: $fragment"
        }
    }
    $scaleProfile = Get-Content -Raw -LiteralPath $scaleEvidence | ConvertFrom-Json
    $policyProfile = Get-Content -Raw -LiteralPath $policyEvidence | ConvertFrom-Json
    $precheckProfile = Get-Content -Raw -LiteralPath $precheckEvidence | ConvertFrom-Json
    if (
        $scaleProfile.schema_version -ne 'rocketmq-sre.production-readiness-scale-fragment.v1' -or
        $policyProfile.schema_version -ne 'rocketmq-sre.production-readiness-policy-fragment.v1' -or
        $precheckProfile.schema_version -ne 'rocketmq-sre.production-readiness-precheck-fragment.v1' -or
        [int]$scaleProfile.model_provider_network_calls -ne 0 -or
        [int]$policyProfile.model_provider_network_calls -ne 0 -or
        [int]$precheckProfile.model_provider_network_calls -ne 0 -or
        [int]$precheckProfile.target_mutations -ne 0
    ) {
        throw 'Production-readiness performance fragments failed closed.'
    }

    & $restoreScript `
        -PostgresContainer $PostgresContainer `
        -DatabaseUrl $DatabaseUrl `
        -CargoTargetDir $CargoTargetDir `
        -CargoHome $CargoHome `
        -TemporaryRoot $TemporaryRoot `
        -EvidenceOutput $restoreEvidence
    if (-not (Test-Path -LiteralPath $restoreEvidence -PathType Leaf)) {
        throw 'Control Plane restore exercise did not emit evidence.'
    }

    & $drScript `
        -Kubeconfig $Kubeconfig `
        -ExpectedContext $ExpectedContext `
        -CargoTargetDir $CargoTargetDir `
        -CargoHome $CargoHome `
        -TemporaryRoot $TemporaryRoot `
        -EvidenceOutput $drEvidence
    if (-not (Test-Path -LiteralPath $drEvidence -PathType Leaf)) {
        throw 'Test-cluster DR exercise did not emit evidence.'
    }

    $restore = Get-Content -Raw -LiteralPath $restoreEvidence | ConvertFrom-Json
    $dr = Get-Content -Raw -LiteralPath $drEvidence | ConvertFrom-Json
    if (
        -not [bool]$dr.message_history_restore_claimed -or
        [int]$dr.message_history.rpo_messages -ne 0
    ) {
        throw 'The enterprise smoke requires verified Broker message-history RPO=0.'
    }
    $evidence = [ordered]@{
        schema_version = 'rocketmq-sre.phase05-enterprise-smoke.v1'
        status = 'passed'
        observed_at = [DateTimeOffset]::UtcNow.ToString('O')
        scenarios = $tests
        scale = [ordered]@{
            clusters = 100
            logical_clusters = 100
            logical_regions = 2
            page_size = 25
            inspection_max_concurrency = 8
            quota_backpressure_verified = $true
            topic_assets = [int]$scaleProfile.topic_assets
            consumer_group_assets = [int]$scaleProfile.consumer_group_assets
            total_assets = [int]$scaleProfile.total_assets
            inventory_payload_bytes = [int64]$scaleProfile.inventory_payload_bytes
            inventory_ingest_millis = [double]$scaleProfile.inventory_ingest_millis
            page_limit = [int]$scaleProfile.page_limit
            page_samples = [int]$scaleProfile.page_samples
            asset_page_p95_millis = [double]$scaleProfile.asset_page_p95_millis
            oversized_page_rejected = [bool]$scaleProfile.oversized_page_rejected
            cleanup_verified = [bool]$scaleProfile.cleanup_verified
        }
        measurements = [ordered]@{
            evidence_query = $scaleProfile.evidence_query
            policy_evaluation = [ordered]@{
                samples = [int]$policyProfile.samples
                p99_millis = [double]$policyProfile.p99_millis
                unit = [string]$policyProfile.unit
            }
            execution_precheck = [ordered]@{
                samples = [int]$precheckProfile.samples
                p95_millis = [double]$precheckProfile.p95_millis
                unit = [string]$precheckProfile.unit
                error_count = [int]$precheckProfile.error_count
                error_rate = [double]$precheckProfile.error_rate
                execution_queue_depth_samples = [int]$precheckProfile.execution_queue_depth_samples
                execution_queue_depth_max = [int]$precheckProfile.execution_queue_depth_max
            }
        }
        integrations = @('itsm', 'chatops', 'pager', 'cmdb', 'gitops', 'ci-cd')
        fleet_release = [ordered]@{
            target_clusters = 2
            readiness_deny_verified = $true
            canary_regression_verified = $true
            pause_and_rollback_verified = $true
        }
        compatibility = [ordered]@{
            components = @('connector', 'execution-agent', 'mcp')
            current = 'full'
            n_minus_one = 'read_only_degraded'
            incompatible = 'denied'
        }
        control_plane_restore = $restore
        test_cluster_dr = $dr
        message_history_restore_claimed = [bool]$dr.message_history_restore_claimed
        repository_commit = (& git -C (Join-Path $sreRoot '..') rev-parse HEAD).Trim()
        model_provider_network_calls = 0
        secrets_recorded = $false
        message_bodies_recorded = $false
    }
    $evidenceDirectory = Split-Path -Parent ([IO.Path]::GetFullPath($EvidenceOutput))
    New-Item -ItemType Directory -Force -Path $evidenceDirectory | Out-Null
    $evidence | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $EvidenceOutput -Encoding utf8
    Write-Host "PHASE05_ENTERPRISE_SMOKE_OK evidence=$EvidenceOutput"
}
finally {
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}

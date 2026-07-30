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

function Invoke-ExactPostgresTest([string]$TestName, [string]$Capability) {
    Invoke-SpaceGuard
    $stopwatch = [Diagnostics.Stopwatch]::StartNew()
    Invoke-Native cargo @(
        'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', 'rocketmq-sre-control-plane',
        $TestName,
        '--',
        '--ignored',
        '--exact'
    ) $Capability | Out-Host
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
    'ROCKETMQ_SRE_TEST_DATABASE_URL'
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

    $tests = @()
    $tests += Invoke-ExactPostgresTest `
        'fleet::repository_scale_tests::postgres_fleet_scale_demo_pages_100_clusters_and_applies_backpressure' `
        '100-cluster pagination, quota, backpressure, and worst-health visibility'
    $tests += Invoke-ExactPostgresTest `
        'fleet::repository_tests::postgres_fleet_scope_routing_compliance_and_inspection_are_bounded' `
        'two-region scope, residency, disconnection degradation, and inspection'
    $tests += Invoke-ExactPostgresTest `
        'fleet::repository_tests::postgres_current_n_minus_one_and_incompatible_runtime_handshakes_fail_closed' `
        'Connector, Execution Agent, and MCP current/N-1 compatibility'
    $tests += Invoke-ExactPostgresTest `
        'release_management::repository_tests::postgres_release_and_integration_records_are_durable_idempotent_and_append_only' `
        'ITSM, ChatOps, Pager outbox idempotency and stale-claim recovery'
    $tests += Invoke-ExactPostgresTest `
        'release_management::repository_tests::postgres_enterprise_integration_events_are_signed_scoped_and_idempotent' `
        'CMDB, GitOps, and CI/CD signed event idempotency'
    $tests += Invoke-ExactPostgresTest `
        'fleet::releases::repository_tests::postgres_two_region_release_denies_unready_target_and_pauses_on_canary_regression' `
        'two-region canary readiness deny, regression pause, and rollback'

    & $restoreScript `
        -PostgresContainer $PostgresContainer `
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
            logical_regions = 2
            page_size = 25
            inspection_max_concurrency = 8
            quota_backpressure_verified = $true
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
        secrets_recorded = $false
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

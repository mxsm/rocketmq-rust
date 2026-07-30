# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [string]$DatabaseUrl = 'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:5432/rocketmq_sre',
    [string]$Kubeconfig = 'D:\BuildCache\rocketmq-sre-temp\kind\phase00-kubeconfig',
    [string]$Namespace = 'rocketmq-system',
    [string]$Workload = 'rocketmq-proxy',
    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',
    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',
    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$manifestPath = Join-Path $sreRoot 'Cargo.toml'

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

function Ensure-CargoCapacity {
    $targetDriveName = [IO.Path]::GetPathRoot(
        [IO.Path]::GetFullPath($CargoTargetDir)
    ).TrimEnd('\').TrimEnd(':')
    $targetFreeGiB = (Get-PSDrive -Name $targetDriveName).Free / 1GB
    Write-Host "${targetDriveName}_FREE_GIB=$([Math]::Round($targetFreeGiB, 2))"
    if ($targetFreeGiB -lt 15) {
        Invoke-Native cargo @(
            '+1.95.0', 'clean',
            '--manifest-path', $manifestPath,
            '--target-dir', $CargoTargetDir
        ) 'low-space Cargo cleanup'
    }
}

foreach ($path in @(
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' },
    @{ Value = $Kubeconfig; Description = 'Kubernetes test kubeconfig' }
)) {
    Assert-DataPath $path.Value $path.Description
}
if (-not (Test-Path -LiteralPath $Kubeconfig -PathType Leaf)) {
    throw "Kubernetes test kubeconfig does not exist: $Kubeconfig"
}

New-Item -ItemType Directory -Force -Path $CargoTargetDir, $CargoHome, $TemporaryRoot | Out-Null
Ensure-CargoCapacity

$savedEnvironment = @{}
foreach ($name in @(
    'CARGO_HOME',
    'CARGO_TARGET_DIR',
    'TEMP',
    'TMP',
    'KUBECONFIG',
    'ROCKETMQ_SRE_TEST_DATABASE_URL',
    'ROCKETMQ_SRE_TEST_PROXY_SCALE',
    'ROCKETMQ_SRE_TEST_PROXY_NAMESPACE',
    'ROCKETMQ_SRE_TEST_PROXY_WORKLOAD'
)) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

try {
    $env:CARGO_HOME = $CargoHome
    $env:CARGO_TARGET_DIR = $CargoTargetDir
    $env:TEMP = $TemporaryRoot
    $env:TMP = $TemporaryRoot
    $env:KUBECONFIG = [IO.Path]::GetFullPath($Kubeconfig)
    $env:ROCKETMQ_SRE_TEST_DATABASE_URL = $DatabaseUrl
    $env:ROCKETMQ_SRE_TEST_PROXY_SCALE = '1'
    $env:ROCKETMQ_SRE_TEST_PROXY_NAMESPACE = $Namespace
    $env:ROCKETMQ_SRE_TEST_PROXY_WORKLOAD = $Workload

    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', 'rocketmq-sre-eval',
        '--test', 'phase3_contracts',
        'phase_three_descriptor_catalog_is_typed_and_fail_closed',
        '--',
        '--exact'
    ) 'Proxy descriptor catalog contract'

    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', 'rocketmq-sre-execution-agent',
        'proxy_scale_out_one'
    ) 'typed Proxy scale handler tests'

    foreach ($testName in @(
        'successful_verification_reaches_succeeded_and_releases_lock',
        'failed_verification_runs_compensation_and_verifies_rollback'
    )) {
        Invoke-Native cargo @(
            '+1.95.0', 'test',
            '--manifest-path', $manifestPath,
            '--locked',
            '-p', 'rocketmq-sre-executor',
            '--test', 'execution_flow',
            $testName,
            '--',
            '--ignored',
            '--exact',
            '--test-threads=1'
        ) "Executor Proxy scenario $testName"
    }

    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', 'rocketmq-sre-execution-agent',
        'real_kind_proxy_scale_round_trip_is_bounded_and_reversible',
        '--',
        '--ignored',
        '--test-threads=1'
    ) 'real Kubernetes Proxy scale and rollback'

    $deployment = kubectl `
        --kubeconfig $Kubeconfig `
        --namespace $Namespace `
        get deployment $Workload `
        --output json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) {
        throw 'reading the restored Proxy Deployment failed.'
    }
    if ($deployment.spec.replicas -ne $deployment.status.readyReplicas) {
        throw 'the Proxy Deployment did not return to a fully ready state.'
    }

    Write-Host (
        'PHASE03_PROXY_SCALE_SMOKE_OK ' +
        "namespace=$Namespace workload=$Workload " +
        'typed_precheck=true supervised_success=true supervised_rollback=true ' +
        'real_scale_out_one=true real_restore=true allowlist_deny=true'
    )
}
finally {
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}

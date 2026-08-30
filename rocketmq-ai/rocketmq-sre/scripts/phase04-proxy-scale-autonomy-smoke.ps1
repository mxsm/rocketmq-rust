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

New-Item -ItemType Directory -Force -Path $CargoTargetDir, $CargoHome, $TemporaryRoot | Out-Null
Ensure-CargoCapacity

$savedEnvironment = @{}
foreach ($name in @(
    'CARGO_HOME',
    'CARGO_TARGET_DIR',
    'TEMP',
    'TMP',
    'ROCKETMQ_SRE_TEST_DATABASE_URL'
)) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

try {
    $env:CARGO_HOME = $CargoHome
    $env:CARGO_TARGET_DIR = $CargoTargetDir
    $env:TEMP = $TemporaryRoot
    $env:TMP = $TemporaryRoot
    $env:ROCKETMQ_SRE_TEST_DATABASE_URL = $DatabaseUrl

    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', 'rocketmq-sre-control-plane',
        '--lib',
        'proxy_scale_qualifies_through_shadow_supervised_and_autonomous',
        '--',
        '--ignored',
        '--test-threads=1'
    ) 'Proxy scale qualification lifecycle'

    foreach ($testName in @(
        'autonomous_execution_persists_live_safety_before_forward_dispatch',
        'autonomous_rollback_reuses_journal_without_a_new_safety_gate'
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
        ) "Autonomous Proxy scenario $testName"
    }

    & (Join-Path $scriptDirectory 'phase03-proxy-scale-smoke.ps1') `
        -DatabaseUrl $DatabaseUrl `
        -Kubeconfig $Kubeconfig `
        -Namespace $Namespace `
        -Workload $Workload `
        -CargoTargetDir $CargoTargetDir `
        -CargoHome $CargoHome `
        -TemporaryRoot $TemporaryRoot

    Write-Host (
        'PHASE04_PROXY_SCALE_AUTONOMY_SMOKE_OK ' +
        'shadow_samples=20 supervised_successes=5 observation_days=7 ' +
        'heterogeneous_critic=true autonomous_success=true autonomous_rollback=true ' +
        'real_scale_out_one=true real_restore=true'
    )
}
finally {
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}

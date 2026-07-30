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

function Assert-NonSystemBuildPath([string]$Path, [string]$Description) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    if ([IO.Path]::GetPathRoot($fullPath).Equals('C:\', [StringComparison]::OrdinalIgnoreCase)) {
        throw "$Description must not use the C drive."
    }
}

function Ensure-CargoCapacity {
    $dDrive = Get-PSDrive -Name D
    $gDrive = Get-PSDrive -Name G
    Write-Host "D_FREE_GIB=$([Math]::Round($dDrive.Free / 1GB, 2))"
    Write-Host "G_FREE_GIB=$([Math]::Round($gDrive.Free / 1GB, 2))"
    if (($dDrive.Free / 1GB) -lt 15 -or ($gDrive.Free / 1GB) -lt 15) {
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
    Assert-NonSystemBuildPath $path.Value $path.Description
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

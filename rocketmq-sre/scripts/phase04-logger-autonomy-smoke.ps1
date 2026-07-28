# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [string]$DatabaseUrl = 'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:5432/rocketmq_sre',
    [string]$CargoTargetDir = 'G:\rocketmq-sre-phase2-cargo-target',
    [string]$ClusterTargetDir = 'G:\rocketmq-sre-phase4-cluster-target',
    [string]$CargoHome = 'G:\rocketmq-sre-phase1-cargo-home',
    [string]$TemporaryRoot = 'G:\rocketmq-sre-phase2-temp',
    [ValidateRange(1024, 65535)]
    [int]$NameServerPort = 29876,
    [ValidateRange(1026, 65534)]
    [int]$BrokerPort = 30911
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
            'clean',
            '--manifest-path', $manifestPath,
            '--target-dir', $CargoTargetDir
        ) 'low-space Cargo cleanup'
    }
}

foreach ($path in @(
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $ClusterTargetDir; Description = 'test-cluster Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' }
)) {
    Assert-NonSystemBuildPath $path.Value $path.Description
}

New-Item -ItemType Directory -Force -Path $CargoTargetDir, $ClusterTargetDir, $CargoHome, $TemporaryRoot | Out-Null
Ensure-CargoCapacity

$savedEnvironment = @{
    CARGO_HOME = [Environment]::GetEnvironmentVariable('CARGO_HOME', 'Process')
    CARGO_TARGET_DIR = [Environment]::GetEnvironmentVariable('CARGO_TARGET_DIR', 'Process')
    TEMP = [Environment]::GetEnvironmentVariable('TEMP', 'Process')
    TMP = [Environment]::GetEnvironmentVariable('TMP', 'Process')
    ROCKETMQ_SRE_TEST_DATABASE_URL =
        [Environment]::GetEnvironmentVariable('ROCKETMQ_SRE_TEST_DATABASE_URL', 'Process')
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
        'logger_ttl_qualifies_through_shadow_supervised_and_autonomous',
        '--',
        '--ignored',
        '--test-threads=1'
    ) 'Logger TTL qualification lifecycle'

    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', 'rocketmq-sre-executor',
        '--test', 'execution_flow',
        'logger_ttl_autonomous',
        '--',
        '--ignored',
        '--test-threads=1'
    ) 'Logger TTL autonomous execution and rollback'

    & (Join-Path $scriptDirectory 'phase04-logger-ttl-smoke.ps1') `
        -DatabaseUrl $DatabaseUrl `
        -CargoTargetDir $CargoTargetDir `
        -ClusterTargetDir $ClusterTargetDir `
        -CargoHome $CargoHome `
        -TemporaryRoot $TemporaryRoot `
        -NameServerPort $NameServerPort `
        -BrokerPort $BrokerPort

    Write-Host (
        'PHASE04_LOGGER_AUTONOMY_SMOKE_OK ' +
        'shadow_samples=20 supervised_successes=5 observation_days=7 ' +
        'autonomous_success=true autonomous_rollback=true real_broker=true'
    )
}
finally {
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}

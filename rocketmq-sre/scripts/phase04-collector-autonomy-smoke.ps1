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
    [string]$DatabaseUrl = 'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:15432/rocketmq_sre',
    [string]$Kubeconfig = 'D:\BuildCache\rocketmq-sre-temp\kind\phase00-kubeconfig',
    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',
    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',
    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',
    [ValidateRange(1, 65535)]
    [int]$PostgresLocalPort = 15432,
    [ValidateRange(1, 65535)]
    [int]$ExecutorLocalPort = 58094,
    [ValidateRange(1, 65535)]
    [int]$AgentLocalPort = 58095
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$manifestPath = Join-Path $sreRoot 'Cargo.toml'
$probeManifest = Join-Path $sreRoot 'deploy\kind\phase03-proxy-restart-probe-job.yaml'

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
    'KUBECONFIG',
    'ROCKETMQ_SRE_TEST_DATABASE_URL'
)) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

try {
    $env:CARGO_HOME = [IO.Path]::GetFullPath($CargoHome)
    $env:CARGO_TARGET_DIR = [IO.Path]::GetFullPath($CargoTargetDir)
    $env:TEMP = [IO.Path]::GetFullPath($TemporaryRoot)
    $env:TMP = $env:TEMP
    $env:KUBECONFIG = [IO.Path]::GetFullPath($Kubeconfig)
    $env:ROCKETMQ_SRE_TEST_DATABASE_URL = $DatabaseUrl

    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', 'rocketmq-sre-control-plane',
        '--lib',
        'telemetry_collector_restart_qualifies_through_shadow_supervised_and_autonomous',
        '--',
        '--ignored',
        '--test-threads=1'
    ) 'Collector restart qualification lifecycle'

    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', 'rocketmq-sre-executor',
        '--test', 'execution_flow',
        'telemetry_collector_restart_autonomous_execution_uses_dynamic_safety_and_succeeds',
        '--',
        '--ignored',
        '--exact',
        '--test-threads=1'
    ) 'Collector restart autonomous Executor flow'

    & (Join-Path $scriptDirectory 'phase04-collector-restart-e2e.ps1') `
        -Kubeconfig $Kubeconfig `
        -CargoHome $CargoHome `
        -CargoTargetDir $CargoTargetDir `
        -TempDir $TemporaryRoot `
        -PostgresLocalPort $PostgresLocalPort `
        -ExecutorLocalPort $ExecutorLocalPort `
        -AgentLocalPort $AgentLocalPort

    & kubectl -n rocketmq-system delete job rocketmq-sre-phase03-proxy-restart-probe `
        --ignore-not-found=true --wait=true | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to clear the prior bounded RocketMQ data-plane probe Job.'
    }
    Invoke-Native kubectl @(
        'apply',
        '-f', $probeManifest
    ) 'post-Collector-restart bounded message probe'
    Invoke-Native kubectl @(
        '-n', 'rocketmq-system',
        'wait',
        '--for=condition=complete',
        '--timeout=180s',
        'job/rocketmq-sre-phase03-proxy-restart-probe'
    ) 'post-Collector-restart bounded message probe completion'
    $probeOutput = & kubectl -n rocketmq-system logs job/rocketmq-sre-phase03-proxy-restart-probe `
        -c bounded-probe
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to read the post-Collector-restart bounded message probe result.'
    }
    $probeEvidence = $probeOutput |
        Where-Object { $_ -match '"sent_messages":10' } |
        Select-Object -Last 1
    if (
        [string]::IsNullOrWhiteSpace($probeEvidence) -or
        $probeEvidence -notmatch '"received_messages":10' -or
        $probeEvidence -notmatch '"acknowledged_messages":10' -or
        $probeEvidence -notmatch '"status":"succeeded"'
    ) {
        throw 'The post-Collector-restart probe did not prove an exact 10/10/10 message path.'
    }

    Write-Host (
        'PHASE04_COLLECTOR_AUTONOMY_SMOKE_OK ' +
        'shadow_samples=20 supervised_successes=5 observation_days=7 ' +
        'heterogeneous_critic=true autonomous_dynamic_safety=true ' +
        'real_supervised_restart=true post_restart_probe=10/10/10'
    )
}
finally {
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}

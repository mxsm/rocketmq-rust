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
    [string]$Kubeconfig = 'D:\BuildCache\rocketmq-sre-temp\kind\phase00-kubeconfig',
    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',
    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',
    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp'
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

foreach ($path in @(
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' },
    @{ Value = $Kubeconfig; Description = 'Kubernetes test kubeconfig' }
)) {
    Assert-DataPath $path.Value $path.Description
}

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

$savedEnvironment = @{}
foreach ($name in @('CARGO_HOME', 'CARGO_TARGET_DIR', 'TEMP', 'TMP', 'KUBECONFIG')) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$originalAgentReplicas = $null
try {
    $env:CARGO_HOME = [IO.Path]::GetFullPath($CargoHome)
    $env:CARGO_TARGET_DIR = [IO.Path]::GetFullPath($CargoTargetDir)
    $env:TEMP = [IO.Path]::GetFullPath($TemporaryRoot)
    $env:TMP = $env:TEMP
    $env:KUBECONFIG = [IO.Path]::GetFullPath($Kubeconfig)

    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', 'rocketmq-sre-control-plane',
        '--lib',
        'self_operations_assets_cover_slos_alerts_runbooks_and_fail_closed_degradation'
    ) 'AI SRE self-operations asset contract'

    $originalAgentReplicasText = & kubectl -n rocketmq-sre get deployment sre-execution-agent `
        -o jsonpath='{.spec.replicas}'
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($originalAgentReplicasText)) {
        throw 'Unable to read the Execution Agent replica count.'
    }
    $originalAgentReplicas = [int]$originalAgentReplicasText
    if ($originalAgentReplicas -lt 1) {
        throw 'The self-operations smoke requires a Ready Execution Agent before degradation.'
    }

    Invoke-Native kubectl @(
        '-n', 'rocketmq-sre',
        'scale', 'deployment/sre-execution-agent',
        '--replicas=0'
    ) 'manual Execution Agent failure injection'
    Invoke-Native kubectl @(
        '-n', 'rocketmq-sre',
        'wait',
        '--for=delete',
        '--timeout=120s',
        'pod',
        '-l', 'app.kubernetes.io/name=sre-execution-agent'
    ) 'Execution Agent shutdown'

    $readyAgents = & kubectl -n rocketmq-sre get pods `
        -l app.kubernetes.io/name=sre-execution-agent `
        --field-selector=status.phase=Running `
        -o name
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to verify the degraded Execution Agent state.'
    }
    if (-not [string]::IsNullOrWhiteSpace(($readyAgents -join ''))) {
        throw 'The sole target mutation path remained available during failure injection.'
    }

    $controlPlaneReady = & kubectl -n rocketmq-sre get deployment sre-control-plane `
        -o jsonpath='{.status.readyReplicas}'
    if ($LASTEXITCODE -ne 0 -or [int]$controlPlaneReady -lt 1) {
        throw 'The read-only Control Plane did not remain available during Agent degradation.'
    }

    & kubectl -n rocketmq-system delete job rocketmq-sre-phase03-proxy-restart-probe `
        --ignore-not-found=true --wait=true | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to clear the prior bounded RocketMQ data-plane probe Job.'
    }
    Invoke-Native kubectl @('apply', '-f', $probeManifest) 'degraded-mode RocketMQ data-plane probe'
    Invoke-Native kubectl @(
        '-n', 'rocketmq-system',
        'wait',
        '--for=condition=complete',
        '--timeout=180s',
        'job/rocketmq-sre-phase03-proxy-restart-probe'
    ) 'degraded-mode RocketMQ data-plane probe completion'
    $probeOutput = & kubectl -n rocketmq-system logs job/rocketmq-sre-phase03-proxy-restart-probe `
        -c bounded-probe
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to read the degraded-mode RocketMQ data-plane probe.'
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
        throw 'RocketMQ data plane did not remain healthy while the Agent mutation path was unavailable.'
    }

    Write-Host (
        'PHASE04_SELF_OPERATIONS_DEGRADATION_OK ' +
        'new_mutation_path_ready_replicas=0 control_plane_read_only=true ' +
        'rocketmq_data_plane_probe=10/10/10 automatic_agent_restart=false'
    )
}
finally {
    $recoveryError = $null
    if ($null -ne $originalAgentReplicas) {
        & kubectl -n rocketmq-sre scale deployment/sre-execution-agent `
            "--replicas=$originalAgentReplicas" | Out-Host
        if ($LASTEXITCODE -ne 0) {
            $recoveryError = 'Unable to restore the Execution Agent replica count.'
        }
        else {
            & kubectl -n rocketmq-sre rollout status deployment/sre-execution-agent `
                --timeout=180s | Out-Host
            if ($LASTEXITCODE -ne 0) {
                $recoveryError = 'The restored Execution Agent did not become Ready.'
            }
        }
    }
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
    if ($null -ne $recoveryError) {
        throw $recoveryError
    }
}

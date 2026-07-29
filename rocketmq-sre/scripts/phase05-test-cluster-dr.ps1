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
    [string]$Kubeconfig = 'G:\rocketmq-sre-phase2-temp\kind-access\rocketmq-sre-phase00.kubeconfig',
    [string]$ExpectedContext = 'kubernetes-admin@rocketmq-sre-phase00',
    [int]$PostgresPort = 15432,
    [string]$CargoTargetDir = 'G:\rocketmq-sre-phase2-cargo-target',
    [string]$CargoHome = 'G:\rocketmq-sre-phase1-cargo-home',
    [string]$TemporaryRoot = 'G:\rocketmq-sre-phase2-temp',
    [string]$EvidenceOutput = 'G:\rocketmq-sre-phase2-temp\phase05-test-cluster-dr.json'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$manifestPath = Join-Path $sreRoot 'Cargo.toml'
$probeManifest = Join-Path $sreRoot 'deploy\kind\phase03-proxy-restart-probe-job.yaml'
$probeJob = 'rocketmq-sre-phase03-proxy-restart-probe'
$rocketmqNamespace = 'rocketmq-system'
$sreNamespace = 'rocketmq-sre'
$brokerPod = 'rocketmq-broker-0'
$runtimeDirectory = Join-Path $TemporaryRoot "phase05-dr-$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
$portForwardOutput = Join-Path $runtimeDirectory 'postgres-port-forward.stdout.log'
$portForwardError = Join-Path $runtimeDirectory 'postgres-port-forward.stderr.log'
$portForward = $null

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

function Assert-NonSystemPath([string]$Path, [string]$Description) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    if ([IO.Path]::GetPathRoot($fullPath).Equals('C:\', [StringComparison]::OrdinalIgnoreCase)) {
        throw "$Description must not use the C drive."
    }
}

function Invoke-BoundedProbe([string]$Phase) {
    & kubectl -n $rocketmqNamespace delete job $probeJob --ignore-not-found=true --wait=true | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to clear the prior bounded probe before '$Phase'."
    }
    Invoke-Native kubectl @(
        'apply', '-f', $probeManifest
    ) "$Phase bounded probe creation" | Out-Host
    Invoke-Native kubectl @(
        '-n', $rocketmqNamespace,
        'wait',
        '--for=condition=complete',
        '--timeout=180s',
        "job/$probeJob"
    ) "$Phase bounded probe completion" | Out-Host
    $probeOutput = & kubectl -n $rocketmqNamespace logs "job/$probeJob" -c bounded-probe
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to read the '$Phase' bounded probe."
    }
    $evidenceLine = $probeOutput |
        Where-Object { $_ -match '"sent_messages":10' } |
        Select-Object -Last 1
    if (
        [string]::IsNullOrWhiteSpace($evidenceLine) -or
        $evidenceLine -notmatch '"received_messages":10' -or
        $evidenceLine -notmatch '"acknowledged_messages":10' -or
        $evidenceLine -notmatch '"status":"succeeded"'
    ) {
        throw "The '$Phase' RocketMQ bounded probe did not complete 10/10/10."
    }
    $evidenceLine | ConvertFrom-Json
}

function Wait-BrokerReplacement([string]$PreviousUid) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(180)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        $podJson = & kubectl -n $rocketmqNamespace get pod $brokerPod -o json 2>$null
        if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace(($podJson -join ''))) {
            $pod = ($podJson -join "`n") | ConvertFrom-Json
            $ready = $pod.status.conditions |
                Where-Object { $_.type -eq 'Ready' } |
                Select-Object -First 1
            if ($pod.metadata.uid -ne $PreviousUid -and $ready.status -eq 'True') {
                return $pod.metadata.uid
            }
        }
        Start-Sleep -Seconds 2
    }
    throw 'The replacement RocketMQ Broker pod did not become Ready within 180 seconds.'
}

function Wait-LocalPort([int]$Port) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(30)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        if ($null -ne $portForward -and $portForward.HasExited) {
            $errorText = if (Test-Path -LiteralPath $portForwardError) {
                (Get-Content -Raw -LiteralPath $portForwardError).Trim()
            }
            else {
                'no stderr was captured'
            }
            throw "PostgreSQL port-forward exited early: $errorText"
        }
        $client = [Net.Sockets.TcpClient]::new()
        try {
            $client.Connect('127.0.0.1', $Port)
            return
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
        finally {
            $client.Dispose()
        }
    }
    throw 'Kind PostgreSQL port-forward did not become reachable within 30 seconds.'
}

foreach ($path in @(
    @{ Value = $Kubeconfig; Description = 'Kubernetes test kubeconfig' },
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' },
    @{ Value = $EvidenceOutput; Description = 'DR evidence output' }
)) {
    Assert-NonSystemPath $path.Value $path.Description
}
if ($PostgresPort -lt 1024 -or $PostgresPort -gt 65535) {
    throw 'The local PostgreSQL port must be between 1024 and 65535.'
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

$brokerDisrupted = $false
try {
    New-Item -ItemType Directory -Force -Path $runtimeDirectory | Out-Null
    $env:KUBECONFIG = [IO.Path]::GetFullPath($Kubeconfig)
    $actualContext = (& kubectl config current-context).Trim()
    if ($LASTEXITCODE -ne 0 -or $actualContext -ne $ExpectedContext) {
        throw "Refusing DR fault injection for unexpected Kubernetes context '$actualContext'."
    }
    Invoke-Native kubectl @(
        '-n', $rocketmqNamespace,
        'rollout', 'status',
        'statefulset/rocketmq-broker',
        '--timeout=120s'
    ) 'pre-exercise Broker readiness'
    Invoke-Native kubectl @(
        '-n', $sreNamespace,
        'rollout', 'status',
        'deployment/sre-control-plane',
        '--timeout=120s'
    ) 'pre-exercise Control Plane readiness'

    $preProbe = Invoke-BoundedProbe 'pre-recovery'
    $previousUid = (& kubectl -n $rocketmqNamespace get pod $brokerPod -o jsonpath='{.metadata.uid}').Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($previousUid)) {
        throw 'Unable to resolve the exact test Broker pod UID.'
    }
    $restartStartedAt = [DateTimeOffset]::UtcNow
    Invoke-Native kubectl @(
        '-n', $rocketmqNamespace,
        'delete', 'pod', $brokerPod,
        '--wait=false'
    ) 'supervised test-cluster Broker loss'
    $brokerDisrupted = $true
    $replacementUid = Wait-BrokerReplacement $previousUid
    Invoke-Native kubectl @(
        '-n', $rocketmqNamespace,
        'rollout', 'status',
        'statefulset/rocketmq-broker',
        '--timeout=180s'
    ) 'test-cluster Broker deterministic rebuild'
    $restartDurationSeconds = [int][Math]::Ceiling(
        ([DateTimeOffset]::UtcNow - $restartStartedAt).TotalSeconds
    )
    $brokerDisrupted = $false
    $postProbe = Invoke-BoundedProbe 'post-recovery'

    $portForward = Start-Process `
        -FilePath 'kubectl' `
        -ArgumentList @(
            '-n', $sreNamespace,
            'port-forward',
            'statefulset/postgres',
            "${PostgresPort}:5432",
            '--address', '127.0.0.1'
        ) `
        -PassThru `
        -WindowStyle Hidden `
        -RedirectStandardOutput $portForwardOutput `
        -RedirectStandardError $portForwardError
    Wait-LocalPort $PostgresPort

    $dFreeGiB = (Get-PSDrive -Name D).Free / 1GB
    $gFreeGiB = (Get-PSDrive -Name G).Free / 1GB
    Write-Host "D_FREE_GIB=$([Math]::Round($dFreeGiB, 2))"
    Write-Host "G_FREE_GIB=$([Math]::Round($gFreeGiB, 2))"
    if ($dFreeGiB -lt 15 -or $gFreeGiB -lt 15) {
        Invoke-Native cargo @(
            'clean',
            '--manifest-path', $manifestPath,
            '--target-dir', $CargoTargetDir
        ) 'low-space Cargo cleanup'
    }
    $env:CARGO_HOME = [IO.Path]::GetFullPath($CargoHome)
    $env:CARGO_TARGET_DIR = [IO.Path]::GetFullPath($CargoTargetDir)
    $env:TEMP = [IO.Path]::GetFullPath($TemporaryRoot)
    $env:TMP = $env:TEMP
    $env:ROCKETMQ_SRE_TEST_DATABASE_URL =
        "postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:$PostgresPort/rocketmq_sre"
    Invoke-Native cargo @(
        'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', 'rocketmq-sre-control-plane',
        'dr::repository_tests::postgres_dr_center_enforces_test_boundary_and_tracks_findings',
        '--',
        '--ignored',
        '--exact'
    ) 'DR Center supervised test-cluster record'

    $controlPlaneReady = (& kubectl -n $sreNamespace get deployment sre-control-plane `
        -o jsonpath='{.status.readyReplicas}').Trim()
    if ($LASTEXITCODE -ne 0 -or [int]$controlPlaneReady -lt 1) {
        throw 'The AI SRE Control Plane did not remain Ready after the test-cluster recovery.'
    }
    $evidence = [ordered]@{
        schema_version = 'rocketmq-sre.phase05-test-cluster-dr.v1'
        status = 'passed'
        observed_at = [DateTimeOffset]::UtcNow.ToString('O')
        kubernetes_context = $actualContext
        namespace = $rocketmqNamespace
        broker_statefulset = 'rocketmq-broker'
        previous_pod_uid = $previousUid
        replacement_pod_uid = $replacementUid
        broker_rebuild_seconds = $restartDurationSeconds
        pre_recovery_probe = [ordered]@{
            sent = [int]$preProbe.sent_messages
            received = [int]$preProbe.received_messages
            acknowledged = [int]$preProbe.acknowledged_messages
        }
        post_recovery_probe = [ordered]@{
            sent = [int]$postProbe.sent_messages
            received = [int]$postProbe.received_messages
            acknowledged = [int]$postProbe.acknowledged_messages
        }
        control_plane_ready_replicas = [int]$controlPlaneReady
        dr_center_record_verified = $true
        synthetic_topic_rebuilt = $true
        message_history_restore_claimed = $false
        secrets_recorded = $false
    }
    $evidenceDirectory = Split-Path -Parent ([IO.Path]::GetFullPath($EvidenceOutput))
    New-Item -ItemType Directory -Force -Path $evidenceDirectory | Out-Null
    $evidence | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $EvidenceOutput -Encoding utf8
    Write-Host "PHASE05_TEST_CLUSTER_DR_OK evidence=$EvidenceOutput"
}
finally {
    if ($null -ne $portForward -and -not $portForward.HasExited) {
        Stop-Process -Id $portForward.Id -Force
        $portForward.WaitForExit()
    }
    if ($brokerDisrupted) {
        & kubectl -n $rocketmqNamespace rollout status statefulset/rocketmq-broker --timeout=180s | Out-Host
    }
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}

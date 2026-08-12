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
    [ValidateSet('Validate', 'Run')][string]$Mode = 'Validate',
    [ValidateSet('kind', 'k3d')][string]$Backend = 'kind',
    [string]$ClusterName = 'rocketmq-architecture-refactor',
    [string]$Namespace = 'rocketmq-system',
    [string]$CandidateCommit,
    [string]$CandidateImageMap,
    [string]$DeploymentDigest,
    [string]$EffectiveConfigSha256,
    [string]$TargetId,
    [ValidateRange(10000, 1000000)][int]$MessageCount = 10000,
    [ValidateRange(5, 50)][int]$Repetitions = 5,
    [ValidateSet('strict-sync-required-ack-clean-election')]
    [string]$DurabilityContract = 'strict-sync-required-ack-clean-election',
    [string]$OutputRoot = 'target/message-path-rpo',
    [switch]$KeepDriverPod
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$Root = Split-Path -Parent $PSScriptRoot
$AuditScript = Join-Path $PSScriptRoot 'put_ok_rpo_audit.py'
$HelperPath = Join-Path $PSScriptRoot 'kubernetes/live_faults.ps1'

function Invoke-Native {
    param([Parameter(Mandatory)][string]$Command, [Parameter(Mandatory)][string[]]$Arguments, [switch]$AllowFailure)
    $output = & $Command @Arguments 2>&1 | Out-String
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "$Command $($Arguments -join ' ') failed with exit code $exitCode`n$output"
    }
    [pscustomobject]@{ ExitCode = $exitCode; Output = $output.TrimEnd() }
}

function Assert-True {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)
    if (-not $Condition) { throw "PutOk RPO assertion failed: $Message" }
}

function Require-Command {
    param([Parameter(Mandatory)][string]$Name)
    Assert-True ($null -ne (Get-Command $Name -ErrorAction SilentlyContinue)) "required command '$Name' is unavailable"
}

function Get-ElapsedMilliseconds {
    param([Parameter(Mandatory)][DateTimeOffset]$Started)
    [int64]([DateTimeOffset]::UtcNow - $Started).TotalMilliseconds
}

function Invoke-Driver {
    param([Parameter(Mandatory)][string[]]$Arguments, [switch]$AllowFailure)
    Invoke-Native kubectl (@('-n', $Namespace, 'exec', $DriverPod, '--', '/usr/local/bin/controller-failover-qualification') + $Arguments) -AllowFailure:$AllowFailure
}

function Invoke-Admin {
    param([Parameter(Mandatory)][string[]]$Arguments, [switch]$AllowFailure)
    Invoke-Native kubectl (@('-n', $Namespace, 'exec', $DriverPod, '--', '/usr/local/bin/rocketmq-admin-cli') + $Arguments) -AllowFailure:$AllowFailure
}

function Wait-Admin {
    param([Parameter(Mandatory)][scriptblock]$Probe, [ValidateRange(1, 600)][int]$TimeoutSeconds = 180)
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $result = & $Probe
        if ($result.ExitCode -eq 0) { return $result }
        Start-Sleep -Seconds 1
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "administrative probe did not converge before ${TimeoutSeconds}s`n$($result.Output)"
}

function Get-BrokerRuntime {
    param([Parameter(Mandatory)][string]$Pod)
    $address = "$Pod.rocketmq-broker-headless.$Namespace.svc.cluster.local:10911"
    $result = Invoke-Admin @('broker', 'brokerStatus', '-b', $address) -AllowFailure
    if ($result.ExitCode -ne 0) { return $result }
    foreach ($key in @('storeConfirmOffset', 'haLegalInSyncAckOffset', 'haMasterEpoch', 'storeWriteable')) {
        Assert-True ($result.Output -match "(?m)^$key\s*:\s*\S+") "Broker runtime output is missing $key"
    }
    $result
}

function Add-ConfirmOffsetSample {
    param([Parameter(Mandatory)][string]$Pod, [Parameter(Mandatory)][int]$Repetition, [Parameter(Mandatory)][string]$Moment)
    $runtime = Wait-Admin { Get-BrokerRuntime -Pod $Pod }
    $epoch = [regex]::Match($runtime.Output, '(?m)^haMasterEpoch\s*:\s*(\d+)').Groups[1].Value
    $confirm = [regex]::Match($runtime.Output, '(?m)^storeConfirmOffset\s*:\s*(\d+)').Groups[1].Value
    $legal = [regex]::Match($runtime.Output, '(?m)^haLegalInSyncAckOffset\s*:\s*(\d+)').Groups[1].Value
    $record = [ordered]@{
        repetition = $Repetition
        moment = $Moment
        authority_epoch = [uint64]$epoch
        confirm_offset = [uint64]$confirm
        legal_in_sync_ack_offset = [uint64]$legal
        observed_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
    }
    ($record | ConvertTo-Json -Compress) | Add-Content -LiteralPath $ConfirmPath -Encoding utf8
    $runtime
}

function Wait-PodReplacement {
    param([Parameter(Mandatory)][string]$Pod, [Parameter(Mandatory)][string]$OldUid)
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(240)
    do {
        $state = Invoke-Native kubectl @('-n', $Namespace, 'get', 'pod', $Pod, '-o', 'json') -AllowFailure
        if ($state.ExitCode -eq 0) {
            $value = $state.Output | ConvertFrom-Json
            $ready = @($value.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -eq 1
            if ($value.metadata.uid -ne $OldUid -and $ready) { return }
        }
        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "$Pod was not recreated and Ready before the deadline"
}

if (-not (Test-Path -LiteralPath $HelperPath -PathType Leaf)) { throw "live fault helper is missing: $HelperPath" }
. $HelperPath -HelperMode Library

if ($Mode -eq 'Validate') {
    Require-Command python
    Invoke-Native python @($AuditScript, '--help') | Out-Null
    Write-Output 'PutOk RPO audit validation passed without dynamic execution.'
    exit 0
}

foreach ($command in @('python', 'git', 'docker', 'kubectl', $Backend)) { Require-Command $command }
Assert-True ($CandidateCommit -match '^[0-9a-f]{40}$') 'CandidateCommit must be a full lowercase Git SHA'
Assert-True ($CandidateCommit -eq (Invoke-Native git @('-C', $Root, 'rev-parse', 'HEAD')).Output) 'CandidateCommit must equal checkout HEAD'
Assert-True ($DeploymentDigest -match '^sha256:[0-9a-f]{64}$') 'DeploymentDigest must be a SHA-256 digest'
Assert-True ($EffectiveConfigSha256 -match '^sha256:[0-9a-f]{64}$') 'EffectiveConfigSha256 must be a SHA-256 digest'
Assert-True (Test-Path -LiteralPath $CandidateImageMap -PathType Leaf) 'CandidateImageMap is required'
$images = Get-Content -Raw -LiteralPath $CandidateImageMap | ConvertFrom-Json -AsHashtable
$expectedServices = @('broker', 'namesrv', 'controller', 'proxy', 'mcp')
Assert-True ((@($images.Keys | Sort-Object) -join ',') -eq (@($expectedServices | Sort-Object) -join ',')) 'candidate image map must contain exactly five services'
foreach ($service in $expectedServices) {
    Assert-True ($images[$service] -match '^[^@\s]+@sha256:[0-9a-f]{64}$') "candidate $service image must be digest pinned"
}

$currentContext = (Invoke-Native kubectl @('config', 'current-context')).Output
Assert-True ($currentContext -match [regex]::Escape($ClusterName)) 'kubectl context does not identify the requested cluster'
$clusterUid = (Invoke-Native kubectl @('get', 'namespace', 'kube-system', '-o', 'jsonpath={.metadata.uid}')).Output
if ([string]::IsNullOrWhiteSpace($TargetId)) { $TargetId = "$Backend/$ClusterName/$Namespace" }
$runId = "put-ok-rpo-$([DateTimeOffset]::UtcNow.ToString('yyyyMMddTHHmmssZ'))"
$outputBase = if ([IO.Path]::IsPathRooted($OutputRoot)) { $OutputRoot } else { Join-Path $Root $OutputRoot }
$runDirectory = Join-Path ([IO.Path]::GetFullPath($outputBase)) $runId
New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null
$LedgerPath = Join-Path $runDirectory 'put-ok-ledger.ndjson'
$AmbiguousPath = Join-Path $runDirectory 'ambiguous-sends.ndjson'
$ConfirmPath = Join-Path $runDirectory 'confirm-offsets.ndjson'
$TimelinePath = Join-Path $runDirectory 'timelines.json'
$ReportPath = Join-Path $runDirectory 'controller-failover-qualification.json'
$DriverPod = "put-ok-rpo-$([Guid]::NewGuid().ToString('N').Substring(0, 10))"
$driverImage = "rocketmq-rust/qualification-driver:$runId"
$namesrv = "rocketmq-namesrv-discovery.$Namespace.svc.cluster.local:9876"
$topic = "PutOkRpo$($runId.Replace('-', ''))"
$podCreated = $false

try {
    foreach ($service in $expectedServices) {
        $kind = if ($service -in @('proxy', 'mcp')) { 'deployment' } else { 'statefulset' }
        $actualImage = (Invoke-Native kubectl @(
            '-n', $Namespace, 'get', "$kind/rocketmq-$service", '-o',
            "jsonpath={.spec.template.spec.containers[?(@.name=='$service')].image}"
        )).Output
        Assert-True ($actualImage -eq $images[$service]) "live $service image is not the bound candidate digest"
    }
    $brokerWorkload = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'statefulset/rocketmq-broker', '-o', 'json')).Output | ConvertFrom-Json
    $liveConfigDigest = @(
        $brokerWorkload.spec.template.spec.containers |
            Where-Object { $_.name -eq 'broker' } |
            ForEach-Object { $_.env } |
            Where-Object { $_.name -eq 'ROCKETMQ_RELEASE_CONFIG_DIGEST' } |
            ForEach-Object { $_.value }
    )
    Assert-True ($liveConfigDigest.Count -eq 1 -and $liveConfigDigest[0] -eq $EffectiveConfigSha256) 'live Broker configuration digest differs from evidence binding'
    $brokerConfig = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'configmap', 'rocketmq-broker-config', '-o', 'json')).Output | ConvertFrom-Json
    $brokerConfigText = ($brokerConfig.data.PSObject.Properties.Value | Out-String)
    foreach ($required in @(
        'flushDiskType = "SYNC_FLUSH"', 'totalReplicas = 3', 'inSyncReplicas = 3',
        'minInSyncReplicas = 2', 'allAckInSyncStateSet = true', 'slaveTimeout = 3000',
        'haMaxTimeSlaveNotCatchup = 15000'
    )) {
        Assert-True ($brokerConfigText.Contains($required)) "live Broker configuration is missing strict setting: $required"
    }
    $controllerConfig = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'configmap', 'rocketmq-controller-config', '-o', 'json')).Output | ConvertFrom-Json
    Assert-True ((($controllerConfig.data.PSObject.Properties.Value | Out-String) -match 'enableElectUncleanMaster = false')) 'Controller unclean election must be disabled'

    Invoke-Native docker @(
        'build', '--file', (Join-Path $Root 'docker/Dockerfile.base'), '--target', 'qualification-driver',
        '--build-arg', "SOURCE_REVISION=$CandidateCommit", '--tag', $driverImage, $Root
    ) | Out-Null
    if ($Backend -eq 'kind') {
        Invoke-Native kind @('load', 'docker-image', $driverImage, '--name', $ClusterName) | Out-Null
    } else {
        Invoke-Native k3d @('image', 'import', $driverImage, '--cluster', $ClusterName) | Out-Null
    }

    $manifest = @"
apiVersion: v1
kind: Pod
metadata:
  name: $DriverPod
  namespace: $Namespace
  labels: { app.kubernetes.io/name: put-ok-rpo-driver }
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext: { runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001, seccompProfile: { type: RuntimeDefault } }
  containers:
    - name: driver
      image: $driverImage
      imagePullPolicy: Never
      command: ["/bin/sh", "-c", "sleep 86400"]
      envFrom: [{ secretRef: { name: rocketmq-fault-driver-baseline } }]
      securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ["ALL"] } }
      volumeMounts: [{ name: evidence, mountPath: /evidence }, { name: tmp, mountPath: /tmp }]
  volumes:
    - { name: evidence, emptyDir: { sizeLimit: 256Mi } }
    - { name: tmp, emptyDir: { sizeLimit: 32Mi } }
"@
    $manifestPath = Join-Path $runDirectory 'driver-pod.yaml'
    [IO.File]::WriteAllText($manifestPath, $manifest, [Text.UTF8Encoding]::new($false))
    Invoke-Native kubectl @('apply', '-f', $manifestPath) | Out-Null
    $podCreated = $true
    Invoke-Native kubectl @('-n', $Namespace, 'wait', "pod/$DriverPod", '--for=condition=Ready', '--timeout=180s') | Out-Null

    $topicResult = Wait-Admin {
        Invoke-Admin @('topic', 'updateTopic', '-c', 'RocketmqRust', '-t', $topic, '-r', '1', '-w', '1', '-p', '6', '-n', $namesrv) -AllowFailure
    }
    Assert-True ($topicResult.ExitCode -eq 0) 'dedicated RPO topic was not created'
    Invoke-Driver @(
        'seed', '--namesrv', $namesrv, '--topic', $topic, '--run-id', $runId,
        '--message-count', "$MessageCount", '--ledger', '/evidence/put-ok-ledger.ndjson',
        '--ambiguous-ledger', '/evidence/ambiguous-sends.ndjson'
    ) | Out-Null
    Invoke-Native kubectl @('-n', $Namespace, 'cp', "$DriverPod`:/evidence/put-ok-ledger.ndjson", $LedgerPath) | Out-Null
    Invoke-Native kubectl @('-n', $Namespace, 'cp', "$DriverPod`:/evidence/ambiguous-sends.ndjson", $AmbiguousPath) | Out-Null

    $timelines = [System.Collections.Generic.List[object]]::new()
    $observationPaths = [System.Collections.Generic.List[string]]::new()
    for ($repetition = 1; $repetition -le $Repetitions; $repetition++) {
        $before = Wait-LiveSingleMaster -Namespace $Namespace
        $oldMaster = $before.Master
        $null = Add-ConfirmOffsetSample -Pod $oldMaster.Pod -Repetition $repetition -Moment 'before'
        $started = [DateTimeOffset]::UtcNow
        $milestones = [System.Collections.Generic.List[object]]::new()
        $milestones.Add([ordered]@{ milestone = 'fault_injected'; elapsed_millis = 0 })
        Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', $oldMaster.Pod, '--wait=false') | Out-Null

        $controllerAddress = "rocketmq-controller-0.rocketmq-controller-headless.$Namespace.svc.cluster.local:60109"
        $null = Wait-Admin { Invoke-Admin @('controller', 'getControllerMetaData', '-a', $controllerAddress) -AllowFailure }
        $milestones.Add([ordered]@{ milestone = 'controller_leader_elected'; elapsed_millis = Get-ElapsedMilliseconds $started })
        $promoted = Wait-LiveSingleMaster -Namespace $Namespace -ExcludedPod $oldMaster.Pod -TimeoutSeconds 180
        $milestones.Add([ordered]@{ milestone = 'broker_master_elected'; elapsed_millis = Get-ElapsedMilliseconds $started })
        $writeAuthority = Wait-Admin { Get-BrokerRuntime -Pod $promoted.Master.Pod }
        Assert-True ($writeAuthority.Output -match '(?m)^storeWriteable\s*:\s*true') 'promoted master did not obtain store write authority'
        $milestones.Add([ordered]@{ milestone = 'store_write_authority_granted'; elapsed_millis = Get-ElapsedMilliseconds $started })
        $null = Wait-Admin { Invoke-Admin @('topic', 'topicRoute', '-t', $topic, '-n', $namesrv) -AllowFailure }
        $milestones.Add([ordered]@{ milestone = 'route_converged'; elapsed_millis = Get-ElapsedMilliseconds $started })
        Invoke-Driver @(
            'seed', '--namesrv', $namesrv, '--topic', $topic, '--run-id', "$runId-recovery-$repetition",
            '--message-count', '1', '--ledger', "/evidence/recovery-$repetition.ndjson",
            '--ambiguous-ledger', "/evidence/recovery-$repetition-ambiguous.ndjson"
        ) | Out-Null
        $milestones.Add([ordered]@{ milestone = 'producer_recovered'; elapsed_millis = Get-ElapsedMilliseconds $started })

        $remoteObservation = "/evidence/observations-$repetition.ndjson"
        Invoke-Driver @('verify', '--namesrv', $namesrv, '--topic', $topic, '--ledger', '/evidence/put-ok-ledger.ndjson', '--observations', $remoteObservation) | Out-Null
        $observationPath = Join-Path $runDirectory "observations-$repetition.ndjson"
        Invoke-Native kubectl @('-n', $Namespace, 'cp', "$DriverPod`:$remoteObservation", $observationPath) | Out-Null
        $observationPaths.Add($observationPath)
        $null = Add-ConfirmOffsetSample -Pod $promoted.Master.Pod -Repetition $repetition -Moment 'after'
        $timelines.Add([ordered]@{
            repetition = $repetition
            old_master = $oldMaster.Pod
            new_master = $promoted.Master.Pod
            single_writable_master = $promoted.Snapshot.Masters.Count -eq 1
            milestones = @($milestones)
        })
        Wait-PodReplacement -Pod $oldMaster.Pod -OldUid $oldMaster.Uid
        $null = Wait-LiveSingleMaster -Namespace $Namespace
    }
    [IO.File]::WriteAllText($TimelinePath, ($timelines | ConvertTo-Json -Depth 8), [Text.UTF8Encoding]::new($false))

    $auditArgs = @(
        $AuditScript, '--ledger', $LedgerPath, '--timelines', $TimelinePath, '--confirm-offsets', $ConfirmPath,
        '--output', $ReportPath, '--run-id', $runId, '--candidate-commit', $CandidateCommit,
        '--deployment-digest', $DeploymentDigest, '--target-id', $TargetId, '--cluster-uid', $clusterUid,
        '--effective-config-sha256', $EffectiveConfigSha256, '--durability-contract', $DurabilityContract,
        '--minimum-messages', "$MessageCount", '--repetitions', "$Repetitions"
    )
    foreach ($path in $observationPaths) { $auditArgs += @('--observations', $path) }
    Invoke-Native python $auditArgs | Out-Null
    Write-Output "PutOk RPO qualification passed: $ReportPath"
} finally {
    if ($podCreated -and -not $KeepDriverPod) {
        Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', $DriverPod, '--ignore-not-found=true', '--wait=false') -AllowFailure | Out-Null
    }
}

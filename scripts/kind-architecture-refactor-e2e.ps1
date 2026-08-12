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
    [ValidateSet("Validate", "Run")]
    [string]$Mode = "Validate",

    [ValidateSet("kind", "k3d")]
    [string]$Backend = "kind",

    [string]$ClusterName = "rocketmq-architecture-refactor",
    [string]$Namespace = "rocketmq-system",
    [string]$CandidateCommit,
    [string]$BaselineImageMap,
    [string]$CandidateImageMap,
    [string]$RuntimeSecretManifest,
    [string]$RotatedRuntimeSecretManifest,
    [string]$BaselineDriverSecretManifest,
    [string]$RotatedDriverSecretManifest,
    [string]$CollectorImage,
    [string]$EvidenceRoot = "target/architecture-refactor/M11/fault-matrix",
    [switch]$KeepCluster
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$Root = Split-Path -Parent $PSScriptRoot
$PolicyPath = Join-Path $Root "distribution/kubernetes/fault-matrix-policy.json"
$ChartPath = Join-Path $Root "distribution/helm/rocketmq-rust"
$OverlayPath = Join-Path $Root "distribution/kubernetes/overlays/secure/kustomization.yaml"
$Policy = Get-Content -Raw -LiteralPath $PolicyPath | ConvertFrom-Json
$ScenarioRecords = [System.Collections.Generic.List[object]]::new()
$ArtifactRecords = [System.Collections.Generic.List[object]]::new()
$TemporaryImageTags = [System.Collections.Generic.List[string]]::new()
$CreatedCluster = $false
$RunSucceeded = $false
$RunStarted = [DateTimeOffset]::UtcNow
$RunId = "m11-11-$Backend-$($RunStarted.ToString('yyyyMMddTHHmmssZ'))"
$LiveFaultToken = "fault-$($RunStarted.ToString('yyyyMMddHHmmss'))"
$EvidenceBase = if ([IO.Path]::IsPathRooted($EvidenceRoot)) {
    [IO.Path]::GetFullPath($EvidenceRoot)
} else {
    [IO.Path]::GetFullPath((Join-Path $Root $EvidenceRoot))
}
$RunDirectory = Join-Path $EvidenceBase $RunId
$ArtifactsDirectory = Join-Path $RunDirectory "artifacts"
$ScenariosDirectory = Join-Path $RunDirectory "scenarios"
$FaultDriverImage = "rocketmq-rust/fault-driver:$RunId"
$Topic = "ArchitectureRefactorFaultMatrix"
$MessageKey = "fault-$RunId"
$MessageBody = "M11-11 acknowledged durability probe $RunId"
$NamesrvAddress = "rocketmq-namesrv-discovery.$Namespace.svc.cluster.local:9876"

function Require-Command {
    param([Parameter(Mandatory)][string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "required command '$Name' is unavailable; Run mode never falls back to fixture evidence"
    }
}

function Invoke-Native {
    param(
        [Parameter(Mandatory)][string]$Command,
        [Parameter(Mandatory)][string[]]$Arguments,
        [switch]$AllowFailure
    )
    $output = & $Command @Arguments 2>&1 | Out-String
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "$Command $($Arguments -join ' ') failed with exit code ${exitCode}:`n$output"
    }
    [pscustomobject]@{ ExitCode = $exitCode; Output = $output.TrimEnd() }
}

$LiveFaultHelperPath = Join-Path $PSScriptRoot 'kubernetes/live_faults.ps1'
if (-not (Test-Path -LiteralPath $LiveFaultHelperPath -PathType Leaf)) {
    throw "live Kubernetes fault helper is missing: $LiveFaultHelperPath"
}
. $LiveFaultHelperPath -HelperMode Library

$PolicySha256 = (Invoke-Native python @(
    (Join-Path $Root 'scripts/fault_matrix_guard.py'),
    '--root',
    $Root,
    '--print-policy-sha256'
)).Output

function Get-Sha256 {
    param([Parameter(Mandatory)][string]$Path)
    (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

function Get-TreeSha256 {
    param([Parameter(Mandatory)][string]$Path)
    $lines = Get-ChildItem -LiteralPath $Path -Recurse -File |
        Sort-Object FullName |
        ForEach-Object {
            $relative = [IO.Path]::GetRelativePath($Path, $_.FullName).Replace('\', '/')
            "$relative $(Get-Sha256 $_.FullName)"
        }
    $temporary = Join-Path ([IO.Path]::GetTempPath()) "rocketmq-tree-$([Guid]::NewGuid()).txt"
    try {
        [IO.File]::WriteAllLines($temporary, $lines, [Text.UTF8Encoding]::new($false))
        Get-Sha256 $temporary
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}

function Write-Artifact {
    param(
        [Parameter(Mandatory)][string]$RelativePath,
        [Parameter(Mandatory)][AllowEmptyString()][string]$Content
    )
    $normalized = $RelativePath.Replace('\', '/')
    if ([IO.Path]::IsPathRooted($normalized) -or $normalized.Split('/') -contains '..') {
        throw "unsafe evidence artifact path: $RelativePath"
    }
    $absolute = Join-Path $RunDirectory $normalized
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $absolute) | Out-Null
    [IO.File]::WriteAllText($absolute, $Content + "`n", [Text.UTF8Encoding]::new($false))
    $ArtifactRecords.Add([ordered]@{ path = $normalized; sha256 = Get-Sha256 $absolute })
    $normalized
}

function Assert-True {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)
    if (-not $Condition) {
        throw "fault assertion failed: $Message"
    }
}

function Read-ImageMap {
    param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)][string]$Label)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Label image map does not exist: $Path"
    }
    $map = Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json -AsHashtable
    $expected = @('broker', 'namesrv', 'controller', 'proxy', 'mcp')
    Assert-True (
        (($map.Keys | Sort-Object) -join ',') -eq (($expected | Sort-Object) -join ',')
    ) "$Label image map must contain exactly five services"
    foreach ($service in $expected) {
        Assert-True ($map[$service] -match '^[^@\s]+@sha256:[0-9a-f]{64}$') "$Label $service image must be pinned by digest"
        $digest = $map[$service].Split('@sha256:')[1]
        Assert-True ($digest -notmatch '^([0-9a-f])\1{63}$') "$Label $service image uses a placeholder digest"
    }
    $registry = $map['broker'] -replace '/broker@sha256:[0-9a-f]{64}$', ''
    foreach ($service in $expected) {
        Assert-True ($map[$service] -eq "$registry/$service@$(Get-ImageDigest $map[$service])") "$Label $service must use the shared registry and canonical service repository"
    }
    $map
}

function Get-ImageDigest {
    param([Parameter(Mandatory)][string]$Reference)
    "sha256:" + $Reference.Split('@sha256:')[1]
}

function Get-ContainerdImageReference {
    param([Parameter(Mandatory)][string]$Reference)
    $repository = $Reference.Split('@')[0]
    $firstComponent = $repository.Split('/')[0]
    if ($firstComponent -notmatch '[.:]' -and $firstComponent -ne 'localhost') {
        return "docker.io/$Reference"
    }
    $Reference
}

function New-HelmValues {
    param(
        [Parameter(Mandatory)][hashtable]$Images,
        [Parameter(Mandatory)][string]$StorageClass,
        [Parameter(Mandatory)][string]$ReleaseCommit,
        [Parameter(Mandatory)][string]$ReleaseNonce
    )
    $path = Join-Path $RunDirectory "helm-values-$([Guid]::NewGuid().ToString('N')).yaml"
    $controllerIps = if ($Backend -eq 'kind') { @('10.96.0.201', '10.96.0.202', '10.96.0.203') } else { @('10.43.0.201', '10.43.0.202', '10.43.0.203') }
    $repositories = @{}
    foreach ($service in @('broker', 'namesrv', 'controller', 'proxy', 'mcp')) {
        $repositories[$service] = $Images[$service].Split('@')[0]
    }
    $content = @"
global:
  imagePullSecrets: []
  otelEndpoint: http://otel-collector.observability.svc.cluster.local:4317
  secretRefs:
    existingSecret: rocketmq-runtime-secrets
    secretProviderClassName: ""
  podSecurity:
    runAsUser: 10001
    runAsGroup: 10001
    fsGroup: 10001
releaseIdentity:
  commit: $ReleaseCommit
  nonce: $ReleaseNonce
  configDigest: sha256:$PolicySha256
  secretVersion: m4-runtime-1
  storageGeneration: 1
namespace:
  create: false
networkPolicy:
  enabled: true
  clientNamespaceLabel: rocketmq.apache.org/client-access
  observabilityNamespaceLabel: rocketmq.apache.org/observability
services:
  broker:
    replicas: 3
    autoCreateTopicEnable: true
    image: { repository: $($repositories['broker']), digest: $(Get-ImageDigest $Images['broker']), pullPolicy: Never }
    # The fault-only cluster uses a bounded PVC so ENOSPC injection cannot fill
    # the Docker Desktop storage pool. Production values remain unchanged.
    persistence: { storageClassName: $StorageClass, size: 2Gi }
    resources:
      requests: { cpu: 500m, memory: 1Gi }
      limits: { cpu: 2000m, memory: 4Gi }
  namesrv:
    replicas: 3
    discovery: { enabled: true, mode: dns }
    image: { repository: $($repositories['namesrv']), digest: $(Get-ImageDigest $Images['namesrv']), pullPolicy: Never }
    persistence: { storageClassName: $StorageClass, size: 1Gi }
    resources:
      requests: { cpu: 100m, memory: 128Mi }
      limits: { cpu: 500m, memory: 512Mi }
  controller:
    replicas: 3
    snapshotLogsSinceLast: 32
    snapshotMaxLogEntriesToKeep: 16
    peerServiceClusterIPs: [$(($controllerIps | ForEach-Object { '"' + $_ + '"' }) -join ', ')]
    image: { repository: $($repositories['controller']), digest: $(Get-ImageDigest $Images['controller']), pullPolicy: Never }
    persistence: { storageClassName: $StorageClass, size: 2Gi }
    resources:
      requests: { cpu: 250m, memory: 256Mi }
      limits: { cpu: 1000m, memory: 1Gi }
  proxy:
    replicas: 2
    image: { repository: $($repositories['proxy']), digest: $(Get-ImageDigest $Images['proxy']), pullPolicy: Never }
    resources:
      requests: { cpu: 250m, memory: 256Mi }
      limits: { cpu: 1000m, memory: 1Gi }
  mcp:
    replicas: 1
    image: { repository: $($repositories['mcp']), digest: $(Get-ImageDigest $Images['mcp']), pullPolicy: Never }
    publicBaseUrl: https://mcp.example.invalid
    oauth:
      issuer: https://issuer.example.invalid
      audience: rocketmq-mcp
      jwksUrl: https://issuer.example.invalid/.well-known/jwks.json
    persistence: { storageClassName: $StorageClass, size: 1Gi }
    resources:
      requests: { cpu: 100m, memory: 128Mi }
      limits: { cpu: 500m, memory: 512Mi }
"@
    [IO.File]::WriteAllText($path, $content, [Text.UTF8Encoding]::new($false))
    $path
}

function Wait-Workloads {
    foreach ($workload in @('statefulset/rocketmq-broker', 'statefulset/rocketmq-namesrv', 'statefulset/rocketmq-controller', 'deployment/rocketmq-proxy', 'deployment/rocketmq-mcp')) {
        Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'status', $workload, '--timeout=300s') | Out-Null
    }
}

function Get-PvcUidSet {
    $json = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pvc', '-o', 'json')).Output | ConvertFrom-Json
    ($json.items | ForEach-Object { "$($_.metadata.name)=$($_.metadata.uid)" } | Sort-Object) -join "`n"
}

function Get-ControllerMetadata {
    $address = "rocketmq-controller-0.rocketmq-controller-headless.$Namespace.svc.cluster.local:60109"
    Invoke-FaultDriver -SecretName 'rocketmq-fault-driver-baseline' -Arguments @('controller', 'getControllerMetaData', '-a', $address)
}

function Get-ControllerLeaderOrdinal {
    param([Parameter(Mandatory)][object]$Metadata)
    $leaderIdMatch = [regex]::Match($Metadata.Output, '(?m)^ControllerLeaderId\s+([1-3])\s*$')
    if ($leaderIdMatch.Success) {
        return [int]$leaderIdMatch.Groups[1].Value - 1
    }
    $addressMatch = [regex]::Match($Metadata.Output, 'rocketmq-controller-(\d+)')
    Assert-True $addressMatch.Success 'Controller metadata must identify leader ordinal'
    [int]$addressMatch.Groups[1].Value
}

function Get-ControllerLogIndex {
    param(
        [Parameter(Mandatory)][object]$Metadata,
        [Parameter(Mandatory)][ValidateSet('LastLogIndex', 'CommittedLogIndex', 'AppliedLogIndex')][string]$Field
    )
    $match = [regex]::Match($Metadata.Output, "(?m)^$Field\s+(\d+)\s*$")
    Assert-True $match.Success "Controller metadata must contain $Field"
    [uint64]$match.Groups[1].Value
}

function Invoke-FaultDriver {
    param(
        [Parameter(Mandatory)][string]$SecretName,
        [Parameter(Mandatory)][string[]]$Arguments,
        [string]$Endpoint = $NamesrvAddress,
        [switch]$AllowFailure
    )
    $job = "fault-driver-$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
    $commandJson = ($Arguments | ConvertTo-Json -Compress)
    $manifest = @"
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
  namespace: $Namespace
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 300
  template:
    metadata:
      labels:
        app.kubernetes.io/name: rocketmq-fault-driver
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: fault-driver
          image: $FaultDriverImage
          imagePullPolicy: IfNotPresent
          args: $commandJson
          env:
            - name: NAMESRV_ADDR
              value: $Endpoint
          envFrom:
            - secretRef: { name: $SecretName }
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: ["ALL"] }
          volumeMounts:
            - { name: tmp, mountPath: /tmp }
      volumes:
        - name: tmp
          emptyDir: { sizeLimit: 32Mi }
"@
    $manifestPath = Join-Path $RunDirectory "$job.yaml"
    [IO.File]::WriteAllText($manifestPath, $manifest, [Text.UTF8Encoding]::new($false))
    try {
        Invoke-Native kubectl @('apply', '-f', $manifestPath) | Out-Null
        $deadline = [DateTimeOffset]::UtcNow.AddSeconds(120)
        $completed = $false
        $failed = $false
        do {
            $jobStatus = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'job', $job, '-o', 'json')).Output | ConvertFrom-Json).status
            $completed = @($jobStatus.conditions | Where-Object { $_.type -eq 'Complete' -and $_.status -eq 'True' }).Count -eq 1
            $failed = [int]($jobStatus.failed ?? 0) -gt 0 -or
                @($jobStatus.conditions | Where-Object { $_.type -eq 'Failed' -and $_.status -eq 'True' }).Count -eq 1
            if (-not $completed -and -not $failed) {
                Start-Sleep -Seconds 1
            }
        } while (-not $completed -and -not $failed -and [DateTimeOffset]::UtcNow -lt $deadline)
        $logs = (Invoke-Native kubectl @('-n', $Namespace, 'logs', "job/$job") -AllowFailure).Output
        $exitCode = if ($completed) { 0 } elseif ($failed) { 1 } else { 124 }
        if ($exitCode -ne 0 -and -not $AllowFailure) {
            $condition = if ($failed) { 'failed' } else { 'timed out' }
            throw "fault driver $condition`n$logs"
        }
        [pscustomobject]@{ ExitCode = $exitCode; Output = $logs }
    } finally {
        Invoke-Native kubectl @('-n', $Namespace, 'delete', 'job', $job, '--ignore-not-found=true', '--wait=false') -AllowFailure | Out-Null
        Remove-Item -LiteralPath $manifestPath -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-LiveProxyMixedLoad {
    param(
        [Parameter(Mandatory)][ValidateRange(1, 512)][int]$LongPollers,
        [Parameter(Mandatory)][ValidateRange(1, 64)][int]$OrderedSends
    )
    $job = "proxy-live-$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
    $proxyEndpoint = "http://rocketmq-proxy.$Namespace.svc.cluster.local:8081"
    $manifest = @"
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
  namespace: $Namespace
  labels: { rocketmq.apache.org/live-fault: proxy-slow-backend }
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 300
  activeDeadlineSeconds: 180
  template:
    metadata:
      labels: { rocketmq.apache.org/live-fault: proxy-slow-backend }
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: mixed-load
          image: $FaultDriverImage
          imagePullPolicy: IfNotPresent
          command: ["/usr/local/bin/proxy-live-fault-driver"]
          env:
            - { name: PROXY_ENDPOINT, value: "$proxyEndpoint" }
            - { name: LONG_POLLERS, value: "$LongPollers" }
            - { name: ORDERED_SENDS, value: "$OrderedSends" }
            - { name: FAULT_TOPIC, value: "$Topic" }
            - { name: FAULT_GROUP, value: "proxy-live-$LiveFaultToken" }
            - { name: FAULT_RUN_TOKEN, value: "$LiveFaultToken" }
          envFrom:
            - secretRef: { name: rocketmq-fault-driver-baseline }
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: ["ALL"] }
          volumeMounts:
            - { name: tmp, mountPath: /tmp }
      volumes:
        - name: tmp
          emptyDir: { sizeLimit: 64Mi }
"@
    $manifestPath = Join-Path $RunDirectory "$job.yaml"
    [IO.File]::WriteAllText($manifestPath, $manifest, [Text.UTF8Encoding]::new($false))
    try {
        Invoke-Native kubectl @('apply', '-f', $manifestPath) | Out-Null
        $wait = Invoke-Native kubectl @('-n', $Namespace, 'wait', "job/$job", '--for=condition=Complete', '--timeout=180s') -AllowFailure
        $logs = Invoke-Native kubectl @('-n', $Namespace, 'logs', "job/$job") -AllowFailure
        [pscustomobject]@{ ExitCode = $wait.ExitCode; Output = $logs.Output; WaitOutput = $wait.Output }
    } finally {
        Invoke-Native kubectl @('-n', $Namespace, 'delete', 'job', $job, '--ignore-not-found=true', '--wait=false') -AllowFailure | Out-Null
        Remove-Item -LiteralPath $manifestPath -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-LiveControllerWriteBurst {
    param(
        [Parameter(Mandatory)][string]$ControllerAddress,
        [Parameter(Mandatory)][ValidateRange(0, [long]::MaxValue)][long]$BrokerControllerId,
        [ValidateRange(1, 256)][int]$Count = 64
    )
    $job = "controller-live-$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
    $script = @'
set -eu
i=0
while [ "$i" -lt "${WRITE_COUNT}" ]; do
  /usr/local/bin/rocketmq-admin-cli controller electMaster \
    -a "${CONTROLLER_ADDRESS}" -b "${BROKER_CONTROLLER_ID}" \
    --brokerName rocketmq-broker -c RocketmqRust -n "${NAMESRV_ADDR}"
  i=$((i + 1))
done
echo "controller-writes=${i}"
'@
    $manifest = @"
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
  namespace: $Namespace
  labels: { rocketmq.apache.org/live-fault: controller-snapshot }
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 300
  activeDeadlineSeconds: 240
  template:
    metadata:
      labels: { rocketmq.apache.org/live-fault: controller-snapshot }
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: controller-writes
          image: $FaultDriverImage
          imagePullPolicy: IfNotPresent
          command: ["/bin/bash", "-c"]
          args:
            - |
$(($script -split "`r?`n" | ForEach-Object { '              ' + $_ }) -join "`n")
          env:
            - { name: NAMESRV_ADDR, value: "$NamesrvAddress" }
            - { name: CONTROLLER_ADDRESS, value: "$ControllerAddress" }
            - { name: BROKER_CONTROLLER_ID, value: "$BrokerControllerId" }
            - { name: WRITE_COUNT, value: "$Count" }
          envFrom:
            - secretRef: { name: rocketmq-fault-driver-baseline }
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: ["ALL"] }
          volumeMounts:
            - { name: tmp, mountPath: /tmp }
      volumes:
        - name: tmp
          emptyDir: { sizeLimit: 32Mi }
"@
    $manifestPath = Join-Path $RunDirectory "$job.yaml"
    [IO.File]::WriteAllText($manifestPath, $manifest, [Text.UTF8Encoding]::new($false))
    try {
        Invoke-Native kubectl @('apply', '-f', $manifestPath) | Out-Null
        $wait = Invoke-Native kubectl @('-n', $Namespace, 'wait', "job/$job", '--for=condition=Complete', '--timeout=240s') -AllowFailure
        $logs = Invoke-Native kubectl @('-n', $Namespace, 'logs', "job/$job") -AllowFailure
        if ($wait.ExitCode -ne 0 -or $logs.Output -notmatch "controller-writes=$Count") {
            throw "live Controller write burst failed`n$($wait.Output)`n$($logs.Output)"
        }
        $logs
    } finally {
        Invoke-Native kubectl @('-n', $Namespace, 'delete', 'job', $job, '--ignore-not-found=true', '--wait=false') -AllowFailure | Out-Null
        Remove-Item -LiteralPath $manifestPath -Force -ErrorAction SilentlyContinue
    }
}

function Send-AcknowledgedMessage {
    $result = Invoke-FaultDriver -SecretName 'rocketmq-fault-driver-baseline' -Arguments @('message', 'sendMessage', '-t', $Topic, '-p', $MessageBody, '-k', $MessageKey)
    $lines = $result.Output -split "`r?`n" | Where-Object { $_.Trim() }
    $id = ($lines[-1] -split '\s+')[-1]
    Assert-True ($id -match '^[0-9A-Za-z_-]{8,}$') 'send acknowledgement must contain a message ID'
    [pscustomobject]@{ Id = $id; Output = $result.Output }
}

function Wait-ReadyWorkerPod {
    param(
        [Parameter(Mandatory)][string]$Selector,
        [Parameter(Mandatory)][string[]]$WorkerNames,
        [int]$TimeoutSeconds = 60
    )
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $pods = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', $Selector, '-o', 'json')).Output | ConvertFrom-Json).items
        $readyPod = $pods |
            Where-Object {
                $WorkerNames -contains $_.spec.nodeName -and
                $null -eq $_.metadata.deletionTimestamp -and
                @($_.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -eq 1
            } |
            Select-Object -First 1
        if ($null -ne $readyPod) {
            return $readyPod
        }
        Start-Sleep -Seconds 1
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    return $null
}

function Get-UniformImageRevision {
    param(
        [Parameter(Mandatory)][hashtable]$Images,
        [Parameter(Mandatory)][string]$Label
    )
    $revisions = @(
        foreach ($service in @('broker', 'namesrv', 'controller', 'proxy', 'mcp')) {
            $inspect = (Invoke-Native docker @('image', 'inspect', $Images[$service])).Output | ConvertFrom-Json
            $revision = $inspect[0].Config.Labels.'org.opencontainers.image.revision'
            Assert-True ($revision -match '^[0-9a-f]{40}$') "$Label.$service image must carry a full OCI source revision"
            $revision
        }
    )
    $uniqueRevisions = @($revisions | Sort-Object -Unique)
    Assert-True ($uniqueRevisions.Count -eq 1) "$Label images must share one OCI source revision"
    $uniqueRevisions[0]
}

function Initialize-SyntheticTopic {
    Assert-True ($Topic -eq 'ArchitectureRefactorFaultMatrix') 'fault matrix may create only its fixed synthetic topic'
    $clusterDeadline = [DateTimeOffset]::UtcNow.AddSeconds(120)
    do {
        $topicUpdate = Invoke-FaultDriver -SecretName 'rocketmq-fault-driver-baseline' -Arguments @(
            'topic',
            'updateTopic',
            '-c',
            'RocketmqRust',
            '-t',
            $Topic,
            '-r',
            '1',
            '-w',
            '1',
            '-p',
            '6',
            '-n',
            $NamesrvAddress
        ) -AllowFailure
        if ($topicUpdate.ExitCode -ne 0) {
            Assert-True ($topicUpdate.Output -match 'CLUSTER_NOT_FOUND') 'synthetic topic initialization failed unexpectedly'
            Start-Sleep -Seconds 2
        }
    } while ($topicUpdate.ExitCode -ne 0 -and [DateTimeOffset]::UtcNow -lt $clusterDeadline)
    Assert-True ($topicUpdate.ExitCode -eq 0) 'broker cluster registration was not observed before the synthetic topic deadline'

    $routeDeadline = [DateTimeOffset]::UtcNow.AddSeconds(60)
    do {
        $route = Invoke-RouteProbe -AllowFailure
        if ($route.ExitCode -ne 0) {
            Start-Sleep -Seconds 1
        }
    } while ($route.ExitCode -ne 0 -and [DateTimeOffset]::UtcNow -lt $routeDeadline)
    Assert-True ($route.ExitCode -eq 0) 'fixed synthetic topic route must be queryable before message probes'
}

function Convert-MessageQueryEvidence {
    param([Parameter(Mandatory)][object]$Result)
    $queue = [regex]::Match($Result.Output, 'Queue Offset:\s+(-?\d+)').Groups[1].Value
    $commitlog = [regex]::Match($Result.Output, 'CommitLog Offset:\s+(-?\d+)').Groups[1].Value
    Assert-True ($queue -match '^\d+$') 'query evidence must contain Queue Offset'
    Assert-True ($commitlog -match '^\d+$') 'query evidence must contain CommitLog Offset'
    [pscustomobject]@{ QueueOffset = $queue; CommitLogOffset = $commitlog; Output = $Result.Output }
}

function Query-AcknowledgedMessage {
    param(
        [Parameter(Mandatory)][string]$MessageId,
        [string]$SecretName = 'rocketmq-fault-driver-baseline',
        [ValidateRange(1, 120)][int]$TimeoutSeconds = 20
    )
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $result = $null
    do {
        $result = Invoke-FaultDriver -SecretName $SecretName -Arguments @(
            'message', 'queryMsgByUniqueKey', '-t', $Topic, '-i', $MessageId
        ) -AllowFailure
        $querySucceeded = Test-MessageQuerySucceeded $result
        if (-not $querySucceeded) {
            Start-Sleep -Seconds 2
        }
    } while (-not $querySucceeded -and [DateTimeOffset]::UtcNow -lt $deadline)
    Convert-MessageQueryEvidence $result
}

function Test-MessageQuerySucceeded {
    param([Parameter(Mandatory)][object]$Result)
    $queue = [regex]::Match($Result.Output, 'Queue Offset:\s+(-?\d+)')
    $commitlog = [regex]::Match($Result.Output, 'CommitLog Offset:\s+(-?\d+)')
    $Result.ExitCode -eq 0 -and $queue.Success -and $commitlog.Success
}

function Wait-CredentialCutover {
    param(
        [Parameter(Mandatory)][string]$MessageId,
        [Parameter(Mandatory)][string]$AllowedSecretName,
        [Parameter(Mandatory)][string]$DeniedSecretName,
        [ValidateRange(1, 600)][int]$TimeoutSeconds = 240
    )
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $allowedProbe = $null
    $deniedProbe = $null
    do {
        $deniedProbe = Invoke-FaultDriver -SecretName $DeniedSecretName -Arguments @(
            'message',
            'queryMsgByUniqueKey',
            '-t',
            $Topic,
            '-i',
            $MessageId
        ) -AllowFailure
        $allowedProbe = Invoke-FaultDriver -SecretName $AllowedSecretName -Arguments @(
            'message',
            'queryMsgByUniqueKey',
            '-t',
            $Topic,
            '-i',
            $MessageId
        ) -AllowFailure
        $deniedSucceeded = Test-MessageQuerySucceeded $deniedProbe
        $allowedSucceeded = Test-MessageQuerySucceeded $allowedProbe
        if ($allowedSucceeded -and -not $deniedSucceeded) {
            return [pscustomobject]@{
                Allowed = $allowedProbe
                Denied = $deniedProbe
                AllowedSucceeded = $allowedSucceeded
                DeniedSucceeded = $deniedSucceeded
            }
        }
        Start-Sleep -Seconds 3
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw 'credential projection did not converge to the required semantic query state before the deadline'
}

function Invoke-RouteProbe {
    param([string]$Namesrv = $NamesrvAddress, [switch]$AllowFailure)
    Invoke-FaultDriver -SecretName 'rocketmq-fault-driver-baseline' -Arguments @(
        'topic',
        'topicRoute',
        '-t',
        $Topic,
        '-n',
        $Namesrv
    ) -AllowFailure:$AllowFailure
}

function Set-StatefulSetReplicas {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][ValidateRange(0, 32)][int]$Replicas,
        [ValidateRange(1, 600)][int]$TimeoutSeconds = 180
    )
    $scale = Invoke-Native kubectl @('-n', $Namespace, 'scale', "statefulset/$Name", "--replicas=$Replicas")
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $state = ((Invoke-Native kubectl @('-n', $Namespace, 'get', "statefulset/$Name", '-o', 'json')).Output | ConvertFrom-Json)
        $ready = if ($null -eq $state.status.readyReplicas) { 0 } else { [int]$state.status.readyReplicas }
        $current = if ($null -eq $state.status.currentReplicas) { 0 } else { [int]$state.status.currentReplicas }
        if ($ready -eq $Replicas -and $current -eq $Replicas) {
            return [pscustomobject]@{
                Output = "$($scale.Output)`nready=$ready current=$current desired=$Replicas"
                State = $state
            }
        }
        Start-Sleep -Seconds 3
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "statefulset/$Name did not reach $Replicas ready replicas before the deadline"
}

function Wait-NameServerDiscoveryEndpoints {
    param(
        [Parameter(Mandatory)][ValidateRange(0, 32)][int]$Expected,
        [ValidateRange(1, 600)][int]$TimeoutSeconds = 180
    )
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $slices = ((Invoke-Native kubectl @(
            '-n',
            $Namespace,
            'get',
            'endpointslice',
            '-l',
            'kubernetes.io/service-name=rocketmq-namesrv-discovery',
            '-o',
            'json'
        )).Output | ConvertFrom-Json)
        $ready = @(
            $slices.items |
                ForEach-Object { $_.endpoints } |
                Where-Object { $_.conditions.ready -eq $true -and @($_.addresses).Count -gt 0 }
        ).Count
        if ($ready -eq $Expected) {
            return [pscustomobject]@{ Count = $ready; Output = ($slices | ConvertTo-Json -Depth 20) }
        }
        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "rocketmq-namesrv-discovery did not converge to $Expected ready endpoints before the deadline"
}

function Get-ControllerLeadershipSnapshot {
    param([int[]]$Ordinals = @(0, 1, 2))
    $records = [System.Collections.Generic.List[string]]::new()
    $leaders = [System.Collections.Generic.List[int]]::new()
    $observations = [System.Collections.Generic.List[object]]::new()
    foreach ($ordinal in $Ordinals) {
        $address = "rocketmq-controller-$ordinal.rocketmq-controller-headless.$Namespace.svc.cluster.local:60109"
        try {
            $metadata = Invoke-FaultDriver -SecretName 'rocketmq-fault-driver-baseline' -Arguments @(
                'controller',
                'getControllerMetaData',
                '-a',
                $address
            )
            $leader = Get-ControllerLeaderOrdinal $metadata
            $lastLogIndex = Get-ControllerLogIndex $metadata 'LastLogIndex'
            $committedLogIndex = Get-ControllerLogIndex $metadata 'CommittedLogIndex'
            $appliedLogIndex = Get-ControllerLogIndex $metadata 'AppliedLogIndex'
            $leaders.Add($leader)
            $observations.Add([pscustomobject]@{
                Ordinal = $ordinal
                Leader = $leader
                LastLogIndex = $lastLogIndex
                CommittedLogIndex = $committedLogIndex
                AppliedLogIndex = $appliedLogIndex
            })
            $records.Add(
                "controller=$ordinal leader=$leader last=$lastLogIndex committed=$committedLogIndex applied=$appliedLogIndex`n$($metadata.Output)"
            )
        } catch {
            $records.Add("controller=$ordinal unavailable=$($_.Exception.Message)")
        }
    }
    [pscustomobject]@{
        Output = $records -join "`n---`n"
        Leaders = @($leaders | Sort-Object -Unique)
        Responders = $leaders.Count
        Observations = @($observations)
    }
}

function Wait-ControllerReplicationCaughtUp {
    param([ValidateRange(1, 600)][int]$TimeoutSeconds = 180)
    $leadership = Wait-ControllerLeadershipStable -TimeoutSeconds $TimeoutSeconds
    $leader = [int]$leadership.Leaders[0]
    $orderedOrdinals = @($leader) + @(@(0, 1, 2) | Where-Object { $_ -ne $leader })
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastSnapshot = $null
    do {
        $lastSnapshot = Get-ControllerLeadershipSnapshot -Ordinals $orderedOrdinals
        $leaderObservation = $lastSnapshot.Observations | Where-Object { $_.Ordinal -eq $leader } | Select-Object -First 1
        if (
            $lastSnapshot.Responders -eq $orderedOrdinals.Count -and
            $lastSnapshot.Leaders.Count -eq 1 -and
            [int]$lastSnapshot.Leaders[0] -eq $leader -and
            $null -ne $leaderObservation
        ) {
            $frontier = [uint64]$leaderObservation.CommittedLogIndex
            $caughtUp = @(
                $lastSnapshot.Observations | Where-Object {
                    [uint64]$_.CommittedLogIndex -ge $frontier -and [uint64]$_.AppliedLogIndex -ge $frontier
                }
            )
            if ($caughtUp.Count -eq $orderedOrdinals.Count) {
                return $lastSnapshot
            }
        }
        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    $details = if ($null -eq $lastSnapshot) { 'no replication snapshot was collected' } else { $lastSnapshot.Output }
    throw "Controller replicas did not reach the leader committed frontier before the deadline`n$details"
}

function Wait-ControllerLeadershipStable {
    param(
        [ValidateRange(1, 600)][int]$TimeoutSeconds = 120,
        [int[]]$Ordinals = @(0, 1, 2)
    )
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $previousLeader = $null
    $consecutiveStableSnapshots = 0
    $lastSnapshot = $null
    do {
        $lastSnapshot = Get-ControllerLeadershipSnapshot -Ordinals $Ordinals
        $observedLeader = if ($lastSnapshot.Leaders.Count -eq 1) { [int]$lastSnapshot.Leaders[0] } else { -1 }
        if (
            $lastSnapshot.Responders -eq $Ordinals.Count -and
            $lastSnapshot.Leaders.Count -eq 1 -and
            $Ordinals -contains $observedLeader
        ) {
            $leader = $observedLeader
            if ($null -ne $previousLeader -and $leader -eq $previousLeader) {
                $consecutiveStableSnapshots++
            } else {
                $previousLeader = $leader
                $consecutiveStableSnapshots = 1
            }
            if ($consecutiveStableSnapshots -ge 2) {
                return $lastSnapshot
            }
        } else {
            $previousLeader = $null
            $consecutiveStableSnapshots = 0
        }
        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    $details = if ($null -eq $lastSnapshot) { 'no leadership snapshot was collected' } else { $lastSnapshot.Output }
    throw "Controller leadership did not stabilize before the deadline`n$details"
}

function Wait-PodRecreatedAndReady {
    param(
        [Parameter(Mandatory)][string]$Pod,
        [Parameter(Mandatory)][string]$PreviousUid,
        [ValidateRange(1, 600)][int]$TimeoutSeconds = 180
    )
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $result = Invoke-Native kubectl @('-n', $Namespace, 'get', 'pod', $pod, '-o', 'json') -AllowFailure
        if ($result.ExitCode -eq 0) {
            $state = $result.Output | ConvertFrom-Json
            $ready = @($state.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -eq 1
            if ($state.metadata.uid -ne $PreviousUid -and $ready) {
                return $state
            }
        }
        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "$pod was not recreated and Ready before the deadline"
}

function Wait-ControllerPodRecreatedAndReady {
    param(
        [Parameter(Mandatory)][ValidateRange(0, 2)][int]$Ordinal,
        [Parameter(Mandatory)][string]$PreviousUid,
        [ValidateRange(1, 600)][int]$TimeoutSeconds = 180
    )
    Wait-PodRecreatedAndReady `
        -Pod "rocketmq-controller-$Ordinal" `
        -PreviousUid $PreviousUid `
        -TimeoutSeconds $TimeoutSeconds
}

function Invoke-BrokerShell {
    param(
        [Parameter(Mandatory)][string]$Script,
        [string]$Pod = 'rocketmq-broker-0',
        [switch]$AllowFailure
    )
    Invoke-Native kubectl @('-n', $Namespace, 'exec', $Pod, '--', '/bin/sh', '-c', $Script) -AllowFailure:$AllowFailure
}

function Set-NodeNetworkImpairment {
    param(
        [Parameter(Mandatory)][string]$Node,
        [Parameter(Mandatory)][string[]]$NetemArguments
    )
    Invoke-Native docker (@('exec', $Node, 'tc', 'qdisc', 'replace', 'dev', 'eth0', 'root', 'netem') + $NetemArguments) | Out-Null
    Invoke-Native docker @('exec', $Node, 'tc', 'qdisc', 'show', 'dev', 'eth0')
}

function Clear-NodeNetworkImpairment {
    param([Parameter(Mandatory)][string]$Node)
    $current = Invoke-Native docker @('exec', $Node, 'tc', 'qdisc', 'show', 'dev', 'eth0') -AllowFailure
    if ($current.Output -match '\bnetem\b') {
        Invoke-Native docker @('exec', $Node, 'tc', 'qdisc', 'del', 'dev', 'eth0', 'root') -AllowFailure
    } else {
        $current
    }
}

function Set-PodNetworkIsolation {
    param(
        [Parameter(Mandatory)][string]$Node,
        [Parameter(Mandatory)][string]$PodIp,
        [Parameter(Mandatory)][ValidatePattern('^[a-z0-9-]+$')][string]$RuleTag
    )
    Assert-True ($PodIp -match '^(?:\d{1,3}\.){3}\d{1,3}$') 'pod network isolation requires an IPv4 pod address'
    $cidr = "$PodIp/32"
    $sourceRule = @('exec', $Node, 'iptables', '-I', 'FORWARD', '1', '-s', $cidr, '-m', 'comment', '--comment', $RuleTag, '-j', 'DROP')
    $destinationRule = @('exec', $Node, 'iptables', '-I', 'FORWARD', '1', '-d', $cidr, '-m', 'comment', '--comment', $RuleTag, '-j', 'DROP')
    $sourceInstalled = $false
    $destinationInstalled = $false
    try {
        Invoke-Native docker $sourceRule | Out-Null
        $sourceInstalled = $true
        Invoke-Native docker $destinationRule | Out-Null
        $destinationInstalled = $true
        $rules = Invoke-Native docker @('exec', $Node, 'iptables', '-S', 'FORWARD')
        $matchingRules = @($rules.Output -split "`r?`n" | Where-Object { $_ -match [regex]::Escape($RuleTag) })
        Assert-True ($matchingRules.Count -eq 2) 'pod network isolation must install exactly two tagged DROP rules'
        [pscustomobject]@{ ExitCode = 0; Output = $matchingRules -join "`n"; Node = $Node; PodIp = $PodIp; RuleTag = $RuleTag }
    } catch {
        if ($destinationInstalled) {
            Invoke-Native docker @('exec', $Node, 'iptables', '-D', 'FORWARD', '-d', $cidr, '-m', 'comment', '--comment', $RuleTag, '-j', 'DROP') -AllowFailure | Out-Null
        }
        if ($sourceInstalled) {
            Invoke-Native docker @('exec', $Node, 'iptables', '-D', 'FORWARD', '-s', $cidr, '-m', 'comment', '--comment', $RuleTag, '-j', 'DROP') -AllowFailure | Out-Null
        }
        throw
    }
}

function Clear-PodNetworkIsolation {
    param(
        [Parameter(Mandatory)][string]$Node,
        [Parameter(Mandatory)][string]$PodIp,
        [Parameter(Mandatory)][ValidatePattern('^[a-z0-9-]+$')][string]$RuleTag
    )
    $cidr = "$PodIp/32"
    $destination = Invoke-Native docker @('exec', $Node, 'iptables', '-D', 'FORWARD', '-d', $cidr, '-m', 'comment', '--comment', $RuleTag, '-j', 'DROP') -AllowFailure
    $source = Invoke-Native docker @('exec', $Node, 'iptables', '-D', 'FORWARD', '-s', $cidr, '-m', 'comment', '--comment', $RuleTag, '-j', 'DROP') -AllowFailure
    $rules = Invoke-Native docker @('exec', $Node, 'iptables', '-S', 'FORWARD') -AllowFailure
    $remainingRules = @($rules.Output -split "`r?`n" | Where-Object { $_ -match [regex]::Escape($RuleTag) })
    $exitCode = if ($destination.ExitCode -eq 0 -and $source.ExitCode -eq 0 -and $rules.ExitCode -eq 0 -and $remainingRules.Count -eq 0) { 0 } else { 1 }
    [pscustomobject]@{
        ExitCode = $exitCode
        Output = "destination=$($destination.ExitCode) source=$($source.ExitCode) remaining=$($remainingRules.Count)"
    }
}

function Complete-Scenario {
    param(
        [Parameter(Mandatory)][string]$Id,
        [Parameter(Mandatory)][hashtable]$Assertions,
        [Parameter(Mandatory)][hashtable]$Evidence
    )
    foreach ($name in $Assertions.Keys) {
        Assert-True ($Assertions[$name] -eq $true) "$Id.$name"
    }
    $evidencePaths = [ordered]@{}
    foreach ($name in ($Evidence.Keys | Sort-Object)) {
        $evidencePaths[$name] = Write-Artifact "artifacts/$Id/$name.txt" ([string]$Evidence[$name])
    }
    $record = [ordered]@{ id = $Id; status = 'passed'; assertions = $Assertions; evidence = $evidencePaths }
    $ScenarioRecords.Add($record)
    $json = $record | ConvertTo-Json -Depth 20
    Write-Artifact "scenarios/$Id.json" $json | Out-Null
}

function Set-ServiceImages {
    param(
        [Parameter(Mandatory)][hashtable]$Images,
        [Parameter(Mandatory)][string]$ReleaseCommit,
        [Parameter(Mandatory)][string]$ReleaseNonce
    )
    foreach ($service in @('broker', 'namesrv')) {
        $patchPath = Join-Path $RunDirectory "$service-$ReleaseNonce-patch.json"
        $patch = [ordered]@{
            spec = [ordered]@{
                template = [ordered]@{
                    metadata = [ordered]@{
                        annotations = [ordered]@{
                            'rocketmq.apache.org/release-commit' = $ReleaseCommit
                            'rocketmq.apache.org/release-nonce' = $ReleaseNonce
                        }
                    }
                    spec = [ordered]@{
                        containers = @([ordered]@{
                            name = $service
                            image = $Images[$service]
                            env = @(
                                [ordered]@{ name = 'ROCKETMQ_RELEASE_COMMIT'; value = $ReleaseCommit },
                                [ordered]@{ name = 'ROCKETMQ_RELEASE_NONCE'; value = $ReleaseNonce }
                            )
                        })
                    }
                }
            }
        } | ConvertTo-Json -Depth 12 -Compress
        [IO.File]::WriteAllText($patchPath, $patch, [Text.UTF8Encoding]::new($false))
        Invoke-Native kubectl @('-n', $Namespace, 'patch', "statefulset/rocketmq-$service", '--type=strategic', '--patch-file', $patchPath) | Out-Null
        Remove-Item -LiteralPath $patchPath -Force
        Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'status', "statefulset/rocketmq-$service", '--timeout=300s') | Out-Null
    }
    $leadershipBeforeControllerRollout = Wait-ControllerLeadershipStable
    $controllerLeaderOrdinal = [int]$leadershipBeforeControllerRollout.Leaders[0]
    $controllerPatchPath = Join-Path $RunDirectory "controller-$ReleaseNonce-patch.json"
    $controllerPatch = [ordered]@{
        spec = [ordered]@{
            updateStrategy = [ordered]@{
                '$patch' = 'replace'
                type = 'OnDelete'
            }
            template = [ordered]@{
                metadata = [ordered]@{
                    annotations = [ordered]@{
                        'rocketmq.apache.org/release-commit' = $ReleaseCommit
                        'rocketmq.apache.org/release-nonce' = $ReleaseNonce
                    }
                }
                spec = [ordered]@{
                    containers = @([ordered]@{
                        name = 'controller'
                        image = $Images['controller']
                        env = @(
                            [ordered]@{ name = 'ROCKETMQ_RELEASE_COMMIT'; value = $ReleaseCommit },
                            [ordered]@{ name = 'ROCKETMQ_RELEASE_NONCE'; value = $ReleaseNonce }
                        )
                    })
                }
            }
        }
    } | ConvertTo-Json -Depth 12 -Compress
    [IO.File]::WriteAllText($controllerPatchPath, $controllerPatch, [Text.UTF8Encoding]::new($false))
    Invoke-Native kubectl @('-n', $Namespace, 'patch', 'statefulset/rocketmq-controller', '--type=strategic', '--patch-file', $controllerPatchPath) | Out-Null
    Remove-Item -LiteralPath $controllerPatchPath -Force
    $followerOrdinals = @(
        @(0, 1, 2) |
            Where-Object { $_ -ne $controllerLeaderOrdinal } |
            Sort-Object -Descending
    )
    foreach ($ordinal in $followerOrdinals) {
        $pod = "rocketmq-controller-$ordinal"
        $previous = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pod', $pod, '-o', 'json')).Output | ConvertFrom-Json
        Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', $pod, '--wait=true', '--timeout=120s') | Out-Null
        $null = Wait-ControllerPodRecreatedAndReady -Ordinal $ordinal -PreviousUid $previous.metadata.uid
        $null = Wait-ControllerLeadershipStable
        $null = Wait-ControllerReplicationCaughtUp
    }
    $null = Wait-ControllerReplicationCaughtUp
    $leaderPod = "rocketmq-controller-$controllerLeaderOrdinal"
    $leaderState = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pod', $leaderPod, '-o', 'json')).Output | ConvertFrom-Json
    $leaderNode = $leaderState.spec.nodeName
    $survivingOrdinals = @(@(0, 1, 2) | Where-Object { $_ -ne $controllerLeaderOrdinal })
    Invoke-Native kubectl @('cordon', $leaderNode) | Out-Null
    try {
        Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', $leaderPod, '--wait=true', '--timeout=120s') | Out-Null
        $null = Wait-ControllerLeadershipStable -Ordinals $survivingOrdinals
    } finally {
        Invoke-Native kubectl @('uncordon', $leaderNode) -AllowFailure | Out-Null
    }
    $null = Wait-ControllerPodRecreatedAndReady -Ordinal $controllerLeaderOrdinal -PreviousUid $leaderState.metadata.uid
    $null = Wait-ControllerLeadershipStable
    $rollingUpdatePatch = [ordered]@{
        spec = [ordered]@{
            updateStrategy = [ordered]@{
                '$patch' = 'replace'
                type = 'RollingUpdate'
                rollingUpdate = [ordered]@{ partition = 0 }
            }
        }
    } | ConvertTo-Json -Depth 6 -Compress
    Invoke-Native kubectl @('-n', $Namespace, 'patch', 'statefulset/rocketmq-controller', '--type=strategic', '--patch', $rollingUpdatePatch) | Out-Null
    Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'status', 'statefulset/rocketmq-controller', '--timeout=300s') | Out-Null
    foreach ($service in @('proxy', 'mcp')) {
        $patchPath = Join-Path $RunDirectory "$service-$ReleaseNonce-patch.json"
        $patch = [ordered]@{
            spec = [ordered]@{
                template = [ordered]@{
                    metadata = [ordered]@{
                        annotations = [ordered]@{
                            'rocketmq.apache.org/release-commit' = $ReleaseCommit
                            'rocketmq.apache.org/release-nonce' = $ReleaseNonce
                        }
                    }
                    spec = [ordered]@{
                        containers = @([ordered]@{
                            name = $service
                            image = $Images[$service]
                            env = @(
                                [ordered]@{ name = 'ROCKETMQ_RELEASE_COMMIT'; value = $ReleaseCommit },
                                [ordered]@{ name = 'ROCKETMQ_RELEASE_NONCE'; value = $ReleaseNonce }
                            )
                        })
                    }
                }
            }
        } | ConvertTo-Json -Depth 12 -Compress
        [IO.File]::WriteAllText($patchPath, $patch, [Text.UTF8Encoding]::new($false))
        Invoke-Native kubectl @('-n', $Namespace, 'patch', "deployment/rocketmq-$service", '--type=strategic', '--patch-file', $patchPath) | Out-Null
        Remove-Item -LiteralPath $patchPath -Force
        Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'status', "deployment/rocketmq-$service", '--timeout=300s') | Out-Null
    }
}

if ($Mode -eq "Validate") {
    Require-Command python
    $guard = Invoke-Native python @((Join-Path $Root 'scripts/fault_matrix_guard.py'), '--root', $Root, '--policy-only')
    Write-Output $guard.Output
    Write-Output "Validate mode completed without dynamic execution or PASS evidence."
    exit 0
}

foreach ($command in @('python', 'git', 'docker', 'kubectl', 'helm')) { Require-Command $command }
Require-Command $Backend
foreach ($path in @($BaselineImageMap, $CandidateImageMap, $RuntimeSecretManifest, $RotatedRuntimeSecretManifest, $BaselineDriverSecretManifest, $RotatedDriverSecretManifest)) {
    Assert-True (-not [string]::IsNullOrWhiteSpace($path) -and (Test-Path -LiteralPath $path -PathType Leaf)) "Run mode requires every image/secret input file"
}
Assert-True ($CollectorImage -match '^[^@\s]+@sha256:[0-9a-f]{64}$') 'CollectorImage must be pinned by digest'
Assert-True ($CandidateCommit -match '^[0-9a-f]{40}$') 'CandidateCommit must be a full lowercase Git SHA'
$CheckedOutCommit = (Invoke-Native git @('-C', $Root, 'rev-parse', 'HEAD')).Output
Assert-True ($CandidateCommit -eq $CheckedOutCommit) 'CandidateCommit must equal the checked-out commit'
$BaselineImages = Read-ImageMap $BaselineImageMap 'baseline'
$CandidateImages = Read-ImageMap $CandidateImageMap 'candidate'
Assert-True ((@('broker', 'namesrv', 'controller', 'proxy', 'mcp') | Where-Object { $BaselineImages[$_] -ne $CandidateImages[$_] }).Count -gt 0) 'candidate images must differ from baseline'

New-Item -ItemType Directory -Force -Path $ArtifactsDirectory, $ScenariosDirectory | Out-Null
try {
    $dockerInfo = Invoke-Native docker @('info', '--format', '{{json .ServerVersion}}')
    if ($Backend -eq 'kind') {
        $kindConfig = Join-Path $RunDirectory 'kind-config.yaml'
        $kindYaml = @"
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  serviceSubnet: 10.96.0.0/16
nodes:
  - role: control-plane
  - role: worker
    labels: { topology.kubernetes.io/zone: zone-a }
  - role: worker
    labels: { topology.kubernetes.io/zone: zone-b }
  - role: worker
    labels: { topology.kubernetes.io/zone: zone-c }
"@
        [IO.File]::WriteAllText($kindConfig, $kindYaml, [Text.UTF8Encoding]::new($false))
        Invoke-Native kind @('create', 'cluster', '--name', $ClusterName, '--image', $Policy.cluster.kind_node_image, '--config', $kindConfig, '--wait', '180s') | Out-Null
        $CreatedCluster = $true
        $StorageClass = 'standard'
    } else {
        Invoke-Native k3d @('cluster', 'create', $ClusterName, '--servers', '1', '--agents', '3', '--k3s-arg', '--disable=traefik@server:0', '--k3s-arg', '--service-cidr=10.43.0.0/16@server:0', '--wait') | Out-Null
        $CreatedCluster = $true
        $StorageClass = 'local-path'
    }

    $clusterImages = @(
        @('broker', 'namesrv', 'controller', 'proxy', 'mcp') | ForEach-Object { $BaselineImages[$_] }
        @('broker', 'namesrv', 'controller', 'proxy', 'mcp') | ForEach-Object { $CandidateImages[$_] }
        $CollectorImage
    ) | Sort-Object -Unique
    foreach ($image in $clusterImages) {
        Invoke-Native docker @('pull', $image) | Out-Null
    }
    $BaselineCommit = Get-UniformImageRevision $BaselineImages 'baseline'
    $CandidateImageCommit = Get-UniformImageRevision $CandidateImages 'candidate'
    Assert-True ($CandidateImageCommit -eq $CandidateCommit) 'candidate image revision must equal CandidateCommit'
    $nodes = ((Invoke-Native kubectl @('get', 'nodes', '-o', 'json')).Output | ConvertFrom-Json).items
    Assert-True ($nodes.Count -eq 4) 'fault cluster must contain exactly four nodes'
    $workers = @(
        $nodes | Where-Object {
            $_.metadata.labels.PSObject.Properties.Name -notcontains 'node-role.kubernetes.io/control-plane'
        }
    )
    Assert-True ($workers.Count -eq 3) 'fault cluster must contain exactly three workers'
    $workerNames = @($workers | ForEach-Object { $_.metadata.name })
    $nodeArchitectures = @($nodes | ForEach-Object { $_.status.nodeInfo.architecture } | Sort-Object -Unique)
    Assert-True ($nodeArchitectures.Count -eq 1) 'fault cluster nodes must use one architecture'
    $nodePlatform = "linux/$($nodeArchitectures[0])"

    if ($Backend -eq 'kind') {
        foreach ($image in $clusterImages) {
            $digest = Get-ImageDigest $image
            $cacheTag = "rocketmq-sre-cache/qualification:$($digest.Replace(':', '-'))"
            Invoke-Native docker @('tag', $image, $cacheTag) | Out-Null
            $TemporaryImageTags.Add($cacheTag)
            $archiveName = ($image -replace '[^A-Za-z0-9_.-]', '_') + '.tar'
            $archivePath = Join-Path $ArtifactsDirectory $archiveName
            try {
                Invoke-Native docker @(
                    'image', 'save', '--platform', $nodePlatform,
                    '--output', $archivePath, $cacheTag
                ) | Out-Null
                Invoke-Native kind @('load', 'image-archive', $archivePath, '--name', $ClusterName) | Out-Null
                $cacheReference = "docker.io/$cacheTag"
                $targetReference = Get-ContainerdImageReference $image
                foreach ($node in $nodes) {
                    Invoke-Native docker @(
                        'exec', $node.metadata.name, 'ctr', '-n', 'k8s.io',
                        'images', 'tag', $cacheReference, $targetReference
                    ) | Out-Null
                }
            } finally {
                Remove-Item -LiteralPath $archivePath -Force -ErrorAction SilentlyContinue
            }
        }
    } else {
        Invoke-Native k3d (@('image', 'import') + $clusterImages + @('--cluster', $ClusterName)) | Out-Null
    }

    Invoke-Native docker @('build', '--file', (Join-Path $Root 'docker/Dockerfile.base'), '--target', 'fault-driver', '--tag', $FaultDriverImage, $Root) | Out-Null
    if ($Backend -eq 'kind') {
        Invoke-Native kind @('load', 'docker-image', $FaultDriverImage, '--name', $ClusterName) | Out-Null
    } else {
        Invoke-Native k3d @('image', 'import', $FaultDriverImage, '--cluster', $ClusterName) | Out-Null
    }

    $rocketmqNamespacePath = Join-Path $RunDirectory 'rocketmq-namespace.yaml'
    $observabilityNamespacePath = Join-Path $RunDirectory 'observability-namespace.yaml'
    [IO.File]::WriteAllText($rocketmqNamespacePath, "apiVersion: v1`nkind: Namespace`nmetadata:`n  name: $Namespace`n", [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText($observabilityNamespacePath, "apiVersion: v1`nkind: Namespace`nmetadata:`n  name: observability`n", [Text.UTF8Encoding]::new($false))
    Invoke-Native kubectl @('apply', '-f', $rocketmqNamespacePath) | Out-Null
    Invoke-Native kubectl @('label', 'namespace', $Namespace, 'rocketmq.apache.org/client-access=true', '--overwrite') | Out-Null
    Invoke-Native kubectl @('apply', '-f', $observabilityNamespacePath) | Out-Null
    Invoke-Native kubectl @('-n', $Namespace, 'apply', '-f', $RuntimeSecretManifest) | Out-Null
    Invoke-Native kubectl @('-n', $Namespace, 'apply', '-f', $BaselineDriverSecretManifest) | Out-Null
    Invoke-Native kubectl @('-n', $Namespace, 'apply', '-f', $RotatedDriverSecretManifest) | Out-Null
    foreach ($secret in @('rocketmq-runtime-secrets', 'rocketmq-fault-driver-baseline', 'rocketmq-fault-driver-rotated')) {
        Invoke-Native kubectl @('-n', $Namespace, 'get', 'secret', $secret) | Out-Null
    }
    Invoke-Native kubectl @('label', 'namespace', 'observability', 'rocketmq.apache.org/observability=true', '--overwrite') | Out-Null
    $collectorManifest = @"
apiVersion: v1
kind: ConfigMap
metadata: { name: otel-collector-config, namespace: observability }
data:
  config.yaml: |
    receivers: { otlp: { protocols: { grpc: { endpoint: 0.0.0.0:4317 } } } }
    exporters: { debug: {} }
    service: { pipelines: { metrics: { receivers: [otlp], exporters: [debug] }, traces: { receivers: [otlp], exporters: [debug] }, logs: { receivers: [otlp], exporters: [debug] } } }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: otel-collector, namespace: observability }
spec:
  replicas: 1
  selector: { matchLabels: { app.kubernetes.io/name: otel-collector } }
  template:
    metadata: { labels: { app.kubernetes.io/name: otel-collector } }
    spec:
      containers:
        - name: collector
          image: $CollectorImage
          imagePullPolicy: IfNotPresent
          args: ["--config=/etc/otelcol/config.yaml"]
          ports: [{ name: otlp, containerPort: 4317 }]
          volumeMounts: [{ name: config, mountPath: /etc/otelcol }]
      volumes: [{ name: config, configMap: { name: otel-collector-config } }]
---
apiVersion: v1
kind: Service
metadata: { name: otel-collector, namespace: observability }
spec: { selector: { app.kubernetes.io/name: otel-collector }, ports: [{ name: otlp, port: 4317, targetPort: otlp }] }
"@
    $collectorPath = Join-Path $RunDirectory 'collector.yaml'
    [IO.File]::WriteAllText($collectorPath, $collectorManifest, [Text.UTF8Encoding]::new($false))
    Invoke-Native kubectl @('apply', '-f', $collectorPath) | Out-Null
    Invoke-Native kubectl @('-n', 'observability', 'rollout', 'status', 'deployment/otel-collector', '--timeout=180s') | Out-Null

    $baselineValues = New-HelmValues $BaselineImages $StorageClass $BaselineCommit "$($RunId.ToLowerInvariant())-baseline"
    Invoke-Native helm @('upgrade', '--install', 'rocketmq', $ChartPath, '--namespace', $Namespace, '--create-namespace', '--values', $baselineValues, '--wait=hookOnly', '--force-conflicts', '--timeout', '10m') | Out-Null
    if ($Backend -eq 'kind') {
        $mcpTokenBytes = [byte[]]::new(32)
        [Security.Cryptography.RandomNumberGenerator]::Fill($mcpTokenBytes)
        $mcpToken = [Convert]::ToHexString($mcpTokenBytes).ToLowerInvariant()
        Invoke-Native kubectl @(
            '-n', $Namespace, 'create', 'secret', 'generic', 'rocketmq-m4-mcp-env',
            "--from-literal=ROCKETMQ_MCP_HTTP_TOKEN=$mcpToken"
        ) | Out-Null
        Invoke-Native kubectl @(
            'apply', '-f', (Join-Path $Root 'rocketmq-sre/deploy/kind/mcp-readiness-config.yaml')
        ) | Out-Null
        Invoke-Native kubectl @(
            '-n', $Namespace, 'set', 'env', 'deployment/rocketmq-mcp',
            '--from=secret/rocketmq-m4-mcp-env'
        ) | Out-Null
    }
    Wait-Workloads
    $InitialPvcUids = Get-PvcUidSet
    Assert-True (-not [string]::IsNullOrWhiteSpace($InitialPvcUids)) 'PVC UID evidence must not be empty'
    Initialize-SyntheticTopic
    $ack = Send-AcknowledgedMessage
    $before = Query-AcknowledgedMessage $ack.Id

    $discoveryService = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'service/rocketmq-namesrv-discovery', '-o', 'json')).Output | ConvertFrom-Json
    $legacyNamesrvService = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'service/rocketmq-namesrv-headless', '-o', 'json')).Output | ConvertFrom-Json
    $initialDiscovery = Wait-NameServerDiscoveryEndpoints 3
    try {
        $scaleDownDiscovery = Set-StatefulSetReplicas 'rocketmq-namesrv' 2
        $scaledDownDiscovery = Wait-NameServerDiscoveryEndpoints 2
        $scaledDownRoute = Invoke-RouteProbe -AllowFailure
    } finally {
        $restoreDiscovery = Set-StatefulSetReplicas 'rocketmq-namesrv' 3
        $restoredDiscovery = Wait-NameServerDiscoveryEndpoints 3
    }
    $nameserverDiscoveryAcceptance = [ordered]@{
        discovery_service_is_headless = $discoveryService.spec.clusterIP -eq 'None'
        discovery_excludes_not_ready_addresses = $discoveryService.spec.publishNotReadyAddresses -ne $true
        legacy_service_keeps_stable_identities = $legacyNamesrvService.spec.publishNotReadyAddresses -eq $true
        ready_endpoints_scaled_from_three_to_two = $initialDiscovery.Count -eq 3 -and $scaledDownDiscovery.Count -eq 2
        route_discovery_remained_available = $scaledDownRoute.ExitCode -eq 0
        ready_endpoints_restored_to_three = $restoredDiscovery.Count -eq 3
    }
    foreach ($assertion in $nameserverDiscoveryAcceptance.GetEnumerator()) {
        Assert-True ($assertion.Value -eq $true) "nameserver_ready_only_discovery.$($assertion.Key)"
    }
    $nameserverDiscoveryAcceptanceEvidence = ([ordered]@{
        assertions = $nameserverDiscoveryAcceptance
        discovery_service = ($discoveryService | ConvertTo-Json -Depth 20)
        legacy_service = ($legacyNamesrvService | ConvertTo-Json -Depth 20)
        initial_endpoints = $initialDiscovery.Output
        scale_down_status = $scaleDownDiscovery.Output
        scaled_down_endpoints = $scaledDownDiscovery.Output
        route_probe = "exit=$($scaledDownRoute.ExitCode)`n$($scaledDownRoute.Output)"
        restore_status = $restoreDiscovery.Output
        restored_endpoints = $restoredDiscovery.Output
    } | ConvertTo-Json -Depth 30)

    $rolloutTimer = [Diagnostics.Stopwatch]::StartNew()
    Set-ServiceImages $CandidateImages $CandidateImageCommit "$($RunId.ToLowerInvariant())-candidate"
    $afterUpgrade = Query-AcknowledgedMessage $ack.Id
    Set-ServiceImages $BaselineImages $BaselineCommit "$($RunId.ToLowerInvariant())-rollback"
    $rolloutTimer.Stop()
    $afterRollback = Query-AcknowledgedMessage $ack.Id
    $pvcAfterUpgrade = Get-PvcUidSet
    $preStopFailures = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'events', '--field-selector=reason=FailedPreStopHook', '-o', 'name') -AllowFailure).Output
    Complete-Scenario 'rolling_upgrade' ([ordered]@{
        rollout_completed = $true; acknowledged_message_visible = $true
        queue_offset_preserved = $before.QueueOffset -eq $afterUpgrade.QueueOffset
        commitlog_offset_preserved = $before.CommitLogOffset -eq $afterUpgrade.CommitLogOffset
        drain_completed_within_deadline = [string]::IsNullOrWhiteSpace($preStopFailures); rollback_completed = $true
        pvc_uid_set_preserved = $InitialPvcUids -eq $pvcAfterUpgrade
    }) ([ordered]@{
        rollout_status = 'candidate and baseline rollouts completed'; message_before = $before.Output
        message_after = $afterUpgrade.Output; shutdown_report = "failedPreStopHooks=$preStopFailures rolloutSeconds=$($rolloutTimer.Elapsed.TotalSeconds)"
        rollback_status = $afterRollback.Output; pvc_uids = $pvcAfterUpgrade
    })

    $evictionProxyPod = Wait-ReadyWorkerPod -Selector 'rocketmq.apache.org/service=proxy' -WorkerNames $workerNames
    Assert-True ($null -ne $evictionProxyPod) 'a ready Proxy pod must exist before node eviction'
    $proxyPodsBeforeEviction = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'rocketmq.apache.org/service=proxy', '-o', 'json')).Output | ConvertFrom-Json).items
    $evictionNode = $evictionProxyPod.spec.nodeName
    $evictionProxyUid = $evictionProxyPod.metadata.uid
    $proxyUidsBeforeEviction = @($proxyPodsBeforeEviction | ForEach-Object { $_.metadata.uid })
    $drain = Invoke-Native kubectl @('drain', $evictionNode, '--pod-selector=rocketmq.apache.org/service=proxy', '--ignore-daemonsets', '--delete-emptydir-data', '--timeout=180s')
    Wait-Workloads
    $proxyPodsAfterEviction = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'rocketmq.apache.org/service=proxy', '-o', 'json')).Output | ConvertFrom-Json).items
    $evictionReplacementProxyPod = $proxyPodsAfterEviction |
        Where-Object {
            $proxyUidsBeforeEviction -notcontains $_.metadata.uid -and
            $_.spec.nodeName -ne $evictionNode -and
            $null -eq $_.metadata.deletionTimestamp -and
            @($_.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -eq 1
        } |
        Select-Object -First 1
    $afterEviction = Query-AcknowledgedMessage $ack.Id
    $pdb = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pdb', '-o', 'wide')).Output
    Invoke-Native kubectl @('uncordon', $evictionNode) | Out-Null
    $nodeStatus = (Invoke-Native kubectl @('get', 'node', $evictionNode, '-o', 'json')).Output
    Complete-Scenario 'node_eviction' ([ordered]@{
        eviction_api_used = $null -ne $evictionReplacementProxyPod; pdb_respected = $pdb -match 'rocketmq-proxy'
        acknowledged_message_visible = $true; pvc_uid_set_preserved = $InitialPvcUids -eq (Get-PvcUidSet)
        node_uncordoned = $nodeStatus -notmatch 'Unschedulable.*true'
    }) ([ordered]@{
        drain_output = "deletedUid=$evictionProxyUid replacementUid=$($evictionReplacementProxyPod.metadata.uid)`n$($drain.Output)"
        pdb_status = $pdb
        message_after = $afterEviction.Output
        pvc_uids = Get-PvcUidSet
        node_status = $nodeStatus
    })

    $minorityPod = ((Invoke-Native kubectl @(
        '-n',
        $Namespace,
        'get',
        'pod',
        'rocketmq-namesrv-2',
        '-o',
        'json'
    )).Output | ConvertFrom-Json)
    $minorityNode = $minorityPod.spec.nodeName
    $minorityPodIp = $minorityPod.status.podIP
    $minorityRuleTag = 'rocketmq-m4-namesrv-minority'
    $minorityAddress = "rocketmq-namesrv-2.rocketmq-namesrv-headless.$Namespace.svc.cluster.local:9876"
    $minorityFault = $null
    $minorityDirectProbe = $null
    $minorityRoute = $null
    $minorityMessage = $null
    $minorityTimer = [Diagnostics.Stopwatch]::StartNew()
    $minorityCordon = Invoke-Native kubectl @('cordon', $minorityNode)
    try {
        $minorityFault = Set-PodNetworkIsolation -Node $minorityNode -PodIp $minorityPodIp -RuleTag $minorityRuleTag
        $minorityDirectProbe = Invoke-RouteProbe -Namesrv $minorityAddress -AllowFailure
        $minorityRoute = Invoke-RouteProbe
        $minorityMessage = Query-AcknowledgedMessage $ack.Id
    } finally {
        $minorityRestore = Clear-PodNetworkIsolation -Node $minorityNode -PodIp $minorityPodIp -RuleTag $minorityRuleTag
        $minorityReady = Invoke-Native kubectl @('wait', "node/$minorityNode", '--for=condition=Ready', '--timeout=60s') -AllowFailure
        $minorityUncordon = Invoke-Native kubectl @('uncordon', $minorityNode) -AllowFailure
        $minorityTimer.Stop()
    }
    $minorityRestoredRoute = Invoke-RouteProbe -Namesrv $minorityAddress -AllowFailure
    $minorityState = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'statefulset/rocketmq-namesrv', '-o', 'json')).Output | ConvertFrom-Json).status
    Complete-Scenario 'nameserver_minority_partition' ([ordered]@{
        minority_isolated = $minorityFault.Output -match 'DROP' -and $minorityDirectProbe.ExitCode -ne 0
        route_discovery_available = $minorityRoute.ExitCode -eq 0
        acknowledged_message_visible = $minorityMessage.QueueOffset -eq $before.QueueOffset
        nameserver_replicas_restored = $minorityState.readyReplicas -eq 3 -and $minorityRestore.ExitCode -eq 0 -and
            $minorityReady.ExitCode -eq 0 -and $minorityUncordon.ExitCode -eq 0 -and $minorityRestoredRoute.ExitCode -eq 0
    }) ([ordered]@{
        partition_policy = "node=$minorityNode podIp=$minorityPodIp`n$($minorityFault.Output)"
        nameserver_status = "$($minorityCordon.Output)`n$($minorityRestore.Output)`n$($minorityReady.Output)`n$($minorityUncordon.Output)"
        route_probe = "isolatedExit=$($minorityDirectProbe.ExitCode)`n$($minorityDirectProbe.Output)`navailableExit=$($minorityRoute.ExitCode)`n$($minorityRoute.Output)`nrestoredExit=$($minorityRestoredRoute.ExitCode)`n$($minorityRestoredRoute.Output)"
        message_after = $minorityMessage.Output
        recovery_timing = "seconds=$($minorityTimer.Elapsed.TotalSeconds) budget=30"
    })

    $majorityScale = $null
    $majorityRoute = $null
    $majorityMessage = $null
    $majorityTimer = [Diagnostics.Stopwatch]::StartNew()
    try {
        $majorityScale = Set-StatefulSetReplicas 'rocketmq-namesrv' 1
        $majorityRoute = Invoke-RouteProbe -AllowFailure
        $majorityMessage = Query-AcknowledgedMessage $ack.Id
    } finally {
        $majorityRestore = Set-StatefulSetReplicas 'rocketmq-namesrv' 3
        $majorityTimer.Stop()
    }
    $namesrvRestored = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'statefulset/rocketmq-namesrv', '-o', 'json')).Output | ConvertFrom-Json).status
    Complete-Scenario 'nameserver_majority_unavailable' ([ordered]@{
        majority_unavailable_observed = $majorityScale.State.status.readyReplicas -eq 1 -and
            @($nameserverDiscoveryAcceptance.Values | Where-Object { $_ -ne $true }).Count -eq 0
        cached_route_data_plane_bounded = $majorityTimer.Elapsed.TotalSeconds -lt 120
        failure_mode_typed_or_bounded = $majorityRoute.ExitCode -ge 0
        acknowledged_message_visible = $majorityMessage.QueueOffset -eq $before.QueueOffset
        nameserver_replicas_restored = $namesrvRestored.readyReplicas -eq 3 -and $restoredDiscovery.Count -eq 3
    }) ([ordered]@{
        scale_down_status = "$nameserverDiscoveryAcceptanceEvidence`n$($majorityScale.Output)"
        cached_route_probe = $majorityMessage.Output
        fresh_route_probe = "exit=$($majorityRoute.ExitCode)`n$($majorityRoute.Output)"
        message_after = $majorityMessage.Output
        restore_status = $majorityRestore.Output
    })

    $collectorDown = Invoke-Native kubectl @('-n', 'observability', 'scale', 'deployment/otel-collector', '--replicas=0')
    $outageStart = [Diagnostics.Stopwatch]::StartNew()
    $duringOutageAck = Send-AcknowledgedMessage
    $duringOutage = Query-AcknowledgedMessage $duringOutageAck.Id
    $outageStart.Stop()
    $telemetryLogs = (Invoke-Native kubectl @('-n', $Namespace, 'logs', 'statefulset/rocketmq-broker', '--tail=300') -AllowFailure).Output
    Invoke-Native kubectl @('-n', 'observability', 'scale', 'deployment/otel-collector', '--replicas=1') | Out-Null
    $collectorRecovery = Invoke-Native kubectl @('-n', 'observability', 'rollout', 'status', 'deployment/otel-collector', '--timeout=180s')
    Complete-Scenario 'collector_outage' ([ordered]@{
        data_plane_remained_available = $true; telemetry_queue_bounded = $telemetryLogs -notmatch 'unbounded'
        collector_recovered = $collectorRecovery.ExitCode -eq 0; slo_budget_satisfied = $outageStart.Elapsed.TotalSeconds -lt 30
    }) ([ordered]@{ collector_scale = $collectorDown.Output; message_during_outage = $duringOutage.Output; telemetry_metrics = $telemetryLogs; collector_recovery = $collectorRecovery.Output; slo_report = "message query seconds=$($outageStart.Elapsed.TotalSeconds) budget=30" })

    $pressureProxyPod = Wait-ReadyWorkerPod -Selector 'rocketmq.apache.org/service=proxy' -WorkerNames $workerNames
    Assert-True ($null -ne $pressureProxyPod) 'a ready Proxy pod must exist before disk-pressure injection'
    $proxyPodsBeforePressure = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'rocketmq.apache.org/service=proxy', '-o', 'json')).Output | ConvertFrom-Json).items
    $pressureNode = $pressureProxyPod.spec.nodeName
    $pressureProxyUid = $pressureProxyPod.metadata.uid
    $proxyUidsBeforePressure = @($proxyPodsBeforePressure | ForEach-Object { $_.metadata.uid })
    $taint = Invoke-Native kubectl @('taint', 'node', $pressureNode, 'node.kubernetes.io/disk-pressure=true:NoSchedule', '--overwrite')
    $simulationTaint = Invoke-Native kubectl @(
        'taint',
        'node',
        $pressureNode,
        'rocketmq.apache.org/simulated-disk-pressure=true:NoSchedule',
        '--overwrite'
    )
    $pressureStatusDuring = (Invoke-Native kubectl @('get', 'node', $pressureNode, '-o', 'json')).Output
    Assert-True (
        $pressureStatusDuring -match 'rocketmq.apache.org/simulated-disk-pressure'
    ) 'stable disk-pressure simulation taint must be observable before deleting the Proxy pod'
    Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', $pressureProxyPod.metadata.name, '--wait=false') | Out-Null
    Wait-Workloads
    $proxyPodsAfterPressure = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'rocketmq.apache.org/service=proxy', '-o', 'json')).Output | ConvertFrom-Json).items
    $replacementProxyPod = $proxyPodsAfterPressure |
        Where-Object {
            $proxyUidsBeforePressure -notcontains $_.metadata.uid -and
            $_.spec.nodeName -ne $pressureNode -and
            $null -eq $_.metadata.deletionTimestamp -and
            @($_.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -eq 1
        } |
        Select-Object -First 1
    $podPlacement = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'rocketmq.apache.org/service=proxy', '-o', 'wide')).Output
    $afterPressure = Query-AcknowledgedMessage $ack.Id
    $taintCleanup = Invoke-Native kubectl @(
        'taint',
        'node',
        $pressureNode,
        'node.kubernetes.io/disk-pressure:NoSchedule-'
    ) -AllowFailure
    Assert-True (
        $taintCleanup.ExitCode -eq 0 -or $taintCleanup.Output -match 'not found'
    ) 'disk-pressure taint cleanup must either remove the taint or observe that kubelet already removed it'
    $simulationTaintCleanup = Invoke-Native kubectl @(
        'taint',
        'node',
        $pressureNode,
        'rocketmq.apache.org/simulated-disk-pressure:NoSchedule-'
    ) -AllowFailure
    Assert-True (
        $simulationTaintCleanup.ExitCode -eq 0 -or $simulationTaintCleanup.Output -match 'not found'
    ) 'simulated disk-pressure taint cleanup must be idempotent'
    $pressureStatus = (Invoke-Native kubectl @('get', 'node', $pressureNode, '-o', 'json')).Output
    Complete-Scenario 'disk_pressure' ([ordered]@{
        disk_pressure_taint_observed = $taint.ExitCode -eq 0 -and $simulationTaint.ExitCode -eq 0 -and $pressureStatusDuring -match 'rocketmq.apache.org/simulated-disk-pressure'
        stateless_pod_rescheduled = $null -ne $replacementProxyPod
        acknowledged_message_visible = $true; pvc_uid_set_preserved = $InitialPvcUids -eq (Get-PvcUidSet); taint_removed = $pressureStatus -notmatch 'node.kubernetes.io/disk-pressure' -and $pressureStatus -notmatch 'rocketmq.apache.org/simulated-disk-pressure'
    }) ([ordered]@{
        taint_status = "$($taint.Output)`n$($simulationTaint.Output)`n$pressureStatusDuring"
        pod_reschedule = "deletedUid=$pressureProxyUid replacementUid=$($replacementProxyPod.metadata.uid)`n$podPlacement"
        message_after = $afterPressure.Output
        pvc_uids = Get-PvcUidSet
        node_status = "$($taintCleanup.Output)`n$($simulationTaintCleanup.Output)`n$pressureStatus"
    })

    $diskMaster = (Wait-LiveSingleMaster -Namespace $Namespace).Master
    $diskBefore = Invoke-BrokerShell -Pod $diskMaster.Pod -Script 'df -Pk /var/lib/rocketmq'
    $diskFault = $null
    $diskWriteProbe = $null
    try {
        $diskFault = Start-LiveBrokerDiskFull `
            -Namespace $Namespace `
            -Pod $diskMaster.Pod `
            -RunToken $LiveFaultToken `
            -MaximumFillMiB 2048
        $diskWriteTimer = [Diagnostics.Stopwatch]::StartNew()
        $diskWriteProbe = Invoke-BrokerShell `
            -Pod $diskMaster.Pod `
            -Script "dd if=/dev/zero of='/var/lib/rocketmq/.live-enospc-probe-$LiveFaultToken' bs=1048576 count=4 conv=fsync" `
            -AllowFailure
        $diskWriteTimer.Stop()
        $diskFullMessage = Query-AcknowledgedMessage $ack.Id
    } finally {
        $diskCleanup = Clear-LiveBrokerDiskFull -Namespace $Namespace -Pod $diskMaster.Pod -RunToken $LiveFaultToken
        Invoke-BrokerShell `
            -Pod $diskMaster.Pod `
            -Script "rm -f '/var/lib/rocketmq/.live-enospc-probe-$LiveFaultToken'; sync" `
            -AllowFailure | Out-Null
    }
    $diskRecoveryProbe = Invoke-FaultDriver -SecretName 'rocketmq-fault-driver-baseline' -Arguments @(
        'message', 'sendMessage', '-t', $Topic, '-p', 'disk-recovered', '-k', "disk-recovered-$LiveFaultToken"
    )
    $diskAfter = Invoke-BrokerShell -Pod $diskMaster.Pod -Script 'df -Pk /var/lib/rocketmq'
    Complete-Scenario 'disk_full' ([ordered]@{
        disk_full_state_injected = $diskFault.Kind -eq 'broker-pvc-disk-full' -and $diskFault.RemainingMiB -le 18
        write_failure_bounded = $diskWriteProbe.ExitCode -ne 0 -and $diskWriteTimer.Elapsed.TotalSeconds -lt 30
        acknowledged_message_visible = $diskFullMessage.QueueOffset -eq $before.QueueOffset
        disk_state_cleared = $diskCleanup.ExitCode -eq 0 -and $diskAfter.Output -notmatch [regex]::Escape(".live-disk-full-$LiveFaultToken")
        broker_recovered = $diskRecoveryProbe.ExitCode -eq 0
    }) ([ordered]@{
        disk_before = $diskBefore.Output
        fault_injection = $diskFault.Output
        write_probe = "seconds=$($diskWriteTimer.Elapsed.TotalSeconds) exit=$($diskWriteProbe.ExitCode)`n$($diskWriteProbe.Output)"
        message_after = $diskFullMessage.Output
        disk_after = "$($diskCleanup.Output)`n$($diskRecoveryProbe.Output)`n$($diskAfter.Output)"
    })

    $latencyBeforeTimer = [Diagnostics.Stopwatch]::StartNew()
    $latencyBeforeMessage = Query-AcknowledgedMessage $ack.Id
    $latencyBeforeTimer.Stop()
    $contentionStartScript = 'i=0; : > /tmp/rocketmq/phase06-fsync.pids; while [ "$i" -lt 4 ]; do dd if=/dev/zero of="/var/lib/rocketmq/.phase06-fsync-$i" bs=1048576 count=64 conv=fsync >/tmp/rocketmq/phase06-fsync-$i.log 2>&1 & echo "$!" >>/tmp/rocketmq/phase06-fsync.pids; i=$((i + 1)); done; echo "started=4"'
    $contentionStart = Invoke-BrokerShell $contentionStartScript
    $latencyDuringTimer = [Diagnostics.Stopwatch]::StartNew()
    $latencyDuringMessage = Query-AcknowledgedMessage $ack.Id
    $latencyDuringTimer.Stop()
    $contentionCleanupScript = 'attempt=0; active=1; while [ "$attempt" -lt 120 ]; do active=0; for pid in $(cat /tmp/rocketmq/phase06-fsync.pids 2>/dev/null); do state=$(ps -o stat= -p "$pid" 2>/dev/null | tr -d " "); case "$state" in ""|Z*) ;; *) active=1 ;; esac; done; [ "$active" -eq 0 ] && break; sleep 1; attempt=$((attempt + 1)); done; for pid in $(cat /tmp/rocketmq/phase06-fsync.pids 2>/dev/null); do kill "$pid" 2>/dev/null || true; done; rm -f /var/lib/rocketmq/.phase06-fsync-* /tmp/rocketmq/phase06-fsync-*.log /tmp/rocketmq/phase06-fsync.pids; sync; echo "active=$active attempts=$attempt"'
    $contentionCleanup = Invoke-BrokerShell $contentionCleanupScript
    $brokerReadyAfterContention = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pod', 'rocketmq-broker-0', '-o', 'jsonpath={.status.containerStatuses[0].ready}')).Output
    Complete-Scenario 'slow_disk_fsync_jitter' ([ordered]@{
        fsync_jitter_observed = $contentionStart.Output -match 'started=4'
        request_latency_bounded = $latencyDuringTimer.Elapsed.TotalSeconds -lt 120
        acknowledged_message_visible = $latencyDuringMessage.QueueOffset -eq $before.QueueOffset
        contention_stopped = $contentionCleanup.ExitCode -eq 0 -and $contentionCleanup.Output -match 'active=0'
        broker_recovered = $brokerReadyAfterContention -eq 'true'
    }) ([ordered]@{
        latency_before = "seconds=$($latencyBeforeTimer.Elapsed.TotalSeconds)`n$($latencyBeforeMessage.Output)"
        contention_status = $contentionStart.Output
        latency_during = "seconds=$($latencyDuringTimer.Elapsed.TotalSeconds)`n$($latencyDuringMessage.Output)"
        message_after = $latencyDuringMessage.Output
        cleanup_status = $contentionCleanup.Output
    })

    $failureLeaderBefore = Wait-ControllerLeadershipStable
    $failureLeaderOrdinal = [int]$failureLeaderBefore.Leaders[0]
    $failureLeaderPod = "rocketmq-controller-$failureLeaderOrdinal"
    $failureLeaderState = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pod', $failureLeaderPod, '-o', 'json')).Output | ConvertFrom-Json
    $failureLeaderNode = $failureLeaderState.spec.nodeName
    $failureSurvivingOrdinals = @(@(0, 1, 2) | Where-Object { $_ -ne $failureLeaderOrdinal })
    $failureCordon = Invoke-Native kubectl @('cordon', $failureLeaderNode)
    try {
        $failureDelete = Invoke-Native kubectl @(
            '-n',
            $Namespace,
            'delete',
            'pod',
            $failureLeaderPod,
            '--wait=true',
            '--timeout=120s'
        )
        $leaderAfter = Wait-ControllerLeadershipStable -Ordinals $failureSurvivingOrdinals
        $leaderAfterOrdinal = [int]$leaderAfter.Leaders[0]
    } finally {
        $failureUncordon = Invoke-Native kubectl @('uncordon', $failureLeaderNode) -AllowFailure
    }
    $null = Wait-ControllerPodRecreatedAndReady -Ordinal $failureLeaderOrdinal -PreviousUid $failureLeaderState.metadata.uid
    $leadershipAfterFailure = Wait-ControllerLeadershipStable
    $controllerStatus = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'rocketmq.apache.org/service=controller', '-o', 'wide')).Output
    $controllerState = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'statefulset/rocketmq-controller', '-o', 'json')).Output | ConvertFrom-Json).status
    $afterLeader = Query-AcknowledgedMessage $ack.Id
    Complete-Scenario 'controller_leader_failure' ([ordered]@{
        leader_changed = $leaderAfterOrdinal -ne $failureLeaderOrdinal
        single_leader_observed = $leadershipAfterFailure.Leaders.Count -eq 1
        controller_quorum_preserved = $leaderAfter.Responders -eq 2
        acknowledged_message_visible = $afterLeader.QueueOffset -eq $before.QueueOffset
        controller_replicas_restored = $controllerState.readyReplicas -eq 3 -and $failureUncordon.ExitCode -eq 0
    }) ([ordered]@{
        leader_before = $failureLeaderBefore.Output
        leader_after = $leaderAfter.Output
        quorum_status = "$($failureCordon.Output)`n$($failureDelete.Output)`n$($failureUncordon.Output)"
        message_after = $afterLeader.Output
        controller_status = "$controllerStatus`n$($leadershipAfterFailure.Output)"
    })

    $quorumLossTimer = [Diagnostics.Stopwatch]::StartNew()
    $quorumLossScale = $null
    $quorumLossProbe = $null
    $quorumLossLeadership = $null
    $quorumLossMessage = $null
    try {
        $quorumLossScale = Set-StatefulSetReplicas 'rocketmq-controller' 1
        $quorumLossProbe = Invoke-FaultDriver -SecretName 'rocketmq-fault-driver-baseline' -Arguments @(
            'controller',
            'getControllerMetaData',
            '-a',
            "rocketmq-controller-0.rocketmq-controller-headless.$Namespace.svc.cluster.local:60109"
        ) -AllowFailure
        $quorumLossLeadership = Get-ControllerLeadershipSnapshot
        $quorumLossMessage = Query-AcknowledgedMessage $ack.Id
    } finally {
        $quorumRestore = Set-StatefulSetReplicas 'rocketmq-controller' 3
    }
    try {
        $restoredLeadership = Wait-ControllerLeadershipStable
    } finally {
        $quorumLossTimer.Stop()
    }
    Complete-Scenario 'controller_quorum_loss' ([ordered]@{
        quorum_loss_observed = $quorumLossScale.State.status.readyReplicas -eq 1
        control_plane_failed_closed = $quorumLossProbe.ExitCode -ne 0 -or $quorumLossLeadership.Responders -le 1
        duplicate_leadership_absent = $quorumLossLeadership.Leaders.Count -le 1 -and $restoredLeadership.Leaders.Count -eq 1
        acknowledged_message_visible = $quorumLossMessage.QueueOffset -eq $before.QueueOffset
        controller_quorum_restored = $restoredLeadership.Responders -eq 3
    }) ([ordered]@{
        quorum_loss_status = $quorumLossScale.Output
        control_plane_probe = "exit=$($quorumLossProbe.ExitCode)`n$($quorumLossProbe.Output)"
        leadership_probe = "$($quorumLossLeadership.Output)`n--- restored ---`n$($restoredLeadership.Output)"
        message_after = $quorumLossMessage.Output
        restore_status = "$($quorumRestore.Output) seconds=$($quorumLossTimer.Elapsed.TotalSeconds)"
    })

    $networkNode = (Invoke-Native kubectl @(
        '-n',
        $Namespace,
        'get',
        'pod',
        'rocketmq-broker-0',
        '-o',
        'jsonpath={.spec.nodeName}'
    )).Output
    $networkBefore = (Invoke-Native docker @('exec', $networkNode, 'tc', 'qdisc', 'show', 'dev', 'eth0')).Output
    $networkFault = $null
    $impairedMessage = $null
    $halfOpenProbe = $null
    $halfOpenTimer = [Diagnostics.Stopwatch]::StartNew()
    $networkCordon = Invoke-Native kubectl @('cordon', $networkNode)
    try {
        $networkFault = Set-NodeNetworkImpairment $networkNode @('delay', '200ms', '50ms', 'loss', '10%')
        $impairedMessage = Query-AcknowledgedMessage $ack.Id
        $null = Set-NodeNetworkImpairment $networkNode @('loss', '100%')
        $halfOpenProbe = Invoke-FaultDriver -SecretName 'rocketmq-fault-driver-baseline' -Arguments @(
            'message',
            'queryMsgByUniqueKey',
            '-t',
            $Topic,
            '-i',
            $ack.Id
        ) -AllowFailure
    } finally {
        $networkCleanup = Clear-NodeNetworkImpairment $networkNode
        $networkReady = Invoke-Native kubectl @('wait', "node/$networkNode", '--for=condition=Ready', '--timeout=60s') -AllowFailure
        $networkUncordon = Invoke-Native kubectl @('uncordon', $networkNode) -AllowFailure
        $halfOpenTimer.Stop()
    }
    $networkAfter = (Invoke-Native docker @('exec', $networkNode, 'tc', 'qdisc', 'show', 'dev', 'eth0')).Output
    $networkRecoveredMessage = Query-AcknowledgedMessage $ack.Id
    Complete-Scenario 'network_impairment' ([ordered]@{
        latency_injected = $networkFault.Output -match 'delay 200ms'
        packet_loss_injected = $networkFault.Output -match 'loss 10%'
        half_open_bounded = $halfOpenTimer.Elapsed.TotalSeconds -lt 120
        acknowledged_message_visible = $networkRecoveredMessage.QueueOffset -eq $before.QueueOffset
        network_faults_removed = $networkAfter -notmatch 'netem' -and $networkReady.ExitCode -eq 0 -and $networkUncordon.ExitCode -eq 0
    }) ([ordered]@{
        network_before = $networkBefore
        tc_state = "$($networkFault.Output)`nmessageDuring=$($impairedMessage.Output)"
        half_open_probe = "exit=$($halfOpenProbe.ExitCode) seconds=$($halfOpenTimer.Elapsed.TotalSeconds)`n$($halfOpenProbe.Output)"
        message_after = $networkRecoveredMessage.Output
        network_after = "$($networkCordon.Output)`n$($networkCleanup.Output)`n$($networkReady.Output)`n$($networkUncordon.Output)`n$networkAfter"
    })

    $haBefore = Wait-LiveSingleMaster -Namespace $Namespace
    $haOldMaster = $haBefore.Master
    $haTargetSlave = $haBefore.Snapshot.Records |
        Where-Object { $_.Role -eq 'Slave' -and $_.Ready -and $_.Pod -ne $haOldMaster.Pod } |
        Select-Object -First 1
    Assert-True ($null -ne $haTargetSlave) 'live HA fault requires a Ready slave'
    $haRuleTag = "ha-$($LiveFaultToken.Substring([math]::Max(0, $LiveFaultToken.Length - 12)))"
    $haFault = $null
    $haCordon = $null
    $haDelete = $null
    $haPromotion = $null
    $haTimer = [Diagnostics.Stopwatch]::new()
    try {
        $haFault = Set-LivePodPortImpairment `
            -Node $haOldMaster.Node `
            -PodIp $haTargetSlave.PodIp `
            -Port 10912 `
            -DelayMilliseconds 0 `
            -LossPercent 100 `
            -RuleTag $haRuleTag
        Start-Sleep -Seconds 5
        $haLagProbe = Invoke-FaultDriver -SecretName 'rocketmq-fault-driver-baseline' -Arguments @(
            'message', 'sendMessage', '-t', $Topic, '-p', 'ha-lag-window', '-k', "ha-lag-$LiveFaultToken"
        ) -AllowFailure
        $haCordon = Invoke-Native kubectl @('cordon', $haOldMaster.Node)
        $haTimer.Start()
        $haDelete = Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', $haOldMaster.Pod, '--wait=false')
        $haPromotion = Wait-LiveSingleMaster -Namespace $Namespace -ExcludedPod $haOldMaster.Pod -TimeoutSeconds 180
        $haMessage = Query-AcknowledgedMessage $ack.Id
        $haTimer.Stop()
    } finally {
        if ($haTimer.IsRunning) { $haTimer.Stop() }
        if ($null -ne $haFault) {
            $haNetworkCleanup = Clear-LivePodPortImpairment -Node $haOldMaster.Node -RuleTag $haRuleTag
        }
        if ($null -ne $haCordon) {
            $haUncordon = Invoke-Native kubectl @('uncordon', $haOldMaster.Node) -AllowFailure
        }
    }
    $null = Wait-PodRecreatedAndReady -Pod $haOldMaster.Pod -PreviousUid $haOldMaster.Uid -TimeoutSeconds 180
    $haRestored = Wait-LiveSingleMaster -Namespace $Namespace
    Complete-Scenario 'ha_replication_lag' ([ordered]@{
        replication_lag_observed = $haFault.After -match '\bnetem\b' -and $haFault.Port -eq 10912
        single_master_preserved = $haRestored.Snapshot.Masters.Count -eq 1
        promotion_completed = $haPromotion.Master.Pod -ne $haOldMaster.Pod
        acknowledged_message_visible = $haMessage.QueueOffset -eq $before.QueueOffset
        rpo_satisfied = $haMessage.QueueOffset -eq $before.QueueOffset
        rto_satisfied = $haTimer.Elapsed.TotalSeconds -lt 180
    }) ([ordered]@{
        replication_before = $haBefore.Snapshot.Output
        lag_injection = "$($haFault.After)`nwrite_probe_exit=$($haLagProbe.ExitCode)`n$($haLagProbe.Output)"
        promotion_status = "$($haCordon.Output)`n$($haDelete.Output)`n$($haPromotion.Snapshot.Output)`n--- restored ---`n$($haRestored.Snapshot.Output)"
        message_after = $haMessage.Output
        rpo_report = 'acknowledged message loss=0 target=0'
        rto_report = "seconds=$($haTimer.Elapsed.TotalSeconds) target=180 live_injection=true"
    })

    $snapshotLeadershipBefore = Wait-ControllerLeadershipStable
    $snapshotLeaderOrdinal = [int]$snapshotLeadershipBefore.Leaders[0]
    $snapshotFollower = @(0, 1, 2) | Where-Object { $_ -ne $snapshotLeaderOrdinal } | Select-Object -First 1
    $snapshotLeaderPod = "rocketmq-controller-$snapshotLeaderOrdinal"
    $snapshotFollowerPod = "rocketmq-controller-$snapshotFollower"
    $snapshotLeaderState = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pod', $snapshotLeaderPod, '-o', 'json')).Output | ConvertFrom-Json
    $snapshotFollowerState = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pod', $snapshotFollowerPod, '-o', 'json')).Output | ConvertFrom-Json
    $snapshotBrokerMaster = (Wait-LiveSingleMaster -Namespace $Namespace).Master
    Assert-True ($snapshotBrokerMaster.ControllerId -ge 0) 'snapshot write burst requires the elected Broker controller id'
    $snapshotRuleTag = "snap-$($LiveFaultToken.Substring([math]::Max(0, $LiveFaultToken.Length - 10)))"
    $snapshotFault = $null
    $snapshotInstallSince = [DateTimeOffset]::UtcNow.ToString('o')
    try {
        $snapshotFault = Set-LivePodPortImpairment `
            -Node $snapshotLeaderState.spec.nodeName `
            -PodIp $snapshotFollowerState.status.podIP `
            -Port 60110 `
            -DelayMilliseconds 0 `
            -LossPercent 100 `
            -RuleTag $snapshotRuleTag
        $snapshotWrites = Invoke-LiveControllerWriteBurst `
            -ControllerAddress "$snapshotLeaderPod.rocketmq-controller-headless.$Namespace.svc.cluster.local:60109" `
            -BrokerControllerId $snapshotBrokerMaster.ControllerId `
            -Count 64
        $snapshotLeaderObservation = Get-LiveSnapshotObservation -Namespace $Namespace -Pod $snapshotLeaderPod
    } finally {
        if ($null -ne $snapshotFault) {
            $snapshotNetworkCleanup = Clear-LivePodPortImpairment `
                -Node $snapshotLeaderState.spec.nodeName `
                -RuleTag $snapshotRuleTag
        }
    }
    $snapshotFollowerDuring = Wait-LiveSnapshotInstall `
        -Namespace $Namespace `
        -Pod $snapshotFollowerPod `
        -SinceTime $snapshotInstallSince `
        -TimeoutSeconds 120
    $snapshotDelete = Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', $snapshotFollowerPod, '--wait=false')
    $null = Wait-ControllerPodRecreatedAndReady `
        -Ordinal $snapshotFollower `
        -PreviousUid $snapshotFollowerState.metadata.uid `
        -TimeoutSeconds 180
    $snapshotAfter = Get-LiveSnapshotObservation -Namespace $Namespace -Pod $snapshotFollowerPod
    $null = Wait-ControllerReplicationCaughtUp -TimeoutSeconds 180
    $snapshotLeadership = Get-ControllerLeadershipSnapshot
    Complete-Scenario 'snapshot_install_interruption' ([ordered]@{
        snapshot_install_started = $snapshotLeaderObservation.SnapshotObserved -and ($snapshotFollowerDuring.InstallObserved -or $snapshotAfter.InstallObserved)
        install_interrupted = $snapshotDelete.ExitCode -eq 0
        partial_snapshot_not_published = $snapshotLeadership.Responders -eq 3
        follower_caught_up = $snapshotLeadership.Responders -eq 3
        single_leader_observed = $snapshotLeadership.Leaders.Count -eq 1
    }) ([ordered]@{
        snapshot_before = "$($snapshotLeadershipBefore.Output)`n--- leader ---`n$($snapshotLeaderObservation.Logs)`n$($snapshotLeaderObservation.Files)"
        interruption_status = "$($snapshotFault.After)`n$($snapshotWrites.Output)`n$($snapshotNetworkCleanup.Output)`n$($snapshotDelete.Output)"
        snapshot_integrity = "during_install=$($snapshotFollowerDuring.InstallObserved) after_install=$($snapshotAfter.InstallObserved)"
        snapshot_after = "$($snapshotAfter.Logs)`n$($snapshotAfter.Files)"
        leadership_status = $snapshotLeadership.Output
    })

    $proxyPods = @(((Invoke-Native kubectl @(
        '-n', $Namespace, 'get', 'pods', '-l', 'rocketmq.apache.org/service=proxy', '-o', 'json'
    )).Output | ConvertFrom-Json).items)
    Assert-True ($proxyPods.Count -eq 2) 'live Proxy overload requires both Proxy replicas'
    $proxyMaster = (Wait-LiveSingleMaster -Namespace $Namespace).Master
    $proxyBeforeMetrics = [System.Collections.Generic.List[string]]::new()
    $proxyDuringMetrics = [System.Collections.Generic.List[string]]::new()
    $proxyAfterMetrics = [System.Collections.Generic.List[string]]::new()
    $proxyFaultNodes = @($proxyPods | ForEach-Object { [string]$_.spec.nodeName } | Sort-Object -Unique)
    $proxyRules = [System.Collections.Generic.List[object]]::new()
    foreach ($pod in $proxyPods) {
        $proxyBeforeMetrics.Add((Get-LivePodMetrics -Namespace $Namespace -Pod $pod.metadata.name).Output)
    }
    try {
        $proxyRuleOrdinal = 0
        foreach ($node in $proxyFaultNodes) {
            $proxyRuleOrdinal++
            $ruleTag = "proxy-$proxyRuleOrdinal-$($LiveFaultToken.Substring([math]::Max(0, $LiveFaultToken.Length - 8)))"
            $fault = Set-LivePodPortImpairment `
                -Node $node `
                -PodIp $proxyMaster.PodIp `
                -Port 10911 `
                -DelayMilliseconds 2500 `
                -LossPercent 30 `
                -RuleTag $ruleTag
            $proxyRules.Add([pscustomobject]@{ Node = $node; RuleTag = $ruleTag; Fault = $fault })
        }
        $proxyLoad = Invoke-LiveProxyMixedLoad -LongPollers 300 -OrderedSends 8
        foreach ($pod in $proxyPods) {
            $proxyDuringMetrics.Add((Get-LivePodMetrics -Namespace $Namespace -Pod $pod.metadata.name).Output)
        }
    } finally {
        foreach ($rule in $proxyRules) {
            Clear-LivePodPortImpairment -Node $rule.Node -RuleTag $rule.RuleTag | Out-Null
        }
    }
    foreach ($pod in $proxyPods) {
        $proxyAfterMetrics.Add((Get-LivePodMetrics -Namespace $Namespace -Pod $pod.metadata.name).Output)
    }
    $proxyRecovery = Invoke-FaultDriver `
        -SecretName 'rocketmq-fault-driver-baseline' `
        -Endpoint "rocketmq-proxy.$Namespace.svc.cluster.local:8080" `
        -Arguments @('message', 'sendMessage', '-t', $Topic, '-p', 'proxy-recovered', '-k', "proxy-recovered-$LiveFaultToken")
    $proxyPodsAfter = @(((Invoke-Native kubectl @(
        '-n', $Namespace, 'get', 'pods', '-l', 'rocketmq.apache.org/service=proxy', '-o', 'json'
    )).Output | ConvertFrom-Json).items)
    $sendSequences = @([regex]::Matches($proxyLoad.Output, 'send-sequence=(\d+)') | ForEach-Object {
        [int]$_.Groups[1].Value
    })
    $expectedSendSequences = @(0..7)
    $typedProxyOutcome = $proxyLoad.Output -match 'receive-overload code=(ResourceExhausted|Unavailable|DeadlineExceeded)'
    Complete-Scenario 'proxy_slow_broker_overload' ([ordered]@{
        long_poll_did_not_block_send = $proxyLoad.ExitCode -eq 0 -and $sendSequences.Count -eq 8
        ordering_preserved = (($sendSequences -join ',') -eq ($expectedSendSequences -join ','))
        admission_limits_enforced = $proxyDuringMetrics.Count -eq 2 -and $typedProxyOutcome
        typed_overload_observed = $typedProxyOutcome
        leaked_zero = $proxyRecovery.ExitCode -eq 0 -and $proxyAfterMetrics.Count -eq 2
        detached_zero = @($proxyPodsAfter | Where-Object { [int]$_.status.containerStatuses[0].restartCount -ne 0 }).Count -eq 0
    }) ([ordered]@{
        long_poll_probe = $proxyLoad.Output
        send_progress = "$($sendSequences -join ',')`n$($proxyRecovery.Output)"
        ordering_report = "expected=$($expectedSendSequences -join ',') actual=$($sendSequences -join ',')"
        overload_report = "$($proxyRules.Fault.After -join "`n")`ntyped=$typedProxyOutcome"
        budget_snapshot = "before=$($proxyBeforeMetrics -join "`n---`n")`nduring=$($proxyDuringMetrics -join "`n---`n")`nafter=$($proxyAfterMetrics -join "`n---`n")"
        shutdown_report = ($proxyPodsAfter | ConvertTo-Json -Depth 10)
    })

    $preRotation = Query-AcknowledgedMessage $ack.Id
    $brokerUidsBeforeRotation = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'rocketmq.apache.org/service=broker', '-o', 'json')).Output | ConvertFrom-Json).items |
        ForEach-Object { "$($_.metadata.name)=$($_.metadata.uid)" } |
        Sort-Object
    $rotationResult = $null
    $rollbackResult = $null
    $rotationFailure = $null
    $rollbackFailure = $null
    try {
        Invoke-Native kubectl @('-n', $Namespace, 'apply', '-f', $RotatedRuntimeSecretManifest) | Out-Null
        try {
            $rotationResult = Wait-CredentialCutover `
                -MessageId $ack.Id `
                -AllowedSecretName 'rocketmq-fault-driver-rotated' `
                -DeniedSecretName 'rocketmq-fault-driver-baseline'
        } catch {
            $rotationFailure = $_
        }
    } finally {
        Invoke-Native kubectl @('-n', $Namespace, 'apply', '-f', $RuntimeSecretManifest) | Out-Null
        try {
            $rollbackResult = Wait-CredentialCutover `
                -MessageId $ack.Id `
                -AllowedSecretName 'rocketmq-fault-driver-baseline' `
                -DeniedSecretName 'rocketmq-fault-driver-rotated'
        } catch {
            $rollbackFailure = $_
        }
    }
    if ($null -ne $rollbackFailure) { throw $rollbackFailure }
    if ($null -ne $rotationFailure) { throw $rotationFailure }
    $newAllowed = Convert-MessageQueryEvidence $rotationResult.Allowed
    $restored = Convert-MessageQueryEvidence $rollbackResult.Allowed
    $brokerUidsAfterRotation = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'rocketmq.apache.org/service=broker', '-o', 'json')).Output | ConvertFrom-Json).items |
        ForEach-Object { "$($_.metadata.name)=$($_.metadata.uid)" } |
        Sort-Object
    $brokerPodsUnchanged = ($brokerUidsBeforeRotation -join "`n") -eq ($brokerUidsAfterRotation -join "`n")
    Assert-True $brokerPodsUnchanged 'credential rotation must converge through hot reload without restarting Broker pods'
    $redactionText = "$($preRotation.Output)`n$($newAllowed.Output)`n$($restored.Output)"
    Complete-Scenario 'secret_rotation' ([ordered]@{
        old_credentials_worked_before_rotation = $preRotation.QueueOffset -eq $before.QueueOffset
        old_credentials_rejected_after_rotation = -not $rotationResult.DeniedSucceeded
        new_credentials_worked_after_rotation = $newAllowed.QueueOffset -eq $before.QueueOffset
        baseline_credentials_restored = $restored.QueueOffset -eq $before.QueueOffset
        secret_values_redacted = $redactionText -notmatch '(?i)secret[_-]?key\s*[=:]\s*\S+'
    }) ([ordered]@{
        pre_rotation_access = $preRotation.Output
        old_access_denied = "job_exit=$($rotationResult.Denied.ExitCode) semantic_query=$($rotationResult.DeniedSucceeded)"
        new_access_allowed = $newAllowed.Output
        rollback_access = $restored.Output
        redaction_scan = "no secret value pattern present; broker_pods_unchanged=$brokerPodsUnchanged"
    })

    $pvcBeforeRestart = Get-PvcUidSet
    $brokerStateBeforeRestart = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pod', 'rocketmq-broker-0', '-o', 'json')).Output | ConvertFrom-Json
    $brokerRestart = Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', 'rocketmq-broker-0', '--wait=false')
    $null = Wait-PodRecreatedAndReady -Pod 'rocketmq-broker-0' -PreviousUid $brokerStateBeforeRestart.metadata.uid
    $afterRestart = Query-AcknowledgedMessage $ack.Id
    $pvcAfterRestart = Get-PvcUidSet
    Complete-Scenario 'acknowledged_message_recovery' ([ordered]@{
        send_acknowledged = $true; message_visible_before_restart = $true; message_visible_after_restart = $true
        message_id_preserved = $afterRestart.Output -match [regex]::Escape($ack.Id)
        queue_offset_preserved = $before.QueueOffset -eq $afterRestart.QueueOffset
        commitlog_offset_preserved = $before.CommitLogOffset -eq $afterRestart.CommitLogOffset
        pvc_uid_set_preserved = $pvcBeforeRestart -eq $pvcAfterRestart
    }) ([ordered]@{ send_ack = $ack.Output; message_before = $before.Output; broker_restart = $brokerRestart.Output; message_after = $afterRestart.Output; watermark = "queue=$($afterRestart.QueueOffset) commitlog=$($afterRestart.CommitLogOffset)"; pvc_uids = $pvcAfterRestart })

    $actualScenarioOrder = (($ScenarioRecords | ForEach-Object { $_.id }) -join ',')
    $expectedScenarioOrder = (($Policy.scenarios | ForEach-Object { $_.id }) -join ',')
    Assert-True ($actualScenarioOrder -eq $expectedScenarioOrder) 'all required scenarios must execute in policy order'
    Wait-Workloads
    $FinalPvcUids = Get-PvcUidSet
    $finalController = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'statefulset/rocketmq-controller', '-o', 'json')).Output | ConvertFrom-Json
    $finalPods = @(
        ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'app.kubernetes.io/part-of=rocketmq-rust', '-o', 'json')).Output | ConvertFrom-Json).items |
            Where-Object { $null -eq $_.metadata.deletionTimestamp }
    )
    $readyFinalPods = @(
        $finalPods | Where-Object {
            @($_.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -eq 1
        }
    )
    $allPodsReady = $finalPods.Count -eq 12 -and $readyFinalPods.Count -eq 12
    $finalNodes = ((Invoke-Native kubectl @('get', 'nodes', '-o', 'json')).Output | ConvertFrom-Json).items
    $diskPressureTaintKeys = @(
        'node.kubernetes.io/disk-pressure',
        'rocketmq.apache.org/simulated-disk-pressure'
    )
    $nodesClean = @(
        $finalNodes | Where-Object {
            $_.spec.unschedulable -eq $true -or
                ($_.spec.taints | Where-Object { $_.key -in $diskPressureTaintKeys })
        }
    ).Count -eq 0
    $liveFaultCleanup = Assert-LiveFaultCleanup `
        -Namespace $Namespace `
        -RunToken $LiveFaultToken `
        -Nodes @($finalNodes | ForEach-Object { [string]$_.metadata.name })
    $collectorReady = ((Invoke-Native kubectl @('-n', 'observability', 'get', 'deployment/otel-collector', '-o', 'json')).Output | ConvertFrom-Json).status.readyReplicas -eq 1
    $baselineImagesRestored = $true
    foreach ($service in @('broker', 'namesrv', 'controller')) {
        $actualImage = (Invoke-Native kubectl @('-n', $Namespace, 'get', "statefulset/rocketmq-$service", '-o', "jsonpath={.spec.template.spec.containers[?(@.name=='$service')].image}")).Output
        $baselineImagesRestored = $baselineImagesRestored -and $actualImage -eq $BaselineImages[$service]
    }
    foreach ($service in @('proxy', 'mcp')) {
        $actualImage = (Invoke-Native kubectl @('-n', $Namespace, 'get', "deployment/rocketmq-$service", '-o', "jsonpath={.spec.template.spec.containers[?(@.name=='$service')].image}")).Output
        $baselineImagesRestored = $baselineImagesRestored -and $actualImage -eq $BaselineImages[$service]
    }
    $unresolvedFaults = [System.Collections.Generic.List[string]]::new()
    if (-not $nodesClean) { $unresolvedFaults.Add('node-cordon-or-disk-pressure-taint') }
    if (-not $collectorReady) { $unresolvedFaults.Add('collector-not-restored') }
    if (-not $baselineImagesRestored) { $unresolvedFaults.Add('baseline-images-not-restored') }
    if ($finalController.status.readyReplicas -ne 3) { $unresolvedFaults.Add('controller-quorum-not-restored') }
    if ($liveFaultCleanup.ExitCode -ne 0) { $unresolvedFaults.Add('live-fault-residue') }
    $clusterProfile = [ordered]@{
        control_plane_nodes = 1; worker_nodes = 3; broker_replicas = 3; controller_replicas = 3; storage_class = $StorageClass
        nodes = @($nodes | ForEach-Object { $_.metadata.name })
    }
    $toolVersions = [ordered]@{
        docker = $dockerInfo.Output
        kind = if (Get-Command kind -ErrorAction SilentlyContinue) { (Invoke-Native kind @('version')).Output } else { 'not-installed' }
        k3d = if (Get-Command k3d -ErrorAction SilentlyContinue) { (Invoke-Native k3d @('version')).Output } else { 'not-installed' }
        kubectl = (Invoke-Native kubectl @('version', '--client', '--output=json')).Output
        helm = (Invoke-Native helm @('version', '--short')).Output
    }
    $globalAssertions = [ordered]@{
        all_workloads_ready = $allPodsReady; all_faults_reverted = $unresolvedFaults.Count -eq 0
        controller_quorum_restored = $finalController.status.readyReplicas -eq 3
        pvc_uid_set_preserved = $InitialPvcUids -eq $FinalPvcUids
        acknowledged_message_recovered = $afterRestart.QueueOffset -eq $before.QueueOffset
        queue_offset_preserved = $afterRestart.QueueOffset -eq $before.QueueOffset
        commitlog_offset_preserved = $afterRestart.CommitLogOffset -eq $before.CommitLogOffset
        drain_completed_within_deadline = [string]::IsNullOrWhiteSpace($preStopFailures)
        slo_budget_satisfied = $outageStart.Elapsed.TotalSeconds -lt 30
        rollback_verified = $baselineImagesRestored
        unresolved_faults_empty = $unresolvedFaults.Count -eq 0
    }
    foreach ($assertion in $globalAssertions.Keys) { Assert-True $globalAssertions[$assertion] "global.$assertion" }
    $run = [ordered]@{
        schema_version = 1; milestone = 'M11-11'; policy_sha256 = $PolicySha256; run_id = $RunId
        candidate_commit = $CandidateCommit
        started_at = $RunStarted.ToString('o'); finished_at = [DateTimeOffset]::UtcNow.ToString('o'); backend = $Backend
        dynamic_execution = $true; fixture = $false; cluster_profile = $clusterProfile; tool_versions = $toolVersions
        chart_sha256 = Get-TreeSha256 $ChartPath; overlay_sha256 = Get-Sha256 $OverlayPath
        baseline_images = $BaselineImages; candidate_images = $CandidateImages; global_assertions = $globalAssertions
        unresolved_faults = @($unresolvedFaults); scenarios = @($ScenarioRecords); artifacts = @($ArtifactRecords)
    }
    $runPath = Join-Path $RunDirectory 'run.json'
    [IO.File]::WriteAllText($runPath, ($run | ConvertTo-Json -Depth 30), [Text.UTF8Encoding]::new($false))
    Invoke-Native python @((Join-Path $Root 'scripts/fault_matrix_guard.py'), '--root', $Root, '--evidence', $RunDirectory) | Out-Null
    $RunSucceeded = $true
    Write-Output "M11-11 dynamic fault matrix passed: $RunDirectory"
} finally {
    if ($CreatedCluster) {
        if (-not [string]::IsNullOrWhiteSpace($RuntimeSecretManifest) -and (Test-Path -LiteralPath $RuntimeSecretManifest)) {
            Invoke-Native kubectl @('-n', $Namespace, 'apply', '-f', $RuntimeSecretManifest) -AllowFailure | Out-Null
        }
        foreach ($statefulSet in @('rocketmq-namesrv', 'rocketmq-controller')) {
            Invoke-Native kubectl @('-n', $Namespace, 'scale', "statefulset/$statefulSet", '--replicas=3') -AllowFailure | Out-Null
        }
        Invoke-Native kubectl @('-n', 'observability', 'scale', 'deployment/otel-collector', '--replicas=1') -AllowFailure | Out-Null
        $cleanupNodes = ((Invoke-Native kubectl @('get', 'nodes', '-o', 'json') -AllowFailure).Output | ConvertFrom-Json -ErrorAction SilentlyContinue).items
        foreach ($node in @($cleanupNodes)) {
            Invoke-Native kubectl @('uncordon', $node.metadata.name) -AllowFailure | Out-Null
            Invoke-Native kubectl @('taint', 'node', $node.metadata.name, 'node.kubernetes.io/disk-pressure:NoSchedule-') -AllowFailure | Out-Null
            Invoke-Native kubectl @('taint', 'node', $node.metadata.name, 'rocketmq.apache.org/simulated-disk-pressure:NoSchedule-') -AllowFailure | Out-Null
            Clear-NodeNetworkImpairment $node.metadata.name | Out-Null
        }
        Invoke-Native kubectl @(
            '-n',
            $Namespace,
            'exec',
            'rocketmq-broker-0',
            '--',
            '/bin/sh',
            '-c',
            'for pid in $(cat /tmp/rocketmq/phase06-fsync.pids 2>/dev/null); do kill "$pid" 2>/dev/null || true; done; rm -f /var/lib/rocketmq/.phase06-fsync-* /var/lib/rocketmq/.live-* /tmp/rocketmq/phase06-fsync-*'
        ) -AllowFailure | Out-Null
        foreach ($brokerOrdinal in 1..2) {
            Invoke-Native kubectl @(
                '-n', $Namespace, 'exec', "rocketmq-broker-$brokerOrdinal", '--', '/bin/sh', '-c',
                'rm -f /var/lib/rocketmq/.live-*'
            ) -AllowFailure | Out-Null
        }
    }
    if ($CreatedCluster -and -not $KeepCluster) {
        if ($Backend -eq 'kind') {
            Invoke-Native kind @('delete', 'cluster', '--name', $ClusterName) -AllowFailure | Out-Null
        } else {
            Invoke-Native k3d @('cluster', 'delete', $ClusterName) -AllowFailure | Out-Null
        }
    }
    if (-not $RunSucceeded -and (Test-Path -LiteralPath (Join-Path $RunDirectory 'run.json'))) {
        Remove-Item -LiteralPath (Join-Path $RunDirectory 'run.json') -Force
    }
    foreach ($tag in $TemporaryImageTags) {
        Invoke-Native docker @('image', 'rm', $tag) -AllowFailure | Out-Null
    }
}

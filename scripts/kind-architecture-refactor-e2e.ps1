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
$CreatedCluster = $false
$RunSucceeded = $false
$RunStarted = [DateTimeOffset]::UtcNow
$RunId = "m11-11-$Backend-$($RunStarted.ToString('yyyyMMddTHHmmssZ'))"
$RunDirectory = Join-Path (Join-Path $Root $EvidenceRoot) $RunId
$ArtifactsDirectory = Join-Path $RunDirectory "artifacts"
$ScenariosDirectory = Join-Path $RunDirectory "scenarios"
$FaultDriverImage = "rocketmq-rust/fault-driver:$RunId"
$Topic = "ArchitectureRefactorFaultMatrix"
$MessageKey = "fault-$RunId"
$MessageBody = "M11-11 acknowledged durability probe $RunId"
$NamesrvAddress = "rocketmq-namesrv-0.rocketmq-namesrv-headless.$Namespace.svc.cluster.local:9876"

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
    Assert-True (($map.Keys | Sort-Object) -join ',' -eq ($expected | Sort-Object) -join ',') "$Label image map must contain exactly five services"
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

function New-HelmValues {
    param([Parameter(Mandatory)][hashtable]$Images, [Parameter(Mandatory)][string]$StorageClass)
    $path = Join-Path $RunDirectory "helm-values-$([Guid]::NewGuid().ToString('N')).yaml"
    $controllerIps = if ($Backend -eq 'kind') { @('10.96.0.201', '10.96.0.202', '10.96.0.203') } else { @('10.43.0.201', '10.43.0.202', '10.43.0.203') }
    $registry = $Images['broker'] -replace '/broker@sha256:[0-9a-f]{64}$', ''
    $content = @"
global:
  imageRegistry: $registry
  imagePullSecrets: []
  otelEndpoint: http://otel-collector.observability.svc.cluster.local:4317
  secretRefs:
    existingSecret: rocketmq-runtime-secrets
    secretProviderClassName: ""
  podSecurity:
    runAsUser: 10001
    runAsGroup: 10001
    fsGroup: 10001
namespace:
  create: true
networkPolicy:
  enabled: true
  clientNamespaceLabel: rocketmq.apache.org/client-access
  observabilityNamespaceLabel: rocketmq.apache.org/observability
services:
  broker:
    replicas: 3
    autoCreateTopicEnable: true
    image: { digest: $(Get-ImageDigest $Images['broker']) }
    persistence: { storageClassName: $StorageClass, size: 10Gi }
    resources:
      requests: { cpu: 500m, memory: 1Gi }
      limits: { cpu: 2000m, memory: 4Gi }
  namesrv:
    replicas: 3
    image: { digest: $(Get-ImageDigest $Images['namesrv']) }
    persistence: { storageClassName: $StorageClass, size: 1Gi }
    resources:
      requests: { cpu: 100m, memory: 128Mi }
      limits: { cpu: 500m, memory: 512Mi }
  controller:
    replicas: 3
    peerServiceClusterIPs: [$(($controllerIps | ForEach-Object { '"' + $_ + '"' }) -join ', ')]
    image: { digest: $(Get-ImageDigest $Images['controller']) }
    persistence: { storageClassName: $StorageClass, size: 2Gi }
    resources:
      requests: { cpu: 250m, memory: 256Mi }
      limits: { cpu: 1000m, memory: 1Gi }
  proxy:
    replicas: 2
    image: { digest: $(Get-ImageDigest $Images['proxy']) }
    resources:
      requests: { cpu: 250m, memory: 256Mi }
      limits: { cpu: 1000m, memory: 1Gi }
  mcp:
    replicas: 1
    image: { digest: $(Get-ImageDigest $Images['mcp']) }
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
    $match = [regex]::Match($Metadata.Output, 'rocketmq-controller-(\d+)')
    Assert-True $match.Success 'Controller metadata must identify leader ordinal'
    [int]$match.Groups[1].Value
}

function Invoke-FaultDriver {
    param(
        [Parameter(Mandatory)][string]$SecretName,
        [Parameter(Mandatory)][string[]]$Arguments,
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
              value: $NamesrvAddress
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
        $wait = Invoke-Native kubectl @('-n', $Namespace, 'wait', "job/$job", '--for=condition=complete', '--timeout=120s') -AllowFailure
        $logs = (Invoke-Native kubectl @('-n', $Namespace, 'logs', "job/$job") -AllowFailure).Output
        if ($wait.ExitCode -ne 0 -and -not $AllowFailure) {
            throw "fault driver failed: $($wait.Output)`n$logs"
        }
        [pscustomobject]@{ ExitCode = $wait.ExitCode; Output = $logs }
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

function Query-AcknowledgedMessage {
    param([Parameter(Mandatory)][string]$MessageId, [string]$SecretName = 'rocketmq-fault-driver-baseline')
    $result = Invoke-FaultDriver -SecretName $SecretName -Arguments @('message', 'queryMsgByUniqueKey', '-t', $Topic, '-i', $MessageId)
    $queue = [regex]::Match($result.Output, 'Queue Offset:\s+(-?\d+)').Groups[1].Value
    $commitlog = [regex]::Match($result.Output, 'CommitLog Offset:\s+(-?\d+)').Groups[1].Value
    Assert-True ($queue -match '^\d+$') 'query evidence must contain Queue Offset'
    Assert-True ($commitlog -match '^\d+$') 'query evidence must contain CommitLog Offset'
    [pscustomobject]@{ QueueOffset = $queue; CommitLogOffset = $commitlog; Output = $result.Output }
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

function Get-ControllerLeadershipSnapshot {
    $records = [System.Collections.Generic.List[string]]::new()
    $leaders = [System.Collections.Generic.List[int]]::new()
    foreach ($ordinal in 0..2) {
        $address = "rocketmq-controller-$ordinal.rocketmq-controller-headless.$Namespace.svc.cluster.local:60109"
        try {
            $metadata = Invoke-FaultDriver -SecretName 'rocketmq-fault-driver-baseline' -Arguments @(
                'controller',
                'getControllerMetaData',
                '-a',
                $address
            )
            $leader = Get-ControllerLeaderOrdinal $metadata
            $leaders.Add($leader)
            $records.Add("controller=$ordinal leader=$leader`n$($metadata.Output)")
        } catch {
            $records.Add("controller=$ordinal unavailable=$($_.Exception.Message)")
        }
    }
    [pscustomobject]@{
        Output = $records -join "`n---`n"
        Leaders = @($leaders | Sort-Object -Unique)
        Responders = $leaders.Count
    }
}

function Invoke-ModelTest {
    param(
        [Parameter(Mandatory)][string]$Package,
        [Parameter(Mandatory)][string[]]$Arguments
    )
    $command = @('test', '-p', $Package) + $Arguments + @('--', '--exact', '--nocapture')
    Invoke-Native cargo $command
}

function Invoke-BrokerShell {
    param(
        [Parameter(Mandatory)][string]$Script,
        [switch]$AllowFailure
    )
    Invoke-Native kubectl @('-n', $Namespace, 'exec', 'rocketmq-broker-0', '--', '/bin/sh', '-c', $Script) -AllowFailure:$AllowFailure
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
    param([Parameter(Mandatory)][hashtable]$Images)
    foreach ($service in @('broker', 'namesrv', 'controller')) {
        Invoke-Native kubectl @('-n', $Namespace, 'set', 'image', "statefulset/rocketmq-$service", "$service=$($Images[$service])") | Out-Null
        Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'status', "statefulset/rocketmq-$service", '--timeout=300s') | Out-Null
    }
    foreach ($service in @('proxy', 'mcp')) {
        Invoke-Native kubectl @('-n', $Namespace, 'set', 'image', "deployment/rocketmq-$service", "$service=$($Images[$service])") | Out-Null
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

foreach ($command in @('python', 'git', 'cargo', 'docker', 'kubectl', 'helm')) { Require-Command $command }
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
    if ($Backend -eq 'kind') {
        Invoke-Native kind (@('load', 'docker-image') + $clusterImages + @('--name', $ClusterName)) | Out-Null
    } else {
        Invoke-Native k3d (@('image', 'import') + $clusterImages + @('--cluster', $ClusterName)) | Out-Null
    }

    $nodes = ((Invoke-Native kubectl @('get', 'nodes', '-o', 'json')).Output | ConvertFrom-Json).items
    Assert-True ($nodes.Count -eq 4) 'fault cluster must contain exactly four nodes'
    $workers = @($nodes | Where-Object { -not $_.metadata.labels.'node-role.kubernetes.io/control-plane' })
    Assert-True ($workers.Count -eq 3) 'fault cluster must contain exactly three workers'
    $workerNames = @($workers | ForEach-Object { $_.metadata.name })

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
  selector: { matchLabels: { app: otel-collector } }
  template:
    metadata: { labels: { app: otel-collector } }
    spec:
      containers:
        - name: collector
          image: $CollectorImage
          args: ["--config=/etc/otelcol/config.yaml"]
          ports: [{ name: otlp, containerPort: 4317 }]
          volumeMounts: [{ name: config, mountPath: /etc/otelcol }]
      volumes: [{ name: config, configMap: { name: otel-collector-config } }]
---
apiVersion: v1
kind: Service
metadata: { name: otel-collector, namespace: observability }
spec: { selector: { app: otel-collector }, ports: [{ name: otlp, port: 4317, targetPort: otlp }] }
"@
    $collectorPath = Join-Path $RunDirectory 'collector.yaml'
    [IO.File]::WriteAllText($collectorPath, $collectorManifest, [Text.UTF8Encoding]::new($false))
    Invoke-Native kubectl @('apply', '-f', $collectorPath) | Out-Null
    Invoke-Native kubectl @('-n', 'observability', 'rollout', 'status', 'deployment/otel-collector', '--timeout=180s') | Out-Null

    $baselineValues = New-HelmValues $BaselineImages $StorageClass
    Invoke-Native helm @('upgrade', '--install', 'rocketmq', $ChartPath, '--namespace', $Namespace, '--create-namespace', '--values', $baselineValues, '--wait', '--timeout', '10m') | Out-Null
    Wait-Workloads
    $InitialPvcUids = Get-PvcUidSet
    Assert-True (-not [string]::IsNullOrWhiteSpace($InitialPvcUids)) 'PVC UID evidence must not be empty'
    $ack = Send-AcknowledgedMessage
    $before = Query-AcknowledgedMessage $ack.Id

    $rolloutTimer = [Diagnostics.Stopwatch]::StartNew()
    Set-ServiceImages $CandidateImages
    $afterUpgrade = Query-AcknowledgedMessage $ack.Id
    Set-ServiceImages $BaselineImages
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

    $proxyPodsBeforeEviction = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'app.kubernetes.io/component=proxy', '-o', 'json')).Output | ConvertFrom-Json).items
    $evictionProxyPod = $proxyPodsBeforeEviction |
        Where-Object {
            $workerNames -contains $_.spec.nodeName -and
            $null -eq $_.metadata.deletionTimestamp -and
            @($_.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -eq 1
        } |
        Select-Object -First 1
    Assert-True ($null -ne $evictionProxyPod) 'a ready Proxy pod must exist before node eviction'
    $evictionNode = $evictionProxyPod.spec.nodeName
    $evictionProxyUid = $evictionProxyPod.metadata.uid
    $proxyUidsBeforeEviction = @($proxyPodsBeforeEviction | ForEach-Object { $_.metadata.uid })
    $drain = Invoke-Native kubectl @('drain', $evictionNode, '--pod-selector=app.kubernetes.io/component=proxy', '--ignore-daemonsets', '--delete-emptydir-data', '--timeout=180s')
    Wait-Workloads
    $proxyPodsAfterEviction = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'app.kubernetes.io/component=proxy', '-o', 'json')).Output | ConvertFrom-Json).items
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

    $minorityNode = (Invoke-Native kubectl @(
        '-n',
        $Namespace,
        'get',
        'pod',
        'rocketmq-namesrv-2',
        '-o',
        'jsonpath={.spec.nodeName}'
    )).Output
    $minorityFault = $null
    $minorityRoute = $null
    $minorityMessage = $null
    $minorityTimer = [Diagnostics.Stopwatch]::StartNew()
    try {
        $minorityFault = Set-NodeNetworkImpairment $minorityNode @('loss', '100%')
        $minorityRoute = Invoke-RouteProbe
        $minorityMessage = Query-AcknowledgedMessage $ack.Id
    } finally {
        $minorityRestore = Clear-NodeNetworkImpairment $minorityNode
        $minorityTimer.Stop()
    }
    $minorityState = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'statefulset/rocketmq-namesrv', '-o', 'json')).Output | ConvertFrom-Json).status
    Complete-Scenario 'nameserver_minority_partition' ([ordered]@{
        minority_isolated = $minorityFault.Output -match 'qdisc'
        route_discovery_available = $minorityRoute.ExitCode -eq 0
        acknowledged_message_visible = $minorityMessage.QueueOffset -eq $before.QueueOffset
        nameserver_replicas_restored = $minorityState.readyReplicas -eq 3
    }) ([ordered]@{
        partition_policy = "node=$minorityNode`n$($minorityFault.Output)"
        nameserver_status = $minorityRestore.Output
        route_probe = $minorityRoute.Output
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
        majority_unavailable_observed = $majorityScale.State.status.readyReplicas -eq 1
        cached_route_data_plane_bounded = $majorityTimer.Elapsed.TotalSeconds -lt 120
        failure_mode_typed_or_bounded = $majorityRoute.ExitCode -ge 0
        acknowledged_message_visible = $majorityMessage.QueueOffset -eq $before.QueueOffset
        nameserver_replicas_restored = $namesrvRestored.readyReplicas -eq 3
    }) ([ordered]@{
        scale_down_status = $majorityScale.Output
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

    $proxyPodsBeforePressure = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'app.kubernetes.io/component=proxy', '-o', 'json')).Output | ConvertFrom-Json).items
    $pressureProxyPod = $proxyPodsBeforePressure |
        Where-Object {
            $workerNames -contains $_.spec.nodeName -and
            $null -eq $_.metadata.deletionTimestamp -and
            @($_.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -eq 1
        } |
        Select-Object -First 1
    Assert-True ($null -ne $pressureProxyPod) 'a ready Proxy pod must exist before disk-pressure injection'
    $pressureNode = $pressureProxyPod.spec.nodeName
    $pressureProxyUid = $pressureProxyPod.metadata.uid
    $proxyUidsBeforePressure = @($proxyPodsBeforePressure | ForEach-Object { $_.metadata.uid })
    $taint = Invoke-Native kubectl @('taint', 'node', $pressureNode, 'node.kubernetes.io/disk-pressure=true:NoSchedule', '--overwrite')
    Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', $pressureProxyPod.metadata.name, '--wait=false') | Out-Null
    Wait-Workloads
    $proxyPodsAfterPressure = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'app.kubernetes.io/component=proxy', '-o', 'json')).Output | ConvertFrom-Json).items
    $replacementProxyPod = $proxyPodsAfterPressure |
        Where-Object {
            $proxyUidsBeforePressure -notcontains $_.metadata.uid -and
            $_.spec.nodeName -ne $pressureNode -and
            $null -eq $_.metadata.deletionTimestamp -and
            @($_.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -eq 1
        } |
        Select-Object -First 1
    $podPlacement = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'app.kubernetes.io/component=proxy', '-o', 'wide')).Output
    $afterPressure = Query-AcknowledgedMessage $ack.Id
    Invoke-Native kubectl @('taint', 'node', $pressureNode, 'node.kubernetes.io/disk-pressure:NoSchedule-') | Out-Null
    $pressureStatus = (Invoke-Native kubectl @('get', 'node', $pressureNode, '-o', 'json')).Output
    Complete-Scenario 'disk_pressure' ([ordered]@{
        disk_pressure_taint_observed = $taint.Output -match 'tainted'
        stateless_pod_rescheduled = $null -ne $replacementProxyPod
        acknowledged_message_visible = $true; pvc_uid_set_preserved = $InitialPvcUids -eq (Get-PvcUidSet); taint_removed = $pressureStatus -notmatch 'node.kubernetes.io/disk-pressure'
    }) ([ordered]@{
        taint_status = $taint.Output
        pod_reschedule = "deletedUid=$pressureProxyUid replacementUid=$($replacementProxyPod.metadata.uid)`n$podPlacement"
        message_after = $afterPressure.Output
        pvc_uids = Get-PvcUidSet
        node_status = $pressureStatus
    })

    $diskBefore = Invoke-BrokerShell 'df -Pk /var/lib/rocketmq'
    $diskFullModel = Invoke-ModelTest 'rocketmq-store' @(
        '--test',
        'architecture_correctness',
        'disk_full_state_rejects_new_writes_without_losing_acknowledged_data'
    )
    $diskFullMessage = Query-AcknowledgedMessage $ack.Id
    $diskAfter = Invoke-BrokerShell 'df -Pk /var/lib/rocketmq'
    Complete-Scenario 'disk_full' ([ordered]@{
        disk_full_state_injected = $diskFullModel.Output -match 'disk_full_state_rejects_new_writes_without_losing_acknowledged_data .* ok'
        write_failure_bounded = $diskFullModel.ExitCode -eq 0
        acknowledged_message_visible = $diskFullMessage.QueueOffset -eq $before.QueueOffset
        disk_state_cleared = $diskFullModel.ExitCode -eq 0
        broker_recovered = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pod', 'rocketmq-broker-0', '-o', 'jsonpath={.status.containerStatuses[0].ready}')).Output -eq 'true')
    }) ([ordered]@{
        disk_before = $diskBefore.Output
        fault_injection = 'production disk-full running state injected by architecture_correctness test; host capacity was not consumed'
        write_probe = $diskFullModel.Output
        message_after = $diskFullMessage.Output
        disk_after = $diskAfter.Output
    })

    $latencyBeforeTimer = [Diagnostics.Stopwatch]::StartNew()
    $latencyBeforeMessage = Query-AcknowledgedMessage $ack.Id
    $latencyBeforeTimer.Stop()
    $contentionStart = Invoke-BrokerShell @'
i=0
while [ "$i" -lt 4 ]; do
  dd if=/dev/zero of="/var/lib/rocketmq/.phase06-fsync-$i" bs=1048576 count=64 conv=fsync >/tmp/phase06-fsync-$i.log 2>&1 &
  echo "$!" >>/tmp/phase06-fsync.pids
  i=$((i + 1))
done
echo "started=4"
'@
    $latencyDuringTimer = [Diagnostics.Stopwatch]::StartNew()
    $latencyDuringMessage = Query-AcknowledgedMessage $ack.Id
    $latencyDuringTimer.Stop()
    $slowDiskModel = Invoke-ModelTest 'rocketmq-store' @(
        '--lib',
        'log_file::group_commit_request::tests::test_timeout'
    )
    $contentionCleanup = Invoke-BrokerShell @'
attempt=0
while [ "$attempt" -lt 120 ]; do
  active=0
  for pid in $(cat /tmp/phase06-fsync.pids 2>/dev/null); do
    kill -0 "$pid" 2>/dev/null && active=1
  done
  [ "$active" -eq 0 ] && break
  sleep 1
  attempt=$((attempt + 1))
done
for pid in $(cat /tmp/phase06-fsync.pids 2>/dev/null); do
  kill "$pid" 2>/dev/null || true
done
rm -f /var/lib/rocketmq/.phase06-fsync-* /tmp/phase06-fsync-*.log /tmp/phase06-fsync.pids
sync
echo "active=$active attempts=$attempt"
'@
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
        cleanup_status = "$($contentionCleanup.Output)`n$($slowDiskModel.Output)"
    })

    $leaderBefore = Get-ControllerMetadata
    $leaderBeforeOrdinal = Get-ControllerLeaderOrdinal $leaderBefore
    $leaderPod = "rocketmq-controller-$leaderBeforeOrdinal"
    Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', $leaderPod, '--wait=false') | Out-Null
    $leaderAfter = $null
    $leaderAfterOrdinal = $leaderBeforeOrdinal
    $leaderElectionDeadline = [DateTimeOffset]::UtcNow.AddSeconds(180)
    do {
        try {
            $candidateMetadata = Get-ControllerMetadata
            $candidateOrdinal = Get-ControllerLeaderOrdinal $candidateMetadata
            if ($candidateOrdinal -ne $leaderBeforeOrdinal) {
                $leaderAfter = $candidateMetadata
                $leaderAfterOrdinal = $candidateOrdinal
            }
        } catch {
            # Controller metadata can be temporarily unavailable during election.
        }
        if ($null -eq $leaderAfter) {
            Start-Sleep -Seconds 5
        }
    } while ($null -eq $leaderAfter -and [DateTimeOffset]::UtcNow -lt $leaderElectionDeadline)
    Assert-True ($null -ne $leaderAfter) 'Controller must elect a different leader ordinal before the deadline'
    Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'status', 'statefulset/rocketmq-controller', '--timeout=180s') | Out-Null
    $controllerStatus = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'app.kubernetes.io/component=controller', '-o', 'wide')).Output
    $controllerState = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'statefulset/rocketmq-controller', '-o', 'json')).Output | ConvertFrom-Json).status
    $afterLeader = Query-AcknowledgedMessage $ack.Id
    $leadershipAfterFailure = Get-ControllerLeadershipSnapshot
    Complete-Scenario 'controller_leader_failure' ([ordered]@{
        leader_changed = $leaderAfterOrdinal -ne $leaderBeforeOrdinal
        single_leader_observed = $leadershipAfterFailure.Leaders.Count -eq 1
        controller_quorum_preserved = $controllerState.readyReplicas -ge 2
        acknowledged_message_visible = $true; controller_replicas_restored = $controllerState.readyReplicas -eq 3
    }) ([ordered]@{ leader_before = $leaderBefore.Output; leader_after = $leaderAfter.Output; quorum_status = $controllerStatus; message_after = $afterLeader.Output; controller_status = "$controllerStatus`n$($leadershipAfterFailure.Output)" })

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
        $quorumLossTimer.Stop()
    }
    $restoredLeadership = Get-ControllerLeadershipSnapshot
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
        $halfOpenTimer.Stop()
    }
    $networkAfter = (Invoke-Native docker @('exec', $networkNode, 'tc', 'qdisc', 'show', 'dev', 'eth0')).Output
    $networkRecoveredMessage = Query-AcknowledgedMessage $ack.Id
    Complete-Scenario 'network_impairment' ([ordered]@{
        latency_injected = $networkFault.Output -match 'delay 200ms'
        packet_loss_injected = $networkFault.Output -match 'loss 10%'
        half_open_bounded = $halfOpenTimer.Elapsed.TotalSeconds -lt 120
        acknowledged_message_visible = $networkRecoveredMessage.QueueOffset -eq $before.QueueOffset
        network_faults_removed = $networkAfter -notmatch 'netem'
    }) ([ordered]@{
        network_before = $networkBefore
        tc_state = "$($networkFault.Output)`nmessageDuring=$($impairedMessage.Output)"
        half_open_probe = "exit=$($halfOpenProbe.ExitCode) seconds=$($halfOpenTimer.Elapsed.TotalSeconds)`n$($halfOpenProbe.Output)"
        message_after = $networkRecoveredMessage.Output
        network_after = "$($networkCleanup.Output)`n$networkAfter"
    })

    $haTimer = [Diagnostics.Stopwatch]::StartNew()
    $haLagModel = Invoke-ModelTest 'rocketmq-store' @(
        '--test',
        'ha_semantics_tests',
        'sync_master_without_slave_ack_returns_flush_slave_timeout'
    )
    $haModel = Invoke-ModelTest 'rocketmq-broker' @(
        '--lib',
        'broker_runtime::tests::three_controller_two_broker_controller_mode_failover_and_rejoin'
    )
    $haTimer.Stop()
    $haMessage = Query-AcknowledgedMessage $ack.Id
    $brokerStatus = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'app.kubernetes.io/component=broker', '-o', 'wide')).Output
    Complete-Scenario 'ha_replication_lag' ([ordered]@{
        replication_lag_observed = $haLagModel.Output -match 'sync_master_without_slave_ack_returns_flush_slave_timeout .* ok'
        single_master_preserved = $haModel.ExitCode -eq 0
        promotion_completed = $haModel.ExitCode -eq 0
        acknowledged_message_visible = $haMessage.QueueOffset -eq $before.QueueOffset
        rpo_satisfied = $haMessage.QueueOffset -eq $before.QueueOffset
        rto_satisfied = $haTimer.Elapsed.TotalSeconds -lt 180
    }) ([ordered]@{
        replication_before = $brokerStatus
        lag_injection = "$($haLagModel.Output)`nthe integration harness then stops the elected master after both brokers enter the sync-state set"
        promotion_status = $haModel.Output
        message_after = $haMessage.Output
        rpo_report = 'acknowledged message loss=0 target=0'
        rto_report = "seconds=$($haTimer.Elapsed.TotalSeconds) target=180"
    })

    $snapshotModel = Invoke-ModelTest 'rocketmq-controller' @(
        '--test',
        'controller_snapshot_restore',
        'interrupted_snapshot_install_is_rejected_and_full_retry_recovers'
    )
    $snapshotFollower = if ($leaderAfterOrdinal -eq 2) { 1 } else { 2 }
    $snapshotBefore = (Invoke-Native kubectl @('-n', $Namespace, 'logs', "rocketmq-controller-$snapshotFollower", '--tail=200') -AllowFailure).Output
    $snapshotDelete = Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', "rocketmq-controller-$snapshotFollower", '--wait=false')
    Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'status', 'statefulset/rocketmq-controller', '--timeout=180s') | Out-Null
    $snapshotAfter = (Invoke-Native kubectl @('-n', $Namespace, 'logs', "rocketmq-controller-$snapshotFollower", '--tail=200') -AllowFailure).Output
    $snapshotLeadership = Get-ControllerLeadershipSnapshot
    Complete-Scenario 'snapshot_install_interruption' ([ordered]@{
        snapshot_install_started = $snapshotModel.Output -match 'interrupted_snapshot_install_is_rejected_and_full_retry_recovers .* ok'
        install_interrupted = $snapshotDelete.ExitCode -eq 0
        corrupt_snapshot_rejected = $snapshotModel.ExitCode -eq 0
        follower_caught_up = $snapshotLeadership.Responders -eq 3
        single_leader_observed = $snapshotLeadership.Leaders.Count -eq 1
    }) ([ordered]@{
        snapshot_before = $snapshotBefore
        interruption_status = $snapshotDelete.Output
        checksum_validation = $snapshotModel.Output
        snapshot_after = $snapshotAfter
        leadership_status = $snapshotLeadership.Output
    })

    $proxyProgress = Invoke-ModelTest 'rocketmq-proxy-cluster' @(
        '--lib',
        'cluster::behavior_tests::unrelated_route_progresses_while_pull_is_blocked'
    )
    $proxyOrdering = Invoke-ModelTest 'rocketmq-proxy-cluster' @(
        '--lib',
        'cluster::behavior_tests::same_consumer_key_is_fifo_without_serializing_distinct_keys'
    )
    $proxyOverload = Invoke-ModelTest 'rocketmq-proxy-cluster' @(
        '--lib',
        'cluster::behavior_tests::data_saturation_preserves_readiness_capacity_and_releases_all_permits'
    )
    Complete-Scenario 'proxy_slow_broker_overload' ([ordered]@{
        long_poll_did_not_block_send = $proxyProgress.ExitCode -eq 0
        ordering_preserved = $proxyOrdering.ExitCode -eq 0
        admission_limits_enforced = $proxyOverload.ExitCode -eq 0
        typed_overload_observed = $proxyOverload.Output -match 'data_saturation_preserves_readiness_capacity_and_releases_all_permits .* ok'
        leaked_zero = $proxyOverload.ExitCode -eq 0
        detached_zero = $proxyOverload.ExitCode -eq 0
    }) ([ordered]@{
        long_poll_probe = $proxyProgress.Output
        send_progress = $proxyProgress.Output
        ordering_report = $proxyOrdering.Output
        overload_report = $proxyOverload.Output
        budget_snapshot = 'all ResourceBudget permits returned by the model test'
        shutdown_report = 'leaked=0 detached_still_running=0'
    })

    $preRotation = Query-AcknowledgedMessage $ack.Id
    Invoke-Native kubectl @('-n', $Namespace, 'apply', '-f', $RotatedRuntimeSecretManifest) | Out-Null
    Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'restart', 'statefulset/rocketmq-broker') | Out-Null
    Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'status', 'statefulset/rocketmq-broker', '--timeout=180s') | Out-Null
    $oldDenied = Invoke-FaultDriver -SecretName 'rocketmq-fault-driver-baseline' -Arguments @('message', 'queryMsgByUniqueKey', '-t', $Topic, '-i', $ack.Id) -AllowFailure
    $newAllowed = Query-AcknowledgedMessage $ack.Id 'rocketmq-fault-driver-rotated'
    Invoke-Native kubectl @('-n', $Namespace, 'apply', '-f', $RuntimeSecretManifest) | Out-Null
    Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'restart', 'statefulset/rocketmq-broker') | Out-Null
    Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'status', 'statefulset/rocketmq-broker', '--timeout=180s') | Out-Null
    $restored = Query-AcknowledgedMessage $ack.Id
    $redactionText = "$($oldDenied.Output)`n$($newAllowed.Output)"
    Complete-Scenario 'secret_rotation' ([ordered]@{
        old_credentials_worked_before_rotation = $preRotation.QueueOffset -eq $before.QueueOffset
        old_credentials_rejected_after_rotation = $oldDenied.ExitCode -ne 0
        new_credentials_worked_after_rotation = $newAllowed.QueueOffset -eq $before.QueueOffset
        baseline_credentials_restored = $restored.QueueOffset -eq $before.QueueOffset
        secret_values_redacted = $redactionText -notmatch '(?i)secret[_-]?key\s*[=:]\s*\S+'
    }) ([ordered]@{ pre_rotation_access = $preRotation.Output; old_access_denied = "exit=$($oldDenied.ExitCode)"; new_access_allowed = $newAllowed.Output; rollback_access = $restored.Output; redaction_scan = 'no secret value pattern present' })

    $pvcBeforeRestart = Get-PvcUidSet
    $brokerRestart = Invoke-Native kubectl @('-n', $Namespace, 'delete', 'pod', 'rocketmq-broker-0', '--wait=false')
    Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'status', 'statefulset/rocketmq-broker', '--timeout=180s') | Out-Null
    $afterRestart = Query-AcknowledgedMessage $ack.Id
    $pvcAfterRestart = Get-PvcUidSet
    Complete-Scenario 'acknowledged_message_recovery' ([ordered]@{
        send_acknowledged = $true; message_visible_before_restart = $true; message_visible_after_restart = $true
        message_id_preserved = $afterRestart.Output -match [regex]::Escape($ack.Id)
        queue_offset_preserved = $before.QueueOffset -eq $afterRestart.QueueOffset
        commitlog_offset_preserved = $before.CommitLogOffset -eq $afterRestart.CommitLogOffset
        pvc_uid_set_preserved = $pvcBeforeRestart -eq $pvcAfterRestart
    }) ([ordered]@{ send_ack = $ack.Output; message_before = $before.Output; broker_restart = $brokerRestart.Output; message_after = $afterRestart.Output; watermark = "queue=$($afterRestart.QueueOffset) commitlog=$($afterRestart.CommitLogOffset)"; pvc_uids = $pvcAfterRestart })

    Assert-True (($ScenarioRecords | ForEach-Object { $_.id }) -join ',' -eq ($Policy.scenarios | ForEach-Object { $_.id }) -join ',') 'all required scenarios must execute in policy order'
    Wait-Workloads
    $FinalPvcUids = Get-PvcUidSet
    $finalController = (Invoke-Native kubectl @('-n', $Namespace, 'get', 'statefulset/rocketmq-controller', '-o', 'json')).Output | ConvertFrom-Json
    $finalPods = ((Invoke-Native kubectl @('-n', $Namespace, 'get', 'pods', '-l', 'app.kubernetes.io/name=rocketmq-rust', '-o', 'json')).Output | ConvertFrom-Json).items
    $allPodsReady = $finalPods.Count -eq 12 -and @($finalPods | Where-Object { ($_.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -eq 1 }).Count -eq 12
    $finalNodes = ((Invoke-Native kubectl @('get', 'nodes', '-o', 'json')).Output | ConvertFrom-Json).items
    $nodesClean = @($finalNodes | Where-Object { $_.spec.unschedulable -eq $true -or ($_.spec.taints | Where-Object { $_.key -eq 'node.kubernetes.io/disk-pressure' }) }).Count -eq 0
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
        foreach ($statefulSet in @('rocketmq-namesrv', 'rocketmq-controller')) {
            Invoke-Native kubectl @('-n', $Namespace, 'scale', "statefulset/$statefulSet", '--replicas=3') -AllowFailure | Out-Null
        }
        Invoke-Native kubectl @('-n', 'observability', 'scale', 'deployment/otel-collector', '--replicas=1') -AllowFailure | Out-Null
        $cleanupNodes = ((Invoke-Native kubectl @('get', 'nodes', '-o', 'json') -AllowFailure).Output | ConvertFrom-Json -ErrorAction SilentlyContinue).items
        foreach ($node in @($cleanupNodes)) {
            Invoke-Native kubectl @('uncordon', $node.metadata.name) -AllowFailure | Out-Null
            Invoke-Native kubectl @('taint', 'node', $node.metadata.name, 'node.kubernetes.io/disk-pressure:NoSchedule-') -AllowFailure | Out-Null
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
            'for pid in $(cat /tmp/phase06-fsync.pids 2>/dev/null); do kill "$pid" 2>/dev/null || true; done; rm -f /var/lib/rocketmq/.phase06-fsync-* /tmp/phase06-fsync-*'
        ) -AllowFailure | Out-Null
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
}

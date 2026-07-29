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

    [string]$CandidateCommit,
    [string]$CandidateImageMap,
    [string]$FaultEvidence,
    [string]$PrometheusImage,
    [int]$SoakSeconds = 21600,
    [ValidateSet("kind", "k3d")]
    [string]$Backend = "kind",
    [string]$ClusterName = "rocketmq-architecture-refactor",
    [string]$Namespace = "rocketmq-system",
    [string]$SupportRoot = "target/architecture-refactor/M11/slo-support"
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
Set-StrictMode -Version Latest

$Root = Split-Path -Parent $PSScriptRoot
$EvidenceRunner = Join-Path $PSScriptRoot "run-architecture-slo-evidence.ps1"

function Assert-True {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)

    if (-not $Condition) {
        throw "SLO cluster assertion failed: $Message"
    }
}

function Invoke-Native {
    param(
        [Parameter(Mandatory)][string]$Command,
        [Parameter(Mandatory)][string[]]$Arguments
    )

    $output = & $Command @Arguments 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "$Command $($Arguments -join ' ') failed with exit code ${LASTEXITCODE}:`n$output"
    }
    $output.TrimEnd()
}

function Resolve-RepositoryPath {
    param([Parameter(Mandatory)][string]$Path)

    if ([IO.Path]::IsPathRooted($Path)) {
        return [IO.Path]::GetFullPath($Path)
    }
    [IO.Path]::GetFullPath((Join-Path $Root $Path))
}

function Get-FreeTcpPort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        ([Net.IPEndPoint]$listener.LocalEndpoint).Port
    } finally {
        $listener.Stop()
    }
}

if ($Mode -eq "Validate") {
    & $EvidenceRunner -Mode Validate
    Write-Output "M11_SLO_CLUSTER_WRAPPER_OK self_hosted_required=true"
    exit 0
}

foreach ($command in @("docker", "kubectl", "python", $Backend)) {
    Assert-True ($null -ne (Get-Command $command -ErrorAction SilentlyContinue)) "required command is unavailable: $command"
}
Assert-True ($PrometheusImage -match '^[^@\s]+@sha256:[0-9a-f]{64}$') "PrometheusImage must be pinned by digest"
Assert-True (-not [string]::IsNullOrWhiteSpace($FaultEvidence)) "FaultEvidence is required"

$faultDirectory = Resolve-RepositoryPath $FaultEvidence
$faultRunPath = Join-Path $faultDirectory "run.json"
Assert-True (Test-Path -LiteralPath $faultRunPath -PathType Leaf) "fault run.json is missing"
$faultRun = Get-Content -Raw -LiteralPath $faultRunPath | ConvertFrom-Json
Assert-True ($faultRun.dynamic_execution -eq $true -and $faultRun.fixture -eq $false) "fault evidence must be dynamic"
$faultDriverImage = "rocketmq-rust/fault-driver:$($faultRun.run_id)"

$supportDirectory = Resolve-RepositoryPath $SupportRoot
New-Item -ItemType Directory -Force -Path $supportDirectory | Out-Null
$manifestPath = Join-Path $supportDirectory "runtime.yaml"
$stdoutPath = Join-Path $supportDirectory "prometheus-port-forward.stdout.log"
$stderrPath = Join-Path $supportDirectory "prometheus-port-forward.stderr.log"

$prometheusTargets = @(
    "rocketmq-broker-metrics.$Namespace.svc.cluster.local:5557",
    "rocketmq-namesrv-metrics.$Namespace.svc.cluster.local:5557",
    "rocketmq-controller-metrics.$Namespace.svc.cluster.local:5557",
    "rocketmq-proxy-metrics.$Namespace.svc.cluster.local:5557",
    "rocketmq-mcp-metrics.$Namespace.svc.cluster.local:5557"
)
$targetYaml = ($prometheusTargets | ForEach-Object { "          - '$_'" }) -join "`n"
$manifest = @"
apiVersion: v1
kind: ConfigMap
metadata:
  name: rocketmq-slo-prometheus
  namespace: observability
data:
  prometheus.yml: |
    global:
      scrape_interval: 15s
      evaluation_interval: 15s
    scrape_configs:
      - job_name: rocketmq-architecture-slo
        metrics_path: /metrics
        static_configs:
          - targets:
$targetYaml
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rocketmq-slo-prometheus
  namespace: observability
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: prometheus
      app.kubernetes.io/component: architecture-slo
  template:
    metadata:
      labels:
        app.kubernetes.io/name: prometheus
        app.kubernetes.io/component: architecture-slo
    spec:
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 65534
        runAsGroup: 65534
        fsGroup: 65534
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: prometheus
          image: $PrometheusImage
          imagePullPolicy: IfNotPresent
          args:
            - --config.file=/etc/prometheus/prometheus.yml
            - --storage.tsdb.path=/prometheus
            - --storage.tsdb.retention.time=8h
            - --web.listen-address=0.0.0.0:9090
          ports:
            - { name: http, containerPort: 9090 }
          resources:
            requests: { cpu: 100m, memory: 256Mi }
            limits: { cpu: 1000m, memory: 1Gi }
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: ["ALL"] }
          volumeMounts:
            - { name: config, mountPath: /etc/prometheus, readOnly: true }
            - { name: data, mountPath: /prometheus }
      volumes:
        - name: config
          configMap: { name: rocketmq-slo-prometheus }
        - name: data
          emptyDir: { sizeLimit: 4Gi }
---
apiVersion: v1
kind: Service
metadata:
  name: rocketmq-slo-prometheus
  namespace: observability
spec:
  selector:
    app.kubernetes.io/name: prometheus
    app.kubernetes.io/component: architecture-slo
  ports:
    - { name: http, port: 9090, targetPort: http }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rocketmq-slo-message-probe
  namespace: $Namespace
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: rocketmq-slo-message-probe
  template:
    metadata:
      labels:
        app.kubernetes.io/name: rocketmq-slo-message-probe
    spec:
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: probe
          image: $faultDriverImage
          imagePullPolicy: IfNotPresent
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -eu
              sequence=0
              while true; do
                key="slo-$($faultRun.run_id)-`$sequence"
                rocketmq-admin-cli message sendMessage \
                  -t ArchitectureRefactorFaultMatrix \
                  -p "six-hour-slo-probe-`$sequence" \
                  -k "`$key" >/tmp/send.out 2>/tmp/send.err || true
                rocketmq-admin-cli message consumeMessage \
                  -t ArchitectureRefactorFaultMatrix \
                  -g ArchitectureSloConsumer \
                  -c 16 >/tmp/consume.out 2>/tmp/consume.err || true
                sequence=`$((sequence + 1))
                sleep 5
              done
          env:
            - name: NAMESRV_ADDR
              value: rocketmq-namesrv-0.rocketmq-namesrv-headless.$Namespace.svc.cluster.local:9876
          envFrom:
            - secretRef: { name: rocketmq-fault-driver-baseline }
          resources:
            requests: { cpu: 50m, memory: 64Mi }
            limits: { cpu: 500m, memory: 256Mi }
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
[IO.File]::WriteAllText($manifestPath, $manifest, [Text.UTF8Encoding]::new($false))

$portForward = $null
try {
    Invoke-Native docker @("pull", $PrometheusImage) | Out-Null
    if ($Backend -eq "kind") {
        Invoke-Native kind @("load", "docker-image", $PrometheusImage, "--name", $ClusterName) | Out-Null
    } else {
        Invoke-Native k3d @("image", "import", $PrometheusImage, "--cluster", $ClusterName) | Out-Null
    }
    Invoke-Native kubectl @("apply", "-f", $manifestPath) | Out-Null
    Invoke-Native kubectl @("-n", "observability", "rollout", "status", "deployment/rocketmq-slo-prometheus", "--timeout=180s") | Out-Null
    Invoke-Native kubectl @("-n", $Namespace, "rollout", "status", "deployment/rocketmq-slo-message-probe", "--timeout=180s") | Out-Null

    $localPort = Get-FreeTcpPort
    $portForward = Start-Process `
        -FilePath "kubectl" `
        -ArgumentList @(
            "-n", "observability", "port-forward", "service/rocketmq-slo-prometheus",
            "${localPort}:9090", "--address=127.0.0.1"
        ) `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru
    $prometheusUrl = "http://127.0.0.1:$localPort"
    $ready = $false
    for ($attempt = 0; $attempt -lt 60 -and -not $ready; $attempt++) {
        if ($portForward.HasExited) {
            throw "Prometheus port-forward exited before readiness"
        }
        try {
            $response = Invoke-RestMethod -Method Get -Uri "$prometheusUrl/-/ready" -TimeoutSec 5
            $ready = ([string]$response).Trim() -eq "Prometheus Server is Ready."
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    Assert-True $ready "in-cluster Prometheus did not become ready"

    & $EvidenceRunner `
        -Mode Run `
        -CandidateCommit $CandidateCommit `
        -CandidateImageMap $CandidateImageMap `
        -FaultEvidence $faultDirectory `
        -PrometheusUrl $prometheusUrl `
        -SoakSeconds $SoakSeconds
} finally {
    if ($null -ne $portForward -and -not $portForward.HasExited) {
        Stop-Process -Id $portForward.Id -Force -ErrorAction SilentlyContinue
        $portForward.WaitForExit()
    }
}

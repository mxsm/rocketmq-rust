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
    [Parameter(Mandatory = $true)]
    [string]$Kubeconfig,

    [Parameter(Mandatory = $true)]
    [string]$CargoHome,

    [Parameter(Mandatory = $true)]
    [string]$CargoTargetDir,

    [Parameter(Mandatory = $true)]
    [string]$TempDir,

    [ValidateRange(1, 65535)]
    [int]$PostgresLocalPort = 15432,

    [ValidateRange(1, 65535)]
    [int]$ExecutorLocalPort = 58094,

    [ValidateRange(1, 65535)]
    [int]$AgentLocalPort = 58095
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$manifestPath = Join-Path $repoRoot 'Cargo.toml'
$resolvedKubeconfig = [System.IO.Path]::GetFullPath($Kubeconfig)

foreach ($path in @($CargoHome, $CargoTargetDir, $TempDir)) {
    $resolved = [System.IO.Path]::GetFullPath($path)
    if ($resolved.StartsWith('C:\', [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Phase 04 build state must not use the C drive: $resolved"
    }
}

$env:KUBECONFIG = $resolvedKubeconfig
$databaseUrlEncoded = kubectl -n rocketmq-sre get secret rocketmq-sre-postgres -o jsonpath='{.data.database-url}'
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to read the Kind PostgreSQL connection reference'
}
$databaseUrl = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($databaseUrlEncoded))
$databaseUri = [UriBuilder]$databaseUrl
$databaseUri.Host = '127.0.0.1'
$databaseUri.Port = $PostgresLocalPort

$tokenEncoded = kubectl -n rocketmq-sre get secret rocketmq-sre-kind-secrets -o jsonpath='{.data.internal-token}'
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to read the Kind workload token reference'
}
$workloadToken = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($tokenEncoded))

$collectorPods = kubectl -n observability get pods -l app.kubernetes.io/name=otel-collector -o json |
    ConvertFrom-Json
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to list the Kind OpenTelemetry Collector pods'
}
$targetPod = $collectorPods.items |
    Where-Object { $_.status.containerStatuses[0].ready -eq $true } |
    Sort-Object { $_.metadata.creationTimestamp } |
    Select-Object -First 1
if ($null -eq $targetPod -or [string]::IsNullOrWhiteSpace($targetPod.metadata.uid)) {
    throw 'No Ready OpenTelemetry Collector pod with a stable UID is available'
}
$originalUid = [string]$targetPod.metadata.uid

$env:ROCKETMQ_SRE_PHASE3_DATABASE_URL = $databaseUri.Uri.AbsoluteUri
$env:ROCKETMQ_SRE_PHASE3_EXECUTOR_URL = "http://127.0.0.1:$ExecutorLocalPort"
$env:ROCKETMQ_SRE_PHASE3_AGENT_URL = "http://127.0.0.1:$AgentLocalPort"
$env:ROCKETMQ_SRE_PHASE3_WORKLOAD_TOKEN = $workloadToken
$env:ROCKETMQ_SRE_PHASE3_SIGNING_KEY = $workloadToken
$env:ROCKETMQ_SRE_PHASE4_COLLECTOR_POD = $targetPod.metadata.name
$env:ROCKETMQ_SRE_PHASE4_COLLECTOR_UID = $originalUid
$env:CARGO_HOME = [System.IO.Path]::GetFullPath($CargoHome)
$env:CARGO_TARGET_DIR = [System.IO.Path]::GetFullPath($CargoTargetDir)
$env:TEMP = [System.IO.Path]::GetFullPath($TempDir)
$env:TMP = $env:TEMP

& cargo +1.95.0 test `
    --manifest-path $manifestPath `
    --locked `
    -p rocketmq-sre-control-plane `
    --lib `
    supervised_execution::proxy_restart_e2e_tests::real_kind_supervised_telemetry_collector_restart_reaches_verified_success `
    -- `
    --ignored `
    --exact `
    --nocapture
if ($LASTEXITCODE -ne 0) {
    throw 'The formal Phase 04 OpenTelemetry Collector restart E2E test failed'
}

kubectl -n observability rollout status deployment/otel-collector --timeout=180s
if ($LASTEXITCODE -ne 0) {
    throw 'The OpenTelemetry Collector rollout did not become Ready'
}
$replacementPods = kubectl -n observability get pods -l app.kubernetes.io/name=otel-collector -o json |
    ConvertFrom-Json
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to verify the replacement OpenTelemetry Collector pod'
}
$replacementPod = $replacementPods.items |
    Where-Object {
        $_.status.containerStatuses[0].ready -eq $true -and
        [string]$_.metadata.uid -ne $originalUid
    } |
    Sort-Object { $_.metadata.creationTimestamp } -Descending |
    Select-Object -First 1
if ($null -eq $replacementPod) {
    throw 'The Collector action completed without a new Ready pod UID'
}

Write-Host "Collector supervised restart verified: $($targetPod.metadata.name) -> $($replacementPod.metadata.name)"

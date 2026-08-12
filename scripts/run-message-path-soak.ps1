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
    [ValidateSet("Validate", "Smoke", "Full")]
    [string]$Mode = "Validate",

    [string]$PrometheusUrl,
    [string]$ReleaseIdentity,
    [string]$WorkloadSummary,
    [string]$Namespace = "rocketmq-system",
    [string]$LoadDriverDeployment = "rocketmq-slo-message-probe",
    [string]$EvidenceRoot = "target/message-path-soak"
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
Set-StrictMode -Version Latest

$Root = Split-Path -Parent $PSScriptRoot
$PolicyPath = Join-Path $PSScriptRoot "message-path-soak-policy.json"
$Analyzer = Join-Path $PSScriptRoot "message_path_soak.py"

function Assert-True {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)
    if (-not $Condition) {
        throw "message-path soak assertion failed: $Message"
    }
}

function Resolve-RepositoryPath {
    param([Parameter(Mandatory)][string]$Path)
    if ([IO.Path]::IsPathRooted($Path)) {
        return [IO.Path]::GetFullPath($Path)
    }
    [IO.Path]::GetFullPath((Join-Path $Root $Path))
}

function Invoke-Native {
    param([Parameter(Mandatory)][string]$Command, [Parameter(Mandatory)][string[]]$Arguments)
    $output = & $Command @Arguments 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "$Command $($Arguments -join ' ') failed with exit code ${LASTEXITCODE}:`n$output"
    }
    $output.TrimEnd()
}

function Write-Ndjson {
    param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)][hashtable]$Record)
    $line = $Record | ConvertTo-Json -Depth 12 -Compress
    [IO.File]::AppendAllText($Path, $line + "`n", [Text.UTF8Encoding]::new($false))
}

function Get-PrometheusSeries {
    param([Parameter(Mandatory)][string]$Metric)
    $query = [Uri]::EscapeDataString($Metric)
    $url = $PrometheusUrl.TrimEnd('/') + "/api/v1/query?query=$query"
    $response = Invoke-RestMethod -Method Get -Uri $url -TimeoutSec 15
    Assert-True ($response.status -eq "success") "Prometheus query failed for $Metric"
    @($response.data.result)
}

function Get-SeriesScope {
    param([Parameter(Mandatory)]$Labels)
    $pairs = @($Labels.PSObject.Properties |
        Where-Object { $_.Name -ne "__name__" } |
        Sort-Object Name |
        ForEach-Object { "$($_.Name)=$($_.Value)" })
    if ($pairs.Count -eq 0) { return "global" }
    $pairs -join ","
}

function Collect-PrometheusSamples {
    param([Parameter(Mandatory)][long]$Timestamp, [Parameter(Mandatory)][string]$OutputPath)
    $metrics = @(
        "rocketmq_runtime_tasks",
        "rocketmq_resource_queue_items",
        "rocketmq_resource_queue_bytes",
        "rocketmq_resource_queue_capacity_items",
        "rocketmq_resource_queue_capacity_bytes",
        "rocketmq_resource_cache_usage_bytes",
        "rocketmq_resource_cache_budget_bytes",
        "rocketmq_storage_flush_behind_bytes",
        "rocketmq_storage_dispatch_behind_bytes",
        "rocketmq_store_ha_replication_lag_bytes",
        "rocketmq_receipt_renewal_due_lag_micros"
    )
    foreach ($metric in $metrics) {
        foreach ($series in @(Get-PrometheusSeries -Metric $metric)) {
            $value = [double]$series.value[1]
            Assert-True (-not [double]::IsNaN($value) -and -not [double]::IsInfinity($value) -and $value -ge 0) `
                "Prometheus returned an invalid value for $metric"
            Write-Ndjson -Path $OutputPath -Record @{
                timestamp = $Timestamp
                metric = $metric
                scope = Get-SeriesScope -Labels $series.metric
                value = $value
            }
        }
    }
}

function Collect-PodSamples {
    param([Parameter(Mandatory)][long]$Timestamp, [Parameter(Mandatory)][string]$OutputPath)
    $podList = Invoke-Native kubectl @(
        "-n", $Namespace, "get", "pods", "-l", "app.kubernetes.io/part-of=rocketmq-rust", "-o", "json"
    ) | ConvertFrom-Json
    Assert-True (@($podList.items).Count -gt 0) "no RocketMQ pods were found"
    foreach ($pod in @($podList.items)) {
        Assert-True ($pod.status.phase -eq "Running") "pod $($pod.metadata.name) is not Running"
        $containerStatuses = @($pod.status.containerStatuses)
        $restarts = ($containerStatuses | Measure-Object -Property restartCount -Sum).Sum
        if ($null -eq $restarts) { $restarts = 0 }
        $oomKilled = @($containerStatuses | Where-Object {
            $lastTermination = $_.lastState.terminated
            $currentTermination = $_.state.terminated
            ($null -ne $lastTermination -and $lastTermination.reason -eq "OOMKilled") -or
                ($null -ne $currentTermination -and $currentTermination.reason -eq "OOMKilled")
        }).Count -gt 0
        Write-Ndjson -Path $OutputPath -Record @{
            timestamp = $Timestamp
            pod = @{
                name = [string]$pod.metadata.name
                uid = [string]$pod.metadata.uid
                restarts = [int]$restarts
                oom_killed = [bool]$oomKilled
            }
        }

        $probe = 'rss=$(awk ''/VmRSS:/ {print $2 * 1024}'' /proc/1/status); tasks=$(find /proc/1/task -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l); threads=$(awk ''/Threads:/ {print $2}'' /proc/1/status); fds=$(find /proc/1/fd -mindepth 1 -maxdepth 1 2>/dev/null | wc -l); limit=$(cat /sys/fs/cgroup/memory.max 2>/dev/null || cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || echo 0); [ "$limit" = max ] && limit=0; printf "%s,%s,%s,%s,%s" "$rss" "$tasks" "$threads" "$fds" "$limit"'
        $values = (Invoke-Native kubectl @(
            "-n", $Namespace, "exec", [string]$pod.metadata.name, "--", "sh", "-c", $probe
        )).Trim().Split(',')
        Assert-True ($values.Count -eq 5) "pod resource probe returned an invalid payload for $($pod.metadata.name)"
        $scope = "pod/$($pod.metadata.name)"
        $measurements = @{
            process_rss_bytes = [double]$values[0]
            process_tasks = [double]$values[1]
            process_threads = [double]$values[2]
            process_open_fds = [double]$values[3]
            process_memory_limit_bytes = [double]$values[4]
        }
        foreach ($entry in $measurements.GetEnumerator()) {
            Write-Ndjson -Path $OutputPath -Record @{
                timestamp = $Timestamp
                metric = $entry.Key
                scope = $scope
                value = $entry.Value
            }
        }
    }
}

Invoke-Native python @($Analyzer, "--policy", $PolicyPath, "validate-policy") | Write-Output
if ($Mode -eq "Validate") {
    Write-Output "MESSAGE_PATH_SOAK_RUNNER_OK full_observation_seconds=21600"
    exit 0
}

foreach ($command in @("python", "kubectl")) {
    Assert-True ($null -ne (Get-Command $command -ErrorAction SilentlyContinue)) "required command is unavailable: $command"
}
Assert-True (-not [string]::IsNullOrWhiteSpace($PrometheusUrl)) "PrometheusUrl is required"
Assert-True (-not [string]::IsNullOrWhiteSpace($ReleaseIdentity)) "ReleaseIdentity is required"
$identityPath = Resolve-RepositoryPath $ReleaseIdentity
Assert-True (Test-Path -LiteralPath $identityPath -PathType Leaf) "release identity file is missing"

$policy = Get-Content -Raw -LiteralPath $PolicyPath | ConvertFrom-Json
$profileName = $Mode.ToLowerInvariant()
$profile = $policy.profiles.$profileName
if ($Mode -eq "Full") {
    Assert-True (-not [string]::IsNullOrWhiteSpace($WorkloadSummary)) "Full mode requires WorkloadSummary"
}

$runId = "message-path-soak-$profileName-$([DateTimeOffset]::UtcNow.ToString('yyyyMMddTHHmmssZ'))"
$runDirectory = Join-Path (Resolve-RepositoryPath $EvidenceRoot) $runId
Assert-True (-not (Test-Path -LiteralPath $runDirectory)) "evidence directory already exists: $runDirectory"
New-Item -ItemType Directory -Path $runDirectory | Out-Null
$samplesPath = Join-Path $runDirectory "raw-samples.ndjson"
$reportPath = Join-Path $runDirectory "soak-report.json"
$boundIdentityPath = Join-Path $runDirectory "release-identity.json"
Copy-Item -LiteralPath $identityPath -Destination $boundIdentityPath
$boundWorkloadPath = $null
$resolvedWorkload = $null
if (-not [string]::IsNullOrWhiteSpace($WorkloadSummary)) {
    $resolvedWorkload = Resolve-RepositoryPath $WorkloadSummary
    Assert-True (Test-Path -LiteralPath $resolvedWorkload -PathType Leaf) "workload summary file is missing"
    $boundWorkloadPath = Join-Path $runDirectory "workload-summary.json"
}

$ready = Invoke-RestMethod -Method Get -Uri ($PrometheusUrl.TrimEnd('/') + "/-/ready") -TimeoutSec 10
Assert-True (([string]$ready).Trim() -eq "Prometheus Server is Ready.") "Prometheus is not ready"
$start = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$observationEnd = $start + [int]$profile.warmup_seconds + [int]$profile.observation_seconds
$end = $observationEnd + [int]$profile.cooldown_seconds
$driverStopped = $false

while ($true) {
    $timestamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    if (-not $driverStopped -and $timestamp -ge $observationEnd) {
        if (-not [string]::IsNullOrWhiteSpace($LoadDriverDeployment)) {
            Invoke-Native kubectl @(
                "-n", $Namespace, "scale", "deployment/$LoadDriverDeployment", "--replicas=0"
            ) | Out-Null
        }
        $driverStopped = $true
    }
    Collect-PodSamples -Timestamp $timestamp -OutputPath $samplesPath
    Collect-PrometheusSamples -Timestamp $timestamp -OutputPath $samplesPath
    if ($timestamp -ge $end) { break }
    $sleepSeconds = [Math]::Min([int]$profile.sample_interval_seconds, [Math]::Max(1, $end - $timestamp))
    Start-Sleep -Seconds $sleepSeconds
}

# Bind the final workload counters after the observation and cooldown windows.
# The long-running probe may update its summary until it is scaled down.
if ($null -ne $boundWorkloadPath) {
    Assert-True (Test-Path -LiteralPath $resolvedWorkload -PathType Leaf) "final workload summary file is missing"
    Copy-Item -LiteralPath $resolvedWorkload -Destination $boundWorkloadPath
}

$arguments = @(
    $Analyzer, "--policy", $PolicyPath, "analyze", "--profile", $profileName,
    "--samples", $samplesPath, "--identity", $boundIdentityPath, "--output", $reportPath
)
if ($null -ne $boundWorkloadPath) {
    $arguments += @("--workload-summary", $boundWorkloadPath)
}
Invoke-Native python $arguments | Write-Output
Invoke-Native python @($Analyzer, "--policy", $PolicyPath, "validate-report", "--report", $reportPath) | Write-Output
Write-Output "MESSAGE_PATH_SOAK_COMPLETE evidence=$runDirectory"

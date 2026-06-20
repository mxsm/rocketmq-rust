[CmdletBinding()]
param(
    [ValidateSet("before", "after", "prototype")]
    [string]$Mode = "before",
    [string]$OutputDirectory = "target/runtime-baseline",
    [int]$SampleProcessId = 0,
    [int]$SampleMilliseconds = 5000,
    [int]$SampleIntervalMilliseconds = 200
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$modeRoot = Join-Path (Join-Path $workspaceRoot $OutputDirectory) $Mode

if (-not (Test-Path $modeRoot)) {
    New-Item -ItemType Directory -Force -Path $modeRoot | Out-Null
}

function Write-JsonArtifact {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][hashtable]$Metrics
    )

    $payload = [ordered]@{
        generated_at = (Get-Date -Format o)
        mode = $Mode
        source = "scripts/bench-runtime-model.ps1"
        metrics = $Metrics
    }

    $path = Join-Path $modeRoot $Name
    $payload | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 $path
    return $path
}

function Get-ThreadSampleSummary {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path $Path)) {
        return [ordered]@{
            sample_count = 0
            min_threads = $null
            max_threads = $null
            avg_threads = $null
        }
    }

    $samples = @(Import-Csv $Path | ForEach-Object { [int]$_.threads })
    if ($samples.Count -eq 0) {
        return [ordered]@{
            sample_count = 0
            min_threads = $null
            max_threads = $null
            avg_threads = $null
        }
    }

    $stats = $samples | Measure-Object -Minimum -Maximum -Average
    return [ordered]@{
        sample_count = $samples.Count
        min_threads = [int]$stats.Minimum
        max_threads = [int]$stats.Maximum
        avg_threads = [math]::Round([double]$stats.Average, 2)
    }
}

function Measure-Threads {
    param([Parameter(Mandatory = $true)][int]$Pid)

    $path = Join-Path $modeRoot "thread-sampling.csv"
    "ts,threads" | Out-File -Encoding utf8 $path
    $deadline = [DateTimeOffset]::UtcNow.AddMilliseconds($SampleMilliseconds)

    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        try {
            $process = Get-Process -Id $Pid -ErrorAction Stop
            $ts = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
            "$ts,$($process.Threads.Count)" | Out-File -Append -Encoding utf8 $path
            Start-Sleep -Milliseconds $SampleIntervalMilliseconds
        }
        catch {
            break
        }
    }
}

$threadSamplingPath = Join-Path $modeRoot "thread-sampling.csv"
if ($SampleProcessId -gt 0) {
    Measure-Threads -Pid $SampleProcessId
}
else {
    "ts,threads" | Out-File -Encoding utf8 $threadSamplingPath
}

$jsonArtifacts = @()

$jsonArtifacts += Write-JsonArtifact -Name "task-group-lifecycle.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-runtime --bench task_group_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/task-group-lifecycle-report.json"
    required_metrics = @("task_count", "elapsed_ms", "completed", "cancelled", "leaked")
}

$jsonArtifacts += Write-JsonArtifact -Name "client-runtime-spawn.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-client-rust --bench client_runtime_spawn_benchmark"
    artifact = "target/runtime-baseline/prototype/client-runtime-spawn-report.json"
    required_metrics = @("elapsed_us", "runtime_created_delta", "runtime_reused_delta", "active_tasks", "active_leases")
}

$jsonArtifacts += Write-JsonArtifact -Name "namesrv-shutdown.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-namesrv --bench namesrv_shutdown_drain_bench"
    artifact = "target/runtime-baseline/prototype/namesrv-shutdown-drain-report.json"
    required_metrics = @("elapsed_us", "healthy", "scheduled", "server", "root")
}

$jsonArtifacts += Write-JsonArtifact -Name "broker-runtime-lifecycle.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-broker --bench broker_runtime_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/broker-runtime-lifecycle-report.json"
    required_metrics = @("scheduled_shutdown_elapsed_us", "basic_shutdown_elapsed_us", "scheduled_task_drop_count", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "remoting-connection-lifecycle.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-remoting --bench remoting_connection_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/remoting-connection-lifecycle-report.json"
    required_metrics = @("elapsed_us", "healthy", "connection_count", "shutdown_report")
}

$jsonArtifacts += Write-JsonArtifact -Name "scheduler.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-runtime --bench scheduled_task_group_bench"
    artifact = "target/runtime-baseline/prototype/scheduled-task-group-report.json"
    required_metrics = @("run_count", "skip_count", "overlap_count", "shutdown_wait_ms")
}

$jsonArtifacts += Write-JsonArtifact -Name "blocking-executor.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-runtime --bench blocking_executor_bench"
    artifact = "target/runtime-baseline/prototype/blocking-executor-report.json"
    required_metrics = @("max_active", "blocking_still_running", "timed_out_still_running", "late_exit_cleaned")
}

$jsonArtifacts += Write-JsonArtifact -Name "store-blocking.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-store --bench store_blocking_executor_bench"
    artifact = "target/runtime-baseline/prototype/store-blocking-executor-report.json"
    required_metrics = @("elapsed_us", "queue_wait_p95_us", "queue_wait_p99_us", "max_active", "blocking_still_running", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "auth-sync-bridge.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-auth --bench auth_sync_bridge_bench"
    artifact = "target/runtime-baseline/prototype/auth-sync-bridge-report.json"
    required_metrics = @("sync_bridge_calls", "multi_thread_block_in_place", "current_thread_handoffs", "shared_runtime_created", "shared_runtime_reused", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "proxy-housekeeping.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-proxy --bench proxy_housekeeping_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/proxy-housekeeping-lifecycle-report.json"
    required_metrics = @("shutdown_elapsed_us", "task_count_before_shutdown", "task_count_after_shutdown", "healthy", "shutdown_report")
}

$manifest = [ordered]@{
    generated_at = (Get-Date -Format o)
    mode = $Mode
    source = "scripts/bench-runtime-model.ps1"
    output_directory = $modeRoot
    thread_sampling = [ordered]@{
        file = $threadSamplingPath
        process_id = $SampleProcessId
        duration_ms = $SampleMilliseconds
        interval_ms = $SampleIntervalMilliseconds
        summary = Get-ThreadSampleSummary -Path $threadSamplingPath
    }
    artifacts = @(
        @($jsonArtifacts) | ForEach-Object {
            [ordered]@{
                file = $_
                name = Split-Path $_ -Leaf
            }
        }
    )
}

$manifest | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 (Join-Path $modeRoot "manifest.json")

Write-Host "Runtime model baseline artifacts written to $modeRoot"

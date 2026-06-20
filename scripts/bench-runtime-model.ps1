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

    $payload | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 (Join-Path $modeRoot $Name)
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

if ($SampleProcessId -gt 0) {
    Measure-Threads -Pid $SampleProcessId
}
else {
    "ts,threads" | Out-File -Encoding utf8 (Join-Path $modeRoot "thread-sampling.csv")
}

Write-JsonArtifact -Name "client-runtime-spawn.json" -Metrics @{
    status = "pending-benchmark"
    expected_bench = "cargo bench -p rocketmq-client-rust --bench client_runtime_spawn_benchmark"
    required_metrics = @("total_elapsed_ms", "spawn_to_complete_p95_ms", "peak_thread_count", "runtime_creation_count")
}

Write-JsonArtifact -Name "namesrv-shutdown.json" -Metrics @{
    status = "pending-benchmark"
    expected_bench = "cargo bench -p rocketmq-namesrv --bench namesrv_shutdown_drain_bench"
    required_metrics = @("shutdown_p95_ms", "tracked_task_after_shutdown", "unregister_pending_count")
}

Write-JsonArtifact -Name "broker-runtime-lifecycle.json" -Metrics @{
    status = "pending-benchmark"
    expected_bench = "cargo bench -p rocketmq-broker --bench broker_runtime_lifecycle_bench"
    required_metrics = @("idle_thread_count", "shutdown_p99_ms", "task_leaked", "long_polling_wakeup_percent")
}

Write-JsonArtifact -Name "remoting-connection-lifecycle.json" -Metrics @{
    status = "pending-benchmark"
    expected_bench = "cargo bench -p rocketmq-remoting --bench remoting_connection_lifecycle_bench"
    required_metrics = @("connection_handler_alive", "channel_send_alive", "shutdown_drain_ms")
}

Write-JsonArtifact -Name "scheduler.json" -Metrics @{
    status = "pending-benchmark"
    expected_bench = "cargo bench -p rocketmq --bench scheduled_task_group_bench"
    required_metrics = @("run_count", "skip_count", "overlap_count", "shutdown_wait_ms")
}

Write-JsonArtifact -Name "store-blocking.json" -Metrics @{
    status = "pending-benchmark"
    expected_bench = "cargo bench -p rocketmq-store --bench store_blocking_executor_bench"
    required_metrics = @("blocking_queue_wait_p95_ms", "blocking_duration_p95_ms", "blocking_still_running")
}

Write-Host "Runtime model baseline artifacts written to $modeRoot"

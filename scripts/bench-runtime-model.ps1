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

    $crateName = "unknown"
    if ($Metrics.ContainsKey("expected_bench") -and $Metrics.expected_bench -match "-p\s+([^\s]+)") {
        $crateName = $Matches[1]
    }
    $scenarioName = [System.IO.Path]::GetFileNameWithoutExtension($Name)
    $stableMetrics = [ordered]@{
        thread_count = 0
        task_count = 0
        shutdown_latency_us = 0
        timed_out_tasks = 0
        fallback_runtime_count = 0
        blocking_still_running_count = 0
    }
    foreach ($key in $Metrics.Keys) {
        $stableMetrics[$key] = $Metrics[$key]
    }

    $payload = [ordered]@{
        generated_at = (Get-Date -Format o)
        mode = $Mode
        crate = $crateName
        scenario = $scenarioName
        source = "scripts/bench-runtime-model.ps1"
        metrics = $stableMetrics
    }

    $path = Join-Path $modeRoot $Name
    $payload | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 $path
    return $path
}

function Assert-GeneratedArtifactsExist {
    param([Parameter(Mandatory = $true)][array]$Paths)

    $missing = @($Paths | Where-Object { -not (Test-Path $_) })
    if ($missing.Count -gt 0) {
        throw "Missing generated runtime model artifact(s): $($missing -join ', ')"
    }
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

$jsonArtifacts += Write-JsonArtifact -Name "broker-client-housekeeping.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-broker --bench broker_client_housekeeping_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/broker-client-housekeeping-lifecycle-report.json"
    required_metrics = @("task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "shutdown_report", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "broker-topic-queue-mapping-clean.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-broker --bench broker_topic_queue_mapping_clean_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/broker-topic-queue-mapping-clean-lifecycle-report.json"
    required_metrics = @("task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "shutdown_report", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "broker-fast-failure.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-broker --bench broker_fast_failure_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/broker-fast-failure-lifecycle-report.json"
    required_metrics = @("task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "cleaned_response_received", "shutdown_elapsed_us", "shutdown_report", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "remoting-connection-lifecycle.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-remoting --bench remoting_connection_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/remoting-connection-lifecycle-report.json"
    required_metrics = @("elapsed_us", "healthy", "connection_count", "shutdown_report")
}

$jsonArtifacts += Write-JsonArtifact -Name "remoting-connection-pool-cleanup.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-remoting --bench remoting_connection_pool_cleanup_bench"
    artifact = "target/runtime-baseline/prototype/remoting-connection-pool-cleanup-report.json"
    required_metrics = @("task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "shutdown_report", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "scheduler.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-runtime --bench scheduled_task_group_bench"
    artifact = "target/runtime-baseline/prototype/scheduled-task-group-report.json"
    required_metrics = @("run_count", "skip_count", "overlap_count", "shutdown_wait_ms")
}

$jsonArtifacts += Write-JsonArtifact -Name "rocketmq-scheduler-lifecycle.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-rust --bench scheduler_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/rocketmq-scheduler-lifecycle-report.json"
    required_metrics = @("task_count_before_shutdown", "task_count_after_shutdown", "shutdown_elapsed_us", "cancelled", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "rocketmq-service-manager-lifecycle.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-rust --bench service_manager_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/rocketmq-service-manager-lifecycle-report.json"
    required_metrics = @("task_count_before_shutdown", "task_count_after_shutdown", "task_group_count_before_shutdown", "task_group_count_after_shutdown", "task_group_completed", "task_group_cancelled", "shutdown_elapsed_us", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "rocketmq-scheduled-task-manager-lifecycle.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-rust --bench scheduler_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/rocketmq-scheduled-task-manager-lifecycle-report.json"
    required_metrics = @("task_count_before_shutdown", "driver_task_count_before_shutdown", "driver_task_count_after_shutdown", "shutdown_elapsed_us", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "rocketmq-task-executor-lifecycle.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-rust --bench scheduler_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/rocketmq-task-executor-lifecycle-report.json"
    required_metrics = @("running_task_count_before_shutdown", "task_group_count_before_shutdown", "task_group_count_after_shutdown", "cancelled_by_shutdown", "shutdown_elapsed_us", "healthy")
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

$jsonArtifacts += Write-JsonArtifact -Name "store-kv-compaction.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-store --bench store_kv_compaction_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/store-kv-compaction-lifecycle-report.json"
    required_metrics = @("compacted", "task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "shutdown_report", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "store-rocksdb-maintenance.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-store --features rocksdb_store --bench store_rocksdb_maintenance_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/store-rocksdb-maintenance-lifecycle-report.json"
    required_metrics = @("task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "shutdown_report", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "store-local-file-scheduled.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-store --bench store_local_file_scheduled_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/store-local-file-scheduled-lifecycle-report.json"
    required_metrics = @("task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "store-stats-service.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-store --bench store_stats_service_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/store-stats-service-lifecycle-report.json"
    required_metrics = @("snapshot_count", "task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "shutdown_report", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "store-timer-scheduler.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-store --bench store_timer_scheduler_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/store-timer-scheduler-lifecycle-report.json"
    required_metrics = @("task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "shutdown_report", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "common-executor.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-common --bench common_executor_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/common-executor-lifecycle-report.json"
    required_metrics = @("task_count", "completed", "spawn_wait_elapsed_us", "shutdown_elapsed_us", "runs", "max_active", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "common-completable-future.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-common --bench common_completable_future_bench"
    artifact = "target/runtime-baseline/prototype/common-completable-future-report.json"
    required_metrics = @("future_count", "outside_runtime_completed", "inside_runtime_completed", "outside_runtime_elapsed_us", "inside_runtime_elapsed_us", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "common-service-thread.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-common --bench common_service_thread_bench"
    artifact = "target/runtime-baseline/prototype/common-service-thread-report.json"
    required_metrics = @("service_count", "started", "completed", "joined_shutdown_elapsed_us", "timeout_shutdown_elapsed_us", "timed_out_thread_finished_after_release", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "common-stats-scheduler.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-common --bench common_stats_scheduler_bench"
    artifact = "target/runtime-baseline/prototype/common-stats-scheduler-report.json"
    required_metrics = @("item_count", "moment_item_shutdown_elapsed_us", "moment_set_shutdown_elapsed_us", "statistics_manager_shutdown_elapsed_us", "statistics_manager_inc_success", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "auth-sync-bridge.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-auth --bench auth_sync_bridge_bench"
    artifact = "target/runtime-baseline/prototype/auth-sync-bridge-report.json"
    required_metrics = @("sync_bridge_calls", "multi_thread_block_in_place", "current_thread_handoffs", "shared_runtime_created", "shared_runtime_reused", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "auth-acl-watcher.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-auth --bench auth_acl_watcher_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/auth-acl-watcher-lifecycle-report.json"
    required_metrics = @("scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "reload_success", "shutdown_elapsed_us", "shutdown_report", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "proxy-housekeeping.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-proxy --bench proxy_housekeeping_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/proxy-housekeeping-lifecycle-report.json"
    required_metrics = @("shutdown_elapsed_us", "task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "healthy", "report", "housekeeping_report")
}

$jsonArtifacts += Write-JsonArtifact -Name "observability-prometheus.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-observability --features prometheus --bench observability_prometheus_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/observability-prometheus-lifecycle-report.json"
    required_metrics = @("response_status_ok", "response_contains_metric", "shutdown_elapsed_us", "healthy", "shutdown_report")
}

$jsonArtifacts += Write-JsonArtifact -Name "tieredstore-dispatcher.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-tieredstore --bench tieredstore_dispatcher_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/tieredstore-dispatcher-lifecycle-report.json"
    required_metrics = @("request_count", "task_count_before_shutdown", "task_count_after_shutdown", "last_message_read", "healthy", "shutdown_report")
}

$jsonArtifacts += Write-JsonArtifact -Name "tieredstore-cleanup.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-tieredstore --bench tieredstore_cleanup_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/tieredstore-cleanup-lifecycle-report.json"
    required_metrics = @("task_count_before_shutdown", "task_count_after_shutdown", "shutdown_elapsed_us", "cleanup_completed", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "healthy", "shutdown_report")
}

$jsonArtifacts += Write-JsonArtifact -Name "controller-heartbeat.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-controller --bench controller_heartbeat_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/controller-heartbeat-lifecycle-report.json"
    required_metrics = @("task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "healthy", "shutdown_report")
}

$jsonArtifacts += Write-JsonArtifact -Name "controller-broker-metadata.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-controller --bench controller_broker_metadata_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/controller-broker-metadata-lifecycle-report.json"
    required_metrics = @("expired_broker_removed", "task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "healthy", "shutdown_report")
}

$jsonArtifacts += Write-JsonArtifact -Name "controller-leadership-watch.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-controller --bench controller_leadership_watch_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/controller-leadership-watch-lifecycle-report.json"
    required_metrics = @("scheduling_enabled", "task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "healthy")
}

$jsonArtifacts += Write-JsonArtifact -Name "controller-openraft-scan.json" -Metrics @{
    status = "implemented-benchmark"
    expected_bench = "cargo bench -p rocketmq-controller --bench controller_openraft_scan_lifecycle_bench"
    artifact = "target/runtime-baseline/prototype/controller-openraft-scan-lifecycle-report.json"
    required_metrics = @("became_leader", "task_count_before_shutdown", "task_count_after_shutdown", "scheduled_runs", "scheduled_skips", "scheduled_overlaps", "scheduled_failures", "shutdown_elapsed_us", "healthy")
}

$manifest = [ordered]@{
    generated_at = (Get-Date -Format o)
    mode = $Mode
    crate = "workspace"
    scenario = "runtime-model-baseline-manifest"
    source = "scripts/bench-runtime-model.ps1"
    output_directory = $modeRoot
    full_benchmark_command = "Run the expected_bench commands listed in artifacts, then rerun this script and compare target/runtime-baseline outputs."
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

Assert-GeneratedArtifactsExist -Paths $jsonArtifacts
$manifest | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 (Join-Path $modeRoot "manifest.json")
Assert-GeneratedArtifactsExist -Paths @((Join-Path $modeRoot "manifest.json"))

Write-Host "Runtime model baseline artifacts written to $modeRoot"

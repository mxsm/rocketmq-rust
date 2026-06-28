[CmdletBinding()]
param(
    [ValidateSet("baseline", "current", "prototype")]
    [string]$Mode = "current",
    [string]$OutputDirectory = "target/client-perf",
    [string]$OutputFile = "",
    [string]$MarkdownFile = "",
    [string[]]$Bench = @(),
    [switch]$Quick,
    [switch]$CompileOnly,
    [switch]$SkipBenchmarkRun,
    [string]$CriterionArgs = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$outputRoot = Join-Path $workspaceRoot $OutputDirectory
$logRoot = Join-Path $outputRoot "logs"
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"

if ([string]::IsNullOrWhiteSpace($OutputFile)) {
    $OutputFile = Join-Path $outputRoot "$Mode-$timestamp.json"
}
elseif (-not [System.IO.Path]::IsPathRooted($OutputFile)) {
    $OutputFile = Join-Path $workspaceRoot $OutputFile
}

if ([string]::IsNullOrWhiteSpace($MarkdownFile)) {
    $MarkdownFile = [System.IO.Path]::ChangeExtension($OutputFile, ".md")
}
elseif (-not [System.IO.Path]::IsPathRooted($MarkdownFile)) {
    $MarkdownFile = Join-Path $workspaceRoot $MarkdownFile
}

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
New-Item -ItemType Directory -Force -Path $logRoot | Out-Null

function Get-GitValue {
    param([Parameter(Mandatory = $true)][string]$Arguments)

    $output = & git -C $workspaceRoot $Arguments.Split(" ") 2>$null
    if ($LASTEXITCODE -ne 0) {
        return ""
    }
    return ($output -join "`n").Trim()
}

function Get-CommandOutput {
    param(
        [Parameter(Mandatory = $true)][string]$FileName,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    $output = & $FileName @Arguments 2>$null
    if ($LASTEXITCODE -ne 0) {
        return ""
    }
    return ($output -join "`n").Trim()
}

function Split-CommandArguments {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return @()
    }

    return @(
        [regex]::Matches($Value, '("[^"]*"|''[^'']*''|\S+)') |
            ForEach-Object {
                $item = $_.Value
                if (($item.StartsWith('"') -and $item.EndsWith('"')) -or
                    ($item.StartsWith("'") -and $item.EndsWith("'"))) {
                    $item.Substring(1, $item.Length - 2)
                }
                else {
                    $item
                }
            }
    )
}

function Join-NativeArguments {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    return (($Arguments | ForEach-Object {
            if ($_ -match '\s') {
                '"' + $_.Replace('"', '\"') + '"'
            }
            else {
                $_
            }
        }) -join " ")
}

function Format-CommandLine {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    return "cargo " + (Join-NativeArguments -Arguments $Arguments)
}

function Invoke-CargoProcess {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = "cargo"
    $startInfo.Arguments = Join-NativeArguments -Arguments $Arguments
    $startInfo.WorkingDirectory = $workspaceRoot
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.CreateNoWindow = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()

    $combined = @()
    if (-not [string]::IsNullOrWhiteSpace($stdout)) {
        $combined += $stdout.TrimEnd()
    }
    if (-not [string]::IsNullOrWhiteSpace($stderr)) {
        $combined += $stderr.TrimEnd()
    }

    return [ordered]@{
        exit_code = $process.ExitCode
        output = ($combined -join "`n")
    }
}

function Get-RelativePath {
    param(
        [Parameter(Mandatory = $true)][string]$BasePath,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $baseFullPath = (Resolve-Path $BasePath).Path.TrimEnd('\', '/')
    $pathFullPath = (Resolve-Path $Path).Path
    $baseUri = [Uri]($baseFullPath + [System.IO.Path]::DirectorySeparatorChar)
    $pathUri = [Uri]$pathFullPath
    return [Uri]::UnescapeDataString($baseUri.MakeRelativeUri($pathUri).ToString()).Replace('\', '/')
}

function Get-BenchmarkField {
    param(
        [Parameter(Mandatory = $true)]$Benchmark,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $value = $Benchmark
    while ($value -is [System.Array] -and $value.Count -eq 1) {
        $value = $value[0]
    }

    if ($value -is [hashtable]) {
        return $value[$Name]
    }

    if ($value -is [string] -and $value.Contains("|")) {
        $parts = $value.Split("|", 4)
        $indexByName = @{
            Name = 0
            Package = 1
            Bench = 2
            Purpose = 3
        }
        if ($indexByName.ContainsKey($Name) -and $parts.Count -gt $indexByName[$Name]) {
            return $parts[$indexByName[$Name]]
        }
    }

    if ($value.PSObject.Properties.Name -contains $Name) {
        return $value.PSObject.Properties[$Name].Value
    }

    $typeName = if ($null -eq $value) { "<null>" } else { $value.GetType().FullName }
    $propertyNames = if ($null -eq $value) { "" } else { ($value.PSObject.Properties.Name -join ",") }
    throw "Benchmark entry is missing field '$Name'. Type=$typeName Properties=$propertyNames"
}

function Invoke-BenchmarkCommand {
    param(
        [Parameter(Mandatory = $true)]$Benchmark,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    $name = Get-BenchmarkField -Benchmark $Benchmark -Name "Name"
    $logPath = Join-Path $logRoot "$name-$timestamp.txt"
    $commandLine = Format-CommandLine -Arguments $Arguments
    $startedAt = Get-Date
    $exitCode = 0
    $status = "passed"

    Write-Host "==> $name"
    Write-Host "    $commandLine"

    if ($SkipBenchmarkRun) {
        "Skipped benchmark command: $commandLine" | Set-Content -Encoding utf8 -Path $logPath
        $status = "skipped"
    }
    else {
        $processResult = Invoke-CargoProcess -Arguments $Arguments
        $exitCode = [int]$processResult.exit_code
        $processResult.output | Tee-Object -FilePath $logPath | Out-Host
        if ($exitCode -ne 0) {
            $status = "failed"
        }
    }

    $endedAt = Get-Date
    $durationMs = [math]::Round(($endedAt - $startedAt).TotalMilliseconds, 2)
    $result = [ordered]@{
        name = $name
        package = Get-BenchmarkField -Benchmark $Benchmark -Name "Package"
        bench = Get-BenchmarkField -Benchmark $Benchmark -Name "Bench"
        purpose = Get-BenchmarkField -Benchmark $Benchmark -Name "Purpose"
        command = $commandLine
        log = (Get-RelativePath -BasePath $workspaceRoot -Path $logPath)
        started_at = $startedAt.ToUniversalTime().ToString("o")
        duration_ms = $durationMs
        exit_code = $exitCode
        status = $status
    }

    if ($exitCode -ne 0) {
        throw "$name failed with exit code $exitCode. See $logPath"
    }

    return $result
}

function Get-CriterionEstimates {
    param([Parameter(Mandatory = $true)][datetime]$Since)

    $criterionRoot = Join-Path $workspaceRoot "target/criterion"
    if (-not (Test-Path $criterionRoot)) {
        return @()
    }

    $results = @()
    $threshold = $Since.AddSeconds(-5)
    $estimateFiles = Get-ChildItem -Path $criterionRoot -Recurse -Filter "estimates.json" |
        Where-Object { $_.FullName -match "[\\/]+new[\\/]+estimates\.json$" -and $_.LastWriteTime -ge $threshold } |
        Sort-Object FullName

    foreach ($file in $estimateFiles) {
        $json = Get-Content -Path $file.FullName -Raw | ConvertFrom-Json
        $scenarioDir = $file.Directory.Parent.FullName
        $id = Get-RelativePath -BasePath $criterionRoot -Path $scenarioDir
        $results += [ordered]@{
            id = $id
            unit = "ns"
            source = Get-RelativePath -BasePath $workspaceRoot -Path $file.FullName
            mean_point_estimate = [double]$json.mean.point_estimate
            mean_lower_bound = [double]$json.mean.confidence_interval.lower_bound
            mean_upper_bound = [double]$json.mean.confidence_interval.upper_bound
            median_point_estimate = [double]$json.median.point_estimate
            median_lower_bound = [double]$json.median.confidence_interval.lower_bound
            median_upper_bound = [double]$json.median.confidence_interval.upper_bound
            slope_point_estimate = [double]$json.slope.point_estimate
        }
    }

    return $results
}

function Get-PrototypeArtifacts {
    param([Parameter(Mandatory = $true)][datetime]$Since)

    $artifactRoot = Join-Path $workspaceRoot "target/runtime-baseline/prototype"
    if (-not (Test-Path $artifactRoot)) {
        return @()
    }

    $threshold = $Since.AddSeconds(-5)
    $artifacts = @()
    $artifactFiles = Get-ChildItem -Path $artifactRoot -Filter "*.json" |
        Where-Object { $_.LastWriteTime -ge $threshold } |
        Sort-Object FullName

    foreach ($file in $artifactFiles) {
        $json = Get-Content -Path $file.FullName -Raw | ConvertFrom-Json
        $metrics = if ($json.PSObject.Properties.Name -contains "metrics") { $json.metrics } else { $json }
        $artifacts += [ordered]@{
            path = Get-RelativePath -BasePath $workspaceRoot -Path $file.FullName
            scenario = if ($json.PSObject.Properties.Name -contains "scenario") { $json.scenario } else { [System.IO.Path]::GetFileNameWithoutExtension($file.Name) }
            source = if ($json.PSObject.Properties.Name -contains "source") { $json.source } else { "benchmark-artifact" }
            metrics = $metrics
        }
    }

    return $artifacts
}

$coreBenches = @(
    "client-hot-path|rocketmq-client-rust|client_hot_path_benchmark|producer route lookup, queue select, batch encode, lite-pull clone"
    "client-send-pipeline|rocketmq-client-rust|client_send_pipeline_benchmark|message construction, request construction, callback dispatch, async retry"
    "client-consume-pipeline|rocketmq-client-rust|client_consume_pipeline_benchmark|ProcessQueue put/take/remove/max-span hot paths"
    "produce-accumulator|rocketmq-client-rust|produce_accumulator_benchmark|batch accumulation throughput and capacity reservation"
    "produce-accumulator-guard|rocketmq-client-rust|produce_accumulator_guard_lifecycle_bench|batch guard lifecycle and shutdown cost"
    "client-runtime-spawn|rocketmq-client-rust|client_runtime_spawn_benchmark|fallback runtime task spawn lifecycle"
    "client-pull-scheduler|rocketmq-client-rust|client_pull_scheduler_benchmark|delayed pull scheduler queue behavior"
    "pull-message-service|rocketmq-client-rust|pull_message_service_lifecycle_bench|pull service task lifecycle"
    "trace-worker|rocketmq-client-rust|trace_worker_lifecycle_bench|trace queue depth and event-driven flush"
    "request-future-holder|rocketmq-client-rust|request_future_holder_lifecycle_bench|request-reply deadline scanning"
    "namesrv-refresh|rocketmq-client-rust|namesrv_refresh_lifecycle_bench|topic route refresh sharding"
    "connection-event-listener|rocketmq-client-rust|connection_event_listener_lifecycle_bench|heartbeat failure broker route index"
)

$requestedBenches = @(
    $Bench |
        ForEach-Object { ([string]$_).Split(",", [System.StringSplitOptions]::RemoveEmptyEntries) } |
        ForEach-Object { ([string]$_).Trim() } |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
)

$selectedBenches = $coreBenches
if ($requestedBenches.Count -gt 0) {
    $knownNames = @($coreBenches | ForEach-Object { Get-BenchmarkField -Benchmark $_ -Name "Name" })
    $knownBenchTargets = @($coreBenches | ForEach-Object { Get-BenchmarkField -Benchmark $_ -Name "Bench" })
    $unknown = @($requestedBenches | Where-Object { $knownNames -notcontains $_ -and $knownBenchTargets -notcontains $_ })
    if ($unknown.Count -gt 0) {
        throw "Unknown client benchmark selector(s): $($unknown -join ', '). Known names: $($knownNames -join ', ')"
    }

    $selectedBenches = @()
    foreach ($coreBench in $coreBenches) {
        $coreName = Get-BenchmarkField -Benchmark $coreBench -Name "Name"
        $coreBenchTarget = Get-BenchmarkField -Benchmark $coreBench -Name "Bench"
        if ($requestedBenches -contains $coreName -or $requestedBenches -contains $coreBenchTarget) {
            $selectedBenches += $coreBench
        }
    }
}

$effectiveCriterionArgs = $CriterionArgs
if ($Quick -and [string]::IsNullOrWhiteSpace($effectiveCriterionArgs)) {
    $effectiveCriterionArgs = "--sample-size 10 --warm-up-time 1 --measurement-time 1 --noplot"
}

$runStartedAt = Get-Date
$commands = @()
Push-Location $workspaceRoot
try {
    foreach ($bench in $selectedBenches) {
        $arguments = @(
            "bench",
            "-p",
            (Get-BenchmarkField -Benchmark $bench -Name "Package"),
            "--bench",
            (Get-BenchmarkField -Benchmark $bench -Name "Bench")
        )
        if ($CompileOnly) {
            $arguments += "--no-run"
        }
        elseif (-not [string]::IsNullOrWhiteSpace($effectiveCriterionArgs)) {
            $arguments += "--"
            $arguments += Split-CommandArguments -Value $effectiveCriterionArgs
        }

        $commands += Invoke-BenchmarkCommand -Benchmark $bench -Arguments $arguments
    }
}
finally {
    Pop-Location
}

$criterionResults = if ($CompileOnly -or $SkipBenchmarkRun) { @() } else { Get-CriterionEstimates -Since $runStartedAt }
$artifactResults = if ($CompileOnly -or $SkipBenchmarkRun) { @() } else { Get-PrototypeArtifacts -Since $runStartedAt }

$payload = [ordered]@{
    schema_version = 1
    source = "scripts/client-perf-baseline.ps1"
    mode = $Mode
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    output_directory = $OutputDirectory.Replace('\', '/')
    git = [ordered]@{
        branch = Get-GitValue -Arguments "rev-parse --abbrev-ref HEAD"
        commit = Get-GitValue -Arguments "rev-parse HEAD"
        short_commit = Get-GitValue -Arguments "rev-parse --short HEAD"
    }
    environment = [ordered]@{
        os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
        architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        processor_count = [Environment]::ProcessorCount
        powershell = $PSVersionTable.PSVersion.ToString()
        rustc = Get-CommandOutput -FileName "rustc" -Arguments @("--version")
        cargo = Get-CommandOutput -FileName "cargo" -Arguments @("--version")
    }
    options = [ordered]@{
        quick = [bool]$Quick
        compile_only = [bool]$CompileOnly
        skip_benchmark_run = [bool]$SkipBenchmarkRun
        criterion_args = $effectiveCriterionArgs
    }
    thresholds = [ordered]@{
        default_max_regression_percent = 5.0
        lower_is_better_unit = "ns"
        lifecycle_failure_metric_patterns = @("healthy=false", "leak", "still_running", "timed_out", "failed", "failure")
    }
    selected_benches = @($selectedBenches | ForEach-Object {
            [ordered]@{
                name = Get-BenchmarkField -Benchmark $_ -Name "Name"
                package = Get-BenchmarkField -Benchmark $_ -Name "Package"
                bench = Get-BenchmarkField -Benchmark $_ -Name "Bench"
                purpose = Get-BenchmarkField -Benchmark $_ -Name "Purpose"
            }
        })
    commands = $commands
    criterion_results = $criterionResults
    artifact_results = $artifactResults
}

$payload | ConvertTo-Json -Depth 24 | Out-File -Encoding utf8 -FilePath $OutputFile

$markdown = @(
    "# Client Performance $Mode Report",
    "",
    "- Generated at: $($payload.generated_at)",
    "- Branch: $($payload.git.branch)",
    "- Commit: $($payload.git.short_commit)",
    "- OS: $($payload.environment.os)",
    "- Processor count: $($payload.environment.processor_count)",
    "- Criterion results: $(@($criterionResults).Count)",
    "- Runtime artifact results: $(@($artifactResults).Count)",
    "",
    "## Commands",
    "",
    "| Benchmark | Status | Duration ms | Log |",
    "|---|---:|---:|---|"
)

foreach ($command in $commands) {
    $markdown += "| $($command.name) | $($command.status) | $($command.duration_ms) | `$($command.log)` |"
}

$markdown += @(
    "",
    "## Gate",
    "",
    "Compare this report with:",
    "",
    '```powershell',
    ".\scripts\client-perf-compare.ps1 -Baseline target\client-perf\baseline.json -Current target\client-perf\current.json",
    '```'
)

$markdown | Set-Content -Encoding utf8 -Path $MarkdownFile

Write-Host "Client performance JSON report: $OutputFile"
Write-Host "Client performance Markdown report: $MarkdownFile"

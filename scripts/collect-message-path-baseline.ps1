[CmdletBinding()]
param(
    [ValidateSet("send", "store", "ha", "consume", "proxy", "all")]
    [string]$Scope = "all",
    [string]$OutputDirectory = "target/message-path-baseline",
    [switch]$Quick,
    [switch]$ListOnly,
    [string]$CriterionArgs = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$targetRoot = [System.IO.Path]::GetFullPath((Join-Path $workspaceRoot "target"))
$requestedOutputRoot = if ([System.IO.Path]::IsPathRooted($OutputDirectory)) {
    [System.IO.Path]::GetFullPath($OutputDirectory)
}
else {
    [System.IO.Path]::GetFullPath((Join-Path $workspaceRoot $OutputDirectory))
}
$targetPrefix = $targetRoot.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar
if (-not $requestedOutputRoot.StartsWith($targetPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "OutputDirectory must resolve below the workspace target directory: $targetRoot"
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmssfff"
$runRoot = Join-Path $requestedOutputRoot $timestamp
$logRoot = Join-Path $runRoot "logs"
$manifestPath = Join-Path $runRoot "manifest.json"
New-Item -ItemType Directory -Force -Path $logRoot | Out-Null

$benchmarks = @(
    [pscustomobject]@{
        Scope = "send"
        Name = "client-send-pipeline"
        Package = "rocketmq-client-rust"
        Target = "client_send_pipeline_benchmark"
        Features = @()
        Purpose = "producer construction, request building, callback dispatch, and retry"
    },
    [pscustomobject]@{
        Scope = "store"
        Name = "mapped-buffer"
        Package = "rocketmq-store"
        Target = "mapped_buffer_bench"
        Features = @()
        Purpose = "mapped-buffer write, read, flush, and concurrency microbenchmarks"
    },
    [pscustomobject]@{
        Scope = "ha"
        Name = "ha-transfer"
        Package = "rocketmq-store"
        Target = "ha_transfer_benchmark"
        Features = @("test-support")
        Purpose = "HA frame planning and transfer microbenchmarks"
    },
    [pscustomobject]@{
        Scope = "consume"
        Name = "client-consume-pipeline"
        Package = "rocketmq-client-rust"
        Target = "client_consume_pipeline_benchmark"
        Features = @("test-support")
        Purpose = "ProcessQueue put, take, remove, and span hot paths"
    },
    [pscustomobject]@{
        Scope = "proxy"
        Name = "cluster-executor"
        Package = "rocketmq-proxy-cluster"
        Target = "cluster_executor"
        Features = @("bench-support")
        Purpose = "same-key and distinct-key proxy execution lanes"
    }
)

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
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$LogPath
    )

    $stdoutPath = "$LogPath.stdout"
    $stderrPath = "$LogPath.stderr"
    try {
        $process = Start-Process `
            -FilePath "cargo" `
            -ArgumentList (Join-NativeArguments -Arguments $Arguments) `
            -WorkingDirectory $workspaceRoot `
            -NoNewWindow `
            -Wait `
            -PassThru `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath

        $combinedOutput = @()
        if (Test-Path -LiteralPath $stdoutPath) {
            $combinedOutput += @(Get-Content -LiteralPath $stdoutPath -Encoding utf8)
        }
        if (Test-Path -LiteralPath $stderrPath) {
            $combinedOutput += @(Get-Content -LiteralPath $stderrPath -Encoding utf8)
        }
        $combinedOutput | Tee-Object -FilePath $LogPath | Out-Host

        return [ordered]@{ ExitCode = $process.ExitCode }
    }
    finally {
        Remove-Item -LiteralPath $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
    }
}

function Get-RelativePath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $workspaceUri = [Uri]($workspaceRoot.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar)
    $pathUri = [Uri]([System.IO.Path]::GetFullPath($Path))
    return [Uri]::UnescapeDataString($workspaceUri.MakeRelativeUri($pathUri).ToString()).Replace('\', '/')
}

function Invoke-Git {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $output = & git -C $workspaceRoot @Arguments 2>$null
    if ($LASTEXITCODE -ne 0) {
        return ""
    }
    return ($output -join "`n").Trim()
}

function Invoke-ToolVersion {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    $output = & $Name @Arguments 2>$null
    if ($LASTEXITCODE -ne 0) {
        return ""
    }
    return ($output -join "`n").Trim()
}

function Get-HostDetails {
    $cpu = ""
    $memoryBytes = $null
    $isWindows = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )

    if ($isWindows) {
        try {
            $processors = @(Get-CimInstance Win32_Processor -ErrorAction Stop)
            $cpu = (($processors | Select-Object -ExpandProperty Name -Unique) -join "; ").Trim()
            $computer = Get-CimInstance Win32_ComputerSystem -ErrorAction Stop
            $memoryBytes = [uint64]$computer.TotalPhysicalMemory
        }
        catch {
            $cpu = $env:PROCESSOR_IDENTIFIER
        }
    }
    else {
        if (Test-Path -LiteralPath "/proc/cpuinfo") {
            $modelLine = Get-Content -LiteralPath "/proc/cpuinfo" | Where-Object { $_ -match '^model name\s*:' } | Select-Object -First 1
            if ($modelLine) {
                $cpu = ($modelLine -split ':', 2)[1].Trim()
            }
        }
        if (Test-Path -LiteralPath "/proc/meminfo") {
            $memoryLine = Get-Content -LiteralPath "/proc/meminfo" | Where-Object { $_ -match '^MemTotal\s*:' } | Select-Object -First 1
            if ($memoryLine -and $memoryLine -match '(\d+)') {
                $memoryBytes = [uint64]$Matches[1] * 1024
            }
        }
    }

    return [ordered]@{
        os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
        architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        cpu = $cpu
        processor_count = [Environment]::ProcessorCount
        memory_bytes = $memoryBytes
        powershell = $PSVersionTable.PSVersion.ToString()
        rustc = Invoke-ToolVersion -Name "rustc" -Arguments @("--version", "--verbose")
        cargo = Invoke-ToolVersion -Name "cargo" -Arguments @("--version")
    }
}

function Get-CriterionMeasurements {
    param([Parameter(Mandatory = $true)][datetime]$Since)

    $criterionRoot = Join-Path $workspaceRoot "target/criterion"
    if (-not (Test-Path -LiteralPath $criterionRoot)) {
        return @()
    }

    $threshold = $Since.AddSeconds(-2)
    $measurements = @()
    $estimateFiles = Get-ChildItem -Path $criterionRoot -Recurse -Filter "estimates.json" |
        Where-Object {
            $_.FullName -match "[\\/]+new[\\/]+estimates\.json$" -and
            $_.LastWriteTime -ge $threshold
        } |
        Sort-Object FullName

    foreach ($estimateFile in $estimateFiles) {
        $estimate = Get-Content -LiteralPath $estimateFile.FullName -Raw | ConvertFrom-Json
        $benchmarkPath = Join-Path $estimateFile.Directory.FullName "benchmark.json"
        $benchmark = if (Test-Path -LiteralPath $benchmarkPath) {
            Get-Content -LiteralPath $benchmarkPath -Raw | ConvertFrom-Json
        }
        else {
            $null
        }

        $throughputUnit = $null
        $throughputPerIteration = $null
        $throughputPerSecond = $null
        if ($null -ne $benchmark -and $null -ne $benchmark.throughput) {
            $throughputProperty = @($benchmark.throughput.PSObject.Properties) | Select-Object -First 1
            if ($null -ne $throughputProperty) {
                $throughputUnit = $throughputProperty.Name
                $throughputPerIteration = [double]$throughputProperty.Value
                if ([double]$estimate.median.point_estimate -gt 0) {
                    $throughputPerSecond = $throughputPerIteration * 1e9 / [double]$estimate.median.point_estimate
                }
            }
        }

        $scenarioPath = $estimateFile.Directory.Parent.FullName
        $scenario = if ($null -ne $benchmark) {
            $benchmark.full_id
        }
        else {
            Get-RelativePath -Path $scenarioPath
        }

        $measurements += [ordered]@{
            benchmark_name = $scenario
            payload = $null
            batch = $null
            concurrency = $null
            parameter_source = "criterion benchmark id"
            median = [ordered]@{
                value = [double]$estimate.median.point_estimate
                unit = "ns"
                confidence_level = [double]$estimate.median.confidence_interval.confidence_level
                lower_bound = [double]$estimate.median.confidence_interval.lower_bound
                upper_bound = [double]$estimate.median.confidence_interval.upper_bound
            }
            p95 = $null
            p99 = $null
            quantiles_supported = $false
            throughput = [ordered]@{
                unit = $throughputUnit
                per_iteration = $throughputPerIteration
                median_per_second = $throughputPerSecond
            }
            evidence_type = "micro"
            raw_artifact = Get-RelativePath -Path $estimateFile.FullName
        }
    }

    return @($measurements)
}

$selectedBenchmarks = @($benchmarks | Where-Object { $Scope -eq "all" -or $_.Scope -eq $Scope })
if ($selectedBenchmarks.Count -eq 0) {
    throw "No benchmarks are registered for scope '$Scope'."
}

if (-not $ListOnly) {
    Get-Command cargo -ErrorAction Stop | Out-Null
}

$criterionOptions = @()
if (-not [string]::IsNullOrWhiteSpace($CriterionArgs)) {
    $criterionOptions = @(
        [regex]::Matches($CriterionArgs, '("[^"]*"|''[^'']*''|\S+)') | ForEach-Object {
            $_.Value.Trim('"', "'")
        }
    )
}
elseif ($Quick) {
    $criterionOptions = @("--sample-size", "10", "--warm-up-time", "1", "--measurement-time", "1", "--noplot")
}

$concurrentCompilers = @(Get-Process -Name cargo, rustc -ErrorAction SilentlyContinue)
$runResults = New-Object System.Collections.Generic.List[object]
$failure = $null

foreach ($benchmark in $selectedBenchmarks) {
    $arguments = @("bench", "-p", $benchmark.Package)
    if ($benchmark.Features.Count -gt 0) {
        $arguments += "--features"
        $arguments += ($benchmark.Features -join ",")
    }
    $arguments += @("--bench", $benchmark.Target)
    if ($criterionOptions.Count -gt 0) {
        $arguments += "--"
        $arguments += $criterionOptions
    }

    $commandLine = Format-CommandLine -Arguments $arguments
    $logPath = Join-Path $logRoot "$($benchmark.Name).log"
    Write-Host "[$($benchmark.Scope)] $commandLine"
    $startedAt = Get-Date

    if ($ListOnly) {
        "Listed benchmark command: $commandLine" | Set-Content -LiteralPath $logPath -Encoding utf8
        $processResult = [ordered]@{ ExitCode = 0; Output = "" }
        $status = "listed"
        $measurements = @()
    }
    else {
        $processResult = Invoke-CargoProcess -Arguments $arguments -LogPath $logPath
        $status = if ($processResult.ExitCode -eq 0) { "passed" } else { "failed" }
        $measurements = if ($processResult.ExitCode -eq 0) {
            @(Get-CriterionMeasurements -Since $startedAt)
        }
        else {
            @()
        }
    }

    $endedAt = Get-Date
    $runResults.Add([ordered]@{
            name = $benchmark.Name
            scope = $benchmark.Scope
            package = $benchmark.Package
            benchmark_target = $benchmark.Target
            purpose = $benchmark.Purpose
            payload = $null
            batch = $null
            concurrency = $null
            median = $null
            p95 = $null
            p99 = $null
            throughput = $null
            evidence_type = "micro"
            command = $commandLine
            status = $status
            exit_code = [int]$processResult.ExitCode
            started_at = $startedAt.ToUniversalTime().ToString("o")
            duration_ms = [math]::Round(($endedAt - $startedAt).TotalMilliseconds, 2)
            raw_artifact = Get-RelativePath -Path $logPath
            measurements = $measurements
        }) | Out-Null

    if ($processResult.ExitCode -ne 0) {
        $failure = "Benchmark '$($benchmark.Name)' failed with exit code $($processResult.ExitCode)."
        break
    }
}

$dirtyOutput = Invoke-Git -Arguments @("status", "--porcelain")
$manifest = [ordered]@{
    schema_version = 1
    source = "scripts/collect-message-path-baseline.ps1"
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    evidence_type = "micro"
    git = [ordered]@{
        commit = Invoke-Git -Arguments @("rev-parse", "HEAD")
        branch = Invoke-Git -Arguments @("rev-parse", "--abbrev-ref", "HEAD")
        dirty = -not [string]::IsNullOrWhiteSpace($dirtyOutput)
    }
    environment = Get-HostDetails
    validity = [ordered]@{
        concurrent_compilation_detected_at_start = $concurrentCompilers.Count -gt 0
        valid_for_comparison = $concurrentCompilers.Count -eq 0
        note = if ($concurrentCompilers.Count -gt 0) {
            "Another cargo or rustc process was active when collection started; do not use this run for comparisons."
        }
        else {
            "No concurrent cargo or rustc process was detected when collection started."
        }
    }
    options = [ordered]@{
        scope = $Scope
        quick = [bool]$Quick
        list_only = [bool]$ListOnly
        criterion_args = $criterionOptions
    }
    artifact_root = Get-RelativePath -Path $runRoot
    benchmarks = $runResults.ToArray()
}

$manifest | ConvertTo-Json -Depth 24 | Set-Content -LiteralPath $manifestPath -Encoding utf8
Write-Host "Message-path baseline manifest: $manifestPath"

if ($null -ne $failure) {
    throw "$failure See $manifestPath and the benchmark log for details."
}

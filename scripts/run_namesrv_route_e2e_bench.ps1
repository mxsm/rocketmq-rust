[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("rust", "java")]
    [string]$Server,
    [Parameter(Mandatory = $true)]
    [string]$Profile,
    [string]$Manifest = "rocketmq-namesrv/benches/fixtures/route_workloads.json",
    [string]$Workload = "smoke",
    [string]$JavaRocketmqHome = "",
    [string]$JavaHome = $env:JAVA_HOME,
    [string]$NamesrvHost = "127.0.0.1",
    [int]$NamesrvPort = 0,
    [string]$OutputRoot = "target/namesrv-bench"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$manifestPath = [System.IO.Path]::GetFullPath((Join-Path $workspaceRoot $Manifest))
$outputRootPath = [System.IO.Path]::GetFullPath((Join-Path $workspaceRoot $OutputRoot))
$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runRoot = Join-Path $outputRootPath "$Server-$Profile-$Workload-$runStamp"
$logRoot = Join-Path $runRoot "logs"
$serverProcess = $null
$originalEnvironment = @{}

function New-BenchmarkDirectory {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function Get-FreeTcpPort {
    $listener = [System.Net.Sockets.TcpListener]::new(
        [System.Net.IPAddress]::Parse("127.0.0.1"),
        0
    )
    try {
        $listener.Start()
        return ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
    }
    finally {
        $listener.Stop()
    }
}

function Set-ProcessEnvironment {
    param([hashtable]$Values)
    foreach ($entry in $Values.GetEnumerator()) {
        if (-not $originalEnvironment.ContainsKey($entry.Key)) {
            $originalEnvironment[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, "Process")
        }
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, "Process")
    }
}

function Restore-ProcessEnvironment {
    foreach ($entry in $originalEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, "Process")
    }
}

function Wait-ForTcpPort {
    param(
        [Parameter(Mandatory = $true)][string]$EndpointHost,
        [Parameter(Mandatory = $true)][int]$Port,
        [int]$TimeoutSeconds = 180
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $result = $client.BeginConnect($EndpointHost, $Port, $null, $null)
            if ($result.AsyncWaitHandle.WaitOne(1000) -and $client.Connected) {
                $client.EndConnect($result)
                return
            }
        }
        catch {
        }
        finally {
            $client.Dispose()
        }
        Start-Sleep -Milliseconds 250
    }
    throw "Timed out waiting for NameServer $EndpointHost`:$Port"
}

function Get-ProcessTreeIds {
    param([Parameter(Mandatory = $true)][int]$RootProcessId)
    $result = [System.Collections.Generic.List[int]]::new()
    $pending = [System.Collections.Generic.Queue[int]]::new()
    $pending.Enqueue($RootProcessId)
    while ($pending.Count -gt 0) {
        $processId = $pending.Dequeue()
        if ($result.Contains($processId)) {
            continue
        }
        $result.Add($processId)
        $children = Get-CimInstance Win32_Process -Filter "ParentProcessId = $processId" -ErrorAction SilentlyContinue
        foreach ($child in $children) {
            $pending.Enqueue([int]$child.ProcessId)
        }
    }
    return $result
}

function Get-ProcessTreeSample {
    param([Parameter(Mandatory = $true)][int]$RootProcessId)
    $cpuSeconds = 0.0
    $rssBytes = [int64]0
    foreach ($processId in (Get-ProcessTreeIds -RootProcessId $RootProcessId)) {
        $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
        if ($null -ne $process) {
            $cpuSeconds += $process.CPU
            $rssBytes += $process.WorkingSet64
        }
    }
    return [ordered]@{ cpuSeconds = $cpuSeconds; rssBytes = $rssBytes }
}

function Stop-ProcessTree {
    param([Parameter(Mandatory = $true)][int]$RootProcessId)
    $processIds = @(Get-ProcessTreeIds -RootProcessId $RootProcessId)
    [array]::Reverse($processIds)
    foreach ($processId in $processIds) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-CargoBenchWithSampling {
    param([Parameter(Mandatory = $true)][int]$ObservedProcessId)
    $stdoutPath = Join-Path $logRoot "cargo-bench.stdout.log"
    $stderrPath = Join-Path $logRoot "cargo-bench.stderr.log"
    $before = Get-ProcessTreeSample -RootProcessId $ObservedProcessId
    $peakRssBytes = [int64]$before.rssBytes
    $benchStarted = Get-Date
    $bench = Start-Process `
        -FilePath "cargo" `
        -ArgumentList @("bench", "-p", "rocketmq-namesrv", "--bench", "namesrv_route_e2e_bench", "--", "--noplot") `
        -WorkingDirectory $workspaceRoot `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -WindowStyle Hidden `
        -PassThru
    while (-not $bench.HasExited) {
        $sample = Get-ProcessTreeSample -RootProcessId $ObservedProcessId
        if ([int64]$sample.rssBytes -gt $peakRssBytes) {
            $peakRssBytes = [int64]$sample.rssBytes
        }
        Start-Sleep -Milliseconds 200
        $bench.Refresh()
    }
    $bench.WaitForExit()
    $bench.Refresh()
    $after = Get-ProcessTreeSample -RootProcessId $ObservedProcessId
    $stdout = if (Test-Path -LiteralPath $stdoutPath) { [System.IO.File]::ReadAllText($stdoutPath) } else { "" }
    $stderr = if (Test-Path -LiteralPath $stderrPath) { [System.IO.File]::ReadAllText($stderrPath) } else { "" }
    if (-not [string]::IsNullOrWhiteSpace($stdout)) {
        Write-Host -NoNewline $stdout
    }
    if (-not [string]::IsNullOrWhiteSpace($stderr)) {
        Write-Host -NoNewline $stderr
    }
    $benchmarkArtifact = Join-Path $runRoot "route-benchmark.json"
    if (($null -ne $bench.ExitCode -and $bench.ExitCode -ne 0) -or -not (Test-Path -LiteralPath $benchmarkArtifact)) {
        throw "NameServer route benchmark failed with exit code $($bench.ExitCode)"
    }
    return [ordered]@{
        serverProcessId = $ObservedProcessId
        elapsedMillis = [int64]((Get-Date) - $benchStarted).TotalMilliseconds
        cpuSecondsBefore = [double]$before.cpuSeconds
        cpuSecondsAfter = [double]$after.cpuSeconds
        cpuSecondsDelta = [math]::Max(0.0, [double]$after.cpuSeconds - [double]$before.cpuSeconds)
        rssBytesBefore = [int64]$before.rssBytes
        rssBytesAfter = [int64]$after.rssBytes
        peakSampledRssBytes = $peakRssBytes
        sampleIntervalMillis = 200
    }
}

if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw "Route workload manifest does not exist: $manifestPath"
}
if ($NamesrvPort -eq 0) {
    $NamesrvPort = Get-FreeTcpPort
}
if ($NamesrvPort -lt 1 -or $NamesrvPort -gt 65535) {
    throw "NamesrvPort must be in 1..65535"
}

New-BenchmarkDirectory -Path $runRoot
New-BenchmarkDirectory -Path $logRoot
$namesrvAddr = "$NamesrvHost`:$NamesrvPort"

try {
    if ($Server -eq "rust") {
        & cargo build --release -p rocketmq-namesrv --bin rocketmq-namesrv-rust
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to build the Rust NameServer benchmark binary"
        }
        $isolatedRustNamesrv = Join-Path $runRoot "rocketmq-namesrv-rust.exe"
        Copy-Item `
            -LiteralPath (Join-Path $workspaceRoot "target/release/rocketmq-namesrv-rust.exe") `
            -Destination $isolatedRustNamesrv `
            -Force
        Set-ProcessEnvironment -Values @{ ROCKETMQ_HOME = $runRoot }
        $serverProcess = Start-Process `
            -FilePath $isolatedRustNamesrv `
            -ArgumentList @("--listenPort", $NamesrvPort, "--bindAddress", $NamesrvHost) `
            -WorkingDirectory $workspaceRoot `
            -RedirectStandardOutput (Join-Path $logRoot "rust-namesrv.stdout.log") `
            -RedirectStandardError (Join-Path $logRoot "rust-namesrv.stderr.log") `
            -WindowStyle Hidden `
            -PassThru
    }
    else {
        if ([string]::IsNullOrWhiteSpace($JavaRocketmqHome)) {
            throw "JavaRocketmqHome is required for the Java benchmark server"
        }
        $javaHomePath = [System.IO.Path]::GetFullPath($JavaRocketmqHome)
        $javaBin = Join-Path $javaHomePath "bin"
        if (-not (Test-Path -LiteralPath (Join-Path $javaBin "mqnamesrv.cmd") -PathType Leaf)) {
            throw "JavaRocketmqHome is not a RocketMQ distribution: $javaHomePath"
        }
        if ([string]::IsNullOrWhiteSpace($JavaHome) -or -not (Test-Path -LiteralPath (Join-Path $JavaHome "bin/java.exe"))) {
            throw "JavaHome must point to a JDK containing bin/java.exe"
        }
        $javaConfig = Join-Path $runRoot "java-namesrv.properties"
        $javaConfigText = @"
listenPort=$NamesrvPort
kvConfigPath=$((Join-Path $runRoot 'java-kv.json') -replace '\\','/')
configStorePath=$($javaConfig -replace '\\','/')
"@
        [System.IO.File]::WriteAllText($javaConfig, $javaConfigText, [System.Text.Encoding]::ASCII)
        Set-ProcessEnvironment -Values @{ ROCKETMQ_HOME = $javaHomePath; JAVA_HOME = $JavaHome }
        $serverProcess = Start-Process `
            -FilePath "cmd.exe" `
            -ArgumentList @("/c", (Join-Path $javaBin "mqnamesrv.cmd"), "-c", $javaConfig) `
            -WorkingDirectory $javaBin `
            -RedirectStandardOutput (Join-Path $logRoot "java-namesrv.stdout.log") `
            -RedirectStandardError (Join-Path $logRoot "java-namesrv.stderr.log") `
            -WindowStyle Hidden `
            -PassThru
    }

    Wait-ForTcpPort -EndpointHost $NamesrvHost -Port $NamesrvPort
    Set-ProcessEnvironment -Values @{
        NAMESRV_BENCH_ENDPOINT = $namesrvAddr
        NAMESRV_BENCH_SERVER = $Server
        NAMESRV_BENCH_PROFILE = $Profile
        NAMESRV_BENCH_WORKLOAD = $Workload
        NAMESRV_BENCH_MANIFEST = $manifestPath
        NAMESRV_BENCH_OUTPUT = $runRoot
        NAMESRV_BENCH_JAVA_VERSION = if ($Server -eq "java") { Split-Path $JavaRocketmqHome -Leaf } else { "" }
    }
    $processMetrics = Invoke-CargoBenchWithSampling -ObservedProcessId $serverProcess.Id
    [System.IO.File]::WriteAllText(
        (Join-Path $runRoot "process-metrics.json"),
        ($processMetrics | ConvertTo-Json -Depth 4),
        [System.Text.Encoding]::UTF8
    )
    $metadata = [ordered]@{
        server = $Server
        profile = $Profile
        workload = $Workload
        endpoint = $namesrvAddr
        rustCommit = (& git -C $workspaceRoot rev-parse HEAD).Trim()
        fixtureSha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $manifestPath).Hash.ToLowerInvariant()
        generatedAt = (Get-Date).ToUniversalTime().ToString("o")
        allocationBytesPerOperation = $null
    }
    [System.IO.File]::WriteAllText(
        (Join-Path $runRoot "run-metadata.json"),
        ($metadata | ConvertTo-Json -Depth 4),
        [System.Text.Encoding]::UTF8
    )
    Write-Host "NameServer route benchmark complete. Artifacts: $runRoot"
}
finally {
    Restore-ProcessEnvironment
    if ($null -ne $serverProcess -and -not $serverProcess.HasExited) {
        Stop-ProcessTree -RootProcessId $serverProcess.Id
    }
}

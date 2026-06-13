[CmdletBinding()]
param(
    [string]$OutputDir = "target/client-production-bench",
    [switch]$CompileOnly,
    [switch]$Quick,
    [string]$RustCriterionArgs = "",
    [string[]]$RustBench = @(),
    [string]$JavaRocketMQRoot = "D:\Github\Java\rocketmq",
    [string]$JavaBenchCommand = "",
    [string]$RustBrokerBenchCommand = "",
    [string]$NamesrvAddr = $env:ROCKETMQ_NAMESRV_ADDR,
    [string]$Topic = $(if ([string]::IsNullOrWhiteSpace($env:ROCKETMQ_TEST_TOPIC)) { "TopicTest" } else { $env:ROCKETMQ_TEST_TOPIC }),
    [int]$BrokerBenchMessageCount = 0,
    [int]$BrokerBenchMessageSize = 128,
    [int]$BrokerBenchTimeoutMillis = 30000,
    [ValidateSet("sync", "async", "batch", "lite-pull")]
    [string]$RustBrokerBenchScenario = "sync",
    [double]$MinRustToJavaTpsRatio = 1.0,
    [double]$MaxRustToJavaAverageRtRatio = 1.0,
    [string]$RustBrokerBenchOutput = "",
    [string]$JavaBenchOutput = "",
    [switch]$UseTls,
    [switch]$Acl,
    [string]$AccessKey = $env:ROCKETMQ_ACL_ACCESS_KEY,
    [string]$SecretKey = $env:ROCKETMQ_ACL_SECRET_KEY,
    [string]$SecurityToken = $env:ROCKETMQ_ACL_SECURITY_TOKEN,
    [switch]$CompareOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

function Invoke-LoggedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [string]$Command,
        [Parameter(Mandatory = $true)]
        [string]$WorkingDirectory,
        [Parameter(Mandatory = $true)]
        [string]$OutputFile
    )

    Write-Host "==> $Name"
    Write-Host "    $Command"

    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = "cmd.exe"
    $startInfo.Arguments = "/c $Command 2>&1"
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $false
    $startInfo.CreateNoWindow = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    [void]$process.Start()
    $combined = $process.StandardOutput.ReadToEnd()
    $process.WaitForExit()

    $combined | Tee-Object -FilePath $OutputFile | Out-Host

    if ($process.ExitCode -ne 0) {
        throw "$Name failed with exit code $($process.ExitCode). See $OutputFile"
    }
}

function Get-GitValue {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Repository,
        [Parameter(Mandatory = $true)]
        [string]$Arguments
    )

    if (-not (Test-Path (Join-Path $Repository ".git"))) {
        return ""
    }

    $safeRepository = $Repository.Replace('\', '/')
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = "cmd.exe"
    $startInfo.Arguments = "/c git -c safe.directory=`"$safeRepository`" -C `"$Repository`" $Arguments"
    $startInfo.WorkingDirectory = $Repository
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.CreateNoWindow = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $process.StandardError.ReadToEnd() | Out-Null
    $process.WaitForExit()

    if ($process.ExitCode -ne 0) {
        return ""
    }

    return $stdout.Trim()
}

function Format-CommandArgument {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    return '"' + $Value.Replace('"', '\"') + '"'
}

function New-RustBrokerBenchCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Namesrv,
        [Parameter(Mandatory = $true)]
        [string]$TopicName,
        [Parameter(Mandatory = $true)]
        [int]$Messages
    )

    $command = "cargo run -p rocketmq-client-rust --example client-production-benchmark --release -- " +
        "--namesrv $(Format-CommandArgument $Namesrv) " +
        "--topic $(Format-CommandArgument $TopicName) " +
        "--scenario $RustBrokerBenchScenario " +
        "--message-count $Messages " +
        "--message-size $BrokerBenchMessageSize " +
        "--timeout-ms $BrokerBenchTimeoutMillis"

    if ($UseTls) {
        $command = "$command --tls"
    }
    if ($Acl) {
        if ([string]::IsNullOrWhiteSpace($AccessKey) -or [string]::IsNullOrWhiteSpace($SecretKey)) {
            throw "ACL Rust broker benchmark requires ROCKETMQ_ACL_ACCESS_KEY/ROCKETMQ_ACL_SECRET_KEY or -AccessKey/-SecretKey."
        }
        $command = "$command --acl --access-key $(Format-CommandArgument $AccessKey) --secret-key $(Format-CommandArgument $SecretKey)"
        if (-not [string]::IsNullOrWhiteSpace($SecurityToken)) {
            $command = "$command --security-token $(Format-CommandArgument $SecurityToken)"
        }
    }

    return $command
}

function Get-BenchmarkMetrics {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    if (-not (Test-Path $Path)) {
        throw "$Label benchmark output does not exist: $Path"
    }

    $content = Get-Content -Path $Path -Raw
    $pattern = '\[Complete\]\s+(?<operation>Send|Consume) Total:\s*(?<total>\d+)(?:\s+\|\s+(?:Send|Consume) TPS:\s*(?<tps>\d+))?(?:\s+\|\s+Max RT\(ms\):\s*(?<maxrt>[0-9.]+))?(?:\s+\|\s+Average RT\(ms\):\s*(?<avgrt>[0-9.]+))?(?:\s+\|\s+(?:Send|Consume) Failed:\s*(?<failed>\d+))?(?:\s+\|\s+Response Failed:\s*(?<responsefailed>\d+))?'
    $matches = [regex]::Matches($content, $pattern)
    if ($matches.Count -eq 0) {
        throw "$Label benchmark output did not contain a parseable [Complete] summary: $Path"
    }

    $match = $matches[$matches.Count - 1]
    $metrics = [ordered]@{
        Label = $Label
        Operation = $match.Groups["operation"].Value
        Total = [int64]$match.Groups["total"].Value
        Tps = if ($match.Groups["tps"].Success) { [double]$match.Groups["tps"].Value } else { [double]::NaN }
        MaxRtMs = if ($match.Groups["maxrt"].Success) { [double]$match.Groups["maxrt"].Value } else { [double]::NaN }
        AverageRtMs = if ($match.Groups["avgrt"].Success) { [double]$match.Groups["avgrt"].Value } else { [double]::NaN }
        Failed = if ($match.Groups["failed"].Success) { [int64]$match.Groups["failed"].Value } else { 0 }
        ResponseFailed = if ($match.Groups["responsefailed"].Success) { [int64]$match.Groups["responsefailed"].Value } else { 0 }
    }

    if ([double]::IsNaN($metrics.Tps)) {
        throw "$Label benchmark output did not contain TPS: $Path"
    }

    return $metrics
}

function Assert-BenchmarkComparison {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Rust,
        [Parameter(Mandatory = $true)]
        [hashtable]$Java
    )

    if ($Rust.Operation -ne $Java.Operation) {
        throw "Rust/Java benchmark operations differ: Rust=$($Rust.Operation), Java=$($Java.Operation)"
    }
    if ($Rust.Failed -ne 0 -or $Rust.ResponseFailed -ne 0) {
        throw "Rust benchmark had failures: $($Rust.Operation)Failed=$($Rust.Failed), ResponseFailed=$($Rust.ResponseFailed)"
    }
    if ($Java.Failed -ne 0 -or $Java.ResponseFailed -ne 0) {
        throw "Java benchmark had failures: $($Java.Operation)Failed=$($Java.Failed), ResponseFailed=$($Java.ResponseFailed)"
    }

    $tpsRatio = if ($Java.Tps -gt 0) { $Rust.Tps / $Java.Tps } else { [double]::PositiveInfinity }
    if ($tpsRatio -lt $MinRustToJavaTpsRatio) {
        throw ("Rust TPS ratio {0:N3} is below required {1:N3}. RustTPS={2:N3}, JavaTPS={3:N3}" -f `
                $tpsRatio, $MinRustToJavaTpsRatio, $Rust.Tps, $Java.Tps)
    }

    $rtRatio = [double]::NaN
    if (-not [double]::IsNaN($Rust.AverageRtMs) -and -not [double]::IsNaN($Java.AverageRtMs) -and $Java.AverageRtMs -gt 0) {
        $rtRatio = $Rust.AverageRtMs / $Java.AverageRtMs
        if ($rtRatio -gt $MaxRustToJavaAverageRtRatio) {
            throw ("Rust average RT ratio {0:N3} is above allowed {1:N3}. RustAverageRT={2:N3}, JavaAverageRT={3:N3}" -f `
                    $rtRatio, $MaxRustToJavaAverageRtRatio, $Rust.AverageRtMs, $Java.AverageRtMs)
        }
    }

    return [ordered]@{
        RustTps = $Rust.Tps
        JavaTps = $Java.Tps
        TpsRatio = $tpsRatio
        RustAverageRtMs = $Rust.AverageRtMs
        JavaAverageRtMs = $Java.AverageRtMs
        AverageRtRatio = $rtRatio
    }
}

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$resolvedOutputDir = Join-Path $workspaceRoot $OutputDir
$requestedRustBench = @(
    $RustBench |
        ForEach-Object { ([string]$_).Split(',', [System.StringSplitOptions]::RemoveEmptyEntries) } |
        ForEach-Object { ([string]$_).Trim() } |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
)

if (-not (Test-Path $resolvedOutputDir)) {
    New-Item -ItemType Directory -Path $resolvedOutputDir | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$summaryFile = Join-Path $resolvedOutputDir "summary-$timestamp.txt"

$rustBenches = @(
    @{
        Name = "client-hot-path"
        Command = "cargo bench -p rocketmq-client-rust --bench client_hot_path_benchmark"
    },
    @{
        Name = "queue-selector"
        Command = "cargo bench -p rocketmq-client-rust --bench select_queue_benchmark"
    },
    @{
        Name = "producer-accumulator"
        Command = "cargo bench -p rocketmq-client-rust --bench produce_accumulator_benchmark"
    },
    @{
        Name = "oneway"
        Command = "cargo bench -p rocketmq-client-rust --bench oneway_benchmark"
    },
    @{
        Name = "message-util"
        Command = "cargo bench -p rocketmq-client-rust --bench message_util_bench"
    },
    @{
        Name = "thread-local-index"
        Command = "cargo bench -p rocketmq-client-rust --bench thread_local_index_bench"
    },
    @{
        Name = "concurrent-optimization"
        Command = "cargo bench -p rocketmq-client-rust --bench concurrent_optimization_benchmark"
    },
    @{
        Name = "remoting-encode-decode"
        Command = "cargo bench -p rocketmq-remoting --bench encode_decode_bench"
    },
    @{
        Name = "auth-hot-path"
        Command = "cargo bench -p rocketmq-auth --bench auth_hot_path_bench"
    }
)

"RocketMQ client production benchmark run" | Set-Content -Path $summaryFile
"timestamp=$timestamp" | Add-Content -Path $summaryFile
"workspace=$workspaceRoot" | Add-Content -Path $summaryFile
"compileOnly=$CompileOnly" | Add-Content -Path $summaryFile
"quick=$Quick" | Add-Content -Path $summaryFile
"rustCriterionArgs=$RustCriterionArgs" | Add-Content -Path $summaryFile
"rustBench=$($requestedRustBench -join ',')" | Add-Content -Path $summaryFile
"javaRoot=$JavaRocketMQRoot" | Add-Content -Path $summaryFile
"javaBenchCommand=$JavaBenchCommand" | Add-Content -Path $summaryFile
"rustBrokerBenchCommand=$RustBrokerBenchCommand" | Add-Content -Path $summaryFile
"namesrv=$NamesrvAddr" | Add-Content -Path $summaryFile
"topic=$Topic" | Add-Content -Path $summaryFile
"brokerBenchMessageCount=$BrokerBenchMessageCount" | Add-Content -Path $summaryFile
"brokerBenchMessageSize=$BrokerBenchMessageSize" | Add-Content -Path $summaryFile
"rustBrokerBenchScenario=$RustBrokerBenchScenario" | Add-Content -Path $summaryFile
"minRustToJavaTpsRatio=$MinRustToJavaTpsRatio" | Add-Content -Path $summaryFile
"maxRustToJavaAverageRtRatio=$MaxRustToJavaAverageRtRatio" | Add-Content -Path $summaryFile
"compareOnly=$CompareOnly" | Add-Content -Path $summaryFile
"rustBranch=$(Get-GitValue -Repository $workspaceRoot -Arguments 'rev-parse --abbrev-ref HEAD')" | Add-Content -Path $summaryFile
"rustCommit=$(Get-GitValue -Repository $workspaceRoot -Arguments 'rev-parse --short HEAD')" | Add-Content -Path $summaryFile
if (Test-Path $JavaRocketMQRoot) {
    $javaRootForGit = (Resolve-Path $JavaRocketMQRoot).Path
    "javaBranch=$(Get-GitValue -Repository $javaRootForGit -Arguments 'rev-parse --abbrev-ref HEAD')" | Add-Content -Path $summaryFile
    "javaCommit=$(Get-GitValue -Repository $javaRootForGit -Arguments 'rev-parse --short HEAD')" | Add-Content -Path $summaryFile
}
"" | Add-Content -Path $summaryFile

Push-Location $workspaceRoot
try {
    $rustBrokerOutputForComparison = $RustBrokerBenchOutput
    $javaOutputForComparison = $JavaBenchOutput
    if ($CompareOnly) {
        if ([string]::IsNullOrWhiteSpace($rustBrokerOutputForComparison) -or [string]::IsNullOrWhiteSpace($javaOutputForComparison)) {
            throw "-CompareOnly requires -RustBrokerBenchOutput and -JavaBenchOutput."
        }
    }

    $criterionArgs = $RustCriterionArgs
    if ($Quick -and [string]::IsNullOrWhiteSpace($criterionArgs)) {
        $criterionArgs = "--sample-size 10 --warm-up-time 1 --measurement-time 1 --noplot"
    }

    $selectedRustBenches = $rustBenches
    if ($requestedRustBench.Count -gt 0) {
        $knownBenchNames = @($rustBenches | ForEach-Object { $_.Name })
        $unknownBenchNames = @($requestedRustBench | Where-Object { $knownBenchNames -notcontains $_ })
        if ($unknownBenchNames.Count -gt 0) {
            throw "Unknown Rust benchmark name(s): $($unknownBenchNames -join ', '). Known names: $($knownBenchNames -join ', ')"
        }

        $selectedRustBenches = @($rustBenches | Where-Object { $requestedRustBench -contains $_.Name })
    }

    if (-not $CompareOnly) {
        foreach ($bench in $selectedRustBenches) {
            $command = $bench.Command
            if ($CompileOnly) {
                $command = "$command --no-run"
            } elseif (-not [string]::IsNullOrWhiteSpace($criterionArgs)) {
                $command = "$command -- $criterionArgs"
            }

            $outputFile = Join-Path $resolvedOutputDir "$($bench.Name)-$timestamp.txt"
            Invoke-LoggedCommand -Name "rust-$($bench.Name)" -Command $command -WorkingDirectory $workspaceRoot -OutputFile $outputFile
            "rust-$($bench.Name)=$outputFile" | Add-Content -Path $summaryFile
        }

        if ($CompileOnly) {
            $outputFile = Join-Path $resolvedOutputDir "rust-broker-client-compile-$timestamp.txt"
            Invoke-LoggedCommand `
                -Name "rust-broker-client-compile" `
                -Command "cargo build -p rocketmq-client-rust --example client-production-benchmark --release" `
                -WorkingDirectory $workspaceRoot `
                -OutputFile $outputFile
            "rust-broker-client-compile=$outputFile" | Add-Content -Path $summaryFile
        }
    }

    if (-not $CompileOnly -and -not $CompareOnly -and -not [string]::IsNullOrWhiteSpace($JavaBenchCommand)) {
        $effectiveRustBrokerBenchCommand = $RustBrokerBenchCommand
        $messages = $BrokerBenchMessageCount
        if ($messages -le 0) {
            $messages = if ($Quick) { 100 } else { 10000 }
        }
        if ([string]::IsNullOrWhiteSpace($effectiveRustBrokerBenchCommand)) {
            if ([string]::IsNullOrWhiteSpace($NamesrvAddr)) {
                throw "Rust broker benchmark requires ROCKETMQ_NAMESRV_ADDR/-NamesrvAddr or -RustBrokerBenchCommand."
            }
            $effectiveRustBrokerBenchCommand = New-RustBrokerBenchCommand `
                -Namesrv $NamesrvAddr `
                -TopicName $Topic `
                -Messages $messages
        }

        $rustBrokerOutputFile = Join-Path $resolvedOutputDir "rust-broker-client-$timestamp.txt"
        Invoke-LoggedCommand `
            -Name "rust-broker-client" `
            -Command $effectiveRustBrokerBenchCommand `
            -WorkingDirectory $workspaceRoot `
            -OutputFile $rustBrokerOutputFile
        "rust-broker-client=$rustBrokerOutputFile" | Add-Content -Path $summaryFile
        $rustBrokerOutputForComparison = $rustBrokerOutputFile

        if (-not (Test-Path $JavaRocketMQRoot)) {
            throw "Java RocketMQ root does not exist: $JavaRocketMQRoot"
        }

        $javaRoot = (Resolve-Path $JavaRocketMQRoot).Path
        $javaOutputFile = Join-Path $resolvedOutputDir "java-client-$timestamp.txt"
        Invoke-LoggedCommand -Name "java-client" -Command $JavaBenchCommand -WorkingDirectory $javaRoot -OutputFile $javaOutputFile
        "java-client=$javaOutputFile" | Add-Content -Path $summaryFile
        $javaOutputForComparison = $javaOutputFile
    }

    if (-not [string]::IsNullOrWhiteSpace($rustBrokerOutputForComparison) -and -not [string]::IsNullOrWhiteSpace($javaOutputForComparison)) {
        $rustMetrics = Get-BenchmarkMetrics -Path $rustBrokerOutputForComparison -Label "Rust"
        $javaMetrics = Get-BenchmarkMetrics -Path $javaOutputForComparison -Label "Java"
        $comparison = Assert-BenchmarkComparison -Rust $rustMetrics -Java $javaMetrics
        "" | Add-Content -Path $summaryFile
        "Performance comparison:" | Add-Content -Path $summaryFile
        "operation=$($rustMetrics.Operation)" | Add-Content -Path $summaryFile
        "rustTps=$($comparison.RustTps)" | Add-Content -Path $summaryFile
        "javaTps=$($comparison.JavaTps)" | Add-Content -Path $summaryFile
        "tpsRatio=$($comparison.TpsRatio)" | Add-Content -Path $summaryFile
        "rustAverageRtMs=$($comparison.RustAverageRtMs)" | Add-Content -Path $summaryFile
        "javaAverageRtMs=$($comparison.JavaAverageRtMs)" | Add-Content -Path $summaryFile
        "averageRtRatio=$($comparison.AverageRtRatio)" | Add-Content -Path $summaryFile
        Write-Host "Performance comparison passed. TPS ratio=$($comparison.TpsRatio), average RT ratio=$($comparison.AverageRtRatio)"
    }

    Write-Host "Benchmark summary saved to $summaryFile"
}
finally {
    Pop-Location
}

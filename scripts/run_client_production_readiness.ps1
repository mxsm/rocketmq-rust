[CmdletBinding()]
param(
    [string]$OutputDir = "target/client-production-readiness",
    [ValidateSet("Auto", "Skip", "Require")]
    [string]$BrokerSmoke = "Auto",
    [switch]$SkipFormat,
    [switch]$SkipClippy,
    [switch]$SkipUnitTests,
    [switch]$SkipApiParity,
    [switch]$RustOnly,
    [switch]$SkipBenchCompile,
    [switch]$RunBenchmarks,
    [switch]$QuickBenchmarks,
    [ValidateSet("sync", "async", "batch", "lite-pull")]
    [string]$RustBrokerBenchScenario = "sync",
    [switch]$RequireExternalGates,
    [string[]]$RustBench = @(),
    [string]$RustCriterionArgs = "",
    [string]$JavaRocketMQRoot = "D:\Github\Java\rocketmq",
    [ValidateSet("Auto", "Skip", "Require")]
    [string]$JavaGateCompile = "Auto",
    [string]$JavaCompatibilityCommand = "",
    [string]$JavaBenchCommand = "",
    [switch]$UseExistingJavaClassesForJavaGate,
    [double]$MinRustToJavaTpsRatio = 1.0,
    [double]$MaxRustToJavaAverageRtRatio = 1.0,
    [double]$MinFreeDiskGB = 5.0,
    [int]$CommandTimeoutSeconds = 1800,
    [int]$BrokerSmokeTimeoutSeconds = 300
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

function Test-EnvNonEmpty {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    return -not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($Name))
}

function Test-EnvFlagEnabled {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        return $false
    }

    return @("1", "true", "yes", "on") -contains $value.Trim().ToLowerInvariant()
}

function Get-FreeDiskGB {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $root = [System.IO.Path]::GetPathRoot((Resolve-Path $Path).Path)
    if ([string]::IsNullOrWhiteSpace($root)) {
        return $null
    }

    $driveName = $root.TrimEnd('\').TrimEnd(':')
    $drive = Get-PSDrive -Name $driveName -ErrorAction SilentlyContinue
    if ($null -ne $drive -and $null -ne $drive.Free) {
        return [math]::Round($drive.Free / 1GB, 2)
    }

    try {
        $driveInfo = [System.IO.DriveInfo]::new($driveName)
        return [math]::Round($driveInfo.AvailableFreeSpace / 1GB, 2)
    } catch {
        return $null
    }
}

function Stop-ProcessTree {
    param(
        [Parameter(Mandatory = $true)]
        [int]$ProcessId
    )

    $children = Get-CimInstance Win32_Process -Filter "ParentProcessId = $ProcessId" -ErrorAction SilentlyContinue
    foreach ($child in $children) {
        Stop-ProcessTree -ProcessId ([int]$child.ProcessId)
    }

    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

function Write-OutputFileDelta {
    param(
        [Parameter(Mandatory = $true)]
        [string]$OutputFile,
        [Parameter(Mandatory = $true)]
        [ref]$Position
    )

    if (-not (Test-Path $OutputFile)) {
        return
    }

    $stream = [System.IO.File]::Open($OutputFile, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    try {
        if ($stream.Length -lt $Position.Value) {
            $Position.Value = 0
        }
        [void]$stream.Seek([int64]$Position.Value, [System.IO.SeekOrigin]::Begin)
        $reader = New-Object System.IO.StreamReader($stream, [System.Text.Encoding]::UTF8, $true, 4096, $true)
        try {
            $text = $reader.ReadToEnd()
            $Position.Value = $stream.Position
        } finally {
            $reader.Dispose()
        }
    } finally {
        $stream.Dispose()
    }

    if (-not [string]::IsNullOrEmpty($text)) {
        Write-Host -NoNewline $text
    }
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
        [string]$OutputFile,
        [int]$TimeoutSeconds = 0
    )

    Write-Host "==> $Name"
    Write-Host "    $Command"

    Set-Content -Path $OutputFile -Value "" -NoNewline
    $quotedOutputFile = Format-CommandArgument $OutputFile
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = "cmd.exe"
    $startInfo.Arguments = "/c $Command > $quotedOutputFile 2>&1"
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $false
    $startInfo.RedirectStandardError = $false
    $startInfo.CreateNoWindow = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    [void]$process.Start()

    $position = 0
    $deadline = $null
    if ($TimeoutSeconds -gt 0) {
        $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    }

    while (-not $process.WaitForExit(500)) {
        Write-OutputFileDelta -OutputFile $OutputFile -Position ([ref]$position)
        if ($null -ne $deadline -and (Get-Date) -gt $deadline) {
            Stop-ProcessTree -ProcessId $process.Id
            $process.WaitForExit()
            Write-OutputFileDelta -OutputFile $OutputFile -Position ([ref]$position)
            throw "$Name timed out after $TimeoutSeconds seconds. See $OutputFile"
        }
    }

    Write-OutputFileDelta -OutputFile $OutputFile -Position ([ref]$position)

    if ($process.ExitCode -ne 0) {
        throw "$Name failed with exit code $($process.ExitCode). See $OutputFile"
    }
}

function Add-ExternalGate {
    param(
        [System.Collections.Generic.List[string]]$Gates,
        [Parameter(Mandatory = $true)]
        [string]$Gate
    )

    if (-not $Gates.Contains($Gate)) {
        $Gates.Add($Gate) | Out-Null
    }
}

function Format-CommandArgument {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    return '"' + $Value.Replace('"', '\"') + '"'
}

function New-JavaGateCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Mode,
        [Parameter(Mandatory = $true)]
        [string]$ScriptPath,
        [Parameter(Mandatory = $true)]
        [string]$JavaRoot,
        [Parameter(Mandatory = $true)]
        [string]$Namesrv,
        [Parameter(Mandatory = $true)]
        [string]$Topic,
        [bool]$Quick,
        [bool]$Acl,
        [bool]$Trace,
        [bool]$Tls = $false,
        [bool]$Recall = $false,
        [bool]$UseExistingJavaClasses = $false,
        [bool]$CompileOnly = $false,
        [string[]]$Scenarios = @()
    )

    $command = "powershell -NoProfile -ExecutionPolicy Bypass -File $(Format-CommandArgument $ScriptPath) " +
        "-Mode $Mode -JavaRocketMQRoot $(Format-CommandArgument $JavaRoot) " +
        "-NamesrvAddr $(Format-CommandArgument $Namesrv) -Topic $(Format-CommandArgument $Topic)"
    if ($Quick) {
        $command = "$command -Quick"
    }
    if ($Acl) {
        $command = "$command -Acl"
    }
    if ($Trace) {
        $command = "$command -Trace"
    }
    if ($Tls) {
        $command = "$command -Tls"
    }
    if ($Recall) {
        $command = "$command -Recall"
    }
    foreach ($scenario in $Scenarios) {
        if (-not [string]::IsNullOrWhiteSpace($scenario)) {
            $command = "$command -Scenario $(Format-CommandArgument $scenario)"
        }
    }
    if ($UseExistingJavaClasses) {
        $command = "$command -UseExistingJavaClasses"
    }
    if ($CompileOnly) {
        $command = "$command -CompileOnly"
    }

    return $command
}

function Test-JavaExistingClassesAvailable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$JavaRoot
    )

    $requiredClassDirs = @(
        "client\target\classes",
        "common\target\classes",
        "remoting\target\classes",
        "srvutil\target\classes",
        "example\target\classes"
    )
    foreach ($relativePath in $requiredClassDirs) {
        if (-not (Test-Path (Join-Path $JavaRoot $relativePath))) {
            return $false
        }
    }

    $distributionTarget = Join-Path $JavaRoot "distribution\target"
    if (-not (Test-Path $distributionTarget)) {
        return $false
    }

    $libDir = Get-ChildItem -Path $distributionTarget -Directory -Recurse -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -eq "lib" } |
        Select-Object -First 1
    return $null -ne $libDir
}

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$resolvedOutputDir = Join-Path $workspaceRoot $OutputDir
if (-not (Test-Path $resolvedOutputDir)) {
    New-Item -ItemType Directory -Path $resolvedOutputDir | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runId = "$timestamp-$PID"
$summaryFile = Join-Path $resolvedOutputDir "summary-$runId.txt"
$externalGates = [System.Collections.Generic.List[string]]::new()
$javaGateScript = Join-Path $PSScriptRoot "run_client_java_gate.ps1"
$javaGateTopic = if ([string]::IsNullOrWhiteSpace($env:ROCKETMQ_TEST_TOPIC)) { "TopicTest" } else { $env:ROCKETMQ_TEST_TOPIC }
$javaGateAcl = (Test-EnvNonEmpty "ROCKETMQ_ACL_ACCESS_KEY") -and (Test-EnvNonEmpty "ROCKETMQ_ACL_SECRET_KEY")
$javaGateTrace = Test-EnvFlagEnabled "ROCKETMQ_ENABLE_TRACE_SMOKE"
$javaGateTls = Test-EnvFlagEnabled "ROCKETMQ_ENABLE_TLS_SMOKE"
$javaGateRecall = Test-EnvFlagEnabled "ROCKETMQ_ENABLE_RECALL_SMOKE"
$workspaceFreeDiskGB = Get-FreeDiskGB -Path $workspaceRoot

if ([string]::IsNullOrWhiteSpace($env:__COMPAT_LAYER)) {
    $env:__COMPAT_LAYER = "RunAsInvoker"
}

"RocketMQ client production readiness run" | Set-Content -Path $summaryFile
"timestamp=$timestamp" | Add-Content -Path $summaryFile
"runId=$runId" | Add-Content -Path $summaryFile
"workspace=$workspaceRoot" | Add-Content -Path $summaryFile
"brokerSmoke=$BrokerSmoke" | Add-Content -Path $summaryFile
"runBenchmarks=$RunBenchmarks" | Add-Content -Path $summaryFile
"quickBenchmarks=$QuickBenchmarks" | Add-Content -Path $summaryFile
"rustBrokerBenchScenario=$RustBrokerBenchScenario" | Add-Content -Path $summaryFile
"requireExternalGates=$RequireExternalGates" | Add-Content -Path $summaryFile
"skipApiParity=$SkipApiParity" | Add-Content -Path $summaryFile
"rustOnly=$RustOnly" | Add-Content -Path $summaryFile
"javaGateCompile=$JavaGateCompile" | Add-Content -Path $summaryFile
"useExistingJavaClassesForJavaGate=$UseExistingJavaClassesForJavaGate" | Add-Content -Path $summaryFile
"javaGateAcl=$javaGateAcl" | Add-Content -Path $summaryFile
"javaGateTrace=$javaGateTrace" | Add-Content -Path $summaryFile
"javaGateTls=$javaGateTls" | Add-Content -Path $summaryFile
"javaGateRecall=$javaGateRecall" | Add-Content -Path $summaryFile
"minRustToJavaTpsRatio=$MinRustToJavaTpsRatio" | Add-Content -Path $summaryFile
"maxRustToJavaAverageRtRatio=$MaxRustToJavaAverageRtRatio" | Add-Content -Path $summaryFile
"commandTimeoutSeconds=$CommandTimeoutSeconds" | Add-Content -Path $summaryFile
"brokerSmokeTimeoutSeconds=$BrokerSmokeTimeoutSeconds" | Add-Content -Path $summaryFile
"workspaceFreeDiskGB=$workspaceFreeDiskGB" | Add-Content -Path $summaryFile
"minFreeDiskGB=$MinFreeDiskGB" | Add-Content -Path $summaryFile
if ($RustOnly) {
    "javaRoot=skipped" | Add-Content -Path $summaryFile
} else {
    "javaRoot=$JavaRocketMQRoot" | Add-Content -Path $summaryFile
}
"" | Add-Content -Path $summaryFile

$commands = @()
if (-not $SkipFormat) {
    $commands += @{
        Name = "cargo-fmt-check"
        Command = "cargo fmt --all -- --check"
    }
}
if (-not $SkipClippy) {
    $commands += @{
        Name = "workspace-clippy"
        Command = "cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings"
    }
}
if (-not $SkipUnitTests) {
    $commands += @(
        @{
            Name = "client-lib-tests"
            Command = "cargo test -p rocketmq-client-rust --lib"
        },
        @{
            Name = "client-integration-tests"
            Command = "set ROCKETMQ_NAMESRV_ADDR=& set ROCKETMQ_REQUIRE_BROKER_BACKED_SMOKE=& cargo test -p rocketmq-client-rust --tests"
        },
        @{
            Name = "auth-tests"
            Command = "cargo test -p rocketmq-auth"
        },
        @{
            Name = "admin-core-lib-tests"
            Command = "cargo test -p rocketmq-admin-core --lib"
        },
        @{
            Name = "admin-core-integration-tests"
            Command = "cargo test -p rocketmq-admin-core --tests"
        },
        @{
            Name = "remoting-lib-tests"
            Command = "cargo test -p rocketmq-remoting --lib"
        }
    )
}

Push-Location $workspaceRoot
try {
    $hasNamesrv = Test-EnvNonEmpty "ROCKETMQ_NAMESRV_ADDR"
    $buildLikeGatesNeedDisk =
        (-not $SkipFormat) -or
        (-not $SkipClippy) -or
        (-not $SkipUnitTests) -or
        (-not $SkipBenchCompile) -or
        $RunBenchmarks -or
        (($BrokerSmoke -ne "Skip") -and $hasNamesrv) -or
        ((-not $RustOnly) -and (-not [string]::IsNullOrWhiteSpace($JavaCompatibilityCommand))) -or
        ((-not $RustOnly) -and (-not [string]::IsNullOrWhiteSpace($JavaBenchCommand)))
    $skipBuildLikeGatesForDisk =
        $buildLikeGatesNeedDisk -and
        $MinFreeDiskGB -gt 0 -and
        $null -ne $workspaceFreeDiskGB -and
        $workspaceFreeDiskGB -lt $MinFreeDiskGB
    if ($skipBuildLikeGatesForDisk) {
        Add-ExternalGate `
            -Gates $externalGates `
            -Gate "workspace drive free disk ${workspaceFreeDiskGB}GB is below required ${MinFreeDiskGB}GB for local build/test/bench gates"
    }

    if ($SkipApiParity) {
        $gate = if ($RustOnly) {
            "Rust client production audit not run; remove -SkipApiParity"
        } else {
            "Java/Rust client API parity audit not run; remove -SkipApiParity"
        }
        Add-ExternalGate -Gates $externalGates -Gate $gate
    } else {
        $auditScript = Join-Path $PSScriptRoot "audit_client_java_parity.ps1"
        $auditCommand = "powershell -NoProfile -ExecutionPolicy Bypass -File $(Format-CommandArgument $auditScript) -FailOnMissing"
        $auditName = if ($RustOnly) { "rust-production-audit" } else { "api-parity-audit" }

        if ($RustOnly) {
            $auditCommand = "$auditCommand -RustOnly"
        } else {
            $javaClientRoot = Join-Path $JavaRocketMQRoot "client\src\main\java\org\apache\rocketmq\client"
            if (-not (Test-Path $javaClientRoot)) {
                Add-ExternalGate -Gates $externalGates -Gate "Java/Rust client API parity audit missing Java client root: $javaClientRoot"
                $auditCommand = ""
            } else {
                $auditCommand = "$auditCommand -JavaRocketMQRoot $(Format-CommandArgument $JavaRocketMQRoot)"
            }
        }

        if (-not [string]::IsNullOrWhiteSpace($auditCommand)) {
            $outputFile = Join-Path $resolvedOutputDir "$auditName-$runId.txt"
            Invoke-LoggedCommand -Name $auditName -Command $auditCommand -WorkingDirectory $workspaceRoot -OutputFile $outputFile -TimeoutSeconds $CommandTimeoutSeconds
            "$auditName=$outputFile" | Add-Content -Path $summaryFile
        }
    }

    if ($skipBuildLikeGatesForDisk -and $commands.Count -gt 0) {
        Add-ExternalGate -Gates $externalGates -Gate "Cargo validation gates skipped because workspace drive free disk is below minimum"
    } else {
        foreach ($entry in $commands) {
            $outputFile = Join-Path $resolvedOutputDir "$($entry.Name)-$runId.txt"
            Invoke-LoggedCommand -Name $entry.Name -Command $entry.Command -WorkingDirectory $workspaceRoot -OutputFile $outputFile -TimeoutSeconds $CommandTimeoutSeconds
            "$($entry.Name)=$outputFile" | Add-Content -Path $summaryFile
        }
    }

    if ($RustOnly) {
        "java-compatibility-harness-compile=skipped-rust-only" | Add-Content -Path $summaryFile
    } elseif ($JavaGateCompile -eq "Skip") {
        Add-ExternalGate -Gates $externalGates -Gate "Java compatibility harness compile not run; use -JavaGateCompile Auto or Require"
    } else {
        if (-not (Test-Path $javaGateScript)) {
            $gate = "Java compatibility harness compile missing script: $javaGateScript"
            Add-ExternalGate -Gates $externalGates -Gate $gate
            if ($JavaGateCompile -eq "Require") {
                throw $gate
            }
        } elseif (-not (Test-Path $JavaRocketMQRoot)) {
            $gate = "Java compatibility harness compile missing Java root: $JavaRocketMQRoot"
            Add-ExternalGate -Gates $externalGates -Gate $gate
            if ($JavaGateCompile -eq "Require") {
                throw $gate
            }
        } else {
            $compileNamesrv = if ($hasNamesrv) { $env:ROCKETMQ_NAMESRV_ADDR } else { "127.0.0.1:9876" }
            $compileUsesExistingClasses =
                [bool]$UseExistingJavaClassesForJavaGate -or
                (Test-JavaExistingClassesAvailable -JavaRoot $JavaRocketMQRoot)
            $javaCompileCommand = New-JavaGateCommand `
                -Mode "Compatibility" `
                -ScriptPath $javaGateScript `
                -JavaRoot $JavaRocketMQRoot `
                -Namesrv $compileNamesrv `
                -Topic $javaGateTopic `
                -Quick $true `
                -Acl $false `
                -Trace $javaGateTrace `
                -Tls $javaGateTls `
                -Recall $javaGateRecall `
                -UseExistingJavaClasses $compileUsesExistingClasses `
                -CompileOnly $true

            $outputFile = Join-Path $resolvedOutputDir "java-compatibility-harness-compile-$runId.txt"
            try {
                Invoke-LoggedCommand `
                    -Name "java-compatibility-harness-compile" `
                    -Command $javaCompileCommand `
                    -WorkingDirectory $workspaceRoot `
                    -OutputFile $outputFile `
                    -TimeoutSeconds $CommandTimeoutSeconds
                "java-compatibility-harness-compile=$outputFile" | Add-Content -Path $summaryFile
            } catch {
                $gate = "Java compatibility harness compile failed; see $outputFile"
                Add-ExternalGate -Gates $externalGates -Gate $gate
                if ($JavaGateCompile -eq "Require") {
                    throw
                }
            }
        }
    }

    if ($BrokerSmoke -eq "Skip") {
        Add-ExternalGate -Gates $externalGates -Gate "broker-backed smoke not run; set ROCKETMQ_NAMESRV_ADDR and use -BrokerSmoke Require"
    } elseif (-not $hasNamesrv) {
        $gate = "broker-backed smoke missing ROCKETMQ_NAMESRV_ADDR"
        Add-ExternalGate -Gates $externalGates -Gate $gate
        if ($BrokerSmoke -eq "Require") {
            throw $gate
        }
    } elseif ($skipBuildLikeGatesForDisk) {
        Add-ExternalGate -Gates $externalGates -Gate "broker-backed smoke skipped because workspace drive free disk is below minimum"
    } else {
        if (-not ((Test-EnvNonEmpty "ROCKETMQ_ACL_ACCESS_KEY") -and (Test-EnvNonEmpty "ROCKETMQ_ACL_SECRET_KEY"))) {
            Add-ExternalGate -Gates $externalGates -Gate "ACL broker smoke missing ROCKETMQ_ACL_ACCESS_KEY/ROCKETMQ_ACL_SECRET_KEY"
        }
        if (-not (Test-EnvFlagEnabled "ROCKETMQ_ENABLE_TLS_SMOKE")) {
            Add-ExternalGate -Gates $externalGates -Gate "TLS broker smoke missing ROCKETMQ_ENABLE_TLS_SMOKE"
        }
        if (-not (Test-EnvFlagEnabled "ROCKETMQ_ENABLE_TRACE_SMOKE")) {
            Add-ExternalGate -Gates $externalGates -Gate "Trace broker smoke missing ROCKETMQ_ENABLE_TRACE_SMOKE"
        }
        if (-not (Test-EnvFlagEnabled "ROCKETMQ_ENABLE_RECALL_SMOKE")) {
            Add-ExternalGate -Gates $externalGates -Gate "Recall broker smoke missing ROCKETMQ_ENABLE_RECALL_SMOKE"
        }
        if (-not (Test-EnvFlagEnabled "ROCKETMQ_ENABLE_CREATE_TOPIC_SMOKE")) {
            Add-ExternalGate -Gates $externalGates -Gate "Create-topic broker smoke missing ROCKETMQ_ENABLE_CREATE_TOPIC_SMOKE"
        }

        $outputFile = Join-Path $resolvedOutputDir "broker-backed-smoke-$runId.txt"
        Invoke-LoggedCommand `
            -Name "broker-backed-smoke" `
            -Command "cargo test -p rocketmq-client-rust --test broker_backed_smoke -- --nocapture --test-threads=1" `
            -WorkingDirectory $workspaceRoot `
            -OutputFile $outputFile `
            -TimeoutSeconds $BrokerSmokeTimeoutSeconds
        "broker-backed-smoke=$outputFile" | Add-Content -Path $summaryFile
    }

    if ($skipBuildLikeGatesForDisk -and -not $SkipBenchCompile) {
        Add-ExternalGate -Gates $externalGates -Gate "benchmark compile gate skipped because workspace drive free disk is below minimum"
    } elseif (-not $SkipBenchCompile) {
        $benchScript = Join-Path $PSScriptRoot "run_client_production_bench.ps1"
        $benchOutputDir = Join-Path $OutputDir "bench-compile"
        $benchCommand = "powershell -NoProfile -ExecutionPolicy Bypass -File $(Format-CommandArgument $benchScript) -CompileOnly -OutputDir $(Format-CommandArgument $benchOutputDir)"
        if ($RustBench.Count -gt 0) {
            $benchCommand = "$benchCommand -RustBench $(Format-CommandArgument ($RustBench -join ','))"
        }

        $outputFile = Join-Path $resolvedOutputDir "bench-compile-$runId.txt"
        Invoke-LoggedCommand -Name "bench-compile" -Command $benchCommand -WorkingDirectory $workspaceRoot -OutputFile $outputFile -TimeoutSeconds $CommandTimeoutSeconds
        "bench-compile=$outputFile" | Add-Content -Path $summaryFile
    }

    if ($skipBuildLikeGatesForDisk -and $RunBenchmarks) {
        Add-ExternalGate -Gates $externalGates -Gate "Rust/Java benchmark run skipped because workspace drive free disk is below minimum"
    } elseif ($RunBenchmarks) {
        $benchScript = Join-Path $PSScriptRoot "run_client_production_bench.ps1"
        $benchOutputDir = Join-Path $OutputDir "bench-run"
        $benchCommand = "powershell -NoProfile -ExecutionPolicy Bypass -File $(Format-CommandArgument $benchScript) " +
            "-OutputDir $(Format-CommandArgument $benchOutputDir) " +
            "-NamesrvAddr $(Format-CommandArgument $env:ROCKETMQ_NAMESRV_ADDR) " +
            "-Topic $(Format-CommandArgument $javaGateTopic) " +
            "-RustBrokerBenchScenario $RustBrokerBenchScenario " +
            "-MinRustToJavaTpsRatio $MinRustToJavaTpsRatio " +
            "-MaxRustToJavaAverageRtRatio $MaxRustToJavaAverageRtRatio"
        if ($QuickBenchmarks) {
            $benchCommand = "$benchCommand -Quick"
        }
        if ($javaGateAcl) {
            $benchCommand = "$benchCommand -Acl"
        }
        if ($javaGateTls) {
            $benchCommand = "$benchCommand -UseTls"
        }
        if (-not [string]::IsNullOrWhiteSpace($RustCriterionArgs)) {
            $benchCommand = "$benchCommand -RustCriterionArgs $(Format-CommandArgument $RustCriterionArgs)"
        }
        if ($RustBench.Count -gt 0) {
            $benchCommand = "$benchCommand -RustBench $(Format-CommandArgument ($RustBench -join ','))"
        }
        $effectiveJavaBenchCommand = $JavaBenchCommand
        if ($RustOnly) {
            $effectiveJavaBenchCommand = ""
        }
        if ((-not $RustOnly) -and [string]::IsNullOrWhiteSpace($effectiveJavaBenchCommand)) {
            if (-not $hasNamesrv) {
                Add-ExternalGate -Gates $externalGates -Gate "default Java same-machine benchmark missing ROCKETMQ_NAMESRV_ADDR"
            } elseif (-not (Test-Path $javaGateScript)) {
                Add-ExternalGate -Gates $externalGates -Gate "default Java same-machine benchmark missing script: $javaGateScript"
            } elseif (-not (Test-Path $JavaRocketMQRoot)) {
                Add-ExternalGate -Gates $externalGates -Gate "default Java same-machine benchmark missing Java root: $JavaRocketMQRoot"
            } else {
                $javaBenchmarkScenarios = switch ($RustBrokerBenchScenario) {
                    "async" { @("ProducerAsync") }
                    "batch" { @("ProducerBatchBenchmark") }
                    "lite-pull" { @("LitePullBenchmark") }
                    default { @("ProducerSync") }
                }
                $effectiveJavaBenchCommand = New-JavaGateCommand `
                    -Mode "Benchmark" `
                    -ScriptPath $javaGateScript `
                    -JavaRoot $JavaRocketMQRoot `
                    -Namesrv $env:ROCKETMQ_NAMESRV_ADDR `
                    -Topic $javaGateTopic `
                    -Quick ([bool]$QuickBenchmarks) `
                    -Acl $javaGateAcl `
                    -Trace $javaGateTrace `
                    -Tls ($javaGateTls -and $RustBrokerBenchScenario -eq "lite-pull") `
                    -Scenarios $javaBenchmarkScenarios `
                    -UseExistingJavaClasses ([bool]$UseExistingJavaClassesForJavaGate)
            }
        }

        if ((-not $RustOnly) -and -not [string]::IsNullOrWhiteSpace($effectiveJavaBenchCommand)) {
            $benchCommand = "$benchCommand -JavaBenchCommand $(Format-CommandArgument $effectiveJavaBenchCommand) -JavaRocketMQRoot $(Format-CommandArgument $JavaRocketMQRoot)"
        }

        $outputFile = Join-Path $resolvedOutputDir "bench-run-$runId.txt"
        Invoke-LoggedCommand -Name "bench-run" -Command $benchCommand -WorkingDirectory $workspaceRoot -OutputFile $outputFile -TimeoutSeconds $CommandTimeoutSeconds
        "bench-run=$outputFile" | Add-Content -Path $summaryFile
    } elseif ($RustOnly) {
        "rust-java-performance-comparison=skipped-rust-only" | Add-Content -Path $summaryFile
    } else {
        Add-ExternalGate -Gates $externalGates -Gate "real Rust/Java performance comparison not run; use -RunBenchmarks, optionally with -JavaBenchCommand to override the default Java producer gate"
    }

    if ($RustOnly) {
        "java-compatibility=skipped-rust-only" | Add-Content -Path $summaryFile
    } else {
        $effectiveJavaCompatibilityCommand = $JavaCompatibilityCommand
        if ([string]::IsNullOrWhiteSpace($effectiveJavaCompatibilityCommand)) {
            if (-not $hasNamesrv) {
                Add-ExternalGate -Gates $externalGates -Gate "default Java same-broker compatibility smoke missing ROCKETMQ_NAMESRV_ADDR"
            } elseif (-not (Test-Path $javaGateScript)) {
                Add-ExternalGate -Gates $externalGates -Gate "default Java same-broker compatibility smoke missing script: $javaGateScript"
            } elseif (-not (Test-Path $JavaRocketMQRoot)) {
                Add-ExternalGate -Gates $externalGates -Gate "default Java same-broker compatibility smoke missing Java root: $JavaRocketMQRoot"
            } else {
                $effectiveJavaCompatibilityCommand = New-JavaGateCommand `
                    -Mode "Compatibility" `
                    -ScriptPath $javaGateScript `
                    -JavaRoot $JavaRocketMQRoot `
                    -Namesrv $env:ROCKETMQ_NAMESRV_ADDR `
                    -Topic $javaGateTopic `
                    -Quick $true `
                    -Acl $javaGateAcl `
                    -Trace $javaGateTrace `
                    -Tls $javaGateTls `
                    -Recall $javaGateRecall `
                    -UseExistingJavaClasses ([bool]$UseExistingJavaClassesForJavaGate)
            }
        }

        if (-not [string]::IsNullOrWhiteSpace($effectiveJavaCompatibilityCommand)) {
            if (-not (Test-Path $JavaRocketMQRoot)) {
                throw "Java RocketMQ root does not exist: $JavaRocketMQRoot"
            }

            $javaRoot = (Resolve-Path $JavaRocketMQRoot).Path
            $outputFile = Join-Path $resolvedOutputDir "java-compatibility-$runId.txt"
            Invoke-LoggedCommand -Name "java-compatibility" -Command $effectiveJavaCompatibilityCommand -WorkingDirectory $javaRoot -OutputFile $outputFile -TimeoutSeconds $CommandTimeoutSeconds
            "java-compatibility=$outputFile" | Add-Content -Path $summaryFile
        }
    }

    "" | Add-Content -Path $summaryFile
    "External gates still required:" | Add-Content -Path $summaryFile
    if ($externalGates.Count -eq 0) {
        "none" | Add-Content -Path $summaryFile
        Write-Host "All configured production readiness gates passed."
    } else {
        foreach ($gate in $externalGates) {
            "- $gate" | Add-Content -Path $summaryFile
        }

        Write-Host "Configured local gates passed. External gates still required:"
        foreach ($gate in $externalGates) {
            Write-Host " - $gate"
        }

        if ($RequireExternalGates) {
            Write-Host "Readiness summary saved to $summaryFile"
            throw "Production readiness external gates are still open. See $summaryFile"
        }
    }

    Write-Host "Readiness summary saved to $summaryFile"
}
finally {
    Pop-Location
}

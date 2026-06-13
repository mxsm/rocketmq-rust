[CmdletBinding()]
param(
    [ValidateSet("Compatibility", "Benchmark")]
    [string]$Mode = "Compatibility",
    [string]$JavaRocketMQRoot = "D:\Github\Java\rocketmq",
    [string]$NamesrvAddr = $env:ROCKETMQ_NAMESRV_ADDR,
    [string]$Topic = $(if ([string]::IsNullOrWhiteSpace($env:ROCKETMQ_TEST_TOPIC)) { "TopicTest" } else { $env:ROCKETMQ_TEST_TOPIC }),
    [string[]]$Scenario = @(),
    [int]$MessageCount = 0,
    [int]$ThreadCount = 1,
    [int]$MessageSize = 128,
    [int]$BatchSize = 16,
    [int]$ReportIntervalMillis = 1000,
    [int]$TimeoutMillis = 30000,
    [switch]$Quick,
    [switch]$Acl,
    [switch]$Trace,
    [switch]$Tls,
    [switch]$Recall,
    [switch]$UseBenchmarkProducer,
    [string]$AccessKey = $env:ROCKETMQ_ACL_ACCESS_KEY,
    [string]$SecretKey = $env:ROCKETMQ_ACL_SECRET_KEY,
    [string]$SecurityToken = $env:ROCKETMQ_ACL_SECURITY_TOKEN,
    [string]$MavenExecutable = "mvn",
    [string]$HarnessSource = "",
    [string]$HarnessClassOutputDir = "",
    [switch]$UseExistingJavaClasses,
    [switch]$CompileOnly,
    [switch]$PrintOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

function Format-CommandArgument {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    return '"' + $Value.Replace('"', '\"') + '"'
}

function Invoke-LoggedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [string]$Command,
        [Parameter(Mandatory = $true)]
        [string]$WorkingDirectory
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
    $stdout = $process.StandardOutput.ReadToEnd()
    $process.WaitForExit()

    if (-not [string]::IsNullOrEmpty($stdout)) {
        Write-Output $stdout
    }

    if ($process.ExitCode -ne 0) {
        throw "$Name failed with exit code $($process.ExitCode)."
    }
}

function New-ProducerCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ScenarioName,
        [Parameter(Mandatory = $true)]
        [string]$Namesrv,
        [Parameter(Mandatory = $true)]
        [string]$TopicName,
        [Parameter(Mandatory = $true)]
        [int]$Messages,
        [Parameter(Mandatory = $true)]
        [int]$Threads,
        [Parameter(Mandatory = $true)]
        [int]$BodySize,
        [Parameter(Mandatory = $true)]
        [int]$ReportInterval
    )

    $producerArgs = [System.Collections.Generic.List[string]]::new()
    $producerArgs.Add("-n") | Out-Null
    $producerArgs.Add($Namesrv) | Out-Null
    $producerArgs.Add("-t") | Out-Null
    $producerArgs.Add($TopicName) | Out-Null
    $producerArgs.Add("-q") | Out-Null
    $producerArgs.Add($Messages.ToString()) | Out-Null
    $producerArgs.Add("-w") | Out-Null
    $producerArgs.Add($Threads.ToString()) | Out-Null
    $producerArgs.Add("-s") | Out-Null
    $producerArgs.Add($BodySize.ToString()) | Out-Null
    $producerArgs.Add("-ri") | Out-Null
    $producerArgs.Add($ReportInterval.ToString()) | Out-Null

    if ($ScenarioName -eq "ProducerAsync") {
        $producerArgs.Add("-y") | Out-Null
        $producerArgs.Add("true") | Out-Null
    }
    if ($Acl) {
        if ([string]::IsNullOrWhiteSpace($AccessKey) -or [string]::IsNullOrWhiteSpace($SecretKey)) {
            throw "ACL Java gate requires ROCKETMQ_ACL_ACCESS_KEY/ROCKETMQ_ACL_SECRET_KEY or -AccessKey/-SecretKey."
        }
        $producerArgs.Add("-a") | Out-Null
        $producerArgs.Add("true") | Out-Null
        $producerArgs.Add("-ak") | Out-Null
        $producerArgs.Add($AccessKey) | Out-Null
        $producerArgs.Add("-sk") | Out-Null
        $producerArgs.Add($SecretKey) | Out-Null
    }
    if ($Trace) {
        $producerArgs.Add("-m") | Out-Null
        $producerArgs.Add("true") | Out-Null
    }

    $execArgs = $producerArgs -join " "
    return "$MavenExecutable -pl example -am -DskipTests compile exec:java " +
        "$(Format-CommandArgument '-Dexec.mainClass=org.apache.rocketmq.example.benchmark.Producer') " +
        "$(Format-CommandArgument "-Dexec.args=$execArgs")"
}

function New-HarnessCompileCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SourcePath,
        [Parameter(Mandatory = $true)]
        [string]$ClassOutputDir
    )

    $execArgs = "-encoding UTF-8 -cp %classpath -d $ClassOutputDir $SourcePath"
    return "$MavenExecutable -pl example -am -DskipTests compile exec:exec " +
        "$(Format-CommandArgument '-Dexec.executable=javac') " +
        "$(Format-CommandArgument "-Dexec.args=$execArgs")"
}

function Get-ExistingJavaClasspath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$JavaRoot
    )

    $entries = [System.Collections.Generic.List[string]]::new()
    $moduleClassDirs = Get-ChildItem -Path $JavaRoot -Directory |
        ForEach-Object { Join-Path $_.FullName "target\classes" } |
        Where-Object { Test-Path $_ }
    foreach ($classDir in $moduleClassDirs) {
        $entries.Add((Resolve-Path $classDir).Path) | Out-Null
    }

    $distributionTarget = Join-Path $JavaRoot "distribution\target"
    if (Test-Path $distributionTarget) {
        $libDir = Get-ChildItem -Path $distributionTarget -Directory -Recurse |
            Where-Object { $_.Name -eq "lib" } |
            Select-Object -First 1
        if ($null -ne $libDir) {
            $entries.Add((Join-Path $libDir.FullName "*")) | Out-Null
        }
    }

    if ($entries.Count -eq 0) {
        throw "No existing Java target/classes or distribution lib directory found under $JavaRoot."
    }

    return ($entries -join ";")
}

function New-ExistingClassesCompileCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SourcePath,
        [Parameter(Mandatory = $true)]
        [string]$ClassOutputDir,
        [Parameter(Mandatory = $true)]
        [string]$Classpath
    )

    return "javac -encoding UTF-8 -cp $(Format-CommandArgument $Classpath) " +
        "-d $(Format-CommandArgument $ClassOutputDir) $(Format-CommandArgument $SourcePath)"
}

function New-HarnessRunCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ClassOutputDir,
        [Parameter(Mandatory = $true)]
        [string[]]$Scenarios,
        [Parameter(Mandatory = $true)]
        [int]$Messages
    )

    $harnessArgs = [System.Collections.Generic.List[string]]::new()
    $harnessArgs.Add("--namesrv") | Out-Null
    $harnessArgs.Add($NamesrvAddr) | Out-Null
    $harnessArgs.Add("--topic") | Out-Null
    $harnessArgs.Add($Topic) | Out-Null
    $harnessArgs.Add("--scenarios") | Out-Null
    $harnessArgs.Add(($Scenarios -join ",")) | Out-Null
    $harnessArgs.Add("--message-count") | Out-Null
    $harnessArgs.Add($Messages.ToString()) | Out-Null
    $harnessArgs.Add("--message-size") | Out-Null
    $harnessArgs.Add($MessageSize.ToString()) | Out-Null
    $harnessArgs.Add("--batch-size") | Out-Null
    $harnessArgs.Add($BatchSize.ToString()) | Out-Null
    $harnessArgs.Add("--timeout-ms") | Out-Null
    $harnessArgs.Add($TimeoutMillis.ToString()) | Out-Null

    if ($Acl) {
        if ([string]::IsNullOrWhiteSpace($AccessKey) -or [string]::IsNullOrWhiteSpace($SecretKey)) {
            throw "ACL Java gate requires ROCKETMQ_ACL_ACCESS_KEY/ROCKETMQ_ACL_SECRET_KEY or -AccessKey/-SecretKey."
        }
        $harnessArgs.Add("--acl") | Out-Null
        $harnessArgs.Add("--access-key") | Out-Null
        $harnessArgs.Add($AccessKey) | Out-Null
        $harnessArgs.Add("--secret-key") | Out-Null
        $harnessArgs.Add($SecretKey) | Out-Null
        if (-not [string]::IsNullOrWhiteSpace($SecurityToken)) {
            $harnessArgs.Add("--security-token") | Out-Null
            $harnessArgs.Add($SecurityToken) | Out-Null
        }
    }
    if ($Trace) {
        $harnessArgs.Add("--trace") | Out-Null
    }
    if ($Tls) {
        $harnessArgs.Add("--tls") | Out-Null
    }

    $execArgs = "-cp %classpath;$ClassOutputDir ClientCompatibilitySmoke $($harnessArgs -join ' ')"
    return "$MavenExecutable -pl example -am -DskipTests compile exec:exec " +
        "$(Format-CommandArgument '-Dexec.executable=java') " +
        "$(Format-CommandArgument "-Dexec.args=$execArgs")"
}

function New-ExistingClassesRunCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ClassOutputDir,
        [Parameter(Mandatory = $true)]
        [string]$Classpath,
        [Parameter(Mandatory = $true)]
        [string[]]$Scenarios,
        [Parameter(Mandatory = $true)]
        [int]$Messages
    )

    $harnessArgs = [System.Collections.Generic.List[string]]::new()
    $harnessArgs.Add("--namesrv") | Out-Null
    $harnessArgs.Add($NamesrvAddr) | Out-Null
    $harnessArgs.Add("--topic") | Out-Null
    $harnessArgs.Add($Topic) | Out-Null
    $harnessArgs.Add("--scenarios") | Out-Null
    $harnessArgs.Add(($Scenarios -join ",")) | Out-Null
    $harnessArgs.Add("--message-count") | Out-Null
    $harnessArgs.Add($Messages.ToString()) | Out-Null
    $harnessArgs.Add("--message-size") | Out-Null
    $harnessArgs.Add($MessageSize.ToString()) | Out-Null
    $harnessArgs.Add("--batch-size") | Out-Null
    $harnessArgs.Add($BatchSize.ToString()) | Out-Null
    $harnessArgs.Add("--timeout-ms") | Out-Null
    $harnessArgs.Add($TimeoutMillis.ToString()) | Out-Null

    if ($Acl) {
        if ([string]::IsNullOrWhiteSpace($AccessKey) -or [string]::IsNullOrWhiteSpace($SecretKey)) {
            throw "ACL Java gate requires ROCKETMQ_ACL_ACCESS_KEY/ROCKETMQ_ACL_SECRET_KEY or -AccessKey/-SecretKey."
        }
        $harnessArgs.Add("--acl") | Out-Null
        $harnessArgs.Add("--access-key") | Out-Null
        $harnessArgs.Add($AccessKey) | Out-Null
        $harnessArgs.Add("--secret-key") | Out-Null
        $harnessArgs.Add($SecretKey) | Out-Null
        if (-not [string]::IsNullOrWhiteSpace($SecurityToken)) {
            $harnessArgs.Add("--security-token") | Out-Null
            $harnessArgs.Add($SecurityToken) | Out-Null
        }
    }
    if ($Trace) {
        $harnessArgs.Add("--trace") | Out-Null
    }
    if ($Tls) {
        $harnessArgs.Add("--tls") | Out-Null
    }

    $runtimeClasspath = "$Classpath;$ClassOutputDir"
    return "java -cp $(Format-CommandArgument $runtimeClasspath) ClientCompatibilitySmoke $($harnessArgs -join ' ')"
}

$javaRootPath = Join-Path $JavaRocketMQRoot "pom.xml"
if (-not (Test-Path $javaRootPath)) {
    throw "Java RocketMQ root does not contain pom.xml: $JavaRocketMQRoot"
}
if ([string]::IsNullOrWhiteSpace($NamesrvAddr)) {
    throw "Java client gate requires ROCKETMQ_NAMESRV_ADDR or -NamesrvAddr."
}

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if ([string]::IsNullOrWhiteSpace($HarnessSource)) {
    $HarnessSource = Join-Path $PSScriptRoot "java\ClientCompatibilitySmoke.java"
}
if ([string]::IsNullOrWhiteSpace($HarnessClassOutputDir)) {
    $HarnessClassOutputDir = Join-Path $workspaceRoot "target\client-java-gate\classes"
}

$selectedScenarios = @($Scenario | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
$requiresHarnessBenchmark = ($selectedScenarios -contains "LitePullBenchmark") -or ($selectedScenarios -contains "ProducerBatchBenchmark")
$useHarness = (-not $UseBenchmarkProducer) -and (($Mode -eq "Compatibility") -or $requiresHarnessBenchmark)
$harnessSourcePath = ""
$harnessClassOutputPath = ""
if ($useHarness) {
    if (-not (Test-Path $HarnessSource)) {
        throw "Java compatibility harness source does not exist: $HarnessSource"
    }
    if (-not (Test-Path $HarnessClassOutputDir)) {
        New-Item -ItemType Directory -Path $HarnessClassOutputDir | Out-Null
    }
    $harnessSourcePath = (Resolve-Path $HarnessSource).Path
    $harnessClassOutputPath = (Resolve-Path $HarnessClassOutputDir).Path
}

$defaultHarnessScenarios = @(
    "ProducerSync",
    "ProducerAsync",
    "ProducerOneway",
    "ProducerBatch",
    "TransactionProducer",
    "PushConcurrent",
    "LitePullAssign",
    "LitePullSubscribe",
    "RequestReply"
)
if ($Recall) {
    $defaultHarnessScenarios += "ProducerRecall"
}
if ($selectedScenarios.Count -eq 0) {
    $selectedScenarios = if ($useHarness) {
        $defaultHarnessScenarios
    } elseif ($Mode -eq "Compatibility") {
        @("ProducerSync", "ProducerAsync")
    } else {
        @("ProducerSync")
    }
}

$allowedScenarios = if ($useHarness) {
    $defaultHarnessScenarios + @("ProducerRecall", "ProducerBatchBenchmark", "LitePullBenchmark")
} else {
    @("ProducerSync", "ProducerAsync")
}
$unknownScenarios = @($selectedScenarios | Where-Object { $allowedScenarios -notcontains $_ })
if ($unknownScenarios.Count -gt 0) {
    throw "Unknown Java client gate scenario(s): $($unknownScenarios -join ', '). Known scenarios: $($allowedScenarios -join ', ')"
}

$messages = $MessageCount
if ($messages -le 0) {
    if ($Mode -eq "Compatibility") {
        $messages = if ($Quick) { 1 } else { 10 }
    } else {
        $messages = if ($Quick) { 100 } else { 10000 }
    }
}

Write-Host "RocketMQ Java client gate"
Write-Host "mode=$Mode"
Write-Host "javaRoot=$JavaRocketMQRoot"
Write-Host "namesrv=$NamesrvAddr"
Write-Host "topic=$Topic"
Write-Host "scenarios=$($selectedScenarios -join ',')"
Write-Host "messageCount=$messages"
Write-Host "threadCount=$ThreadCount"
Write-Host "messageSize=$MessageSize"
Write-Host "batchSize=$BatchSize"
Write-Host "timeoutMillis=$TimeoutMillis"
Write-Host "acl=$Acl"
Write-Host "trace=$Trace"
Write-Host "tls=$Tls"
Write-Host "recall=$Recall"
Write-Host "harness=$useHarness"
Write-Host "useExistingJavaClasses=$UseExistingJavaClasses"
Write-Host "compileOnly=$CompileOnly"

if ($useHarness) {
    if ($UseExistingJavaClasses) {
        $existingJavaClasspath = Get-ExistingJavaClasspath -JavaRoot $JavaRocketMQRoot
        $compileCommand = New-ExistingClassesCompileCommand `
            -SourcePath $harnessSourcePath `
            -ClassOutputDir $harnessClassOutputPath `
            -Classpath $existingJavaClasspath
        $runCommand = New-ExistingClassesRunCommand `
            -ClassOutputDir $harnessClassOutputPath `
            -Classpath $existingJavaClasspath `
            -Scenarios $selectedScenarios `
            -Messages $messages
    } else {
        $compileCommand = New-HarnessCompileCommand -SourcePath $harnessSourcePath -ClassOutputDir $harnessClassOutputPath
        $runCommand = New-HarnessRunCommand -ClassOutputDir $harnessClassOutputPath -Scenarios $selectedScenarios -Messages $messages
    }

    if ($PrintOnly) {
        Write-Host "==> java-compatibility-harness-compile"
        Write-Host "    $compileCommand"
        if (-not $CompileOnly) {
            Write-Host "==> java-compatibility-harness-run"
            Write-Host "    $runCommand"
        }
    } else {
        Invoke-LoggedCommand -Name "java-compatibility-harness-compile" -Command $compileCommand -WorkingDirectory $JavaRocketMQRoot
        if (-not $CompileOnly) {
            Invoke-LoggedCommand -Name "java-compatibility-harness-run" -Command $runCommand -WorkingDirectory $JavaRocketMQRoot
        }
    }
} else {
    if ($UseExistingJavaClasses) {
        throw "-UseExistingJavaClasses is only supported by the Java compatibility harness; remove -UseBenchmarkProducer or use -Mode Compatibility."
    }
    if ($CompileOnly) {
        throw "-CompileOnly is only supported by the Java compatibility harness; remove -UseBenchmarkProducer or use -Mode Compatibility."
    }
    if ($Tls) {
        throw "The Java benchmark Producer gate does not expose a TLS option; use compatibility harness mode or provide a custom Java command."
    }
    if ($Recall) {
        throw "The Java benchmark Producer gate does not expose recall; use compatibility harness mode or provide a custom Java command."
    }

    foreach ($scenarioName in $selectedScenarios) {
        $command = New-ProducerCommand `
            -ScenarioName $scenarioName `
            -Namesrv $NamesrvAddr `
            -TopicName $Topic `
            -Messages $messages `
            -Threads $ThreadCount `
            -BodySize $MessageSize `
            -ReportInterval $ReportIntervalMillis

        if ($PrintOnly) {
            Write-Host "==> java-$scenarioName"
            Write-Host "    $command"
        } else {
            Invoke-LoggedCommand -Name "java-$scenarioName" -Command $command -WorkingDirectory $JavaRocketMQRoot
        }
    }
}

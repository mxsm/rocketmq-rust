[CmdletBinding()]
param(
    [ValidateSet("rust-namesrv-java-broker", "java-namesrv-rust-broker")]
    [string]$Mode = "rust-namesrv-java-broker",
    [string]$JavaRocketmqHome,
    [string]$NamesrvHost = "127.0.0.1",
    [int]$NamesrvPort = 19876,
    [int]$BrokerPort = 10911,
    [string]$BrokerDataRoot = "",
    [int]$MaxDataDriveUsedPercent = 85,
    [string]$Topic = "Phase5Topic",
    [string]$ClusterName = "Phase5Cluster",
    [string]$BrokerName = "phase5Broker",
    [string]$JavaHome = $env:JAVA_HOME,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runRoot = Join-Path $workspaceRoot "target\namesrv-parity\$Mode-$runStamp"
$logRoot = Join-Path $runRoot "logs"
$defaultDataRoot = Join-Path $runRoot "data"
$dataRoot = $defaultDataRoot
$namesrvAddr = "$NamesrvHost`:$NamesrvPort"
$brokerAddr = "$NamesrvHost`:$BrokerPort"
$processes = @()
$script:ResolvedJavaHome = ""

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

function New-Directory {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function Write-AsciiFile {
    param(
        [string]$Path,
        [string]$Content
    )
    [System.IO.File]::WriteAllText($Path, $Content, [System.Text.Encoding]::ASCII)
}

function ConvertTo-FullPath {
    param([string]$Path)

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }

    return [System.IO.Path]::GetFullPath((Join-Path $workspaceRoot $Path))
}

function Get-PathDriveUsedPercent {
    param([string]$Path)

    try {
        $fullPath = ConvertTo-FullPath -Path $Path
        $driveRoot = [System.IO.Path]::GetPathRoot($fullPath)
        if ([string]::IsNullOrWhiteSpace($driveRoot)) {
            return $null
        }

        $driveInfo = [System.IO.DriveInfo]::new($driveRoot)
        if (-not $driveInfo.IsReady -or $driveInfo.TotalSize -le 0) {
            return $null
        }

        return [math]::Round((($driveInfo.TotalSize - $driveInfo.AvailableFreeSpace) * 100.0) / $driveInfo.TotalSize, 2)
    }
    catch {
        return $null
    }
}

function Resolve-TestDataRoot {
    if (-not [string]::IsNullOrWhiteSpace($BrokerDataRoot)) {
        return ConvertTo-FullPath -Path $BrokerDataRoot
    }

    $defaultUsedPercent = Get-PathDriveUsedPercent -Path $defaultDataRoot
    if ($null -eq $defaultUsedPercent -or $defaultUsedPercent -le $MaxDataDriveUsedPercent) {
        return $defaultDataRoot
    }

    $candidateRoots = @()
    if (-not [string]::IsNullOrWhiteSpace($env:ROCKETMQ_E2E_DATA_ROOT)) {
        $candidateRoots += $env:ROCKETMQ_E2E_DATA_ROOT
    }
    if (-not [string]::IsNullOrWhiteSpace($env:TEMP)) {
        $candidateRoots += (Join-Path $env:TEMP "rocketmq-rust-e2e-data")
    }

    $driveCandidates = [System.IO.DriveInfo]::GetDrives() |
        Where-Object { $_.DriveType -eq [System.IO.DriveType]::Fixed -and $_.IsReady -and $_.TotalSize -gt 0 } |
        Sort-Object AvailableFreeSpace -Descending
    foreach ($drive in $driveCandidates) {
        $candidateRoots += (Join-Path $drive.RootDirectory.FullName "rocketmq-rust-e2e-data")
    }

    foreach ($candidate in $candidateRoots) {
        if ([string]::IsNullOrWhiteSpace($candidate)) {
            continue
        }

        $candidateUsedPercent = Get-PathDriveUsedPercent -Path $candidate
        if ($null -ne $candidateUsedPercent -and $candidateUsedPercent -le $MaxDataDriveUsedPercent) {
            return Join-Path (ConvertTo-FullPath -Path $candidate) "$Mode-$runStamp"
        }
    }

    return $defaultDataRoot
}

function Resolve-JavaHome {
    param([string]$Candidate)

    if (-not [string]::IsNullOrWhiteSpace($Candidate)) {
        $javaExe = Join-Path $Candidate "bin\java.exe"
        if (Test-Path $javaExe) {
            return (Resolve-Path $Candidate).Path
        }
    }

    $javaCommand = Get-Command java.exe -ErrorAction SilentlyContinue
    if ($null -ne $javaCommand) {
        $startInfo = New-Object System.Diagnostics.ProcessStartInfo
        $startInfo.FileName = $javaCommand.Source
        $startInfo.Arguments = "-XshowSettings:properties -version"
        $startInfo.UseShellExecute = $false
        $startInfo.RedirectStandardOutput = $true
        $startInfo.RedirectStandardError = $true
        $startInfo.CreateNoWindow = $true
        $process = New-Object System.Diagnostics.Process
        $process.StartInfo = $startInfo
        [void]$process.Start()
        $outputText = $process.StandardOutput.ReadToEnd() + [Environment]::NewLine + $process.StandardError.ReadToEnd()
        $process.WaitForExit()
        $output = $outputText -split "\r?\n"
        foreach ($line in $output) {
            if ($line -match '^\s*java\.home\s*=\s*(.+?)\s*$') {
                $candidateHome = $Matches[1].Trim()
                if (Test-Path (Join-Path $candidateHome "bin\java.exe")) {
                    return (Resolve-Path $candidateHome).Path
                }
            }
        }
    }

    throw "JAVA_HOME is not set and java.home could not be inferred from java.exe. Pass -JavaHome."
}

function Write-JavaNamesrvConfig {
    param([string]$Path)

    $content = @"
listenPort=$NamesrvPort
kvConfigPath=$((Join-Path $dataRoot 'java-namesrv-kv.json') -replace '\\','/')
configStorePath=$($Path -replace '\\','/')
"@
    Write-AsciiFile -Path $Path -Content $content
}

function Start-LoggedProcess {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$WorkingDirectory,
        [string]$LogPath,
        [hashtable]$Environment = @{}
    )

    $display = "$FilePath $($ArgumentList -join ' ')"
    if ($DryRun) {
        Write-Host "[dry-run] $display"
        return $null
    }

    $originalEnvironment = @{}
    foreach ($entry in $Environment.GetEnumerator()) {
        $originalEnvironment[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, "Process")
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, "Process")
    }

    try {
        $process = Start-Process `
            -FilePath $FilePath `
            -ArgumentList $ArgumentList `
            -WorkingDirectory $WorkingDirectory `
            -RedirectStandardOutput $LogPath `
            -RedirectStandardError "$LogPath.err" `
            -WindowStyle Hidden `
            -PassThru
    }
    finally {
        foreach ($entry in $Environment.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($entry.Key, $originalEnvironment[$entry.Key], "Process")
        }
    }

    Write-Host "[started] pid=$($process.Id) cmd=$display"
    return $process
}

function Wait-ForTcpPort {
    param(
        [string]$EndpointHost,
        [int]$Port,
        [int]$TimeoutSeconds = 180
    )

    if ($DryRun) {
        Write-Host "[dry-run] wait for $EndpointHost`:$Port"
        return
    }

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $client = New-Object System.Net.Sockets.TcpClient
        try {
            $asyncResult = $client.BeginConnect($EndpointHost, $Port, $null, $null)
            if ($asyncResult.AsyncWaitHandle.WaitOne(1000) -and $client.Connected) {
                $client.EndConnect($asyncResult)
                $client.Dispose()
                return
            }
        }
        catch {
        }
        finally {
            $client.Dispose()
        }
        Start-Sleep -Milliseconds 500
    }

    throw "Timed out waiting for $EndpointHost`:$Port"
}

function Invoke-LoggedCommand {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$WorkingDirectory,
        [string]$OutputPath,
        [hashtable]$Environment = @{}
    )

    $display = "$FilePath $($ArgumentList -join ' ')"
    if ($DryRun) {
        Write-Host "[dry-run] $display"
        return ""
    }

    $originalEnvironment = @{}
    foreach ($entry in $Environment.GetEnumerator()) {
        $originalEnvironment[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, "Process")
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, "Process")
    }

    $stdoutPath = "$OutputPath.stdout.tmp"
    $stderrPath = "$OutputPath.stderr.tmp"

    try {
        $process = Start-Process `
            -FilePath $FilePath `
            -ArgumentList $ArgumentList `
            -WorkingDirectory $WorkingDirectory `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath `
            -WindowStyle Hidden `
            -PassThru `
            -Wait

        $stdoutText = if (Test-Path $stdoutPath) { [System.IO.File]::ReadAllText($stdoutPath) } else { "" }
        $stderrText = if (Test-Path $stderrPath) { [System.IO.File]::ReadAllText($stderrPath) } else { "" }
        $output = $stdoutText
        if (-not [string]::IsNullOrEmpty($stderrText)) {
            if (-not [string]::IsNullOrEmpty($output) -and -not $output.EndsWith([Environment]::NewLine)) {
                $output += [Environment]::NewLine
            }
            $output += $stderrText
        }

        [System.IO.File]::WriteAllText($OutputPath, $output, [System.Text.Encoding]::UTF8)
        if (-not [string]::IsNullOrEmpty($output)) {
            Write-Host -NoNewline $output
        }
        if ($process.ExitCode -ne 0) {
            throw "Command failed with exit code $($process.ExitCode): $display"
        }
        return $output
    }
    finally {
        Remove-Item -LiteralPath $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
        foreach ($entry in $Environment.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($entry.Key, $originalEnvironment[$entry.Key], "Process")
        }
    }
}

function Invoke-LoggedCommandUntilMatch {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$WorkingDirectory,
        [string]$OutputPath,
        [hashtable]$Environment = @{},
        [string[]]$RequiredValues,
        [string]$Description,
        [int]$MaxAttempts = 12,
        [int]$DelaySeconds = 5
    )

    $lastOutput = ""
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        if ($attempt -gt 1) {
            Write-Host "Retrying $Description ($attempt/$MaxAttempts)..."
        }

        $lastOutput = Invoke-LoggedCommand `
            -FilePath $FilePath `
            -ArgumentList $ArgumentList `
            -WorkingDirectory $WorkingDirectory `
            -OutputPath $OutputPath `
            -Environment $Environment

        $missingValues = @()
        foreach ($value in $RequiredValues) {
            if ($lastOutput -notmatch [regex]::Escape($value)) {
                $missingValues += $value
            }
        }

        if ($missingValues.Count -eq 0) {
            return $lastOutput
        }

        if ($attempt -lt $MaxAttempts) {
            Start-Sleep -Seconds $DelaySeconds
        }
    }

    throw "$Description output does not contain required value(s): $($missingValues -join ', ')."
}

function Stop-ProcessTree {
    param([Parameter(Mandatory = $true)][int]$ProcessId)

    $children = Get-CimInstance Win32_Process -Filter "ParentProcessId = $ProcessId" -ErrorAction SilentlyContinue
    foreach ($child in $children) {
        Stop-ProcessTree -ProcessId ([int]$child.ProcessId)
    }

    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

function Stop-StartedProcesses {
    foreach ($process in $processes) {
        if ($null -ne $process -and -not $process.HasExited) {
            Stop-ProcessTree -ProcessId $process.Id
        }
    }
}

function Write-JavaBrokerConfig {
    param([string]$Path)

    $content = @"
brokerClusterName=$ClusterName
brokerName=$BrokerName
brokerId=0
brokerIP1=$NamesrvHost
listenPort=$BrokerPort
namesrvAddr=$namesrvAddr
autoCreateTopicEnable=true
recallMessageEnable=true
deleteWhen=04
fileReservedTime=1
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH
storePathRootDir=$($dataRoot -replace '\\','/')
storePathCommitLog=$((Join-Path $dataRoot 'commitlog') -replace '\\','/')
storePathConsumeQueue=$((Join-Path $dataRoot 'consumequeue') -replace '\\','/')
"@
    Write-AsciiFile -Path $Path -Content $content
}

function Write-RustBrokerConfig {
    param([string]$Path)

    $content = @"
namesrvAddr = "$namesrvAddr"
brokerIp1 = "$NamesrvHost"
listenPort = $BrokerPort
storePathRootDir = "$($dataRoot -replace '\\','/')"
storePathCommitLog = "$((Join-Path $dataRoot 'commitlog') -replace '\\','/')"
enableControllerMode = false

[brokerServerConfig]
listenPort = $BrokerPort
bindAddress = "0.0.0.0"

[brokerIdentity]
brokerName = "$BrokerName"
brokerClusterName = "$ClusterName"
brokerId = 0
"@
    Write-AsciiFile -Path $Path -Content $content
}

$dataRoot = Resolve-TestDataRoot
New-Directory $runRoot
New-Directory $logRoot
New-Directory $dataRoot

$javaBinRoot = if ($JavaRocketmqHome) { Join-Path $JavaRocketmqHome "bin" } else { $null }

if ($Mode -match "java" -or -not [string]::IsNullOrWhiteSpace($JavaRocketmqHome)) {
    if ([string]::IsNullOrWhiteSpace($JavaRocketmqHome)) {
        throw "JavaRocketmqHome is required for mode '$Mode'."
    }
    if (-not (Test-Path (Join-Path $javaBinRoot "mqadmin.cmd"))) {
        throw "JavaRocketmqHome does not look like a RocketMQ distribution home: $JavaRocketmqHome"
    }
    $script:ResolvedJavaHome = Resolve-JavaHome -Candidate $JavaHome
}

try {
    switch ($Mode) {
        "rust-namesrv-java-broker" {
            $rustNamesrvLog = Join-Path $logRoot "rust-namesrv.log"
            $processes += Start-LoggedProcess `
                -FilePath "cargo" `
                -ArgumentList @(
                    "run", "-p", "rocketmq-namesrv", "--bin", "rocketmq-namesrv-rust", "--",
                    "--listenPort", $NamesrvPort,
                    "--bindAddress", $NamesrvHost
                ) `
                -WorkingDirectory $workspaceRoot `
                -LogPath $rustNamesrvLog
            Wait-ForTcpPort -EndpointHost $NamesrvHost -Port $NamesrvPort

            $javaBrokerConfig = Join-Path $runRoot "java-broker.properties"
            Write-JavaBrokerConfig -Path $javaBrokerConfig
            $javaBrokerLog = Join-Path $logRoot "java-broker.log"
            $processes += Start-LoggedProcess `
                -FilePath "cmd.exe" `
                -ArgumentList @("/c", (Join-Path $javaBinRoot "mqbroker.cmd"), "-c", $javaBrokerConfig, "-n", $namesrvAddr) `
                -WorkingDirectory $javaBinRoot `
                -LogPath $javaBrokerLog `
                -Environment @{ ROCKETMQ_HOME = $JavaRocketmqHome; JAVA_HOME = $script:ResolvedJavaHome }
            Wait-ForTcpPort -EndpointHost $NamesrvHost -Port $BrokerPort
        }

        "java-namesrv-rust-broker" {
            $javaNamesrvConfig = Join-Path $runRoot "java-namesrv.properties"
            Write-JavaNamesrvConfig -Path $javaNamesrvConfig
            $javaNamesrvLog = Join-Path $logRoot "java-namesrv.log"
            $processes += Start-LoggedProcess `
                -FilePath "cmd.exe" `
                -ArgumentList @("/c", (Join-Path $javaBinRoot "mqnamesrv.cmd"), "-c", $javaNamesrvConfig) `
                -WorkingDirectory $javaBinRoot `
                -LogPath $javaNamesrvLog `
                -Environment @{ ROCKETMQ_HOME = $JavaRocketmqHome; JAVA_HOME = $script:ResolvedJavaHome }
            Wait-ForTcpPort -EndpointHost $NamesrvHost -Port $NamesrvPort

            $rustBrokerConfig = Join-Path $runRoot "rust-broker.toml"
            Write-RustBrokerConfig -Path $rustBrokerConfig
            $rustBrokerLog = Join-Path $logRoot "rust-broker.log"
            $processes += Start-LoggedProcess `
                -FilePath "cargo" `
                -ArgumentList @(
                    "run", "-p", "rocketmq-broker", "--bin", "rocketmq-broker-rust", "--",
                    "-c", $rustBrokerConfig,
                    "-n", $namesrvAddr
                ) `
                -WorkingDirectory $workspaceRoot `
                -LogPath $rustBrokerLog `
                -Environment @{ ROCKETMQ_HOME = $runRoot }
            Wait-ForTcpPort -EndpointHost $NamesrvHost -Port $BrokerPort
        }
    }

    $clusterListOutput = Invoke-LoggedCommandUntilMatch `
        -FilePath "cmd.exe" `
        -ArgumentList @("/c", (Join-Path $javaBinRoot "mqadmin.cmd"), "clusterList", "-n", $namesrvAddr) `
        -WorkingDirectory $javaBinRoot `
        -OutputPath (Join-Path $runRoot "clusterList.txt") `
        -Environment @{ ROCKETMQ_HOME = $JavaRocketmqHome; JAVA_HOME = $script:ResolvedJavaHome } `
        -RequiredValues @($ClusterName, $BrokerName) `
        -Description "clusterList"

    $topicUpdateOutput = Invoke-LoggedCommand `
        -FilePath "cmd.exe" `
        -ArgumentList @("/c", (Join-Path $javaBinRoot "mqadmin.cmd"), "updateTopic", "-n", $namesrvAddr, "-b", $brokerAddr, "-t", $Topic, "-r", "4", "-w", "4") `
        -WorkingDirectory $javaBinRoot `
        -OutputPath (Join-Path $runRoot "updateTopic.txt") `
        -Environment @{ ROCKETMQ_HOME = $JavaRocketmqHome; JAVA_HOME = $script:ResolvedJavaHome }

    $topicRouteOutput = Invoke-LoggedCommandUntilMatch `
        -FilePath "cmd.exe" `
        -ArgumentList @("/c", (Join-Path $javaBinRoot "mqadmin.cmd"), "topicRoute", "-n", $namesrvAddr, "-t", $Topic) `
        -WorkingDirectory $javaBinRoot `
        -OutputPath (Join-Path $runRoot "topicRoute.txt") `
        -Environment @{ ROCKETMQ_HOME = $JavaRocketmqHome; JAVA_HOME = $script:ResolvedJavaHome } `
        -RequiredValues @($BrokerName, $brokerAddr) `
        -Description "topicRoute"

    if (-not $DryRun) {
        if ($clusterListOutput -notmatch [regex]::Escape($ClusterName)) {
            throw "clusterList output does not contain cluster '$ClusterName'."
        }
        if ($clusterListOutput -notmatch [regex]::Escape($BrokerName)) {
            throw "clusterList output does not contain broker '$BrokerName'."
        }
        if ($topicUpdateOutput -notmatch [regex]::Escape($Topic)) {
            throw "updateTopic output does not mention topic '$Topic'."
        }
        if ($topicRouteOutput -notmatch [regex]::Escape($BrokerName)) {
            throw "topicRoute output does not contain broker '$BrokerName'."
        }
        if ($topicRouteOutput -notmatch [regex]::Escape($brokerAddr)) {
            throw "topicRoute output does not contain broker address '$brokerAddr'."
        }
    }

    Write-Host "Parity matrix run complete. Artifacts: $runRoot"
}
finally {
    if (-not $DryRun) {
        Stop-StartedProcesses
    }
}

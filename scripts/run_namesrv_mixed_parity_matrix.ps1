[CmdletBinding()]
param(
    [ValidateSet("rust-namesrv-java-broker", "java-namesrv-rust-broker")]
    [string]$Mode = "rust-namesrv-java-broker",
    [string]$JavaRocketmqHome,
    [string]$NamesrvHost = "127.0.0.1",
    [int]$NamesrvPort = 19876,
    [int]$BrokerPort = 10911,
    [string]$Topic = "Phase5Topic",
    [string]$ClusterName = "Phase5Cluster",
    [string]$BrokerName = "phase5Broker",
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runRoot = Join-Path $workspaceRoot "target\namesrv-parity\$Mode-$runStamp"
$logRoot = Join-Path $runRoot "logs"
$dataRoot = Join-Path $runRoot "data"
$namesrvAddr = "$NamesrvHost`:$NamesrvPort"
$brokerAddr = "$NamesrvHost`:$BrokerPort"
$processes = @()

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

    try {
        $output = & $FilePath @ArgumentList 2>&1
        $output | Tee-Object -FilePath $OutputPath | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw "Command failed: $display"
        }
        return ($output -join [Environment]::NewLine)
    }
    finally {
        foreach ($entry in $Environment.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($entry.Key, $originalEnvironment[$entry.Key], "Process")
        }
    }
}

function Stop-StartedProcesses {
    foreach ($process in $processes) {
        if ($null -ne $process -and -not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
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
[broker_identity]
broker_name = "$BrokerName"
broker_cluster_name = "$ClusterName"
broker_id = 0

namesrv_addr = "$namesrvAddr"
broker_ip1 = "$NamesrvHost"
listen_port = $BrokerPort
store_path_root_dir = "$($dataRoot -replace '\\','/')"
enable_controller_mode = false
"@
    Write-AsciiFile -Path $Path -Content $content
}

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
                -Environment @{ ROCKETMQ_HOME = $JavaRocketmqHome }
            Wait-ForTcpPort -EndpointHost $NamesrvHost -Port $BrokerPort
        }

        "java-namesrv-rust-broker" {
            $javaNamesrvLog = Join-Path $logRoot "java-namesrv.log"
            $processes += Start-LoggedProcess `
                -FilePath "cmd.exe" `
                -ArgumentList @("/c", (Join-Path $javaBinRoot "mqnamesrv.cmd")) `
                -WorkingDirectory $javaBinRoot `
                -LogPath $javaNamesrvLog `
                -Environment @{ ROCKETMQ_HOME = $JavaRocketmqHome }
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

    $clusterListOutput = Invoke-LoggedCommand `
        -FilePath "cmd.exe" `
        -ArgumentList @("/c", (Join-Path $javaBinRoot "mqadmin.cmd"), "clusterList", "-n", $namesrvAddr) `
        -WorkingDirectory $javaBinRoot `
        -OutputPath (Join-Path $runRoot "clusterList.txt") `
        -Environment @{ ROCKETMQ_HOME = $JavaRocketmqHome }

    $topicUpdateOutput = Invoke-LoggedCommand `
        -FilePath "cmd.exe" `
        -ArgumentList @("/c", (Join-Path $javaBinRoot "mqadmin.cmd"), "updateTopic", "-n", $namesrvAddr, "-b", $brokerAddr, "-t", $Topic, "-r", "4", "-w", "4") `
        -WorkingDirectory $javaBinRoot `
        -OutputPath (Join-Path $runRoot "updateTopic.txt") `
        -Environment @{ ROCKETMQ_HOME = $JavaRocketmqHome }

    $topicRouteOutput = Invoke-LoggedCommand `
        -FilePath "cmd.exe" `
        -ArgumentList @("/c", (Join-Path $javaBinRoot "mqadmin.cmd"), "topicRoute", "-n", $namesrvAddr, "-t", $Topic) `
        -WorkingDirectory $javaBinRoot `
        -OutputPath (Join-Path $runRoot "topicRoute.txt") `
        -Environment @{ ROCKETMQ_HOME = $JavaRocketmqHome }

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
        if ($topicRouteOutput -notmatch [regex]::Escape($Topic)) {
            throw "topicRoute output does not contain topic '$Topic'."
        }
        if ($topicRouteOutput -notmatch [regex]::Escape($BrokerName)) {
            throw "topicRoute output does not contain broker '$BrokerName'."
        }
    }

    Write-Host "Parity matrix run complete. Artifacts: $runRoot"
}
finally {
    if (-not $DryRun) {
        Stop-StartedProcesses
    }
}

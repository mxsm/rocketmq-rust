# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [string]$DatabaseUrl = 'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:5432/rocketmq_sre',
    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',
    [string]$ClusterTargetDir = 'D:\BuildCache\rocketmq-sre-phase4-cluster-target',
    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',
    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',
    [ValidateRange(1024, 65535)]
    [int]$NameServerPort = 29876,
    [ValidateRange(1026, 65534)]
    [int]$BrokerPort = 30911
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '..'))

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Description
    )

    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

function Assert-DataPath([string]$Path, [string]$Description) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetPathRoot($fullPath)
    if (
        -not $root.Equals('D:\', [StringComparison]::OrdinalIgnoreCase) -and
        -not $root.Equals('F:\', [StringComparison]::OrdinalIgnoreCase)
    ) {
        throw "$Description must use the D or F drive."
    }
}

function Ensure-CargoCapacity([string]$TargetDirectory) {
    $targetDriveName = [IO.Path]::GetPathRoot(
        [IO.Path]::GetFullPath($TargetDirectory)
    ).TrimEnd('\').TrimEnd(':')
    $targetFreeGiB = (Get-PSDrive -Name $targetDriveName).Free / 1GB
    Write-Host "${targetDriveName}_FREE_GIB=$([Math]::Round($targetFreeGiB, 2))"
    if ($targetFreeGiB -lt 15) {
        Invoke-Native cargo @(
            '+1.95.0', 'clean',
            '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
            '--target-dir', $TargetDirectory
        ) 'low-space Cargo cleanup'
    }
}

function Assert-PortAvailable([int]$Port) {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $Port)
    try {
        $listener.Start()
    }
    catch {
        throw "Loopback port $Port is already in use."
    }
    finally {
        $listener.Stop()
    }
}

function Wait-ProcessPort([Diagnostics.Process]$Process, [int]$Port, [string]$ErrorLog) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(90)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        if ($Process.HasExited) {
            $tail = if (Test-Path -LiteralPath $ErrorLog -PathType Leaf) {
                (Get-Content -LiteralPath $ErrorLog -Tail 60) -join [Environment]::NewLine
            }
            else {
                '<no process error log>'
            }
            throw "Process $($Process.Id) exited before port $Port became ready.`n$tail"
        }
        $client = [Net.Sockets.TcpClient]::new()
        try {
            $client.Connect([Net.IPAddress]::Loopback, $Port)
            return
        }
        catch {
            Start-Sleep -Milliseconds 250
        }
        finally {
            $client.Dispose()
        }
    }
    throw "Timed out waiting for loopback port $Port."
}

function Stop-OwnedProcess([Diagnostics.Process]$Process) {
    if ($null -eq $Process -or $Process.HasExited) {
        return
    }
    Stop-Process -Id $Process.Id -Force
    $Process.WaitForExit(10000) | Out-Null
}

function Restore-Environment([hashtable]$SavedEnvironment) {
    foreach ($entry in $SavedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}

function New-TestSecret {
    $bytes = [byte[]]::new(32)
    $generator = [Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($bytes)
    }
    finally {
        $generator.Dispose()
    }
    return ([BitConverter]::ToString($bytes) -replace '-', '').ToLowerInvariant()
}

foreach ($path in @(
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $ClusterTargetDir; Description = 'test-cluster Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' }
)) {
    Assert-DataPath $path.Value $path.Description
}

foreach ($port in @(
    $NameServerPort,
    ($NameServerPort + 1000),
    ($NameServerPort + 1001),
    ($BrokerPort - 2),
    $BrokerPort,
    ($BrokerPort + 1)
)) {
    Assert-PortAvailable $port
}

New-Item -ItemType Directory -Force -Path $CargoTargetDir, $ClusterTargetDir, $CargoHome, $TemporaryRoot | Out-Null
Ensure-CargoCapacity $CargoTargetDir

$runRoot = [IO.Path]::GetFullPath(
    (Join-Path $TemporaryRoot "phase04-logger-ttl-$([Guid]::NewGuid().ToString('N'))")
)
$expectedTemporaryRoot = [IO.Path]::GetFullPath($TemporaryRoot).TrimEnd('\') + '\'
if (-not $runRoot.StartsWith($expectedTemporaryRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Smoke runtime directory escaped the configured temporary root.'
}
$namesrvData = Join-Path $runRoot 'namesrv'
$brokerData = Join-Path $runRoot 'broker'
$authData = Join-Path $brokerData 'auth'
New-Item -ItemType Directory -Force -Path $namesrvData, $brokerData, $authData | Out-Null

$readAccessKey = 'phase04-logger-reader'
$readSecretKey = New-TestSecret
$mutationAccessKey = 'phase04-logger-writer'
$mutationSecretKey = New-TestSecret
$aclPath = Join-Path $runRoot 'broker-acl.yml'
$acl = @"
globalWhiteRemoteAddresses: []
accounts:
  - accessKey: $readAccessKey
    secretKey: $readSecretKey
    admin: false
    defaultTopicPerm: GET
    defaultGroupPerm: GET
    clusterPerm: GET
  - accessKey: $mutationAccessKey
    secretKey: $mutationSecretKey
    admin: true
    defaultTopicPerm: DENY
    defaultGroupPerm: DENY
"@
[IO.File]::WriteAllText($aclPath, $acl, [Text.UTF8Encoding]::new($false))

$brokerConfigPath = Join-Path $runRoot 'broker.toml'
$brokerRootForToml = $brokerData.Replace('\', '/')
$authPathForToml = (Join-Path $authData 'auth.json').Replace('\', '/')
$aclPathForToml = $aclPath.Replace('\', '/')
$brokerConfig = @"
[broker]
listenPort = $BrokerPort
brokerIp1 = "127.0.0.1"
brokerIp2 = "127.0.0.1"
storePathRootDir = "$brokerRootForToml"
namesrvAddr = "127.0.0.1:$NameServerPort"
autoCreateTopicEnable = false
authenticationEnabled = true
authorizationEnabled = true
signatureAlgorithm = "HmacSHA256"
authConfigPath = "$authPathForToml"
aclFile = "$aclPathForToml"
aclFileWatchEnabled = false

[broker.brokerServerConfig]
bindAddress = "127.0.0.1"

[broker.brokerIdentity]
brokerName = "sre-phase04-logger-broker"
brokerClusterName = "SrePhase04Logger"
brokerId = 0

[logging.reload]
enabled = true

[store]
storePathRootDir = "$brokerRootForToml"
haListenAddress = "127.0.0.1"
haListenPort = $($BrokerPort + 1)
mappedFileSizeCommitLog = 1048576
mappedFileSizeConsumeQueue = 6000
mappedFileSizeConsumeQueueExt = 65536
maxHashSlotNum = 1000
maxIndexNum = 4000
timerWheelEnable = false

[observability.metrics]
exporter = "disable"
"@
[IO.File]::WriteAllText($brokerConfigPath, $brokerConfig, [Text.UTF8Encoding]::new($false))

$environmentNames = @(
    'CARGO_HOME',
    'CARGO_TARGET_DIR',
    'TEMP',
    'TMP',
    'ROCKETMQ_HOME',
    'ROCKETMQ_SECURITY_PROFILE',
    'ROCKETMQ_HEALTH_BIND_ADDR',
    'ROCKETMQ_SRE_TEST_DATABASE_URL',
    'ROCKETMQ_SRE_TEST_NAMESRV_ADDR',
    'ROCKETMQ_SRE_TEST_BROKER_ADDR',
    'ROCKETMQ_SRE_TEST_BROKER_READ_ACCESS_KEY',
    'ROCKETMQ_SRE_TEST_BROKER_READ_SECRET_KEY',
    'ROCKETMQ_SRE_TEST_BROKER_MUTATION_ACCESS_KEY',
    'ROCKETMQ_SRE_TEST_BROKER_MUTATION_SECRET_KEY',
    'RUST_LOG'
)
$savedEnvironment = @{}
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$namesrvProcess = $null
$brokerProcess = $null
$namesrvOut = ''
$namesrvErr = ''
$brokerOut = ''
$brokerErr = ''
$smokeSucceeded = $false
try {
    $env:CARGO_HOME = $CargoHome
    $env:CARGO_TARGET_DIR = $ClusterTargetDir
    $env:TEMP = $TemporaryRoot
    $env:TMP = $TemporaryRoot
    Set-Location $repositoryRoot
    Ensure-CargoCapacity $ClusterTargetDir
    Invoke-Native cargo @(
        '+1.95.0', 'build', '--locked',
        '--package', 'rocketmq-namesrv',
        '--bin', 'rocketmq-namesrv-rust',
        '--package', 'rocketmq-broker',
        '--bin', 'rocketmq-broker-rust'
    ) 'RocketMQ logger test-cluster build'

    $namesrvBinary = Join-Path $ClusterTargetDir 'debug/rocketmq-namesrv-rust.exe'
    $brokerBinary = Join-Path $ClusterTargetDir 'debug/rocketmq-broker-rust.exe'
    foreach ($binary in @($namesrvBinary, $brokerBinary)) {
        if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
            throw "Expected test-cluster binary is missing: $binary"
        }
    }

    $env:ROCKETMQ_SECURITY_PROFILE = 'development-insecure-loopback'
    $env:RUST_LOG = 'info'
    $env:ROCKETMQ_HOME = $namesrvData
    $env:ROCKETMQ_HEALTH_BIND_ADDR = "127.0.0.1:$($NameServerPort + 1000)"
    $namesrvOut = Join-Path $runRoot 'namesrv.out.log'
    $namesrvErr = Join-Path $runRoot 'namesrv.err.log'
    $namesrvProcess = Start-Process `
        -FilePath $namesrvBinary `
        -ArgumentList @(
            '--listenPort', $NameServerPort,
            '--bindAddress', '127.0.0.1',
            '--rocketmqHome', $namesrvData,
            '--kvConfigPath', (Join-Path $namesrvData 'kvConfig.json')
        ) `
        -RedirectStandardOutput $namesrvOut `
        -RedirectStandardError $namesrvErr `
        -WindowStyle Hidden `
        -PassThru
    Wait-ProcessPort $namesrvProcess $NameServerPort $namesrvErr

    $env:ROCKETMQ_HOME = $brokerData
    $env:ROCKETMQ_HEALTH_BIND_ADDR = "127.0.0.1:$($NameServerPort + 1001)"
    $brokerOut = Join-Path $runRoot 'broker.out.log'
    $brokerErr = Join-Path $runRoot 'broker.err.log'
    $brokerProcess = Start-Process `
        -FilePath $brokerBinary `
        -ArgumentList @('--configFile', $brokerConfigPath) `
        -RedirectStandardOutput $brokerOut `
        -RedirectStandardError $brokerErr `
        -WindowStyle Hidden `
        -PassThru
    Wait-ProcessPort $brokerProcess $BrokerPort $brokerErr

    $env:ROCKETMQ_SRE_TEST_DATABASE_URL = $DatabaseUrl
    $env:ROCKETMQ_SRE_TEST_NAMESRV_ADDR = "127.0.0.1:$NameServerPort"
    $env:ROCKETMQ_SRE_TEST_BROKER_ADDR = "127.0.0.1:$BrokerPort"
    $env:ROCKETMQ_SRE_TEST_BROKER_READ_ACCESS_KEY = $readAccessKey
    $env:ROCKETMQ_SRE_TEST_BROKER_READ_SECRET_KEY = $readSecretKey
    $env:ROCKETMQ_SRE_TEST_BROKER_MUTATION_ACCESS_KEY = $mutationAccessKey
    $env:ROCKETMQ_SRE_TEST_BROKER_MUTATION_SECRET_KEY = $mutationSecretKey
    $env:CARGO_TARGET_DIR = $CargoTargetDir
    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
        '--locked',
        '--package', 'rocketmq-sre-execution-agent',
        'real_broker_logger_ttl_applies_verifies_and_restores',
        '--',
        '--ignored',
        '--test-threads=1'
    ) 'real Broker logger-TTL smoke'
    $smokeSucceeded = $true
    Write-Host 'PHASE04_LOGGER_TTL_SMOKE_OK apply=true verify=true restore=true read_identity_isolated=true'
}
finally {
    Stop-OwnedProcess $brokerProcess
    Stop-OwnedProcess $namesrvProcess
    if (Test-Path -LiteralPath $ClusterTargetDir -PathType Container) {
        & cargo '+1.95.0' clean `
            '--manifest-path' (Join-Path $repositoryRoot 'Cargo.toml') `
            '--target-dir' $ClusterTargetDir
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Failed to clean the owned test-cluster target directory: $ClusterTargetDir"
        }
    }
    if (-not $smokeSucceeded) {
        foreach ($logPath in @($brokerErr, $brokerOut, $namesrvErr, $namesrvOut)) {
            if (-not [string]::IsNullOrWhiteSpace($logPath) -and (Test-Path -LiteralPath $logPath -PathType Leaf)) {
                Write-Warning "Failure diagnostics from $([IO.Path]::GetFileName($logPath)):"
                Get-Content -LiteralPath $logPath -Tail 80 | Write-Warning
            }
        }
    }
    Restore-Environment $savedEnvironment
    Set-Location $repositoryRoot
    if (Test-Path -LiteralPath $runRoot -PathType Container) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
}

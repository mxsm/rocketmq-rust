# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [string]$DatabaseUrl = 'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:5432/rocketmq_sre',
    [string]$CargoTargetDir = 'G:\rocketmq-sre-phase2-cargo-target',
    [string]$CargoHome = 'G:\rocketmq-sre-phase1-cargo-home',
    [string]$TemporaryRoot = 'G:\rocketmq-sre-phase2-temp',
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

function Assert-NonSystemBuildPath([string]$Path, [string]$Description) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    if ([IO.Path]::GetPathRoot($fullPath).Equals('C:\', [StringComparison]::OrdinalIgnoreCase)) {
        throw "$Description must not use the C drive."
    }
}

function Ensure-CargoCapacity {
    $dDrive = Get-PSDrive -Name D
    $gDrive = Get-PSDrive -Name G
    Write-Host "D_FREE_GIB=$([Math]::Round($dDrive.Free / 1GB, 2))"
    Write-Host "G_FREE_GIB=$([Math]::Round($gDrive.Free / 1GB, 2))"
    if (($dDrive.Free / 1GB) -lt 15 -or ($gDrive.Free / 1GB) -lt 15) {
        Invoke-Native cargo @(
            '+1.95.0', 'clean',
            '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
            '--target-dir', $CargoTargetDir
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
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    return [Convert]::ToHexString($bytes).ToLowerInvariant()
}

foreach ($path in @(
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' }
)) {
    Assert-NonSystemBuildPath $path.Value $path.Description
}

foreach ($port in @($NameServerPort, $NameServerPort + 1000, $NameServerPort + 1001, $BrokerPort - 2, $BrokerPort, $BrokerPort + 1)) {
    Assert-PortAvailable $port
}

New-Item -ItemType Directory -Force -Path $CargoTargetDir, $CargoHome, $TemporaryRoot | Out-Null
Ensure-CargoCapacity

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
authConfigPath = "$authPathForToml"
aclFile = "$aclPathForToml"
aclFileWatchEnabled = false
metricsExporterType = "disable"

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
    $env:CARGO_TARGET_DIR = $CargoTargetDir
    $env:TEMP = $TemporaryRoot
    $env:TMP = $TemporaryRoot
    Set-Location $repositoryRoot
    Invoke-Native cargo @(
        '+1.95.0', 'build', '--locked',
        '--package', 'rocketmq-namesrv',
        '--bin', 'rocketmq-namesrv-rust',
        '--package', 'rocketmq-broker',
        '--bin', 'rocketmq-broker-rust'
    ) 'RocketMQ logger test-cluster build'
    Ensure-CargoCapacity

    $namesrvBinary = Join-Path $CargoTargetDir 'debug/rocketmq-namesrv-rust.exe'
    $brokerBinary = Join-Path $CargoTargetDir 'debug/rocketmq-broker-rust.exe'
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

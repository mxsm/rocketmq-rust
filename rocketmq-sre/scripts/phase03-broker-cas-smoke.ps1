# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [string]$DatabaseUrl = 'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:5432/rocketmq_sre',
    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',
    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',
    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',
    [ValidateRange(1024, 65535)]
    [int]$NameServerPort = 19876,
    [ValidateRange(1026, 65535)]
    [int]$BrokerPort = 20911
)

$ErrorActionPreference = 'Stop'
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
            $tail = if (Test-Path -LiteralPath $ErrorLog) {
                (Get-Content -LiteralPath $ErrorLog -Tail 40) -join [Environment]::NewLine
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

foreach ($path in @(
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' }
)) {
    Assert-NonSystemBuildPath $path.Value $path.Description
}

if ($BrokerPort -le 2) {
    throw 'BrokerPort must leave room for the fast remoting listener.'
}
Assert-PortAvailable $NameServerPort
Assert-PortAvailable $BrokerPort
Assert-PortAvailable ($BrokerPort - 2)
Assert-PortAvailable ($BrokerPort + 1)

New-Item -ItemType Directory -Force -Path $CargoTargetDir, $CargoHome, $TemporaryRoot | Out-Null
$targetDrive = Get-PSDrive -Name ([IO.Path]::GetPathRoot([IO.Path]::GetFullPath($CargoTargetDir)).Substring(0, 1))
$targetFreeGiB = $targetDrive.Free / 1GB
$targetFreePercent = 100 * $targetDrive.Free / ($targetDrive.Free + $targetDrive.Used)
if ($targetFreeGiB -lt 30 -or $targetFreePercent -lt 15) {
    Invoke-Native cargo @(
        '+1.95.0', 'clean',
        '--target-dir', $CargoTargetDir
    ) 'Broker-reserve Cargo cleanup'
}

$runRoot = Join-Path $TemporaryRoot "phase03-broker-cas-$([Guid]::NewGuid().ToString('N'))"
$runRoot = [IO.Path]::GetFullPath($runRoot)
$expectedTemporaryRoot = [IO.Path]::GetFullPath($TemporaryRoot).TrimEnd('\') + '\'
if (-not $runRoot.StartsWith($expectedTemporaryRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Smoke runtime directory escaped the configured temporary root.'
}
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null
$namesrvData = Join-Path $runRoot 'namesrv'
$brokerData = Join-Path $runRoot 'broker'
New-Item -ItemType Directory -Force -Path $namesrvData, $brokerData | Out-Null

$brokerConfigPath = Join-Path $runRoot 'broker.toml'
$brokerRootForToml = $brokerData.Replace('\', '/')
$brokerConfig = @"
[broker]
listenPort = $BrokerPort
brokerIp1 = "127.0.0.1"
brokerIp2 = "127.0.0.1"
storePathRootDir = "$brokerRootForToml"
namesrvAddr = "127.0.0.1:$NameServerPort"
autoCreateTopicEnable = false
authenticationEnabled = false
authorizationEnabled = false

[broker.brokerServerConfig]
bindAddress = "127.0.0.1"

[broker.brokerIdentity]
brokerName = "sre-phase03-cas-broker"
brokerClusterName = "SrePhase03Cas"
brokerId = 0

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

$savedEnvironment = @{}
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
    'RUST_LOG'
)
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$namesrvProcess = $null
$brokerProcess = $null
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
    ) 'RocketMQ test-cluster build'

    $targetDrive = Get-PSDrive -Name (
        [IO.Path]::GetPathRoot([IO.Path]::GetFullPath($CargoTargetDir)).Substring(0, 1)
    )
    $targetFreeGiB = $targetDrive.Free / 1GB
    $targetFreePercent = 100 * $targetDrive.Free / ($targetDrive.Free + $targetDrive.Used)
    if ($targetFreeGiB -lt 15 -or $targetFreePercent -lt 12) {
        Invoke-Native cargo @(
            '+1.95.0', 'clean',
            '--target-dir', $CargoTargetDir
        ) 'post-build low-space Cargo cleanup'
        throw (
            'The Broker CAS build left insufficient Broker runtime reserve; ' +
            'the owned Cargo target was cleaned. Rerun the smoke test.'
        )
    }

    $namesrvBinary = Join-Path $CargoTargetDir 'debug/rocketmq-namesrv-rust.exe'
    $brokerBinary = Join-Path $CargoTargetDir 'debug/rocketmq-broker-rust.exe'
    foreach ($binary in @($namesrvBinary, $brokerBinary)) {
        if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
            throw "Expected test-cluster binary is missing: $binary"
        }
    }

    $env:ROCKETMQ_SECURITY_PROFILE = 'development-insecure-loopback'
    $env:RUST_LOG = 'warn'
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
    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
        '--locked',
        '--package', 'rocketmq-sre-execution-agent',
        'real_broker_generation_cas_rejects_stale_write_and_rolls_back',
        '--',
        '--ignored',
        '--test-threads=1'
    ) 'real Broker generation-CAS smoke'
    $smokeSucceeded = $true
    Write-Host 'PHASE03_BROKER_CAS_SMOKE_OK generation_advanced=true stale_rejected=true rollback_advanced=true'
}
finally {
    Stop-OwnedProcess $brokerProcess
    Stop-OwnedProcess $namesrvProcess
    if (-not $smokeSucceeded) {
        foreach ($logPath in @($brokerErr, $brokerOut, $namesrvErr, $namesrvOut)) {
            if (-not [string]::IsNullOrWhiteSpace($logPath) -and (Test-Path -LiteralPath $logPath -PathType Leaf)) {
                Write-Warning "Failure diagnostics from $([IO.Path]::GetFileName($logPath)):"
                Get-Content -LiteralPath $logPath -Tail 60 | Write-Warning
            }
        }
    }
    Restore-Environment $savedEnvironment
    Set-Location $repositoryRoot
    if (Test-Path -LiteralPath $runRoot) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
}

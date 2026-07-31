# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Kubeconfig,

    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',

    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',

    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',

    [string]$AdminCliPath = 'D:\BuildCache\rocketmq-sre-target\debug\rocketmq-admin-cli.exe',

    [ValidateRange(1024, 65535)]
    [int]$PostgresLocalPort = 45432,

    [ValidateRange(1024, 65535)]
    [int]$ExecutorLocalPort = 59094,

    [ValidateRange(1024, 65535)]
    [int]$AgentLocalPort = 59095
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '..'))
$manifestPath = Join-Path $sreRoot 'Cargo.toml'
$waveScript = Join-Path $scriptDirectory 'phase03-wave-actions-supervised-e2e.ps1'
$proxyScript = Join-Path $scriptDirectory 'phase03-proxy-restart-e2e.ps1'
$collectorScript = Join-Path $scriptDirectory 'phase04-collector-restart-e2e.ps1'
$credentialScript = Join-Path $scriptDirectory 'phase03-credential-supervised-e2e.ps1'

function Assert-NonSystemPath([string]$Path, [string]$Description) {
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

function Wait-ProcessPort(
    [Diagnostics.Process]$Process,
    [int]$Port,
    [string]$ErrorLog
) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(60)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        if ($Process.HasExited) {
            $tail = if (Test-Path -LiteralPath $ErrorLog -PathType Leaf) {
                (Get-Content -LiteralPath $ErrorLog -Tail 40) -join [Environment]::NewLine
            }
            else {
                '<no port-forward error log>'
            }
            throw "Port-forward process $($Process.Id) exited before port $Port became ready.`n$tail"
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

function Start-PortForward(
    [string]$Namespace,
    [string]$Resource,
    [int]$LocalPort,
    [int]$RemotePort,
    [string]$LogPrefix
) {
    $outLog = Join-Path $runRoot "$LogPrefix.out.log"
    $errorLog = Join-Path $runRoot "$LogPrefix.err.log"
    $process = Start-Process `
        -FilePath 'kubectl' `
        -ArgumentList @(
            '--kubeconfig', $resolvedKubeconfig,
            '-n', $Namespace,
            'port-forward', $Resource,
            "${LocalPort}:${RemotePort}",
            '--address', '127.0.0.1'
        ) `
        -RedirectStandardOutput $outLog `
        -RedirectStandardError $errorLog `
        -WindowStyle Hidden `
        -PassThru
    Wait-ProcessPort $process $LocalPort $errorLog
    return $process
}

function Stop-OwnedProcess([Diagnostics.Process]$Process) {
    if ($null -eq $Process -or $Process.HasExited) {
        return
    }
    Stop-Process -Id $Process.Id -Force
    $Process.WaitForExit(10000) | Out-Null
}

function Invoke-CheckedScript(
    [string]$Path,
    [hashtable]$Parameters,
    [string]$Description
) {
    & $Path @Parameters
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

foreach ($path in @(
    @{ Value = $Kubeconfig; Description = 'Kubernetes kubeconfig' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' },
    @{ Value = $AdminCliPath; Description = 'Admin CLI' }
)) {
    Assert-NonSystemPath $path.Value $path.Description
}
foreach ($port in @($PostgresLocalPort, $ExecutorLocalPort, $AgentLocalPort)) {
    Assert-PortAvailable $port
}
$resolvedKubeconfig = [IO.Path]::GetFullPath($Kubeconfig)
if (-not (Test-Path -LiteralPath $resolvedKubeconfig -PathType Leaf)) {
    throw "Kubernetes kubeconfig does not exist: $resolvedKubeconfig"
}
$resolvedTemporaryRoot = [IO.Path]::GetFullPath($TemporaryRoot)
$resolvedCargoHome = [IO.Path]::GetFullPath($CargoHome)
$resolvedCargoTarget = [IO.Path]::GetFullPath($CargoTargetDir)
$resolvedAdminCli = [IO.Path]::GetFullPath($AdminCliPath)
New-Item -ItemType Directory -Force -Path $resolvedCargoHome, $resolvedCargoTarget, $resolvedTemporaryRoot | Out-Null

$targetDriveName = [IO.Path]::GetPathRoot($resolvedCargoTarget).Substring(0, 1)
$targetDrive = Get-PSDrive -Name $targetDriveName
Write-Host "$($targetDrive.Name)_FREE_GIB=$([Math]::Round($targetDrive.Free / 1GB, 2))"
if (($targetDrive.Free / 1GB) -lt 15) {
    & cargo +1.95.0 clean --manifest-path $manifestPath --target-dir $resolvedCargoTarget
    if ($LASTEXITCODE -ne 0) {
        throw 'Low-space SRE Cargo cleanup failed.'
    }
}
if (-not (Test-Path -LiteralPath $resolvedAdminCli -PathType Leaf)) {
    $savedBuildEnvironment = @{}
    foreach ($name in @('CARGO_HOME', 'CARGO_TARGET_DIR', 'TEMP', 'TMP', 'CARGO_BUILD_JOBS')) {
        $savedBuildEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
    }
    try {
        $env:CARGO_HOME = $resolvedCargoHome
        $env:CARGO_TARGET_DIR = $resolvedCargoTarget
        $env:TEMP = $resolvedTemporaryRoot
        $env:TMP = $resolvedTemporaryRoot
        $env:CARGO_BUILD_JOBS = '1'
        & cargo +1.95.0 build `
            --manifest-path (Join-Path $repositoryRoot 'Cargo.toml') `
            --locked `
            -p rocketmq-admin-cli
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $resolvedAdminCli -PathType Leaf)) {
            throw 'The prebuilt Admin CLI is unavailable after the bounded rebuild.'
        }
    }
    finally {
        foreach ($entry in $savedBuildEnvironment.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
        }
    }
}

$runRoot = [IO.Path]::GetFullPath(
    (Join-Path $resolvedTemporaryRoot "phase03-unified-wave-$([Guid]::NewGuid().ToString('N'))")
)
$expectedTemporaryPrefix = $resolvedTemporaryRoot.TrimEnd('\') + '\'
if (-not $runRoot.StartsWith($expectedTemporaryPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Unified wave runtime directory escaped the configured temporary root.'
}
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

$savedEnvironment = @{}
foreach ($name in @(
    'CARGO_HOME',
    'CARGO_TARGET_DIR',
    'TEMP',
    'TMP',
    'CARGO_BUILD_JOBS',
    'KUBECONFIG',
    'ROCKETMQ_SRE_PHASE3_DATABASE_URL',
    'ROCKETMQ_SRE_PHASE3_EXECUTOR_URL',
    'ROCKETMQ_SRE_PHASE3_AGENT_URL',
    'ROCKETMQ_SRE_PHASE3_WORKLOAD_TOKEN',
    'ROCKETMQ_SRE_PHASE3_SIGNING_KEY',
    'ROCKETMQ_SRE_PHASE3_PROXY_POD',
    'ROCKETMQ_SRE_PHASE3_PROXY_UID',
    'ROCKETMQ_SRE_PHASE4_COLLECTOR_POD',
    'ROCKETMQ_SRE_PHASE4_COLLECTOR_UID'
)) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$postgresForward = $null
$executorForward = $null
$agentForward = $null
try {
    Invoke-CheckedScript $waveScript @{
        Kubeconfig = $resolvedKubeconfig
        CargoHome = $resolvedCargoHome
        CargoTargetDir = $resolvedCargoTarget
        TemporaryRoot = $resolvedTemporaryRoot
    } 'Wave 1 R1 and wave Admin R2 supervised E2E'

    $postgresForward = Start-PortForward `
        'rocketmq-sre' 'service/postgres' $PostgresLocalPort 5432 'postgres'
    $executorForward = Start-PortForward `
        'rocketmq-sre' 'service/sre-executor' $ExecutorLocalPort 8094 'executor'
    $agentForward = Start-PortForward `
        'rocketmq-sre' 'service/sre-execution-agent' $AgentLocalPort 8095 'agent'

    $sharedParameters = @{
        Kubeconfig = $resolvedKubeconfig
        CargoHome = $resolvedCargoHome
        CargoTargetDir = $resolvedCargoTarget
        TempDir = $resolvedTemporaryRoot
        PostgresLocalPort = $PostgresLocalPort
        ExecutorLocalPort = $ExecutorLocalPort
        AgentLocalPort = $AgentLocalPort
    }
    Invoke-CheckedScript $proxyScript $sharedParameters 'Proxy restart supervised E2E'
    Invoke-CheckedScript $collectorScript $sharedParameters 'Collector restart supervised E2E'

    Stop-OwnedProcess $agentForward
    $agentForward = $null
    Stop-OwnedProcess $executorForward
    $executorForward = $null
    Stop-OwnedProcess $postgresForward
    $postgresForward = $null

    Invoke-CheckedScript $credentialScript @{
        Kubeconfig = $resolvedKubeconfig
        CargoHome = $resolvedCargoHome
        CargoTargetDir = $resolvedCargoTarget
        TempDir = $resolvedTemporaryRoot
        AdminCliPath = $resolvedAdminCli
    } 'Credential rotation supervised E2E'

    Write-Host (
        'PHASE03_UNIFIED_WAVE_SUPERVISED_E2E_OK ' +
        'wave1=observability.logger_level_ttl.v1,proxy.scale_out_one.v1,' +
        'proxy.restart_one.v1,broker.config.patch_allowlisted.v1,' +
        'topic.config.patch_allowlisted.v1 ' +
        'wave2=subscription_group.patch_allowlisted.v1,' +
        'security.credential_rotate_overlap.v1,telemetry.collector.restart_one.v1 ' +
        'critic=heterogeneous approval=independent executor=true agent=true ' +
        'verification=true audit=correlated'
    )
}
finally {
    Stop-OwnedProcess $agentForward
    Stop-OwnedProcess $executorForward
    Stop-OwnedProcess $postgresForward
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
    if (
        (Test-Path -LiteralPath $runRoot -PathType Container) -and
        $runRoot.StartsWith($expectedTemporaryPrefix, [StringComparison]::OrdinalIgnoreCase)
    ) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
}

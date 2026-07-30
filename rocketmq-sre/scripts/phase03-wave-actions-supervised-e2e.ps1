# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Kubeconfig,

    [string]$CargoHome = 'G:\rocketmq-sre-phase1-cargo-home',

    [string]$CargoTargetDir = 'G:\rocketmq-sre-phase2-cargo-target',

    [string]$TemporaryRoot = 'G:\rocketmq-sre-phase2-temp',

    [ValidateRange(1024, 65535)]
    [int]$PostgresLocalPort = 35432,

    [ValidateRange(1024, 65535)]
    [int]$ExecutorLocalPort = 58096,

    [ValidateRange(1024, 65535)]
    [int]$AgentLocalPort = 58097
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$manifestPath = Join-Path $sreRoot 'Cargo.toml'
$bootstrapManifest = Join-Path $sreRoot 'deploy\kind\phase03-wave-admin-bootstrap-job.yaml'

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
                '<no process error log>'
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

function Get-DecodedSecretValue(
    [string]$Namespace,
    [string]$Secret,
    [string]$Key
) {
    $encoded = & kubectl `
        --kubeconfig $resolvedKubeconfig `
        -n $Namespace `
        get secret $Secret `
        -o "jsonpath={.data.$Key}"
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($encoded)) {
        throw "Required Kubernetes Secret reference is unavailable: $Namespace/${Secret}:$Key"
    }
    return [Text.Encoding]::UTF8.GetString(
        [Convert]::FromBase64String($encoded)
    )
}

foreach ($path in @(
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' },
    @{ Value = $Kubeconfig; Description = 'Kubernetes kubeconfig' }
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
New-Item -ItemType Directory -Force -Path $CargoHome, $CargoTargetDir, $resolvedTemporaryRoot | Out-Null
$gDrive = Get-PSDrive -Name G
Write-Host "G_FREE_GIB=$([Math]::Round($gDrive.Free / 1GB, 2))"
if (($gDrive.Free / 1GB) -lt 15) {
    & cargo +1.95.0 clean --manifest-path $manifestPath --target-dir $CargoTargetDir
    if ($LASTEXITCODE -ne 0) {
        throw 'Low-space Cargo cleanup failed.'
    }
}

$runRoot = [IO.Path]::GetFullPath(
    (Join-Path $resolvedTemporaryRoot "phase03-wave-actions-$([Guid]::NewGuid().ToString('N'))")
)
$expectedTemporaryPrefix = $resolvedTemporaryRoot.TrimEnd('\') + '\'
if (-not $runRoot.StartsWith($expectedTemporaryPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Wave action runtime directory escaped the configured temporary root.'
}
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

$savedEnvironment = @{}
foreach ($name in @(
    'CARGO_HOME',
    'CARGO_TARGET_DIR',
    'TEMP',
    'TMP',
    'CARGO_BUILD_JOBS',
    'ROCKETMQ_SRE_PHASE3_DATABASE_URL',
    'ROCKETMQ_SRE_PHASE3_EXECUTOR_URL',
    'ROCKETMQ_SRE_PHASE3_AGENT_URL',
    'ROCKETMQ_SRE_PHASE3_WORKLOAD_TOKEN',
    'ROCKETMQ_SRE_PHASE3_SIGNING_KEY',
    'ROCKETMQ_SRE_PHASE3_PROXY_EXPECTED_REPLICAS'
)) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$postgresForward = $null
$executorForward = $null
$agentForward = $null
try {
    & kubectl `
        --kubeconfig $resolvedKubeconfig `
        -n rocketmq-system `
        delete job rocketmq-sre-phase03-wave-admin-bootstrap `
        --ignore-not-found=true `
        --wait=true
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to clear the previous bounded wave Admin bootstrap Job.'
    }
    & kubectl --kubeconfig $resolvedKubeconfig apply -f $bootstrapManifest
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to create the bounded wave Admin bootstrap Job.'
    }
    & kubectl `
        --kubeconfig $resolvedKubeconfig `
        -n rocketmq-system `
        wait `
        --for=condition=complete `
        job/rocketmq-sre-phase03-wave-admin-bootstrap `
        --timeout=180s
    if ($LASTEXITCODE -ne 0) {
        & kubectl `
            --kubeconfig $resolvedKubeconfig `
            -n rocketmq-system `
            logs job/rocketmq-sre-phase03-wave-admin-bootstrap `
            --all-containers=true `
            --tail=80
        throw 'The bounded wave Admin bootstrap Job did not complete.'
    }

    $postgresForward = Start-PortForward `
        'rocketmq-sre' 'service/postgres' $PostgresLocalPort 5432 'postgres'
    $executorForward = Start-PortForward `
        'rocketmq-sre' 'service/sre-executor' $ExecutorLocalPort 8094 'executor'
    $agentForward = Start-PortForward `
        'rocketmq-sre' 'service/sre-execution-agent' $AgentLocalPort 8095 'agent'

    $databaseUrl = Get-DecodedSecretValue `
        'rocketmq-sre' 'rocketmq-sre-postgres' 'database-url'
    $databaseUri = [UriBuilder]$databaseUrl
    $databaseUri.Host = '127.0.0.1'
    $databaseUri.Port = $PostgresLocalPort
    $workloadToken = Get-DecodedSecretValue `
        'rocketmq-sre' 'rocketmq-sre-kind-secrets' 'internal-token'
    $proxy = & kubectl `
        --kubeconfig $resolvedKubeconfig `
        -n rocketmq-system `
        get deployment rocketmq-proxy `
        -o json |
        ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or [int]$proxy.spec.replicas -lt 1) {
        throw 'A live Proxy Deployment with at least one replica is required.'
    }

    $env:CARGO_HOME = [IO.Path]::GetFullPath($CargoHome)
    $env:CARGO_TARGET_DIR = [IO.Path]::GetFullPath($CargoTargetDir)
    $env:TEMP = $resolvedTemporaryRoot
    $env:TMP = $resolvedTemporaryRoot
    $env:CARGO_BUILD_JOBS = '1'
    $env:ROCKETMQ_SRE_PHASE3_DATABASE_URL = $databaseUri.Uri.AbsoluteUri
    $env:ROCKETMQ_SRE_PHASE3_EXECUTOR_URL = "http://127.0.0.1:$ExecutorLocalPort"
    $env:ROCKETMQ_SRE_PHASE3_AGENT_URL = "http://127.0.0.1:$AgentLocalPort"
    $env:ROCKETMQ_SRE_PHASE3_WORKLOAD_TOKEN = $workloadToken
    $env:ROCKETMQ_SRE_PHASE3_SIGNING_KEY = $workloadToken
    $env:ROCKETMQ_SRE_PHASE3_PROXY_EXPECTED_REPLICAS = [string]$proxy.spec.replicas

    & cargo +1.95.0 test `
        --manifest-path $manifestPath `
        --locked `
        -p rocketmq-sre-control-plane `
        --lib `
        supervised_execution::wave_actions_e2e_tests::real_kind_wave_one_r1_actions_share_the_supervised_execution_chain `
        -- `
        --ignored `
        --exact `
        --nocapture
    if ($LASTEXITCODE -ne 0) {
        throw 'Wave 1 R1 formal supervised E2E failed.'
    }

    Write-Host (
        'PHASE03_WAVE1_R1_SUPERVISED_E2E_OK ' +
        'actions=observability.logger_level_ttl.v1,proxy.scale_out_one.v1 ' +
        'approval=independent executor=true agent=true verification=true audit=correlated'
    )

    & cargo +1.95.0 test `
        --manifest-path $manifestPath `
        --locked `
        -p rocketmq-sre-control-plane `
        --lib `
        supervised_execution::wave_admin_actions_e2e_tests::real_kind_wave_admin_actions_share_r2_critic_approval_and_verification `
        -- `
        --ignored `
        --exact `
        --nocapture
    if ($LASTEXITCODE -ne 0) {
        throw 'Wave Admin R2 formal supervised E2E failed.'
    }

    Write-Host (
        'PHASE03_WAVE_ADMIN_R2_SUPERVISED_E2E_OK ' +
        'actions=broker.config.patch_allowlisted.v1,topic.config.patch_allowlisted.v1,' +
        'subscription_group.patch_allowlisted.v1 critic=kimi-moonshot ' +
        'approval=independent executor=true agent=true verification=true audit=correlated'
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
        (Test-Path -LiteralPath $runRoot) -and
        $runRoot.StartsWith($expectedTemporaryPrefix, [StringComparison]::OrdinalIgnoreCase)
    ) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
}

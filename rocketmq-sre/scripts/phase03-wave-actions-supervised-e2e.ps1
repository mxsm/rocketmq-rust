# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Kubeconfig,

    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',

    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',

    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',

    [ValidateRange(1024, 65535)]
    [int]$PostgresLocalPort = 35432,

    [ValidateRange(1024, 65535)]
    [int]$ExecutorLocalPort = 58096,

    [ValidateRange(1024, 65535)]
    [int]$AgentLocalPort = 58097,

    [switch]$R1Only,

    [string]$LiveFragment,

    [string]$RecoveryFragment
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$manifestPath = Join-Path $sreRoot 'Cargo.toml'
$bootstrapManifest = Join-Path $sreRoot 'deploy\kind\phase03-wave-admin-bootstrap-job.yaml'

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
    Assert-DataPath $path.Value $path.Description
}
foreach ($fragment in @(
    @{ Value = $LiveFragment; Description = 'live qualification fragment' },
    @{ Value = $RecoveryFragment; Description = 'recovery qualification fragment' }
)) {
    if (-not [string]::IsNullOrWhiteSpace($fragment.Value)) {
        Assert-DataPath $fragment.Value $fragment.Description
        # Windows PowerShell 5.1 runs on .NET Framework, which does not expose
        # Path.IsPathFullyQualified. Data paths are already restricted to D/F,
        # so an explicit drive-root check provides the same invariant here.
        if ($fragment.Value -notmatch '^[A-Za-z]:[\\/]') {
            throw "$($fragment.Description) path must be absolute."
        }
    }
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
$targetDriveName = [IO.Path]::GetPathRoot(
    [IO.Path]::GetFullPath($CargoTargetDir)
).TrimEnd('\').TrimEnd(':')
$targetFreeGiB = (Get-PSDrive -Name $targetDriveName).Free / 1GB
Write-Host "${targetDriveName}_FREE_GIB=$([Math]::Round($targetFreeGiB, 2))"
if ($targetFreeGiB -lt 15) {
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
    'ROCKETMQ_SRE_PHASE3_PROXY_EXPECTED_REPLICAS',
    'ROCKETMQ_SRE_PHASE3_PROXY_POD',
    'ROCKETMQ_SRE_PHASE3_PROXY_UID',
    'ROCKETMQ_SRE_PHASE4_COLLECTOR_POD',
    'ROCKETMQ_SRE_PHASE4_COLLECTOR_UID',
    'ROCKETMQ_SRE_TEST_DATABASE_URL',
    'ROCKETMQ_SRE_R1_LIVE_FRAGMENT',
    'ROCKETMQ_SRE_R1_RECOVERY_FRAGMENT'
)) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$postgresForward = $null
$executorForward = $null
$agentForward = $null
$baselineProxyReplicas = $null
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
    $baselineProxyReplicas = [int]$proxy.spec.replicas
    if ($baselineProxyReplicas -lt 2) {
        throw 'The combined R1 qualification requires at least two live Proxy replicas for safe restart.'
    }
    $proxyPods = & kubectl `
        --kubeconfig $resolvedKubeconfig `
        -n rocketmq-system `
        get pods `
        -l app.kubernetes.io/name=rocketmq-proxy `
        -o json |
        ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to list live Proxy pods.'
    }
    $proxyPod = $proxyPods.items |
        Where-Object { $_.status.containerStatuses[0].ready -eq $true } |
        Sort-Object { $_.metadata.creationTimestamp } |
        Select-Object -First 1
    if ($null -eq $proxyPod -or [string]::IsNullOrWhiteSpace($proxyPod.metadata.uid)) {
        throw 'No Ready Proxy pod with a stable UID is available.'
    }
    $collectorPods = & kubectl `
        --kubeconfig $resolvedKubeconfig `
        -n observability `
        get pods `
        -l app.kubernetes.io/name=otel-collector `
        -o json |
        ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to list live OpenTelemetry Collector pods.'
    }
    $collectorPod = $collectorPods.items |
        Where-Object { $_.status.containerStatuses[0].ready -eq $true } |
        Sort-Object { $_.metadata.creationTimestamp } |
        Select-Object -First 1
    if ($null -eq $collectorPod -or [string]::IsNullOrWhiteSpace($collectorPod.metadata.uid)) {
        throw 'No Ready OpenTelemetry Collector pod with a stable UID is available.'
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
    $env:ROCKETMQ_SRE_PHASE3_PROXY_POD = [string]$proxyPod.metadata.name
    $env:ROCKETMQ_SRE_PHASE3_PROXY_UID = [string]$proxyPod.metadata.uid
    $env:ROCKETMQ_SRE_PHASE4_COLLECTOR_POD = [string]$collectorPod.metadata.name
    $env:ROCKETMQ_SRE_PHASE4_COLLECTOR_UID = [string]$collectorPod.metadata.uid
    $env:ROCKETMQ_SRE_TEST_DATABASE_URL = $databaseUri.Uri.AbsoluteUri
    if (-not [string]::IsNullOrWhiteSpace($LiveFragment)) {
        $env:ROCKETMQ_SRE_R1_LIVE_FRAGMENT = [IO.Path]::GetFullPath($LiveFragment)
    }
    if (-not [string]::IsNullOrWhiteSpace($RecoveryFragment)) {
        $env:ROCKETMQ_SRE_R1_RECOVERY_FRAGMENT = [IO.Path]::GetFullPath($RecoveryFragment)
        & cargo +1.95.0 test `
            --manifest-path $manifestPath `
            --locked `
            -p rocketmq-sre-executor `
            --test execution_flow `
            all_r1_actions_persist_recovery_qualification_matrix `
            -- `
            --ignored `
            --exact `
            --nocapture `
            --test-threads=1
        if ($LASTEXITCODE -ne 0) {
            throw 'R1 persisted recovery qualification matrix failed.'
        }
    }

    & cargo +1.95.0 test `
        --manifest-path $manifestPath `
        --locked `
        -p rocketmq-sre-control-plane `
        --lib `
        supervised_execution::wave_actions_e2e_tests::real_kind_all_r1_actions_share_the_supervised_execution_chain `
        -- `
        --ignored `
        --exact `
        --nocapture
    if ($LASTEXITCODE -ne 0) {
        throw 'Wave 1 R1 formal supervised E2E failed.'
    }

    Write-Host (
        'PHASE03_WAVE1_R1_SUPERVISED_E2E_OK ' +
        'actions=observability.logger_level_ttl.v1,proxy.scale_out_one.v1,' +
        'proxy.restart_one.v1,telemetry.collector.restart_one.v1 ' +
        'approval=independent executor=true agent=true verification=true audit=correlated'
    )

    if (-not $R1Only) {
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
}
finally {
    $cleanupFailures = [Collections.Generic.List[string]]::new()
    Stop-OwnedProcess $agentForward
    Stop-OwnedProcess $executorForward
    Stop-OwnedProcess $postgresForward
    if ($null -ne $baselineProxyReplicas) {
        try {
            & kubectl `
                --kubeconfig $resolvedKubeconfig `
                -n rocketmq-system `
                scale deployment/rocketmq-proxy `
                --replicas=$baselineProxyReplicas
            if ($LASTEXITCODE -ne 0) {
                throw 'Unable to restore the original Proxy replica count.'
            }
            & kubectl `
                --kubeconfig $resolvedKubeconfig `
                -n rocketmq-system `
                rollout status deployment/rocketmq-proxy `
                --timeout=300s
            if ($LASTEXITCODE -ne 0) {
                throw 'Proxy Deployment did not become Ready after replica restoration.'
            }
        }
        catch {
            $cleanupFailures.Add($_.Exception.Message)
        }
        try {
            & kubectl `
                --kubeconfig $resolvedKubeconfig `
                -n observability `
                rollout status deployment/otel-collector `
                --timeout=300s
            if ($LASTEXITCODE -ne 0) {
                throw 'Collector Deployment did not remain Ready after qualification.'
            }
        }
        catch {
            $cleanupFailures.Add($_.Exception.Message)
        }
    }
    try {
        & kubectl `
            --kubeconfig $resolvedKubeconfig `
            -n rocketmq-system `
            delete job rocketmq-sre-phase03-wave-admin-bootstrap `
            --ignore-not-found=true `
            --wait=true
        if ($LASTEXITCODE -ne 0) {
            throw 'Unable to remove the bounded wave Admin bootstrap Job.'
        }
    }
    catch {
        $cleanupFailures.Add($_.Exception.Message)
    }
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
    if (
        (Test-Path -LiteralPath $runRoot) -and
        $runRoot.StartsWith($expectedTemporaryPrefix, [StringComparison]::OrdinalIgnoreCase)
    ) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
    if ($cleanupFailures.Count -gt 0) {
        throw "R1 wave cleanup failed: $($cleanupFailures -join '; ')"
    }
}

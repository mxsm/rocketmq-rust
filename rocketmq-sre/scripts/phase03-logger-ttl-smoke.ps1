# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Kubeconfig,

    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',

    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',

    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',

    [ValidateRange(1024, 65535)]
    [int]$PostgresLocalPort = 25432,

    [ValidateRange(1024, 65535)]
    [int]$NameServerLocalPort = 29876,

    [ValidateRange(1024, 65535)]
    [int]$BrokerLocalPort = 30911
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$manifestPath = Join-Path $sreRoot 'Cargo.toml'

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

function Stop-OwnedProcess([Diagnostics.Process]$Process) {
    if ($null -eq $Process -or $Process.HasExited) {
        return
    }
    Stop-Process -Id $Process.Id -Force
    $Process.WaitForExit(10000) | Out-Null
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

function Get-DecodedSecretValue(
    [object]$Secret,
    [string]$Key
) {
    $property = $Secret.data.PSObject.Properties[$Key]
    if ($null -eq $property -or [string]::IsNullOrWhiteSpace([string]$property.Value)) {
        throw "Required Kubernetes Secret key is missing: $Key"
    }
    return [Text.Encoding]::UTF8.GetString(
        [Convert]::FromBase64String([string]$property.Value)
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

foreach ($port in @($PostgresLocalPort, $NameServerLocalPort, $BrokerLocalPort)) {
    Assert-PortAvailable $port
}

$resolvedKubeconfig = [IO.Path]::GetFullPath($Kubeconfig)
if (-not (Test-Path -LiteralPath $resolvedKubeconfig -PathType Leaf)) {
    throw "Kubernetes kubeconfig does not exist: $resolvedKubeconfig"
}

New-Item -ItemType Directory -Force -Path $CargoTargetDir, $CargoHome, $TemporaryRoot | Out-Null
$resolvedTemporaryRoot = [IO.Path]::GetFullPath($TemporaryRoot)
$runRoot = [IO.Path]::GetFullPath(
    (Join-Path $resolvedTemporaryRoot "phase03-logger-ttl-$([Guid]::NewGuid().ToString('N'))")
)
$expectedTemporaryPrefix = $resolvedTemporaryRoot.TrimEnd('\') + '\'
if (-not $runRoot.StartsWith($expectedTemporaryPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Logger TTL smoke runtime directory escaped the configured temporary root.'
}
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

$savedEnvironment = @{}
foreach ($name in @(
    'CARGO_HOME',
    'CARGO_TARGET_DIR',
    'TEMP',
    'TMP',
    'ROCKETMQ_SRE_TEST_DATABASE_URL',
    'ROCKETMQ_SRE_TEST_NAMESRV_ADDR',
    'ROCKETMQ_SRE_TEST_BROKER_ADDR',
    'ROCKETMQ_SRE_TEST_BROKER_READ_ACCESS_KEY',
    'ROCKETMQ_SRE_TEST_BROKER_READ_SECRET_KEY',
    'ROCKETMQ_SRE_TEST_BROKER_MUTATION_ACCESS_KEY',
    'ROCKETMQ_SRE_TEST_BROKER_MUTATION_SECRET_KEY'
)) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$postgresForward = $null
$nameServerForward = $null
$brokerForward = $null
try {
    $postgresForward = Start-PortForward `
        'rocketmq-sre' 'service/postgres' $PostgresLocalPort 5432 'postgres'
    $nameServerForward = Start-PortForward `
        'rocketmq-system' 'service/rocketmq-namesrv' $NameServerLocalPort 9876 'namesrv'
    $brokerForward = Start-PortForward `
        'rocketmq-system' 'service/rocketmq-broker' $BrokerLocalPort 10911 'broker'

    $databaseUrlEncoded = & kubectl `
        --kubeconfig $resolvedKubeconfig `
        -n rocketmq-sre `
        get secret rocketmq-sre-postgres `
        -o jsonpath='{.data.database-url}'
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to read the Kind PostgreSQL connection reference.'
    }
    $databaseUrl = [Text.Encoding]::UTF8.GetString(
        [Convert]::FromBase64String($databaseUrlEncoded)
    )
    $databaseUri = [UriBuilder]$databaseUrl
    $databaseUri.Host = '127.0.0.1'
    $databaseUri.Port = $PostgresLocalPort

    $agentSecret = & kubectl `
        --kubeconfig $resolvedKubeconfig `
        -n rocketmq-sre `
        get secret rocketmq-sre-kind-secrets `
        -o json |
        ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to read the Kind Execution Agent credential references.'
    }

    $env:CARGO_HOME = [IO.Path]::GetFullPath($CargoHome)
    $env:CARGO_TARGET_DIR = [IO.Path]::GetFullPath($CargoTargetDir)
    $env:TEMP = $resolvedTemporaryRoot
    $env:TMP = $resolvedTemporaryRoot
    $env:ROCKETMQ_SRE_TEST_DATABASE_URL = $databaseUri.Uri.AbsoluteUri
    $env:ROCKETMQ_SRE_TEST_NAMESRV_ADDR = "127.0.0.1:$NameServerLocalPort"
    $env:ROCKETMQ_SRE_TEST_BROKER_ADDR = "127.0.0.1:$BrokerLocalPort"
    $env:ROCKETMQ_SRE_TEST_BROKER_READ_ACCESS_KEY = Get-DecodedSecretValue `
        $agentSecret 'agent-read-access-key'
    $env:ROCKETMQ_SRE_TEST_BROKER_READ_SECRET_KEY = Get-DecodedSecretValue `
        $agentSecret 'agent-read-secret-key'
    $env:ROCKETMQ_SRE_TEST_BROKER_MUTATION_ACCESS_KEY = Get-DecodedSecretValue `
        $agentSecret 'agent-mutation-access-key'
    $env:ROCKETMQ_SRE_TEST_BROKER_MUTATION_SECRET_KEY = Get-DecodedSecretValue `
        $agentSecret 'agent-mutation-secret-key'

    & cargo +1.95.0 test `
        --manifest-path $manifestPath `
        --locked `
        --package rocketmq-sre-execution-agent `
        --lib `
        drivers::production_broker_config::logger_level::tests::real_broker_logger_ttl_applies_verifies_and_restores `
        -- `
        --ignored `
        --exact `
        --nocapture `
        --test-threads=1
    if ($LASTEXITCODE -ne 0) {
        throw 'The real Kind Broker logger TTL smoke failed.'
    }

    Write-Host (
        'PHASE03_LOGGER_TTL_SMOKE_OK ' +
        'target=Kind level_applied=true live_verified=true restored=true'
    )
}
finally {
    Stop-OwnedProcess $brokerForward
    Stop-OwnedProcess $nameServerForward
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

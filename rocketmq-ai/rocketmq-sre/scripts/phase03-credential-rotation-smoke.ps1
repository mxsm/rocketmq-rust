# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$DatabaseUrl,
    [string]$Kubeconfig = 'D:\BuildCache\rocketmq-sre-temp\kind\phase00-kubeconfig',
    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',
    [string]$ClusterTargetDir = 'D:\BuildCache\rocketmq-sre-phase3-credential-cluster-target',
    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',
    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',
    [ValidateRange(1024, 65535)]
    [int]$NameServerPort = 43876,
    [ValidateRange(1026, 65534)]
    [int]$BrokerPort = 44911
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$namespace = 'rocketmq-sre'
$credentialSet = 'broker-admin'
$selectorName = 'broker-admin-credential-selector'
$activeSecretName = 'broker-admin-credential-v1'
$candidateSecretName = 'broker-admin-credential-v2'
$invalidSecretName = 'broker-admin-credential-invalid'
$probeTopic = 'SRE_PROBE_CREDENTIAL_ROTATION'

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

function Wait-ProcessPort(
    [Diagnostics.Process]$Process,
    [int]$Port,
    [string]$ErrorLog
) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(90)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        if ($Process.HasExited) {
            $tail = if (Test-Path -LiteralPath $ErrorLog -PathType Leaf) {
                (Get-Content -LiteralPath $ErrorLog -Tail 80) -join [Environment]::NewLine
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

function ConvertTo-Base64([string]$Value) {
    return [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Value))
}

function Apply-JsonResource([hashtable]$Resource, [string]$Description) {
    $payload = $Resource | ConvertTo-Json -Depth 12 -Compress
    $payload | & kubectl --kubeconfig $Kubeconfig apply -f - | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

function New-CredentialSecretResource(
    [string]$Name,
    [string]$Version,
    [string]$AccessKey,
    [string]$SecretKey
) {
    return [ordered]@{
        apiVersion = 'v1'
        kind = 'Secret'
        immutable = $true
        metadata = [ordered]@{
            name = $Name
            namespace = $namespace
            annotations = [ordered]@{
                'rocketmqrust.com/sre-credential-set' = $credentialSet
                'rocketmqrust.com/sre-credential-version' = $Version
            }
        }
        type = 'Opaque'
        data = [ordered]@{
            'access-key' = ConvertTo-Base64 $AccessKey
            'secret-key' = ConvertTo-Base64 $SecretKey
        }
    }
}

function Assert-ResourceAbsent([string]$Kind, [string]$Name) {
    $existing = & kubectl `
        --kubeconfig $Kubeconfig `
        -n $namespace `
        get $Kind $Name `
        --ignore-not-found `
        -o name
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to check $Kind/$Name."
    }
    if (-not [string]::IsNullOrWhiteSpace(($existing -join ''))) {
        throw "Refusing to overwrite pre-existing $Kind/$Name."
    }
}

function Remove-TestResources {
    & kubectl `
        --kubeconfig $Kubeconfig `
        -n $namespace `
        delete configmap $selectorName `
        --ignore-not-found `
        --wait=false | Out-Null
    & kubectl `
        --kubeconfig $Kubeconfig `
        -n $namespace `
        delete secret $activeSecretName $candidateSecretName $invalidSecretName `
        --ignore-not-found `
        --wait=false | Out-Null
}

foreach ($path in @(
    @{ Value = $Kubeconfig; Description = 'Kubeconfig' },
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $ClusterTargetDir; Description = 'test-cluster Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' }
)) {
    Assert-NonSystemBuildPath $path.Value $path.Description
}
if (-not (Test-Path -LiteralPath $Kubeconfig -PathType Leaf)) {
    throw "Kubeconfig does not exist: $Kubeconfig"
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
foreach ($resource in @(
    @{ Kind = 'configmap'; Name = $selectorName },
    @{ Kind = 'secret'; Name = $activeSecretName },
    @{ Kind = 'secret'; Name = $candidateSecretName },
    @{ Kind = 'secret'; Name = $invalidSecretName }
)) {
    Assert-ResourceAbsent $resource.Kind $resource.Name
}

New-Item -ItemType Directory -Force -Path `
    $CargoTargetDir, $ClusterTargetDir, $CargoHome, $TemporaryRoot | Out-Null
$targetDrive = Get-PSDrive -Name (
    [IO.Path]::GetPathRoot([IO.Path]::GetFullPath($CargoTargetDir)).Substring(0, 1)
)
$targetFreeGiB = $targetDrive.Free / 1GB
$targetFreePercent = 100 * $targetDrive.Free / ($targetDrive.Free + $targetDrive.Used)
if ($targetFreeGiB -lt 30 -or $targetFreePercent -lt 15) {
    Invoke-Native cargo @(
        '+1.95.0', 'clean',
        '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
        '--target-dir', $CargoTargetDir
    ) 'Broker-reserve SRE Cargo cleanup'
    Invoke-Native cargo @(
        '+1.95.0', 'clean',
        '--manifest-path', (Join-Path $repositoryRoot 'Cargo.toml'),
        '--target-dir', $ClusterTargetDir
    ) 'Broker-reserve test-cluster Cargo cleanup'
}

$runRoot = [IO.Path]::GetFullPath(
    (Join-Path $TemporaryRoot "phase03-credential-rotation-$([Guid]::NewGuid().ToString('N'))")
)
$expectedTemporaryRoot = [IO.Path]::GetFullPath($TemporaryRoot).TrimEnd('\') + '\'
if (-not $runRoot.StartsWith($expectedTemporaryRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Smoke runtime directory escaped the configured temporary root.'
}
$namesrvData = Join-Path $runRoot 'namesrv'
$brokerData = Join-Path $runRoot 'broker'
$authData = Join-Path $brokerData 'auth'
$runtimeBin = Join-Path $runRoot 'bin'
New-Item -ItemType Directory -Force -Path $namesrvData, $brokerData, $authData, $runtimeBin | Out-Null

$readAccessKey = 'phase03-credential-reader'
$readSecretKey = New-TestSecret
$mutationAccessKey = 'phase03-credential-writer'
$mutationSecretKey = New-TestSecret
$invalidSecretKey = New-TestSecret
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
brokerName = "sre-phase03-credential-broker"
brokerClusterName = "SrePhase03Credential"
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

$environmentNames = @(
    'CARGO_HOME',
    'CARGO_TARGET_DIR',
    'TEMP',
    'TMP',
    'KUBECONFIG',
    'ROCKETMQ_HOME',
    'ROCKETMQ_SECURITY_PROFILE',
    'ROCKETMQ_HEALTH_BIND_ADDR',
    'ROCKETMQ_ACL_ACCESS_KEY',
    'ROCKETMQ_ACL_SECRET_KEY',
    'ROCKETMQ_SRE_TEST_CREDENTIAL_ROTATION',
    'ROCKETMQ_SRE_TEST_DATABASE_URL',
    'ROCKETMQ_SRE_TEST_NAMESRV_ADDR',
    'ROCKETMQ_SRE_TEST_BROKER_ADDR',
    'ROCKETMQ_SRE_TEST_CREDENTIAL_NAMESPACE',
    'ROCKETMQ_SRE_TEST_CREDENTIAL_SELECTOR',
    'ROCKETMQ_SRE_TEST_CREDENTIAL_SET',
    'ROCKETMQ_SRE_TEST_CREDENTIAL_PROBE_TOPIC',
    'ROCKETMQ_SRE_TEST_ACTIVE_SECRET_REF',
    'ROCKETMQ_SRE_TEST_CANDIDATE_SECRET_REF',
    'ROCKETMQ_SRE_TEST_BAD_CANDIDATE_SECRET_REF',
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
$resourcesCreated = $false
$smokeSucceeded = $false
try {
    $env:CARGO_HOME = $CargoHome
    $env:CARGO_TARGET_DIR = $ClusterTargetDir
    $env:TEMP = $TemporaryRoot
    $env:TMP = $TemporaryRoot
    Set-Location $repositoryRoot
    Invoke-Native cargo @(
        '+1.95.0', 'build', '--locked',
        '--package', 'rocketmq-namesrv',
        '--bin', 'rocketmq-namesrv-rust',
        '--package', 'rocketmq-broker',
        '--bin', 'rocketmq-broker-rust',
        '--package', 'rocketmq-admin-cli',
        '--bin', 'rocketmq-admin-cli'
    ) 'RocketMQ credential-rotation test-cluster build'

    $builtNamesrvBinary = Join-Path $ClusterTargetDir 'debug/rocketmq-namesrv-rust.exe'
    $builtBrokerBinary = Join-Path $ClusterTargetDir 'debug/rocketmq-broker-rust.exe'
    $builtAdminBinary = Join-Path $ClusterTargetDir 'debug/rocketmq-admin-cli.exe'
    foreach ($binary in @($builtNamesrvBinary, $builtBrokerBinary, $builtAdminBinary)) {
        if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
            throw "Expected credential-rotation smoke binary is missing: $binary"
        }
    }
    $namesrvBinary = Join-Path $runtimeBin 'rocketmq-namesrv-rust.exe'
    $brokerBinary = Join-Path $runtimeBin 'rocketmq-broker-rust.exe'
    $adminBinary = Join-Path $runtimeBin 'rocketmq-admin-cli.exe'
    Copy-Item -LiteralPath $builtNamesrvBinary -Destination $namesrvBinary
    Copy-Item -LiteralPath $builtBrokerBinary -Destination $brokerBinary
    Copy-Item -LiteralPath $builtAdminBinary -Destination $adminBinary
    Invoke-Native cargo @(
        '+1.95.0', 'clean',
        '--manifest-path', (Join-Path $repositoryRoot 'Cargo.toml'),
        '--target-dir', $ClusterTargetDir
    ) 'credential test-cluster build cleanup'
    $targetDrive = Get-PSDrive -Name (
        [IO.Path]::GetPathRoot([IO.Path]::GetFullPath($ClusterTargetDir)).Substring(0, 1)
    )
    $targetFreeGiB = $targetDrive.Free / 1GB
    $targetFreePercent = 100 * $targetDrive.Free / ($targetDrive.Free + $targetDrive.Used)
    if ($targetFreeGiB -lt 15 -or $targetFreePercent -lt 12) {
        throw (
            'Test-cluster build cleanup left insufficient Broker runtime reserve ' +
            'on the target drive.'
        )
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

    $env:ROCKETMQ_ACL_ACCESS_KEY = $mutationAccessKey
    $env:ROCKETMQ_ACL_SECRET_KEY = $mutationSecretKey
    Invoke-Native $adminBinary @(
        'topic', 'updateTopic',
        '-n', "127.0.0.1:$NameServerPort",
        '-b', "127.0.0.1:$BrokerPort",
        '-t', $probeTopic,
        '-r', '1',
        '-w', '1',
        '-p', '6'
    ) 'dedicated credential validation Topic bootstrap'
    Remove-Item Env:\ROCKETMQ_ACL_ACCESS_KEY -ErrorAction SilentlyContinue
    Remove-Item Env:\ROCKETMQ_ACL_SECRET_KEY -ErrorAction SilentlyContinue

    $resourcesCreated = $true
    Apply-JsonResource (
        New-CredentialSecretResource `
            $activeSecretName 'v1' $readAccessKey $readSecretKey
    ) 'active credential Secret creation'
    Apply-JsonResource (
        New-CredentialSecretResource `
            $candidateSecretName 'v2' $readAccessKey $readSecretKey
    ) 'candidate credential Secret creation'
    Apply-JsonResource (
        New-CredentialSecretResource `
            $invalidSecretName 'v-bad' $readAccessKey $invalidSecretKey
    ) 'invalid credential Secret creation'
    Apply-JsonResource ([ordered]@{
        apiVersion = 'v1'
        kind = 'ConfigMap'
        metadata = [ordered]@{
            name = $selectorName
            namespace = $namespace
            annotations = [ordered]@{
                'rocketmqrust.com/sre-credential-set' = $credentialSet
                'rocketmqrust.com/sre-active-credential-version' = 'v1'
                'rocketmqrust.com/sre-active-credential-ref' =
                    "kubernetes://$namespace/$activeSecretName"
                'rocketmqrust.com/sre-candidate-probe-healthy' = 'false'
            }
        }
        data = [ordered]@{
            description = 'Phase 03 bounded credential selector fixture'
        }
    }) 'credential selector ConfigMap creation'
    $env:CARGO_TARGET_DIR = $CargoTargetDir
    $env:KUBECONFIG = $Kubeconfig
    $env:ROCKETMQ_SRE_TEST_CREDENTIAL_ROTATION = '1'
    $env:ROCKETMQ_SRE_TEST_DATABASE_URL = $DatabaseUrl
    $env:ROCKETMQ_SRE_TEST_NAMESRV_ADDR = "127.0.0.1:$NameServerPort"
    $env:ROCKETMQ_SRE_TEST_BROKER_ADDR = "127.0.0.1:$BrokerPort"
    $env:ROCKETMQ_SRE_TEST_CREDENTIAL_NAMESPACE = $namespace
    $env:ROCKETMQ_SRE_TEST_CREDENTIAL_SELECTOR = $selectorName
    $env:ROCKETMQ_SRE_TEST_CREDENTIAL_SET = $credentialSet
    $env:ROCKETMQ_SRE_TEST_CREDENTIAL_PROBE_TOPIC = $probeTopic
    $env:ROCKETMQ_SRE_TEST_ACTIVE_SECRET_REF =
        "kubernetes://$namespace/$activeSecretName"
    $env:ROCKETMQ_SRE_TEST_CANDIDATE_SECRET_REF =
        "kubernetes://$namespace/$candidateSecretName"
    $env:ROCKETMQ_SRE_TEST_BAD_CANDIDATE_SECRET_REF =
        "kubernetes://$namespace/$invalidSecretName"
    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
        '--locked',
        '--package', 'rocketmq-sre-execution-agent',
        '--lib',
        'drivers::production_credential_rotation::tests::real_credential_overlap_rejects_bad_candidate_and_restores_previous_selector',
        '--',
        '--ignored',
        '--exact',
        '--nocapture',
        '--test-threads=1'
    ) 'real credential-overlap smoke'
    $smokeSucceeded = $true
    Write-Host (
        'PHASE03_CREDENTIAL_ROTATION_SMOKE_OK ' +
        'invalid_candidate_rejected=true selector_unchanged=true ' +
        'candidate_probe=true overlap_recorded=true rollback_restored=true'
    )
}
finally {
    if ($resourcesCreated) {
        Remove-TestResources
    }
    Stop-OwnedProcess $brokerProcess
    Stop-OwnedProcess $namesrvProcess
    if (Test-Path -LiteralPath $ClusterTargetDir -PathType Container) {
        & cargo '+1.95.0' clean `
            '--manifest-path' (Join-Path $repositoryRoot 'Cargo.toml') `
            '--target-dir' $ClusterTargetDir
        if ($LASTEXITCODE -ne 0) {
            Write-Warning (
                "Failed to clean the owned test-cluster target directory: $ClusterTargetDir"
            )
        }
    }
    if (-not $smokeSucceeded) {
        foreach ($logPath in @($brokerErr, $brokerOut, $namesrvErr, $namesrvOut)) {
            if (-not [string]::IsNullOrWhiteSpace($logPath) -and
                (Test-Path -LiteralPath $logPath -PathType Leaf)) {
                Write-Warning "Failure diagnostics from $([IO.Path]::GetFileName($logPath)):"
                Get-Content -LiteralPath $logPath -Tail 80 | Write-Warning
            }
        }
    }
    Restore-Environment $savedEnvironment
    Set-Location $repositoryRoot
    if ((Test-Path -LiteralPath $runRoot -PathType Container) -and
        $runRoot.StartsWith($expectedTemporaryRoot, [StringComparison]::OrdinalIgnoreCase)) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
}

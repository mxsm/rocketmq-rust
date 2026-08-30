# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$DatabaseUrl,
    [Parameter(Mandatory = $true)]
    [string]$ImageDigest,
    [string]$Kubeconfig = 'D:\BuildCache\rocketmq-sre-temp\kind\phase00-kubeconfig',
    [string]$Namespace = 'rocketmq-system',
    [string]$Workload = 'rocketmq-proxy',
    [string]$Container = 'proxy',
    [string]$KindNodes = 'rocketmq-sre-phase00-control-plane',
    [string]$ImageReference = 'docker.io/rocketmq-rust/proxy:local',
    [string]$ImageRepository = 'docker.io/rocketmq-rust/proxy',
    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',
    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',
    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp'
)

$ErrorActionPreference = 'Stop'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))

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

function Restore-Environment([hashtable]$SavedEnvironment) {
    foreach ($entry in $SavedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}

foreach ($path in @(
    @{ Value = $Kubeconfig; Description = 'Kubeconfig' },
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' }
)) {
    Assert-NonSystemBuildPath $path.Value $path.Description
}
if (-not (Test-Path -LiteralPath $Kubeconfig -PathType Leaf)) {
    throw "Kubeconfig does not exist: $Kubeconfig"
}
if ($ImageDigest -notmatch '^sha256:[0-9a-f]{64}$') {
    throw 'ImageDigest must be an immutable lowercase SHA-256 digest.'
}
if ($ImageRepository -notmatch '^[a-z0-9._:/-]+$' -or $ImageReference -notmatch '^[a-z0-9._:/-]+$') {
    throw 'ImageRepository and ImageReference must be explicit normalized container references.'
}
$nodes = $KindNodes.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }
if ($nodes.Count -eq 0 -or $nodes | Where-Object { $_ -notmatch '^[a-z0-9.-]+$' }) {
    throw 'KindNodes must contain only explicit Docker container names.'
}

New-Item -ItemType Directory -Force -Path $CargoTargetDir, $CargoHome, $TemporaryRoot | Out-Null
$targetDriveName = [IO.Path]::GetPathRoot([IO.Path]::GetFullPath($CargoTargetDir)).Substring(0, 1)
$targetDrive = Get-PSDrive -Name $targetDriveName
if (($targetDrive.Free / 1GB) -lt 15) {
    Invoke-Native cargo @(
        '+1.95.0', 'clean',
        '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
        '--target-dir', $CargoTargetDir
    ) 'low-space Cargo cleanup'
}

$deployment = Invoke-Native kubectl @(
    '--kubeconfig', $Kubeconfig,
    '-n', $Namespace,
    'get', 'deployment', $Workload,
    '-o', 'json'
) 'Proxy Deployment lookup'
$deployment = $deployment | ConvertFrom-Json
$containerSpecs = @($deployment.spec.template.spec.containers | Where-Object { $_.name -eq $Container })
if ($containerSpecs.Count -ne 1) {
    throw 'The exact Proxy container was not found.'
}
$containerSpec = $containerSpecs[0]
if ($deployment.status.readyReplicas -ne $deployment.spec.replicas -or $deployment.status.observedGeneration -ne $deployment.metadata.generation) {
    throw 'The original Proxy Deployment is not fully ready.'
}
if (Invoke-Native kubectl @(
    '--kubeconfig', $Kubeconfig,
    '-n', $Namespace,
    'get', 'deployment', "$Workload-sre-canary",
    '--ignore-not-found',
    '-o', 'name'
) 'pre-existing canary lookup') {
    throw 'A pre-existing SRE canary must be reconciled before running this smoke.'
}

foreach ($node in $nodes) {
    Invoke-Native docker @(
        'exec', $node,
        'ctr', '--namespace', 'k8s.io',
        'images', 'tag',
        '--force',
        $ImageReference,
        "$ImageRepository@$ImageDigest"
    ) "Kind image digest registration on $node"
}

$environmentNames = @(
    'CARGO_HOME',
    'CARGO_TARGET_DIR',
    'TEMP',
    'TMP',
    'KUBECONFIG',
    'ROCKETMQ_SRE_TEST_DATABASE_URL',
    'ROCKETMQ_SRE_TEST_PROXY_CANARY',
    'ROCKETMQ_SRE_TEST_PROXY_NAMESPACE',
    'ROCKETMQ_SRE_TEST_PROXY_WORKLOAD',
    'ROCKETMQ_SRE_TEST_PROXY_CONTAINER',
    'ROCKETMQ_SRE_TEST_PROXY_IMAGE_DIGEST'
)
$savedEnvironment = @{}
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

try {
    $env:CARGO_HOME = $CargoHome
    $env:CARGO_TARGET_DIR = $CargoTargetDir
    $env:TEMP = $TemporaryRoot
    $env:TMP = $TemporaryRoot
    $env:KUBECONFIG = $Kubeconfig
    $env:ROCKETMQ_SRE_TEST_DATABASE_URL = $DatabaseUrl
    $env:ROCKETMQ_SRE_TEST_PROXY_CANARY = '1'
    $env:ROCKETMQ_SRE_TEST_PROXY_NAMESPACE = $Namespace
    $env:ROCKETMQ_SRE_TEST_PROXY_WORKLOAD = $Workload
    $env:ROCKETMQ_SRE_TEST_PROXY_CONTAINER = $Container
    $env:ROCKETMQ_SRE_TEST_PROXY_IMAGE_DIGEST = $ImageDigest
    Set-Location $repositoryRoot
    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
        '--locked',
        '--package', 'rocketmq-sre-execution-agent',
        'real_kind_proxy_image_canary_is_one_replica_and_reversible',
        '--',
        '--ignored',
        '--test-threads=1'
    ) 'real Kind Proxy image canary smoke'
    Write-Host (
        'PHASE03_PROXY_IMAGE_CANARY_SMOKE_OK canary_replicas=1 ' +
        'digest_only=true old_replicas_unchanged=true rollback_deleted=true'
    )
}
finally {
    Restore-Environment $savedEnvironment
    Set-Location $repositoryRoot
}

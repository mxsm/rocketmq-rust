# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidateSet('Offline', 'Compose', 'Kind')]
    [string]$Target = 'Offline',

    [ValidateSet('Mock', 'RulesOnly', 'Outage')]
    [string]$Provider = 'Mock',

    [ValidatePattern('^[a-z0-9][a-z0-9-]{0,39}$')]
    [string]$ClusterName = 'rocketmq-sre-phase00',

    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$targetRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot 'target'))
$artifactRoot = [IO.Path]::GetFullPath((Join-Path $targetRoot 'phase01-shadow'))
$manifestPath = Join-Path $sreRoot 'tests/fixtures/e2e/wave-a-manifest.v1.yaml'
$fixturesRoot = Join-Path $sreRoot 'tests/fixtures'
$providerMode = switch ($Provider) {
    'Mock' { 'mock' }
    'RulesOnly' { 'rules-only' }
    'Outage' { 'outage' }
}

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found."
    }
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $Command @Arguments 2>&1 | Out-String
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "$Command failed with exit code $exitCode.`n$output"
    }
    [pscustomobject]@{
        ExitCode = $exitCode
        Output = $output.TrimEnd()
    }
}

function Assert-ArtifactRoot {
    if (-not $artifactRoot.StartsWith(
        $targetRoot + [IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase
    )) {
        throw 'Phase 01 shadow artifacts escaped the SRE target directory.'
    }
}

function Ensure-BuildSpace {
    $driveName = ([IO.Path]::GetPathRoot($sreRoot)).TrimEnd('\').TrimEnd(':')
    $drive = Get-PSDrive -Name $driveName
    if ($drive.Free -lt 15GB) {
        Write-Host "Build drive has less than 15 GiB free; cleaning only $sreRoot/target."
        Invoke-Native cargo @(
            '+1.95.0', 'clean',
            '--manifest-path', (Join-Path $sreRoot 'Cargo.toml')
        ) | Out-Null
        $drive = Get-PSDrive -Name $driveName
        if ($drive.Free -lt 15GB) {
            throw "Build drive still has less than 15 GiB free after cargo clean."
        }
    }
}

function Get-ShadowSummary([string]$Output) {
    $jsonLine = @(
        $Output -split '\r?\n' |
            Where-Object {
                $_ -match '^\s*\{.*"schema_version":"rocketmq-sre\.shadow-eval\.v1".*\}\s*$'
            }
    ) | Select-Object -Last 1
    if ([string]::IsNullOrWhiteSpace($jsonLine)) {
        throw "Shadow evaluator did not emit its compact summary.`n$Output"
    }
    $jsonLine | ConvertFrom-Json
}

function Assert-ShadowSummary([object]$Summary) {
    if (
        -not $Summary.passed `
            -or [int]$Summary.pack_count -ne 8 `
            -or [int]$Summary.fixture_count -ne 24 `
            -or [int]$Summary.mutation_calls -ne 0 `
            -or [int]$Summary.executor_calls -ne 0 `
            -or [bool]$Summary.executor_connected
    ) {
        throw 'Phase 01 shadow invariants were not satisfied.'
    }
    if (
        [int]$Summary.class_counts.normal -ne 8 `
            -or [int]$Summary.class_counts.fault -ne 8 `
            -or [int]$Summary.class_counts.missing -ne 8
    ) {
        throw 'Phase 01 shadow did not execute all normal, fault, and missing cases.'
    }
}

function Save-ShadowSummary([object]$Summary, [string]$Suffix) {
    Assert-ArtifactRoot
    New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null
    $path = Join-Path $artifactRoot "phase01-shadow-$Suffix.json"
    [IO.File]::WriteAllText(
        $path,
        ($Summary | ConvertTo-Json -Depth 20),
        [Text.UTF8Encoding]::new($false)
    )
    Write-Host "Saved shadow result to $path"
}

function Invoke-Offline {
    Require-Command cargo
    Ensure-BuildSpace
    $result = Invoke-Native cargo @(
        '+1.95.0', 'run',
        '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
        '--locked',
        '--package', 'rocketmq-sre-eval',
        '--bin', 'phase01-shadow-eval',
        '--',
        '--manifest', $manifestPath,
        '--fixtures-root', $fixturesRoot,
        '--provider', $providerMode,
        '--compact'
    )
    $summary = Get-ShadowSummary $result.Output
    Assert-ShadowSummary $summary
    Save-ShadowSummary $summary "offline-$providerMode"
}

function Invoke-Compose {
    Require-Command docker
    $previousMode = $env:ROCKETMQ_SRE_SHADOW_PROVIDER_MODE
    try {
        $env:ROCKETMQ_SRE_SHADOW_PROVIDER_MODE = $providerMode
        $result = Invoke-Native docker @(
            'compose',
            '--file', (Join-Path $sreRoot 'deploy/dev/compose.yaml'),
            '--file', (Join-Path $sreRoot 'deploy/dev/compose.phase1-shadow.yaml'),
            '--profile', 'shadow',
            'run', '--rm', '--build',
            'sre-phase1-shadow'
        )
    }
    finally {
        $env:ROCKETMQ_SRE_SHADOW_PROVIDER_MODE = $previousMode
    }
    $summary = Get-ShadowSummary $result.Output
    Assert-ShadowSummary $summary
    Save-ShadowSummary $summary "compose-$providerMode"
}

function Invoke-Kind {
    foreach ($command in @('docker', 'kind', 'kubectl')) {
        Require-Command $command
    }
    $clusters = (Invoke-Native kind @('get', 'clusters')).Output -split '\r?\n'
    if ($clusters -notcontains $ClusterName) {
        throw "Kind cluster '$ClusterName' does not exist. Start the Phase 00 Kind stack first."
    }
    $image = 'rocketmq-rust/sre-phase1-shadow:local'
    if (-not $SkipBuild) {
        Invoke-Native docker @(
            'build',
            '--file', (Join-Path $sreRoot 'deploy/docker/phase1-shadow.Dockerfile'),
            '--tag', $image,
            $repositoryRoot
        ) | Out-Null
    }
    Invoke-Native kind @('load', 'docker-image', $image, '--name', $ClusterName) | Out-Null

    $kubeconfig = Join-Path $repositoryRoot 'target/phase00-kind/kubeconfig'
    $context = "kind-$ClusterName"
    $kubectlPrefix = @('--kubeconfig', $kubeconfig, '--context', $context)
    Invoke-Native kubectl ($kubectlPrefix + @(
        'apply', '--filename',
        (Join-Path $sreRoot 'deploy/kind/phase1-shadow/network-policy.yaml')
    )) | Out-Null

    Assert-ArtifactRoot
    New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null
    $config = Invoke-Native kubectl ($kubectlPrefix + @(
        '--namespace', 'rocketmq-sre',
        'create', 'configmap', 'rocketmq-sre-phase1-shadow',
        "--from-literal=provider-mode=$providerMode",
        '--from-literal=mutation-supported=false',
        '--from-literal=executor-connected=false',
        '--from-literal=connector-identity=read_only',
        '--from-literal=manifest-schema=rocketmq-sre.shadow-eval.v1',
        '--dry-run=client',
        '--output=yaml'
    ))
    $generatedConfig = Join-Path $artifactRoot 'kind-shadow-config.yaml'
    [IO.File]::WriteAllText(
        $generatedConfig,
        $config.Output,
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Native kubectl ($kubectlPrefix + @('apply', '--filename', $generatedConfig)) |
        Out-Null
    Invoke-Native kubectl ($kubectlPrefix + @(
        '--namespace', 'rocketmq-sre',
        'delete', 'job', 'rocketmq-sre-phase1-shadow',
        '--ignore-not-found=true',
        '--wait=true'
    )) | Out-Null
    Invoke-Native kubectl ($kubectlPrefix + @(
        'apply', '--filename',
        (Join-Path $sreRoot 'deploy/kind/phase1-shadow/shadow-job.yaml')
    )) | Out-Null
    $wait = Invoke-Native kubectl ($kubectlPrefix + @(
        '--namespace', 'rocketmq-sre',
        'wait', '--for=condition=complete',
        'job/rocketmq-sre-phase1-shadow',
        '--timeout=180s'
    )) -AllowFailure
    $logs = Invoke-Native kubectl ($kubectlPrefix + @(
        '--namespace', 'rocketmq-sre',
        'logs', 'job/rocketmq-sre-phase1-shadow'
    )) -AllowFailure
    if ($wait.ExitCode -ne 0) {
        throw "Kind shadow job failed.`n$($wait.Output)`n$($logs.Output)"
    }
    $summary = Get-ShadowSummary $logs.Output
    Assert-ShadowSummary $summary
    Save-ShadowSummary $summary "kind-$providerMode"
}

switch ($Target) {
    'Offline' { Invoke-Offline }
    'Compose' { Invoke-Compose }
    'Kind' { Invoke-Kind }
}

Write-Host "PHASE01_SHADOW_OK target=$Target provider=$Provider mutation_calls=0 executor_calls=0"

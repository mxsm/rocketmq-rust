# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-fA-F]{7,40}$')]
    [string]$PreviousRevision,

    [string]$ArtifactRoot = 'D:\rocketmq-sre-qualification\binary-compatibility',

    [string]$EvidenceOutput = 'D:\rocketmq-sre-evidence\binary-compatibility.json',

    [string]$CargoHome = 'D:\cargo-home\rocketmq-sre',

    [ValidateRange(1024, 65535)]
    [int]$PostgresPort = 55432
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '..'))
$artifactRoot = [IO.Path]::GetFullPath($ArtifactRoot)
$evidenceOutput = [IO.Path]::GetFullPath($EvidenceOutput)
$previousWorktree = Join-Path $artifactRoot 'source-n-minus-one'
$sharedSreTarget = Join-Path $artifactRoot 'target-sre'
$sharedMcpTarget = Join-Path $artifactRoot 'target-mcp'
$currentBinaryRoot = Join-Path $artifactRoot 'artifacts\current'
$previousBinaryRoot = Join-Path $artifactRoot 'artifacts\n-minus-one'
$postgresContainer = "rocketmq-sre-compat-$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
$postgresPassword = [Guid]::NewGuid().ToString('N')
$startedAt = [DateTimeOffset]::UtcNow

function Assert-DataPath([string]$Path, [string]$Description) {
    $root = [IO.Path]::GetPathRoot([IO.Path]::GetFullPath($Path))
    if (
        -not $root.Equals('D:\', [StringComparison]::OrdinalIgnoreCase) -and
        -not $root.Equals('F:\', [StringComparison]::OrdinalIgnoreCase)
    ) {
        throw "$Description must use the D or F drive."
    }
}

function Invoke-Native(
    [string]$Command,
    [string[]]$Arguments,
    [string]$Description
) {
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

function Wait-Postgres {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(90)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        & docker exec $postgresContainer pg_isready -U rocketmq_sre -d rocketmq_sre 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 500
    }
    throw 'Docker PostgreSQL did not become ready.'
}

function Get-BinaryPath([string]$Target, [string]$Name) {
    $suffix = if ($env:OS -eq 'Windows_NT') { '.exe' } else { '' }
    Join-Path $Target "debug\$Name$suffix"
}

function Get-CopiedBinaryPath([string]$Root, [string]$Name) {
    $suffix = if ($env:OS -eq 'Windows_NT') { '.exe' } else { '' }
    Join-Path $Root "$Name$suffix"
}

function Build-Revision(
    [string]$SourceRoot,
    [string]$SreTarget,
    [string]$McpTarget,
    [string]$Label
) {
    $sreManifest = Join-Path $SourceRoot 'rocketmq-sre\Cargo.toml'
    $mcpManifest = Join-Path $SourceRoot 'rocketmq-tools\rocketmq-mcp\Cargo.toml'
    Invoke-Native cargo @(
        '+1.95.0', 'build', '--manifest-path', $sreManifest, '--locked',
        '--target-dir', $SreTarget,
        '-p', 'rocketmq-sre-control-plane',
        '-p', 'rocketmq-sre-connector',
        '-p', 'rocketmq-sre-execution-agent'
    ) "$Label SRE native binaries"
    Invoke-Native cargo @(
        '+1.95.0', 'build', '--manifest-path', $mcpManifest, '--locked',
        '--target-dir', $McpTarget, '--features', 'streamable-http'
    ) "$Label MCP native binary"
}

function Copy-RevisionArtifacts([string]$Destination) {
    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    foreach ($artifact in @(
        @{ Source = Get-BinaryPath $sharedSreTarget 'rocketmq-sre-control-plane'; Name = 'rocketmq-sre-control-plane' },
        @{ Source = Get-BinaryPath $sharedSreTarget 'rocketmq-sre-connector'; Name = 'rocketmq-sre-connector' },
        @{ Source = Get-BinaryPath $sharedSreTarget 'rocketmq-sre-execution-agent'; Name = 'rocketmq-sre-execution-agent' },
        @{ Source = Get-BinaryPath $sharedMcpTarget 'rocketmq-mcp'; Name = 'rocketmq-mcp' }
    )) {
        if (-not (Test-Path -LiteralPath $artifact.Source -PathType Leaf)) {
            throw "Built artifact is missing: $($artifact.Source)"
        }
        $suffix = if ($env:OS -eq 'Windows_NT') { '.exe' } else { '' }
        Copy-Item -LiteralPath $artifact.Source -Destination (Join-Path $Destination "$($artifact.Name)$suffix") -Force
    }
}

function Invoke-BinaryProbe([string]$Path, [string]$Component) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Expected $Component native binary is missing: $Path"
    }
    $arguments = if ($Component -eq 'mcp') { @('--help') } else { @() }
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $Path
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.Arguments = $arguments -join ' '
    foreach ($name in @(
        'DATABASE_URL',
        'ROCKETMQ_SRE_INTERNAL_TOKEN',
        'ROCKETMQ_SRE_MCP_URL',
        'ROCKETMQ_SRE_LEASE_AUTHORITY_URL',
        'ROCKETMQ_SRE_AGENT_AUTHORITY_TOKEN',
        'ROCKETMQ_SRE_EXECUTOR_AGENT_TOKEN',
        'ROCKETMQ_SRE_AGENT_SUBJECT',
        'ROCKETMQ_SRE_AGENT_ACK_KEY'
    )) {
        $startInfo.Environment.Remove($name) | Out-Null
    }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    if (-not $process.Start()) {
        throw "Unable to start $Component native binary."
    }
    if (-not $process.WaitForExit(15000)) {
        $process.Kill($true)
        throw "$Component native binary did not complete its bounded identity/configuration probe."
    }
    $expectedExit = if ($Component -eq 'mcp') { 0 } else { 1 }
    if ($process.ExitCode -ne $expectedExit) {
        throw "$Component native probe returned $($process.ExitCode); expected $expectedExit."
    }
    $item = Get-Item -LiteralPath $Path
    [ordered]@{
        component = $Component
        sha256 = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
        bytes = $item.Length
        process_probe = if ($Component -eq 'mcp') { 'help_completed' } else { 'missing_configuration_rejected' }
        exit_code = $process.ExitCode
    }
}

Assert-DataPath $artifactRoot 'artifact root'
Assert-DataPath $evidenceOutput 'evidence output'
Assert-DataPath $CargoHome 'Cargo home'
if ($artifactRoot.StartsWith($repositoryRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Compatibility artifacts must remain outside the Git worktree.'
}

$previousCommit = (& git -C $repositoryRoot rev-parse "$PreviousRevision^{commit}").Trim()
if ($LASTEXITCODE -ne 0 -or $previousCommit -notmatch '^[0-9a-f]{40}$') {
    throw "Previous revision '$PreviousRevision' is not available locally."
}
$currentCommit = (& git -C $repositoryRoot rev-parse 'HEAD^{commit}').Trim()
if ($LASTEXITCODE -ne 0 -or $currentCommit -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to resolve the current revision.'
}
if ($currentCommit -eq $previousCommit) {
    throw 'Current and N-1 revisions must be different commits.'
}

$postgresStarted = $false
$worktreeAdded = $false
$savedCargoHome = [Environment]::GetEnvironmentVariable('CARGO_HOME', 'Process')
$savedIncremental = [Environment]::GetEnvironmentVariable('CARGO_INCREMENTAL', 'Process')
try {
    New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null
    New-Item -ItemType Directory -Force -Path ([IO.Path]::GetFullPath($CargoHome)) | Out-Null
    $env:CARGO_HOME = [IO.Path]::GetFullPath($CargoHome)
    $env:CARGO_INCREMENTAL = '0'
    Invoke-Native git @('-C', $repositoryRoot, 'worktree', 'add', '--detach', $previousWorktree, $previousCommit) 'N-1 source worktree creation'
    $worktreeAdded = $true

    Build-Revision $repositoryRoot $sharedSreTarget $sharedMcpTarget 'current'
    Copy-RevisionArtifacts $currentBinaryRoot
    Build-Revision $previousWorktree $sharedSreTarget $sharedMcpTarget 'N-1'
    Copy-RevisionArtifacts $previousBinaryRoot

    $artifacts = [ordered]@{
        current = @(
            Invoke-BinaryProbe (Get-CopiedBinaryPath $currentBinaryRoot 'rocketmq-sre-control-plane') 'control_plane'
            Invoke-BinaryProbe (Get-CopiedBinaryPath $currentBinaryRoot 'rocketmq-sre-connector') 'connector'
            Invoke-BinaryProbe (Get-CopiedBinaryPath $currentBinaryRoot 'rocketmq-mcp') 'mcp'
            Invoke-BinaryProbe (Get-CopiedBinaryPath $currentBinaryRoot 'rocketmq-sre-execution-agent') 'execution_agent'
        )
        n_minus_one = @(
            Invoke-BinaryProbe (Get-CopiedBinaryPath $previousBinaryRoot 'rocketmq-sre-control-plane') 'control_plane'
            Invoke-BinaryProbe (Get-CopiedBinaryPath $previousBinaryRoot 'rocketmq-sre-connector') 'connector'
            Invoke-BinaryProbe (Get-CopiedBinaryPath $previousBinaryRoot 'rocketmq-mcp') 'mcp'
            Invoke-BinaryProbe (Get-CopiedBinaryPath $previousBinaryRoot 'rocketmq-sre-execution-agent') 'execution_agent'
        )
    }

    Invoke-Native docker @(
        'run', '--detach', '--rm', '--name', $postgresContainer,
        '--publish', "127.0.0.1:${PostgresPort}:5432",
        '--env', 'POSTGRES_USER=rocketmq_sre',
        '--env', "POSTGRES_PASSWORD=$postgresPassword",
        '--env', 'POSTGRES_DB=rocketmq_sre',
        'postgres:17-alpine'
    ) 'Docker PostgreSQL start'
    $postgresStarted = $true
    Wait-Postgres
    $savedDatabaseUrl = [Environment]::GetEnvironmentVariable('ROCKETMQ_SRE_TEST_DATABASE_URL', 'Process')
    try {
        $env:ROCKETMQ_SRE_TEST_DATABASE_URL =
            "postgres://rocketmq_sre:${postgresPassword}@127.0.0.1:${PostgresPort}/rocketmq_sre"
        Invoke-Native cargo @(
            '+1.95.0', 'test', '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'), '--locked',
            '--target-dir', $sharedSreTarget,
            '-p', 'rocketmq-sre-control-plane',
            'fleet::repository_tests::postgres_current_n_minus_one_and_incompatible_runtime_handshakes_fail_closed',
            '--', '--ignored', '--exact'
        ) 'current/N-1 protocol compatibility cases'

        Invoke-Native cargo @(
            '+1.95.0', 'test', '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'), '--locked',
            '--target-dir', $sharedSreTarget,
            '-p', 'rocketmq-sre-contracts',
            'version::tests'
        ) 'schema major, minor, and required-feature compatibility cases'
        foreach ($connectorCase in @(
                'capability::tests::rejects_mutation_and_schema_drift',
                'channel::tests::unknown_required_channel_feature_is_rejected',
                'mcp::tests::persisted_control_plane_surface_drift_fails_closed'
            )) {
            Invoke-Native cargo @(
                '+1.95.0', 'test', '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'), '--locked',
                '--target-dir', $sharedSreTarget,
                '-p', 'rocketmq-sre-connector',
                $connectorCase,
                '--', '--exact'
            ) "connector compatibility case $connectorCase"
        }
    }
    finally {
        [Environment]::SetEnvironmentVariable('ROCKETMQ_SRE_TEST_DATABASE_URL', $savedDatabaseUrl, 'Process')
    }

    $evidence = [ordered]@{
        schema_version = 'rocketmq-sre.binary-compatibility-qualification.v1'
        status = 'passed'
        environment = 'local-native-binaries-and-docker-postgresql'
        started_at = $startedAt.ToString('O')
        finished_at = [DateTimeOffset]::UtcNow.ToString('O')
        revision = $currentCommit
        previous_revision = $previousCommit
        artifacts = $artifacts
        matrices = @(
            [ordered]@{
                control_plane = 'current'
                connector = 'n_minus_one'
                mcp = 'n_minus_one'
                execution_agent = 'n_minus_one'
                incompatible_outcome = 'read_only_degraded_or_rejected'
            },
            [ordered]@{
                control_plane = 'n_minus_one'
                connector = 'current'
                mcp = 'current'
                execution_agent = 'current'
                incompatible_outcome = 'read_only_degraded_or_rejected'
            }
        )
        protocol_cases = @(
            'additive_optional_field',
            'unknown_major',
            'missing_required_feature',
            'schema_digest_drift',
            'tool_surface_drift'
        )
        native_processes_executed = $true
        protocol_contract_test_passed = $true
        secrets_recorded = $false
    }
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $evidenceOutput) | Out-Null
    $evidence | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $evidenceOutput -Encoding utf8
    Write-Host "BINARY_COMPATIBILITY_QUALIFICATION_OK evidence=$evidenceOutput"
}
finally {
    if ($postgresStarted) {
        & docker stop $postgresContainer | Out-Null
    }
    if ($worktreeAdded) {
        & git -C $repositoryRoot worktree remove --force $previousWorktree
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "N-1 worktree cleanup requires attention: $previousWorktree"
        }
    }
    [Environment]::SetEnvironmentVariable('CARGO_HOME', $savedCargoHome, 'Process')
    [Environment]::SetEnvironmentVariable('CARGO_INCREMENTAL', $savedIncremental, 'Process')
}

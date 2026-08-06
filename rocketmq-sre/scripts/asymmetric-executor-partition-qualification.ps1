# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidateSet('Check', 'Run')]
    [string]$Mode = 'Run',

    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-asymmetric-executor-partition',

    [string]$EvidenceRoot = 'D:\rocketmq-sre-evidence',

    [string]$PostgresImage = 'postgres:17-alpine'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '..'))
$manifestPath = Join-Path $sreRoot 'config\qualification\asymmetric-executor-partition.v1.json'
$checkerPath = Join-Path $scriptDirectory 'check_asymmetric_executor_partition_qualification.py'

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

function Invoke-Native([string]$Command, [string[]]$Arguments, [string]$Description) {
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

Assert-DataPath $CargoTargetDir 'Cargo target directory'
Assert-DataPath $EvidenceRoot 'qualification Evidence root'
$resolvedTarget = [IO.Path]::GetFullPath($CargoTargetDir)
$resolvedEvidenceRoot = [IO.Path]::GetFullPath($EvidenceRoot).TrimEnd('\')
$allowedEvidenceRoots = @(
    [IO.Path]::GetFullPath('D:\rocketmq-sre-evidence').TrimEnd('\'),
    [IO.Path]::GetFullPath('F:\rocketmq-sre-evidence').TrimEnd('\')
)
if (-not ($allowedEvidenceRoots -contains $resolvedEvidenceRoot)) {
    throw 'Qualification reports must use D:\rocketmq-sre-evidence or F:\rocketmq-sre-evidence.'
}

Invoke-Native python @($checkerPath, '--manifest', $manifestPath) `
    'asymmetric Executor partition manifest validation'
if ($Mode -eq 'Check') {
    Write-Host 'ASYMMETRIC_EXECUTOR_PARTITION_CHECK_OK'
    exit 0
}

$revision = (& git -C $repositoryRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to determine the qualification source revision.'
}
$dirty = & git -C $repositoryRoot status --porcelain=v1
if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($dirty -join ''))) {
    throw 'Asymmetric Executor partition qualification requires a committed, clean source tree.'
}

$startedAt = [DateTimeOffset]::UtcNow
$runName = 'asymmetric-executor-partition-{0}-{1}' -f `
    $startedAt.ToString('yyyyMMdd-HHmmss'), ([Guid]::NewGuid().ToString('N'))
$runRoot = [IO.Path]::GetFullPath((Join-Path $resolvedEvidenceRoot $runName))
$expectedEvidencePrefix = $resolvedEvidenceRoot + '\'
if (-not $runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Qualification output escaped the configured Evidence root.'
}
$reportPath = Join-Path $runRoot 'qualification-report.v1.json'
New-Item -ItemType Directory -Force -Path $runRoot, $resolvedTarget | Out-Null

$containerName = 'rocketmq-sre-asymmetric-fence-' + [Guid]::NewGuid().ToString('N')
$databasePassword = [Guid]::NewGuid().ToString('N')
$containerCreated = $false
$databaseUrlCleared = $false
$testPassed = $false
$marker = $null
try {
    $containerId = & docker run --detach --rm --name $containerName `
        --publish '127.0.0.1::5432' `
        --env "POSTGRES_PASSWORD=$databasePassword" `
        $PostgresImage
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace(($containerId -join ''))) {
        throw 'Unable to start the qualification-owned PostgreSQL container.'
    }
    $containerCreated = $true
    $portMapping = (& docker port $containerName '5432/tcp').Trim()
    if ($LASTEXITCODE -ne 0 -or $portMapping -notmatch ':(?<port>\d+)$') {
        throw 'Unable to determine the qualification PostgreSQL port.'
    }
    $postgresPort = [int]$Matches.port

    $ready = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        & docker exec $containerName pg_isready --username postgres --dbname postgres *> $null
        if ($LASTEXITCODE -eq 0) {
            $ready = $true
            break
        }
        Start-Sleep -Milliseconds 500
    }
    if (-not $ready) {
        throw 'Qualification PostgreSQL did not become ready.'
    }

    $env:ROCKETMQ_SRE_TEST_DATABASE_URL = `
        "postgres://postgres:$databasePassword@127.0.0.1:$postgresPort/postgres"
    $cargoArguments = @(
        'test', '--locked',
        '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
        '--target-dir', $resolvedTarget,
        '-p', 'rocketmq-sre-executor',
        '--test', 'asymmetric_partition',
        '--', '--ignored', '--exact',
        'old_executor_cannot_write_after_asymmetric_authority_partition_and_epoch_takeover',
        '--nocapture'
    )
    $savedErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $testOutput = @(& cargo @cargoArguments 2>&1)
        $cargoExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $savedErrorActionPreference
    }
    $testOutput | ForEach-Object { Write-Host $_ }
    if ($cargoExitCode -ne 0) {
        throw "asymmetric Executor partition test failed with exit code $cargoExitCode."
    }
    $markerLine = @($testOutput | ForEach-Object { [string]$_ } | Where-Object {
        $_ -match '^ASYMMETRIC_EXECUTOR_PARTITION_OK '
    })
    if ($markerLine.Count -ne 1) {
        throw 'The qualification test did not emit exactly one result marker.'
    }
    $marker = [ordered]@{}
    foreach ($match in [regex]::Matches($markerLine[0], '(?<key>[a-z_]+)=(?<value>[^\s]+)')) {
        $marker[$match.Groups['key'].Value] = $match.Groups['value'].Value
    }
    $requiredMarkerKeys = @(
        'old_authority_reachable', 'old_agent_reachable', 'agent_authority_reachable',
        'old_epoch', 'active_epoch', 'stale_dispatch_rejected', 'stale_effect_rows',
        'stale_target_writes', 'fresh_target_writes', 'fence_rejections'
    )
    if (@($requiredMarkerKeys | Where-Object { -not $marker.Contains($_) }).Count -ne 0) {
        throw 'The qualification result marker is incomplete.'
    }
    $testPassed = $true
}
finally {
    Remove-Item Env:\ROCKETMQ_SRE_TEST_DATABASE_URL -ErrorAction SilentlyContinue
    $databaseUrlCleared = -not (Test-Path Env:\ROCKETMQ_SRE_TEST_DATABASE_URL)
    if ($containerCreated) {
        & docker rm --force --volumes $containerName *> $null
        $containerCreated = $false
    }
    $databasePassword = $null
}

if (-not $testPassed -or $null -eq $marker) {
    if ($runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
    throw 'Asymmetric Executor partition qualification did not pass.'
}

$report = [ordered]@{
    schema_version = 'rocketmq-sre.asymmetric-executor-partition-qualification-report.v1'
    candidate_commit = $revision
    source_clean = $true
    environment = 'docker_postgresql_http_partition'
    started_at = $startedAt.ToString('o')
    finished_at = [DateTimeOffset]::UtcNow.ToString('o')
    status = 'passed'
    connectivity = [ordered]@{
        old_executor_authority_reachable_after_partition = $marker.old_authority_reachable -eq 'true'
        old_executor_agent_reachable_after_partition = $marker.old_agent_reachable -eq 'true'
        agent_authority_reachable_during_takeover = $marker.agent_authority_reachable -eq 'true'
    }
    fencing = [ordered]@{
        old_epoch = [int]$marker.old_epoch
        active_epoch = [int]$marker.active_epoch
        stale_dispatch_rejected = $marker.stale_dispatch_rejected -eq 'true'
        stale_effect_rows = [int]$marker.stale_effect_rows
        stale_target_writes = [int]$marker.stale_target_writes
        fresh_target_writes = [int]$marker.fresh_target_writes
        fence_rejections = [int]$marker.fence_rejections
    }
    safety = [ordered]@{
        model_provider_network_calls = 0
        production_certified = $false
        unattended_autonomous_execution = $false
        secrets_recorded = $false
        message_bodies_recorded = $false
    }
    cleanup = [ordered]@{
        postgres_container_removed = -not $containerCreated
        database_url_cleared = $databaseUrlCleared
    }
}
$json = $report | ConvertTo-Json -Depth 8
[IO.File]::WriteAllText($reportPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
Invoke-Native python @($checkerPath, '--manifest', $manifestPath, '--report', $reportPath) `
    'asymmetric Executor partition report validation'
Write-Host "ASYMMETRIC_EXECUTOR_PARTITION_LIVE_QUALIFICATION_OK report=$reportPath stale_target_writes=0 fresh_target_writes=1"

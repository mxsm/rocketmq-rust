# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidateSet('Check', 'Run')]
    [string]$Mode = 'Run',

    [string]$SecretFile,

    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-deepseek-diagnosis',

    [string]$EvidenceRoot = 'D:\rocketmq-sre-evidence',

    [string]$PostgresImage = 'postgres:17-alpine'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '..'))
$manifestPath = Join-Path $sreRoot 'config\qualification\deepseek-diagnosis.v1.json'
$checkerPath = Join-Path $scriptDirectory 'check_deepseek_diagnosis_qualification.py'
$apiKeyEnvironment = 'ROCKETMQ_SRE_LIVE_DEEPSEEK_API_KEY'
$databaseUrlEnvironment = 'ROCKETMQ_SRE_TEST_DATABASE_URL'

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

Invoke-Native python @($checkerPath, '--manifest', $manifestPath) 'DeepSeek diagnosis manifest validation'
if ($Mode -eq 'Check') {
    Write-Host 'DEEPSEEK_DIAGNOSIS_CHECK_OK'
    exit 0
}

if ([string]::IsNullOrWhiteSpace($SecretFile)) {
    throw 'Run mode requires an explicit local -SecretFile path.'
}

$resolvedSecretFile = [IO.Path]::GetFullPath($SecretFile)
if (-not [IO.File]::Exists($resolvedSecretFile)) {
    throw 'The explicitly selected local secret file does not exist.'
}
if ($resolvedSecretFile.StartsWith($repositoryRoot + '\', [StringComparison]::OrdinalIgnoreCase)) {
    throw 'The DeepSeek credential file must remain outside the repository.'
}
$secretLength = [IO.FileInfo]::new($resolvedSecretFile).Length
if ($secretLength -lt 1 -or $secretLength -gt 4096) {
    throw 'The local DeepSeek credential file size is invalid.'
}

$revision = (& git -C $repositoryRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to determine the qualification source revision.'
}
$dirty = & git -C $repositoryRoot status --porcelain=v1
if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($dirty -join ''))) {
    throw 'DeepSeek diagnosis qualification requires a committed, clean source tree.'
}

$startedAt = [DateTimeOffset]::UtcNow
$runName = 'deepseek-diagnosis-{0}-{1}' -f `
    $startedAt.ToString('yyyyMMdd-HHmmss'), ([Guid]::NewGuid().ToString('N'))
$runRoot = [IO.Path]::GetFullPath((Join-Path $resolvedEvidenceRoot $runName))
$expectedEvidencePrefix = $resolvedEvidenceRoot + '\'
if (-not $runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Qualification output escaped the configured Evidence root.'
}
$reportPath = Join-Path $runRoot 'qualification-report.v1.json'
New-Item -ItemType Directory -Force -Path $runRoot, $resolvedTarget | Out-Null

$containerName = 'rocketmq-sre-deepseek-' + [Guid]::NewGuid().ToString('N')
$databasePassword = [Guid]::NewGuid().ToString('N')
$containerCreated = $false
$databaseUrlCleared = $false
$apiKeyEnvironmentCleared = $false
$testPassed = $false
$marker = $null
$apiKey = $null
try {
    $apiKey = [IO.File]::ReadAllText($resolvedSecretFile).Trim()
    if ([string]::IsNullOrWhiteSpace($apiKey)) {
        throw 'The local DeepSeek credential file is empty.'
    }
    [Environment]::SetEnvironmentVariable($apiKeyEnvironment, $apiKey, 'Process')

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

    [Environment]::SetEnvironmentVariable(
        $databaseUrlEnvironment,
        "postgres://postgres:$databasePassword@127.0.0.1:$postgresPort/postgres",
        'Process'
    )
    $cargoArguments = @(
        'test', '--locked',
        '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
        '--target-dir', $resolvedTarget,
        '-p', 'rocketmq-sre-control-plane', '--lib',
        'models::service::live_deepseek::deepseek_responses_produces_persisted_read_only_sre_diagnosis',
        '--', '--ignored', '--exact', '--nocapture'
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
    if ($cargoExitCode -ne 0) {
        throw "DeepSeek diagnosis test failed with exit code $cargoExitCode; model payloads were not retained."
    }
    $markerLine = @($testOutput | ForEach-Object { [string]$_ } | Where-Object {
        $_ -match '^DEEPSEEK_DIAGNOSIS_QUALIFICATION_OK '
    })
    if ($markerLine.Count -ne 1) {
        throw 'The qualification test did not emit exactly one sanitized result marker.'
    }
    $markerJson = $markerLine[0].Substring('DEEPSEEK_DIAGNOSIS_QUALIFICATION_OK '.Length)
    $marker = $markerJson | ConvertFrom-Json
    $testPassed = $true
}
finally {
    [Environment]::SetEnvironmentVariable($apiKeyEnvironment, $null, 'Process')
    [Environment]::SetEnvironmentVariable($databaseUrlEnvironment, $null, 'Process')
    $apiKeyEnvironmentCleared = [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($apiKeyEnvironment))
    $databaseUrlCleared = [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($databaseUrlEnvironment))
    $apiKey = $null
    $databasePassword = $null
    if ($containerCreated) {
        & docker rm --force --volumes $containerName *> $null
        $containerCreated = $false
    }
}

if (-not $testPassed -or $null -eq $marker) {
    if ($runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
    throw 'DeepSeek diagnosis qualification did not pass.'
}

$report = [ordered]@{
    schema_version = 'rocketmq-sre.deepseek-diagnosis-qualification-report.v1'
    candidate_commit = $revision
    source_clean = $true
    environment = 'docker_postgresql_deepseek_responses'
    provider = 'deepseek'
    protocol = 'responses_api'
    model = 'deepseek-v4-flash'
    started_at = $startedAt.ToString('o')
    finished_at = [DateTimeOffset]::UtcNow.ToString('o')
    status = 'passed'
    diagnosis = [ordered]@{
        mode = [string]$marker.mode
        authorized_evidence_citations = [bool]$marker.authorized_evidence_citations
        cited_evidence_count = [int]$marker.cited_evidence_count
        input_tokens_present = [bool]$marker.input_tokens_present
        output_tokens_present = [bool]$marker.output_tokens_present
        schema_repairs = [int]$marker.schema_repairs
        diagnosis_attempts = [int]$marker.diagnosis_attempts
        rules_only_fallbacks = [int]$marker.rules_only_fallbacks
        model_network_calls = [int]$marker.model_network_calls
        invocation_persisted = [bool]$marker.invocation_persisted
        stream_sessions = [int]$marker.stream_sessions
        completed_semantic_streams = [int]$marker.completed_semantic_streams
        stream_event_count = [int]$marker.stream_event_count
        stream_terminal_verified = [bool]$marker.stream_terminal_verified
        stream_cancellation_verified = [bool]$marker.stream_cancellation_verified
        read_only_tool_selections = [int]$marker.read_only_tool_selections
        tool_selection_protocol = [string]$marker.tool_selection_protocol
        tool_execution_calls = [int]$marker.tool_execution_calls
        mutation_calls = [int]$marker.mutation_calls
        execution_eligible = [bool]$marker.execution_eligible
    }
    safety = [ordered]@{
        production_certified = $false
        unattended_autonomous_execution = $false
        secrets_recorded = $false
        prompts_recorded = $false
        responses_recorded = $false
        message_bodies_recorded = $false
    }
    cleanup = [ordered]@{
        postgres_container_removed = -not $containerCreated
        database_url_cleared = $databaseUrlCleared
        api_key_environment_cleared = $apiKeyEnvironmentCleared
    }
}
$json = $report | ConvertTo-Json -Depth 8
[IO.File]::WriteAllText($reportPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
Invoke-Native python @($checkerPath, '--manifest', $manifestPath, '--report', $reportPath) `
    'DeepSeek diagnosis report validation'
Write-Host "DEEPSEEK_DIAGNOSIS_LIVE_QUALIFICATION_OK report=$reportPath model=deepseek-v4-flash production_certified=false"

# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidateSet('Check', 'Run')]
    [string]$Mode = 'Run',

    [string]$SecretFile,

    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-provider-failover',

    [string]$EvidenceRoot = 'D:\rocketmq-sre-evidence',

    [string]$PostgresImage = 'postgres:17-alpine'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '..'))
$manifestPath = Join-Path $sreRoot 'config\qualification\provider-failover.v1.json'
$checkerPath = Join-Path $scriptDirectory 'check_provider_failover_qualification.py'
$apiKeyEnvironment = 'ROCKETMQ_SRE_LIVE_DEEPSEEK_API_KEY'
$loopbackCredentialEnvironment = 'ROCKETMQ_SRE_LIVE_DEEPSEEK_LOOPBACK_TOKEN'
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
    if ($fullPath.TrimEnd('\') -eq $root.TrimEnd('\')) {
        throw "$Description must use a dedicated directory below the drive root."
    }
}

function Invoke-Native([string]$Command, [string[]]$Arguments, [string]$Description) {
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

foreach ($dependency in @($manifestPath, $checkerPath)) {
    if (-not [IO.File]::Exists($dependency)) {
        throw "Qualification dependency is missing: $dependency"
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
    throw 'Qualification reports must use the dedicated D: or F: evidence root.'
}

Invoke-Native python @($checkerPath, '--manifest', $manifestPath) 'provider-failover manifest validation'
if ($Mode -eq 'Check') {
    Write-Host 'PROVIDER_FAILOVER_CHECK_OK scenarios=6'
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
    throw 'Provider-failover qualification requires a committed, clean source tree.'
}

$startedAt = [DateTimeOffset]::UtcNow
$runName = 'provider-failover-{0}-{1}' -f `
    $startedAt.ToString('yyyyMMdd-HHmmss'), ([Guid]::NewGuid().ToString('N'))
$runRoot = [IO.Path]::GetFullPath((Join-Path $resolvedEvidenceRoot $runName))
$expectedEvidencePrefix = $resolvedEvidenceRoot + '\'
if (-not $runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Qualification output escaped the configured Evidence root.'
}
$reportPath = Join-Path $runRoot 'qualification-report.v1.json'
New-Item -ItemType Directory -Force -Path $runRoot, $resolvedTarget | Out-Null

$containerName = 'rocketmq-sre-provider-failover-' + [Guid]::NewGuid().ToString('N')
$databasePassword = [Guid]::NewGuid().ToString('N')
$containerCreated = $false
$databaseUrlCleared = $false
$apiKeyEnvironmentCleared = $false
$loopbackCredentialEnvironmentCleared = $false
$testPassed = $false
$marker = $null
$apiKey = $null
try {
    $apiKey = [IO.File]::ReadAllText($resolvedSecretFile).Trim()
    if ([string]::IsNullOrWhiteSpace($apiKey)) {
        throw 'The local DeepSeek credential file is empty.'
    }
    [Environment]::SetEnvironmentVariable($apiKeyEnvironment, $apiKey, 'Process')
    [Environment]::SetEnvironmentVariable(
        $loopbackCredentialEnvironment,
        [Guid]::NewGuid().ToString('N'),
        'Process'
    )

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
        'models::service::live_provider_failover::transient_primary_falls_back_to_live_deepseek_and_failures_remain_rules_only',
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
        throw "Provider-failover test failed with exit code $cargoExitCode; model payloads were not retained."
    }
    $markerLine = @($testOutput | ForEach-Object { [string]$_ } | Where-Object {
        $_ -match '^PROVIDER_FAILOVER_QUALIFICATION_OK '
    })
    if ($markerLine.Count -ne 1) {
        throw 'The qualification test did not emit exactly one sanitized result marker.'
    }
    $markerJson = $markerLine[0].Substring('PROVIDER_FAILOVER_QUALIFICATION_OK '.Length)
    $marker = $markerJson | ConvertFrom-Json
    $testPassed = $true
}
finally {
    [Environment]::SetEnvironmentVariable($apiKeyEnvironment, $null, 'Process')
    [Environment]::SetEnvironmentVariable($loopbackCredentialEnvironment, $null, 'Process')
    [Environment]::SetEnvironmentVariable($databaseUrlEnvironment, $null, 'Process')
    $apiKeyEnvironmentCleared = [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($apiKeyEnvironment))
    $loopbackCredentialEnvironmentCleared = [string]::IsNullOrEmpty(
        [Environment]::GetEnvironmentVariable($loopbackCredentialEnvironment)
    )
    $databaseUrlCleared = [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($databaseUrlEnvironment))
    $apiKey = $null
    $databasePassword = $null
    if ($containerCreated) {
        & docker rm --force --volumes $containerName *> $null
        $containerCreated = $false
    }
}

if (-not $testPassed -or $null -eq $marker) {
    if (
        [IO.Directory]::Exists($runRoot) -and
        $runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)
    ) {
        [IO.Directory]::Delete($runRoot, $true)
    }
    throw 'Provider-failover qualification did not pass.'
}

$report = [ordered]@{
    schema_version = 'rocketmq-sre.provider-failover-qualification-report.v1'
    status = 'passed'
    candidate_commit = $revision
    source_clean = $true
    environment = 'docker_postgresql_local_primary_deepseek_secondary'
    started_at = $startedAt.ToString('o')
    finished_at = [DateTimeOffset]::UtcNow.ToString('o')
    scenarios = [ordered]@{
        transient_primary_to_live_secondary = $marker.scenarios.transient_primary_to_live_secondary
        policy_denial_stops_fallback = $marker.scenarios.policy_denial_stops_fallback
        unsupported_capability_stops_fallback = $marker.scenarios.unsupported_capability_stops_fallback
        invalid_schema_stops_fallback = $marker.scenarios.invalid_schema_stops_fallback
        invalid_citation_stops_fallback = $marker.scenarios.invalid_citation_stops_fallback
        all_unavailable_rules_only = $marker.scenarios.all_unavailable_rules_only
    }
    provider_certification = [ordered]@{
        deepseek = [string]$marker.provider_certification.deepseek
        zhipu_glm = [string]$marker.provider_certification.zhipu_glm
        kimi_moonshot = [string]$marker.provider_certification.kimi_moonshot
    }
    safety = [ordered]@{
        effective_access = 'read_only'
        production_certified = $false
        unattended_autonomous_execution = $false
        mutation_calls = [int]$marker.mutation_calls
        executor_calls = [int]$marker.executor_calls
        execution_agent_calls = [int]$marker.execution_agent_calls
        secrets_recorded = $false
        prompts_recorded = $false
        responses_recorded = $false
        message_bodies_recorded = $false
    }
    cleanup = [ordered]@{
        postgres_container_removed = -not $containerCreated
        database_url_cleared = $databaseUrlCleared
        api_key_environment_cleared = $apiKeyEnvironmentCleared
        loopback_credential_environment_cleared = $loopbackCredentialEnvironmentCleared
    }
}
$json = $report | ConvertTo-Json -Depth 10
[IO.File]::WriteAllText($reportPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
Invoke-Native python @($checkerPath, '--manifest', $manifestPath, '--report', $reportPath) `
    'provider-failover report validation'
Write-Host "PROVIDER_FAILOVER_LIVE_QUALIFICATION_OK report=$reportPath production_certified=false"

# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidateSet('Check', 'Run')]
    [string]$Mode = 'Run',

    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-local-model',

    [string]$EvidenceRoot = 'D:\rocketmq-sre-evidence',

    [string]$OllamaImage = 'ollama/ollama:0.13.3',

    [string]$Model = 'qwen2.5:0.5b'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$manifestPath = Join-Path $sreRoot 'config\qualification\local-model.v1.json'
$checkerPath = Join-Path $scriptDirectory 'check_local_model_qualification.py'
$endpointEnvironment = 'ROCKETMQ_SRE_LOCAL_MODEL_QUALIFICATION_ENDPOINT'
$modelEnvironment = 'ROCKETMQ_SRE_LOCAL_MODEL_QUALIFICATION_MODEL'

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

function Get-DockerImageId([string]$Image) {
    $savedErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = @(& docker image inspect --format '{{.Id}}' $Image 2>$null)
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $savedErrorActionPreference
    }
    if ($exitCode -ne 0) {
        return $null
    }
    return ($output -join '').Trim()
}

function Test-DockerObjectAbsent([string[]]$Arguments) {
    $savedErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & docker @Arguments 2>$null | Out-Null
        return $LASTEXITCODE -ne 0
    }
    finally {
        $ErrorActionPreference = $savedErrorActionPreference
    }
}

if ($OllamaImage -ne 'ollama/ollama:0.13.3') {
    throw 'The qualification runtime image must remain pinned to ollama/ollama:0.13.3.'
}
if ($Model -ne 'qwen2.5:0.5b') {
    throw 'The qualification model must remain pinned to qwen2.5:0.5b.'
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

Invoke-Native python @($checkerPath, '--manifest', $manifestPath) 'local-model manifest validation'
if ($Mode -eq 'Check') {
    Write-Host 'LOCAL_MODEL_QUALIFICATION_CHECK_OK'
    exit 0
}

Invoke-Native docker @('version', '--format', '{{.Server.Version}}') 'Docker server availability check'
$revision = (& git -C $repositoryRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to determine the qualification source revision.'
}
$dirty = & git -C $repositoryRoot status --porcelain=v1
if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($dirty -join ''))) {
    throw 'Local-model qualification requires a committed, clean source tree.'
}

$startedAt = [DateTimeOffset]::UtcNow
$runName = 'local-model-{0}-{1}' -f `
    $startedAt.ToString('yyyyMMdd-HHmmss'), ([Guid]::NewGuid().ToString('N'))
$runRoot = [IO.Path]::GetFullPath((Join-Path $resolvedEvidenceRoot $runName))
$expectedEvidencePrefix = $resolvedEvidenceRoot + '\'
if (-not $runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Qualification output escaped the configured Evidence root.'
}
$reportPath = Join-Path $runRoot 'qualification-report.v1.json'
New-Item -ItemType Directory -Force -Path $runRoot, $resolvedTarget | Out-Null

$suffix = [Guid]::NewGuid().ToString('N')
$containerName = "rocketmq-sre-ollama-$suffix"
$volumeName = "rocketmq-sre-ollama-model-$suffix"
$imageIdBefore = Get-DockerImageId $OllamaImage
$imageExistedBefore = -not [string]::IsNullOrWhiteSpace($imageIdBefore)
$containerCreated = $false
$volumeCreated = $false
$containerRemoved = $false
$volumeRemoved = $false
$endpointEnvironmentCleared = $false
$modelEnvironmentCleared = $false
$imageStateRestored = $false
$testPassed = $false
$marker = $null
$qualificationError = $null
$imageId = $null
$modelDigest = $null
$modelSize = 0
try {
    Invoke-Native docker @('volume', 'create', $volumeName) 'qualification model volume creation'
    $volumeCreated = $true
    $containerId = & docker run --detach --name $containerName `
        --publish '127.0.0.1::11434' `
        --volume "${volumeName}:/root/.ollama" `
        $OllamaImage
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace(($containerId -join ''))) {
        throw 'Unable to start the qualification-owned Ollama container.'
    }
    $containerCreated = $true
    $imageId = Get-DockerImageId $OllamaImage
    if ($imageId -notmatch '^sha256:[0-9a-f]{64}$') {
        throw 'Unable to resolve the pinned Ollama image digest.'
    }
    $portMapping = (& docker port $containerName '11434/tcp').Trim()
    if ($LASTEXITCODE -ne 0 -or $portMapping -notmatch '127\.0\.0\.1:(?<port>\d+)$') {
        throw 'Unable to determine the loopback-only Ollama port.'
    }
    $ollamaPort = [int]$Matches.port

    $ready = $false
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        try {
            $version = Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:$ollamaPort/api/version" -TimeoutSec 2
            if (-not [string]::IsNullOrWhiteSpace([string]$version.version)) {
                $ready = $true
                break
            }
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
    }
    if (-not $ready) {
        throw 'Qualification-owned Ollama did not become ready.'
    }

    Invoke-Native docker @('exec', $containerName, 'ollama', 'pull', $Model) 'bounded local model pull'
    $tags = Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:$ollamaPort/api/tags" -TimeoutSec 10
    $qualifiedModels = @($tags.models | Where-Object { $_.name -eq $Model -or $_.model -eq $Model })
    if ($qualifiedModels.Count -ne 1) {
        throw 'The qualification-owned Ollama runtime did not expose exactly one pinned model.'
    }
    $modelDigest = [string]$qualifiedModels[0].digest
    if ($modelDigest -match '^[0-9a-f]{64}$') {
        $modelDigest = "sha256:$modelDigest"
    }
    $modelSize = [int64]$qualifiedModels[0].size
    if ($modelDigest -notmatch '^sha256:[0-9a-f]{64}$' -or $modelSize -lt 1 -or $modelSize -gt 450000000) {
        throw 'The local model digest or artifact size violated the qualification contract.'
    }

    [Environment]::SetEnvironmentVariable(
        $endpointEnvironment,
        "http://127.0.0.1:$ollamaPort/v1",
        'Process'
    )
    [Environment]::SetEnvironmentVariable($modelEnvironment, $Model, 'Process')
    $cargoArguments = @(
        'test', '--locked',
        '--manifest-path', (Join-Path $sreRoot 'Cargo.toml'),
        '--target-dir', $resolvedTarget,
        '-p', 'rocketmq-sre-model-gateway', '--test', 'live_local_model_smoke',
        'live_ollama_openai_compatible_endpoint_when_explicitly_configured',
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
        throw "Local-model test failed with exit code $cargoExitCode; model payloads were not retained."
    }
    $markerLine = @($testOutput | ForEach-Object { [string]$_ } | Where-Object {
        $_ -match '^LOCAL_MODEL_QUALIFICATION_OK '
    })
    if ($markerLine.Count -ne 1) {
        throw 'The local-model test did not emit exactly one sanitized result marker.'
    }
    $markerJson = $markerLine[0].Substring('LOCAL_MODEL_QUALIFICATION_OK '.Length)
    $marker = $markerJson | ConvertFrom-Json
    $testPassed = $true
}
catch {
    $qualificationError = $_.Exception.Message
}
finally {
    [Environment]::SetEnvironmentVariable($endpointEnvironment, $null, 'Process')
    [Environment]::SetEnvironmentVariable($modelEnvironment, $null, 'Process')
    $endpointEnvironmentCleared = [string]::IsNullOrEmpty(
        [Environment]::GetEnvironmentVariable($endpointEnvironment, 'Process')
    )
    $modelEnvironmentCleared = [string]::IsNullOrEmpty(
        [Environment]::GetEnvironmentVariable($modelEnvironment, 'Process')
    )
    if ($containerCreated) {
        & docker rm --force --volumes $containerName *> $null
    }
    $containerRemoved = Test-DockerObjectAbsent -Arguments @('container', 'inspect', $containerName)
    if ($volumeCreated) {
        & docker volume rm --force $volumeName *> $null
    }
    $volumeRemoved = Test-DockerObjectAbsent -Arguments @('volume', 'inspect', $volumeName)
    if (-not $imageExistedBefore) {
        & docker image rm --force $OllamaImage *> $null
        $imageStateRestored = [string]::IsNullOrWhiteSpace((Get-DockerImageId $OllamaImage))
    }
    else {
        $imageStateRestored = (Get-DockerImageId $OllamaImage) -eq $imageIdBefore
    }
}

if (
    $null -ne $qualificationError -or -not $testPassed -or $null -eq $marker -or
    -not $containerRemoved -or -not $volumeRemoved -or
    -not $endpointEnvironmentCleared -or -not $modelEnvironmentCleared -or
    -not $imageStateRestored
) {
    if ($runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
    throw 'Local-model qualification or cleanup did not pass.'
}

$report = [ordered]@{
    schema_version = 'rocketmq-sre.local-model-qualification-report.v1'
    candidate_commit = $revision
    source_clean = $true
    environment = 'disposable_docker_loopback_ollama'
    operating_mode = 'supervised_read_only'
    started_at = $startedAt.ToString('o')
    finished_at = [DateTimeOffset]::UtcNow.ToString('o')
    status = 'passed'
    runtime = [ordered]@{
        provider = [string]$marker.provider
        protocol = 'openai_compatible_chat_completions'
        image = $OllamaImage
        image_id = $imageId
        model = $Model
        model_digest = $modelDigest
        model_size_bytes = $modelSize
        endpoint_scope = 'loopback_only'
        model_calls = 1
        response_non_empty = [bool]$marker.response_non_empty
        response_bytes = [int]$marker.response_bytes
        tool_calls = [int]$marker.tool_calls
        credential_present = [bool]$marker.credential_present
        input_tokens = [int]$marker.input_tokens
        output_tokens = [int]$marker.output_tokens
        artifact_download_network = $true
    }
    safety = [ordered]@{
        production_certified = $false
        unattended_autonomous_execution = $false
        external_model_provider_calls = 0
        target_mutations = 0
        executor_calls = 0
        execution_agent_calls = 0
        secrets_recorded = $false
        prompts_recorded = $false
        responses_recorded = $false
        message_bodies_recorded = $false
    }
    cleanup = [ordered]@{
        container_removed = $containerRemoved
        volume_removed = $volumeRemoved
        endpoint_environment_cleared = $endpointEnvironmentCleared
        model_environment_cleared = $modelEnvironmentCleared
        image_preexisting_before = $imageExistedBefore
        image_state_restored = $imageStateRestored
    }
}
$json = $report | ConvertTo-Json -Depth 8
[IO.File]::WriteAllText($reportPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
Invoke-Native python @($checkerPath, '--manifest', $manifestPath, '--report', $reportPath) `
    'local-model qualification report validation'
Write-Host "LOCAL_MODEL_LIVE_QUALIFICATION_OK report=$reportPath provider=ollama model=$Model production_certified=false"

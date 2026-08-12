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
    [ValidateSet("Plan", "Prepare", "Validate")]
    [string]$Mode = "Plan",
    [string]$BaselineRoot,
    [string]$CandidateRoot,
    [string]$OutputDirectory = "target/message-path-evidence-inputs",
    [string]$Registry = "127.0.0.1:5001",
    [string]$RegistryContainerName = "rocketmq-message-path-registry"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$Services = @("broker", "namesrv", "controller", "proxy", "mcp")

function Invoke-Captured {
    param([Parameter(Mandatory)][string]$Executable, [Parameter(ValueFromRemainingArguments)][string[]]$Arguments)

    $value = (& $Executable @Arguments 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) { throw "$Executable failed with exit code $LASTEXITCODE`n$value" }
    return $value
}

function Write-Json {
    param([Parameter(Mandatory)][object]$Value, [Parameter(Mandatory)][string]$Path)

    $json = $Value | ConvertTo-Json -Depth 32
    [System.IO.File]::WriteAllText($Path, ($json.TrimEnd() + "`n"), [System.Text.UTF8Encoding]::new($false))
}

function Get-FileSha256 {
    param([Parameter(Mandatory)][string]$Path)
    return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

function Get-TextSha256 {
    param([Parameter(Mandatory)][string]$Text)
    $algorithm = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.UTF8Encoding]::new($false).GetBytes($Text)
        return [BitConverter]::ToString($algorithm.ComputeHash($bytes)).Replace("-", "").ToLowerInvariant()
    }
    finally { $algorithm.Dispose() }
}

function Resolve-CleanCheckout {
    param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)][string]$Role)

    if ([string]::IsNullOrWhiteSpace($Path)) { throw "$Role root is required" }
    $resolved = (Resolve-Path -LiteralPath $Path).Path
    $inside = Invoke-Captured git -C $resolved rev-parse --is-inside-work-tree
    if ($inside -ne "true") { throw "$Role root is not a Git worktree" }
    $dirty = Invoke-Captured git -C $resolved status --porcelain --untracked-files=normal
    if (-not [string]::IsNullOrWhiteSpace($dirty)) { throw "$Role worktree must be clean" }
    return $resolved
}

function Test-ImageMap {
    param([Parameter(Mandatory)][object]$Map, [Parameter(Mandatory)][string]$Role)

    $names = @($Map.PSObject.Properties.Name | Sort-Object)
    if (($names -join ",") -ne (($Services | Sort-Object) -join ",")) {
        throw "$Role image map must contain exactly: $($Services -join ', ')"
    }
    foreach ($service in $Services) {
        $value = [string]$Map.$service
        if ($value -notmatch '^[^@\s]+@sha256:[0-9a-f]{64}$') {
            throw "$Role $service must use a registry manifest digest"
        }
    }
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$output = if ([System.IO.Path]::IsPathRooted($OutputDirectory)) {
    [System.IO.Path]::GetFullPath($OutputDirectory)
}
else { [System.IO.Path]::GetFullPath((Join-Path $root $OutputDirectory)) }
$rootPrefix = $root.TrimEnd("\", "/") + [System.IO.Path]::DirectorySeparatorChar
if (-not $output.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "OutputDirectory must remain inside the current repository"
}
if ($Registry -notmatch '^(?:localhost|127\.0\.0\.1):[1-9][0-9]{0,4}$') {
    throw "Registry must be a loopback host and explicit port"
}
if ($RegistryContainerName -notmatch '^[a-z0-9][a-z0-9_.-]{2,62}$') {
    throw "RegistryContainerName is invalid"
}

$baselineMapPath = Join-Path $output "baseline-images.json"
$candidateMapPath = Join-Path $output "candidate-images.json"
$provenancePath = Join-Path $output "image-provenance.json"
if ($Mode -eq "Validate") {
    foreach ($path in @($baselineMapPath, $candidateMapPath, $provenancePath)) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "missing evidence image artifact: $path" }
    }
    $baselineMap = Get-Content -Raw -LiteralPath $baselineMapPath | ConvertFrom-Json
    $candidateMap = Get-Content -Raw -LiteralPath $candidateMapPath | ConvertFrom-Json
    Test-ImageMap -Map $baselineMap -Role "baseline"
    Test-ImageMap -Map $candidateMap -Role "candidate"
    $provenance = Get-Content -Raw -LiteralPath $provenancePath | ConvertFrom-Json
    if ($provenance.schema_version -ne 1 -or $provenance.artifact_kind -ne "rocketmq_local_evidence_image_provenance") {
        throw "image provenance contract is invalid"
    }
    if ($provenance.baseline.commit -eq $provenance.candidate.commit) {
        throw "baseline and candidate commits must differ"
    }
    if ($provenance.baseline.image_map_sha256 -ne ("sha256:" + (Get-FileSha256 $baselineMapPath))) {
        throw "baseline image map hash differs from provenance"
    }
    if ($provenance.candidate.image_map_sha256 -ne ("sha256:" + (Get-FileSha256 $candidateMapPath))) {
        throw "candidate image map hash differs from provenance"
    }
    Write-Host "LOCAL_EVIDENCE_IMAGE_MAPS_VALID"
    return
}

$baseline = Resolve-CleanCheckout -Path $BaselineRoot -Role "baseline"
$candidate = Resolve-CleanCheckout -Path $CandidateRoot -Role "candidate"
$baselineCommit = (Invoke-Captured git -C $baseline rev-parse HEAD).Trim()
$candidateCommit = (Invoke-Captured git -C $candidate rev-parse HEAD).Trim()
if ($baselineCommit -eq $candidateCommit) { throw "baseline and candidate commits must differ" }
if ($Mode -eq "Plan") {
    Write-Host "LOCAL_EVIDENCE_IMAGE_PLAN baseline=$baselineCommit candidate=$candidateCommit registry=$Registry"
    return
}

foreach ($command in @("docker", "git")) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) { throw "required command is unavailable: $command" }
}
Invoke-Captured docker info | Out-Null
$registryState = (& docker container inspect --format '{{.State.Running}}' $RegistryContainerName 2>$null | Out-String).Trim()
if ($LASTEXITCODE -ne 0) {
    $port = ($Registry -split ':')[-1]
    Invoke-Captured docker run -d --restart=no -p "$port`:5000" --name $RegistryContainerName registry:2.8.3 | Out-Null
}
elseif ($registryState -ne "true") {
    Invoke-Captured docker start $RegistryContainerName | Out-Null
}

New-Item -ItemType Directory -Force -Path $output | Out-Null
$roleData = [ordered]@{}
foreach ($role in @("baseline", "candidate")) {
    $checkout = if ($role -eq "baseline") { $baseline } else { $candidate }
    $commit = if ($role -eq "baseline") { $baselineCommit } else { $candidateCommit }
    $short = $commit.Substring(0, 12)
    $buildOutput = ".rocketmq/evidence-$role"
    & (Join-Path $checkout "scripts/build-production-images.ps1") `
        -Load `
        -OutputDirectory $buildOutput `
        -IdentityNonce "evidence-$role-$short" `
        -ClusterKind kind `
        -ClusterName rocketmq-message-path-evidence
    if ($LASTEXITCODE -ne 0) { throw "$role production image build failed" }
    $releaseStatePath = Join-Path $checkout "$buildOutput/release-state.json"
    $releaseState = Get-Content -Raw -LiteralPath $releaseStatePath | ConvertFrom-Json
    if ($releaseState.source_commit -ne $commit) { throw "$role release state commit mismatch" }
    $map = [ordered]@{}
    foreach ($service in $Services) {
        $localReference = [string]$releaseState.images.$service.reference
        $remoteReference = "$Registry/rocketmq-evidence/$role-$service`:$short"
        Invoke-Captured docker tag $localReference $remoteReference | Out-Null
        Invoke-Captured docker push $remoteReference | Out-Null
        Invoke-Captured docker pull $remoteReference | Out-Null
        $repoDigests = Invoke-Captured docker image inspect --format '{{json .RepoDigests}}' $remoteReference | ConvertFrom-Json
        $digestReference = @($repoDigests | Where-Object { $_ -like "$Registry/*@sha256:*" }) | Select-Object -First 1
        if ([string]$digestReference -notmatch '^[^@\s]+@sha256:[0-9a-f]{64}$') {
            throw "$role $service did not resolve to a registry manifest digest"
        }
        $revision = Invoke-Captured docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' $digestReference
        if ($revision -ne $commit) { throw "$role $service OCI revision differs from the checkout" }
        Invoke-Captured docker pull $digestReference | Out-Null
        $map[$service] = [string]$digestReference
    }
    $mapPath = if ($role -eq "baseline") { $baselineMapPath } else { $candidateMapPath }
    Write-Json -Value $map -Path $mapPath
    Test-ImageMap -Map $map -Role $role
    $manifest = (($Services | ForEach-Object { "$($_)=$($map[$_])" }) -join "`n") + "`n"
    $roleData[$role] = [ordered]@{
        commit = $commit
        release_state_sha256 = "sha256:" + (Get-FileSha256 $releaseStatePath)
        image_map_sha256 = "sha256:" + (Get-FileSha256 $mapPath)
        deployment_digest = "sha256:" + (Get-TextSha256 $manifest)
        images = $map
    }
}

$provenance = [ordered]@{
    schema_version = 1
    artifact_kind = "rocketmq_local_evidence_image_provenance"
    generated_at = [DateTimeOffset]::UtcNow.ToString("o")
    registry = $Registry
    registry_container = $RegistryContainerName
    baseline = $roleData.baseline
    candidate = $roleData.candidate
    remote_registry_push_performed = $false
}
Write-Json -Value $provenance -Path $provenancePath
Write-Host "LOCAL_EVIDENCE_IMAGES_READY output=$output"

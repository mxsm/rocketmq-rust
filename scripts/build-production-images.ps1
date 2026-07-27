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
    [switch]$Load,
    [string]$OutputDirectory = ".rocketmq/candidate",
    [string]$IdentityNonce = "",
    [string]$SecretName = "rocketmq-runtime-secrets",
    [string]$SecretVersion = "local-reference-1",
    [ValidateRange(1, [long]::MaxValue)]
    [long]$StorageGeneration = 1,
    [ValidateSet("none", "kind", "k3d")]
    [string]$ClusterKind = "none",
    [string]$ClusterName = "rocketmq"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Invoke-Checked {
    param(
        [Parameter(Mandatory)]
        [string]$Executable,
        [Parameter(ValueFromRemainingArguments)]
        [string[]]$Arguments
    )

    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Executable failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Captured {
    param(
        [Parameter(Mandatory)]
        [string]$Executable,
        [Parameter(ValueFromRemainingArguments)]
        [string[]]$Arguments
    )

    $output = (& $Executable @Arguments | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "$Executable failed with exit code $LASTEXITCODE"
    }
    return $output
}

function Get-Sha256 {
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
    finally {
        $algorithm.Dispose()
    }
}

function Write-Json {
    param(
        [Parameter(Mandatory)][object]$Value,
        [Parameter(Mandatory)][string]$Path
    )

    $json = $Value | ConvertTo-Json -Depth 16
    [System.IO.File]::WriteAllText(
        $Path,
        ($json.TrimEnd() + "`n"),
        [System.Text.UTF8Encoding]::new($false)
    )
}

function Get-RepositoryRelativePath {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$Path
    )

    $rootPath = [System.IO.Path]::GetFullPath($Root).TrimEnd("\", "/") +
        [System.IO.Path]::DirectorySeparatorChar
    $artifactPath = [System.IO.Path]::GetFullPath($Path)
    if (-not $artifactPath.StartsWith($rootPath, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "release artifact must remain under the repository root: $artifactPath"
    }

    $rootUri = [System.Uri]::new($rootPath)
    $artifactUri = [System.Uri]::new($artifactPath)
    return [System.Uri]::UnescapeDataString($rootUri.MakeRelativeUri($artifactUri).ToString())
}

if (-not $Load) {
    throw "production images are local-only; invoke this script with -Load"
}

foreach ($commandName in @("docker", "git")) {
    if (-not (Get-Command $commandName -ErrorAction SilentlyContinue)) {
        throw "required command is unavailable: $commandName"
    }
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dirty = Invoke-Captured git -C $root status --porcelain --untracked-files=normal
if (-not [string]::IsNullOrWhiteSpace($dirty)) {
    throw "production images must be built from a clean checkout"
}

Invoke-Captured docker info | Out-Null
Invoke-Captured docker buildx version | Out-Null

$profilePath = Join-Path $root "distribution/config/production-feature-profile.json"
$containerPolicyPath = Join-Path $root "docker/container-policy.json"
$dockerfilePath = Join-Path $root "docker/Dockerfile.base"
$cargoLockPath = Join-Path $root "Cargo.lock"
$profile = Get-Content -Raw -LiteralPath $profilePath | ConvertFrom-Json
$containerPolicy = Get-Content -Raw -LiteralPath $containerPolicyPath | ConvertFrom-Json

if ($profile.profile -ne "production" -or -not $profile.build_mode.local_images_only) {
    throw "production feature profile must require local-only images"
}
if ($profile.build_mode.remote_push_enabled) {
    throw "production feature profile unexpectedly permits a remote push"
}

$sourceCommit = (Invoke-Captured git -C $root rev-parse HEAD).Trim()
$shortCommit = (Invoke-Captured git -C $root rev-parse --short HEAD).Trim()
if ($sourceCommit -notmatch "^[0-9a-f]{40}$" -or $shortCommit -notmatch "^[0-9a-f]{7,12}$") {
    throw "git returned a non-canonical source revision"
}
if ([string]::IsNullOrWhiteSpace($IdentityNonce)) {
    $IdentityNonce = "local-$shortCommit"
}
if ($IdentityNonce -notmatch "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$") {
    throw "IdentityNonce must be a canonical 1..=63 character release nonce"
}
if ($SecretName -notmatch "^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$") {
    throw "SecretName must be a canonical Kubernetes object name"
}
if ($SecretVersion -notmatch "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$") {
    throw "SecretVersion must be an opaque 1..=128 character version identifier"
}
if ($ClusterName -notmatch "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$") {
    throw "ClusterName must be a canonical local cluster name"
}

$outputPath = if ([System.IO.Path]::IsPathRooted($OutputDirectory)) {
    [System.IO.Path]::GetFullPath($OutputDirectory)
}
else {
    [System.IO.Path]::GetFullPath((Join-Path $root $OutputDirectory))
}
$rootPrefix = $root.TrimEnd("\", "/") + [System.IO.Path]::DirectorySeparatorChar
if (-not $outputPath.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "OutputDirectory must remain inside the repository so artifact paths stay portable"
}
New-Item -ItemType Directory -Force -Path $outputPath | Out-Null

$configFiles = @($profile.config_bundle_files)
$configEntries = [System.Collections.Generic.List[object]]::new()
foreach ($relativePath in $configFiles) {
    if (
        [System.IO.Path]::IsPathRooted($relativePath) -or
        $relativePath.Replace("\", "/").Split("/") -contains ".."
    ) {
        throw "configuration bundle path must be repository-relative: $relativePath"
    }
    $absolutePath = Join-Path $root $relativePath
    if (-not (Test-Path -LiteralPath $absolutePath -PathType Leaf)) {
        throw "configuration bundle file is missing: $relativePath"
    }
    $configEntries.Add([ordered]@{
        path = $relativePath.Replace("\", "/")
        sha256 = Get-Sha256 $absolutePath
    })
}
$configManifest = (@($configEntries) | ForEach-Object { "$($_.sha256)  $($_.path)" }) -join "`n"
$configDigest = "sha256:" + (Get-TextSha256 ($configManifest + "`n"))
$cargoLockSha256 = Get-Sha256 $cargoLockPath
$featureProfileSha256 = Get-Sha256 $profilePath
$rustToolchain = [string]$containerPolicy.build.rust_toolchain
$releaseId = "$shortCommit-$IdentityNonce"
$generatedAt = [DateTimeOffset]::UtcNow.ToString("o")
$sourceVersion = "1.0.0-$shortCommit"
$imageEvidence = [System.Collections.Generic.List[object]]::new()

foreach ($serviceProperty in $profile.services.PSObject.Properties) {
    $serviceName = $serviceProperty.Name
    $service = $serviceProperty.Value
    $imageReference = "rocketmq-rust/$serviceName`:$shortCommit"

    Write-Host "==> Building local production image $imageReference"
    Invoke-Checked docker buildx build `
        --load `
        --file $dockerfilePath `
        --target $service.docker_target `
        --tag $imageReference `
        --build-arg "SOURCE_REVISION=$sourceCommit" `
        --build-arg "SOURCE_VERSION=$sourceVersion" `
        --build-arg "CARGO_LOCK_SHA256=$cargoLockSha256" `
        --build-arg "PRODUCTION_FEATURE_PROFILE_SHA256=$featureProfileSha256" `
        --build-arg "RELEASE_CONFIG_SHA256=$configDigest" `
        $root

    $inspect = Invoke-Captured docker image inspect --format "{{json .}}" $imageReference |
        ConvertFrom-Json
    $imageId = [string]$inspect.Id
    if ($imageId -notmatch "^sha256:[0-9a-f]{64}$") {
        throw "$serviceName image has a non-canonical image ID: $imageId"
    }
    $labels = $inspect.Config.Labels
    $expectedLabels = [ordered]@{
        "org.opencontainers.image.revision" = $sourceCommit
        "io.rocketmq.build.rust-toolchain" = $rustToolchain
        "io.rocketmq.build.cargo-lock-sha256" = $cargoLockSha256
        "io.rocketmq.build.production-feature-profile-sha256" = $featureProfileSha256
        "io.rocketmq.release.config-digest" = $configDigest
    }
    foreach ($label in $expectedLabels.GetEnumerator()) {
        $property = $labels.PSObject.Properties[$label.Key]
        if ($null -eq $property -or [string]$property.Value -ne $label.Value) {
            throw "$serviceName image label mismatch: $($label.Key)"
        }
    }

    $binaryPath = "/usr/local/bin/$($service.binary)"
    $binaryHashOutput = Invoke-Captured docker run --rm --entrypoint sha256sum $imageReference $binaryPath
    $binarySha256 = ($binaryHashOutput -split "\s+")[0].ToLowerInvariant()
    if ($binarySha256 -notmatch "^[0-9a-f]{64}$") {
        throw "$serviceName binary returned a non-canonical SHA-256 digest"
    }

    $packageOutput = Invoke-Captured -Executable docker -Arguments @(
        "run",
        "--rm",
        "--entrypoint",
        "dpkg-query",
        $imageReference,
        "-W",
        '-f=${Package}\t${Version}\t${Architecture}\n'
    )
    $components = [System.Collections.Generic.List[object]]::new()
    foreach ($line in ($packageOutput -split "`r?`n")) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }
        $fields = $line -split "`t", 3
        if ($fields.Count -ne 3) {
            throw "$serviceName returned an invalid dpkg-query row"
        }
        $components.Add([ordered]@{
            type = "library"
            name = $fields[0]
            version = $fields[1]
            purl = "pkg:deb/ubuntu/$($fields[0])@$([Uri]::EscapeDataString($fields[1]))?arch=$($fields[2])"
        })
    }
    $components = @($components | Sort-Object name, version)

    $sbom = [ordered]@{
        bomFormat = "CycloneDX"
        specVersion = "1.6"
        serialNumber = "urn:uuid:$([Guid]::NewGuid())"
        version = 1
        metadata = [ordered]@{
            timestamp = $generatedAt
            component = [ordered]@{
                type = "application"
                name = $service.package
                version = $sourceVersion
                hashes = @(
                    [ordered]@{
                        alg = "SHA-256"
                        content = $binarySha256
                    }
                )
            }
            properties = @(
                [ordered]@{ name = "rocketmq:source-commit"; value = $sourceCommit }
                [ordered]@{ name = "rocketmq:image-reference"; value = $imageReference }
                [ordered]@{ name = "rocketmq:config-digest"; value = $configDigest }
            )
        }
        components = $components
    }
    $sbomPath = Join-Path $outputPath "$serviceName.cdx.json"
    Write-Json -Value $sbom -Path $sbomPath

    $imageEvidence.Add([ordered]@{
        service = $serviceName
        reference = $imageReference
        image_id = $imageId
        image_config_digest = $imageId
        binary_sha256 = $binarySha256
        resolved_features = @($service.resolved_features)
        sbom_path = Get-RepositoryRelativePath -Root $root -Path $sbomPath
        sbom_sha256 = Get-Sha256 $sbomPath
    })
}

$provenance = [ordered]@{
    schema_version = 1
    release_id = $releaseId
    generated_at = $generatedAt
    source_commit = $sourceCommit
    rust_toolchain = $rustToolchain
    cargo_lock_sha256 = $cargoLockSha256
    feature_profile_sha256 = $featureProfileSha256
    config_digest = $configDigest
    local_only = $true
    remote_push_performed = $false
    images = @($imageEvidence)
}
$provenancePath = Join-Path $outputPath "provenance.json"
Write-Json -Value $provenance -Path $provenancePath
$provenanceSha256 = Get-Sha256 $provenancePath
$provenanceRelativePath = Get-RepositoryRelativePath -Root $root -Path $provenancePath

$images = [ordered]@{}
foreach ($evidence in $imageEvidence) {
    $images[$evidence.service] = [ordered]@{
        service = $evidence.service
        reference = $evidence.reference
        image_id = $evidence.image_id
        image_config_digest = $evidence.image_config_digest
        binary_sha256 = $evidence.binary_sha256
        source_commit = $sourceCommit
        rust_toolchain = $rustToolchain
        cargo_lock_sha256 = $cargoLockSha256
        feature_profile_sha256 = $featureProfileSha256
        resolved_features = @($evidence.resolved_features)
        config_digest = $configDigest
        sbom = [ordered]@{
            path = $evidence.sbom_path
            sha256 = $evidence.sbom_sha256
            format = "cyclonedx-1.6-json"
        }
        provenance = [ordered]@{
            path = $provenanceRelativePath
            sha256 = $provenanceSha256
            format = "rocketmq-local-provenance-v1"
        }
    }
}

$releaseState = [ordered]@{
    schema_version = 1
    release_id = $releaseId
    created_at = $generatedAt
    source_commit = $sourceCommit
    images = $images
    config_bundle = [ordered]@{
        digest = $configDigest
        files = @($configEntries)
        helm_values = "distribution/helm/rocketmq-rust/values-production-controller-ha.yaml"
        config_map_template = "distribution/helm/rocketmq-rust/templates/configmaps.yaml"
        schema = "distribution/config/release-state.schema.json"
    }
    secret_references = @(
        [ordered]@{
            name = $SecretName
            namespace = "rocketmq"
            provider = "kubernetes"
            version = $SecretVersion
            mount_path = "/var/run/secrets/rocketmq"
        }
    )
    identity = [ordered]@{
        commit = $sourceCommit
        nonce = $IdentityNonce
        config_digest = $configDigest
        secret_version = $SecretVersion
        storage_generation = $StorageGeneration
    }
    storage_generation = $StorageGeneration
    cluster_import = [ordered]@{
        kind = $ClusterKind
        name = $ClusterName
    }
}
$releaseStatePath = Join-Path $outputPath "release-state.json"
Write-Json -Value $releaseState -Path $releaseStatePath

Write-Host "PRODUCTION_IMAGES_OK services=$($imageEvidence.Count) release_state=$releaseStatePath"

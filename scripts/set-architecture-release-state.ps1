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

[CmdletBinding(DefaultParameterSetName = "Validate")]
param(
    [string]$StatePath = ".rocketmq/candidate/release-state.json",
    [Parameter(ParameterSetName = "Validate")]
    [switch]$ValidateOnly,
    [Parameter(ParameterSetName = "Validate")]
    [switch]$SchemaOnly,
    [Parameter(Mandatory, ParameterSetName = "Apply")]
    [switch]$Apply,
    [Parameter(ParameterSetName = "Apply")]
    [string]$ReleaseName = "rocketmq",
    [Parameter(ParameterSetName = "Apply")]
    [string]$Namespace = "rocketmq"
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

    $json = $Value | ConvertTo-Json -Depth 20
    [System.IO.File]::WriteAllText(
        $Path,
        ($json.TrimEnd() + "`n"),
        [System.Text.UTF8Encoding]::new($false)
    )
}

function Assert-ExactProperties {
    param(
        [Parameter(Mandatory)][object]$Value,
        [Parameter(Mandatory)][string[]]$Names,
        [Parameter(Mandatory)][string]$Context
    )

    if ($null -eq $Value) {
        throw "$Context must be an object"
    }
    $actual = @($Value.PSObject.Properties.Name | Sort-Object)
    $expected = @($Names | Sort-Object)
    if (($actual -join "`n") -ne ($expected -join "`n")) {
        throw "$Context properties must be exactly: $($expected -join ', ')"
    }
}

function Assert-Digest {
    param(
        [Parameter(Mandatory)][string]$Value,
        [Parameter(Mandatory)][string]$Context
    )

    if ($Value -notmatch "^sha256:[0-9a-f]{64}$") {
        throw "$Context must be a lowercase SHA-256 digest"
    }
}

function Assert-Sha256 {
    param(
        [Parameter(Mandatory)][string]$Value,
        [Parameter(Mandatory)][string]$Context
    )

    if ($Value -notmatch "^[0-9a-f]{64}$") {
        throw "$Context must be a lowercase SHA-256 hash"
    }
}

function Resolve-RepositoryPath {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$RelativePath,
        [Parameter(Mandatory)][string]$Context
    )

    $normalized = $RelativePath.Replace("\", "/")
    if (
        [System.IO.Path]::IsPathRooted($RelativePath) -or
        $normalized.Split("/") -contains ".." -or
        $normalized -notmatch "^[A-Za-z0-9._/-]+$"
    ) {
        throw "$Context must be a safe repository-relative path"
    }
    $resolved = [System.IO.Path]::GetFullPath((Join-Path $Root $normalized))
    $prefix = $Root.TrimEnd("\", "/") + [System.IO.Path]::DirectorySeparatorChar
    if (-not $resolved.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "$Context escapes the repository"
    }
    return $resolved
}

function Assert-Artifact {
    param(
        [Parameter(Mandatory)][object]$Artifact,
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$Context,
        [switch]$RequireFormat
    )

    $properties = if ($RequireFormat) { @("path", "sha256", "format") } else { @("path", "sha256") }
    Assert-ExactProperties -Value $Artifact -Names $properties -Context $Context
    Assert-Sha256 -Value ([string]$Artifact.sha256) -Context "$Context.sha256"
    $artifactPath = Resolve-RepositoryPath -Root $Root -RelativePath ([string]$Artifact.path) -Context "$Context.path"
    if (-not (Test-Path -LiteralPath $artifactPath -PathType Leaf)) {
        throw "$Context is missing: $($Artifact.path)"
    }
    $actualHash = Get-Sha256 $artifactPath
    if ($actualHash -ne $Artifact.sha256) {
        throw "$Context hash mismatch"
    }
    return $artifactPath
}

function Get-OptionalHelmStatus {
    param(
        [Parameter(Mandatory)][string]$Helm,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$TargetNamespace
    )

    $output = & $Helm status $Name --namespace $TargetNamespace --output json 2>$null | Out-String
    if ($LASTEXITCODE -ne 0) {
        return $null
    }
    return $output.Trim() | ConvertFrom-Json
}

function New-ClusterSnapshot {
    param(
        [Parameter(Mandatory)][string]$Helm,
        [Parameter(Mandatory)][string]$Kubectl,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$TargetNamespace,
        [Parameter(Mandatory)][string]$Phase,
        [Parameter(Mandatory)][object]$ReleaseState
    )

    $helmStatus = Get-OptionalHelmStatus -Helm $Helm -Name $Name -TargetNamespace $TargetNamespace
    $workloadOutput = & $Kubectl get statefulsets,deployments,pods `
        --namespace $TargetNamespace `
        --selector app.kubernetes.io/instance=$Name `
        --output json 2>$null | Out-String
    $workloads = if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($workloadOutput)) {
        $workloadOutput.Trim() | ConvertFrom-Json
    }
    else {
        [ordered]@{
            apiVersion = "v1"
            kind = "List"
            items = @()
        }
    }
    return [ordered]@{
        schema_version = 1
        captured_at = [DateTimeOffset]::UtcNow.ToString("o")
        phase = $Phase
        release_id = $ReleaseState.release_id
        helm_status = $helmStatus
        workloads = $workloads
    }
}

if ($SchemaOnly -and -not $ValidateOnly) {
    throw "-SchemaOnly is valid only with -ValidateOnly"
}
if (-not $ValidateOnly -and -not $Apply) {
    throw "specify -ValidateOnly or the explicit mutating switch -Apply"
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$resolvedStatePath = if ([System.IO.Path]::IsPathRooted($StatePath)) {
    [System.IO.Path]::GetFullPath($StatePath)
}
else {
    [System.IO.Path]::GetFullPath((Join-Path $root $StatePath))
}
if (-not (Test-Path -LiteralPath $resolvedStatePath -PathType Leaf)) {
    throw "ReleaseState is missing: $resolvedStatePath"
}

$rawState = Get-Content -Raw -LiteralPath $resolvedStatePath
if ($rawState -match "(?i)BEGIN [A-Z ]*PRIVATE KEY") {
    throw "ReleaseState contains private key material"
}
if ($rawState -match '(?i)"(?:password|token|secret_value|client_secret|private_key)"\s*:') {
    throw "ReleaseState contains a forbidden Secret field"
}
$state = $rawState | ConvertFrom-Json
$profilePath = Join-Path $root "distribution/config/production-feature-profile.json"
$policyPath = Join-Path $root "distribution/kubernetes/release-state-transition-policy.json"
$profile = Get-Content -Raw -LiteralPath $profilePath | ConvertFrom-Json
$policy = Get-Content -Raw -LiteralPath $policyPath | ConvertFrom-Json

Assert-ExactProperties -Value $state -Names @(
    "schema_version",
    "release_id",
    "created_at",
    "source_commit",
    "images",
    "config_bundle",
    "secret_references",
    "identity",
    "storage_generation",
    "cluster_import"
) -Context "ReleaseState"
if ($state.schema_version -ne 1) {
    throw "ReleaseState.schema_version must be 1"
}
if ($state.release_id -notmatch "^[a-z0-9][a-z0-9._-]{0,127}$") {
    throw "ReleaseState.release_id is not canonical"
}
$createdAt = [DateTimeOffset]::MinValue
if (-not [DateTimeOffset]::TryParse([string]$state.created_at, [ref]$createdAt)) {
    throw "ReleaseState.created_at must be an RFC 3339 timestamp"
}
if ($state.source_commit -notmatch "^[0-9a-f]{40}$" -or $state.source_commit -eq ("0" * 40)) {
    throw "ReleaseState.source_commit must be a non-zero lowercase commit"
}
if ([long]$state.storage_generation -lt 1) {
    throw "ReleaseState.storage_generation must be at least 1"
}

Assert-ExactProperties -Value $state.identity -Names @(
    "commit",
    "nonce",
    "config_digest",
    "secret_version",
    "storage_generation"
) -Context "ReleaseState.identity"
if ($state.identity.commit -ne $state.source_commit) {
    throw "ReleaseState identity commit must match source_commit"
}
if ($state.identity.nonce -notmatch "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$") {
    throw "ReleaseState identity nonce is not canonical"
}
Assert-Digest -Value ([string]$state.identity.config_digest) -Context "ReleaseState.identity.config_digest"
if ($state.identity.secret_version -notmatch "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$") {
    throw "ReleaseState identity Secret version is not canonical"
}
if ([long]$state.identity.storage_generation -ne [long]$state.storage_generation) {
    throw "ReleaseState identity storage generation must match storage_generation"
}

Assert-ExactProperties -Value $state.cluster_import -Names @("kind", "name") -Context "ReleaseState.cluster_import"
if ($state.cluster_import.kind -notin @("none", "kind", "k3d")) {
    throw "ReleaseState cluster import kind must be none, kind, or k3d"
}
if ($state.cluster_import.name -notmatch "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$") {
    throw "ReleaseState cluster import name is not canonical"
}

$secretReferences = @($state.secret_references)
if ($secretReferences.Count -ne 1) {
    throw "ReleaseState must contain exactly one chart Secret reference"
}
$secretReference = $secretReferences[0]
Assert-ExactProperties -Value $secretReference -Names @(
    "name",
    "namespace",
    "provider",
    "version",
    "mount_path"
) -Context "ReleaseState.secret_references[0]"
if ($secretReference.name -notmatch "^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$") {
    throw "Secret reference name is not canonical"
}
if ($secretReference.namespace -notmatch "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$") {
    throw "Secret reference namespace is not canonical"
}
if ($secretReference.provider -notin @("kubernetes", "external-secrets", "secrets-store-csi")) {
    throw "Secret reference provider is unsupported"
}
if ($secretReference.version -ne $state.identity.secret_version) {
    throw "Secret reference version must match the release identity"
}
if ($secretReference.mount_path -ne "/var/run/secrets/rocketmq") {
    throw "Secret reference mount_path must use the hardened runtime location"
}

Assert-ExactProperties -Value $state.config_bundle -Names @(
    "digest",
    "files",
    "helm_values",
    "config_map_template",
    "schema"
) -Context "ReleaseState.config_bundle"
Assert-Digest -Value ([string]$state.config_bundle.digest) -Context "ReleaseState.config_bundle.digest"
if ($state.config_bundle.digest -ne $state.identity.config_digest) {
    throw "configuration bundle digest must match release identity"
}
$expectedConfigPaths = @($profile.config_bundle_files)
$configFiles = @($state.config_bundle.files)
$actualConfigPaths = @($configFiles | ForEach-Object { [string]$_.path })
if (($actualConfigPaths -join "`n") -ne ($expectedConfigPaths -join "`n")) {
    throw "ReleaseState configuration bundle must contain the complete production file set"
}
$configEntries = [System.Collections.Generic.List[object]]::new()
foreach ($entry in $configFiles) {
    Assert-ExactProperties -Value $entry -Names @("path", "sha256") -Context "ReleaseState.config_bundle.files"
    Assert-Sha256 -Value ([string]$entry.sha256) -Context "configuration file hash"
    $configPath = Resolve-RepositoryPath -Root $root -RelativePath ([string]$entry.path) -Context "configuration file path"
    if (-not (Test-Path -LiteralPath $configPath -PathType Leaf)) {
        throw "configuration bundle file is missing: $($entry.path)"
    }
    $actualHash = Get-Sha256 $configPath
    if ($actualHash -ne $entry.sha256) {
        throw "configuration bundle file hash mismatch: $($entry.path)"
    }
    $configEntries.Add([ordered]@{
        path = [string]$entry.path
        sha256 = $actualHash
    })
}
$configManifest = (@($configEntries) | ForEach-Object { "$($_.sha256)  $($_.path)" }) -join "`n"
$actualConfigDigest = "sha256:" + (Get-TextSha256 ($configManifest + "`n"))
if ($actualConfigDigest -ne $state.config_bundle.digest) {
    throw "configuration bundle aggregate digest mismatch"
}
foreach ($pathField in @("helm_values", "config_map_template", "schema")) {
    $path = Resolve-RepositoryPath `
        -Root $root `
        -RelativePath ([string]$state.config_bundle.$pathField) `
        -Context "ReleaseState.config_bundle.$pathField"
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "ReleaseState.config_bundle.$pathField is missing"
    }
}

$services = @("broker", "namesrv", "controller", "proxy", "mcp")
Assert-ExactProperties -Value $state.images -Names $services -Context "ReleaseState.images"
$featureProfileSha256 = Get-Sha256 $profilePath
$provenancePaths = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::Ordinal
)
foreach ($serviceName in $services) {
    $image = $state.images.$serviceName
    Assert-ExactProperties -Value $image -Names @(
        "service",
        "reference",
        "image_id",
        "image_config_digest",
        "binary_sha256",
        "source_commit",
        "rust_toolchain",
        "cargo_lock_sha256",
        "feature_profile_sha256",
        "resolved_features",
        "config_digest",
        "sbom",
        "provenance"
    ) -Context "ReleaseState.images.$serviceName"
    if ($image.service -ne $serviceName) {
        throw "$serviceName image owner mismatch"
    }
    if ($image.reference -notmatch "^rocketmq-rust/$serviceName`:[0-9a-f]{7,12}$") {
        throw "$serviceName image reference must be a local commit tag"
    }
    Assert-Digest -Value ([string]$image.image_id) -Context "$serviceName image_id"
    Assert-Digest -Value ([string]$image.image_config_digest) -Context "$serviceName image_config_digest"
    Assert-Sha256 -Value ([string]$image.binary_sha256) -Context "$serviceName binary_sha256"
    Assert-Sha256 -Value ([string]$image.cargo_lock_sha256) -Context "$serviceName cargo_lock_sha256"
    Assert-Sha256 -Value ([string]$image.feature_profile_sha256) -Context "$serviceName feature_profile_sha256"
    Assert-Digest -Value ([string]$image.config_digest) -Context "$serviceName config_digest"
    if (
        $image.source_commit -ne $state.source_commit -or
        $image.config_digest -ne $state.config_bundle.digest -or
        $image.feature_profile_sha256 -ne $featureProfileSha256
    ) {
        throw "$serviceName image metadata is not bound to the complete ReleaseState"
    }
    $expectedFeatures = @($profile.services.$serviceName.resolved_features | Sort-Object)
    $actualFeatures = @($image.resolved_features | Sort-Object)
    if (($actualFeatures -join "`n") -ne ($expectedFeatures -join "`n")) {
        throw "$serviceName resolved production features drifted"
    }
    $sbomPath = Assert-Artifact `
        -Artifact $image.sbom `
        -Root $root `
        -Context "$serviceName SBOM" `
        -RequireFormat
    $sbom = Get-Content -Raw -LiteralPath $sbomPath | ConvertFrom-Json
    if ($sbom.bomFormat -ne "CycloneDX" -or $sbom.specVersion -ne "1.6") {
        throw "$serviceName SBOM must be CycloneDX 1.6 JSON"
    }
    $provenancePath = Assert-Artifact `
        -Artifact $image.provenance `
        -Root $root `
        -Context "$serviceName provenance" `
        -RequireFormat
    [void]$provenancePaths.Add($provenancePath)

    if (-not $SchemaOnly) {
        if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
            throw "docker is required for full ReleaseState validation"
        }
        $actualImageId = (Invoke-Captured docker image inspect --format "{{.Id}}" $image.reference).Trim()
        if ($actualImageId -ne $image.image_id -or $actualImageId -ne $image.image_config_digest) {
            throw "$serviceName local image identity drifted"
        }
        $labels = Invoke-Captured docker image inspect --format "{{json .Config.Labels}}" $image.reference |
            ConvertFrom-Json
        $expectedLabels = [ordered]@{
            "org.opencontainers.image.revision" = $state.source_commit
            "io.rocketmq.build.rust-toolchain" = $image.rust_toolchain
            "io.rocketmq.build.cargo-lock-sha256" = $image.cargo_lock_sha256
            "io.rocketmq.build.production-feature-profile-sha256" = $featureProfileSha256
            "io.rocketmq.release.config-digest" = $state.config_bundle.digest
        }
        foreach ($label in $expectedLabels.GetEnumerator()) {
            $property = $labels.PSObject.Properties[$label.Key]
            if ($null -eq $property -or [string]$property.Value -ne $label.Value) {
                throw "$serviceName image label mismatch: $($label.Key)"
            }
        }
        $binary = [string]$profile.services.$serviceName.binary
        $hashOutput = Invoke-Captured docker run --rm --entrypoint sha256sum `
            $image.reference "/usr/local/bin/$binary"
        if (($hashOutput -split "\s+")[0].ToLowerInvariant() -ne $image.binary_sha256) {
            throw "$serviceName binary digest drifted"
        }
    }
}
if ($provenancePaths.Count -ne 1) {
    throw "all five images must share one complete provenance statement"
}
$provenancePath = @($provenancePaths)[0]
$provenance = Get-Content -Raw -LiteralPath $provenancePath | ConvertFrom-Json
if (
    $provenance.schema_version -ne 1 -or
    $provenance.source_commit -ne $state.source_commit -or
    $provenance.config_digest -ne $state.config_bundle.digest -or
    -not $provenance.local_only -or
    $provenance.remote_push_performed -or
    @($provenance.images).Count -ne 5
) {
    throw "combined image provenance is incomplete or permits remote publication"
}

$policySteps = @($policy.apply_order | ForEach-Object { [string]$_.step })
$expectedSteps = @(
    "validate_complete_state",
    "snapshot_before",
    "import_local_images",
    "apply_complete_state",
    "wait_readiness",
    "snapshot_after",
    "publish_active_state"
)
if (($policySteps -join "`n") -ne ($expectedSteps -join "`n")) {
    throw "release-state transition apply order drifted"
}
$expectedCompensation = @($expectedSteps)
[array]::Reverse($expectedCompensation)
if ((@($policy.compensation_order) -join "`n") -ne ($expectedCompensation -join "`n")) {
    throw "release-state transition compensation order must reverse apply order"
}
if (-not $policy.local_images_only -or $policy.remote_push_enabled -or -not $policy.forbid_image_only_transition) {
    throw "release-state transition policy must remain local-only and reject image-only transitions"
}

if ($ValidateOnly) {
    $mode = if ($SchemaOnly) { "schema" } else { "complete" }
    Write-Host "RELEASE_STATE_VALIDATION_OK mode=$mode release_id=$($state.release_id)"
    exit 0
}

if ($state.cluster_import.kind -notin @("kind", "k3d")) {
    throw "applying a ReleaseState requires cluster_import.kind kind or k3d"
}
if ($secretReference.provider -ne "kubernetes") {
    throw "the Helm reconciler currently requires a Kubernetes Secret reference"
}
if ($secretReference.namespace -ne $Namespace) {
    throw "the Secret reference namespace must match the Helm release namespace"
}
foreach ($commandName in @("helm", "kubectl", $state.cluster_import.kind)) {
    if (-not (Get-Command $commandName -ErrorAction SilentlyContinue)) {
        throw "required reconcile command is unavailable: $commandName"
    }
}

$helm = (Get-Command helm).Source
$kubectl = (Get-Command kubectl).Source
$chartPath = Join-Path $root "distribution/helm/rocketmq-rust"
$valuesPath = Resolve-RepositoryPath `
    -Root $root `
    -RelativePath ([string]$state.config_bundle.helm_values) `
    -Context "ReleaseState.config_bundle.helm_values"
$snapshotRoot = Join-Path $root ".rocketmq/snapshots/$($state.release_id)"
$activeRoot = Join-Path $root ".rocketmq/active"
New-Item -ItemType Directory -Force -Path $snapshotRoot, $activeRoot | Out-Null
$activeStatePath = Join-Path $activeRoot "release-state.json"
$previousActiveStatePath = Join-Path $snapshotRoot "previous-active-release-state.json"
if (Test-Path -LiteralPath $activeStatePath) {
    Copy-Item -LiteralPath $activeStatePath -Destination $previousActiveStatePath -Force
}

$completedSteps = [System.Collections.Generic.List[string]]::new()
$previousHelmStatus = Get-OptionalHelmStatus `
    -Helm $helm `
    -Name $ReleaseName `
    -TargetNamespace $Namespace
$previousRevision = if ($null -eq $previousHelmStatus) {
    0
}
else {
    [int]$previousHelmStatus.version
}
$helmRestored = $false

try {
    $completedSteps.Add("validate_complete_state")

    $before = New-ClusterSnapshot `
        -Helm $helm `
        -Kubectl $kubectl `
        -Name $ReleaseName `
        -TargetNamespace $Namespace `
        -Phase "before" `
        -ReleaseState $state
    Write-Json -Value $before -Path (Join-Path $snapshotRoot "before.json")
    $completedSteps.Add("snapshot_before")

    $imageReferences = @($services | ForEach-Object { [string]$state.images.$_.reference })
    if ($state.cluster_import.kind -eq "kind") {
        Invoke-Checked kind load docker-image @imageReferences --name $state.cluster_import.name
    }
    else {
        Invoke-Checked k3d image import @imageReferences --cluster $state.cluster_import.name
    }
    $completedSteps.Add("import_local_images")

    $helmArguments = [System.Collections.Generic.List[string]]::new()
    foreach ($argument in @(
        "upgrade",
        "--install",
        $ReleaseName,
        $chartPath,
        "--namespace",
        $Namespace,
        "--create-namespace",
        "--values",
        $valuesPath,
        "--set-string",
        "releaseIdentity.commit=$($state.identity.commit)",
        "--set-string",
        "releaseIdentity.nonce=$($state.identity.nonce)",
        "--set-string",
        "releaseIdentity.configDigest=$($state.identity.config_digest)",
        "--set-string",
        "releaseIdentity.secretVersion=$($state.identity.secret_version)",
        "--set",
        "releaseIdentity.storageGeneration=$($state.storage_generation)",
        "--set-string",
        "global.secretRefs.existingSecret=$($secretReference.name)"
    )) {
        $helmArguments.Add([string]$argument)
    }
    foreach ($serviceName in $services) {
        $reference = [string]$state.images.$serviceName.reference
        $separator = $reference.LastIndexOf(":")
        $repository = $reference.Substring(0, $separator)
        $tag = $reference.Substring($separator + 1)
        foreach ($argument in @(
            "--set-string",
            "services.$serviceName.image.repository=$repository",
            "--set-string",
            "services.$serviceName.image.tag=$tag",
            "--set-string",
            "services.$serviceName.image.digest=",
            "--set-string",
            "services.$serviceName.image.pullPolicy=Never"
        )) {
            $helmArguments.Add([string]$argument)
        }
    }
    Invoke-Checked $helm @helmArguments
    $completedSteps.Add("apply_complete_state")

    foreach ($workload in @($policy.readiness.workloads)) {
        Invoke-Checked $kubectl rollout status `
            $workload `
            --namespace $Namespace `
            "--timeout=$($policy.readiness.timeout)"
    }
    $completedSteps.Add("wait_readiness")

    $after = New-ClusterSnapshot `
        -Helm $helm `
        -Kubectl $kubectl `
        -Name $ReleaseName `
        -TargetNamespace $Namespace `
        -Phase "after" `
        -ReleaseState $state
    Write-Json -Value $after -Path (Join-Path $snapshotRoot "after.json")
    $completedSteps.Add("snapshot_after")

    $candidateActiveStatePath = "$activeStatePath.candidate"
    Copy-Item -LiteralPath $resolvedStatePath -Destination $candidateActiveStatePath -Force
    Move-Item -LiteralPath $candidateActiveStatePath -Destination $activeStatePath -Force
    $completedSteps.Add("publish_active_state")
}
catch {
    $failure = $_
    $reverseSteps = @($completedSteps)
    [array]::Reverse($reverseSteps)
    foreach ($step in $reverseSteps) {
        switch ($step) {
            "publish_active_state" {
                if (Test-Path -LiteralPath $previousActiveStatePath) {
                    Copy-Item -LiteralPath $previousActiveStatePath -Destination $activeStatePath -Force
                }
            }
            { $_ -in @("wait_readiness", "apply_complete_state") } {
                if (-not $helmRestored) {
                    if ($previousRevision -gt 0) {
                        Invoke-Checked $helm rollback `
                            $ReleaseName `
                            $previousRevision `
                            --namespace $Namespace `
                            "--timeout=$($policy.readiness.timeout)" `
                            --wait
                    }
                    else {
                        Invoke-Checked $helm uninstall $ReleaseName --namespace $Namespace
                    }
                    $helmRestored = $true
                }
            }
            "import_local_images" {
                Write-Warning "retaining imported local images during compensation"
            }
            "snapshot_after" {
                Write-Warning "retaining after snapshot for failure diagnosis"
            }
            "snapshot_before" {
                Write-Warning "retaining before snapshot for failure diagnosis"
            }
        }
    }
    throw $failure
}

Write-Host "RELEASE_STATE_APPLY_OK release_id=$($state.release_id) snapshot_dir=$snapshotRoot"

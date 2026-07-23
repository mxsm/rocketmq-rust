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
    [string]$ImageRef = "rocketmq-rust/runtime-base:verification",
    [string]$Dockerfile = "docker/Dockerfile.base",
    [string]$OutputDirectory = "target/container-foundation",
    [string]$PublishedImageDigest = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        # PowerShell abbreviates named parameters, so `Command` would consume
        # native `-c` arguments intended for tools such as /bin/sh.
        [string]$Executable,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )

    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Executable failed with exit code $LASTEXITCODE"
    }
}

foreach ($command in @("docker", "syft", "trivy", "cosign", "git")) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "required command is unavailable: $command"
    }
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dockerfilePath = (Resolve-Path (Join-Path $root $Dockerfile)).Path
$outputPath = Join-Path $root $OutputDirectory
New-Item -ItemType Directory -Force -Path $outputPath | Out-Null

$policy = Get-Content -Raw -LiteralPath (Join-Path $root "docker/container-policy.json") | ConvertFrom-Json
$tmpfsOptions = "$($policy.runtime.tmpfs_path):rw,noexec,nosuid,size=16m,uid=$($policy.runtime.uid),gid=$($policy.runtime.gid),mode=0700"
$sourceCommit = (& git -C $root rev-parse HEAD).Trim()
$sourceEpoch = (& git -C $root show -s --format=%ct HEAD).Trim()
if ($LASTEXITCODE -ne 0) {
    throw "failed to resolve source revision"
}
$sourceVersion = "1.0.0-$($sourceCommit.Substring(0, 12))"
$env:SOURCE_DATE_EPOCH = $sourceEpoch

Invoke-Checked docker buildx build --load --file $dockerfilePath --target $policy.build.smoke_target --tag $ImageRef --build-arg "SOURCE_REVISION=$sourceCommit" --build-arg "SOURCE_VERSION=$sourceVersion" $root

$configuredUser = (& docker image inspect --format "{{.Config.User}}" $ImageRef).Trim()
if ($LASTEXITCODE -ne 0 -or $configuredUser -ne "$($policy.runtime.uid):$($policy.runtime.gid)") {
    throw "runtime image user mismatch: $configuredUser"
}
$labels = (& docker image inspect --format "{{json .Config.Labels}}" $ImageRef) | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) {
    throw "failed to inspect runtime image labels"
}
foreach ($property in $policy.runtime.required_labels.PSObject.Properties) {
    $actual = $labels.PSObject.Properties[$property.Name].Value
    if ($actual -ne $property.Value) {
        throw "runtime image label mismatch: $($property.Name)"
    }
}

$smoke = @(
    "set -eu",
    ('test "$(id -u)" = {0}' -f $policy.runtime.uid),
    ('test "$(id -g)" = {0}' -f $policy.runtime.gid),
    "! touch /opt/rocketmq/rootfs-must-stay-read-only 2>/dev/null",
    "touch $($policy.runtime.writable_data_path)/data-write",
    "touch $($policy.runtime.tmpfs_path)/tmp-write"
) -join "; "
Invoke-Checked docker run --rm --network none --read-only --mount "type=volume,destination=$($policy.runtime.writable_data_path)" --tmpfs $tmpfsOptions $ImageRef /bin/sh -c $smoke

$sbomPath = Join-Path $outputPath "runtime-base.cdx.json"
$trivyPath = Join-Path $outputPath "runtime-base.trivy.json"
$bundlePath = Join-Path $outputPath "runtime-base.sbom.sigstore.json"
$keyPrefix = Join-Path $outputPath "ephemeral-container-foundation"
$privateKey = "$keyPrefix.key"
$publicKey = "$keyPrefix.pub"

Invoke-Checked syft scan "docker:$ImageRef" --scope all-layers --output "cyclonedx-json=$sbomPath"
Invoke-Checked trivy image --scanners vuln --severity CRITICAL --exit-code 1 --format json --output $trivyPath $ImageRef
$sbom = Get-Content -Raw -LiteralPath $sbomPath | ConvertFrom-Json
if ($sbom.bomFormat -ne "CycloneDX") {
    throw "Syft output is not a CycloneDX SBOM"
}
$trivyReport = Get-Content -Raw -LiteralPath $trivyPath | ConvertFrom-Json
$criticalFindings = @(
    foreach ($result in @($trivyReport.Results)) {
        $vulnerabilities = $result.PSObject.Properties["Vulnerabilities"]
        if ($null -ne $vulnerabilities) {
            foreach ($vulnerability in @($vulnerabilities.Value)) {
                if ($vulnerability.Severity -eq "CRITICAL") {
                    $vulnerability
                }
            }
        }
    }
).Count
if ($criticalFindings -gt $policy.supply_chain.critical_vulnerability_policy.maximum_findings) {
    throw "critical vulnerability policy failed: $criticalFindings findings"
}

$oldPassword = $env:COSIGN_PASSWORD
$randomBytes = New-Object byte[] 32
$random = [System.Security.Cryptography.RandomNumberGenerator]::Create()
try {
    $random.GetBytes($randomBytes)
    $env:COSIGN_PASSWORD = [Convert]::ToBase64String($randomBytes)
    Invoke-Checked cosign generate-key-pair --output-key-prefix $keyPrefix
    Invoke-Checked cosign sign-blob --yes --key $privateKey --bundle $bundlePath $sbomPath
    Invoke-Checked cosign verify-blob --key $publicKey --bundle $bundlePath $sbomPath
}
finally {
    $random.Dispose()
    $env:COSIGN_PASSWORD = $oldPassword
    if (Test-Path -LiteralPath $privateKey) {
        Remove-Item -LiteralPath $privateKey -Force
    }
}
if (Test-Path -LiteralPath $privateKey) {
    throw "ephemeral signing private key was not deleted"
}

$publishedImageVerified = $false
if ($PublishedImageDigest) {
    if ($PublishedImageDigest -notmatch $policy.supply_chain.signature.published_digest_pattern) {
        throw "published image must be an approved GHCR service digest"
    }
    Invoke-Checked cosign sign --yes $PublishedImageDigest
    Invoke-Checked cosign verify --certificate-identity-regexp $policy.supply_chain.signature.certificate_identity_regexp --certificate-oidc-issuer $policy.supply_chain.signature.certificate_oidc_issuer $PublishedImageDigest
    $publishedImageVerified = $true
}

$imageId = (& docker image inspect --format "{{.Id}}" $ImageRef).Trim()
$evidence = [ordered]@{
    schema_version = 1
    source_commit = $sourceCommit
    source_date_epoch = [long]$sourceEpoch
    image_ref = $ImageRef
    image_id = $imageId
    configured_user = $configuredUser
    builder_image = $policy.base_images.builder.reference
    runtime_image = $policy.base_images.runtime.reference
    rust_toolchain = $policy.build.rust_toolchain
    debian_snapshot = $policy.build.debian_snapshot
    sbom_format = $sbom.bomFormat
    critical_vulnerability_findings = $criticalFindings
    sbom_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $sbomPath).Hash.ToLowerInvariant()
    trivy_report_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $trivyPath).Hash.ToLowerInvariant()
    signature_bundle_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $bundlePath).Hash.ToLowerInvariant()
    private_key_retained = $false
    published_image_digest = $PublishedImageDigest
    published_image_verified = $publishedImageVerified
}
$evidence | ConvertTo-Json -Depth 8 | Set-Content -Encoding utf8 -LiteralPath (Join-Path $outputPath "provenance.json")
Write-Host "CONTAINER_SUPPLY_CHAIN_OK image=$ImageRef output=$OutputDirectory"

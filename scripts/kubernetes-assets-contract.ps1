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
    [string]$ToolsDirectory = "target/kubernetes-tools",
    [string]$ArtifactsDirectory = "target/kubernetes-assets",
    [switch]$UpdateGeneratedBase
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$policyPath = Join-Path $repoRoot "distribution/kubernetes/deployment-policy.json"
$policy = Get-Content -Raw -LiteralPath $policyPath | ConvertFrom-Json
$toolsRoot = Join-Path $repoRoot $ToolsDirectory
$artifactsRoot = Join-Path $repoRoot $ArtifactsDirectory
$cacheRoot = Join-Path $toolsRoot "archives"
New-Item -ItemType Directory -Force -Path $toolsRoot, $artifactsRoot, $cacheRoot | Out-Null

$isWindows = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
    [System.Runtime.InteropServices.OSPlatform]::Windows
)
if (-not [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.Equals(
        [System.Runtime.InteropServices.Architecture]::X64
    )) {
    throw "M11-09 validation supports only amd64 hosts"
}
$platform = if ($isWindows) { "windows_amd64" } else { "linux_amd64" }
$archiveSuffix = if ($isWindows) { ".zip" } else { ".tar.gz" }
$executableSuffix = if ($isWindows) { ".exe" } else { "" }

function Get-ToolSpec {
    param([Parameter(Mandatory)][string]$Name)

    $entry = $policy.tools.$Name
    $version = [string]$entry.version
    $plainVersion = $version.TrimStart("v")
    $shaProperty = "${platform}_sha256"
    $sha256 = [string]$entry.$shaProperty
    switch ($Name) {
        "helm" {
            $file = "helm-$version-" + ($platform -replace "_", "-") + $archiveSuffix
            $url = "https://get.helm.sh/$file"
        }
        "kustomize" {
            $file = "kustomize_${version}_" + ($platform -replace "_", "_") + $archiveSuffix
            $url = "https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2F$version/$file"
        }
        "kubeconform" {
            $file = "kubeconform-" + ($platform -replace "_", "-") + $archiveSuffix
            $url = "https://github.com/yannh/kubeconform/releases/download/$version/$file"
        }
        default { throw "unsupported validation tool: $Name" }
    }
    [pscustomobject]@{
        Name = $Name
        Version = $version
        PlainVersion = $plainVersion
        Sha256 = $sha256
        File = $file
        Url = $url
    }
}

function Install-PinnedTool {
    param([Parameter(Mandatory)][pscustomobject]$Spec)

    $destination = Join-Path $toolsRoot ($Spec.Name + $executableSuffix)
    if (Test-Path -LiteralPath $destination) {
        return $destination
    }

    $archive = Join-Path $cacheRoot $Spec.File
    if (-not (Test-Path -LiteralPath $archive)) {
        Write-Host "Downloading $($Spec.Name) $($Spec.Version) from $($Spec.Url)"
        Invoke-WebRequest -UseBasicParsing -Uri $Spec.Url -OutFile $archive
    }
    $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $archive).Hash.ToLowerInvariant()
    if ($actualHash -ne $Spec.Sha256) {
        Remove-Item -LiteralPath $archive -Force
        throw "$($Spec.Name) archive hash mismatch: expected $($Spec.Sha256), got $actualHash"
    }

    $extractRoot = Join-Path $toolsRoot ("extract-" + $Spec.Name)
    if (Test-Path -LiteralPath $extractRoot) {
        Remove-Item -LiteralPath $extractRoot -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $extractRoot | Out-Null
    if ($isWindows) {
        Expand-Archive -LiteralPath $archive -DestinationPath $extractRoot -Force
    } else {
        & tar -xzf $archive -C $extractRoot
        if ($LASTEXITCODE -ne 0) {
            throw "failed to extract $($Spec.Name)"
        }
    }
    $candidate = Get-ChildItem -LiteralPath $extractRoot -Recurse -File |
        Where-Object { $_.Name -eq ($Spec.Name + $executableSuffix) } |
        Select-Object -First 1
    if ($null -eq $candidate) {
        throw "$($Spec.Name) executable was not present in the verified archive"
    }
    Copy-Item -LiteralPath $candidate.FullName -Destination $destination -Force
    if (-not $isWindows) {
        & chmod 0755 $destination
        if ($LASTEXITCODE -ne 0) {
            throw "failed to mark $($Spec.Name) executable"
        }
    }
    Remove-Item -LiteralPath $extractRoot -Recurse -Force
    return $destination
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory)][string]$Description,
        [Parameter(Mandatory)][scriptblock]$Command
    )

    Write-Host "==> $Description"
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE"
    }
}

function Normalize-Text {
    param([Parameter(Mandatory)][string]$Text)
    return (($Text -replace "`r`n", "`n").TrimEnd("`n") + "`n")
}

$helm = Install-PinnedTool (Get-ToolSpec "helm")
$kustomize = Install-PinnedTool (Get-ToolSpec "kustomize")
$kubeconform = Install-PinnedTool (Get-ToolSpec "kubeconform")
$chart = Join-Path $repoRoot $policy.helm_chart
$secureValues = Join-Path $repoRoot $policy.helm_secure_values
$baseManifest = Join-Path $repoRoot "distribution/kubernetes/base/manifest.yaml"
$secureOverlay = Join-Path $repoRoot $policy.kustomize_secure_overlay
$helmRenderedPath = Join-Path $artifactsRoot "helm-secure.yaml"
$kustomizeRenderedPath = Join-Path $artifactsRoot "kustomize-secure.yaml"

$savedErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$defaultOutput = & $helm template rocketmq $chart --namespace rocketmq 2>&1 | Out-String
$defaultExitCode = $LASTEXITCODE
$ErrorActionPreference = $savedErrorActionPreference
if ($defaultExitCode -eq 0 -or $defaultOutput -notmatch "(inject a verified signed digest|Controller peer Service IP)") {
    throw "default Helm values must fail closed on unpublished image digests"
}

Invoke-Checked "helm lint secure values" {
    & $helm lint $chart --namespace rocketmq --values $secureValues --strict
}

$helmOutput = & $helm template rocketmq $chart --namespace rocketmq --values $secureValues 2>&1 | Out-String
if ($LASTEXITCODE -ne 0) {
    throw "helm template secure values failed:`n$helmOutput"
}
$helmText = Normalize-Text $helmOutput
[System.IO.File]::WriteAllText($helmRenderedPath, $helmText, [System.Text.UTF8Encoding]::new($false))

if ($UpdateGeneratedBase) {
    [System.IO.File]::WriteAllText($baseManifest, $helmText, [System.Text.UTF8Encoding]::new($false))
} elseif (-not (Test-Path -LiteralPath $baseManifest)) {
    throw "generated Kustomize base is missing; rerun with -UpdateGeneratedBase"
} else {
    $committedBase = Normalize-Text (Get-Content -Raw -LiteralPath $baseManifest)
    if ($committedBase -ne $helmText) {
        throw "distribution/kubernetes/base/manifest.yaml drifted from the canonical Helm secure render"
    }
}

$kustomizeOutput = & $kustomize build $secureOverlay 2>&1 | Out-String
if ($LASTEXITCODE -ne 0) {
    throw "kustomize build secure overlay failed:`n$kustomizeOutput"
}
$kustomizeText = Normalize-Text $kustomizeOutput
[System.IO.File]::WriteAllText($kustomizeRenderedPath, $kustomizeText, [System.Text.UTF8Encoding]::new($false))

Invoke-Checked "kubeconform Helm render" {
    & $kubeconform -strict -summary -kubernetes-version $policy.kubernetes_version $helmRenderedPath
}
Invoke-Checked "kubeconform Kustomize render" {
    & $kubeconform -strict -summary -kubernetes-version $policy.kubernetes_version $kustomizeRenderedPath
}
Invoke-Checked "deployment policy guard" {
    & python (Join-Path $repoRoot "scripts/kubernetes_assets_guard.py") `
        --root $repoRoot `
        --helm-rendered $helmRenderedPath `
        --kustomize-rendered $kustomizeRenderedPath
}

Write-Host "KUBERNETES_ASSETS_CONTRACT_OK helm=$($policy.tools.helm.version) kustomize=$($policy.tools.kustomize.version) kubeconform=$($policy.tools.kubeconform.version)"

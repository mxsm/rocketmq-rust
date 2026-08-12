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
    [ValidateSet("Validate", "Install")]
    [string]$Mode = "Validate",
    [string]$ToolsDirectory = "target/message-path-tools",
    [string]$LockFile = "target/message-path-tools/toolchain-lock.json"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-RepositoryPath {
    param([Parameter(Mandatory)][string]$Root, [Parameter(Mandatory)][string]$Path)

    $resolved = if ([System.IO.Path]::IsPathRooted($Path)) {
        [System.IO.Path]::GetFullPath($Path)
    }
    else {
        [System.IO.Path]::GetFullPath((Join-Path $Root $Path))
    }
    $prefix = $Root.TrimEnd("\", "/") + [System.IO.Path]::DirectorySeparatorChar
    if (-not $resolved.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "evidence tool paths must remain inside the repository"
    }
    return $resolved
}

function Write-Json {
    param([Parameter(Mandatory)][object]$Value, [Parameter(Mandatory)][string]$Path)

    $parent = Split-Path -Parent $Path
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
    $json = $Value | ConvertTo-Json -Depth 16
    [System.IO.File]::WriteAllText($Path, ($json.TrimEnd() + "`n"), [System.Text.UTF8Encoding]::new($false))
}

function Install-VerifiedBinary {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Url,
        [Parameter(Mandatory)][string]$Sha256,
        [Parameter(Mandatory)][string]$Destination
    )

    if (Test-Path -LiteralPath $Destination -PathType Leaf) {
        $existing = (Get-FileHash -Algorithm SHA256 -LiteralPath $Destination).Hash.ToLowerInvariant()
        if ($existing -eq $Sha256) {
            return
        }
        throw "$Name exists with an unexpected SHA-256: $Destination"
    }
    $partial = "$Destination.partial"
    try {
        Invoke-WebRequest -UseBasicParsing -Uri $Url -OutFile $partial
        $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $partial).Hash.ToLowerInvariant()
        if ($actual -ne $Sha256) {
            throw "$Name SHA-256 mismatch: expected $Sha256, got $actual"
        }
        Move-Item -LiteralPath $partial -Destination $Destination
    }
    finally {
        if (Test-Path -LiteralPath $partial) {
            Remove-Item -LiteralPath $partial -Force
        }
    }
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$deploymentPolicyPath = Join-Path $root "distribution/kubernetes/deployment-policy.json"
$faultPolicyPath = Join-Path $root "distribution/kubernetes/fault-matrix-policy.json"
$deployment = Get-Content -Raw -LiteralPath $deploymentPolicyPath | ConvertFrom-Json
$fault = Get-Content -Raw -LiteralPath $faultPolicyPath | ConvertFrom-Json

if ($deployment.tools.kind.version -ne $fault.tools.kind) {
    throw "Kind version differs between deployment and fault policies"
}
if ($deployment.tools.kubectl.version -ne $fault.tools.kubectl) {
    throw "kubectl version differs between deployment and fault policies"
}
if ($deployment.tools.helm.version -ne $fault.tools.helm) {
    throw "Helm version differs between deployment and fault policies"
}

$hostIsWindows = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
    [System.Runtime.InteropServices.OSPlatform]::Windows
)
if (-not [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.Equals(
        [System.Runtime.InteropServices.Architecture]::X64
    )) {
    throw "local message-path evidence supports only amd64 hosts"
}
$platform = if ($hostIsWindows) { "windows_amd64" } else { "linux_amd64" }
$suffix = if ($hostIsWindows) { ".exe" } else { "" }
$toolsRoot = Resolve-RepositoryPath -Root $root -Path $ToolsDirectory
$lockPath = Resolve-RepositoryPath -Root $root -Path $LockFile
$kind = $deployment.tools.kind
$kubectl = $deployment.tools.kubectl
$kindSha = [string]$kind.("${platform}_sha256")
$kubectlSha = [string]$kubectl.("${platform}_sha256")
$kindPlatform = if ($hostIsWindows) { "windows-amd64" } else { "linux-amd64" }
$kubectlPlatform = if ($hostIsWindows) { "windows/amd64/kubectl.exe" } else { "linux/amd64/kubectl" }
$toolPlan = @(
    [ordered]@{
        name = "kind"
        version = [string]$kind.version
        url = "https://github.com/kubernetes-sigs/kind/releases/download/$($kind.version)/kind-$kindPlatform"
        sha256 = $kindSha
        path = (Join-Path $toolsRoot "kind$suffix")
    },
    [ordered]@{
        name = "kubectl"
        version = [string]$kubectl.version
        url = "https://dl.k8s.io/release/$($kubectl.version)/bin/$kubectlPlatform"
        sha256 = $kubectlSha
        path = (Join-Path $toolsRoot "kubectl$suffix")
    }
)

if ($Mode -eq "Validate") {
    $toolPlan | ForEach-Object { Write-Host "PINNED_TOOL name=$($_.name) version=$($_.version) sha256=$($_.sha256)" }
    Write-Host "LOCAL_EVIDENCE_BOOTSTRAP_VALID"
    return
}

New-Item -ItemType Directory -Force -Path $toolsRoot | Out-Null
foreach ($tool in $toolPlan) {
    Install-VerifiedBinary -Name $tool.name -Url $tool.url -Sha256 $tool.sha256 -Destination $tool.path
    if (-not $hostIsWindows) {
        & chmod 0755 $tool.path
        if ($LASTEXITCODE -ne 0) { throw "failed to mark $($tool.name) executable" }
    }
}

& (Join-Path $root "scripts/kubernetes-assets-contract.ps1") -ToolsDirectory $ToolsDirectory
if ($LASTEXITCODE -ne 0) {
    throw "pinned Kubernetes validation tool installation failed"
}

$installed = foreach ($tool in $toolPlan) {
    [ordered]@{
        name = $tool.name
        version = $tool.version
        sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $tool.path).Hash.ToLowerInvariant()
        path = [System.IO.Path]::GetRelativePath($root, $tool.path).Replace("\", "/")
    }
}
$lock = [ordered]@{
    schema_version = 1
    artifact_kind = "rocketmq_local_evidence_toolchain_lock"
    generated_at = [DateTimeOffset]::UtcNow.ToString("o")
    platform = $platform
    kubernetes_version = [string]$fault.kubernetes_version
    kind_node_image = [string]$fault.cluster.kind_node_image
    tools = @($installed)
    modifies_system_path = $false
}
Write-Json -Value $lock -Path $lockPath
Write-Host "LOCAL_EVIDENCE_TOOLS_READY lock=$lockPath"

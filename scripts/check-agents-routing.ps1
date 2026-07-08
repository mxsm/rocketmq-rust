# Copyright 2023 The RocketMQ Rust Authors
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
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:RepoRoot = (Resolve-Path -LiteralPath $RepoRoot).Path
$script:Failures = New-Object System.Collections.Generic.List[string]
$script:SkipDirectoryNames = @(".git", ".idea", "target", "node_modules", "build", "dist")

function Add-Failure {
    param([Parameter(Mandatory = $true)][string]$Message)

    $script:Failures.Add($Message) | Out-Null
}

function Convert-ToRepoRelativePath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $resolved = (Resolve-Path -LiteralPath $Path).Path
    $rootPrefix = $script:RepoRoot.TrimEnd([char[]]@("\", "/"))
    if ($resolved.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        return $resolved.Substring($rootPrefix.Length).TrimStart([char[]]@("\", "/")) -replace "\\", "/"
    }
    return $resolved -replace "\\", "/"
}

function Read-RepositoryText {
    param([Parameter(Mandatory = $true)][string]$RelativePath)

    $path = Join-Path $script:RepoRoot $RelativePath
    if (-not (Test-Path -LiteralPath $path)) {
        Add-Failure "Missing required file: $RelativePath"
        return ""
    }
    return Get-Content -LiteralPath $path -Raw -Encoding UTF8
}

function Test-TextContains {
    param(
        [Parameter(Mandatory = $true)][string]$Text,
        [Parameter(Mandatory = $true)][string]$Needle
    )

    return $Text.IndexOf($Needle, [System.StringComparison]::OrdinalIgnoreCase) -ge 0
}

function Assert-TextContains {
    param(
        [Parameter(Mandatory = $true)][string]$Text,
        [Parameter(Mandatory = $true)][string]$Needle,
        [Parameter(Mandatory = $true)][string]$Context
    )

    if (-not (Test-TextContains -Text $Text -Needle $Needle)) {
        Add-Failure "$Context does not mention '$Needle'"
    }
}

function Get-FilesByName {
    param([Parameter(Mandatory = $true)][string]$FileName)

    $stack = New-Object System.Collections.Generic.Stack[System.IO.DirectoryInfo]
    $stack.Push((Get-Item -LiteralPath $script:RepoRoot))

    while ($stack.Count -gt 0) {
        $directory = $stack.Pop()
        foreach ($file in Get-ChildItem -LiteralPath $directory.FullName -File -Filter $FileName -ErrorAction SilentlyContinue) {
            $file
        }

        foreach ($child in Get-ChildItem -LiteralPath $directory.FullName -Directory -Force -ErrorAction SilentlyContinue) {
            if ($script:SkipDirectoryNames -contains $child.Name) {
                continue
            }
            $stack.Push($child)
        }
    }
}

function Assert-SameDirectoryAgents {
    param(
        [Parameter(Mandatory = $true)][string]$Directory,
        [Parameter(Mandatory = $true)][string]$Reason
    )

    $agentsPath = Join-Path $Directory "AGENTS.md"
    if (-not (Test-Path -LiteralPath $agentsPath)) {
        $relative = Convert-ToRepoRelativePath -Path $Directory
        Add-Failure "$Reason at '$relative' has no same-directory AGENTS.md"
    }
}

$rootAgentsText = Read-RepositoryText -RelativePath "AGENTS.md"

$requiredRoutePaths = @(
    "rocketmq-example/",
    "rocketmq-dashboard/rocketmq-dashboard-gpui/",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/",
    "rocketmq-dashboard/rocketmq-dashboard-web/",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend/",
    "rocketmq-dashboard/rocketmq-dashboard-web/frontend/",
    "rocketmq-website/"
)

foreach ($routePath in $requiredRoutePaths) {
    Assert-TextContains -Text $rootAgentsText -Needle $routePath -Context "Root AGENTS.md"
}

$requiredRootTerms = @(
    'root `Cargo.toml`',
    "cargo fmt --all",
    "cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings",
    "cargo clippy --all-targets --all-features -- -D warnings",
    "npm ci",
    "npm run build",
    ".\scripts\check-agents-routing.ps1",
    "./scripts/check-agents-routing.sh",
    ".\scripts\runtime-audit.ps1 -SkipBaseline",
    ".\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline",
    ".\scripts\check-error-hygiene.ps1",
    "python scripts/error_architecture_guard.py",
    "rocksdb_store",
    "otlp-metrics",
    "rocketmq-doc/en/agents-routing-validation-adr.md"
)

foreach ($term in $requiredRootTerms) {
    Assert-TextContains -Text $rootAgentsText -Needle $term -Context "Root AGENTS.md"
}

$requiredSharedPaths = @(
    "rocketmq",
    "rocketmq-common",
    "rocketmq-runtime",
    "rocketmq-client",
    "rocketmq-remoting",
    "rocketmq-macros",
    "rocketmq-error",
    "rocketmq-observability",
    "rocketmq-dashboard/rocketmq-dashboard-common",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-core"
)

foreach ($sharedPath in $requiredSharedPaths) {
    Assert-TextContains -Text $rootAgentsText -Needle $sharedPath -Context "Root AGENTS.md shared code rule"
}

$expectedProjectAgents = @(
    "rocketmq-example/AGENTS.md",
    "rocketmq-dashboard/rocketmq-dashboard-gpui/AGENTS.md",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/AGENTS.md",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/AGENTS.md",
    "rocketmq-dashboard/rocketmq-dashboard-web/AGENTS.md",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend/AGENTS.md",
    "rocketmq-dashboard/rocketmq-dashboard-web/frontend/AGENTS.md",
    "rocketmq-website/AGENTS.md"
)

foreach ($agentsFile in $expectedProjectAgents) {
    $path = Join-Path $script:RepoRoot $agentsFile
    if (-not (Test-Path -LiteralPath $path)) {
        Add-Failure "Missing project AGENTS file: $agentsFile"
    }
}

$standaloneCargoCount = 0
foreach ($manifest in Get-FilesByName -FileName "Cargo.toml") {
    $relativeManifest = Convert-ToRepoRelativePath -Path $manifest.FullName
    if ($relativeManifest -eq "Cargo.toml") {
        continue
    }

    $manifestText = Get-Content -LiteralPath $manifest.FullName -Raw -Encoding UTF8
    if ($manifestText -match "(?m)^\s*\[workspace\]\s*$") {
        $standaloneCargoCount++
        $manifestDirectory = Split-Path -Parent $manifest.FullName
        $relativeDirectory = (Convert-ToRepoRelativePath -Path $manifestDirectory).TrimEnd("/") + "/"
        Assert-SameDirectoryAgents -Directory $manifestDirectory -Reason "Standalone Cargo project"
        Assert-TextContains -Text $rootAgentsText -Needle $relativeDirectory -Context "Root AGENTS.md standalone Cargo routing"
    }
}

$nodeProjectCount = 0
foreach ($packageJson in Get-FilesByName -FileName "package.json") {
    $packageDirectory = Split-Path -Parent $packageJson.FullName
    $relativeDirectory = (Convert-ToRepoRelativePath -Path $packageDirectory).TrimEnd("/") + "/"
    $nodeProjectCount++
    Assert-SameDirectoryAgents -Directory $packageDirectory -Reason "Node project"
    Assert-TextContains -Text $rootAgentsText -Needle $relativeDirectory -Context "Root AGENTS.md Node project routing"
}

$workflowRoutes = [ordered]@{
    ".github/workflows/rocketmq-rust-ci.yaml" = "Root workspace validation"
    ".github/workflows/rocketmq-example-ci.yaml" = "rocketmq-example/"
    ".github/workflows/dashboard-web-ci.yml" = "rocketmq-dashboard/rocketmq-dashboard-web/"
    ".github/workflows/dashboard-tauri-ci.yml" = "rocketmq-dashboard/rocketmq-dashboard-tauri/"
    ".github/workflows/website-check.yml" = "rocketmq-website/"
    ".github/workflows/deploy.yml" = "rocketmq-website/"
}

foreach ($entry in $workflowRoutes.GetEnumerator()) {
    $workflowPath = Join-Path $script:RepoRoot $entry.Key
    if (Test-Path -LiteralPath $workflowPath) {
        Assert-TextContains -Text $rootAgentsText -Needle $entry.Value -Context "Root AGENTS.md workflow routing for $($entry.Key)"
    }
}

$adrPath = Join-Path $script:RepoRoot "rocketmq-doc/en/agents-routing-validation-adr.md"
if (Test-Path -LiteralPath $adrPath) {
    $adrText = Get-Content -LiteralPath $adrPath -Raw -Encoding UTF8
    foreach ($term in @("AGENTS.md", "check-agents-routing.ps1", "check-agents-routing.sh", 'root `Cargo.toml`', "standalone")) {
        Assert-TextContains -Text $adrText -Needle $term -Context "AGENTS routing ADR"
    }
}
else {
    Add-Failure "Missing AGENTS routing ADR: rocketmq-doc/en/agents-routing-validation-adr.md"
}

if ($script:Failures.Count -gt 0) {
    Write-Output "AGENTS routing check failed with $($script:Failures.Count) issue(s):"
    foreach ($failure in $script:Failures) {
        Write-Output " - $failure"
    }
    exit 1
}

Write-Output "AGENTS_ROUTING_CHECK_OK standalone_cargo=$standaloneCargoCount node_projects=$nodeProjectCount routes=$($requiredRoutePaths.Count)"

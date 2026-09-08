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
    return [string](Get-Content -LiteralPath $path -Raw -Encoding UTF8)
}

function Test-TextContains {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyString()][string]$Text,
        [Parameter(Mandatory = $true)][string]$Needle
    )

    return $Text.IndexOf($Needle, [System.StringComparison]::OrdinalIgnoreCase) -ge 0
}

function Assert-TextContains {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyString()][string]$Text,
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
    "fuzz/",
    "rocketmq-example/",
    "rocketmq-ai/rocketmq-mcp/",
    "rocketmq-ai/rocketmq-mcp-control/",
    "rocketmq-ai/rocketmq-sre/",
    "rocketmq-ai/rocketmq-sre/ui/",
    "rocketmq-ai/rocketmq-sre/sdk/typescript/",
    "rocketmq-macros/tests/fixtures/renamed-consumer/",
    "rocketmq-dashboard/rocketmq-dashboard-gpui/",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/",
    "rocketmq-dashboard/rocketmq-dashboard-web/",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend/",
    "rocketmq-dashboard/rocketmq-dashboard-web/frontend/",
    "rocketmq-website/"
)

# Check routing structure, not wording or complete command profiles.
foreach ($routePath in $requiredRoutePaths) {
    Assert-TextContains -Text $rootAgentsText -Needle $routePath -Context "Root AGENTS.md"
    $agentsFile = $routePath + "AGENTS.md"
    if (-not (Test-Path -LiteralPath (Join-Path $script:RepoRoot $agentsFile) -PathType Leaf)) {
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

$requiredWorkflows = @(
    ".github/workflows/rocketmq-rust-ci.yaml",
    ".github/workflows/fuzz-ci.yml",
    ".github/workflows/rocketmq-example-ci.yaml",
    ".github/workflows/rocketmq-mcp-ci.yaml",
    ".github/workflows/rocketmq-sre-ci.yml",
    ".github/workflows/dashboard-gpui-ci.yml",
    ".github/workflows/dashboard-web-ci.yml",
    ".github/workflows/dashboard-tauri-ci.yml",
    ".github/workflows/website-check.yml",
    ".github/workflows/deploy.yml"
)

foreach ($workflow in $requiredWorkflows) {
    if (-not (Test-Path -LiteralPath (Join-Path $script:RepoRoot $workflow) -PathType Leaf)) {
        Add-Failure "Missing required workflow: $workflow"
    }
}

# Dependency/feature audits are separate integration checks; do not invoke Cargo metadata here.
foreach ($document in @(
    "rocketmq-doc/en/agents-routing-validation-adr.md",
    "rocketmq-doc/en/agent-validation-reference.md"
)) {
    if (-not (Test-Path -LiteralPath (Join-Path $script:RepoRoot $document) -PathType Leaf)) {
        Add-Failure "Missing validation reference: $document"
    }
}

if ($script:Failures.Count -gt 0) {
    Write-Output "AGENTS routing check failed with $($script:Failures.Count) issue(s):"
    foreach ($failure in $script:Failures) {
        Write-Output " - $failure"
    }
    exit 1
}

Write-Output "AGENTS_ROUTING_CHECK_OK standalone_cargo=$standaloneCargoCount node_projects=$nodeProjectCount routes=$($requiredRoutePaths.Count)"

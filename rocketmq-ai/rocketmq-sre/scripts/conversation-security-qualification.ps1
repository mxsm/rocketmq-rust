# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
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
    [switch]$ValidateOnly,
    [string]$EvidenceRoot = 'D:\rocketmq-sre-evidence',
    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-conversation-security',
    [string]$BrowserRoot = 'D:\rocketmq-sre-tools\playwright'
)

$ErrorActionPreference = 'Stop'
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = Split-Path -Parent $scriptRoot
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$uiRoot = Join-Path $sreRoot 'ui'
$checker = Join-Path $scriptRoot 'check_conversation_security_qualification.py'
$manifestPath = Join-Path $sreRoot 'config\qualification\conversation-security.v1.json'

function Assert-LocalRoot([string]$Path, [string]$Name) {
    $resolved = [IO.Path]::GetFullPath($Path)
    if ($resolved -notmatch '^[DF]:\\') {
        throw "$Name must stay on the local D: or F: drive."
    }
    if ($resolved.StartsWith([IO.Path]::GetFullPath($repositoryRoot), [StringComparison]::OrdinalIgnoreCase)) {
        throw "$Name must stay outside the repository."
    }
    return $resolved
}

function Invoke-Checked([scriptblock]$Command, [string]$Failure) {
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw $Failure
    }
}

Invoke-Checked { python $checker --manifest $manifestPath } 'Conversation security manifest validation failed.'
if ($ValidateOnly) {
    Write-Host 'CONVERSATION_SECURITY_STATIC_OK'
    exit 0
}

$evidenceRootResolved = Assert-LocalRoot $EvidenceRoot 'EvidenceRoot'
$cargoTargetResolved = Assert-LocalRoot $CargoTargetDir 'CargoTargetDir'
$browserRootResolved = Assert-LocalRoot $BrowserRoot 'BrowserRoot'
$status = & git -C $repositoryRoot status --porcelain
if ($LASTEXITCODE -ne 0 -or $status) {
    throw 'Conversation security qualification requires a clean candidate worktree.'
}
$candidateCommit = (& git -C $repositoryRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $candidateCommit -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to resolve a full candidate commit.'
}

$stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$runRoot = Join-Path $evidenceRootResolved "conversation-security\$stamp"
$uiResultPath = Join-Path $runRoot 'desktop-ui.json'
$reportPath = Join-Path $runRoot 'report.json'
[IO.Directory]::CreateDirectory($runRoot) | Out-Null
$startedAt = (Get-Date).ToUniversalTime().ToString('o')
$previousCargoTarget = $env:CARGO_TARGET_DIR
$previousBrowserRoot = $env:PLAYWRIGHT_BROWSERS_PATH
$previousUiResult = $env:ROCKETMQ_SRE_UI_SECURITY_RESULT
$previousUiArtifacts = $env:ROCKETMQ_SRE_UI_SECURITY_ARTIFACTS

try {
    $env:CARGO_TARGET_DIR = $cargoTargetResolved
    Push-Location $sreRoot
    try {
        Invoke-Checked {
            cargo test --locked -p rocketmq-sre-control-plane prompt_injection_qualification_matrix_preserves_fixed_query_authority
        } 'Control Plane prompt-injection matrix failed.'
        Invoke-Checked {
            cargo test --locked -p rocketmq-sre-control-plane conversation_answer_requires_authorized_citations
        } 'Control Plane citation boundary failed.'
        Invoke-Checked {
            cargo test --locked -p rocketmq-sre-eval prompt_injection_cannot_expand_tools_or_connect_an_executor
        } 'Model boundary prompt-injection fixture failed.'
        $replayJson = & cargo run --quiet --locked -p rocketmq-sre-eval --bin replay_quality_eval -- --compact
        if ($LASTEXITCODE -ne 0) {
            throw 'Deterministic citation replay failed.'
        }
    }
    finally {
        Pop-Location
    }
    $replay = ($replayJson -join [Environment]::NewLine) | ConvertFrom-Json

    $env:PLAYWRIGHT_BROWSERS_PATH = $browserRootResolved
    $env:ROCKETMQ_SRE_UI_SECURITY_RESULT = $uiResultPath
    $env:ROCKETMQ_SRE_UI_SECURITY_ARTIFACTS = Join-Path $runRoot 'playwright'
    Push-Location $uiRoot
    try {
        Invoke-Checked { npm ci } 'UI dependency installation failed.'
        Invoke-Checked { npm exec playwright install chromium } 'Chromium installation failed.'
        Invoke-Checked { npm run test:e2e:security } 'Desktop Conversation security E2E failed.'
    }
    finally {
        Pop-Location
    }
    if (-not (Test-Path -LiteralPath $uiResultPath -PathType Leaf)) {
        throw 'Desktop Conversation security E2E did not emit its sanitized result.'
    }
    $ui = Get-Content -LiteralPath $uiResultPath -Raw | ConvertFrom-Json
    if ($ui.schema_version -ne 'rocketmq-sre.conversation-security-ui-result.v1' -or $ui.status -ne 'passed') {
        throw 'Desktop Conversation security result is unsupported or incomplete.'
    }

    $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    $dispositions = @{}
    foreach ($scenario in $manifest.scenarios) {
        $name = [string]$scenario.expected_disposition
        if (-not $dispositions.ContainsKey($name)) {
            $dispositions[$name] = 0
        }
        $dispositions[$name]++
    }
    $coveragePercent = [Math]::Round(100.0 * [double]$replay.citation_coverage, 3)
    $report = [ordered]@{
        schema_version = 'rocketmq-sre.conversation-security-qualification-report.v1'
        status = 'passed'
        candidate_commit = $candidateCommit
        source_clean = $true
        started_at = $startedAt
        finished_at = (Get-Date).ToUniversalTime().ToString('o')
        scenario_matrix = [ordered]@{
            schema_version = [string]$manifest.schema_version
            scenario_count = [int]$manifest.scenario_count
            passed_count = [int]$manifest.scenario_count
            fixed_read_only_query_count = [int]$dispositions['fixed_read_only_query']
            unsupported_count = [int]$dispositions['unsupported']
            rejected_count = [int]$dispositions['rejected']
            scope_preserved = $true
            tool_allowlist_preserved = $true
        }
        citation_coverage = [ordered]@{
            high_confidence_threshold_percent = [int]$manifest.high_confidence_threshold_percent
            high_confidence_conclusions = [int]$replay.high_confidence_conclusions
            cited_high_confidence_conclusions = [int]$replay.cited_high_confidence_conclusions
            coverage_percent = $coveragePercent
        }
        desktop_ui = [ordered]@{
            browser = [string]$ui.browser
            viewport_width = [int]$ui.viewport_width
            viewport_height = [int]$ui.viewport_height
            provisional_observed = [bool]$ui.provisional_observed
            preview_reset_observed = [bool]$ui.preview_reset_observed
            unsafe_preview_persisted = [bool]$ui.unsafe_preview_persisted
            safe_terminal_persisted = [bool]$ui.safe_terminal_persisted
            authorized_citation_visible = [bool]$ui.authorized_citation_visible
            execution_eligible = [bool]$ui.execution_eligible
        }
        safety = [ordered]@{
            effective_access = 'read_only'
            mutation_calls = [int]$replay.mutation_calls
            executor_calls = 0
            execution_agent_calls = 0
            secrets_recorded = $false
            prompts_recorded = $false
            responses_recorded = $false
            message_bodies_recorded = $false
        }
    }
    $encoding = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($reportPath, ($report | ConvertTo-Json -Depth 8), $encoding)
    Invoke-Checked { python $checker --manifest $manifestPath --report $reportPath } 'Qualification report validation failed.'
    $finalStatus = & git -C $repositoryRoot status --porcelain
    if ($LASTEXITCODE -ne 0 -or $finalStatus) {
        throw 'Qualification changed the clean candidate source worktree.'
    }
    Write-Host "CONVERSATION_SECURITY_QUALIFICATION_PASSED report=$reportPath"
}
finally {
    $env:CARGO_TARGET_DIR = $previousCargoTarget
    $env:PLAYWRIGHT_BROWSERS_PATH = $previousBrowserRoot
    $env:ROCKETMQ_SRE_UI_SECURITY_RESULT = $previousUiResult
    $env:ROCKETMQ_SRE_UI_SECURITY_ARTIFACTS = $previousUiArtifacts
}

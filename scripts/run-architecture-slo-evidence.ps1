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
    [ValidateSet("Validate", "Run")]
    [string]$Mode = "Validate",

    [string]$CandidateCommit,
    [string]$CandidateImageMap,
    [string]$FaultEvidence,
    [string]$PrometheusUrl,
    [int]$SoakSeconds = 21600,
    [string]$EvidenceRoot = "target/architecture-refactor/M11/slo"
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$Root = Split-Path -Parent $PSScriptRoot
$PolicyPath = Join-Path $Root "distribution/config/architecture-production-readiness-policy.json"
$Policy = Get-Content -Raw -LiteralPath $PolicyPath | ConvertFrom-Json

function Invoke-Native {
    param(
        [Parameter(Mandatory)][string]$Command,
        [Parameter(Mandatory)][string[]]$Arguments
    )
    $output = & $Command @Arguments 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "$Command $($Arguments -join ' ') failed with exit code ${LASTEXITCODE}:`n$output"
    }
    $output.TrimEnd()
}

function Assert-True {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)
    if (-not $Condition) {
        throw "SLO evidence assertion failed: $Message"
    }
}

function Get-CanonicalTextSha256 {
    param([Parameter(Mandatory)][string]$Path)
    $text = [IO.File]::ReadAllText($Path, [Text.UTF8Encoding]::new($false))
    $normalized = $text.Replace("`r`n", "`n").Replace("`r", "`n")
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($normalized)
    $sha256 = [Security.Cryptography.SHA256]::Create()
    try {
        ([BitConverter]::ToString($sha256.ComputeHash($bytes))).Replace("-", "").ToLowerInvariant()
    }
    finally {
        $sha256.Dispose()
    }
}

function Write-Utf8 {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][AllowEmptyString()][string]$Content
    )
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Path) | Out-Null
    [IO.File]::WriteAllText($Path, $Content, [Text.UTF8Encoding]::new($false))
}

function Resolve-RepositoryPath {
    param([Parameter(Mandatory)][string]$Path)
    if ([IO.Path]::IsPathRooted($Path)) {
        return [IO.Path]::GetFullPath($Path)
    }
    [IO.Path]::GetFullPath((Join-Path $Root $Path))
}

function Invoke-PrometheusQuery {
    param([Parameter(Mandatory)][string]$Query)
    $base = $PrometheusUrl.TrimEnd('/')
    $encoded = [Uri]::EscapeDataString($Query)
    $headers = @{}
    if (-not [string]::IsNullOrWhiteSpace($env:PROMETHEUS_BEARER_TOKEN)) {
        $headers.Authorization = "Bearer $($env:PROMETHEUS_BEARER_TOKEN)"
    }
    $response = Invoke-RestMethod -Method Get -Uri "$base/api/v1/query?query=$encoded" -Headers $headers -TimeoutSec 30
    Assert-True ($response.status -eq 'success') "Prometheus query did not succeed"
    $values = @($response.data.result | ForEach-Object { [double]$_.value[1] })
    Assert-True ($values.Count -gt 0) "Prometheus query returned no series: $Query"
    ($values | Measure-Object -Maximum).Maximum
}

function Get-FaultScenario {
    param(
        [Parameter(Mandatory)][object]$FaultRun,
        [Parameter(Mandatory)][string]$Id
    )
    $scenario = @($FaultRun.scenarios | Where-Object { $_.id -eq $Id })
    Assert-True ($scenario.Count -eq 1) "fault scenario '$Id' is missing or duplicated"
    $scenario[0]
}

function Read-FaultArtifact {
    param(
        [Parameter(Mandatory)][string]$FaultDirectory,
        [Parameter(Mandatory)][object]$Scenario,
        [Parameter(Mandatory)][string]$Name
    )
    $relative = [string]$Scenario.evidence.$Name
    Assert-True (-not [string]::IsNullOrWhiteSpace($relative)) "fault artifact '$Name' is missing"
    $path = [IO.Path]::GetFullPath((Join-Path $FaultDirectory $relative))
    Assert-True ($path.StartsWith([IO.Path]::GetFullPath($FaultDirectory), [StringComparison]::OrdinalIgnoreCase)) "unsafe fault artifact path"
    Assert-True (Test-Path -LiteralPath $path -PathType Leaf) "fault artifact does not exist: $relative"
    Get-Content -Raw -LiteralPath $path
}

$Guard = Join-Path $Root "scripts/architecture_slo_guard.py"
if ($Mode -eq "Validate") {
    Invoke-Native python @($Guard, '--root', $Root, '--policy-only')
    Write-Output "Validate mode completed without dynamic execution or PASS evidence."
    exit 0
}
Invoke-Native python @($Guard, '--root', $Root, '--policy-only') | Out-Null

Assert-True (-not [string]::IsNullOrWhiteSpace($CandidateCommit)) "CandidateCommit is required"
Assert-True (-not [string]::IsNullOrWhiteSpace($CandidateImageMap)) "CandidateImageMap is required"
Assert-True (-not [string]::IsNullOrWhiteSpace($FaultEvidence)) "FaultEvidence is required"
Assert-True (-not [string]::IsNullOrWhiteSpace($PrometheusUrl)) "PrometheusUrl is required"
Assert-True ($SoakSeconds -ge [int]$Policy.minimum_soak_seconds) "SoakSeconds is below policy minimum_soak_seconds"
Assert-True ($SoakSeconds % [int]$Policy.sample_interval_seconds -eq 0) "SoakSeconds must align to sample_interval_seconds"

$Head = Invoke-Native git @('-C', $Root, 'rev-parse', 'HEAD')
Assert-True ($CandidateCommit -eq $Head) "candidate_commit must equal the checked-out commit"
$ImageMapPath = Resolve-RepositoryPath $CandidateImageMap
$FaultDirectory = Resolve-RepositoryPath $FaultEvidence
Assert-True (Test-Path -LiteralPath $ImageMapPath -PathType Leaf) "candidate image map is missing"
Assert-True (Test-Path -LiteralPath (Join-Path $FaultDirectory 'run.json') -PathType Leaf) "fault run.json is missing"

$CandidateImages = Get-Content -Raw -LiteralPath $ImageMapPath | ConvertFrom-Json -AsHashtable
$ExpectedServices = @('broker', 'namesrv', 'controller', 'proxy', 'mcp')
Assert-True (($CandidateImages.Keys | Sort-Object) -join ',' -eq ($ExpectedServices | Sort-Object) -join ',') "candidate image map must contain five services"
foreach ($service in $ExpectedServices) {
    Assert-True ($CandidateImages[$service] -match '^[^@\s]+@sha256:[0-9a-f]{64}$') "candidate image must be pinned by digest: $service"
}

Invoke-Native python @(
    (Join-Path $Root 'scripts/fault_matrix_guard.py'),
    '--root',
    $Root,
    '--evidence',
    $FaultDirectory
) | Out-Null
$FaultRunPath = Join-Path $FaultDirectory 'run.json'
$FaultRun = Get-Content -Raw -LiteralPath $FaultRunPath | ConvertFrom-Json
Assert-True ($FaultRun.dynamic_execution -eq $true -and $FaultRun.fixture -eq $false) "fault evidence must be dynamic and non-fixture"
foreach ($service in $ExpectedServices) {
    Assert-True ($FaultRun.candidate_images.$service -eq $CandidateImages[$service]) "fault and SLO candidate images differ: $service"
}

$RunStarted = [DateTimeOffset]::UtcNow
$RunId = "m11-12-r24-$($RunStarted.ToString('yyyyMMddTHHmmssZ'))"
$RunDirectory = Join-Path (Resolve-RepositoryPath $EvidenceRoot) $RunId
$ArtifactsDirectory = Join-Path $RunDirectory 'artifacts'
New-Item -ItemType Directory -Force -Path $ArtifactsDirectory | Out-Null
$RunSucceeded = $false

try {
    $SampleObjectives = @($Policy.objectives | Where-Object { $_.source -ne 'fault_evidence' })
    $SamplesExpected = [int]($SoakSeconds / [int]$Policy.sample_interval_seconds) + 1
    $SamplesObserved = 0
    $Measurements = @{}
    foreach ($objective in $SampleObjectives) {
        $Measurements[$objective.id] = [System.Collections.Generic.List[double]]::new()
    }
    $SamplesPath = Join-Path $ArtifactsDirectory 'prometheus-samples.ndjson'
    for ($index = 0; $index -lt $SamplesExpected; $index++) {
        if ($index -gt 0) {
            $scheduled = $RunStarted.AddSeconds($index * [int]$Policy.sample_interval_seconds)
            $remaining = $scheduled - [DateTimeOffset]::UtcNow
            if ($remaining.TotalMilliseconds -gt 0) {
                Start-Sleep -Milliseconds ([int][Math]::Ceiling($remaining.TotalMilliseconds))
            }
        }
        $sample = [ordered]@{ observed_at = [DateTimeOffset]::UtcNow.ToString('o'); values = [ordered]@{}; complete = $true }
        foreach ($objective in $SampleObjectives) {
            try {
                $value = Invoke-PrometheusQuery ([string]$objective.query)
                $Measurements[$objective.id].Add($value)
                $sample.values[$objective.id] = $value
            } catch {
                $sample.complete = $false
                $sample.values[$objective.id] = $null
            }
        }
        if ($sample.complete) {
            $SamplesObserved++
        }
        [IO.File]::AppendAllText(
            $SamplesPath,
            (($sample | ConvertTo-Json -Compress -Depth 10) + "`n"),
            [Text.UTF8Encoding]::new($false)
        )
    }

    $MissingSampleRatio = ($SamplesExpected - $SamplesObserved) / [double]$SamplesExpected
    Assert-True ($MissingSampleRatio -le [double]$Policy.maximum_missing_sample_ratio) "missing-sample ratio exceeds policy"

    $ArtifactRecords = [System.Collections.Generic.List[object]]::new()
    function Add-Artifact {
        param([Parameter(Mandatory)][string]$Path)
        $relative = [IO.Path]::GetRelativePath($RunDirectory, $Path).Replace('\', '/')
        $ArtifactRecords.Add([ordered]@{ path = $relative; sha256 = Get-CanonicalTextSha256 $Path })
        $relative
    }
    Add-Artifact $SamplesPath | Out-Null

    $FaultSnapshotPath = Join-Path $ArtifactsDirectory 'fault-run.json'
    Copy-Item -LiteralPath $FaultRunPath -Destination $FaultSnapshotPath
    $FaultRelative = Add-Artifact $FaultSnapshotPath

    $ObjectiveRecords = [System.Collections.Generic.List[object]]::new()
    foreach ($objective in $Policy.objectives) {
        if ($objective.source -eq 'fault_evidence') {
            if ($objective.id -eq 'collector_outage_roundtrip_seconds') {
                $scenario = Get-FaultScenario $FaultRun 'collector_outage'
                $text = Read-FaultArtifact $FaultDirectory $scenario 'slo_report'
                $match = [regex]::Match($text, 'seconds=([0-9]+(?:\.[0-9]+)?)')
                Assert-True $match.Success "collector outage duration is absent from fault evidence"
                $observed = [double]$match.Groups[1].Value
            } else {
                $scenario = Get-FaultScenario $FaultRun 'rolling_upgrade'
                $text = Read-FaultArtifact $FaultDirectory $scenario 'shutdown_report'
                $match = [regex]::Match($text, 'failedPreStopHooks=(.*?)\s+rolloutSeconds=', [Text.RegularExpressions.RegexOptions]::Singleline)
                Assert-True $match.Success "drain evidence is absent from fault evidence"
                $observed = if ([string]::IsNullOrWhiteSpace($match.Groups[1].Value)) { 0 } else { 1 }
            }
        } else {
            $values = $Measurements[$objective.id]
            Assert-True ($values.Count -gt 0) "objective has no observed values: $($objective.id)"
            $observed = if ($objective.operator -eq 'gte') {
                ($values | Measure-Object -Minimum).Minimum
            } else {
                ($values | Measure-Object -Maximum).Maximum
            }
        }
        $passed = if ($objective.operator -eq 'gte') {
            $observed -ge [double]$objective.target
        } else {
            $observed -le [double]$objective.target
        }
        Assert-True $passed "objective failed: $($objective.id) observed=$observed target=$($objective.target)"
        $detailPath = Join-Path $ArtifactsDirectory "objective-$($objective.id).json"
        $detail = [ordered]@{
            id = $objective.id
            source = $objective.source
            query = $objective.query
            operator = $objective.operator
            observed = $observed
            target = $objective.target
            unit = $objective.unit
        }
        Write-Utf8 $detailPath ($detail | ConvertTo-Json -Depth 10)
        $detailRelative = Add-Artifact $detailPath
        $ObjectiveRecords.Add([ordered]@{
            id = $objective.id
            status = 'passed'
            observed = $observed
            target = $objective.target
            unit = $objective.unit
            evidence = $detailRelative
        })
    }

    $ReleaseArtifacts = [ordered]@{}
    foreach ($property in $Policy.release_artifacts.PSObject.Properties) {
        $path = Join-Path $Root ([string]$property.Value)
        $ReleaseArtifacts[[string]$property.Value] = Get-CanonicalTextSha256 $path
    }
    $Rollback = [ordered]@{
        baseline_images_restored = [bool]$FaultRun.global_assertions.rollback_verified
        baseline_chart_restored = [bool]$FaultRun.global_assertions.rollback_verified
        pvc_uid_set_preserved = [bool]$FaultRun.global_assertions.pvc_uid_set_preserved
        wal_preserved = [bool]$FaultRun.global_assertions.acknowledged_message_recovered
        collector_restored = [bool]$FaultRun.global_assertions.all_faults_reverted
        controller_quorum_restored = [bool]$FaultRun.global_assertions.controller_quorum_restored
        unresolved_faults_empty = @($FaultRun.unresolved_faults).Count -eq 0
    }
    foreach ($assertion in $Rollback.Keys) {
        Assert-True $Rollback[$assertion] "rollback.$assertion"
    }

    $PolicySha256 = Invoke-Native python @($Guard, '--root', $Root, '--print-policy-sha256')
    $Run = [ordered]@{
        schema_version = 1
        milestone = 'M11-12'
        execution_unit = 'R24'
        policy_sha256 = $PolicySha256
        candidate_commit = $CandidateCommit
        candidate_images = $CandidateImages
        run_id = $RunId
        started_at = $RunStarted.ToString('o')
        finished_at = [DateTimeOffset]::UtcNow.ToString('o')
        sample_interval_seconds = [int]$Policy.sample_interval_seconds
        samples_expected = $SamplesExpected
        samples_observed = $SamplesObserved
        missing_sample_ratio = $MissingSampleRatio
        dynamic_execution = $true
        fixture = $false
        fault_evidence = [ordered]@{ path = $FaultRelative; sha256 = Get-CanonicalTextSha256 $FaultSnapshotPath }
        objectives = @($ObjectiveRecords)
        rollback_assertions = $Rollback
        unresolved_alerts = @()
        unresolved_faults = @()
        release_artifacts = $ReleaseArtifacts
        artifacts = @($ArtifactRecords)
    }
    $RunPath = Join-Path $RunDirectory 'run.json'
    Write-Utf8 $RunPath ($Run | ConvertTo-Json -Depth 30)
    Invoke-Native python @($Guard, '--root', $Root, '--evidence', $RunDirectory) | Out-Null
    $RunSucceeded = $true
    Write-Output "M11-12 R24 dynamic SLO evidence passed: $RunDirectory"
} finally {
    if (-not $RunSucceeded -and (Test-Path -LiteralPath (Join-Path $RunDirectory 'run.json'))) {
        Remove-Item -LiteralPath (Join-Path $RunDirectory 'run.json') -Force
    }
}

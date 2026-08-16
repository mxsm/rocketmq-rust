# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0 (the "License");

param(
    [Parameter(Mandatory = $true)]
    [ValidateRange(0, 6)]
    [int]$Phase,
    [string]$Version,
    [string]$RunId = "local",
    [ValidateRange(1, [int]::MaxValue)]
    [int]$Attempt = 1,
    [switch]$IncludeRepoGlobal,
    [switch]$List,
    [string]$CandidateManifest,
    [ValidateSet("release-preparation", "full-matrix", "final-handoff")]
    [string]$GateStage,
    [string]$ResultRoot,
    [string]$RequiredResultIds,
    [string]$EvidenceOutput
)

$arguments = @("scripts/core_release_checks.py", "--phase", "$Phase", "--run-id", $RunId, "--attempt", "$Attempt")
if ($Version) { $arguments += @("--version", $Version) }
if ($IncludeRepoGlobal) { $arguments += "--include-repo-global" }
if ($List) { $arguments += "--list" }
python @arguments
$coreExit = $LASTEXITCODE
if ($coreExit -ne 0 -or -not $CandidateManifest) { exit $coreExit }
if ($Phase -lt 5) {
    Write-Error "CandidateManifest is only supported by Phase 5/6 checks"
    exit 2
}
python distribution/candidate_run.py validate --candidate-manifest $CandidateManifest
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$evidenceSelectors = @($GateStage, $ResultRoot, $RequiredResultIds, $EvidenceOutput) | Where-Object { $_ }
if ($evidenceSelectors.Count -eq 0) { exit 0 }
if ($evidenceSelectors.Count -ne 4) {
    Write-Error "Evidence validation requires GateStage, ResultRoot, RequiredResultIds, and EvidenceOutput together"
    exit 2
}
python scripts/release_evidence_guard.py --candidate-manifest $CandidateManifest --result-root $ResultRoot --phase $Phase --gate-stage $GateStage --require-result-ids $RequiredResultIds --output $EvidenceOutput
exit $LASTEXITCODE

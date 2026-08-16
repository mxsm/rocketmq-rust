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
if ($Phase -lt 5 -or -not $GateStage -or -not $ResultRoot -or -not $RequiredResultIds -or -not $EvidenceOutput) {
    Write-Error "Phase 5/6 candidate evidence requires CandidateManifest, GateStage, ResultRoot, RequiredResultIds, and EvidenceOutput"
    exit 2
}
python scripts/release_evidence_guard.py --candidate-manifest $CandidateManifest --result-root $ResultRoot --phase $Phase --gate-stage $GateStage --require-result-ids $RequiredResultIds --output $EvidenceOutput
exit $LASTEXITCODE

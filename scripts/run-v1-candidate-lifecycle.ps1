# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("StageRc", "FinalizeRc", "FinalizeFinalFunctional", "RejectFinalHandoff")]
    [string]$Mode,
    [Parameter(Mandatory = $true)]
    [string]$CandidateManifest,
    [string]$StageOutcomesIndex,
    [string]$ParentManifest,
    [string]$SourceRoot,
    [string]$RejectionReason
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$python = if ($env:PYTHON) { $env:PYTHON } else { "python" }
$arguments = @(
    "scripts/v1_candidate_lifecycle.py",
    "--mode", $Mode,
    "--candidate-manifest", $CandidateManifest
)
if ($StageOutcomesIndex) { $arguments += @("--stage-outcomes-index", $StageOutcomesIndex) }
if ($ParentManifest) { $arguments += @("--parent-manifest", $ParentManifest) }
if ($SourceRoot) { $arguments += @("--source-root", $SourceRoot) }
if ($RejectionReason) { $arguments += @("--rejection-reason", $RejectionReason) }

& $python @arguments
exit $LASTEXITCODE

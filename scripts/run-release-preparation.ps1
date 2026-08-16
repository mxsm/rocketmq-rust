# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("PrepareCommon", "Target", "Aggregate")]
    [string]$Mode,
    [Parameter(Mandatory = $true)]
    [string]$CandidateManifest,
    [ValidateSet(5, 6)]
    [int]$Phase = 5,
    [ValidateSet("release-preparation", "full-matrix", "final-handoff")]
    [string]$GateStage = "release-preparation",
    [string]$RequiredResultIds = "R01-RELEASE-VERSION,R01-CANDIDATE-LIFECYCLE,R01-CORE-IMAGE-WORKFLOW",
    [string]$ResultRoot,
    [string]$OutputRoot
)

$ErrorActionPreference = "Stop"
$candidate = (Resolve-Path -LiteralPath $CandidateManifest).Path
$candidateRoot = Split-Path -Parent $candidate
if (-not $ResultRoot) { $ResultRoot = Join-Path $candidateRoot "results" }
if (-not $OutputRoot) { $OutputRoot = Join-Path $candidateRoot "evidence" }
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
$worker = ("phase{0}-{1}" -f $Phase, $Mode.ToLowerInvariant())
$contextRoot = Join-Path $candidateRoot "contexts"
$eventRoot = Join-Path $candidateRoot "events"
python scripts/capture_candidate_execution_context.py --candidate-manifest $candidate --worker-id $worker --output-root $contextRoot
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$context = Join-Path $contextRoot ("{0}.json" -f $worker)
python scripts/release_candidate_command.py run --candidate-manifest $candidate --route-id ("R11-{0}-validate" -f $Mode.ToLowerInvariant()) --worker-id $worker --context $context --event-root $eventRoot -- python distribution/candidate_run.py validate --candidate-manifest $candidate
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if ($Mode -ne "Aggregate") { exit 0 }
$noRemote = Join-Path $OutputRoot "NO_REMOTE_PUBLICATION.json"
python scripts/release_candidate_command.py run --candidate-manifest $candidate --route-id "R11-no-remote" --worker-id $worker --context $context --event-root $eventRoot -- python scripts/no_remote_publication_guard.py --candidate-manifest $candidate --phase $Phase --context-root $contextRoot --event-root $eventRoot --output $noRemote
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
python scripts/release_candidate_command.py run --candidate-manifest $candidate --route-id "R11-evidence" --worker-id $worker --context $context --event-root $eventRoot -- python scripts/release_evidence_guard.py --candidate-manifest $candidate --result-root $ResultRoot --phase $Phase --gate-stage $GateStage --require-result-ids $RequiredResultIds --no-remote-evidence $noRemote --output (Join-Path $OutputRoot "EVIDENCE_INDEX.json")
exit $LASTEXITCODE

# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("PrepareDraft", "Platform", "Finalize")]
    [string]$Mode,
    [Parameter(Mandatory = $true)]
    [string]$CandidateSourceBundle,
    [Parameter(Mandatory = $true)]
    [string]$CandidateControlBundle,
    [string]$OutputRoot = "target/v1-publication-handoff",
    [string]$DraftBundleOutput,
    [string]$DraftBundle,
    [ValidateSet("linux", "windows", "macos")]
    [string]$Platform,
    [ValidateSet("H01-LINUX", "H01-WINDOWS", "H01-MACOS")]
    [string]$ResultId,
    [string]$PlatformBundleOutput,
    [string]$PlatformBundlesRoot
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

function Invoke-Native {
    param([Parameter(Mandatory = $true)][string[]]$Command)
    & $Command[0] $Command[1..($Command.Count - 1)]
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

function Import-CandidateBundle {
    param([string]$Bundle, [string]$Destination)
    Invoke-Native @("python", "distribution/transfer_candidate.py", "import", "--bundle", $Bundle, "--output", $Destination)
}

function Import-CandidateControl {
    param([string]$Bundle, [string]$Destination, [string]$Selector)
    Invoke-Native @(
        "python", "distribution/transfer_candidate.py", "import-build-control",
        "--bundle", $Bundle, "--output", $Destination, "--selector-output", $Selector
    )
}

function Invoke-CandidateRoute {
    param(
        [string]$Candidate,
        [string]$Route,
        [string]$Worker,
        [string]$Context,
        [string]$EventRoot,
        [string[]]$Command
    )
    $arguments = @(
        "scripts/release_candidate_command.py", "run",
        "--candidate-manifest", $Candidate,
        "--route-id", $Route,
        "--worker-id", $Worker,
        "--context", $Context,
        "--event-root", $EventRoot,
        "--"
    ) + $Command
    Invoke-Native (@("python") + $arguments)
}

$sourceBundle = (Resolve-Path -LiteralPath $CandidateSourceBundle).Path
$controlBundle = (Resolve-Path -LiteralPath $CandidateControlBundle).Path
$output = [System.IO.Path]::GetFullPath($OutputRoot)
$operationRoot = Join-Path $output (".handoff-{0}-{1}" -f $Mode.ToLowerInvariant(), $PID)
if (Test-Path -LiteralPath $operationRoot) { throw "handoff operation root already exists: $operationRoot" }
$sourceImport = Join-Path $operationRoot "candidate-source"
$controlImport = Join-Path $operationRoot "candidate-control"
Import-CandidateBundle $sourceBundle $sourceImport
$candidateSelector = Join-Path $operationRoot "candidate-selector.json"
Import-CandidateControl $controlBundle $controlImport $candidateSelector
$candidateSelectorValue = Get-Content -Raw -LiteralPath $candidateSelector | ConvertFrom-Json
$candidate = [string]$candidateSelectorValue.candidate_manifest
$candidateValue = Get-Content -Raw -LiteralPath $candidate | ConvertFrom-Json
$repositorySource = Join-Path $sourceImport "repository-source"

if ($Mode -eq "PrepareDraft") {
    if (-not $DraftBundleOutput) { throw "PrepareDraft requires -DraftBundleOutput" }
    $worker = "handoff-prepare"
    $contextRoot = Join-Path $operationRoot "contexts"
    $eventRoot = Join-Path $operationRoot "events"
    Invoke-Native @("python", "scripts/capture_candidate_execution_context.py", "--candidate-manifest", $candidate, "--worker-id", $worker, "--output-root", $contextRoot)
    $context = Join-Path $contextRoot "$worker.json"
    Invoke-CandidateRoute $candidate "H00-PREPARE-DRAFT" $worker $context $eventRoot @(
        "python", "distribution/build_publication_handoff.py", "--draft",
        "--candidate-manifest", $candidate,
        "--candidate-root", $sourceImport,
        "--source-root", $repositorySource,
        "--output-root", $output
    )
    $draft = Join-Path $output ("{0}/{1}/.attempt-{2}.staging" -f $candidateValue.version, $candidateValue.run_id, $candidateValue.attempt)
    Invoke-CandidateRoute $candidate "H00-EXPORT-DRAFT" $worker $context $eventRoot @(
        "python", "distribution/transfer_handoff_draft.py", "export",
        "--draft", $draft,
        "--candidate-manifest", $candidate,
        "--output", ([System.IO.Path]::GetFullPath($DraftBundleOutput))
    )
    Invoke-CandidateRoute $candidate "H00-DISCARD-EXPORTED-DRAFT" $worker $context $eventRoot @(
        "python", "distribution/build_publication_handoff.py", "--discard-draft", $draft
    )
    exit 0
}

if (-not $DraftBundle) { throw "$Mode requires -DraftBundle" }
$draftTransfer = (Resolve-Path -LiteralPath $DraftBundle).Path

if ($Mode -eq "Platform") {
    if (-not $Platform -or -not $ResultId -or -not $PlatformBundleOutput) {
        throw "Platform requires -Platform, -ResultId, and -PlatformBundleOutput"
    }
    $expected = @{ linux = "H01-LINUX"; windows = "H01-WINDOWS"; macos = "H01-MACOS" }
    if ($expected[$Platform] -ne $ResultId) { throw "platform and result ID disagree" }
    $bundleRoot = [System.IO.Path]::GetFullPath($PlatformBundleOutput)
    if (Test-Path -LiteralPath $bundleRoot) { throw "platform bundle output already exists: $bundleRoot" }
    New-Item -ItemType Directory -Path $bundleRoot | Out-Null
    $draft = Join-Path $operationRoot "draft"
    Invoke-Native @("python", "distribution/transfer_handoff_draft.py", "import", "--bundle", $draftTransfer, "--candidate-manifest", $candidate, "--output", $draft)
    $worker = "handoff-$Platform"
    $contextRoot = Join-Path $bundleRoot "contexts"
    $eventRoot = Join-Path $bundleRoot "events"
    Invoke-Native @("python", "scripts/capture_candidate_execution_context.py", "--candidate-manifest", $candidate, "--worker-id", $worker, "--output-root", $contextRoot)
    $context = Join-Path $contextRoot "$worker.json"
    Invoke-Native @(
        "python", "scripts/release_candidate_command.py", "run",
        "--candidate-manifest", $candidate, "--route-id", $ResultId,
        "--worker-id", $worker, "--context", $context, "--event-root", $eventRoot,
        "--portable-root", $bundleRoot, "--",
        "python", "distribution/verify_publication_handoff.py",
        "--handoff", $draft,
        "--candidate-manifest", $candidate,
        "--candidate-root", $sourceImport,
        "--source-root", $repositorySource,
        "--draft-pre-ready",
        "--platform", $Platform,
        "--worker-id", $worker,
        "--result-id", $ResultId,
        "--output", (Join-Path $bundleRoot "$ResultId.json")
    )
    exit 0
}

if (-not $PlatformBundlesRoot) { throw "Finalize requires -PlatformBundlesRoot" }
$platformBundles = (Resolve-Path -LiteralPath $PlatformBundlesRoot).Path
$staging = Join-Path $output ("{0}/{1}/.attempt-{2}.staging" -f $candidateValue.version, $candidateValue.run_id, $candidateValue.attempt)
Invoke-Native @("python", "distribution/transfer_handoff_draft.py", "import", "--bundle", $draftTransfer, "--candidate-manifest", $candidate, "--output", $staging)
$worker = "handoff-finalize"
$contextRoot = Join-Path $sourceImport "contexts"
$eventRoot = Join-Path $sourceImport "events"
Invoke-Native @("python", "scripts/capture_candidate_execution_context.py", "--candidate-manifest", $candidate, "--worker-id", $worker, "--output-root", $contextRoot)
$context = Join-Path $contextRoot "$worker.json"
Invoke-CandidateRoute $candidate "H01-MERGE" $worker $context $eventRoot @(
    "python", "distribution/merge_handoff_platform_results.py",
    "--candidate-manifest", $candidate,
    "--bundles-root", $platformBundles,
    "--evidence-root", (Join-Path $sourceImport "evidence"),
    "--event-root", $eventRoot,
    "--context-root", $contextRoot,
    "--base-evidence-index", (Join-Path $sourceImport "EVIDENCE_INDEX.json")
)
Invoke-CandidateRoute $candidate "H01-REFRESH" $worker $context $eventRoot @(
    "python", "distribution/build_publication_handoff.py", "--refresh-evidence", $staging,
    "--candidate-manifest", $candidate,
    "--candidate-root", $sourceImport,
    "--evidence-index", (Join-Path $sourceImport "evidence/EVIDENCE_INDEX.json"),
    "--no-remote-evidence", (Join-Path $sourceImport "NO_REMOTE_PUBLICATION.json")
)
Invoke-CandidateRoute $candidate "H02-DRAFT-SEMANTIC" $worker $context $eventRoot @(
    "python", "distribution/verify_publication_handoff.py", "--handoff", $staging,
    "--candidate-manifest", $candidate, "--candidate-root", $sourceImport, "--source-root", $repositorySource,
    "--draft-pre-ready", "--result-id", "H02-DRAFT-SEMANTIC", "--output", (Join-Path $sourceImport "evidence/H02-DRAFT-SEMANTIC.json")
)
Invoke-CandidateRoute $candidate "H03-DRAFT-NO-REMOTE" $worker $context $eventRoot @(
    "python", "scripts/no_remote_publication_guard.py", "--candidate-manifest", $candidate, "--phase", "6",
    "--audit-point", "handoff-draft",
    "--result-id", "H03-DRAFT-NO-REMOTE", "--gate-stage", "final-handoff",
    "--context-root", $contextRoot, "--event-root", $eventRoot, "--handoff", $staging,
    "--output", (Join-Path $sourceImport "evidence/H03-DRAFT-NO-REMOTE.json")
)
Invoke-CandidateRoute $candidate "H00-FINALIZE" $worker $context $eventRoot @(
    "python", "distribution/build_publication_handoff.py", "--finalize", $staging
)
$final = Join-Path $output ("{0}/{1}/attempt-{2}" -f $candidateValue.version, $candidateValue.run_id, $candidateValue.attempt)
Invoke-CandidateRoute $candidate "H04-FINAL-SEMANTIC" $worker $context $eventRoot @(
    "python", "distribution/verify_publication_handoff.py", "--handoff", $final,
    "--candidate-manifest", $candidate, "--candidate-root", $sourceImport, "--source-root", $repositorySource,
    "--final-pre-ready", "--final-read-only", "--result-id", "H04-FINAL-SEMANTIC", "--output", (Join-Path $sourceImport "evidence/H04-FINAL-SEMANTIC.json")
)
Invoke-CandidateRoute $candidate "H05-FINAL-NO-REMOTE" $worker $context $eventRoot @(
    "python", "scripts/no_remote_publication_guard.py", "--candidate-manifest", $candidate, "--phase", "6",
    "--audit-point", "handoff-final",
    "--result-id", "H05-FINAL-NO-REMOTE", "--gate-stage", "final-handoff",
    "--context-root", $contextRoot, "--event-root", $eventRoot, "--handoff", $final,
    "--output", (Join-Path $sourceImport "evidence/H05-FINAL-NO-REMOTE.json")
)
$finalEvidence = Join-Path $sourceImport "evidence/FINAL_HANDOFF_EVIDENCE.json"
Invoke-CandidateRoute $candidate "H06-FINAL-EVIDENCE" $worker $context $eventRoot @(
    "python", "scripts/release_evidence_guard.py", "--candidate-manifest", $candidate,
    "--result-root", (Join-Path $sourceImport "evidence"), "--phase", "6",
    "--gate-stage", "final-handoff",
    "--require-result-ids", "H01-LINUX,H01-WINDOWS,H01-MACOS,H02-DRAFT-SEMANTIC,H03-DRAFT-NO-REMOTE,H04-FINAL-SEMANTIC,H05-FINAL-NO-REMOTE",
    "--event-root", $eventRoot, "--context-root", $contextRoot,
    "--no-remote-evidence", (Join-Path $sourceImport "evidence/H05-FINAL-NO-REMOTE.json"),
    "--output", $finalEvidence
)
$retainedCandidate = Join-Path ([string]$candidateValue.candidate_root) "CANDIDATE_RUN.json"
$retainedRoot = Split-Path -Parent $retainedCandidate
$retainedContextRoot = Join-Path $retainedRoot "contexts"
$retainedEventRoot = Join-Path $retainedRoot "events"
Invoke-Native @("python", "scripts/capture_candidate_execution_context.py", "--candidate-manifest", $retainedCandidate, "--worker-id", $worker, "--output-root", $retainedContextRoot)
$retainedContext = Join-Path $retainedContextRoot "$worker.json"
Invoke-CandidateRoute $retainedCandidate "H06-PUBLICATION-READY" $worker $retainedContext $retainedEventRoot @(
    "python", "scripts/release_lifecycle_guard.py", "--candidate-manifest", $retainedCandidate,
    "--transition", "publication-ready", "--phase", "6", "--handoff-ready",
    "--gate-evidence", $finalEvidence,
    "--handoff-evidence-root", (Join-Path $sourceImport "evidence"),
    "--current-route-id", "H06-PUBLICATION-READY", "--publication-marker", (Join-Path $final "PUBLICATION_READY.json")
)
$retainedValue = Get-Content -Raw -LiteralPath $retainedCandidate | ConvertFrom-Json
$candidateControlOutput = Join-Path $retainedRoot "transfer/CANDIDATE_CONTROL_BUNDLE.tar"
Invoke-Native @("python", "distribution/transfer_candidate.py", "export-build-control", "--candidate-manifest", $retainedCandidate, "--output", $candidateControlOutput)
$seriesControlOutput = Join-Path (Split-Path -Parent ([string]$retainedValue.series_manifest)) "RELEASE_SERIES_CONTROL_BUNDLE.tar"
Invoke-Native @("python", "distribution/release_series.py", "export-control", "--series-manifest", ([string]$retainedValue.series_manifest), "--output", $seriesControlOutput)
$portableControlRoot = Join-Path $output "terminal-control"
New-Item -ItemType Directory -Path (Join-Path $portableControlRoot "candidate"), (Join-Path $portableControlRoot "series") | Out-Null
Copy-Item -LiteralPath $candidateControlOutput -Destination (Join-Path $portableControlRoot "candidate/CANDIDATE_CONTROL_BUNDLE.tar")
Copy-Item -LiteralPath $seriesControlOutput -Destination (Join-Path $portableControlRoot "series/RELEASE_SERIES_CONTROL_BUNDLE.tar")
exit 0

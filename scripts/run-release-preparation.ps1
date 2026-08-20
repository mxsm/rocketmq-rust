# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("PrepareCommon", "Target", "Aggregate")]
    [string]$Mode,
    [Parameter(Mandatory = $true)]
    [string]$CandidateManifest,
    [string]$CommonInputsBundleOutput,
    [string]$BuildSourceBundleOutput,
    [string]$BuildControlBundleOutput,
    [string]$CommonInputsBundle,
    [string]$BuildSourceBundle,
    [string]$BuildControlBundle,
    [ValidateSet("x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc", "x86_64-apple-darwin")]
    [string]$Target,
    [string]$TargetBundleOutput,
    [string]$TargetBundlesRoot,
    [string]$CandidateSourceBundleOutput,
    [string]$SourceRoot
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

# The shared engine owns release_candidate_command.py, no_remote_publication_guard.py,
# and release_evidence_guard.py so both platform wrappers have identical semantics.
$arguments = @(
    "scripts/release_preparation.py",
    "--mode", $Mode,
    "--candidate-manifest", $CandidateManifest
)
$optional = @{
    "--common-inputs-bundle-output" = $CommonInputsBundleOutput
    "--build-source-bundle-output" = $BuildSourceBundleOutput
    "--build-control-bundle-output" = $BuildControlBundleOutput
    "--common-inputs-bundle" = $CommonInputsBundle
    "--build-source-bundle" = $BuildSourceBundle
    "--build-control-bundle" = $BuildControlBundle
    "--target" = $Target
    "--target-bundle-output" = $TargetBundleOutput
    "--target-bundles-root" = $TargetBundlesRoot
    "--candidate-source-bundle-output" = $CandidateSourceBundleOutput
    "--source-root" = $SourceRoot
}
foreach ($name in $optional.Keys) {
    if ($optional[$name]) {
        $arguments += @($name, $optional[$name])
    }
}

python @arguments
exit $LASTEXITCODE

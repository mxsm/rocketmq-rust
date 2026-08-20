# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding(DefaultParameterSetName = 'Profile')]
param(
    [Parameter(Mandatory = $true)]
    [string]$CandidateManifest,
    [Parameter(Mandatory = $true, ParameterSetName = 'Profile')]
    [string]$Profile,
    [Parameter(Mandatory = $true, ParameterSetName = 'Scenario')]
    [string]$Scenario,
    [Parameter(Mandatory = $true, ParameterSetName = 'All')]
    [switch]$AllScenarios,
    [string]$Matrix = 'scripts/v1-functional-test-matrix.json',
    [string]$Target
)

$ErrorActionPreference = 'Stop'
$arguments = @('scripts/v1_functional_acceptance.py', '--candidate-manifest', $CandidateManifest, '--matrix', $Matrix)
if ($Target) { $arguments += @('--target', $Target) }
switch ($PSCmdlet.ParameterSetName) {
    'Profile' { $arguments += @('--profile', $Profile) }
    'Scenario' { $arguments += @('--scenario', $Scenario) }
    'All' { $arguments += '--all-scenarios' }
}
& python @arguments
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

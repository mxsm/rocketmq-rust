# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0

[CmdletBinding()]
param(
    [ValidateSet('Both', 'JavaMasterRustSlave', 'RustMasterJavaSlave')]
    [string]$Direction = 'Both',
    [ValidateRange(1, 600)]
    [int]$DurationSeconds = 90,
    [string]$Scenario,
    [string]$JavaRoot = 'D:\Github\Java\rocketmq',
    [string]$RustRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path,
    [string]$Output = (Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path 'target\default-ha-interop'),
    [string]$CaseDriver,
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
$runner = Join-Path $PSScriptRoot 'run_default_ha_interop.py'
$arguments = @(
    $runner,
    '--direction', $Direction,
    '--duration-seconds', $DurationSeconds,
    '--java-root', $JavaRoot,
    '--rust-root', $RustRoot,
    '--output', $Output
)
if ($Scenario) { $arguments += @('--scenario', $Scenario) }
if ($CaseDriver) { $arguments += @('--case-driver', $CaseDriver) }
if ($DryRun) { $arguments += '--dry-run' }

& python @arguments
if ($LASTEXITCODE -ne 0) {
    throw "DefaultHA interoperability runner failed with exit code $LASTEXITCODE"
}

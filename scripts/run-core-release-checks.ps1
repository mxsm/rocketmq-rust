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
    [switch]$List
)

$arguments = @("scripts/core_release_checks.py", "--phase", "$Phase", "--run-id", $RunId, "--attempt", "$Attempt")
if ($Version) { $arguments += @("--version", $Version) }
if ($IncludeRepoGlobal) { $arguments += "--include-repo-global" }
if ($List) { $arguments += "--list" }
python @arguments
exit $LASTEXITCODE

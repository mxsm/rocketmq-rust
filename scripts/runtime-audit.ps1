[CmdletBinding()]
param(
    [string]$OutputDirectory = "target/runtime-audit",
    [ValidateSet("core-release", "all")]
    [string]$Scope = "all"
)

$ErrorActionPreference = "Stop"
& python (Join-Path $PSScriptRoot "runtime_audit.py") --output $OutputDirectory --scope $Scope
exit $LASTEXITCODE

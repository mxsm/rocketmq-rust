# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidatePattern('^[a-z0-9][a-z0-9-]{0,39}$')]
    [string]$ClusterName = 'rocketmq-sre-phase00',

    [ValidateSet('D', 'F')]
    [string]$EvidenceDrive = 'D',

    [switch]$ValidateOnly
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$kindSmoke = Join-Path $scriptDirectory 'phase01-kind-smoke.ps1'
$kindRunner = Join-Path $scriptDirectory 'kind.ps1'
$checker = Join-Path $scriptDirectory 'check_live_conversation_qualification.py'
$test = Join-Path $scriptDirectory 'tests/test_check_live_conversation_qualification.py'
$timestamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$evidenceRoot = "${EvidenceDrive}:\rocketmq-sre-evidence\live-conversations\$timestamp"
$report = Join-Path $evidenceRoot 'report.json'
$env:PYTHONDONTWRITEBYTECODE = '1'

function Invoke-Checked([string]$Command, [string[]]$Arguments) {
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Command failed with exit code $LASTEXITCODE."
    }
}

Invoke-Checked python @($checker)
Invoke-Checked python @($test, '-v')
& $kindSmoke -ValidateOnly
if (-not $?) {
    throw 'Conversation Kind static validation failed.'
}
if ($ValidateOnly) {
    Write-Host 'LIVE_CONVERSATION_QUALIFICATION_STATIC_OK streams=2 read_only=true'
    return
}

$status = (& git -C $repositoryRoot status --porcelain | Out-String).Trim()
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to inspect the candidate Git source.'
}
if (-not [string]::IsNullOrWhiteSpace($status)) {
    throw 'Live Conversation qualification requires a clean candidate worktree.'
}

New-Item -ItemType Directory -Path $evidenceRoot -Force | Out-Null
$clusterAttempted = $false
try {
    $clusterAttempted = $true
    & $kindRunner -Action Up -ClusterName $ClusterName
    if (-not $?) {
        throw 'Live Conversation Kind environment could not start.'
    }
    & $kindSmoke `
        -ClusterName $ClusterName `
        -SkipPhase00Parity `
        -ConversationQualificationReport $report
    if (-not $?) {
        throw 'Live Conversation Kind qualification failed.'
    }
    Invoke-Checked python @($checker, '--report', $report)
}
finally {
    if ($clusterAttempted) {
        & $kindRunner -Action Down -ClusterName $ClusterName
        if (-not $?) {
            throw 'Live Conversation Kind cluster cleanup failed.'
        }
    }
}
Write-Host "LIVE_CONVERSATION_QUALIFICATION_OK report=$report"

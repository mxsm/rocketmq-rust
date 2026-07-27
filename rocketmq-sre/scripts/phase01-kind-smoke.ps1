# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidatePattern('^[a-z0-9][a-z0-9-]{0,39}$')]
    [string]$ClusterName = 'rocketmq-sre-phase00',

    [switch]$SkipPhase00Parity,

    [switch]$ValidateOnly
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$probeManifest = Join-Path $sreRoot 'deploy/kind/phase01-live-probe-job.yaml'
$phase01Smoke = Join-Path $scriptDirectory 'phase01-smoke.ps1'
$kindRunner = Join-Path $scriptDirectory 'kind.ps1'
$sreStack = Join-Path $sreRoot 'deploy/kind/sre-stack.yaml'
$networkPolicy = Join-Path $sreRoot 'deploy/kind/control-plane-network-policy.yaml'
$dockerfile = Join-Path $sreRoot 'deploy/docker/Dockerfile'
$thisScript = $MyInvocation.MyCommand.Path

function Assert-PowerShellSyntax([string]$Path) {
    $tokens = $null
    $parseErrors = $null
    [Management.Automation.Language.Parser]::ParseFile(
        $Path,
        [ref]$tokens,
        [ref]$parseErrors
    ) | Out-Null
    if ($parseErrors.Count -gt 0) {
        $messages = $parseErrors | ForEach-Object { $_.Message }
        throw "PowerShell syntax validation failed for '$Path': $($messages -join '; ')"
    }
}

function Assert-StaticContract {
    foreach ($script in @($thisScript, $phase01Smoke, $kindRunner)) {
        Assert-PowerShellSyntax $script
    }

    $manifest = Get-Content -Raw -LiteralPath $probeManifest
    $kindRunnerText = Get-Content -Raw -LiteralPath $kindRunner
    $smokeText = Get-Content -Raw -LiteralPath $phase01Smoke
    $sreStackText = Get-Content -Raw -LiteralPath $sreStack
    $networkPolicyText = Get-Content -Raw -LiteralPath $networkPolicy
    $dockerfileText = Get-Content -Raw -LiteralPath $dockerfile
    foreach ($contract in @(
        @{ Text = $manifest; Value = 'automountServiceAccountToken: false' }
        @{ Text = $manifest; Value = 'rocketmq.apache.org/sre-probe: "true"' }
        @{ Text = $manifest; Value = 'rocketmq-rust/fault-driver:local' }
        @{ Text = $manifest; Value = 'rocketmq-rust/sre-probe:phase00-local' }
        @{ Text = $manifest; Value = 'ROCKETMQ_SRE_PROBE_MAX_MESSAGES, value: "10"' }
        @{ Text = $manifest; Value = 'ROCKETMQ_SRE_PROBE_PAYLOAD_BYTES, value: "64"' }
        @{ Text = $manifest; Value = 'ROCKETMQ_SRE_PROBE_DURATION_SECONDS, value: "60"' }
        @{ Text = $kindRunnerText; Value = "'sre-probe' = 'rocketmq-rust/sre-probe:phase00-local'" }
        @{ Text = $kindRunnerText; Value = "'sre-model-mock' = 'rocketmq-rust/sre-model-mock:phase00-local'" }
        @{ Text = $kindRunnerText; Value = "'fault-driver' = 'rocketmq-rust/fault-driver:local'" }
        @{ Text = $kindRunnerText; Value = 'VITE_SRE_AUTH_MODE=development' }
        @{ Text = $kindRunnerText; Value = 'VITE_SRE_DEV_TENANT=00000000-0000-4000-8000-000000000002' }
        @{ Text = $kindRunnerText; Value = 'VITE_SRE_DEV_CLUSTERS=00000000-0000-4000-8000-000000000001' }
        @{ Text = $smokeText; Value = "[ValidateSet('Compose', 'Kind')]" }
        @{ Text = $smokeText; Value = 'Assert-ReadOnlyCapabilityBoundary' }
        @{ Text = $smokeText; Value = 'Assert-CrossClusterDenied' }
        @{ Text = $smokeText; Value = 'Wait-ConnectorOnline' }
        @{ Text = $smokeText; Value = '"/v1/evidence/$evidenceId/content"' }
        @{ Text = $smokeText; Value = 'positive live Consumer Lag Evidence returned through the mTLS Connector channel' }
        @{ Text = $smokeText; Value = "mode -ne 'model_assisted'" }
        @{ Text = $smokeText; Value = 'Persisted model provider lineage is incomplete' }
        @{ Text = $smokeText; Value = 'mutation_calls=0 executor_calls=0' }
        @{ Text = $sreStackText; Value = 'name: sre-model-mock' }
        @{ Text = $sreStackText; Value = 'ROCKETMQ_SRE_MODEL_LOCAL_ENDPOINT, value: "http://sre-model-mock:8094/v1"' }
        @{ Text = $sreStackText; Value = 'ROCKETMQ_SRE_MODEL_SECRET_PROVIDER, value: "none"' }
        @{ Text = $networkPolicyText; Value = 'name: sre-model-mock-isolation' }
        @{ Text = $networkPolicyText; Value = 'egress: []' }
        @{ Text = $dockerfileText; Value = 'FROM runtime-base AS model-mock' }
    )) {
        if ($contract.Text.IndexOf($contract.Value, [StringComparison]::Ordinal) -lt 0) {
            throw "Phase 01 Kind static contract is missing '$($contract.Value)'."
        }
    }

    foreach ($forbidden in @(
        'hostNetwork: true',
        'ROCKETMQ_SRE_INTERNAL_TOKEN',
        'mcp-token',
        'execution-agent',
        'executor'
    )) {
        if ($manifest.IndexOf($forbidden, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            throw "Phase 01 Kind probe manifest contains forbidden capability '$forbidden'."
        }
    }
    foreach ($forbidden in @(
        '/internal/v1/evidence/query',
        '/internal/v1/capabilities',
        'service/rocketmq-sre-connector'
    )) {
        if ($smokeText.IndexOf($forbidden, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            throw "Phase 01 live smoke bypasses the Control Plane reverse channel via '$forbidden'."
        }
    }

    Write-Host 'PHASE01_KIND_STATIC_OK bounded_probe=true mutation_tools=false'
}

Assert-StaticContract
if ($ValidateOnly) {
    return
}

if (-not $SkipPhase00Parity) {
    & $kindRunner -Action Smoke -ClusterName $ClusterName
    if (-not $?) {
        throw 'Phase 00 Kind deployment-parity smoke failed.'
    }
}

& $phase01Smoke -Target Kind -ClusterName $ClusterName
if (-not $?) {
    throw 'Phase 01 Kind live smoke failed.'
}

Write-Host "PHASE01_KIND_E2E_OK cluster=$ClusterName read_only=true"

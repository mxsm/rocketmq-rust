# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

param(
    [Alias("h")]
    [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($Help) {
    @"
Runs the ignored RocketMQ MCP Control real-cluster E2E test.

The runner builds the repository's Rust NameServer and Broker binaries, starts
them only through the test harness on dynamically reserved loopback ports, and
runs the TLS Streamable HTTP mutation scenario serially. The harness owns and
reaps its child processes and removes its unique temporary resource root.

Usage:
  .\scripts\run_real_cluster_e2e.ps1
  .\scripts\run_real_cluster_e2e.ps1 -Help
"@ | Write-Host
    exit 0
}

$projectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$repositoryRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..\..\..")).Path
$repositoryManifest = Join-Path $repositoryRoot "Cargo.toml"
$controlManifest = Join-Path $projectRoot "Cargo.toml"
$testName = "transport::real_cluster_e2e::real_cluster_tls_mcp_mutations_restore_and_reap_every_owned_resource"

Write-Host "Building the owned Rust NameServer and Broker test processes..."
& cargo build --locked --manifest-path $repositoryManifest -p rocketmq-namesrv --bin rocketmq-namesrv-rust
if ($LASTEXITCODE -ne 0) {
    throw "NameServer build failed with exit code $LASTEXITCODE"
}
& cargo build --locked --manifest-path $repositoryManifest -p rocketmq-broker --bin rocketmq-broker-rust
if ($LASTEXITCODE -ne 0) {
    throw "Broker build failed with exit code $LASTEXITCODE"
}

$metadataText = & cargo metadata --locked --manifest-path $repositoryManifest --format-version 1 --no-deps
if ($LASTEXITCODE -ne 0) {
    throw "Cargo metadata failed with exit code $LASTEXITCODE"
}
$targetDirectory = ($metadataText | ConvertFrom-Json).target_directory
$executableSuffix = if ($IsWindows) { ".exe" } else { "" }
$debugDirectory = Join-Path $targetDirectory "debug"
$namesrvBinary = Join-Path $debugDirectory "rocketmq-namesrv-rust$executableSuffix"
$brokerBinary = Join-Path $debugDirectory "rocketmq-broker-rust$executableSuffix"
foreach ($binary in @($namesrvBinary, $brokerBinary)) {
    if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
        throw "Expected built test process was not found: $binary"
    }
}

$ownedEnvironment = @(
    "INSTA_UPDATE",
    "RUST_MIN_STACK",
    "ROCKETMQ_MCP_CONTROL_REAL_CLUSTER_E2E",
    "ROCKETMQ_MCP_CONTROL_E2E_NAMESRV_BIN",
    "ROCKETMQ_MCP_CONTROL_E2E_BROKER_BIN"
)
$priorEnvironment = @{}
foreach ($name in $ownedEnvironment) {
    $priorEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}

try {
    $env:INSTA_UPDATE = "no"
    Remove-Item Env:RUST_MIN_STACK -ErrorAction SilentlyContinue
    $env:ROCKETMQ_MCP_CONTROL_REAL_CLUSTER_E2E = "1"
    $env:ROCKETMQ_MCP_CONTROL_E2E_NAMESRV_BIN = $namesrvBinary
    $env:ROCKETMQ_MCP_CONTROL_E2E_BROKER_BIN = $brokerBinary

    Write-Host "Running the ignored real-cluster E2E test serially..."
    & cargo test --locked --manifest-path $controlManifest --features write-tools --lib $testName -- --ignored --exact --test-threads=1 --nocapture
    if ($LASTEXITCODE -ne 0) {
        throw "Real-cluster E2E test failed with exit code $LASTEXITCODE"
    }
}
finally {
    foreach ($name in $ownedEnvironment) {
        $value = $priorEnvironment[$name]
        if ($null -eq $value) {
            Remove-Item "Env:$name" -ErrorAction SilentlyContinue
        }
        else {
            Set-Item "Env:$name" $value
        }
    }
}

Write-Host "Real-cluster E2E test completed; owned resources were restored and reaped."

# Copyright 2023 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$JavaRepo
)

$ErrorActionPreference = 'Stop'
$expectedCommit = '2daf0e2ca91a1592d18235d43e5d709d1c35d15f'
$scriptRoot = Split-Path -Parent $PSCommandPath
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $scriptRoot '..\..')).Path
$resolvedJavaRepo = (Resolve-Path -LiteralPath $JavaRepo).Path
$actualCommit = (& git -C $resolvedJavaRepo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $actualCommit -ne $expectedCommit) {
    throw "Java HEAD must be $expectedCommit; found $actualCommit"
}
if ((& git -C $resolvedJavaRepo status --short) -join "`n") {
    throw 'Cross-language release verification requires a clean Java worktree'
}

$output = Join-Path $repoRoot 'target\request-header-codec-contract\rust-golden'
New-Item -ItemType Directory -Force -Path $output | Out-Null
$previousOutput = $env:ROCKETMQ_RUST_HEADER_GOLDEN_OUTPUT
try {
    $env:ROCKETMQ_RUST_HEADER_GOLDEN_OUTPUT = $output
    & cargo test -p rocketmq-protocol --test request_header_java_compatibility `
        rust_production_frames_preserve_the_java_canonical_maps -- --exact
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to generate Rust request-header frames'
    }
} finally {
    $env:ROCKETMQ_RUST_HEADER_GOLDEN_OUTPUT = $previousOutput
}

$mapping = Join-Path $scriptRoot 'header-class-map.json'
$fixtures = Join-Path $repoRoot 'rocketmq-protocol\tests\fixtures\request_header_codec'
$harnessPom = Join-Path $scriptRoot 'java-harness\pom.xml'
& mvn -f $harnessPom compile exec:java `
    '-Dexec.mainClass=org.apache.rocketmq.headercodec.GoldenFixtureVerifier' `
    "-Dheader.mapping=$mapping" `
    "-Dheader.fixtureDirectory=$fixtures" `
    "-Dheader.rustFrameDirectory=$output"
if ($LASTEXITCODE -ne 0) {
    throw 'Pinned Java failed to verify Rust request-header frames'
}

Write-Output 'Java-to-Rust and Rust-to-Java request-header fixtures passed'

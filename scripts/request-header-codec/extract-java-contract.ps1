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
    [string]$JavaRepo,

    [Parameter(Mandatory = $true)]
    [string]$Output,

    [switch]$DiagnosticAllowDirty
)

$ErrorActionPreference = 'Stop'
$expectedCommit = '2daf0e2ca91a1592d18235d43e5d709d1c35d15f'
$scriptRoot = Split-Path -Parent $PSCommandPath
$mapping = Join-Path $scriptRoot 'header-class-map.json'
$overrides = Join-Path $scriptRoot 'schema-overrides.json'
$goldenInputs = Join-Path $scriptRoot 'golden-inputs-v1.json'
$harnessPom = Join-Path $scriptRoot 'java-harness\pom.xml'
$resolvedJavaRepo = (Resolve-Path -LiteralPath $JavaRepo).Path
$resolvedOutput = [System.IO.Path]::GetFullPath($Output)

$actualCommit = (& git -C $resolvedJavaRepo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $actualCommit -ne $expectedCommit) {
    throw "Java HEAD must be $expectedCommit; found $actualCommit"
}

$dirty = (& git -C $resolvedJavaRepo status --short) -join "`n"
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to inspect the Java worktree'
}
if ($dirty -and -not $DiagnosticAllowDirty) {
    throw 'Release contract extraction requires a clean Java worktree'
}

New-Item -ItemType Directory -Force -Path $resolvedOutput | Out-Null

& mvn -f (Join-Path $resolvedJavaRepo 'pom.xml') `
    -pl remoting -am install `
    '-Dmaven.test.skip=true' `
    '-Dcheckstyle.skip=true' `
    '-Drat.skip=true' `
    '-Dspotbugs.skip=true' `
    '-Dspotless.check.skip=true'
if ($LASTEXITCODE -ne 0) {
    throw 'Failed to build the pinned RocketMQ Java remoting artifact'
}

$schemaOutput = Join-Path $resolvedOutput 'java-schema.json'
& mvn -f $harnessPom compile exec:java `
    '-Dexec.mainClass=org.apache.rocketmq.headercodec.HeaderContractExtractor' `
    "-Dheader.mapping=$mapping" `
    "-Dheader.overrides=$overrides" `
    "-Dheader.output=$schemaOutput" `
    "-Dheader.javaCommit=$expectedCommit"
if ($LASTEXITCODE -ne 0) {
    throw 'Failed to extract the Java request-header schema'
}

$goldenOutput = Join-Path $resolvedOutput 'golden'
New-Item -ItemType Directory -Force -Path $goldenOutput | Out-Null
& mvn -f $harnessPom compile exec:java `
    '-Dexec.mainClass=org.apache.rocketmq.headercodec.GoldenFixtureGenerator' `
    "-Dheader.mapping=$mapping" `
    "-Dheader.goldenInputs=$goldenInputs" `
    "-Dheader.goldenOutput=$goldenOutput" `
    "-Dheader.javaCommit=$expectedCommit"
if ($LASTEXITCODE -ne 0) {
    throw 'Failed to generate Java request-header golden fixtures'
}

$evidence = [ordered]@{
    schemaVersion = 1
    javaCommit = $actualCommit
    dirty = [bool]$dirty
    releasable = -not [bool]$dirty
    schema = 'java-schema.json'
    goldenIndex = 'golden/index.json'
}
$evidenceJson = ($evidence | ConvertTo-Json -Depth 5) + "`n"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText(
    (Join-Path $resolvedOutput 'extractor-evidence.json'),
    $evidenceJson,
    $utf8NoBom
)

Write-Output "Java request-header contract extracted to $resolvedOutput"

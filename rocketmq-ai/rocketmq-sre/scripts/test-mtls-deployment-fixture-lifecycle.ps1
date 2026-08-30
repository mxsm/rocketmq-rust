# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$targetDirectory = [IO.Path]::GetFullPath((Join-Path $repositoryRoot 'target'))
$fixtureDirectory = [IO.Path]::GetFullPath((Join-Path $targetDirectory 'phase00-certs'))
$composeDirectory = Join-Path $sreRoot 'deploy/dev'
$composeFile = Join-Path $composeDirectory 'compose.yaml'
$validator = Join-Path $scriptDirectory 'verify-mtls-deployment.ps1'
$preservedFixture = Join-Path $fixtureDirectory 'admin-read.env'
$preservedContent = @(
    'ROCKETMQ_SRE_ADMIN_ACCESS_KEY=preserved-validation-only-denied'
    'ROCKETMQ_SRE_ADMIN_SECRET_KEY=preserved-validation-only-denied'
    ''
) -join "`n"
$transientFixtures = @('agent-broker.env', 'probe.env', 'bootstrap.env')
$fixtureDirectoryCreated = $false
$preservedFixtureCreated = $false

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found."
    }
}

function Assert-FixtureDirectoryBoundary {
    $targetPrefix = $targetDirectory + [IO.Path]::DirectorySeparatorChar
    if (-not $fixtureDirectory.StartsWith($targetPrefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Test fixture output escaped the repository target directory.'
    }
}

Assert-FixtureDirectoryBoundary
Require-Command docker

if (Test-Path -LiteralPath $fixtureDirectory) {
    throw "Fixture lifecycle test requires a clean directory: $fixtureDirectory"
}

try {
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & docker @(
            'compose',
            '--project-directory', $composeDirectory,
            '--file', $composeFile,
            '--profile', 'observability',
            'config', '--quiet'
        ) *> $null
        $bareComposeExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($bareComposeExitCode -eq 0) {
        throw 'Bare Compose validation unexpectedly succeeded without required env fixtures.'
    }

    & $validator
    if ($LASTEXITCODE -ne 0) {
        throw "Self-contained deployment validation failed with exit code $LASTEXITCODE."
    }
    if (Test-Path -LiteralPath $fixtureDirectory) {
        throw 'Self-contained deployment validation did not remove its temporary directory.'
    }

    New-Item -ItemType Directory -Path $fixtureDirectory | Out-Null
    $fixtureDirectoryCreated = $true
    [IO.File]::WriteAllText($preservedFixture, $preservedContent, [Text.UTF8Encoding]::new($false))
    $preservedFixtureCreated = $true

    & $validator
    if ($LASTEXITCODE -ne 0) {
        throw "Deployment validation with a preserved fixture failed with exit code $LASTEXITCODE."
    }
    if ([IO.File]::ReadAllText($preservedFixture) -cne $preservedContent) {
        throw 'Deployment validation changed a pre-existing fixture.'
    }
    foreach ($fixture in $transientFixtures) {
        if (Test-Path -LiteralPath (Join-Path $fixtureDirectory $fixture)) {
            throw "Deployment validation retained a transient fixture: $fixture"
        }
    }
}
finally {
    if ($preservedFixtureCreated -and (Test-Path -LiteralPath $preservedFixture -PathType Leaf)) {
        Remove-Item -LiteralPath $preservedFixture -Force
    }
    if ($fixtureDirectoryCreated -and
        (Test-Path -LiteralPath $fixtureDirectory -PathType Container) -and
        -not (Get-ChildItem -LiteralPath $fixtureDirectory -Force | Select-Object -First 1)) {
        Remove-Item -LiteralPath $fixtureDirectory -Force
    }
}

Write-Host 'Deployment validation fixture lifecycle is correct.'

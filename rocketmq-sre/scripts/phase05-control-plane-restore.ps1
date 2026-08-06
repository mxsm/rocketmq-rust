# Copyright 2026 The RocketMQ Rust Authors
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
    [string]$PostgresContainer = 'rocketmq-rust-ai-sre-phase00-postgres-1',
    [string]$DatabaseUrl = '',
    [string]$SourceDatabase = 'rocketmq_sre',
    [string]$DatabaseUser = 'rocketmq_sre',
    [string]$DatabasePassword = 'rocketmq_sre',
    [int]$PublicPort = 18091,
    [int]$ConnectorPort = 18093,
    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',
    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',
    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',
    [string]$EvidenceOutput = 'D:\BuildCache\rocketmq-sre-temp\phase05-control-plane-restore.json'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$manifestPath = Join-Path $sreRoot 'Cargo.toml'
$controlPlaneExecutable = Join-Path $CargoTargetDir 'debug\rocketmq-sre-control-plane.exe'
$restoreDatabase = "rocketmq_sre_restore_$([Guid]::NewGuid().ToString('N').Substring(0, 12))"
$dumpPath = "/tmp/$restoreDatabase.dump"
$runtimeDirectory = Join-Path $TemporaryRoot $restoreDatabase
$stdoutPath = Join-Path $runtimeDirectory 'control-plane.stdout.log'
$stderrPath = Join-Path $runtimeDirectory 'control-plane.stderr.log'
$process = $null
$restoreCreated = $false
$exerciseStartedAt = [DateTimeOffset]::UtcNow
$restoreStartedAt = $null
$databaseHost = '127.0.0.1'
$databasePort = 5432

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Description
    )

    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

function Invoke-PostgresScalar {
    param(
        [Parameter(Mandatory = $true)][string]$Database,
        [Parameter(Mandatory = $true)][string]$Query
    )

    $value = & docker exec $PostgresContainer psql `
        --username $DatabaseUser `
        --dbname $Database `
        --tuples-only `
        --no-align `
        --command $Query
    if ($LASTEXITCODE -ne 0) {
        throw "PostgreSQL query failed for database '$Database'."
    }
    ($value | Select-Object -Last 1).Trim()
}

function Assert-SafeIdentifier([string]$Value, [string]$Description) {
    if ($Value -notmatch '^[A-Za-z][A-Za-z0-9_]{0,62}$') {
        throw "$Description must be a bounded PostgreSQL identifier."
    }
}

function Assert-DataPath([string]$Path, [string]$Description) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetPathRoot($fullPath)
    if (
        -not $root.Equals('D:\', [StringComparison]::OrdinalIgnoreCase) -and
        -not $root.Equals('F:\', [StringComparison]::OrdinalIgnoreCase)
    ) {
        throw "$Description must use the D or F drive."
    }
}

if (-not [string]::IsNullOrWhiteSpace($DatabaseUrl)) {
    try {
        $databaseUri = [Uri]::new($DatabaseUrl)
    }
    catch {
        throw 'DatabaseUrl must be an absolute PostgreSQL URL.'
    }
    if (
        -not $databaseUri.IsAbsoluteUri -or
        @('postgres', 'postgresql') -notcontains $databaseUri.Scheme
    ) {
        throw 'DatabaseUrl must use the postgres or postgresql scheme.'
    }
    if (@('127.0.0.1', 'localhost') -notcontains $databaseUri.Host) {
        throw 'DatabaseUrl must target the loopback-published PostgreSQL container.'
    }
    if (-not [string]::IsNullOrEmpty($databaseUri.Query) -or -not [string]::IsNullOrEmpty($databaseUri.Fragment)) {
        throw 'DatabaseUrl must not contain query or fragment components.'
    }
    $credentialSeparator = $databaseUri.UserInfo.IndexOf(':')
    if ($credentialSeparator -lt 1) {
        throw 'DatabaseUrl must include an explicit user and password.'
    }
    $DatabaseUser = [Uri]::UnescapeDataString($databaseUri.UserInfo.Substring(0, $credentialSeparator))
    $DatabasePassword = [Uri]::UnescapeDataString($databaseUri.UserInfo.Substring($credentialSeparator + 1))
    $SourceDatabase = [Uri]::UnescapeDataString($databaseUri.AbsolutePath.TrimStart('/'))
    $databaseHost = $databaseUri.Host
    $databasePort = if ($databaseUri.Port -eq -1) { 5432 } else { $databaseUri.Port }
}

function Wait-ControlPlaneReady {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(45)
    $healthUri = "http://127.0.0.1:$PublicPort/healthz"
    $readyUri = "http://127.0.0.1:$PublicPort/readyz"
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        if ($null -ne $process -and $process.HasExited) {
            $stderr = if (Test-Path -LiteralPath $stderrPath) {
                (Get-Content -Raw -LiteralPath $stderrPath).Trim()
            }
            else {
                'no stderr was captured'
            }
            throw "Restored Control Plane exited before readiness: $stderr"
        }
        try {
            $health = Invoke-RestMethod -Method Get -Uri $healthUri -TimeoutSec 2
            $ready = Invoke-RestMethod -Method Get -Uri $readyUri -TimeoutSec 2
            if ($health.status -eq 'healthy' -and $ready.ready -eq $true) {
                return $ready
            }
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
    }
    throw 'Restored Control Plane did not become ready within 45 seconds.'
}

foreach ($identifier in @(
    @{ Value = $SourceDatabase; Description = 'source database' },
    @{ Value = $DatabaseUser; Description = 'database user' },
    @{ Value = $restoreDatabase; Description = 'restore database' }
)) {
    Assert-SafeIdentifier $identifier.Value $identifier.Description
}
foreach ($path in @(
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' },
    @{ Value = $EvidenceOutput; Description = 'restore evidence output' }
)) {
    Assert-DataPath $path.Value $path.Description
}
if ($PublicPort -lt 1024 -or $PublicPort -gt 65535 -or $ConnectorPort -lt 1024 -or $ConnectorPort -gt 65535) {
    throw 'Control Plane restore ports must be between 1024 and 65535.'
}
if ($databasePort -lt 1024 -or $databasePort -gt 65535) {
    throw 'DatabaseUrl PostgreSQL port must be between 1024 and 65535.'
}
if ($PublicPort -eq $ConnectorPort) {
    throw 'Control Plane public and Connector ports must differ.'
}

Invoke-Native docker @(
    'inspect',
    '--format', '{{.State.Running}}',
    $PostgresContainer
) 'PostgreSQL container inspection'

$savedEnvironment = @{}
foreach ($name in @(
    'CARGO_HOME',
    'CARGO_TARGET_DIR',
    'TEMP',
    'TMP',
    'DATABASE_URL',
    'ROCKETMQ_SRE_BIND_ADDR',
    'ROCKETMQ_SRE_CONNECTOR_BIND_ADDR',
    'ROCKETMQ_SRE_DATABASE_MAX_CONNECTIONS',
    'ROCKETMQ_SRE_INTERNAL_TOKEN',
    'ROCKETMQ_SRE_DEV_AUTH',
    'ROCKETMQ_SRE_MODEL_ENABLED',
    'ROCKETMQ_SRE_OBJECT_STORE_LOCAL_PATH',
    'RUST_LOG'
)) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

try {
    New-Item -ItemType Directory -Force -Path $runtimeDirectory | Out-Null
    $sourceCounts = [ordered]@{
        migrations = [int64](Invoke-PostgresScalar $SourceDatabase 'SELECT COUNT(*) FROM _sqlx_migrations')
        clusters = [int64](Invoke-PostgresScalar $SourceDatabase 'SELECT COUNT(*) FROM clusters')
        incidents = [int64](Invoke-PostgresScalar $SourceDatabase 'SELECT COUNT(*) FROM sre_incidents')
        fleet_releases = [int64](Invoke-PostgresScalar $SourceDatabase 'SELECT COUNT(*) FROM fleet_releases')
    }

    Invoke-Native docker @(
        'exec', $PostgresContainer,
        'pg_dump',
        '--username', $DatabaseUser,
        '--dbname', $SourceDatabase,
        '--format', 'custom',
        '--no-owner',
        '--file', $dumpPath
    ) 'Control Plane PostgreSQL backup'
    $restoreStartedAt = [DateTimeOffset]::UtcNow
    Invoke-Native docker @(
        'exec', $PostgresContainer,
        'createdb',
        '--username', $DatabaseUser,
        $restoreDatabase
    ) 'isolated restore database creation'
    $restoreCreated = $true
    Invoke-Native docker @(
        'exec', $PostgresContainer,
        'pg_restore',
        '--username', $DatabaseUser,
        '--dbname', $restoreDatabase,
        '--no-owner',
        '--no-privileges',
        '--exit-on-error',
        $dumpPath
    ) 'Control Plane PostgreSQL restore'

    $restoredCounts = [ordered]@{
        migrations = [int64](Invoke-PostgresScalar $restoreDatabase 'SELECT COUNT(*) FROM _sqlx_migrations')
        clusters = [int64](Invoke-PostgresScalar $restoreDatabase 'SELECT COUNT(*) FROM clusters')
        incidents = [int64](Invoke-PostgresScalar $restoreDatabase 'SELECT COUNT(*) FROM sre_incidents')
        fleet_releases = [int64](Invoke-PostgresScalar $restoreDatabase 'SELECT COUNT(*) FROM fleet_releases')
    }
    foreach ($name in $sourceCounts.Keys) {
        if ($sourceCounts[$name] -ne $restoredCounts[$name]) {
            throw "Restored '$name' count differs from the source database."
        }
    }

    $targetDriveName = [IO.Path]::GetPathRoot(
        [IO.Path]::GetFullPath($CargoTargetDir)
    ).TrimEnd('\').TrimEnd(':')
    $targetFreeGiB = (Get-PSDrive -Name $targetDriveName).Free / 1GB
    Write-Host "${targetDriveName}_FREE_GIB=$([Math]::Round($targetFreeGiB, 2))"
    if ($targetFreeGiB -lt 15) {
        Invoke-Native cargo @(
            'clean',
            '--manifest-path', $manifestPath,
            '--target-dir', $CargoTargetDir
        ) 'low-space Cargo cleanup'
    }

    $env:CARGO_HOME = [IO.Path]::GetFullPath($CargoHome)
    $env:CARGO_TARGET_DIR = [IO.Path]::GetFullPath($CargoTargetDir)
    $env:TEMP = [IO.Path]::GetFullPath($TemporaryRoot)
    $env:TMP = $env:TEMP
    Invoke-Native cargo @(
        'build',
        '--manifest-path', $manifestPath,
        '--locked',
        '-p', 'rocketmq-sre-control-plane',
        '--bin', 'rocketmq-sre-control-plane'
    ) 'current Control Plane build'
    if (-not (Test-Path -LiteralPath $controlPlaneExecutable -PathType Leaf)) {
        throw 'Current Control Plane executable was not produced in the selected D/F target directory.'
    }

    $encodedUser = [Uri]::EscapeDataString($DatabaseUser)
    $encodedPassword = [Uri]::EscapeDataString($DatabasePassword)
    $env:DATABASE_URL = 'postgres://{0}:{1}@{2}:{3}/{4}' -f @(
        $encodedUser,
        $encodedPassword,
        $databaseHost,
        $databasePort,
        $restoreDatabase
    )
    $env:ROCKETMQ_SRE_BIND_ADDR = "127.0.0.1:$PublicPort"
    $env:ROCKETMQ_SRE_CONNECTOR_BIND_ADDR = "127.0.0.1:$ConnectorPort"
    $env:ROCKETMQ_SRE_DATABASE_MAX_CONNECTIONS = '4'
    $env:ROCKETMQ_SRE_INTERNAL_TOKEN = "phase05-restore-$([Guid]::NewGuid().ToString('N'))"
    $env:ROCKETMQ_SRE_DEV_AUTH = 'true'
    $env:ROCKETMQ_SRE_MODEL_ENABLED = 'false'
    $env:ROCKETMQ_SRE_OBJECT_STORE_LOCAL_PATH = Join-Path $runtimeDirectory 'evidence'
    $env:RUST_LOG = 'rocketmq_sre_control_plane=info'
    New-Item -ItemType Directory -Force -Path $env:ROCKETMQ_SRE_OBJECT_STORE_LOCAL_PATH | Out-Null

    $process = Start-Process `
        -FilePath $controlPlaneExecutable `
        -PassThru `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath
    $ready = Wait-ControlPlaneReady
    $finishedAt = [DateTimeOffset]::UtcNow
    $revision = (& git -C $sreRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') {
        throw 'Unable to resolve the Control Plane restore revision.'
    }

    $evidence = [ordered]@{
        schema_version = 'rocketmq-sre.phase05-control-plane-restore.v1'
        status = 'passed'
        environment = 'docker-postgresql-backup-restore'
        started_at = $exerciseStartedAt.ToString('O')
        finished_at = $finishedAt.ToString('O')
        observed_at = $finishedAt.ToString('O')
        revision = $revision
        source_database = $SourceDatabase
        restore_database_ephemeral = $true
        backup_format = 'postgres-custom'
        source_counts = $sourceCounts
        restored_counts = $restoredCounts
        control_plane_health = 'healthy'
        control_plane_ready = [bool]$ready.ready
        restore_verified = [bool]$ready.ready
        rpo_rows = 0
        rto_seconds = [int][Math]::Ceiling(($finishedAt - $restoreStartedAt).TotalSeconds)
        public_port = $PublicPort
        secrets_recorded = $false
    }
    $evidenceDirectory = Split-Path -Parent ([IO.Path]::GetFullPath($EvidenceOutput))
    New-Item -ItemType Directory -Force -Path $evidenceDirectory | Out-Null
    $evidence | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $EvidenceOutput -Encoding utf8
    Write-Host "PHASE05_CONTROL_PLANE_RESTORE_OK evidence=$EvidenceOutput"
}
finally {
    if ($null -ne $process -and -not $process.HasExited) {
        Stop-Process -Id $process.Id -Force
        $process.WaitForExit()
    }
    if ($restoreCreated) {
        & docker exec $PostgresContainer dropdb `
            --username $DatabaseUser `
            --if-exists `
            --force `
            $restoreDatabase | Out-Host
    }
    & docker exec $PostgresContainer rm -f $dumpPath | Out-Null
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}

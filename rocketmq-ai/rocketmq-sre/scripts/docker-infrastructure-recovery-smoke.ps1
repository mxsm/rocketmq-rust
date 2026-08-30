# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [string]$OutputRoot = 'D:\rocketmq-sre-evidence\infrastructure-recovery',
    [ValidateRange(1024, 65535)][int]$PrimaryPort = 56432,
    [ValidateRange(1024, 65535)][int]$StandbyPort = 56433
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$outputRoot = [IO.Path]::GetFullPath($OutputRoot)
$root = [IO.Path]::GetPathRoot($outputRoot)
if (
    -not $root.Equals('D:\', [StringComparison]::OrdinalIgnoreCase) -and
    -not $root.Equals('F:\', [StringComparison]::OrdinalIgnoreCase)
) {
    throw 'Recovery evidence must use the D or F drive.'
}
if ($outputRoot.TrimEnd('\') -eq $root.TrimEnd('\')) {
    throw 'Recovery evidence must use a dedicated directory below the D or F drive root.'
}
if ($PrimaryPort -eq $StandbyPort) {
    throw 'Primary and standby ports must differ.'
}

$runId = [Guid]::NewGuid().ToString('N').Substring(0, 12)
$network = "sre-recovery-$runId"
$primary = "sre-pg-primary-$runId"
$standby = "sre-pg-standby-$runId"
$primaryVolume = "sre-pg-primary-$runId"
$standbyVolume = "sre-pg-standby-$runId"
$minioSource = "sre-minio-source-$runId"
$minioRestore = "sre-minio-restore-$runId"
$minioSourceVolume = "sre-minio-source-$runId"
$minioRestoreVolume = "sre-minio-restore-$runId"
$postgresPassword = [Guid]::NewGuid().ToString('N')
$replicationPassword = [Guid]::NewGuid().ToString('N')
$minioAccess = "sre$runId"
$minioSecret = "$([Guid]::NewGuid().ToString('N'))$([Guid]::NewGuid().ToString('N'))"
$postgresEvidencePath = Join-Path $outputRoot 'postgresql-ha.json'
$objectEvidencePath = Join-Path $outputRoot 'object-recovery.json'
$objectSource = Join-Path $outputRoot 'object-source'
$objectBackup = Join-Path $outputRoot 'object-backup'
$objectRestore = Join-Path $outputRoot 'object-restored'

function Invoke-Native(
    [string]$Command,
    [string[]]$Arguments,
    [string]$Description,
    [switch]$AllowFailure
) {
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $Command @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "$Description failed with exit code $exitCode.`n$($output -join [Environment]::NewLine)"
    }
    [pscustomobject]@{ ExitCode = $exitCode; Output = ($output -join "`n").Trim() }
}

function Wait-Postgres([string]$Container) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(90)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        $probe = Invoke-Native docker @(
            'exec', $Container, 'pg_isready', '-U', 'rocketmq_sre', '-d', 'rocketmq_sre'
        ) "$Container readiness" -AllowFailure
        if ($probe.ExitCode -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 500
    }
    throw "$Container did not become ready."
}

function Invoke-Psql([string]$Container, [string]$Sql) {
    (Invoke-Native docker @(
        'exec', '--env', "PGPASSWORD=$postgresPassword", $Container,
        'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'rocketmq_sre', '-d', 'rocketmq_sre',
        '-At', '-c', $Sql
    ) "$Container SQL").Output
}

function Wait-SynchronousStandby {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(90)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        $state = Invoke-Psql $primary "SELECT state || ':' || sync_state FROM pg_stat_replication LIMIT 1;"
        if ($state -match '^streaming:(sync|quorum)$') {
            return $state
        }
        Start-Sleep -Milliseconds 500
    }
    throw 'PostgreSQL standby did not reach synchronous streaming state.'
}

function Wait-Minio([string]$Alias, [string]$Container) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(90)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        $probe = Invoke-Native docker @(
            'run', '--rm', '--network', $network,
            '--env', "MC_HOST_${Alias}=http://${minioAccess}:${minioSecret}@${Container}:9000",
            'minio/mc:RELEASE.2025-04-16T18-13-26Z', 'ready', $Alias
        ) "$Container readiness" -AllowFailure
        if ($probe.ExitCode -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 500
    }
    throw "$Container did not become ready."
}

function Invoke-Mc([string]$Alias, [string]$Container, [string[]]$Arguments, [string[]]$Mounts = @()) {
    $dockerArguments = @('run', '--rm', '--network', $network)
    foreach ($mount in $Mounts) {
        $dockerArguments += @('--volume', $mount)
    }
    $dockerArguments += @(
        '--env', "MC_HOST_${Alias}=http://${minioAccess}:${minioSecret}@${Container}:9000",
        'minio/mc:RELEASE.2025-04-16T18-13-26Z'
    )
    $dockerArguments += $Arguments
    Invoke-Native docker $dockerArguments "MinIO client $($Arguments -join ' ')" | Out-Null
}

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
foreach ($objectDirectory in @($objectSource, $objectBackup, $objectRestore)) {
    if (-not $objectDirectory.StartsWith(
            $outputRoot + [IO.Path]::DirectorySeparatorChar,
            [StringComparison]::OrdinalIgnoreCase
        )) {
        throw "Object recovery directory escaped the evidence root: $objectDirectory"
    }
    if (Test-Path -LiteralPath $objectDirectory) {
        Remove-Item -LiteralPath $objectDirectory -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $objectDirectory | Out-Null
}
$startedAt = [DateTimeOffset]::UtcNow
$revision = (& git -C (Split-Path -Parent $PSScriptRoot) rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to resolve the infrastructure recovery revision.'
}
$createdResources = $false
try {
    Invoke-Native docker @('network', 'create', $network) 'recovery network creation' | Out-Null
    Invoke-Native docker @('volume', 'create', $primaryVolume) 'primary volume creation' | Out-Null
    Invoke-Native docker @('volume', 'create', $standbyVolume) 'standby volume creation' | Out-Null
    Invoke-Native docker @(
        'run', '--detach', '--name', $primary, '--network', $network,
        '--publish', "127.0.0.1:${PrimaryPort}:5432",
        '--env', 'POSTGRES_USER=rocketmq_sre',
        '--env', "POSTGRES_PASSWORD=$postgresPassword",
        '--env', 'POSTGRES_DB=rocketmq_sre',
        '--volume', "${primaryVolume}:/var/lib/postgresql/data",
        'postgres:17-alpine',
        '-c', 'wal_level=replica',
        '-c', 'max_wal_senders=10',
        '-c', 'max_replication_slots=10',
        '-c', 'synchronous_commit=on'
    ) 'PostgreSQL primary start' | Out-Null
    $createdResources = $true
    Wait-Postgres $primary
    Invoke-Psql $primary "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '$replicationPassword';" | Out-Null
    Invoke-Native docker @(
        'exec', $primary, 'sh', '-ec',
        "echo 'host replication replicator 0.0.0.0/0 scram-sha-256' >> /var/lib/postgresql/data/pg_hba.conf"
    ) 'replication access configuration' | Out-Null
    Invoke-Psql $primary 'SELECT pg_reload_conf();' | Out-Null
    $baseBackupCommand =
        "rm -rf /var/lib/postgresql/data/* && " +
        "pg_basebackup -h $primary -U replicator -D /var/lib/postgresql/data " +
        '-Fp -Xs -P -R -C -S sre_standby'
    Invoke-Native docker @(
        'run', '--rm', '--network', $network,
        '--env', "PGPASSWORD=$replicationPassword",
        '--volume', "${standbyVolume}:/var/lib/postgresql/data",
        'postgres:17-alpine', 'sh', '-ec',
        $baseBackupCommand
    ) 'PostgreSQL standby base backup' | Out-Null
    Invoke-Native docker @(
        'run', '--detach', '--name', $standby, '--network', $network,
        '--publish', "127.0.0.1:${StandbyPort}:5432",
        '--env', "POSTGRES_PASSWORD=$postgresPassword",
        '--volume', "${standbyVolume}:/var/lib/postgresql/data",
        'postgres:17-alpine'
    ) 'PostgreSQL standby start' | Out-Null
    Wait-Postgres $standby
    Invoke-Psql $primary "ALTER SYSTEM SET synchronous_standby_names = '*';" | Out-Null
    Invoke-Psql $primary 'SELECT pg_reload_conf();' | Out-Null
    $replicationState = Wait-SynchronousStandby

    Invoke-Psql $primary @'
CREATE TABLE approval_records (id TEXT PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE audit_records (id TEXT PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE step_intent_records (id TEXT PRIMARY KEY, payload TEXT NOT NULL);
INSERT INTO approval_records VALUES ('approval-1', 'approved');
INSERT INTO audit_records VALUES ('audit-1', 'recorded');
INSERT INTO step_intent_records VALUES ('intent-1', 'persisted-before-effect');
CHECKPOINT;
'@ | Out-Null
    $primaryCount = [int](Invoke-Psql $primary @'
SELECT (SELECT count(*) FROM approval_records)
     + (SELECT count(*) FROM audit_records)
     + (SELECT count(*) FROM step_intent_records);
'@)
    if ($primaryCount -ne 3) {
        throw 'Primary did not persist the three required safety records.'
    }
    $failureStartedAt = [DateTimeOffset]::UtcNow
    Invoke-Native docker @('stop', '--time', '1', $primary) 'PostgreSQL primary failure injection' | Out-Null
    Invoke-Native docker @(
        'exec', '--user', 'postgres', $standby,
        'pg_ctl', '-D', '/var/lib/postgresql/data', 'promote', '-w'
    ) 'PostgreSQL standby promotion' | Out-Null
    $promotedCount = [int](Invoke-Psql $standby @'
SELECT (SELECT count(*) FROM approval_records)
     + (SELECT count(*) FROM audit_records)
     + (SELECT count(*) FROM step_intent_records);
'@)
    $rtoSeconds = [int][Math]::Ceiling(([DateTimeOffset]::UtcNow - $failureStartedAt).TotalSeconds)
    if ($promotedCount -ne $primaryCount) {
        throw 'PostgreSQL synchronous failover lost a safety record.'
    }
    $postgresEvidence = [ordered]@{
        schema_version = 'rocketmq-sre.postgresql-ha-recovery.v1'
        status = 'passed'
        environment = 'docker-postgresql-17-synchronous-standby'
        started_at = $startedAt.ToString('O')
        finished_at = [DateTimeOffset]::UtcNow.ToString('O')
        revision = $revision
        synchronous_replication = $true
        replication_state = $replicationState
        primary_failure_injected = $true
        standby_promoted = $true
        approval_rows = 1
        audit_rows = 1
        step_intent_rows = 1
        rpo_rows = $primaryCount - $promotedCount
        rto_seconds = $rtoSeconds
        secrets_recorded = $false
    }
    $postgresEvidence | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $postgresEvidencePath -Encoding utf8

    $metadata = [ordered]@{
        schema_version = 'rocketmq-sre.object-metadata.v1'
        object_id = "qualification-$runId"
        content_type = 'application/json'
    } | ConvertTo-Json
    $content = [ordered]@{
        schema_version = 'rocketmq-sre.evidence-content.v1'
        classification = 'internal'
        value = [ordered]@{ status = 'bounded'; message_body_included = $false }
    } | ConvertTo-Json -Depth 4
    [IO.File]::WriteAllText((Join-Path $objectSource 'metadata.json'), $metadata, [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText((Join-Path $objectSource 'content.json'), $content, [Text.UTF8Encoding]::new($false))
    $sourceHashes = @{
        metadata = (Get-FileHash -LiteralPath (Join-Path $objectSource 'metadata.json') -Algorithm SHA256).Hash
        content = (Get-FileHash -LiteralPath (Join-Path $objectSource 'content.json') -Algorithm SHA256).Hash
    }

    Invoke-Native docker @('volume', 'create', $minioSourceVolume) 'MinIO source volume creation' | Out-Null
    Invoke-Native docker @(
        'run', '--detach', '--name', $minioSource, '--network', $network,
        '--env', "MINIO_ROOT_USER=$minioAccess", '--env', "MINIO_ROOT_PASSWORD=$minioSecret",
        '--volume', "${minioSourceVolume}:/data",
        'minio/minio:RELEASE.2025-04-22T22-12-26Z', 'server', '/data', '--console-address', ':9001'
    ) 'MinIO source start' | Out-Null
    Wait-Minio 'source' $minioSource
    Invoke-Mc 'source' $minioSource @('mb', '--ignore-existing', 'source/evidence')
    Invoke-Mc 'source' $minioSource @('mirror', '/source', 'source/evidence') @("${objectSource}:/source:ro")
    Invoke-Mc 'source' $minioSource @('mirror', 'source/evidence', '/backup') @("${objectBackup}:/backup")
    Invoke-Native docker @('stop', '--time', '1', $minioSource) 'MinIO source loss injection' | Out-Null
    Invoke-Native docker @('rm', $minioSource) 'MinIO source removal' | Out-Null

    Invoke-Native docker @('volume', 'create', $minioRestoreVolume) 'MinIO restore volume creation' | Out-Null
    Invoke-Native docker @(
        'run', '--detach', '--name', $minioRestore, '--network', $network,
        '--env', "MINIO_ROOT_USER=$minioAccess", '--env', "MINIO_ROOT_PASSWORD=$minioSecret",
        '--volume', "${minioRestoreVolume}:/data",
        'minio/minio:RELEASE.2025-04-22T22-12-26Z', 'server', '/data', '--console-address', ':9001'
    ) 'MinIO restore start' | Out-Null
    Wait-Minio 'restore' $minioRestore
    Invoke-Mc 'restore' $minioRestore @('mb', '--ignore-existing', 'restore/evidence')
    Invoke-Mc 'restore' $minioRestore @('mirror', '/backup', 'restore/evidence') @("${objectBackup}:/backup:ro")
    Invoke-Mc 'restore' $minioRestore @('mirror', 'restore/evidence', '/restored') @("${objectRestore}:/restored")
    $metadataRestored = Test-Path -LiteralPath (Join-Path $objectRestore 'metadata.json') -PathType Leaf
    $contentRestored = Test-Path -LiteralPath (Join-Path $objectRestore 'content.json') -PathType Leaf
    $hashVerified = $metadataRestored -and $contentRestored -and
        (Get-FileHash -LiteralPath (Join-Path $objectRestore 'metadata.json') -Algorithm SHA256).Hash -eq $sourceHashes.metadata -and
        (Get-FileHash -LiteralPath (Join-Path $objectRestore 'content.json') -Algorithm SHA256).Hash -eq $sourceHashes.content
    if (-not $hashVerified) {
        throw 'S3-compatible metadata/content restore hash verification failed.'
    }
    $objectEvidence = [ordered]@{
        schema_version = 'rocketmq-sre.object-recovery.v1'
        status = 'passed'
        environment = 'docker-minio-backup-restore'
        started_at = $startedAt.ToString('O')
        finished_at = [DateTimeOffset]::UtcNow.ToString('O')
        revision = $revision
        source_failure_injected = $true
        metadata_restored = $metadataRestored
        content_restored = $contentRestored
        sha256_verified = $hashVerified
        restored_objects = 2
        lost_objects = 0
        message_bodies_recorded = $false
        secrets_recorded = $false
    }
    $objectEvidence | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $objectEvidencePath -Encoding utf8
    Write-Host "DOCKER_INFRASTRUCTURE_RECOVERY_OK postgres=$postgresEvidencePath object=$objectEvidencePath"
}
finally {
    foreach ($container in @($minioRestore, $minioSource, $standby, $primary)) {
        Invoke-Native docker @('rm', '--force', $container) "cleanup $container" -AllowFailure | Out-Null
    }
    foreach ($volume in @($minioRestoreVolume, $minioSourceVolume, $standbyVolume, $primaryVolume)) {
        Invoke-Native docker @('volume', 'rm', '--force', $volume) "cleanup $volume" -AllowFailure | Out-Null
    }
    if ($createdResources) {
        Invoke-Native docker @('network', 'rm', $network) 'cleanup recovery network' -AllowFailure | Out-Null
    }
}

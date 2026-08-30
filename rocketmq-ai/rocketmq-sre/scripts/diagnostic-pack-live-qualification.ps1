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
    [string]$EvidenceRoot = 'D:\rocketmq-sre-evidence',
    [string]$BuildRoot = 'F:\BuildCache\rocketmq-sre-diagnostic-pack-qualification',
    [ValidateRange(1024, 65535)]
    [int]$PostgresPort = 55432,
    [switch]$KeepEnvironment,
    [switch]$KeepBuildCache
)

$ErrorActionPreference = 'Stop'
$sreRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$manifestPath = Join-Path $sreRoot 'Cargo.toml'
$buildRootPath = [IO.Path]::GetFullPath($BuildRoot)
$evidenceRootPath = [IO.Path]::GetFullPath($EvidenceRoot)
$containerName = 'rocketmq-sre-diagnostic-qualification-postgres'
$token = "qualification-$([Guid]::NewGuid().ToString('N'))"
$databasePassword = [Guid]::NewGuid().ToString('N')
$databaseUrl = "postgres://rocketmq_sre:$databasePassword@127.0.0.1:$PostgresPort/rocketmq_sre"
$timestamp = [DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss')
$runRoot = Join-Path $evidenceRootPath "diagnostic-packs-$timestamp"
$reportPath = Join-Path $runRoot 'qualification-report.v1.json'
$controlPlaneStdout = Join-Path $runRoot 'control-plane.stdout.log'
$controlPlaneStderr = Join-Path $runRoot 'control-plane.stderr.log'
$controlPlaneProcess = $null

function Assert-SafeLocalRoot([string]$Path, [string]$Name) {
    $full = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetPathRoot($full)
    if ($root -notin @('D:\', 'F:\')) {
        throw "$Name must be located on D:\ or F:\."
    }
    if ($full -eq $root) {
        throw "$Name cannot be a drive root."
    }
}

function Invoke-Native([string]$File, [string[]]$Arguments) {
    & $File @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$File failed with exit code $LASTEXITCODE."
    }
}

function Remove-QualificationContainer {
    $existing = & docker container ls --all --quiet --filter "name=^/$containerName$"
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to inspect the qualification PostgreSQL container.'
    }
    if (-not [string]::IsNullOrWhiteSpace("$existing")) {
        Invoke-Native docker @('container', 'rm', '--force', $containerName)
    }
}

function Wait-Postgres {
    $deadline = [DateTime]::UtcNow.AddMinutes(2)
    do {
        & docker exec $containerName pg_isready --username rocketmq_sre --dbname rocketmq_sre *> $null
        if ($LASTEXITCODE -eq 0) {
            return
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'Docker PostgreSQL did not become ready within two minutes.'
}

function Wait-ControlPlane {
    $deadline = [DateTime]::UtcNow.AddMinutes(2)
    do {
        if ($controlPlaneProcess.HasExited) {
            throw "Control Plane exited before readiness; inspect $controlPlaneStderr."
        }
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:8090/readyz' -TimeoutSec 3
            if ($response.StatusCode -eq 200) {
                return
            }
        }
        catch {
            # Startup includes PostgreSQL migrations and can take several probes.
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'Control Plane did not become ready within two minutes.'
}

Assert-SafeLocalRoot $buildRootPath 'BuildRoot'
Assert-SafeLocalRoot $evidenceRootPath 'EvidenceRoot'
if ($containerName -notmatch '^rocketmq-sre-diagnostic-qualification-[a-z0-9-]+$') {
    throw 'Qualification container name is outside the fixed cleanup namespace.'
}
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

try {
    Invoke-Native docker @('version', '--format', '{{.Server.Version}}')
    Remove-QualificationContainer
    Invoke-Native docker @(
        'run', '--detach', '--name', $containerName,
        '--publish', "127.0.0.1:$PostgresPort`:5432",
        '--env', 'POSTGRES_USER=rocketmq_sre',
        '--env', "POSTGRES_PASSWORD=$databasePassword",
        '--env', 'POSTGRES_DB=rocketmq_sre',
        '--tmpfs', '/var/lib/postgresql/data:rw,nosuid,nodev,size=512m',
        'postgres:17-alpine'
    )
    Wait-Postgres

    $env:CARGO_TARGET_DIR = $buildRootPath
    $env:CARGO_INCREMENTAL = '0'
    $env:CARGO_PROFILE_DEV_DEBUG = '0'
    Invoke-Native cargo @('build', '--manifest-path', $manifestPath, '--locked', '-p', 'rocketmq-sre-control-plane', '--bin', 'rocketmq-sre-control-plane')
    Invoke-Native cargo @('build', '--manifest-path', $manifestPath, '--locked', '-p', 'rocketmq-sre-eval', '--bin', 'diagnostic-pack-qualification')

    $env:DATABASE_URL = $databaseUrl
    $env:ROCKETMQ_SRE_BIND_ADDR = '127.0.0.1:8090'
    $env:ROCKETMQ_SRE_CONNECTOR_BIND_ADDR = '127.0.0.1:8093'
    $env:ROCKETMQ_SRE_DATABASE_MAX_CONNECTIONS = '6'
    $env:ROCKETMQ_SRE_SHUTDOWN_SECONDS = '5'
    $env:ROCKETMQ_SRE_INTERNAL_TOKEN = $token
    $env:ROCKETMQ_SRE_GRANT_SIGNING_KEY = $token
    $env:ROCKETMQ_SRE_AGENT_ACK_KEY = $token
    $env:ROCKETMQ_SRE_DEV_AUTH = 'true'
    $env:ROCKETMQ_SRE_MODEL_ENABLED = 'false'
    $env:ROCKETMQ_SRE_EXECUTOR_URL = ''
    $env:ROCKETMQ_SRE_CONTROL_PLANE_EXECUTOR_TOKEN = ''
    $env:ROCKETMQ_SRE_CONFIG_DIR = Join-Path $sreRoot 'config'
    $env:ROCKETMQ_SRE_MIGRATIONS_DIR = Join-Path $sreRoot 'migrations'
    $env:ROCKETMQ_SRE_OBJECT_STORE_LOCAL_PATH = Join-Path $runRoot 'objects'
    $env:OTEL_SDK_DISABLED = 'true'
    New-Item -ItemType Directory -Force -Path $env:ROCKETMQ_SRE_OBJECT_STORE_LOCAL_PATH | Out-Null

    $controlPlaneBinary = Join-Path $buildRootPath 'debug\rocketmq-sre-control-plane.exe'
    $qualificationBinary = Join-Path $buildRootPath 'debug\diagnostic-pack-qualification.exe'
    $controlPlaneProcess = Start-Process `
        -FilePath $controlPlaneBinary `
        -WorkingDirectory $sreRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $controlPlaneStdout `
        -RedirectStandardError $controlPlaneStderr `
        -PassThru
    Wait-ControlPlane

    $revision = (& git -C (Split-Path $sreRoot -Parent) rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') {
        throw 'Unable to determine the qualification source revision.'
    }
    $sourceChanges = & git -C $sreRoot status --porcelain --untracked-files=all
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to determine whether the qualification source is clean.'
    }
    if (-not [string]::IsNullOrWhiteSpace("$sourceChanges")) {
        $revision = "$revision+worktree"
    }
    $env:ROCKETMQ_SRE_QUALIFICATION_TOKEN = $token
    $env:ROCKETMQ_SRE_QUALIFICATION_PUBLIC_URL = 'http://127.0.0.1:8090'
    $env:ROCKETMQ_SRE_QUALIFICATION_CONNECTOR_URL = 'http://127.0.0.1:8093'
    $env:ROCKETMQ_SRE_QUALIFICATION_REVISION = $revision
    $env:ROCKETMQ_SRE_QUALIFICATION_ENVIRONMENT = 'docker-postgresql-local'
    Invoke-Native $qualificationBinary @('run', $reportPath)
    Invoke-Native python @((Join-Path $PSScriptRoot 'check_diagnostic_pack_qualification.py'), '--report', $reportPath)

    Write-Output "DIAGNOSTIC_PACK_LIVE_QUALIFICATION_OK report=$reportPath"
}
finally {
    if ($null -ne $controlPlaneProcess -and -not $controlPlaneProcess.HasExited) {
        Stop-Process -Id $controlPlaneProcess.Id -Force
        $controlPlaneProcess.WaitForExit()
    }
    if (-not $KeepEnvironment) {
        Remove-QualificationContainer
    }
    if (-not $KeepBuildCache -and (Test-Path -LiteralPath $buildRootPath)) {
        $buildParent = [IO.Path]::GetFullPath((Join-Path $buildRootPath '..'))
        if ($buildParent -ne 'F:\BuildCache' -and $buildParent -ne 'D:\BuildCache') {
            throw 'Refusing to remove a build cache outside the fixed D:\BuildCache or F:\BuildCache boundary.'
        }
        Remove-Item -LiteralPath $buildRootPath -Recurse -Force
    }
}

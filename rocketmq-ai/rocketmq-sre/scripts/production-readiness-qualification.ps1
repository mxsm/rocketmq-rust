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
    [ValidateSet('Validate', 'Run')][string]$Mode = 'Validate',
    [string]$FaultMatrixRun,
    [string]$EvidenceRoot = 'D:\rocketmq-sre-evidence',
    [string]$ScratchRoot = 'D:\BuildCache',
    [ValidatePattern('^[a-z0-9][a-z0-9-]{0,39}$')]
    [string]$ClusterName = 'rocketmq-sre-production-readiness',
    [ValidateRange(1024, 65532)][int]$PostgresPort = 55432,
    [ValidateRange(1, 60)][int]$SampleIntervalSeconds = 60,
    [ValidateRange(0, 300)][int]$ComponentOutageSeconds = 15,
    [ValidateRange(0, 300)][int]$CollectorOutageSeconds = 30,
    [switch]$SkipKindBuild
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
Set-StrictMode -Version Latest
$startedAt = [DateTimeOffset]::UtcNow
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$manifestPath = Join-Path $sreRoot 'config/qualification/production-readiness.v1.json'
$checkerPath = Join-Path $scriptDirectory 'check_production_readiness_qualification.py'
$kindScript = Join-Path $scriptDirectory 'kind.ps1'
$enterpriseScript = Join-Path $scriptDirectory 'phase05-enterprise-smoke.ps1'
$soakScript = Join-Path $scriptDirectory 'phase05-soak-chaos.ps1'
$recoveryScript = Join-Path $scriptDirectory 'docker-infrastructure-recovery-smoke.ps1'
$disasterScript = Join-Path $scriptDirectory 'disaster-recovery-qualification.ps1'
$serviceImageScript = Join-Path $repositoryRoot 'scripts/service-image-contract.ps1'
$handoffPath = Join-Path $sreRoot 'docs/phase05-handoff-checklist.md'
$kindArtifactRoot = Join-Path $repositoryRoot 'target/phase00-kind'
$certificateRoot = Join-Path $repositoryRoot 'target/phase00-certs'
$ownedImages = @(
    'rocketmq-rust/broker:local',
    'rocketmq-rust/namesrv:local',
    'rocketmq-rust/controller:local',
    'rocketmq-rust/proxy:local',
    'rocketmq-rust/mcp:local',
    'rocketmq-rust/sre-control-plane:phase00-local',
    'rocketmq-rust/sre-connector:phase00-local',
    'rocketmq-rust/sre-executor:phase03-local',
    'rocketmq-rust/sre-execution-agent:phase03-local',
    'rocketmq-rust/sre-probe:phase00-local',
    'rocketmq-rust/sre-model-mock:phase00-local',
    'rocketmq-rust/sre-ui:phase00-local',
    'rocketmq-rust/fault-driver:local',
    'rocketmq-rust/broker:verification',
    'rocketmq-rust/namesrv:verification',
    'rocketmq-rust/controller:verification',
    'rocketmq-rust/proxy:verification',
    'rocketmq-rust/mcp:verification'
)

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found."
    }
}

function Assert-DataPath([string]$Path, [string]$Description) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    $driveRoot = [IO.Path]::GetPathRoot($fullPath)
    if (
        -not $driveRoot.Equals('D:\', [StringComparison]::OrdinalIgnoreCase) -and
        -not $driveRoot.Equals('F:\', [StringComparison]::OrdinalIgnoreCase)
    ) {
        throw "$Description must use the D or F drive."
    }
    if ($fullPath.TrimEnd('\') -eq $driveRoot.TrimEnd('\')) {
        throw "$Description must use a dedicated directory below the drive root."
    }
    $fullPath
}

function Invoke-Native(
    [string]$Command,
    [string[]]$Arguments,
    [string]$Description,
    [switch]$AllowFailure
) {
    $savedPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $Command @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $savedPreference
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "$Description failed with exit code $exitCode.`n$($output -join [Environment]::NewLine)"
    }
    [pscustomobject]@{ ExitCode = $exitCode; Output = ($output -join "`n").Trim() }
}

function Read-Json([string]$Path, [string]$Description) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Description is missing: $Path"
    }
    try {
        Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
    }
    catch {
        throw "$Description is not valid JSON: $($_.Exception.Message)"
    }
}

function Get-Sha256([string]$Path) {
    "sha256:$((Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant())"
}

function New-Source(
    [string]$Id,
    [string]$Schema,
    [string]$Path,
    [string]$Revision
) {
    [ordered]@{
        id = $Id
        status = 'passed'
        schema_version = $Schema
        sha256 = (Get-Sha256 $Path)
        revision = $Revision
    }
}

function Wait-Postgres([string]$Container) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(90)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        $probe = Invoke-Native docker @(
            'exec', $Container, 'pg_isready',
            '--username', 'rocketmq_sre', '--dbname', 'rocketmq_sre'
        ) 'qualification PostgreSQL readiness' -AllowFailure
        if ($probe.ExitCode -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 500
    }
    throw 'Qualification PostgreSQL did not become ready.'
}

function Assert-Revision($Evidence, [string]$Field, [string]$Revision, [string]$Description) {
    if ([string]$Evidence.$Field -ne $Revision) {
        throw "$Description does not match revision $Revision."
    }
}

foreach ($path in @($manifestPath, $checkerPath, $kindScript, $enterpriseScript, $soakScript, $recoveryScript, $disasterScript, $serviceImageScript, $handoffPath)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Qualification dependency is missing: $path"
    }
}
Require-Command python
Invoke-Native python @($checkerPath, '--manifest', $manifestPath) 'qualification manifest validation' | Out-Null
if ($Mode -eq 'Validate') {
    foreach ($path in @($enterpriseScript, $soakScript, $recoveryScript, $disasterScript)) {
        [void][scriptblock]::Create((Get-Content -Raw -LiteralPath $path))
    }
    Write-Host 'PRODUCTION_READINESS_QUALIFICATION_VALIDATION_OK live_model_calls=0 live_mode_ceiling=supervised'
    exit 0
}

foreach ($command in @('cargo', 'docker', 'git', 'helm', 'kind', 'kubectl', 'syft', 'trivy', 'cosign')) {
    Require-Command $command
}
$evidenceRoot = Assert-DataPath $EvidenceRoot 'evidence root'
$scratchBase = Assert-DataPath $ScratchRoot 'scratch root'
if ([string]::IsNullOrWhiteSpace($FaultMatrixRun)) {
    throw 'Run mode requires a dynamic fault-matrix report through -FaultMatrixRun.'
}
$faultMatrixPath = Assert-DataPath $FaultMatrixRun 'fault-matrix report'
if (-not (Test-Path -LiteralPath $faultMatrixPath -PathType Leaf)) {
    throw "Fault-matrix report is missing: $faultMatrixPath"
}
$revision = (Invoke-Native git @('-C', $repositoryRoot, 'rev-parse', 'HEAD') 'Git revision').Output
if ($revision -notmatch '^[0-9a-f]{40}$') {
    throw 'Qualification requires a full lowercase Git revision.'
}
$sourceStatus = (Invoke-Native git @('-C', $repositoryRoot, 'status', '--porcelain') 'Git source status').Output
if (-not [string]::IsNullOrWhiteSpace($sourceStatus)) {
    throw 'Run mode requires a clean committed source tree.'
}
$faultMatrix = Read-Json $faultMatrixPath 'fault-matrix report'
if (
    -not [bool]$faultMatrix.dynamic_execution -or
    [bool]$faultMatrix.fixture -or
    [string]$faultMatrix.candidate_commit -ne $revision
) {
    throw 'Fault-matrix evidence must be dynamic, non-fixture, and match the current revision.'
}
if ((Invoke-Native kind @('get', 'clusters') 'Kind cluster inventory').Output -split "`n" -contains $ClusterName) {
    throw "Kind cluster '$ClusterName' already exists."
}
foreach ($path in @($kindArtifactRoot, $certificateRoot)) {
    if (Test-Path -LiteralPath $path) {
        throw "Qualification-owned artifact path already exists: $path"
    }
}
foreach ($image in $ownedImages) {
    if ((Invoke-Native docker @('image', 'inspect', $image) "image preflight $image" -AllowFailure).ExitCode -eq 0) {
        throw "Qualification-owned image tag already exists: $image"
    }
}

$runId = "production-readiness-$($startedAt.ToString('yyyyMMddTHHmmssZ'))-$($revision.Substring(0, 12))"
$runRoot = Join-Path $evidenceRoot $runId
$scratchRoot = Join-Path $scratchBase $runId
$cargoTarget = Join-Path $scratchRoot 'cargo-target'
$cargoHome = Join-Path $scratchRoot 'cargo-home'
$temporaryRoot = Join-Path $scratchRoot 'temp'
$infrastructureRoot = Join-Path $runRoot 'infrastructure-recovery'
$enterprisePath = Join-Path $runRoot 'enterprise-smoke.json'
$soakPath = Join-Path $runRoot 'soak-chaos.json'
$disasterPath = Join-Path $runRoot 'disaster-recovery.json'
$controlPlanePath = Join-Path $runRoot 'control-plane-restore.json'
$servicePath = Join-Path $runRoot 'service-images.json'
$reportPath = Join-Path $runRoot 'qualification-report.v1.json'
$serviceOutputRelative = "target/production-readiness-$runId"
$serviceOutput = Join-Path $repositoryRoot $serviceOutputRelative
$serviceProvenance = Join-Path $serviceOutput 'provenance.json'
$kubeconfig = Join-Path $kindArtifactRoot 'kubeconfig'
$postgresContainer = "sre-production-readiness-$($revision.Substring(0, 8))"
$postgresPassword = [Guid]::NewGuid().ToString('N')
$databaseUrl = "postgres://rocketmq_sre:$postgresPassword@127.0.0.1:$PostgresPort/rocketmq_sre"
$kindCreated = $false
$postgresCreated = $false
$qualification = $null
$cleanup = [ordered]@{
    status = 'pending'
    disposable_kind_destroyed = $false
    owned_containers_removed = $false
    owned_artifacts_removed = $false
}

New-Item -ItemType Directory -Force -Path $runRoot, $cargoTarget, $cargoHome, $temporaryRoot | Out-Null
try {
    Invoke-Native docker @(
        'run', '--detach', '--rm', '--name', $postgresContainer,
        '--publish', "127.0.0.1:${PostgresPort}:5432",
        '--env', 'POSTGRES_USER=rocketmq_sre',
        '--env', "POSTGRES_PASSWORD=$postgresPassword",
        '--env', 'POSTGRES_DB=rocketmq_sre',
        'postgres:17-alpine'
    ) 'qualification PostgreSQL start' | Out-Null
    $postgresCreated = $true
    Wait-Postgres $postgresContainer

    & $serviceImageScript -OutputDirectory $serviceOutputRelative
    if (-not (Test-Path -LiteralPath $serviceProvenance -PathType Leaf)) {
        throw 'Service-image contract did not emit provenance.'
    }
    Copy-Item -LiteralPath $serviceProvenance -Destination $servicePath

    & $recoveryScript `
        -OutputRoot $infrastructureRoot `
        -PrimaryPort ($PostgresPort + 1) `
        -StandbyPort ($PostgresPort + 2)

    & $kindScript -Action Up -ClusterName $ClusterName -SkipBuild:$SkipKindBuild
    $kindCreated = $true

    & $enterpriseScript `
        -DatabaseUrl $databaseUrl `
        -PostgresContainer $postgresContainer `
        -Kubeconfig $kubeconfig `
        -ExpectedContext "kind-$ClusterName" `
        -CargoTargetDir $cargoTarget `
        -CargoHome $cargoHome `
        -TemporaryRoot $temporaryRoot `
        -EvidenceOutput $enterprisePath

    & $soakScript `
        -Mode Run `
        -DurationSeconds 21600 `
        -SampleIntervalSeconds $SampleIntervalSeconds `
        -InjectFaults `
        -FullDurationQualification `
        -ComponentOutageSeconds $ComponentOutageSeconds `
        -CollectorOutageSeconds $CollectorOutageSeconds `
        -Kubeconfig $kubeconfig `
        -ExpectedContext "kind-$ClusterName" `
        -KindNodeContainer "$ClusterName-control-plane" `
        -EvidenceOutput $soakPath

    $enterprise = Read-Json $enterprisePath 'enterprise smoke evidence'
    $soak = Read-Json $soakPath 'soak evidence'
    $postgres = Read-Json (Join-Path $infrastructureRoot 'postgresql-ha.json') 'PostgreSQL HA evidence'
    $objectRecovery = Read-Json (Join-Path $infrastructureRoot 'object-recovery.json') 'object recovery evidence'
    $serviceImages = Read-Json $servicePath 'service-image evidence'
    Assert-Revision $enterprise 'repository_commit' $revision 'enterprise smoke evidence'
    Assert-Revision $soak 'repository_commit' $revision 'soak evidence'
    Assert-Revision $postgres 'revision' $revision 'PostgreSQL HA evidence'
    Assert-Revision $objectRecovery 'revision' $revision 'object recovery evidence'
    Assert-Revision $serviceImages 'source_commit' $revision 'service-image evidence'
    $enterprise.control_plane_restore | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $controlPlanePath -Encoding utf8

    & $disasterScript `
        -FaultMatrixRun $faultMatrixPath `
        -PostgresHaEvidence (Join-Path $infrastructureRoot 'postgresql-ha.json') `
        -ObjectRecoveryEvidence (Join-Path $infrastructureRoot 'object-recovery.json') `
        -ControlPlaneRestoreEvidence $controlPlanePath `
        -EvidenceOutput $disasterPath
    $disaster = Read-Json $disasterPath 'disaster-recovery evidence'
    Assert-Revision $disaster 'revision' $revision 'disaster-recovery evidence'

    $precheck = $enterprise.measurements.execution_precheck
    $handoff = Get-Content -Raw -LiteralPath $handoffPath
    foreach ($commandPath in @(
        'phase05-enterprise-smoke.ps1',
        'phase05-control-plane-restore.ps1',
        'phase05-test-cluster-dr.ps1'
    )) {
        if (-not $handoff.Contains($commandPath, [StringComparison]::Ordinal)) {
            throw "Handoff checklist is missing command path '$commandPath'."
        }
    }
    $sources = @(
        New-Source 'soak' ([string]$soak.schema_version) $soakPath $revision
        New-Source 'scale' ([string]$enterprise.schema_version) $enterprisePath $revision
        New-Source 'policy' ([string]$enterprise.schema_version) $enterprisePath $revision
        New-Source 'precheck' ([string]$enterprise.schema_version) $enterprisePath $revision
        New-Source 'postgres_ha' ([string]$postgres.schema_version) (Join-Path $infrastructureRoot 'postgresql-ha.json') $revision
        New-Source 'disaster_recovery' ([string]$disaster.schema_version) $disasterPath $revision
        New-Source 'service_images' 'rocketmq-rust.service-image-provenance.v1' $servicePath $revision
    )
    $qualification = [ordered]@{
        schema_version = 'rocketmq-sre.production-readiness-qualification-report.v1'
        status = 'passed'
        environment = 'disposable_kind'
        revision = $revision
        source_clean = $true
        started_at = $startedAt.ToString('O')
        finished_at = $null
        production_certified = $false
        model_provider_network_calls = 0
        unattended_autonomous_execution = $false
        live_mode_ceiling = 'supervised'
        secrets_recorded = $false
        message_bodies_recorded = $false
        sources = $sources
        soak = $soak
        scale = $enterprise.scale
        measurements = $enterprise.measurements
        operational_measurements = [ordered]@{
            samples = [int]$precheck.samples
            error_count = [int]$precheck.error_count
            error_rate = [double]$precheck.error_rate
            execution_queue_depth_samples = [int]$precheck.execution_queue_depth_samples
            execution_queue_depth_max = [int]$precheck.execution_queue_depth_max
        }
        handoff = [ordered]@{
            checklist_validated = $true
            command_paths_validated = $true
            independent_operator_signoff = $false
            required_for_production = $true
        }
        cleanup = $cleanup
    }
}
finally {
    $kindExists = @((Invoke-Native kind @('get', 'clusters') 'Kind cleanup inventory' -AllowFailure).Output -split "`n") -contains $ClusterName
    if ($kindCreated -or $kindExists) {
        try {
            & $kindScript -Action Down -ClusterName $ClusterName
        }
        catch {
            Write-Warning "Kind cleanup failed: $($_.Exception.Message)"
        }
    }
    $clusterStillExists = @((Invoke-Native kind @('get', 'clusters') 'Kind cleanup verification' -AllowFailure).Output -split "`n") -contains $ClusterName
    $cleanup.disposable_kind_destroyed = -not $clusterStillExists

    if ($postgresCreated) {
        Invoke-Native docker @('rm', '--force', $postgresContainer) 'qualification PostgreSQL cleanup' -AllowFailure | Out-Null
    }
    $containerStillExists = -not [string]::IsNullOrWhiteSpace(
        (Invoke-Native docker @('ps', '--all', '--quiet', '--filter', "name=^/${postgresContainer}$") 'container cleanup verification' -AllowFailure).Output
    )
    $cleanup.owned_containers_removed = -not $containerStillExists

    foreach ($image in $ownedImages) {
        Invoke-Native docker @('image', 'rm', '--force', $image) "image cleanup $image" -AllowFailure | Out-Null
    }
    foreach ($path in @($kindArtifactRoot, $certificateRoot, $serviceOutput, $scratchRoot)) {
        if (Test-Path -LiteralPath $path) {
            $fullPath = [IO.Path]::GetFullPath($path)
            if (-not $fullPath.StartsWith($repositoryRoot, [StringComparison]::OrdinalIgnoreCase) -and
                -not $fullPath.StartsWith($scratchBase, [StringComparison]::OrdinalIgnoreCase)) {
                throw "Cleanup target escaped an owned root: $fullPath"
            }
            Remove-Item -LiteralPath $fullPath -Recurse -Force
        }
    }
    $remainingArtifacts = @(
        @($kindArtifactRoot, $certificateRoot, $serviceOutput, $scratchRoot) |
            Where-Object { Test-Path -LiteralPath $_ }
    )
    $cleanup.owned_artifacts_removed = $remainingArtifacts.Count -eq 0
    if ($cleanup.disposable_kind_destroyed -and $cleanup.owned_containers_removed -and $cleanup.owned_artifacts_removed) {
        $cleanup.status = 'passed'
    }
    else {
        $cleanup.status = 'failed'
    }
}

if ($null -eq $qualification) {
    throw 'Production-readiness qualification did not complete.'
}
$qualification.finished_at = [DateTimeOffset]::UtcNow.ToString('O')
$qualification.cleanup = $cleanup
[IO.File]::WriteAllText(
    $reportPath,
    ($qualification | ConvertTo-Json -Depth 30),
    [Text.UTF8Encoding]::new($false)
)
Invoke-Native python @($checkerPath, '--manifest', $manifestPath, '--report', $reportPath) 'final qualification report validation' | Out-Null
Write-Host "PRODUCTION_READINESS_QUALIFICATION_OK report=$reportPath live_model_calls=0 production_certified=false"

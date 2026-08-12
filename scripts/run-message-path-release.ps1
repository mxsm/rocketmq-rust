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
    [ValidateSet("Validate", "Run")]
    [string]$Mode = "Validate",
    [string]$CandidateCommit,
    [string]$ReleaseRoot,
    [string]$CandidateMeasurement,
    [string]$PerformanceComparison,
    [string]$FaultEvidence,
    [string]$RpoEvidence,
    [string]$SoakReport,
    [string]$BaselineState,
    [string]$CandidateState,
    [string]$RollbackCheckpointSet,
    [string]$ForwardCheckpointSet,
    [string]$RollbackPreservationProof,
    [string]$ForwardPreservationProof,
    [string]$ArchiveOutput,
    [string]$RunId,
    [string]$ReleaseName = "rocketmq",
    [string]$Namespace = "rocketmq-system",
    [string]$OperatorIdentity,
    [string]$MinisignSecretKey,
    [string]$MinisignPublicKey
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$QualificationScript = Join-Path $PSScriptRoot "message_path_qualification.py"
$ReleaseScript = Join-Path $PSScriptRoot "message_path_release.py"
$RollbackRunner = Join-Path $PSScriptRoot "run-architecture-release-rollback.ps1"
$RequiredDirectories = @("environment", "ab", "fault", "rpo", "soak", "qualification")

function Invoke-Checked {
    param(
        [Parameter(Mandatory)][string]$Executable,
        [Parameter(Mandatory)][string[]]$Arguments,
        [string]$LogPath,
        [switch]$AllowFailure
    )

    $content = (& $Executable @Arguments 2>&1 | Out-String).TrimEnd()
    $exitCode = $LASTEXITCODE
    if (-not [string]::IsNullOrWhiteSpace($LogPath)) {
        $parent = Split-Path $LogPath -Parent
        if (-not [string]::IsNullOrWhiteSpace($parent)) {
            New-Item -ItemType Directory -Force -Path $parent | Out-Null
        }
        [IO.File]::WriteAllText($LogPath, $content + "`n", [Text.UTF8Encoding]::new($false))
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "$Executable failed with exit code $exitCode`n$content"
    }
    [pscustomobject]@{ ExitCode = $exitCode; Output = $content }
}

function Require-Command {
    param([Parameter(Mandatory)][string]$Name)

    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($null -eq $command) {
        throw "required command is unavailable: $Name"
    }
    $command.Source
}

function Resolve-ExistingPath {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Label,
        [switch]$Directory,
        [switch]$Any
    )

    if ([string]::IsNullOrWhiteSpace($Path)) {
        throw "$Label is required"
    }
    $exists = if ($Any) {
        Test-Path -LiteralPath $Path
    } else {
        $pathType = if ($Directory) { "Container" } else { "Leaf" }
        Test-Path -LiteralPath $Path -PathType $pathType
    }
    if (-not $exists) {
        throw "$Label does not exist: $Path"
    }
    (Resolve-Path -LiteralPath $Path).Path
}

function Assert-UnderReleaseRoot {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$Label
    )

    $fullPath = [IO.Path]::GetFullPath($Path)
    $prefix = $Root.TrimEnd("\", "/") + [IO.Path]::DirectorySeparatorChar
    if (-not $fullPath.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "$Label must remain below ReleaseRoot"
    }
    $fullPath
}

function Invoke-RollbackTransition {
    param(
        [Parameter(Mandatory)][string]$PowerShell,
        [Parameter(Mandatory)][ValidateSet("Rollback", "Forward")][string]$Direction,
        [Parameter(Mandatory)][string]$CheckpointSet,
        [Parameter(Mandatory)][string]$Proof,
        [Parameter(Mandatory)][string]$LogPath
    )

    Invoke-Checked -Executable $PowerShell -Arguments @(
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $RollbackRunner,
        "-Apply", "-Direction", $Direction,
        "-BaselineStatePath", $script:BaselineStatePath,
        "-CandidateStatePath", $script:CandidateStatePath,
        "-CheckpointSetPath", $CheckpointSet,
        "-PreservationProofPath", $Proof,
        "-ReleaseName", $ReleaseName,
        "-Namespace", $Namespace,
        "-OperatorIdentity", $OperatorIdentity
    ) -LogPath $LogPath | Out-Null
}

$python = Require-Command "python"
$policyResult = Invoke-Checked $python @($QualificationScript, "validate-policy")
$schemaResult = Invoke-Checked $python @($ReleaseScript, "validate-schema")
$powerShell = (Get-Process -Id $PID).Path
$rollbackValidation = Invoke-Checked $powerShell @(
    "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $RollbackRunner, "-ValidateOnly"
)

if ($Mode -eq "Validate") {
    Write-Host "MESSAGE_PATH_RELEASE_VALIDATION_OK policy=$($policyResult.Output) schema=$($schemaResult.Output) rollback=$($rollbackValidation.Output)"
    return
}

if ($PSVersionTable.PSVersion.Major -lt 7) {
    throw "Run mode requires PowerShell 7 or newer"
}
if ($CandidateCommit -notmatch "^[0-9a-f]{40}$") {
    throw "CandidateCommit must be a full lowercase Git SHA"
}
if ([string]::IsNullOrWhiteSpace($OperatorIdentity)) {
    throw "OperatorIdentity is required"
}
if ([string]::IsNullOrWhiteSpace($RunId)) {
    $RunId = "message-path-release-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
}
if ($RunId -notmatch "^[A-Za-z0-9][A-Za-z0-9._-]{2,127}$") {
    throw "RunId must contain 3-128 safe characters"
}
if ([string]::IsNullOrWhiteSpace($MinisignSecretKey) -ne [string]::IsNullOrWhiteSpace($MinisignPublicKey)) {
    throw "MinisignSecretKey and MinisignPublicKey must be supplied together"
}

$releaseRootPath = Resolve-ExistingPath $ReleaseRoot "ReleaseRoot" -Directory
foreach ($directory in $RequiredDirectories) {
    if (-not (Test-Path -LiteralPath (Join-Path $releaseRootPath $directory) -PathType Container)) {
        throw "ReleaseRoot is missing canonical directory: $directory"
    }
}
$archivePath = [IO.Path]::GetFullPath($ArchiveOutput)
if (Test-Path -LiteralPath $archivePath) {
    throw "ArchiveOutput already exists and will not be overwritten: $archivePath"
}
$releasePrefix = $releaseRootPath.TrimEnd("\", "/") + [IO.Path]::DirectorySeparatorChar
if ($archivePath.StartsWith($releasePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw "ArchiveOutput must be outside ReleaseRoot"
}

$inputs = [ordered]@{
    CandidateMeasurement = $CandidateMeasurement
    PerformanceComparison = $PerformanceComparison
    FaultEvidence = $FaultEvidence
    RpoEvidence = $RpoEvidence
    SoakReport = $SoakReport
    BaselineState = $BaselineState
    CandidateState = $CandidateState
    RollbackCheckpointSet = $RollbackCheckpointSet
    ForwardCheckpointSet = $ForwardCheckpointSet
    RollbackPreservationProof = $RollbackPreservationProof
    ForwardPreservationProof = $ForwardPreservationProof
}
foreach ($entry in @($inputs.GetEnumerator())) {
    $allowDirectory = $entry.Key -in @("FaultEvidence", "RpoEvidence", "SoakReport")
    $resolved = Resolve-ExistingPath ([string]$entry.Value) $entry.Key -Any:$allowDirectory
    $inputs[$entry.Key] = Assert-UnderReleaseRoot $resolved $releaseRootPath $entry.Key
}
$script:BaselineStatePath = $inputs.BaselineState
$script:CandidateStatePath = $inputs.CandidateState

$rollbackDirectory = Join-Path $releaseRootPath "qualification/rollback"
New-Item -ItemType Directory -Force -Path $rollbackDirectory | Out-Null
$rollbackLog = Join-Path $rollbackDirectory "rollback.log"
$forwardLog = Join-Path $rollbackDirectory "forward.log"
$rollbackEvidence = Join-Path $rollbackDirectory "rollback-evidence.json"
foreach ($path in @($rollbackLog, $forwardLog, $rollbackEvidence)) {
    if (Test-Path -LiteralPath $path) {
        throw "release evidence output already exists: $path"
    }
}

Invoke-RollbackTransition $powerShell "Rollback" $inputs.RollbackCheckpointSet `
    $inputs.RollbackPreservationProof $rollbackLog
Invoke-RollbackTransition $powerShell "Forward" $inputs.ForwardCheckpointSet `
    $inputs.ForwardPreservationProof $forwardLog

Invoke-Checked $python @(
    $ReleaseScript, "build-rollback-evidence",
    "--candidate-measurement", $inputs.CandidateMeasurement,
    "--baseline-state", $inputs.BaselineState,
    "--candidate-state", $inputs.CandidateState,
    "--rollback-checkpoint", $inputs.RollbackCheckpointSet,
    "--forward-checkpoint", $inputs.ForwardCheckpointSet,
    "--rollback-proof", $inputs.RollbackPreservationProof,
    "--forward-proof", $inputs.ForwardPreservationProof,
    "--rollback-log", $rollbackLog,
    "--forward-log", $forwardLog,
    "--output", $rollbackEvidence
) | Out-Null

$stageRoot = Join-Path $releaseRootPath ".qualification-stage"
if (Test-Path -LiteralPath $stageRoot) {
    throw "qualification staging directory already exists: $stageRoot"
}
$qualificationResult = Invoke-Checked $python @(
    $QualificationScript, "qualify",
    "--candidate-commit", $CandidateCommit,
    "--candidate-measurement", $inputs.CandidateMeasurement,
    "--performance-comparison", $inputs.PerformanceComparison,
    "--fault-evidence", $inputs.FaultEvidence,
    "--rpo-evidence", $inputs.RpoEvidence,
    "--soak-report", $inputs.SoakReport,
    "--rollback-evidence", $rollbackEvidence,
    "--run-id", $RunId,
    "--output-dir", $stageRoot
) -AllowFailure
$stageRun = Join-Path $stageRoot $RunId
$qualificationDirectory = Join-Path $releaseRootPath "qualification"
$qualificationReport = Join-Path $qualificationDirectory "qualification-report.json"
try {
    if (Test-Path -LiteralPath (Join-Path $stageRun "qualification-report.json") -PathType Leaf) {
        if (Test-Path -LiteralPath $qualificationReport) {
            throw "qualification report already exists: $qualificationReport"
        }
        Move-Item -LiteralPath (Join-Path $stageRun "qualification-report.json") -Destination $qualificationReport
        if (Test-Path -LiteralPath (Join-Path $stageRun "external") -PathType Container) {
            Move-Item -LiteralPath (Join-Path $stageRun "external") -Destination (Join-Path $qualificationDirectory "external")
        }
    }
}
finally {
    if (Test-Path -LiteralPath $stageRoot) {
        Remove-Item -LiteralPath $stageRoot -Recurse -Force
    }
}
if ($qualificationResult.ExitCode -ne 0) {
    throw "final qualification returned NO-GO; the report was retained at $qualificationReport`n$($qualificationResult.Output)"
}

$packageArguments = @(
    $ReleaseScript, "package",
    "--source-root", $releaseRootPath,
    "--archive-output", $archivePath,
    "--read-only"
)
if (-not [string]::IsNullOrWhiteSpace($MinisignSecretKey)) {
    $packageArguments += @("--minisign-secret-key", (Resolve-ExistingPath $MinisignSecretKey "MinisignSecretKey"))
}
Invoke-Checked $python $packageArguments | Out-Null

$verifyArguments = @($ReleaseScript, "verify", "--bundle", $archivePath)
if (-not [string]::IsNullOrWhiteSpace($MinisignPublicKey)) {
    $verifyArguments += @("--minisign-public-key", (Resolve-ExistingPath $MinisignPublicKey "MinisignPublicKey"))
}
$verifyResult = Invoke-Checked $python $verifyArguments
Write-Host "MESSAGE_PATH_RELEASE_COMPLETE candidate=$CandidateCommit archive=$archivePath verification=$($verifyResult.Output)"

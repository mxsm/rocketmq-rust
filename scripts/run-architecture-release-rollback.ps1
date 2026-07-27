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

[CmdletBinding(DefaultParameterSetName = "Validate")]
param(
    [Parameter(ParameterSetName = "Validate")]
    [switch]$ValidateOnly,
    [Parameter(Mandatory, ParameterSetName = "Apply")]
    [switch]$Apply,
    [ValidateSet("Rollback", "Forward")]
    [string]$Direction = "Rollback",
    [string]$BaselineStatePath,
    [string]$CandidateStatePath,
    [string]$CheckpointSetPath,
    [string]$PreservationProofPath,
    [string]$PolicyPath = "distribution/kubernetes/rollback-policy.json",
    [Parameter(ParameterSetName = "Apply")]
    [string]$ReleaseName = "rocketmq",
    [Parameter(ParameterSetName = "Apply")]
    [string]$Namespace = "rocketmq",
    [Parameter(ParameterSetName = "Apply")]
    [string]$OperatorIdentity = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$script:RepositoryRoot = Split-Path $PSScriptRoot -Parent
$script:Services = @("controller", "namesrv", "broker", "proxy", "mcp")
$script:Sha256Pattern = "^[0-9a-f]{64}$"
$script:IdentifierPattern = "^[A-Za-z0-9._:/@-]{1,256}$"

function Read-JsonFile {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Context
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Context is missing: $Path"
    }
    try {
        return Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
    }
    catch {
        throw "$Context is not valid JSON: $($_.Exception.Message)"
    }
}

function Write-JsonFile {
    param(
        [Parameter(Mandatory)][object]$Value,
        [Parameter(Mandatory)][string]$Path
    )

    $json = $Value | ConvertTo-Json -Depth 32 -Compress
    [System.IO.File]::WriteAllText(
        $Path,
        ($json + "`n"),
        [System.Text.UTF8Encoding]::new($false)
    )
}

function Get-Sha256 {
    param([Parameter(Mandatory)][string]$Path)

    return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

function Get-TextSha256 {
    param([Parameter(Mandatory)][string]$Text)

    $algorithm = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.UTF8Encoding]::new($false).GetBytes($Text)
        return [BitConverter]::ToString($algorithm.ComputeHash($bytes)).Replace("-", "").ToLowerInvariant()
    }
    finally {
        $algorithm.Dispose()
    }
}

function Assert-ExactProperties {
    param(
        [Parameter(Mandatory)][object]$Value,
        [Parameter(Mandatory)][string[]]$Names,
        [Parameter(Mandatory)][string]$Context
    )

    if ($null -eq $Value) {
        throw "$Context must be an object"
    }
    $actual = @($Value.PSObject.Properties.Name | Sort-Object)
    $expected = @($Names | Sort-Object)
    if (($actual -join "`n") -ne ($expected -join "`n")) {
        throw "$Context properties must be exactly: $($expected -join ', ')"
    }
}

function Assert-Identifier {
    param(
        [Parameter(Mandatory)][string]$Value,
        [Parameter(Mandatory)][string]$Context
    )

    if ($Value -notmatch $script:IdentifierPattern) {
        throw "$Context must be a canonical identifier"
    }
}

function Assert-Sha256 {
    param(
        [Parameter(Mandatory)][string]$Value,
        [Parameter(Mandatory)][string]$Context
    )

    if ($Value -notmatch $script:Sha256Pattern) {
        throw "$Context must be a lowercase SHA-256 hash"
    }
}

function Resolve-RepositoryPath {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Context
    )

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }
    $normalized = $Path.Replace("\", "/")
    if ($normalized.Split("/") -contains ".." -or $normalized -notmatch "^[A-Za-z0-9._/-]+$") {
        throw "$Context must be a safe repository-relative path"
    }
    $resolved = [System.IO.Path]::GetFullPath((Join-Path $script:RepositoryRoot $normalized))
    $rootPrefix = $script:RepositoryRoot.TrimEnd("\", "/") + [System.IO.Path]::DirectorySeparatorChar
    if (-not $resolved.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "$Context escapes the repository"
    }
    return $resolved
}

function Convert-PolicyDuration {
    param(
        [Parameter(Mandatory)][string]$Value,
        [Parameter(Mandatory)][string]$Context
    )

    if ($Value -notmatch "^(?<amount>[1-9][0-9]{0,5})(?<unit>[smh])$") {
        throw "$Context must be a positive whole-second, minute, or hour duration"
    }
    $amount = [int]$Matches.amount
    switch ($Matches.unit) {
        "s" { return [TimeSpan]::FromSeconds($amount) }
        "m" { return [TimeSpan]::FromMinutes($amount) }
        "h" { return [TimeSpan]::FromHours($amount) }
    }
}

function Assert-RollbackPolicy {
    param(
        [Parameter(Mandatory)][object]$Policy,
        [Parameter(Mandatory)][string]$ResolvedPolicyPath
    )

    Assert-ExactProperties -Value $Policy -Names @(
        "schema_version",
        "policy_id",
        "policy_version",
        "maintenance_policy",
        "checkpoint_schema_version",
        "lease",
        "journal",
        "apply_order",
        "compensation_order",
        "workloads",
        "timeouts",
        "preservation",
        "resume"
    ) -Context "rollback policy"
    if ($Policy.schema_version -ne 1 -or $Policy.policy_version -lt 1) {
        throw "rollback policy schema_version and policy_version must be positive version 1 contracts"
    }
    Assert-Identifier -Value ([string]$Policy.policy_id) -Context "rollback policy.policy_id"

    Assert-ExactProperties -Value $Policy.maintenance_policy -Names @("path", "version", "sha256") `
        -Context "rollback policy.maintenance_policy"
    Assert-Sha256 -Value ([string]$Policy.maintenance_policy.sha256) `
        -Context "rollback policy.maintenance_policy.sha256"
    if ($Policy.maintenance_policy.version -lt 1 -or $Policy.checkpoint_schema_version -ne 1) {
        throw "rollback policy must pin positive maintenance policy version and checkpoint schema version 1"
    }

    Assert-ExactProperties -Value $Policy.lease -Names @(
        "api_version",
        "name",
        "duration_seconds",
        "renew_interval_seconds",
        "fencing_annotation"
    ) -Context "rollback policy.lease"
    if ($Policy.lease.api_version -ne "coordination.k8s.io/v1") {
        throw "rollback lease must use coordination.k8s.io/v1"
    }
    Assert-Identifier -Value ([string]$Policy.lease.name) -Context "rollback policy.lease.name"
    if (
        $Policy.lease.duration_seconds -lt 15 -or
        $Policy.lease.renew_interval_seconds -lt 1 -or
        $Policy.lease.renew_interval_seconds * 2 -ge $Policy.lease.duration_seconds
    ) {
        throw "rollback lease renew interval must be less than half its duration"
    }
    if ([string]$Policy.lease.fencing_annotation -notmatch "^[a-z0-9.-]+/[a-z0-9.-]+$") {
        throw "rollback lease fencing_annotation must be a canonical Kubernetes annotation"
    }

    Assert-ExactProperties -Value $Policy.journal -Names @(
        "api_version",
        "config_map_name",
        "data_key",
        "schema_version",
        "cas_field",
        "persistent"
    ) -Context "rollback policy.journal"
    if (
        $Policy.journal.api_version -ne "v1" -or
        $Policy.journal.schema_version -ne 1 -or
        $Policy.journal.cas_field -ne "metadata.resourceVersion" -or
        -not $Policy.journal.persistent
    ) {
        throw "rollback journal must be a persistent v1 ConfigMap using metadata.resourceVersion CAS"
    }
    Assert-Identifier -Value ([string]$Policy.journal.config_map_name) `
        -Context "rollback policy.journal.config_map_name"

    $applyOrder = @($Policy.apply_order | ForEach-Object { [string]$_ })
    if (($applyOrder -join "`n") -ne ($script:Services -join "`n")) {
        throw "rollback apply_order must be controller, namesrv, broker, proxy, mcp"
    }
    $expectedCompensation = @($script:Services)
    [array]::Reverse($expectedCompensation)
    if ((@($Policy.compensation_order) -join "`n") -ne ($expectedCompensation -join "`n")) {
        throw "rollback compensation_order must exactly reverse apply_order"
    }
    Assert-ExactProperties -Value $Policy.workloads -Names $script:Services -Context "rollback policy.workloads"
    foreach ($service in $script:Services) {
        if ([string]::IsNullOrWhiteSpace([string]$Policy.workloads.$service)) {
            throw "rollback policy.workloads.$service must be configured"
        }
    }

    Assert-ExactProperties -Value $Policy.timeouts -Names @("operation", "rollout") `
        -Context "rollback policy.timeouts"
    $operationTimeout = Convert-PolicyDuration -Value ([string]$Policy.timeouts.operation) `
        -Context "rollback policy.timeouts.operation"
    $rolloutTimeout = Convert-PolicyDuration -Value ([string]$Policy.timeouts.rollout) `
        -Context "rollback policy.timeouts.rollout"
    if ($rolloutTimeout -ge $operationTimeout) {
        throw "rollback rollout timeout must be shorter than operation timeout"
    }

    Assert-ExactProperties -Value $Policy.preservation -Names @(
        "reuse_persistent_volumes",
        "retain_wal",
        "verify_acknowledged_messages",
        "verify_consumer_offsets",
        "forbid_pvc_delete",
        "forbid_helm_uninstall"
    ) -Context "rollback policy.preservation"
    foreach ($property in $Policy.preservation.PSObject.Properties) {
        if (-not [bool]$property.Value) {
            throw "rollback preservation.$($property.Name) must remain true"
        }
    }
    Assert-ExactProperties -Value $Policy.resume -Names @(
        "idempotent_completed_stages",
        "resume_running",
        "resume_compensating",
        "reject_compensated_operation"
    ) -Context "rollback policy.resume"
    foreach ($property in $Policy.resume.PSObject.Properties) {
        if (-not [bool]$property.Value) {
            throw "rollback resume.$($property.Name) must remain true"
        }
    }

    $resolvedMaintenancePolicyPath = Resolve-RepositoryPath `
        -Path ([string]$Policy.maintenance_policy.path) `
        -Context "rollback policy.maintenance_policy.path"
    if ($resolvedMaintenancePolicyPath -eq $ResolvedPolicyPath) {
        throw "rollback policy cannot reference itself as the maintenance policy"
    }
    return $resolvedMaintenancePolicyPath
}

function Assert-MaintenancePolicy {
    param(
        [Parameter(Mandatory)][object]$Policy,
        [Parameter(Mandatory)][object]$Reference,
        [Parameter(Mandatory)][string]$ResolvedPath
    )

    if ((Get-Sha256 $ResolvedPath) -ne [string]$Reference.sha256) {
        throw "maintenance policy SHA-256 does not match rollback policy reference"
    }
    Assert-ExactProperties -Value $Policy -Names @(
        "schema_version",
        "policy_id",
        "policy_version",
        "require_authentication",
        "require_authorization",
        "require_fencing_token",
        "max_request_lifetime_millis",
        "resource_budget",
        "principal_bindings",
        "role_grants"
    ) -Context "maintenance policy"
    if (
        $Policy.schema_version -ne 1 -or
        $Policy.policy_version -ne $Reference.version -or
        -not $Policy.require_authentication -or
        -not $Policy.require_authorization -or
        -not $Policy.require_fencing_token
    ) {
        throw "maintenance policy must match its pinned version and remain fail closed"
    }
    Assert-Identifier -Value ([string]$Policy.policy_id) -Context "maintenance policy.policy_id"
    if ($Policy.max_request_lifetime_millis -lt 1000 -or $Policy.max_request_lifetime_millis -gt 86400000) {
        throw "maintenance policy request lifetime is outside the supported range"
    }
    Assert-ExactProperties -Value $Policy.resource_budget -Names @(
        "max_checkpoint_bytes",
        "max_store_members",
        "max_concurrent_operations"
    ) -Context "maintenance policy.resource_budget"
    if (
        $Policy.resource_budget.max_checkpoint_bytes -lt 1 -or
        $Policy.resource_budget.max_store_members -lt 1 -or
        $Policy.resource_budget.max_concurrent_operations -ne 1
    ) {
        throw "maintenance policy must provide positive limits and serialize release operations"
    }

    $releaseOperators = @(
        $Policy.principal_bindings |
            Where-Object { @($_.roles) -contains "release_operator" } |
            ForEach-Object { [string]$_.principal }
    )
    if ($releaseOperators.Count -lt 1) {
        throw "maintenance policy must bind at least one release_operator"
    }
    $releaseGrants = @(
        $Policy.role_grants |
            Where-Object { $_.role -eq "release_operator" -and @($_.capabilities) -contains "release_checkpoint" }
    )
    if ($releaseGrants.Count -ne 1) {
        throw "maintenance policy must grant release_checkpoint exactly once to release_operator"
    }
    $adminGrant = @(
        $Policy.role_grants |
            Where-Object { $_.role -eq "administrator" -and @($_.capabilities) -contains "release_checkpoint" }
    )
    if ($adminGrant.Count -ne 0) {
        throw "ordinary administrator must not receive release_checkpoint"
    }
}

function Assert-CheckpointArtifact {
    param(
        [Parameter(Mandatory)][object]$Artifact,
        [Parameter(Mandatory)][object]$Set,
        [Parameter(Mandatory)][string]$Context
    )

    Assert-ExactProperties -Value $Artifact -Names @(
        "schemaVersion",
        "checkpointId",
        "checkpointSetId",
        "generation",
        "barrierId",
        "createdAtUnixMillis",
        "lengthBytes",
        "sha256",
        "uri"
    ) -Context $Context
    if (
        $Artifact.schemaVersion -ne 1 -or
        $Artifact.checkpointSetId -ne $Set.checkpointSetId -or
        $Artifact.generation -ne $Set.generation -or
        $Artifact.barrierId -ne $Set.barrierId -or
        $Artifact.createdAtUnixMillis -lt 1 -or
        $Artifact.lengthBytes -lt 1
    ) {
        throw "$Context does not match the checkpoint set binding"
    }
    Assert-Identifier -Value ([string]$Artifact.checkpointId) -Context "$Context.checkpointId"
    Assert-Sha256 -Value ([string]$Artifact.sha256) -Context "$Context.sha256"
    if ([string]::IsNullOrWhiteSpace([string]$Artifact.uri) -or [string]$Artifact.uri -match "[`r`n]") {
        throw "$Context.uri must be a non-empty single-line URI"
    }
}

function Assert-CheckpointOffsets {
    param(
        [Parameter(Mandatory)][object]$Offsets,
        [Parameter(Mandatory)][string]$Context
    )

    Assert-ExactProperties -Value $Offsets -Names @(
        "appendedOffset",
        "durableOffset",
        "consumeQueueOffset",
        "indexOffset"
    ) -Context $Context
    if (
        $Offsets.appendedOffset -lt 0 -or
        $Offsets.durableOffset -lt 0 -or
        $Offsets.consumeQueueOffset -lt 0 -or
        $Offsets.indexOffset -lt 0 -or
        $Offsets.durableOffset -gt $Offsets.appendedOffset -or
        $Offsets.consumeQueueOffset -gt $Offsets.durableOffset -or
        $Offsets.indexOffset -gt $Offsets.durableOffset
    ) {
        throw "$Context contains invalid durable or derived offsets"
    }
}

function Assert-CheckpointSet {
    param(
        [Parameter(Mandatory)][object]$CheckpointSet,
        [Parameter(Mandatory)][object]$MaintenancePolicy,
        [Parameter(Mandatory)][object]$SourceState
    )

    Assert-ExactProperties -Value $CheckpointSet -Names @(
        "schemaVersion",
        "checkpointSetId",
        "releaseId",
        "generation",
        "barrierId",
        "policyVersion",
        "fencingToken",
        "createdAtUnixMillis",
        "controller",
        "stores"
    ) -Context "checkpoint set"
    if (
        $CheckpointSet.schemaVersion -ne 1 -or
        $CheckpointSet.releaseId -ne $SourceState.release_id -or
        $CheckpointSet.generation -ne $SourceState.storage_generation -or
        $CheckpointSet.policyVersion -ne $MaintenancePolicy.policy_version -or
        $CheckpointSet.fencingToken -lt 1 -or
        $CheckpointSet.createdAtUnixMillis -lt 1
    ) {
        throw "checkpoint set must bind the source ReleaseState, generation, policy, and fencing token"
    }
    Assert-Identifier -Value ([string]$CheckpointSet.checkpointSetId) -Context "checkpoint set.checkpointSetId"
    Assert-Identifier -Value ([string]$CheckpointSet.releaseId) -Context "checkpoint set.releaseId"
    Assert-Identifier -Value ([string]$CheckpointSet.barrierId) -Context "checkpoint set.barrierId"

    Assert-ExactProperties -Value $CheckpointSet.controller -Names @(
        "artifact",
        "snapshotId",
        "lastAppliedIndex",
        "lastAppliedTerm",
        "voterIds"
    ) -Context "checkpoint set.controller"
    Assert-CheckpointArtifact -Artifact $CheckpointSet.controller.artifact -Set $CheckpointSet `
        -Context "checkpoint set.controller.artifact"
    Assert-Identifier -Value ([string]$CheckpointSet.controller.snapshotId) `
        -Context "checkpoint set.controller.snapshotId"
    if (
        $CheckpointSet.controller.lastAppliedIndex -lt 1 -or
        $CheckpointSet.controller.lastAppliedTerm -lt 1 -or
        @($CheckpointSet.controller.voterIds).Count -lt 1
    ) {
        throw "checkpoint set Controller snapshot must contain applied state and voters"
    }
    $uniqueVoters = @($CheckpointSet.controller.voterIds | Sort-Object -Unique)
    if ($uniqueVoters.Count -ne @($CheckpointSet.controller.voterIds).Count -or $uniqueVoters -contains 0) {
        throw "checkpoint set Controller voters must be unique and non-zero"
    }

    $stores = @($CheckpointSet.stores)
    if ($stores.Count -lt 1 -or $stores.Count -gt $MaintenancePolicy.resource_budget.max_store_members) {
        throw "checkpoint set Store membership is empty or exceeds the maintenance budget"
    }
    $members = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($store in $stores) {
        Assert-ExactProperties -Value $store -Names @(
            "artifact",
            "memberId",
            "backend",
            "offsets",
            "storageIdentity",
            "walRetained",
            "persistentVolumeRetained"
        ) -Context "checkpoint set.store"
        Assert-CheckpointArtifact -Artifact $store.artifact -Set $CheckpointSet `
            -Context "checkpoint set.store.artifact"
        Assert-Identifier -Value ([string]$store.memberId) -Context "checkpoint set.store.memberId"
        if (-not $members.Add([string]$store.memberId)) {
            throw "checkpoint set repeats Store member '$($store.memberId)'"
        }
        if ($store.backend -notin @("local", "rocks_db")) {
            throw "checkpoint set Store backend must be local or rocks_db"
        }
        Assert-CheckpointOffsets -Offsets $store.offsets -Context "checkpoint set.store.offsets"
        Assert-ExactProperties -Value $store.storageIdentity -Names @("volumeId", "walGeneration") `
            -Context "checkpoint set.store.storageIdentity"
        Assert-Identifier -Value ([string]$store.storageIdentity.volumeId) `
            -Context "checkpoint set.store.storageIdentity.volumeId"
        if (
            $store.storageIdentity.walGeneration -ne $CheckpointSet.generation -or
            -not $store.walRetained -or
            -not $store.persistentVolumeRetained
        ) {
            throw "checkpoint set Store must retain WAL/PVC at the checkpoint generation"
        }
    }
}

function Invoke-ReleaseStateValidation {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Context
    )

    $validator = Join-Path $PSScriptRoot "set-architecture-release-state.ps1"
    $powershell = (Get-Process -Id $PID).Path
    $output = & $powershell -NoProfile -ExecutionPolicy Bypass -File $validator `
        -StatePath $Path `
        -ValidateOnly `
        -SchemaOnly 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "$Context failed complete ReleaseState schema validation: $($output.Trim())"
    }
}

function Get-PendingApplyStages {
    param(
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]]$CompletedStages
    )

    if ($CompletedStages.Count -gt $script:Services.Count) {
        throw "journal contains more completed stages than the rollback policy"
    }
    for ($index = 0; $index -lt $CompletedStages.Count; $index++) {
        if ($CompletedStages[$index] -ne $script:Services[$index]) {
            throw "journal completed_stages must be an apply-order prefix"
        }
    }
    return @($script:Services | Select-Object -Skip $CompletedStages.Count)
}

function Get-PendingCompensationStages {
    param(
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]]$CompletedStages,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]]$CompensatedStages
    )

    [void](Get-PendingApplyStages -CompletedStages $CompletedStages)
    $expected = @($CompletedStages)
    [array]::Reverse($expected)
    if ($CompensatedStages.Count -gt $expected.Count) {
        throw "journal contains more compensated stages than completed stages"
    }
    for ($index = 0; $index -lt $CompensatedStages.Count; $index++) {
        if ($CompensatedStages[$index] -ne $expected[$index]) {
            throw "journal compensated_stages must be a reverse-order prefix"
        }
    }
    return @($expected | Select-Object -Skip $CompensatedStages.Count)
}

function Test-RollbackStateMachine {
    foreach ($completedCount in 0..$script:Services.Count) {
        $completed = @($script:Services | Select-Object -First $completedCount)
        $pendingApply = @(Get-PendingApplyStages -CompletedStages $completed)
        if ($pendingApply.Count -ne $script:Services.Count - $completedCount) {
            throw "state-machine resume test failed after $completedCount stages"
        }

        $expectedCompensation = @($completed)
        [array]::Reverse($expectedCompensation)
        foreach ($compensatedCount in 0..$completedCount) {
            $compensated = @($expectedCompensation | Select-Object -First $compensatedCount)
            $pendingCompensation = @(
                Get-PendingCompensationStages `
                    -CompletedStages $completed `
                    -CompensatedStages $compensated
            )
            if ($pendingCompensation.Count -ne $completedCount - $compensatedCount) {
                throw "state-machine compensation resume test failed after $completedCount stages"
            }
        }
    }

    foreach ($failureAfterStage in 0..($script:Services.Count - 1)) {
        $completed = @($script:Services | Select-Object -First ($failureAfterStage + 1))
        $compensated = [System.Collections.Generic.List[string]]::new()
        foreach ($stage in @(Get-PendingCompensationStages -CompletedStages $completed -CompensatedStages @())) {
            $compensated.Add($stage)
        }
        if (
            $compensated.Count -ne $completed.Count -or
            (@(Get-PendingCompensationStages -CompletedStages $completed -CompensatedStages $compensated).Count -ne 0)
        ) {
            throw "failure-after-stage state-machine test failed for $($script:Services[$failureAfterStage])"
        }
    }
}

function Invoke-Captured {
    param(
        [Parameter(Mandatory)][string]$Executable,
        [Parameter(Mandatory)][string[]]$Arguments,
        [switch]$AllowEmpty
    )

    $output = & $Executable @Arguments 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "$Executable failed with exit code $LASTEXITCODE`: $($output.Trim())"
    }
    $trimmed = $output.Trim()
    if (-not $AllowEmpty -and [string]::IsNullOrWhiteSpace($trimmed)) {
        throw "$Executable returned an empty response"
    }
    return $trimmed
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory)][string]$Executable,
        [Parameter(Mandatory)][string[]]$Arguments
    )

    [void](Invoke-Captured -Executable $Executable -Arguments $Arguments -AllowEmpty)
}

function Invoke-KubectlObjectMutation {
    param(
        [Parameter(Mandatory)][string]$Kubectl,
        [Parameter(Mandatory)][ValidateSet("create", "replace")][string]$Verb,
        [Parameter(Mandatory)][object]$Value
    )

    $temporaryPath = Join-Path ([System.IO.Path]::GetTempPath()) "rocketmq-rollback-$([Guid]::NewGuid().ToString('N')).json"
    try {
        Write-JsonFile -Value $Value -Path $temporaryPath
        $output = Invoke-Captured -Executable $Kubectl -Arguments @(
            $Verb,
            "--filename",
            $temporaryPath,
            "--output",
            "json"
        )
        return $output | ConvertFrom-Json
    }
    finally {
        if (Test-Path -LiteralPath $temporaryPath) {
            Remove-Item -LiteralPath $temporaryPath -Force
        }
    }
}

function Get-KubernetesObject {
    param(
        [Parameter(Mandatory)][string]$Kubectl,
        [Parameter(Mandatory)][string]$Kind,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Namespace
    )

    $output = Invoke-Captured -Executable $Kubectl -Arguments @(
        "get",
        $Kind,
        $Name,
        "--namespace",
        $Namespace,
        "--ignore-not-found",
        "--output",
        "json"
    ) -AllowEmpty
    if ([string]::IsNullOrWhiteSpace($output)) {
        return $null
    }
    return $output | ConvertFrom-Json
}

function Get-JournalEnvelope {
    param(
        [Parameter(Mandatory)][string]$Kubectl,
        [Parameter(Mandatory)][object]$Policy,
        [Parameter(Mandatory)][string]$Namespace
    )

    $configMap = Get-KubernetesObject `
        -Kubectl $Kubectl `
        -Kind "configmap" `
        -Name ([string]$Policy.journal.config_map_name) `
        -Namespace $Namespace
    if ($null -eq $configMap) {
        return $null
    }
    $dataKey = [string]$Policy.journal.data_key
    $journalJson = [string]$configMap.data.$dataKey
    if ([string]::IsNullOrWhiteSpace($journalJson)) {
        throw "persistent rollback journal does not contain '$dataKey'"
    }
    try {
        $record = $journalJson | ConvertFrom-Json
    }
    catch {
        throw "persistent rollback journal is invalid JSON"
    }
    return [pscustomobject]@{
        resource = $configMap
        record = $record
    }
}

function Assert-JournalRecord {
    param(
        [Parameter(Mandatory)][object]$Record,
        [Parameter(Mandatory)][object]$Policy
    )

    Assert-ExactProperties -Value $Record -Names @(
        "schema_version",
        "operation_id",
        "direction",
        "status",
        "source_release_id",
        "target_release_id",
        "checkpoint_set_id",
        "fencing_token",
        "policy_version",
        "policy_sha256",
        "started_at",
        "updated_at",
        "completed_stages",
        "compensated_stages",
        "persistent_volume_uids",
        "failure"
    ) -Context "rollback journal"
    if (
        $Record.schema_version -ne $Policy.journal.schema_version -or
        $Record.direction -notin @("Rollback", "Forward") -or
        $Record.status -notin @("running", "compensating", "compensated", "completed") -or
        $Record.fencing_token -lt 1 -or
        $Record.policy_version -ne $Policy.policy_version
    ) {
        throw "rollback journal contains an invalid schema, direction, status, fence, or policy version"
    }
    Assert-Identifier -Value ([string]$Record.operation_id) -Context "rollback journal.operation_id"
    Assert-Sha256 -Value ([string]$Record.policy_sha256) -Context "rollback journal.policy_sha256"
    [void](Get-PendingApplyStages -CompletedStages @($Record.completed_stages))
    [void](Get-PendingCompensationStages `
        -CompletedStages @($Record.completed_stages) `
        -CompensatedStages @($Record.compensated_stages))
}

function Save-JournalRecord {
    param(
        [Parameter(Mandatory)][string]$Kubectl,
        [Parameter(Mandatory)][object]$Policy,
        [Parameter(Mandatory)][string]$Namespace,
        [AllowNull()][object]$Envelope,
        [Parameter(Mandatory)][object]$Record
    )

    Assert-JournalRecord -Record $Record -Policy $Policy
    $dataKey = [string]$Policy.journal.data_key
    $recordJson = $Record | ConvertTo-Json -Depth 16 -Compress
    if ($null -eq $Envelope) {
        $resource = [ordered]@{
            apiVersion = "v1"
            kind = "ConfigMap"
            metadata = [ordered]@{
                name = [string]$Policy.journal.config_map_name
                namespace = $Namespace
                labels = [ordered]@{
                    "app.kubernetes.io/managed-by" = "rocketmq-release-rollback"
                    "rocketmq.apache.org/persistent-journal" = "true"
                }
            }
            immutable = $false
            data = [ordered]@{
                $dataKey = $recordJson
            }
        }
        $saved = Invoke-KubectlObjectMutation -Kubectl $Kubectl -Verb "create" -Value $resource
    }
    else {
        $resource = $Envelope.resource
        $resource.data.$dataKey = $recordJson
        $saved = Invoke-KubectlObjectMutation -Kubectl $Kubectl -Verb "replace" -Value $resource
    }
    return [pscustomobject]@{
        resource = $saved
        record = $Record
    }
}

function Test-LeaseActive {
    param(
        [Parameter(Mandatory)][object]$Lease,
        [Parameter(Mandatory)][DateTimeOffset]$Now
    )

    if ([string]::IsNullOrWhiteSpace([string]$Lease.spec.holderIdentity)) {
        return $false
    }
    $renewTime = [DateTimeOffset]::Parse([string]$Lease.spec.renewTime)
    $expiresAt = $renewTime.AddSeconds([int]$Lease.spec.leaseDurationSeconds)
    return $expiresAt -gt $Now
}

function Acquire-RollbackLease {
    param(
        [Parameter(Mandatory)][string]$Kubectl,
        [Parameter(Mandatory)][object]$Policy,
        [Parameter(Mandatory)][string]$Namespace,
        [Parameter(Mandatory)][string]$HolderIdentity,
        [Parameter(Mandatory)][string]$OperationId,
        [Parameter(Mandatory)][uint64]$FencingToken,
        [Parameter(Mandatory)][bool]$AllowResume
    )

    $now = [DateTimeOffset]::UtcNow
    $lease = Get-KubernetesObject `
        -Kubectl $Kubectl `
        -Kind "lease" `
        -Name ([string]$Policy.lease.name) `
        -Namespace $Namespace
    $fenceAnnotation = [string]$Policy.lease.fencing_annotation
    if ($null -eq $lease) {
        $resource = [ordered]@{
            apiVersion = [string]$Policy.lease.api_version
            kind = "Lease"
            metadata = [ordered]@{
                name = [string]$Policy.lease.name
                namespace = $Namespace
                annotations = [ordered]@{
                    $fenceAnnotation = [string]$FencingToken
                    "rocketmq.apache.org/operation-id" = $OperationId
                }
            }
            spec = [ordered]@{
                holderIdentity = $HolderIdentity
                leaseDurationSeconds = [int]$Policy.lease.duration_seconds
                acquireTime = $now.ToString("o")
                renewTime = $now.ToString("o")
                leaseTransitions = 1
            }
        }
        return Invoke-KubectlObjectMutation -Kubectl $Kubectl -Verb "create" -Value $resource
    }

    if (Test-LeaseActive -Lease $lease -Now $now) {
        throw "rollback Lease is currently held by '$($lease.spec.holderIdentity)'"
    }
    $currentFence = 0L
    if ($null -ne $lease.metadata.annotations -and $null -ne $lease.metadata.annotations.$fenceAnnotation) {
        $currentFence = [uint64]$lease.metadata.annotations.$fenceAnnotation
    }
    if ($FencingToken -lt $currentFence -or ($FencingToken -eq $currentFence -and -not $AllowResume)) {
        throw "checkpoint fencing token $FencingToken is stale; current Lease token is $currentFence"
    }
    if ($FencingToken -eq $currentFence) {
        $leaseOperation = [string]$lease.metadata.annotations."rocketmq.apache.org/operation-id"
        if ($leaseOperation -ne $OperationId) {
            throw "equal fencing token belongs to a different rollback operation"
        }
    }

    if ($null -eq $lease.metadata.annotations) {
        $lease.metadata | Add-Member -MemberType NoteProperty -Name annotations -Value ([pscustomobject]@{})
    }
    $lease.metadata.annotations | Add-Member `
        -MemberType NoteProperty `
        -Name $fenceAnnotation `
        -Value ([string]$FencingToken) `
        -Force
    $lease.metadata.annotations | Add-Member `
        -MemberType NoteProperty `
        -Name "rocketmq.apache.org/operation-id" `
        -Value $OperationId `
        -Force
    $lease.spec.holderIdentity = $HolderIdentity
    $lease.spec.leaseDurationSeconds = [int]$Policy.lease.duration_seconds
    $lease.spec.acquireTime = $now.ToString("o")
    $lease.spec.renewTime = $now.ToString("o")
    $lease.spec.leaseTransitions = [int]$lease.spec.leaseTransitions + 1
    return Invoke-KubectlObjectMutation -Kubectl $Kubectl -Verb "replace" -Value $lease
}

function Renew-RollbackLease {
    param(
        [Parameter(Mandatory)][string]$Kubectl,
        [Parameter(Mandatory)][object]$Policy,
        [Parameter(Mandatory)][string]$Namespace,
        [Parameter(Mandatory)][string]$HolderIdentity,
        [Parameter(Mandatory)][uint64]$FencingToken
    )

    $lease = Get-KubernetesObject `
        -Kubectl $Kubectl `
        -Kind "lease" `
        -Name ([string]$Policy.lease.name) `
        -Namespace $Namespace
    if ($null -eq $lease -or $lease.spec.holderIdentity -ne $HolderIdentity) {
        throw "rollback runner lost Lease ownership"
    }
    $fenceAnnotation = [string]$Policy.lease.fencing_annotation
    if ([uint64]$lease.metadata.annotations.$fenceAnnotation -ne $FencingToken) {
        throw "rollback runner was fenced by a newer Lease token"
    }
    $lease.spec.renewTime = [DateTimeOffset]::UtcNow.ToString("o")
    return Invoke-KubectlObjectMutation -Kubectl $Kubectl -Verb "replace" -Value $lease
}

function Release-RollbackLease {
    param(
        [Parameter(Mandatory)][string]$Kubectl,
        [Parameter(Mandatory)][object]$Policy,
        [Parameter(Mandatory)][string]$Namespace,
        [Parameter(Mandatory)][string]$HolderIdentity,
        [Parameter(Mandatory)][uint64]$FencingToken
    )

    try {
        $lease = Get-KubernetesObject `
            -Kubectl $Kubectl `
            -Kind "lease" `
            -Name ([string]$Policy.lease.name) `
            -Namespace $Namespace
        $fenceAnnotation = [string]$Policy.lease.fencing_annotation
        if (
            $null -eq $lease -or
            $lease.spec.holderIdentity -ne $HolderIdentity -or
            [uint64]$lease.metadata.annotations.$fenceAnnotation -ne $FencingToken
        ) {
            return
        }
        $lease.spec.holderIdentity = ""
        $lease.spec.renewTime = [DateTimeOffset]::UtcNow.ToString("o")
        [void](Invoke-KubectlObjectMutation -Kubectl $Kubectl -Verb "replace" -Value $lease)
    }
    catch {
        Write-Warning "could not release rollback Lease safely: $($_.Exception.Message)"
    }
}

function Get-PersistentVolumeUids {
    param(
        [Parameter(Mandatory)][string]$Kubectl,
        [Parameter(Mandatory)][string]$Namespace
    )

    $output = Invoke-Captured -Executable $Kubectl -Arguments @(
        "get",
        "persistentvolumeclaims",
        "--namespace",
        $Namespace,
        "--output",
        "json"
    )
    $list = $output | ConvertFrom-Json
    $identities = [ordered]@{}
    foreach ($item in @($list.items | Sort-Object { $_.metadata.name })) {
        $name = [string]$item.metadata.name
        if (
            $name -like "data-rocketmq-broker-*" -or
            $name -like "data-rocketmq-controller-*" -or
            $name -like "data-rocketmq-namesrv-*" -or
            $name -eq "rocketmq-mcp-audit"
        ) {
            $identities[$name] = [string]$item.metadata.uid
        }
    }
    if (
        @($identities.Keys | Where-Object { $_ -like "data-rocketmq-broker-*" }).Count -lt 1 -or
        @($identities.Keys | Where-Object { $_ -like "data-rocketmq-controller-*" }).Count -lt 1
    ) {
        throw "rollback requires existing Broker and Controller persistent volumes"
    }
    return [pscustomobject]$identities
}

function Assert-PersistentVolumesUnchanged {
    param(
        [Parameter(Mandatory)][object]$Expected,
        [Parameter(Mandatory)][object]$Actual
    )

    $expectedNames = @($Expected.PSObject.Properties.Name | Sort-Object)
    $actualNames = @($Actual.PSObject.Properties.Name | Sort-Object)
    if (($expectedNames -join "`n") -ne ($actualNames -join "`n")) {
        throw "persistent volume membership changed during ReleaseState transition"
    }
    foreach ($name in $expectedNames) {
        if ([string]$Expected.$name -ne [string]$Actual.$name) {
            throw "persistent volume '$name' was replaced during ReleaseState transition"
        }
    }
}

function Add-ReleaseStateArguments {
    param(
        [Parameter(Mandatory)][System.Collections.Generic.List[string]]$Arguments,
        [Parameter(Mandatory)][object]$DesiredState,
        [Parameter(Mandatory)][object]$SourceState,
        [Parameter(Mandatory)][object]$TargetState,
        [Parameter(Mandatory)][string[]]$TargetStages
    )

    foreach ($argument in @(
        "--set-string",
        "releaseIdentity.commit=$($DesiredState.identity.commit)",
        "--set-string",
        "releaseIdentity.nonce=$($DesiredState.identity.nonce)",
        "--set-string",
        "releaseIdentity.configDigest=$($DesiredState.identity.config_digest)",
        "--set-string",
        "releaseIdentity.secretVersion=$($DesiredState.identity.secret_version)",
        "--set",
        "releaseIdentity.storageGeneration=$($DesiredState.storage_generation)"
    )) {
        $Arguments.Add([string]$argument)
    }

    foreach ($service in $script:Services) {
        $state = if ($TargetStages -contains $service) { $TargetState } else { $SourceState }
        $reference = [string]$state.images.$service.reference
        $separator = $reference.LastIndexOf(":")
        if ($separator -le $reference.LastIndexOf("/")) {
            throw "ReleaseState image reference for $service must use an explicit local tag"
        }
        $repository = $reference.Substring(0, $separator)
        $tag = $reference.Substring($separator + 1)
        foreach ($argument in @(
            "--set-string",
            "services.$service.image.repository=$repository",
            "--set-string",
            "services.$service.image.tag=$tag",
            "--set-string",
            "services.$service.image.digest=",
            "--set-string",
            "services.$service.image.pullPolicy=Never"
        )) {
            $Arguments.Add([string]$argument)
        }
    }
}

function Invoke-ReleaseStage {
    param(
        [Parameter(Mandatory)][string]$Helm,
        [Parameter(Mandatory)][string]$Kubectl,
        [Parameter(Mandatory)][object]$Policy,
        [Parameter(Mandatory)][object]$SourceState,
        [Parameter(Mandatory)][object]$TargetState,
        [Parameter(Mandatory)][string[]]$TargetStages,
        [Parameter(Mandatory)][string]$Stage,
        [Parameter(Mandatory)][bool]$Compensating,
        [Parameter(Mandatory)][string]$ReleaseName,
        [Parameter(Mandatory)][string]$Namespace
    )

    $desiredState = if ($Compensating) { $SourceState } else { $TargetState }
    $valuesPath = Resolve-RepositoryPath `
        -Path ([string]$desiredState.config_bundle.helm_values) `
        -Context "ReleaseState.config_bundle.helm_values"
    $chartPath = Join-Path $script:RepositoryRoot "distribution/helm/rocketmq-rust"
    $arguments = [System.Collections.Generic.List[string]]::new()
    foreach ($argument in @(
        "upgrade",
        $ReleaseName,
        $chartPath,
        "--namespace",
        $Namespace,
        "--values",
        $valuesPath,
        "--timeout",
        [string]$Policy.timeouts.rollout
    )) {
        $arguments.Add([string]$argument)
    }
    Add-ReleaseStateArguments `
        -Arguments $arguments `
        -DesiredState $desiredState `
        -SourceState $SourceState `
        -TargetState $TargetState `
        -TargetStages $TargetStages
    Invoke-Checked -Executable $Helm -Arguments $arguments
    Invoke-Checked -Executable $Kubectl -Arguments @(
        "rollout",
        "status",
        [string]$Policy.workloads.$Stage,
        "--namespace",
        $Namespace,
        "--timeout=$($Policy.timeouts.rollout)"
    )
}

function Wait-PreservationProof {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][object]$CheckpointSet,
        [Parameter(Mandatory)][object]$TargetState,
        [Parameter(Mandatory)][DateTimeOffset]$StartedAt,
        [Parameter(Mandatory)][DateTimeOffset]$Deadline,
        [Parameter(Mandatory)][scriptblock]$RenewLease
    )

    $lastRenewal = [DateTimeOffset]::MinValue
    while ([DateTimeOffset]::UtcNow -lt $Deadline) {
        $now = [DateTimeOffset]::UtcNow
        if (($now - $lastRenewal).TotalSeconds -ge 10) {
            & $RenewLease
            $lastRenewal = $now
        }
        if (Test-Path -LiteralPath $Path -PathType Leaf) {
            $proof = Read-JsonFile -Path $Path -Context "post-transition preservation proof"
            Assert-ExactProperties -Value $proof -Names @(
                "schema_version",
                "checkpoint_set_id",
                "target_release_id",
                "generation",
                "fencing_token",
                "verified_at",
                "acknowledged_messages_preserved",
                "consumer_offsets_preserved",
                "wal_retained",
                "persistent_volumes_reused",
                "store_checkpoint_ids"
            ) -Context "post-transition preservation proof"
            $verifiedAt = [DateTimeOffset]::Parse([string]$proof.verified_at)
            if (
                $proof.schema_version -ne 1 -or
                $proof.checkpoint_set_id -ne $CheckpointSet.checkpointSetId -or
                $proof.target_release_id -ne $TargetState.release_id -or
                $proof.generation -ne $CheckpointSet.generation -or
                $proof.fencing_token -ne $CheckpointSet.fencingToken -or
                $verifiedAt -lt $StartedAt -or
                -not $proof.acknowledged_messages_preserved -or
                -not $proof.consumer_offsets_preserved -or
                -not $proof.wal_retained -or
                -not $proof.persistent_volumes_reused
            ) {
                throw "post-transition proof does not establish every storage preservation invariant"
            }
            $expectedIds = @(
                $CheckpointSet.stores |
                    ForEach-Object { [string]$_.artifact.checkpointId } |
                    Sort-Object
            )
            $actualIds = @($proof.store_checkpoint_ids | ForEach-Object { [string]$_ } | Sort-Object)
            if (($expectedIds -join "`n") -ne ($actualIds -join "`n")) {
                throw "post-transition proof does not cover every Store checkpoint"
            }
            return
        }
        Start-Sleep -Seconds 2
    }
    throw "post-transition preservation proof was not produced before the operation deadline"
}

$resolvedPolicyPath = Resolve-RepositoryPath -Path $PolicyPath -Context "PolicyPath"
$rollbackPolicy = Read-JsonFile -Path $resolvedPolicyPath -Context "rollback policy"
$resolvedMaintenancePolicyPath = Assert-RollbackPolicy `
    -Policy $rollbackPolicy `
    -ResolvedPolicyPath $resolvedPolicyPath
$maintenancePolicy = Read-JsonFile -Path $resolvedMaintenancePolicyPath -Context "maintenance policy"
Assert-MaintenancePolicy `
    -Policy $maintenancePolicy `
    -Reference $rollbackPolicy.maintenance_policy `
    -ResolvedPath $resolvedMaintenancePolicyPath
Test-RollbackStateMachine

$artifactPaths = @(
    @($BaselineStatePath, $CandidateStatePath, $CheckpointSetPath) |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
)
if ($artifactPaths.Count -notin @(0, 3)) {
    throw "BaselineStatePath, CandidateStatePath, and CheckpointSetPath must be supplied together"
}

$baselineState = $null
$candidateState = $null
$checkpointSet = $null
$sourceState = $null
$targetState = $null
if ($artifactPaths.Count -eq 3) {
    $resolvedBaselineStatePath = Resolve-RepositoryPath -Path $BaselineStatePath -Context "BaselineStatePath"
    $resolvedCandidateStatePath = Resolve-RepositoryPath -Path $CandidateStatePath -Context "CandidateStatePath"
    $resolvedCheckpointSetPath = Resolve-RepositoryPath -Path $CheckpointSetPath -Context "CheckpointSetPath"
    Invoke-ReleaseStateValidation -Path $resolvedBaselineStatePath -Context "baseline ReleaseState"
    Invoke-ReleaseStateValidation -Path $resolvedCandidateStatePath -Context "candidate ReleaseState"
    $baselineState = Read-JsonFile -Path $resolvedBaselineStatePath -Context "baseline ReleaseState"
    $candidateState = Read-JsonFile -Path $resolvedCandidateStatePath -Context "candidate ReleaseState"
    if ($baselineState.release_id -eq $candidateState.release_id) {
        throw "baseline and candidate ReleaseState IDs must differ"
    }
    if ($baselineState.storage_generation -ne $candidateState.storage_generation) {
        throw "non-destructive transition requires equal baseline and candidate storage generations"
    }
    if ($Direction -eq "Rollback") {
        $sourceState = $candidateState
        $targetState = $baselineState
    }
    else {
        $sourceState = $baselineState
        $targetState = $candidateState
    }
    $checkpointSet = Read-JsonFile -Path $resolvedCheckpointSetPath -Context "checkpoint set"
    Assert-CheckpointSet `
        -CheckpointSet $checkpointSet `
        -MaintenancePolicy $maintenancePolicy `
        -SourceState $sourceState
}

if (-not $Apply) {
    $artifactMode = if ($artifactPaths.Count -eq 3) { "complete" } else { "policy" }
    Write-Host "RELEASE_ROLLBACK_VALIDATION_OK mode=$artifactMode policy_version=$($rollbackPolicy.policy_version) failure_cases=5 resume_prefixes=6 compensation_prefixes=21"
    exit 0
}

if ($artifactPaths.Count -ne 3) {
    throw "-Apply requires baseline, candidate, and checkpoint-set paths"
}
if ([string]::IsNullOrWhiteSpace($PreservationProofPath)) {
    throw "-Apply requires PreservationProofPath for post-transition message, offset, WAL, and PVC verification"
}
if ([string]::IsNullOrWhiteSpace($OperatorIdentity)) {
    throw "-Apply requires an authenticated release-operator identity"
}
Assert-Identifier -Value $OperatorIdentity -Context "OperatorIdentity"
foreach ($command in @("helm", "kubectl")) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "required rollback command is unavailable: $command"
    }
}

$helm = (Get-Command helm).Source
$kubectl = (Get-Command kubectl).Source
$policySha256 = Get-Sha256 $resolvedPolicyPath
$operationSeed = @(
    $Direction,
    $sourceState.release_id,
    $targetState.release_id,
    $checkpointSet.checkpointSetId,
    $checkpointSet.fencingToken,
    $policySha256
) -join ":"
$operationId = "rollback-" + (Get-TextSha256 $operationSeed).Substring(0, 32)
$holderIdentity = "$OperatorIdentity/$([Guid]::NewGuid().ToString('N'))"
$startedAt = [DateTimeOffset]::UtcNow
$deadline = $startedAt + (Convert-PolicyDuration `
    -Value ([string]$rollbackPolicy.timeouts.operation) `
    -Context "rollback policy.timeouts.operation")
$journalEnvelope = Get-JournalEnvelope `
    -Kubectl $kubectl `
    -Policy $rollbackPolicy `
    -Namespace $Namespace
$allowResume = $false
if ($null -ne $journalEnvelope) {
    Assert-JournalRecord -Record $journalEnvelope.record -Policy $rollbackPolicy
    if ($journalEnvelope.record.operation_id -eq $operationId) {
        if ($journalEnvelope.record.status -eq "completed") {
            Write-Host "RELEASE_ROLLBACK_ALREADY_COMPLETE operation_id=$operationId"
            exit 0
        }
        if ($journalEnvelope.record.status -eq "compensated") {
            throw "operation $operationId was compensated; create a new fenced checkpoint set before retrying"
        }
        if (
            $journalEnvelope.record.source_release_id -ne $sourceState.release_id -or
            $journalEnvelope.record.target_release_id -ne $targetState.release_id -or
            $journalEnvelope.record.checkpoint_set_id -ne $checkpointSet.checkpointSetId -or
            $journalEnvelope.record.fencing_token -ne $checkpointSet.fencingToken -or
            $journalEnvelope.record.policy_sha256 -ne $policySha256
        ) {
            throw "persistent journal operation binding does not match the requested transition"
        }
        $allowResume = $true
        $startedAt = [DateTimeOffset]::Parse([string]$journalEnvelope.record.started_at)
        $deadline = [DateTimeOffset]::UtcNow + (Convert-PolicyDuration `
            -Value ([string]$rollbackPolicy.timeouts.operation) `
            -Context "rollback policy.timeouts.operation")
    }
    elseif ($journalEnvelope.record.status -in @("running", "compensating")) {
        throw "a different rollback operation is still recoverable in the persistent journal"
    }
}

$lease = Acquire-RollbackLease `
    -Kubectl $kubectl `
    -Policy $rollbackPolicy `
    -Namespace $Namespace `
    -HolderIdentity $holderIdentity `
    -OperationId $operationId `
    -FencingToken ([uint64]$checkpointSet.fencingToken) `
    -AllowResume $allowResume

try {
    if (-not $allowResume) {
        $persistentVolumeUids = Get-PersistentVolumeUids -Kubectl $kubectl -Namespace $Namespace
        $record = [ordered]@{
            schema_version = 1
            operation_id = $operationId
            direction = $Direction
            status = "running"
            source_release_id = [string]$sourceState.release_id
            target_release_id = [string]$targetState.release_id
            checkpoint_set_id = [string]$checkpointSet.checkpointSetId
            fencing_token = [uint64]$checkpointSet.fencingToken
            policy_version = [uint64]$rollbackPolicy.policy_version
            policy_sha256 = $policySha256
            started_at = $startedAt.ToString("o")
            updated_at = $startedAt.ToString("o")
            completed_stages = @()
            compensated_stages = @()
            persistent_volume_uids = $persistentVolumeUids
            failure = $null
        }
        $journalEnvelope = Save-JournalRecord `
            -Kubectl $kubectl `
            -Policy $rollbackPolicy `
            -Namespace $Namespace `
            -Envelope $journalEnvelope `
            -Record $record
    }
    else {
        $record = $journalEnvelope.record
    }

    try {
        if ($record.status -eq "compensating") {
            throw "resuming compensation recorded by a previous runner: $($record.failure)"
        }
        foreach ($stage in @(Get-PendingApplyStages -CompletedStages @($record.completed_stages))) {
            if ([DateTimeOffset]::UtcNow -ge $deadline) {
                throw "rollback operation deadline expired before stage '$stage'"
            }
            $lease = Renew-RollbackLease `
                -Kubectl $kubectl `
                -Policy $rollbackPolicy `
                -Namespace $Namespace `
                -HolderIdentity $holderIdentity `
                -FencingToken ([uint64]$checkpointSet.fencingToken)
            $targetStages = @($record.completed_stages) + @($stage)
            Invoke-ReleaseStage `
                -Helm $helm `
                -Kubectl $kubectl `
                -Policy $rollbackPolicy `
                -SourceState $sourceState `
                -TargetState $targetState `
                -TargetStages $targetStages `
                -Stage $stage `
                -Compensating $false `
                -ReleaseName $ReleaseName `
                -Namespace $Namespace
            $actualVolumes = Get-PersistentVolumeUids -Kubectl $kubectl -Namespace $Namespace
            Assert-PersistentVolumesUnchanged `
                -Expected $record.persistent_volume_uids `
                -Actual $actualVolumes
            $record.completed_stages = $targetStages
            $record.updated_at = [DateTimeOffset]::UtcNow.ToString("o")
            $journalEnvelope = Save-JournalRecord `
                -Kubectl $kubectl `
                -Policy $rollbackPolicy `
                -Namespace $Namespace `
                -Envelope $journalEnvelope `
                -Record $record
        }

        $resolvedProofPath = if ([System.IO.Path]::IsPathRooted($PreservationProofPath)) {
            [System.IO.Path]::GetFullPath($PreservationProofPath)
        }
        else {
            [System.IO.Path]::GetFullPath((Join-Path $script:RepositoryRoot $PreservationProofPath))
        }
        Wait-PreservationProof `
            -Path $resolvedProofPath `
            -CheckpointSet $checkpointSet `
            -TargetState $targetState `
            -StartedAt $startedAt `
            -Deadline $deadline `
            -RenewLease {
                $script:lease = Renew-RollbackLease `
                    -Kubectl $kubectl `
                    -Policy $rollbackPolicy `
                    -Namespace $Namespace `
                    -HolderIdentity $holderIdentity `
                    -FencingToken ([uint64]$checkpointSet.fencingToken)
            }
        $record.status = "completed"
        $record.updated_at = [DateTimeOffset]::UtcNow.ToString("o")
        $journalEnvelope = Save-JournalRecord `
            -Kubectl $kubectl `
            -Policy $rollbackPolicy `
            -Namespace $Namespace `
            -Envelope $journalEnvelope `
            -Record $record
    }
    catch {
        $failure = $_
        if ($record.status -ne "compensating") {
            $record.status = "compensating"
            $record.failure = $failure.Exception.Message
            $record.updated_at = [DateTimeOffset]::UtcNow.ToString("o")
            $journalEnvelope = Save-JournalRecord `
                -Kubectl $kubectl `
                -Policy $rollbackPolicy `
                -Namespace $Namespace `
                -Envelope $journalEnvelope `
                -Record $record
        }

        foreach ($stage in @(
            Get-PendingCompensationStages `
                -CompletedStages @($record.completed_stages) `
                -CompensatedStages @($record.compensated_stages)
        )) {
            $lease = Renew-RollbackLease `
                -Kubectl $kubectl `
                -Policy $rollbackPolicy `
                -Namespace $Namespace `
                -HolderIdentity $holderIdentity `
                -FencingToken ([uint64]$checkpointSet.fencingToken)
            $remainingTargetStages = @($record.completed_stages | Where-Object {
                $_ -ne $stage -and @($record.compensated_stages) -notcontains $_
            })
            Invoke-ReleaseStage `
                -Helm $helm `
                -Kubectl $kubectl `
                -Policy $rollbackPolicy `
                -SourceState $sourceState `
                -TargetState $targetState `
                -TargetStages $remainingTargetStages `
                -Stage $stage `
                -Compensating $true `
                -ReleaseName $ReleaseName `
                -Namespace $Namespace
            $actualVolumes = Get-PersistentVolumeUids -Kubectl $kubectl -Namespace $Namespace
            Assert-PersistentVolumesUnchanged `
                -Expected $record.persistent_volume_uids `
                -Actual $actualVolumes
            $record.compensated_stages = @($record.compensated_stages) + @($stage)
            $record.updated_at = [DateTimeOffset]::UtcNow.ToString("o")
            $journalEnvelope = Save-JournalRecord `
                -Kubectl $kubectl `
                -Policy $rollbackPolicy `
                -Namespace $Namespace `
                -Envelope $journalEnvelope `
                -Record $record
        }
        $record.status = "compensated"
        $record.updated_at = [DateTimeOffset]::UtcNow.ToString("o")
        $journalEnvelope = Save-JournalRecord `
            -Kubectl $kubectl `
            -Policy $rollbackPolicy `
            -Namespace $Namespace `
            -Envelope $journalEnvelope `
            -Record $record
        throw $failure
    }
}
finally {
    Release-RollbackLease `
        -Kubectl $kubectl `
        -Policy $rollbackPolicy `
        -Namespace $Namespace `
        -HolderIdentity $holderIdentity `
        -FencingToken ([uint64]$checkpointSet.fencingToken)
}

Write-Host "RELEASE_ROLLBACK_OK operation_id=$operationId direction=$Direction target_release_id=$($targetState.release_id)"

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
    [string]$Kubeconfig = 'D:\BuildCache\rocketmq-sre-temp\kind\phase00-kubeconfig',

    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',

    [string]$CargoTargetDir = 'D:\BuildCache\rocketmq-sre-target',

    [string]$TenantId = '00000000-0000-4000-8000-000000000002',

    [string]$ClusterId = '00000000-0000-4000-8000-000000000001',

    [string]$BrokerResource = 'broker/rocketmq-broker.rocketmq-system.svc.cluster.local:10911',

    [string]$Topic = 'SRE_PROBE_00000000000040008000000000000001_00000000000000000000000000000000',

    [string]$ConsumerGroup = 'SRE_PROBE_G_C_00000000000040008000000000000001_00000000000000000000000000000000',

    [ValidateRange(1024, 65535)]
    [int]$ControlPlaneLocalPort = 18090,

    [ValidateRange(60, 900)]
    [int]$TtlSeconds = 60,

    [ValidateRange(30, 600)]
    [int]$ExecutionTimeoutSeconds = 180
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$namespace = 'rocketmq-sre'
$rocketmqNamespace = 'rocketmq-system'
$probeJob = 'rocketmq-sre-phase01-live-probe'
$actionId = 'observability.logger_level_ttl.v1'
$descriptorVersion = '1.0.0'
$operatorSubject = 'phase05-complete-loop-operator'
$approverSubject = 'phase05-complete-loop-approver'
$controlPlaneUrl = "http://127.0.0.1:$ControlPlaneLocalPort"
$runSuffix = [Guid]::NewGuid().ToString('N')
$runSucceeded = $false
$portForward = $null

function Assert-AllowedStoragePath(
    [string]$Path,
    [string]$Description
) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetPathRoot($fullPath)
    if (
        -not $root.Equals('D:\', [StringComparison]::OrdinalIgnoreCase) -and
        -not $root.Equals('F:\', [StringComparison]::OrdinalIgnoreCase)
    ) {
        throw "$Description must use D: or F:, received '$fullPath'."
    }
    return $fullPath
}

function Assert-StorageReserve(
    [string]$Path,
    [bool]$CleanCargoTarget
) {
    $root = [IO.Path]::GetPathRoot([IO.Path]::GetFullPath($Path))
    $driveName = $root.Substring(0, 1)
    $drive = Get-PSDrive -Name $driveName -PSProvider FileSystem
    if (($drive.Free / 1GB) -ge 15) {
        return
    }
    if ($CleanCargoTarget -and (Test-Path -LiteralPath $Path)) {
        & cargo +1.95.0 clean --target-dir $Path
        if ($LASTEXITCODE -ne 0) {
            throw "Cargo cleanup failed for '$Path'."
        }
        $drive = Get-PSDrive -Name $driveName -PSProvider FileSystem
    }
    if (($drive.Free / 1GB) -lt 15) {
        throw "Drive $driveName`: has less than 15 GiB free."
    }
}

function Assert-Guid(
    [string]$Value,
    [string]$Description
) {
    $parsed = [Guid]::Empty
    if (-not [Guid]::TryParse($Value, [ref]$parsed)) {
        throw "$Description is not a UUID: '$Value'."
    }
}

function Assert-True(
    [bool]$Condition,
    [string]$Message
) {
    if (-not $Condition) {
        throw $Message
    }
}

function Assert-Equal(
    [object]$Actual,
    [object]$Expected,
    [string]$Message
) {
    if ($Actual -ne $Expected) {
        throw "$Message Expected '$Expected', received '$Actual'."
    }
}

function Invoke-Kubectl([string[]]$Arguments) {
    $output = & kubectl --kubeconfig $resolvedKubeconfig @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl failed: $($output -join [Environment]::NewLine)"
    }
    return ($output -join [Environment]::NewLine)
}

function Assert-PortAvailable([int]$Port) {
    $listener = [Net.Sockets.TcpListener]::new(
        [Net.IPAddress]::Loopback,
        $Port
    )
    try {
        $listener.Start()
    }
    catch {
        throw "Loopback port $Port is already in use."
    }
    finally {
        $listener.Stop()
    }
}

function Wait-PortForward(
    [Diagnostics.Process]$Process,
    [string]$ErrorLog
) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(45)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        if ($Process.HasExited) {
            $detail = if (Test-Path -LiteralPath $ErrorLog) {
                (Get-Content -LiteralPath $ErrorLog -Tail 40) -join [Environment]::NewLine
            }
            else {
                '<no port-forward error log>'
            }
            throw "Control Plane port-forward exited early.`n$detail"
        }
        try {
            $response = Invoke-WebRequest `
                -Uri "$controlPlaneUrl/readyz" `
                -TimeoutSec 2 `
                -UseBasicParsing
            if ($response.StatusCode -eq 200) {
                return
            }
        }
        catch {
            Start-Sleep -Milliseconds 300
        }
    }
    throw 'Control Plane did not become ready through the local port-forward.'
}

function Get-InternalToken {
    $encoded = Invoke-Kubectl @(
        '-n', $namespace,
        'get', 'secret', 'rocketmq-sre-kind-secrets',
        '-o', 'jsonpath={.data.internal-token}'
    )
    if ([string]::IsNullOrWhiteSpace($encoded)) {
        throw 'The Kind internal token fixture is empty.'
    }
    try {
        return [Text.Encoding]::UTF8.GetString(
            [Convert]::FromBase64String($encoded.Trim())
        )
    }
    catch {
        throw 'The Kind internal token fixture is not valid base64.'
    }
}

function Get-ApiHeaders([string]$Subject) {
    return @{
        Authorization = "Bearer $internalToken"
        'x-rocketmq-tenant' = $TenantId
        'x-rocketmq-clusters' = $ClusterId
        'x-rocketmq-subject' = $Subject
        'x-correlation-id' = [Guid]::NewGuid().ToString()
    }
}

function Invoke-Api(
    [ValidateSet('Get', 'Post')]
    [string]$Method,
    [string]$Path,
    [object]$Body,
    [string]$Subject,
    [int]$TimeoutSeconds = 90
) {
    $request = @{
        Method = $Method
        Uri = "$controlPlaneUrl$Path"
        Headers = (Get-ApiHeaders $Subject)
        TimeoutSec = $TimeoutSeconds
    }
    if ($null -ne $Body) {
        $request.ContentType = 'application/json'
        $request.Body = $Body | ConvertTo-Json -Depth 30 -Compress
    }
    try {
        return Invoke-RestMethod @request
    }
    catch {
        $detail = if (
            $null -ne $_.ErrorDetails -and
            -not [string]::IsNullOrWhiteSpace($_.ErrorDetails.Message)
        ) {
            $_.ErrorDetails.Message
        }
        else {
            $_.Exception.Message
        }
        throw "Control Plane $Method $Path failed: $detail"
    }
}

function Wait-Execution([string]$ExecutionId) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(
        $ExecutionTimeoutSeconds
    )
    $lastState = 'unknown'
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        $view = Invoke-Api `
            Get `
            "/v1/executions/$ExecutionId" `
            $null `
            $operatorSubject
        $lastState = [string]$view.state
        if ($lastState -eq 'succeeded') {
            return $view
        }
        if (@('rolled_back', 'escalated') -contains $lastState) {
            throw "Execution entered terminal failure state '$lastState'."
        }
        Start-Sleep -Seconds 2
    }
    throw "Execution did not succeed before timeout; last state '$lastState'."
}

function Wait-TtlWindow {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TtlSeconds + 8)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        Start-Sleep -Seconds 2
    }
}

$resolvedKubeconfig = Assert-AllowedStoragePath `
    $Kubeconfig `
    'Kubernetes kubeconfig'
$resolvedTemporaryRoot = Assert-AllowedStoragePath `
    $TemporaryRoot `
    'Temporary root'
$resolvedCargoTargetDir = Assert-AllowedStoragePath `
    $CargoTargetDir `
    'Cargo target directory'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$probeManifest = [IO.Path]::GetFullPath(
    (Join-Path $scriptDirectory '..\deploy\kind\phase01-live-probe-job.yaml')
)
Assert-StorageReserve $resolvedTemporaryRoot $false
Assert-StorageReserve $resolvedCargoTargetDir $true
Assert-Guid $TenantId 'TenantId'
Assert-Guid $ClusterId 'ClusterId'
Assert-PortAvailable $ControlPlaneLocalPort

if (-not (Test-Path -LiteralPath $resolvedKubeconfig -PathType Leaf)) {
    throw "Kubernetes kubeconfig does not exist: $resolvedKubeconfig"
}
if ($BrokerResource -notmatch '^broker/.+:[0-9]+$') {
    throw 'BrokerResource must use the typed broker/<host>:<port> format.'
}
if (-not (Test-Path -LiteralPath $probeManifest -PathType Leaf)) {
    throw "The bounded synthetic probe manifest does not exist: $probeManifest"
}

New-Item -ItemType Directory -Force -Path $resolvedTemporaryRoot | Out-Null
$runRoot = [IO.Path]::GetFullPath(
    (Join-Path $resolvedTemporaryRoot "phase05-complete-loop-$runSuffix")
)
$temporaryPrefix = $resolvedTemporaryRoot.TrimEnd('\') + '\'
if (
    -not $runRoot.StartsWith(
        $temporaryPrefix,
        [StringComparison]::OrdinalIgnoreCase
    )
) {
    throw 'Runtime directory escaped the configured temporary root.'
}
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

try {
    Invoke-Kubectl @(
        '-n', $namespace,
        'wait',
        '--for=condition=available',
        'deployment/sre-control-plane',
        'deployment/sre-executor',
        'deployment/sre-execution-agent',
        '--timeout=180s'
    ) | Out-Null

    Invoke-Kubectl @(
        '-n', $rocketmqNamespace,
        'delete', "job/$probeJob",
        '--ignore-not-found=true',
        '--wait=true'
    ) | Out-Null
    Invoke-Kubectl @('apply', '--filename', $probeManifest) | Out-Null
    Invoke-Kubectl @(
        '-n', $rocketmqNamespace,
        'wait',
        '--for=condition=complete',
        "job/$probeJob",
        '--timeout=180s'
    ) | Out-Null
    $probeLog = Invoke-Kubectl @(
        '-n', $rocketmqNamespace,
        'logs', "job/$probeJob",
        '--container=bounded-probe',
        '--tail=80'
    )
    Assert-True (
        $probeLog.Contains('probe_result command=register cleanup_partial=false') -and
        $probeLog.Contains('probe_result command=send cleanup_partial=false')
    ) 'The bounded synthetic Topic/Consumer Group probe did not complete.'

    $internalToken = Get-InternalToken
    $portForwardOut = Join-Path $runRoot 'control-plane-port-forward.out.log'
    $portForwardError = Join-Path $runRoot 'control-plane-port-forward.err.log'
    $portForward = Start-Process `
        -FilePath 'kubectl' `
        -ArgumentList @(
            '--kubeconfig', $resolvedKubeconfig,
            '-n', $namespace,
            'port-forward', 'service/sre-control-plane',
            "${ControlPlaneLocalPort}:8090",
            '--address', '127.0.0.1'
        ) `
        -RedirectStandardOutput $portForwardOut `
        -RedirectStandardError $portForwardError `
        -WindowStyle Hidden `
        -PassThru
    Wait-PortForward $portForward $portForwardError

    $occurredAt = [DateTimeOffset]::UtcNow.ToString('o')
    $eventEntry = Invoke-Api Post '/v1/event-entries' @{
        schema_version = 'rocketmq-sre.event-entry.v1'
        cluster_id = $ClusterId
        idempotency_key = "phase05.complete-loop.event.$runSuffix"
        occurred_at = $occurredAt
        source_kind = 'alert'
        source = 'synthetic_probe'
        source_event_id = "phase05-complete-loop-$runSuffix"
        resource_kind = 'consumer_group'
        resource_key = "$ConsumerGroup/$Topic"
        display_name = 'Phase 5 complete AI operations loop'
        symptom_family = "consumer_lag_phase05_$runSuffix"
        severity = 'warning'
        status = 'firing'
        summary = 'Synthetic bounded consumer lag signal for the complete AI operations loop.'
        labels = @{
            alertname = 'RocketMqSrePhase05CompleteLoop'
            environment = 'kind'
        }
        evidence_ids = @()
        sequence = 1
    } $operatorSubject
    Assert-Equal $eventEntry.target_kind 'incident' `
        'Unified event entry did not create an Incident target.'
    Assert-Equal $eventEntry.created $true `
        'Unique unified event entry was not created.'
    $incidentId = [string]$eventEntry.target_id
    Assert-Guid $incidentId 'Unified event Incident'

    $diagnosis = Invoke-Api `
        Post `
        "/v1/incidents/$incidentId/diagnose" `
        $null `
        $operatorSubject `
        120
    Assert-Equal $diagnosis.schema_version 'rocketmq-sre.diagnosis.v1' `
        'Diagnosis schema drifted.'
    Assert-Equal $diagnosis.mode 'model_assisted' `
        'Configured model-assisted diagnosis was not used.'
    Assert-Equal $diagnosis.execution_eligible $false `
        'Model output bypassed explicit human confirmation.'
    $diagnosisRevisionId = [string]$diagnosis.revision.id
    Assert-Guid $diagnosisRevisionId 'Diagnosis revision'
    $diagnosisEvidenceIds = @(
        $diagnosis.revision.evidence_ids |
            ForEach-Object { [string]$_ }
    )
    Assert-True ($diagnosisEvidenceIds.Count -gt 0) `
        'Diagnosis did not cite Evidence.'

    $confirmation = Invoke-Api `
        Post `
        "/v1/incidents/$incidentId/diagnosis-revisions/$diagnosisRevisionId/confirm-execution" `
        @{
            human_confirmed = $true
            reason = 'Phase 5 operator reviewed the model diagnosis, cited Evidence, and bounded logger impact.'
        } `
        $operatorSubject
    Assert-Equal $confirmation.execution_eligible $true `
        'Human confirmation did not create an execution-eligible revision.'
    $confirmedRevisionId = [string]$confirmation.confirmed_revision_id
    Assert-Guid $confirmedRevisionId 'Confirmed diagnosis revision'
    $confirmedEvidenceIds = @(
        $confirmation.evidence_ids |
            ForEach-Object { [string]$_ }
    )

    $parameters = @{
        component = 'broker'
        logger = 'rocketmq_broker::processor'
        level = 'DEBUG'
        ttl_seconds = $TtlSeconds
    }
    $preconditionRequest = @{
        cluster_id = $ClusterId
        diagnosis_revision_id = $confirmedRevisionId
        action_id = $actionId
        descriptor_version = $descriptorVersion
        resource = $BrokerResource
        parameters = $parameters
    }
    $precondition = Invoke-Api `
        Post `
        "/v1/incidents/$incidentId/execution-preconditions" `
        $preconditionRequest `
        $operatorSubject
    Assert-Equal `
        $precondition.schema_version `
        'rocketmq-sre.execution-precondition-evidence.v1' `
        'Execution precondition Evidence schema drifted.'
    $preconditionEvidenceId = [string]$precondition.evidence.evidence_id
    Assert-Guid $preconditionEvidenceId 'Execution precondition Evidence'
    Assert-True (
        [string]$precondition.precondition_hash -match
            '^sha256:[0-9A-Fa-f]{64}$'
    ) 'Execution precondition hash is malformed.'

    $planEvidenceIds = @(
        $confirmedEvidenceIds
        $preconditionEvidenceId
    ) | Select-Object -Unique
    $planResult = Invoke-Api Post '/v1/plans' @{
        cluster_id = $ClusterId
        incident_id = $incidentId
        diagnosis_revision_id = $confirmedRevisionId
        steps = @(
            @{
                action_id = $actionId
                descriptor_version = $descriptorVersion
                resource = $BrokerResource
                parameters = $parameters
                evidence_ids = @($planEvidenceIds)
            }
        )
    } $operatorSubject
    Assert-Equal $planResult.kind 'action_plan' `
        'Policy returned a manual-only Runbook instead of the bounded R1 plan.'
    $planId = [string]$planResult.plan.id
    $planHash = [string]$planResult.plan.plan_hash
    $aggregatePreconditionHash = [string]$planResult.precondition_hash
    Assert-Guid $planId 'Action Plan'

    $approval = Invoke-Api `
        Post `
        "/v1/plans/$planId/approve" `
        @{
            plan_hash = $planHash
            precondition_hash = $aggregatePreconditionHash
            reason = 'Independent approver reviewed the exact immutable plan and live precondition.'
            validity_seconds = 300
        } `
        $approverSubject
    Assert-Equal $approval.approval.decision 'approved' `
        'Independent approval was not persisted.'
    Assert-Equal $approval.approval.approver_subject $approverSubject `
        'Approval did not retain the independent approver subject.'
    Assert-True (
        $approval.approval.requester_subject -ne
            $approval.approval.approver_subject
    ) 'Requester and approver subjects were not separated.'

    $submission = Invoke-Api Post '/v1/executions' @{
        plan_id = $planId
        plan_hash = $planHash
        precondition_hash = $aggregatePreconditionHash
        idempotency_key = "phase05.complete-loop.execution.$runSuffix"
    } $operatorSubject 120
    $executionId = [string]$submission.execution.id
    Assert-Guid $executionId 'Execution'
    $execution = Wait-Execution $executionId
    Assert-Equal $execution.state 'succeeded' `
        'Execution did not reach succeeded.'

    $executionCorrelationId = [string]$execution.execution.correlation_id
    Assert-Guid $executionCorrelationId 'Execution correlation'
    $audit = Invoke-Api `
        Get `
        "/v1/audit/$executionCorrelationId" `
        $null `
        $operatorSubject
    Assert-True (@($audit.items).Count -gt 0) `
        'Execution correlation did not return audit events.'

    Wait-TtlWindow
    $restoredPrecondition = Invoke-Api `
        Post `
        "/v1/incidents/$incidentId/execution-preconditions" `
        $preconditionRequest `
        $operatorSubject
    Assert-Equal `
        $restoredPrecondition.schema_version `
        'rocketmq-sre.execution-precondition-evidence.v1' `
        'Logger TTL restoration did not return to a ready precondition.'

    $postmortem = Invoke-Api `
        Post `
        "/v1/incidents/$incidentId/postmortems" `
        @{
            operator_notes = @(
                'The complete AI operations loop used human confirmation, an independent approver, and a bounded TTL action.'
            )
        } `
        $operatorSubject `
        120
    Assert-Equal $postmortem.postmortem.status 'draft' `
        'Postmortem must remain human-reviewable.'
    Assert-True (@($postmortem.revisions).Count -gt 0) `
        'Postmortem did not persist an immutable revision.'
    Assert-Equal $postmortem.execution_journal_empty $false `
        'Postmortem did not include the supervised execution journal.'

    $result = [ordered]@{
        marker = 'COMPLETE_AI_OPERATIONS_LOOP_OK'
        target = 'kind'
        source_kind = [string]$eventEntry.source_kind
        incident_id = $incidentId
        diagnosis_mode = [string]$diagnosis.mode
        diagnosis_revision_id = $diagnosisRevisionId
        confirmed_revision_id = $confirmedRevisionId
        execution_precondition_evidence_id = $preconditionEvidenceId
        plan_id = $planId
        plan_hash = $planHash
        approval_id = [string]$approval.approval.id
        execution_id = $executionId
        execution_state = [string]$execution.state
        audit_event_count = @($audit.items).Count
        ttl_restored = $true
        postmortem_id = [string]$postmortem.postmortem.id
        postmortem_revision_count = @($postmortem.revisions).Count
        action_item_count = @($postmortem.action_items).Count
    }
    $runSucceeded = $true
    $result | ConvertTo-Json -Depth 8 -Compress | Write-Output
}
finally {
    if ($null -ne $portForward -and -not $portForward.HasExited) {
        Stop-Process -Id $portForward.Id -Force
        $portForward.WaitForExit(10000) | Out-Null
    }
    if ($runSucceeded) {
        if (
            (Test-Path -LiteralPath $runRoot) -and
            $runRoot.StartsWith(
                $temporaryPrefix,
                [StringComparison]::OrdinalIgnoreCase
            )
        ) {
            Remove-Item -LiteralPath $runRoot -Recurse -Force
        }
    }
    else {
        Write-Warning "Failure diagnostics were retained under '$runRoot'."
    }
}

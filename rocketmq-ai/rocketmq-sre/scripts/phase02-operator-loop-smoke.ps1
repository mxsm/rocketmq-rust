# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidateSet('Compose', 'Kind')]
    [string]$Target = 'Kind',

    [ValidatePattern('^[a-z0-9][a-z0-9-]{0,39}$')]
    [string]$ClusterName = 'rocketmq-sre-phase00'
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$composeDirectory = Join-Path $sreRoot 'deploy/dev'
$composeFile = Join-Path $composeDirectory 'compose.yaml'
$kindArtifactRoot = Join-Path $repositoryRoot 'target/phase00-kind'
$kubeconfigPath = Join-Path $kindArtifactRoot 'kubeconfig'
$kubeContext = "kind-$ClusterName"
$clusterId = '00000000-0000-4000-8000-000000000001'
$tenantId = '00000000-0000-4000-8000-000000000002'
$notificationTargetId = '00000000-0000-4000-8000-000000000202'
$notificationTargetName = 'phase02-operator-loop-smoke'
$controlPlaneUrl = $null
$internalToken = $null
$portForwardProcesses = [Collections.Generic.List[Diagnostics.Process]]::new()
$notificationTargetEnabled = $false

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found."
    }
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $Command @Arguments 2>&1 | Out-String
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "$Command failed with exit code $exitCode.`n$output"
    }
    [pscustomobject]@{
        ExitCode = $exitCode
        Output = $output.TrimEnd()
    }
}

function Compose-Arguments([string[]]$Arguments) {
    @(
        'compose',
        '--project-directory', $composeDirectory,
        '--file', $composeFile
    ) + $Arguments
}

function Invoke-Kubectl {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure
    )

    Invoke-Native kubectl (@(
        '--kubeconfig', $kubeconfigPath,
        '--context', $kubeContext
    ) + $Arguments) -AllowFailure:$AllowFailure
}

function Get-FreeLoopbackPort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        return ([Net.IPEndPoint]$listener.LocalEndpoint).Port
    }
    finally {
        $listener.Stop()
    }
}

function Start-KubectlPortForward(
    [string]$Namespace,
    [string]$Resource,
    [int]$RemotePort
) {
    $localPort = Get-FreeLoopbackPort
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = (Get-Command kubectl -ErrorAction Stop).Source
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.WindowStyle = [Diagnostics.ProcessWindowStyle]::Hidden
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $arguments = @(
        '--kubeconfig', $kubeconfigPath,
        '--context', $kubeContext,
        '--namespace', $Namespace,
        'port-forward',
        '--address', '127.0.0.1',
        $Resource,
        "${localPort}:$RemotePort"
    )
    $startInfo.Arguments = ($arguments | ForEach-Object {
        if ($_ -match '[\s"]') {
            '"' + $_.Replace('"', '\"') + '"'
        }
        else {
            $_
        }
    }) -join ' '

    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    if (-not $process.Start()) {
        throw "kubectl port-forward could not start for $Namespace/$Resource."
    }
    $portForwardProcesses.Add($process)
    [pscustomobject]@{
        Process = $process
        Port = $localPort
        Target = "$Namespace/$Resource"
    }
}

function Stop-KubectlPortForwards {
    foreach ($process in $portForwardProcesses) {
        try {
            if (-not $process.HasExited) {
                $process.Kill()
                $process.WaitForExit(5000) | Out-Null
            }
        }
        catch {
            Write-Warning "Could not stop kubectl port-forward process $($process.Id): $($_.Exception.Message)"
        }
        finally {
            $process.Dispose()
        }
    }
    $portForwardProcesses.Clear()
}

function Wait-Http([string]$Uri, [int]$Seconds = 120) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    $lastFailure = 'no response'
    do {
        foreach ($process in $portForwardProcesses) {
            if ($process.HasExited) {
                $stderr = $process.StandardError.ReadToEnd()
                throw "kubectl port-forward exited before '$Uri' became ready.`n$stderr"
            }
        }
        try {
            return Invoke-RestMethod -Uri $Uri -TimeoutSec 5
        }
        catch {
            $lastFailure = $_.Exception.Message
            Start-Sleep -Seconds 1
        }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for $Uri`: $lastFailure"
}

function Get-PublicHeaders {
    @{
        Authorization = "Bearer $internalToken"
        'x-rocketmq-tenant' = $tenantId
        'x-rocketmq-clusters' = $clusterId
        'x-rocketmq-subject' = 'phase02-operator-loop-smoke'
        'x-correlation-id' = [Guid]::NewGuid().ToString()
    }
}

function Invoke-PublicApi(
    [ValidateSet('Get', 'Post')]
    [string]$Method,
    [string]$Path,
    [object]$Body,
    [int]$TimeoutSeconds = 60
) {
    $arguments = @{
        Method = $Method
        Uri = "$controlPlaneUrl$Path"
        Headers = (Get-PublicHeaders)
        TimeoutSec = $TimeoutSeconds
    }
    if ($null -ne $Body) {
        $arguments.ContentType = 'application/json'
        $arguments.Body = $Body | ConvertTo-Json -Depth 20 -Compress
    }
    try {
        Invoke-RestMethod @arguments
    }
    catch {
        $detail = if (-not [string]::IsNullOrWhiteSpace($_.ErrorDetails.Message)) {
            $_.ErrorDetails.Message
        }
        else {
            $_.Exception.Message
        }
        throw "Public API $Method $Path failed: $detail"
    }
}

function Invoke-Postgres([string]$Sql) {
    if ($Target -eq 'Kind') {
        return (Invoke-Kubectl @(
            '--namespace', 'rocketmq-sre',
            'exec', 'postgres-0', '--',
            'psql', '-U', 'rocketmq_sre', '-d', 'rocketmq_sre',
            '-At', '-v', 'ON_ERROR_STOP=1', '-c', $Sql
        )).Output.Trim()
    }
    return (Invoke-Native docker (Compose-Arguments @(
        'exec', '-T', 'postgres',
        'psql', '-U', 'rocketmq_sre', '-d', 'rocketmq_sre',
        '-At', '-v', 'ON_ERROR_STOP=1', '-c', $Sql
    ))).Output.Trim()
}

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) {
        throw $Message
    }
}

function Assert-Equal([object]$Actual, [object]$Expected, [string]$Message) {
    if ($Actual -ne $Expected) {
        throw "$Message Expected '$Expected', received '$Actual'."
    }
}

function Enable-SmokeNotificationTarget {
    $sql = @"
INSERT INTO notification_targets (
    id, tenant_id, cluster_id, name, channel, endpoint,
    secret_reference, enabled, created_at, updated_at
) VALUES (
    '$notificationTargetId', '$tenantId', '$clusterId',
    '$notificationTargetName', 'email', 'phase02-local-sink',
    NULL, TRUE, NOW(), NOW()
)
ON CONFLICT (id) DO UPDATE
SET enabled = TRUE, updated_at = NOW()
WHERE notification_targets.tenant_id = EXCLUDED.tenant_id
  AND notification_targets.cluster_id = EXCLUDED.cluster_id
  AND notification_targets.name = EXCLUDED.name
  AND notification_targets.channel = EXCLUDED.channel
RETURNING id;
"@
    $result = Invoke-Postgres $sql
    Assert-True ($result -match [regex]::Escape($notificationTargetId)) `
        'The reserved Phase 2 notification target could not be enabled safely.'
    $script:notificationTargetEnabled = $true
}

function Disable-SmokeNotificationTarget {
    $sql = @"
UPDATE notification_targets
SET enabled = FALSE, updated_at = NOW()
WHERE id = '$notificationTargetId'
  AND tenant_id = '$tenantId'
  AND cluster_id = '$clusterId'
  AND name = '$notificationTargetName';
"@
    Invoke-Postgres $sql | Out-Null
    $script:notificationTargetEnabled = $false
}

function Wait-NotificationDelivered(
    [string]$IncidentId,
    [string]$ForbiddenMarker,
    [int]$Seconds = 90
) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    $lastState = 'missing'
    do {
        $sql = @"
SELECT status || '|' || attempt_count || '|' ||
       COALESCE(last_error_code, '') || '|' || sanitized_summary
FROM notification_outbox
WHERE incident_id = '$IncidentId'
  AND target_id = '$notificationTargetId'
ORDER BY created_at DESC, id DESC
LIMIT 1;
"@
        $row = Invoke-Postgres $sql
        if (-not [string]::IsNullOrWhiteSpace($row)) {
            $lastState = $row
            $parts = $row.Split('|', 4)
            if ($parts.Count -eq 4 -and $parts[0] -eq 'delivered') {
                Assert-Equal ([int]$parts[1]) 1 'Notification delivery was not exactly-once on its first attempt.'
                Assert-True ([string]::IsNullOrWhiteSpace($parts[2])) `
                    "Delivered notification retained an error code '$($parts[2])'."
                Assert-True ($parts[3].IndexOf($ForbiddenMarker, [StringComparison]::Ordinal) -lt 0) `
                    'Notification summary exposed an arbitrary upstream annotation.'
                return [pscustomobject]@{
                    Status = $parts[0]
                    AttemptCount = [int]$parts[1]
                    SanitizedSummary = $parts[3]
                }
            }
        }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Notification outbox did not reach delivered state (last=$lastState)."
}

function Initialize-Compose {
    Require-Command docker
    $running = (Invoke-Native docker (Compose-Arguments @(
        'ps', '--services', '--filter', 'status=running'
    ))).Output -split '\r?\n'
    foreach ($service in @('postgres', 'sre-model-mock', 'sre-control-plane')) {
        if ($running -notcontains $service) {
            throw "Compose service '$service' is not running."
        }
    }
    $tokenResult = Invoke-Native docker (Compose-Arguments @(
        'exec', '-T', 'sre-control-plane',
        'printenv', 'ROCKETMQ_SRE_INTERNAL_TOKEN'
    ))
    $script:internalToken = $tokenResult.Output.Trim()
    $script:controlPlaneUrl = 'http://127.0.0.1:8090'
}

function Initialize-Kind {
    Require-Command kubectl
    if (-not (Test-Path -LiteralPath $kubeconfigPath)) {
        throw "Kind kubeconfig was not found at $kubeconfigPath."
    }
    if ([IO.Path]::GetPathRoot($kubeconfigPath) -eq 'C:\') {
        throw 'Kind acceptance artifacts must not be stored on C:.'
    }
    foreach ($workload in @(
        'statefulset/postgres',
        'deployment/sre-model-mock',
        'deployment/sre-control-plane'
    )) {
        Invoke-Kubectl @(
            '--namespace', 'rocketmq-sre',
            'rollout', 'status', $workload,
            '--timeout=300s'
        ) | Out-Null
    }
    $tokenBase64 = (Invoke-Kubectl @(
        '--namespace', 'rocketmq-sre',
        'get', 'secret', 'rocketmq-sre-kind-secrets',
        '--output=jsonpath={.data.internal-token}'
    )).Output.Trim()
    try {
        $script:internalToken = [Text.Encoding]::UTF8.GetString(
            [Convert]::FromBase64String($tokenBase64)
        )
    }
    catch {
        throw 'Kind internal token secret is not valid base64.'
    }
    $forward = Start-KubectlPortForward 'rocketmq-sre' 'service/sre-control-plane' 8090
    $script:controlPlaneUrl = "http://127.0.0.1:$($forward.Port)"
}

function New-InspectionIncident([object]$Inspection) {
    $recommendations = @($Inspection.recommendations | Where-Object { $null -ne $_ })
    if ($recommendations.Count -gt 0) {
        $promoted = Invoke-PublicApi Post `
            "/v1/recommendations/$($recommendations[0].id)/disposition" `
            @{
                status = 'promoted'
                assignee = $null
                reason = 'Phase 2 live acceptance promotes a read-only finding for operator investigation.'
                promote_to = 'incident'
            }
        Assert-True (-not [string]::IsNullOrWhiteSpace([string]$promoted.incident_id)) `
            'Inspection recommendation promotion did not create an Incident.'
        return [pscustomobject]@{
            IncidentId = [string]$promoted.incident_id
            Source = 'recommendation_promotion'
            RecommendationCount = $recommendations.Count
        }
    }

    $outcome = Invoke-PublicApi Post '/v1/integrations/events' @{
        cluster_id = $clusterId
        source = 'inspection'
        source_event_id = "inspection:$($Inspection.run.id)"
        resource_kind = 'cluster'
        resource_key = $clusterId
        display_name = 'Phase 2 operator-loop inspection'
        symptom_family = 'inspection_needs_evidence'
        severity = 'warning'
        status = 'firing'
        summary = 'The deterministic inspection requires operator review.'
        labels = @{}
        evidence_ids = @()
        sequence = 1
        occurred_at = [DateTime]::UtcNow.ToString('o')
    }
    Assert-True (-not [string]::IsNullOrWhiteSpace([string]$outcome.incident_id)) `
        'Provider-neutral inspection event did not create an Incident.'
    return [pscustomobject]@{
        IncidentId = [string]$outcome.incident_id
        Source = 'inspection_event'
        RecommendationCount = 0
    }
}

try {
    if ($Target -eq 'Kind') {
        Initialize-Kind
    }
    else {
        Initialize-Compose
    }
    Assert-True (-not [string]::IsNullOrWhiteSpace($internalToken)) `
        'Control-plane development token was not available.'
    Wait-Http "$controlPlaneUrl/readyz" | Out-Null

    Enable-SmokeNotificationTarget

    $health = Invoke-PublicApi Get "/v1/clusters/$clusterId/health" $null
    Assert-Equal $health.schema_version 'rocketmq-sre.cluster-health.v1' `
        'Cluster health did not return the Phase 2 contract.'
    Assert-Equal ([string]$health.cluster_id) $clusterId 'Cluster health crossed its cluster scope.'
    Assert-Equal $health.execution_eligible $false 'Read-only health unexpectedly became executable.'

    $inspection = Invoke-PublicApi Post '/v1/inspections' @{
        cluster_id = $clusterId
        template = 'cluster_health'
        schedule = $null
    }
    Assert-True (@('completed', 'needs_evidence') -contains $inspection.run.status) `
        "Inspection ended in unsupported state '$($inspection.run.status)'."
    Assert-True ([int]$inspection.run.finding_count -ge 0) `
        'Inspection finding count was invalid.'
    $inspectionIncident = New-InspectionIncident $inspection
    $inspectionIncidentView = Invoke-PublicApi Get `
        "/v1/incidents/$($inspectionIncident.IncidentId)" `
        $null
    Assert-Equal ([string]$inspectionIncidentView.incident.cluster_id) $clusterId `
        'Inspection-created Incident crossed its cluster scope.'

    $runSuffix = [Guid]::NewGuid().ToString('N')
    $resourceKey = "PHASE02_CONSUMER_$runSuffix"
    $forbiddenMarker = "PHASE02_PRIVATE_ANNOTATION_$runSuffix"
    $occurredAt = [DateTime]::UtcNow.ToString('o')
    $ingestion = @(
        Invoke-PublicApi Post '/v1/integrations/alertmanager/events' @{
            version = '4'
            clusterId = $clusterId
            status = 'firing'
            receiver = 'phase02-operator-loop-smoke'
            groupKey = "phase02:$runSuffix"
            commonLabels = @{
                severity = 'warning'
                symptom_family = 'consumer_lag'
                rocketmq_resource_kind = 'consumer_group'
                rocketmq_resource_key = $resourceKey
            }
            alerts = @(
                @{
                    status = 'firing'
                    labels = @{
                        alertname = 'ConsumerLagHigh'
                    }
                    annotations = @{
                        summary = $forbiddenMarker
                    }
                    startsAt = $occurredAt
                    endsAt = $null
                    fingerprint = "phase02-$runSuffix"
                }
            )
        }
    )
    Assert-Equal $ingestion.Count 1 'Alertmanager ingestion did not return one bounded outcome.'
    Assert-Equal $ingestion[0].created $true 'Unique Phase 2 alert did not create a new Incident.'
    $incidentId = [string]$ingestion[0].incident_id
    Assert-True (-not [string]::IsNullOrWhiteSpace($incidentId)) `
        'Alertmanager ingestion did not return an Incident identifier.'

    $diagnosis = Invoke-PublicApi Post "/v1/incidents/$incidentId/diagnose" $null 90
    Assert-Equal $diagnosis.schema_version 'rocketmq-sre.diagnosis.v1' `
        'Diagnosis did not return the stable Phase 2 contract.'
    Assert-Equal $diagnosis.pack_id 'consumer-lag.v2' `
        'Consumer lag alert selected the wrong DiagnosticPack.'
    Assert-Equal $diagnosis.mode 'model_assisted' `
        'Live operator-loop diagnosis did not invoke the configured model gateway.'
    Assert-Equal $diagnosis.execution_eligible $false `
        'Phase 2 diagnosis unexpectedly became executable.'

    $forecast = Invoke-PublicApi Get "/v1/clusters/$clusterId/forecasts" $null
    Assert-Equal $forecast.schema_version 'rocketmq-sre.cluster-forecast.v1' `
        'Forecast did not return the stable Phase 2 contract.'
    Assert-Equal ([string]$forecast.cluster_id) $clusterId 'Forecast crossed its cluster scope.'
    Assert-True (@($forecast.forecasts).Count -gt 0) `
        'Forecast did not expose any bounded capacity projections.'
    Assert-Equal $forecast.execution_eligible $false `
        'Phase 2 forecast unexpectedly became executable.'

    $simulation = Invoke-PublicApi Post '/v1/simulations' @{
        cluster_id = $clusterId
        kind = 'traffic_increase'
        current_utilization = 0.45
        current_instances = 2
        traffic_increase_percent = 25
        instance_delta = $null
        current_queue_count = $null
        queue_delta = $null
        target_version = $null
        configuration_changes = @()
        affected_resource_keys = @($resourceKey)
        evidence_ids = @()
    }
    Assert-Equal $simulation.status 'completed' 'Traffic-increase simulation did not complete.'
    Assert-Equal $simulation.kind 'traffic_increase' 'Simulation kind drifted.'
    Assert-Equal $simulation.execution_eligible $false `
        'Phase 2 simulation unexpectedly became executable.'

    $notification = Wait-NotificationDelivered $incidentId $forbiddenMarker

    $postmortem = Invoke-PublicApi Post `
        "/v1/incidents/$incidentId/postmortems" `
        @{
            operator_notes = @(
                'Consumer capacity recovery must be verified against lag and throughput evidence.'
            )
        } `
        90
    Assert-Equal $postmortem.postmortem.status 'draft' `
        'Postmortem did not remain a human-reviewable draft.'
    Assert-True (@($postmortem.revisions).Count -ge 1) `
        'Postmortem did not persist an immutable revision.'
    Assert-True (@($postmortem.action_items).Count -ge 1) `
        'Postmortem did not generate any evidence-gap Action Item.'
    Assert-Equal $postmortem.execution_journal_empty $true `
        'Phase 2 Action Items unexpectedly contained execution journals.'

    $actionItems = Invoke-PublicApi Get `
        "/v1/action-items?cluster_id=$clusterId&limit=200" `
        $null
    $postmortemActionIds = @($postmortem.action_items | ForEach-Object { [string]$_.id })
    $listedActionIds = @($actionItems.items | ForEach-Object { [string]$_.id })
    foreach ($actionId in $postmortemActionIds) {
        Assert-True ($listedActionIds -contains $actionId) `
            "Postmortem Action Item '$actionId' was not queryable from the operator workspace."
    }

    [pscustomobject]@{
        target = $Target.ToLowerInvariant()
        inspection_id = [string]$inspection.run.id
        inspection_status = [string]$inspection.run.status
        inspection_incident_id = $inspectionIncident.IncidentId
        inspection_incident_source = $inspectionIncident.Source
        inspection_recommendations = $inspectionIncident.RecommendationCount
        alert_incident_id = $incidentId
        diagnosis_mode = [string]$diagnosis.mode
        diagnostic_pack = [string]$diagnosis.pack_id
        forecast_count = @($forecast.forecasts).Count
        forecast_partial = [bool]$forecast.partial
        simulation_status = [string]$simulation.status
        notification_status = $notification.Status
        notification_attempts = $notification.AttemptCount
        postmortem_id = [string]$postmortem.postmortem.id
        action_item_count = @($postmortem.action_items).Count
        execution_eligible = $false
    } | ConvertTo-Json -Compress | ForEach-Object {
        Write-Host "PHASE02_OPERATOR_LOOP_SMOKE_OK $_"
    }
}
finally {
    if ($notificationTargetEnabled) {
        try {
            Disable-SmokeNotificationTarget
        }
        catch {
            Write-Warning "Could not disable the reserved Phase 2 notification target: $($_.Exception.Message)"
        }
    }
    Stop-KubectlPortForwards
}

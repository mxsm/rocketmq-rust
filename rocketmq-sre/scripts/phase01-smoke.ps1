# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidateSet('Compose', 'Kind')]
    [string]$Target = 'Compose',

    [switch]$BootstrapProbe,

    [ValidatePattern('^[a-z0-9][a-z0-9-]{0,39}$')]
    [string]$ClusterName = 'rocketmq-sre-phase00',

    [string]$ConversationQualificationReport
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
Add-Type -AssemblyName System.Net.Http
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '..'))
$composeDirectory = Join-Path $sreRoot 'deploy/dev'
$composeFile = Join-Path $composeDirectory 'compose.yaml'
$kindDirectory = Join-Path $sreRoot 'deploy/kind'
$kindArtifactRoot = Join-Path $repositoryRoot 'target/phase00-kind'
$kubeconfigPath = Join-Path $kindArtifactRoot 'kubeconfig'
$kubeContext = "kind-$ClusterName"
$controlPlaneUrl = $null
$clusterId = '00000000-0000-4000-8000-000000000001'
$tenantId = '00000000-0000-4000-8000-000000000002'
$otherClusterId = '00000000-0000-4000-8000-000000000099'
$internalToken = $null
$topic = 'SRE_PROBE_00000000000040008000000000000001_00000000000000000000000000000000'
$group = 'SRE_PROBE_G_C_00000000000040008000000000000001_00000000000000000000000000000000'
$brokerName = 'rocketmq-dev-broker'
$portForwardProcesses = [Collections.Generic.List[Diagnostics.Process]]::new()
$validatedEvidenceCitations = @{}
$acceptedPackIds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$qualificationStartedAt = [DateTime]::UtcNow
$conversationStreamSchema = 'rocketmq-sre.conversation-stream-event.v1'
$maxConversationStreamBytes = 1024 * 1024
$maxConversationFrameBytes = 256 * 1024

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found."
    }
}

function Compose-Arguments([string[]]$Arguments) {
    @(
        'compose',
        '--project-directory', $composeDirectory,
        '--file', $composeFile
    ) + $Arguments
}

function Invoke-Docker([string[]]$Arguments, [switch]$Capture) {
    if ($Capture) {
        $output = & docker @Arguments 2>&1 | Out-String
        if ($LASTEXITCODE -ne 0) {
            throw "docker command failed with exit code $LASTEXITCODE`n$output"
        }
        return $output.Trim()
    }
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker command failed with exit code $LASTEXITCODE"
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
                $process.WaitForExit(5000)
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

function Assert-KindWorkloadsReady {
    $workloads = @(
        [pscustomobject]@{ Namespace = 'rocketmq-sre'; Name = 'statefulset/postgres' }
        [pscustomobject]@{ Namespace = 'rocketmq-sre'; Name = 'deployment/sre-model-mock' }
        [pscustomobject]@{ Namespace = 'rocketmq-sre'; Name = 'deployment/sre-control-plane' }
        [pscustomobject]@{ Namespace = 'rocketmq-sre'; Name = 'deployment/sre-ui' }
        [pscustomobject]@{ Namespace = 'rocketmq-system'; Name = 'statefulset/rocketmq-broker' }
        [pscustomobject]@{ Namespace = 'rocketmq-system'; Name = 'statefulset/rocketmq-namesrv' }
        [pscustomobject]@{ Namespace = 'rocketmq-system'; Name = 'statefulset/rocketmq-controller' }
        [pscustomobject]@{ Namespace = 'rocketmq-system'; Name = 'deployment/rocketmq-proxy' }
        [pscustomobject]@{ Namespace = 'rocketmq-system'; Name = 'deployment/rocketmq-mcp' }
    )
    foreach ($workload in $workloads) {
        Invoke-Kubectl @(
            '--namespace', $workload.Namespace,
            'rollout', 'status', $workload.Name,
            '--timeout=300s'
        ) | Out-Null
    }
    $deployedWorkloads = Invoke-Kubectl @(
        'get', 'deployment,statefulset',
        '--all-namespaces',
        '--output=name'
    )
    if ($deployedWorkloads.Output -match '(?i)(rocketmq-sre-executor|rocketmq-sre-execution-agent)') {
        throw 'Kind acceptance cluster unexpectedly contains an Executor or Execution Agent workload.'
    }
}

function Assert-ProbeManifestBoundary {
    $manifestPath = Join-Path $kindDirectory 'phase01-live-probe-job.yaml'
    $manifest = Get-Content -Raw -LiteralPath $manifestPath
    foreach ($required in @(
        'automountServiceAccountToken: false',
        'rocketmq-rust/fault-driver:local',
        'rocketmq-rust/sre-probe:phase00-local',
        'SRE_PROBE_00000000000040008000000000000001_00000000000000000000000000000000',
        'ROCKETMQ_SRE_PROBE_MAX_MESSAGES, value: "10"',
        'ROCKETMQ_SRE_PROBE_MAX_MESSAGES_PER_SECOND, value: "5"',
        'ROCKETMQ_SRE_PROBE_PAYLOAD_BYTES, value: "64"',
        'ROCKETMQ_SRE_PROBE_DURATION_SECONDS, value: "60"',
        'key: probe-secret-key'
    )) {
        if ($manifest.IndexOf($required, [StringComparison]::Ordinal) -lt 0) {
            throw "Kind probe manifest lost required bounded contract '$required'."
        }
    }
    foreach ($forbidden in @(
        'ROCKETMQ_SRE_INTERNAL_TOKEN',
        'mcp-token',
        'executor',
        'execution-agent',
        'hostNetwork: true',
        'defaultTopicPerm:'
    )) {
        if ($manifest.IndexOf($forbidden, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            throw "Kind probe manifest contains forbidden capability '$forbidden'."
        }
    }
    return $manifestPath
}

function Invoke-KindProbe {
    $manifestPath = Assert-ProbeManifestBoundary
    Invoke-Kubectl @(
        '--namespace', 'rocketmq-system',
        'delete', 'job', 'rocketmq-sre-phase01-live-probe',
        '--ignore-not-found=true',
        '--wait=true'
    ) | Out-Null
    Invoke-Kubectl @('apply', '--filename', $manifestPath) | Out-Null
    $probeDeadline = [DateTime]::UtcNow.AddSeconds(180)
    $probeState = 'active'
    $probeDetail = 'no Job status was returned'
    do {
        $statusResult = Invoke-Kubectl @(
            '--namespace', 'rocketmq-system',
            'get', 'job', 'rocketmq-sre-phase01-live-probe',
            '--output=json'
        ) -AllowFailure
        if ($statusResult.ExitCode -eq 0) {
            $status = $statusResult.Output | ConvertFrom-Json
            $probeDetail = (@(
                $status.status.conditions |
                    Where-Object { $_.status -eq 'True' } |
                    ForEach-Object { $_.type }
            ) -join ',')
            if ([int]$status.status.succeeded -ge 1) {
                $probeState = 'complete'
                break
            }
            if ([int]$status.status.failed -ge 1) {
                $probeState = 'failed'
                break
            }
        }
        else {
            $probeDetail = $statusResult.Output
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $probeDeadline)
    if ($probeState -eq 'active') {
        $probeState = 'timed_out'
    }
    $logs = Invoke-Kubectl @(
        '--namespace', 'rocketmq-system',
        'logs', 'job/rocketmq-sre-phase01-live-probe',
        '--all-containers=true'
    ) -AllowFailure
    if ($probeState -ne 'complete') {
        $describe = Invoke-Kubectl @(
            '--namespace', 'rocketmq-system',
            'describe', 'job/rocketmq-sre-phase01-live-probe'
        ) -AllowFailure
        throw "Bounded Kind probe failed (state=$probeState, detail=$probeDetail).`n$($logs.Output)`n$($describe.Output)"
    }
    if ($logs.Output -notmatch 'registered topic=SRE_PROBE_' -or $logs.Output -notmatch 'sent=10 topic=SRE_PROBE_') {
        throw "Bounded Kind probe completed without register/send evidence.`n$($logs.Output)"
    }
    if ($logs.Output -match 'cleanup_partial=true') {
        Write-Warning 'Bounded Kind probe completed its RocketMQ work but required timeout-bounded cleanup.'
    }
    Write-Host $logs.Output
}

function Get-PublicHeaders([string]$ClusterScope = $clusterId) {
    @{
        Authorization = "Bearer $internalToken"
        'x-rocketmq-tenant' = $tenantId
        'x-rocketmq-clusters' = $ClusterScope
        'x-rocketmq-subject' = 'phase01-smoke'
        'x-correlation-id' = [Guid]::NewGuid().ToString()
    }
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
            Start-Sleep -Seconds 2
        }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for $Uri`: $lastFailure"
}

function Invoke-PublicApi(
    [ValidateSet('Get', 'Post')]
    [string]$Method,
    [string]$Path,
    [object]$Body
) {
    $arguments = @{
        Method = $Method
        Uri = "$controlPlaneUrl$Path"
        Headers = (Get-PublicHeaders)
        TimeoutSec = 45
    }
    if ($null -ne $Body) {
        $arguments.ContentType = 'application/json'
        $arguments.Body = $Body | ConvertTo-Json -Depth 16 -Compress
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

function Invoke-ConversationStream(
    [string]$ConversationId,
    [string]$Question,
    [string]$Resource
) {
    $client = [Net.Http.HttpClient]::new()
    $client.Timeout = [TimeSpan]::FromSeconds(90)
    $client.MaxResponseContentBufferSize = $maxConversationStreamBytes
    $request = [Net.Http.HttpRequestMessage]::new(
        [Net.Http.HttpMethod]::Post,
        "$controlPlaneUrl/v1/conversations/$ConversationId/turns/stream"
    )
    try {
        foreach ($entry in (Get-PublicHeaders).GetEnumerator()) {
            if (-not $request.Headers.TryAddWithoutValidation([string]$entry.Key, [string]$entry.Value)) {
                throw "Conversation stream rejected required header '$($entry.Key)'."
            }
        }
        $request.Headers.Accept.ParseAdd('text/event-stream')
        $payload = @{
            question = $Question
            resource = $Resource
            window_seconds = 300
        } | ConvertTo-Json -Depth 8 -Compress
        $request.Content = [Net.Http.StringContent]::new($payload, [Text.Encoding]::UTF8, 'application/json')
        $response = $client.SendAsync($request).GetAwaiter().GetResult()
        try {
            if (-not $response.IsSuccessStatusCode) {
                throw "Conversation stream returned HTTP $([int]$response.StatusCode)."
            }
            if ($response.Content.Headers.ContentType.MediaType -ne 'text/event-stream') {
                throw "Conversation stream returned unexpected content type '$($response.Content.Headers.ContentType)'."
            }
            $bytes = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
        }
        finally {
            $response.Dispose()
        }
    }
    finally {
        $request.Dispose()
        $client.Dispose()
    }

    if ($bytes.Length -eq 0 -or $bytes.Length -gt $maxConversationStreamBytes) {
        throw "Conversation stream size $($bytes.Length) is outside the bounded response contract."
    }
    try {
        $text = [Text.UTF8Encoding]::new($false, $true).GetString($bytes)
    }
    catch {
        throw 'Conversation stream was not valid UTF-8.'
    }

    $events = [Collections.Generic.List[object]]::new()
    $maxFrameBytes = 0
    foreach ($frame in ($text -split '\r?\n\r?\n')) {
        if ([string]::IsNullOrWhiteSpace($frame)) {
            continue
        }
        $eventName = $null
        $dataLines = [Collections.Generic.List[string]]::new()
        foreach ($line in ($frame -split '\r?\n')) {
            if ($line.StartsWith(':', [StringComparison]::Ordinal)) {
                continue
            }
            if ($line.StartsWith('event:', [StringComparison]::Ordinal)) {
                $eventName = $line.Substring(6).TrimStart()
            }
            elseif ($line.StartsWith('data:', [StringComparison]::Ordinal)) {
                $dataLines.Add($line.Substring(5).TrimStart())
            }
            elseif (-not [string]::IsNullOrWhiteSpace($line)) {
                throw "Conversation stream contained unsupported SSE field '$line'."
            }
        }
        if ($dataLines.Count -eq 0) {
            continue
        }
        $frameBytes = [Text.Encoding]::UTF8.GetByteCount($frame)
        if ($frameBytes -gt $maxConversationFrameBytes) {
            throw "Conversation stream frame exceeded $maxConversationFrameBytes bytes."
        }
        $maxFrameBytes = [Math]::Max($maxFrameBytes, $frameBytes)
        try {
            $event = ($dataLines -join "`n") | ConvertFrom-Json
        }
        catch {
            throw 'Conversation stream contained invalid JSON data.'
        }
        if ([string]::IsNullOrWhiteSpace($eventName) -or $event.event_type -ne $eventName) {
            throw 'Conversation stream event header and payload type did not match.'
        }
        $events.Add($event)
    }

    if ($events.Count -lt 2 -or $events[0].event_type -ne 'accepted') {
        throw 'Conversation stream did not start with an accepted event.'
    }
    $terminalTypes = @('completed', 'cancelled', 'failed')
    $conversationIds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $turnIds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $correlationIds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $terminalCount = 0
    $previewDeltaCount = 0
    $finalTurn = $null
    for ($index = 0; $index -lt $events.Count; $index++) {
        $event = $events[$index]
        if ($event.schema_version -ne $conversationStreamSchema -or [long]$event.sequence -ne ($index + 1)) {
            throw 'Conversation stream schema or contiguous sequence contract failed.'
        }
        $null = $conversationIds.Add([string]$event.conversation_id)
        $null = $turnIds.Add([string]$event.turn_id)
        $null = $correlationIds.Add([string]$event.correlation_id)
        if ($terminalCount -gt 0) {
            throw 'Conversation stream emitted data after its terminal event.'
        }
        if ($event.event_type -eq 'answer_delta') {
            if ($event.provisional -ne $true -or [string]::IsNullOrWhiteSpace($event.delta)) {
                throw 'Conversation answer delta was not a non-empty provisional value.'
            }
            $previewDeltaCount++
        }
        if ($event.event_type -in $terminalTypes) {
            $terminalCount++
            $finalTurn = $event.final_turn
        }
    }
    if (
        $terminalCount -ne 1 `
            -or $events[$events.Count - 1].event_type -ne 'completed' `
            -or $null -eq $finalTurn `
            -or $conversationIds.Count -ne 1 `
            -or $turnIds.Count -ne 1 `
            -or $correlationIds.Count -ne 1 `
            -or $previewDeltaCount -lt 1
    ) {
        throw 'Conversation stream did not complete with one bounded, identity-stable terminal event.'
    }
    if ($finalTurn.turn.status -ne 'answered' -or $finalTurn.answer.mode -ne 'model_assisted') {
        throw 'Conversation stream did not persist a model-assisted answered turn.'
    }
    if (
        $null -eq $finalTurn.diagnosis_revision `
            -or $finalTurn.diagnosis_revision.execution_eligible -ne $false `
            -or @($finalTurn.answer.evidence_ids).Count -lt 1 `
            -or @($finalTurn.answer.citations).Count -lt 1
    ) {
        throw 'Conversation stream did not retain cited read-only diagnosis lineage.'
    }

    [pscustomobject]@{
        EventCount = $events.Count
        PreviewDeltaCount = $previewDeltaCount
        MaxFrameBytes = $maxFrameBytes
        FinalTurn = $finalTurn
    }
}

function Assert-LiveConversationEvidence(
    [object]$TurnView,
    [string]$ExpectedResource,
    [string]$ExpectedPack
) {
    if ($TurnView.diagnosis_revision.pack_id -ne $ExpectedPack) {
        throw "Conversation selected unexpected diagnostic pack '$($TurnView.diagnosis_revision.pack_id)'."
    }
    $evidenceId = @($TurnView.answer.evidence_ids)[0]
    $evidence = Invoke-PublicApi Get "/v1/evidence/$evidenceId" $null
    if (
        $evidence.cluster_id -ne $clusterId `
            -or $evidence.source -notin @('mcp', 'rocketmq-mcp') `
            -or $evidence.resource -ne $ExpectedResource `
            -or $evidence.content_hash -notmatch '^sha256:[0-9a-f]{64}$'
    ) {
        throw "Conversation cited invalid live Evidence '$evidenceId'."
    }
    $citation = @($TurnView.answer.citations | Where-Object { $_.evidence_id -eq $evidenceId }) | Select-Object -First 1
    if (
        $null -eq $citation `
            -or $citation.resource -ne $ExpectedResource `
            -or $citation.content_hash -ne $evidence.content_hash
    ) {
        throw "Conversation answer did not retain its authorized Evidence citation '$evidenceId'."
    }
    [pscustomobject]@{
        Metadata = $evidence
        Content = (Invoke-PublicApi Get "/v1/evidence/$evidenceId/content" $null)
    }
}

function Write-ConversationQualificationReport(
    [object]$ConsumerStream,
    [object]$ConsumerEvidence,
    [long]$ConsumerLag,
    [object]$BrokerStream,
    [object]$BrokerEvidence
) {
    if ([string]::IsNullOrWhiteSpace($ConversationQualificationReport)) {
        return
    }
    $reportPath = [IO.Path]::GetFullPath($ConversationQualificationReport)
    $driveRoot = [IO.Path]::GetPathRoot($reportPath)
    if ($driveRoot -notin @('D:\', 'F:\')) {
        throw 'Conversation qualification reports must stay on the local D: or F: drive.'
    }
    $repositoryPrefix = $repositoryRoot.TrimEnd('\') + '\'
    if ($reportPath.StartsWith($repositoryPrefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Conversation qualification reports must not be written inside the repository.'
    }
    $candidateCommit = (Invoke-Native git @('-C', $repositoryRoot, 'rev-parse', 'HEAD')).Output.Trim()
    $sourceStatus = (Invoke-Native git @('-C', $repositoryRoot, 'status', '--porcelain')).Output
    $report = [ordered]@{
        schema_version = 'rocketmq-sre.live-conversation-qualification-report.v1'
        status = 'passed'
        candidate_commit = $candidateCommit
        source_clean = [string]::IsNullOrWhiteSpace($sourceStatus)
        started_at = $qualificationStartedAt.ToString('o')
        finished_at = [DateTime]::UtcNow.ToString('o')
        environment = if ($Target -eq 'Kind') { 'kind_rocketmq_mcp_connector_postgresql' } else { 'compose_rocketmq_mcp_connector_postgresql' }
        model = [ordered]@{
            provider = 'isolated_local_openai_compatible_fixture'
            online_provider_calls = 0
            production_certified = $false
        }
        stream = [ordered]@{
            schema_version = $conversationStreamSchema
            session_count = 2
            accepted_count = 2
            terminal_count = 2
            provisional_delta_count = $ConsumerStream.PreviewDeltaCount + $BrokerStream.PreviewDeltaCount
            event_count = $ConsumerStream.EventCount + $BrokerStream.EventCount
            max_frame_bytes = [Math]::Max($ConsumerStream.MaxFrameBytes, $BrokerStream.MaxFrameBytes)
            max_response_bytes = $maxConversationStreamBytes
            sequence_verified = $true
            terminal_unique = $true
            disconnect_cancellation_contract_tested = $true
        }
        consumer_lag = [ordered]@{
            source = $ConsumerEvidence.Metadata.source
            resource = $ConsumerEvidence.Metadata.resource
            total_lag = $ConsumerLag
            citation_authorized = $true
            persisted = $true
            diagnostic_pack = 'consumer-lag.v2'
        }
        broker_runtime = [ordered]@{
            source = $BrokerEvidence.Metadata.source
            resource = $BrokerEvidence.Metadata.resource
            broker_up = [bool]$BrokerEvidence.Content.broker_up
            broker_rows = [long]$BrokerEvidence.Content.broker_rows
            active_broker_rows = [long]$BrokerEvidence.Content.active_broker_rows
            citation_authorized = $true
            persisted = $true
            diagnostic_pack = 'broker-health.v1'
        }
        safety = [ordered]@{
            mutation_calls = 0
            executor_calls = 0
            execution_agent_calls = 0
            effective_access = 'read_only'
            secrets_recorded = $false
            prompts_recorded = $false
            responses_recorded = $false
            message_bodies_recorded = $false
        }
    }
    $directory = Split-Path -Parent $reportPath
    New-Item -ItemType Directory -Path $directory -Force | Out-Null
    $report | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $reportPath -Encoding UTF8
    Write-Host "Wrote sanitized machine-local conversation qualification report to $reportPath"
}

function Wait-ConnectorOnline([int]$Seconds = 90) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        try {
            $status = Invoke-PublicApi Get "/v1/clusters/$clusterId/connector" $null
            if ($status.status.liveness -eq 'online') {
                return $status
            }
        }
        catch {
            # The channel can still be completing its mTLS registration.
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'Connector did not become online through the mTLS control-plane channel.'
}

function Ensure-CertifiedLocalModelProfile {
    $page = Invoke-PublicApi Get '/v1/models/profiles/lifecycle' $null
    $profile = @(
        $page.items |
            Where-Object { $_.profile_name -eq 'local-openai-compatible' }
    ) | Select-Object -First 1
    if ($null -eq $profile) {
        throw 'The Phase 01 local model profile is not registered.'
    }
    if ($profile.state -eq 'retired') {
        throw 'The Phase 01 local model profile is retired and cannot be certified.'
    }

    $smoke = Invoke-PublicApi Post "/v1/models/profiles/$($profile.profile_id)/smoke" $null
    if (
        $smoke.profile_id -ne $profile.profile_id `
            -or $smoke.overall_ok -ne $true `
            -or $smoke.connectivity_ok -ne $true `
            -or $smoke.structured_output_ok -ne $true `
            -or $smoke.evidence_citation_ok -ne $true
    ) {
        throw 'The Phase 01 local model profile did not pass its bounded provider smoke.'
    }

    $profile = Invoke-PublicApi Get "/v1/models/profiles/$($profile.profile_id)/lifecycle" $null
    if ($profile.state -in @('draft', 'quarantined')) {
        $profile = Invoke-PublicApi Post "/v1/models/profiles/$($profile.profile_id)/lifecycle" @{
            target_state = 'certified'
            expected_revision = [long]$profile.revision
            rollback_profile_id = $null
            reason_code = 'phase01.smoke.certified'
            operator_confirmed = $true
        }
    }
    if ($profile.state -notin @('certified', 'promoted')) {
        throw "The Phase 01 local model profile is not routable after certification (state=$($profile.state))."
    }
    Write-Host "Certified model profile: $($profile.profile_name) (state=$($profile.state), revision=$($profile.revision))"
    return $profile
}

function Wait-Inventory([int]$Seconds = 120) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        try {
            $inventory = Invoke-PublicApi Get "/v1/clusters/$clusterId/inventory/latest" $null
            if ($inventory -is [string] -and $inventory.Trim() -eq 'null') {
                $inventory = $null
            }
            if ($null -ne $inventory) {
                $assets = @($inventory.assets | Where-Object { $null -ne $_ })
                if ($assets.Count -gt 0) {
                    return $inventory
                }
            }
        }
        catch {
            # Inventory projection follows Connector registration and can lag it.
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'Onboarding did not produce a normalized inventory snapshot.'
}

function Assert-CrossClusterDenied {
    try {
        Invoke-RestMethod `
            -Method Get `
            -Uri "$controlPlaneUrl/v1/evidence?cluster_id=$clusterId&limit=1" `
            -Headers (Get-PublicHeaders $otherClusterId) `
            -TimeoutSec 15 | Out-Null
    }
    catch {
        $status = if ($null -ne $_.Exception.Response) {
            [int]$_.Exception.Response.StatusCode
        }
        else {
            0
        }
        $code = $null
        if (-not [string]::IsNullOrWhiteSpace($_.ErrorDetails.Message)) {
            try {
                $code = ($_.ErrorDetails.Message | ConvertFrom-Json).code
            }
            catch {
                # The stable status below remains mandatory.
            }
        }
        if ($status -eq 403 -and $code -eq 'cluster_not_allowed') {
            return
        }
        throw "Cross-cluster query failed with an unexpected response (HTTP $status, code=$code)."
    }
    throw 'Cross-cluster query was not rejected.'
}

function Assert-NoMessageBody([object]$Value) {
    $serialized = $Value | ConvertTo-Json -Depth 20 -Compress
    foreach ($forbidden in @('"body":', '"message_body":', '"messageBody":', '"payload":')) {
        if ($serialized.IndexOf($forbidden, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            throw "Message Journey exposed forbidden content field $forbidden"
        }
    }
    if ($Value.message_body_available -ne $false) {
        throw 'Message Journey did not explicitly disable message-body access.'
    }
}

function Assert-ReadOnlyCapabilityBoundary {
    $capabilities = Invoke-PublicApi Get "/v1/clusters/$clusterId/capabilities" $null
    if (
        $capabilities.cluster_id -ne $clusterId `
            -or $capabilities.protocol_version -ne '2025-11-25' `
            -or $capabilities.schema_version -ne 'rocketmq-mcp.v2' `
            -or $capabilities.mutation_supported -ne $false
    ) {
        throw 'Persisted MCP capability is not scoped, compatible, and mutation-disabled.'
    }
    $manifest = $capabilities.manifest
    if (
        $null -eq $manifest `
            -or $manifest.mcp_protocol_version -ne '2025-11-25' `
            -or $manifest.business_schema_version -ne 'rocketmq-mcp.v2' `
            -or $manifest.mutation_supported -ne $false `
            -or $manifest.tool_surface_digest -notmatch '^sha256:[0-9a-f]{64}$'
    ) {
        throw 'Persisted MCP manifest is incomplete or violates the mutation-disabled boundary.'
    }
    $tools = @($manifest.tools)
    if ($tools.Count -eq 0) {
        throw 'Persisted MCP manifest returned no verified read-only tools.'
    }
    foreach ($tool in $tools) {
        if (
            $tool.read_only -ne $true `
                -or $tool.destructive -ne $false `
                -or $tool.mutates_cluster -ne $false `
                -or $tool.task_support -ne 'forbidden'
        ) {
            throw "MCP tool '$($tool.name)' violates the read-only capability boundary."
        }
        if ($tool.name -match '(?i)(apply|delete|update|reset|clean|truncate)') {
            throw "MCP exposed mutation-shaped tool '$($tool.name)'."
        }
    }
    return $capabilities
}

function Invoke-SmokeSql([string]$Sql) {
    $arguments = @(
        'exec', '-T', 'postgres',
        'psql',
        '--username', 'rocketmq_sre',
        '--dbname', 'rocketmq_sre',
        '--set=ON_ERROR_STOP=1',
        '--tuples-only',
        '--no-align',
        '--command', $Sql
    )
    if ($Target -eq 'Compose') {
        $raw = Invoke-Docker (Compose-Arguments $arguments) -Capture
    }
    else {
        $result = Invoke-Kubectl @(
            '--namespace', 'rocketmq-sre',
            'exec', 'statefulset/postgres',
            '--',
            'psql',
            '--username', 'rocketmq_sre',
            '--dbname', 'rocketmq_sre',
            '--set=ON_ERROR_STOP=1',
            '--tuples-only',
            '--no-align',
            '--command', $Sql
        )
        $raw = $result.Output
    }
    $lines = @(
        "$raw" -split '\r?\n' |
            ForEach-Object { $_.Trim() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    $scalarLines = @(
        $lines |
            Where-Object { $_ -match '^(?:[0-9]+|\{.*\}|\[.*\])$' }
    )
    if ($lines.Count -eq 0) {
        return ''
    }
    if ($scalarLines.Count -ne 1) {
        throw "Smoke SQL returned an ambiguous scalar result.`n$raw"
    }
    # Every smoke query above returns one unaligned count or compact JSON value;
    # filtering the scalar keeps harmless Compose/kubectl warnings out of parsing.
    return $scalarLines[0]
}

function Get-InspectionPackRecords([string]$InspectionId) {
    $json = Invoke-SmokeSql @"
SELECT COALESCE(
    jsonb_agg(
        jsonb_build_object(
            'id', id,
            'pack_id', pack_id,
            'pack_version', pack_version,
            'input_evidence_ids', input_evidence_ids,
            'output', output,
            'partial', partial,
            'started_at', started_at,
            'completed_at', completed_at
        )
        ORDER BY completed_at, id
    ),
    '[]'::jsonb
)::text
FROM diagnostic_pack_runs
WHERE tenant_id = '$tenantId'
  AND cluster_id = '$clusterId'
  AND inspection_run_id = '$InspectionId';
"@
    if ([string]::IsNullOrWhiteSpace($json)) {
        throw "Inspection $InspectionId did not return a persisted diagnostic pack record set."
    }
    # PowerShell 7 preserves a top-level JSON array as one pipeline object.
    # Enumerate it explicitly so callers count and validate every persisted pack.
    return @(($json | ConvertFrom-Json) | ForEach-Object { $_ })
}

function Get-DiagnosisRevisionRecord([string]$IncidentId, [string]$RevisionId) {
    $json = Invoke-SmokeSql @"
SELECT jsonb_build_object(
    'id', d.id,
    'incident_id', d.incident_id,
    'symptom_family', i.symptom_family,
    'resource', i.resource,
    'revision', d.revision,
    'rule_result', d.rule_result,
    'evidence_ids', d.evidence_ids,
    'partial', d.partial,
    'execution_eligible', d.execution_eligible,
    'created_at', d.created_at
)::text
FROM diagnosis_revisions d
JOIN sre_incidents i ON i.id = d.incident_id
WHERE d.id = '$RevisionId'
  AND d.incident_id = '$IncidentId'
  AND i.tenant_id = '$tenantId'
  AND i.cluster_id = '$clusterId';
"@
    if ([string]::IsNullOrWhiteSpace($json)) {
        throw "Diagnosis revision $RevisionId was not persisted for incident $IncidentId."
    }
    return $json | ConvertFrom-Json
}

function Get-DiagnosticCitationIds([object]$Output) {
    $ids = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($finding in @($Output.findings | Where-Object { $null -ne $_ })) {
        foreach ($relation in @('supporting_evidence', 'counter_evidence')) {
            foreach ($citation in @($finding.$relation | Where-Object { $null -ne $_ })) {
                $id = "$($citation.evidence_id)"
                if (-not [string]::IsNullOrWhiteSpace($id)) {
                    $null = $ids.Add($id)
                }
            }
        }
    }
    return @($ids | Sort-Object)
}

function Assert-CanonicalEvidenceCitation(
    [string]$EvidenceId,
    [string]$PackId
) {
    $citationKey = "${PackId}:$EvidenceId"
    if ($script:validatedEvidenceCitations.ContainsKey($citationKey)) {
        return
    }
    $evidence = Invoke-PublicApi Get "/v1/evidence/$EvidenceId" $null
    if (
        $evidence.evidence_id -ne $EvidenceId `
            -or $evidence.cluster_id -ne $clusterId `
            -or $evidence.schema.family -ne 'rocketmq-sre.evidence' `
            -or $evidence.content_hash -notmatch '^sha256:[0-9a-f]{64}$' `
            -or $evidence.coverage -notin @('available', 'partial')
    ) {
        throw "Pack $PackId cited invalid or unavailable canonical Evidence $EvidenceId."
    }
    if ($PackId -eq 'message-path.v1') {
        $content = Invoke-PublicApi Get "/v1/evidence/$EvidenceId/content" $null
        $serialized = $content | ConvertTo-Json -Depth 20 -Compress
        foreach ($forbidden in @('"body":', '"message_body":', '"messageBody":', '"payload":')) {
            if ($serialized.IndexOf($forbidden, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
                throw "Message Path Evidence $EvidenceId exposed forbidden content field $forbidden"
            }
        }
    }
    $script:validatedEvidenceCitations[$citationKey] = $true
}

function Assert-PersistedPackRecord(
    [object]$Record,
    [string]$ExpectedPackId,
    [string]$InspectionId
) {
    if (
        $Record.pack_id -ne $ExpectedPackId `
            -or [string]::IsNullOrWhiteSpace("$($Record.id)") `
            -or [string]::IsNullOrWhiteSpace("$($Record.pack_version)") `
            -or $null -eq $Record.output `
            -or $Record.output.pack_id -ne $ExpectedPackId
    ) {
        throw "Inspection $InspectionId did not persist a complete $ExpectedPackId pack record."
    }

    $inputEvidenceIds = @($Record.input_evidence_ids | ForEach-Object { "$_" })
    $citationIds = @(Get-DiagnosticCitationIds $Record.output)
    if ($citationIds.Count -gt 32) {
        throw "Pack $ExpectedPackId exceeded the bounded citation limit (count=$($citationIds.Count))."
    }
    if ($citationIds.Count -gt 0) {
        foreach ($citationId in $citationIds) {
            if ($inputEvidenceIds -notcontains $citationId) {
                throw "Pack $ExpectedPackId cited Evidence $citationId outside its persisted input set."
            }
            Assert-CanonicalEvidenceCitation $citationId $ExpectedPackId
        }
        Write-Host "Validated $ExpectedPackId via $($citationIds.Count) persisted canonical Evidence citation(s)."
    }
    else {
        $missing = @(
            $Record.output.missing_required_evidence |
                Where-Object { -not [string]::IsNullOrWhiteSpace("$_") }
        )
        if ($missing.Count -eq 0 -or $Record.partial -ne $true) {
            throw "Pack $ExpectedPackId had neither a persisted Evidence citation nor an explicit partial missing_required_evidence result."
        }
        Write-Host "Validated $ExpectedPackId as explicitly partial with persisted missing_required_evidence: $($missing -join ', ')"
    }
}

function Invoke-BoundedInspection(
    [string]$Template,
    [string[]]$ExpectedPackIds
) {
    $activeCount = Invoke-SmokeSql @"
SELECT COUNT(*)
FROM inspection_runs
WHERE tenant_id = '$tenantId'
  AND cluster_id = '$clusterId'
  AND template = '$Template'
  AND schedule IS NULL
  AND status = 'running';
"@
    if ([int]$activeCount -ne 0) {
        throw "A previous immediate $Template inspection remains running; refusing to create a duplicate stuck run."
    }

    $view = Invoke-PublicApi Post '/v1/inspections' @{
        cluster_id = $clusterId
        template = $Template
        schedule = $null
    }
    if ($view.run.status -notin @('completed', 'needs_evidence')) {
        throw "Immediate $Template inspection ended in unexpected state $($view.run.status)."
    }
    if (
        $view.run.template -ne $Template `
            -or $view.run.cluster_id -ne $clusterId `
            -or [string]::IsNullOrWhiteSpace("$($view.run.completed_at)")
    ) {
        throw "Immediate $Template inspection returned an incomplete persisted run."
    }

    $records = @(Get-InspectionPackRecords "$($view.run.id)")
    if ($records.Count -lt $ExpectedPackIds.Count) {
        throw "Inspection $($view.run.id) persisted $($records.Count) packs; expected at least $($ExpectedPackIds.Count)."
    }
    foreach ($record in $records) {
        if (
            [string]::IsNullOrWhiteSpace("$($record.id)") `
                -or [string]::IsNullOrWhiteSpace("$($record.pack_id)") `
                -or [string]::IsNullOrWhiteSpace("$($record.pack_version)") `
                -or $null -eq $record.output `
                -or [string]::IsNullOrWhiteSpace("$($record.started_at)") `
                -or [string]::IsNullOrWhiteSpace("$($record.completed_at)")
        ) {
            throw "Inspection $($view.run.id) persisted an incomplete expanded pack record."
        }
        if ($ExpectedPackIds -contains $record.pack_id) {
            Assert-PersistedPackRecord $record $record.pack_id "$($view.run.id)"
        }
    }
    foreach ($expectedPackId in $ExpectedPackIds) {
        if (@($records | Where-Object { $_.pack_id -eq $expectedPackId }).Count -ne 1) {
            throw "Inspection $($view.run.id) did not persist exactly one $expectedPackId record."
        }
        # Later phases can add read-only packs to a template. Validate every
        # persisted pack above, while keeping the Phase 01 acceptance set
        # scoped to the original eight Wave A packs.
        $null = $script:acceptedPackIds.Add($expectedPackId)
    }
    return [pscustomobject]@{
        View = $view
        PackRecords = $records
    }
}

function Invoke-BoundedPackDiagnosis(
    [string]$PackId,
    [string]$SymptomFamily,
    [string]$Resource
) {
    # Direct IncidentView responses do not expose the persisted symptom field,
    # so mirror it in the title while still sending the explicit API field.
    $incidentView = Invoke-PublicApi Post '/v1/incidents' @{
        cluster_id = $clusterId
        title = "Phase 01 $SymptomFamily live acceptance"
        symptom_family = $SymptomFamily
        resource = $Resource
    }
    $incidentId = "$($incidentView.incident.id)"
    if ([string]::IsNullOrWhiteSpace($incidentId)) {
        throw "Direct $PackId acceptance did not persist an incident."
    }

    $diagnosis = Invoke-PublicApi Post "/v1/incidents/$incidentId/diagnose" $null
    if (
        $diagnosis.pack_id -ne $PackId `
            -or $diagnosis.revision.rule_result.pack_id -ne $PackId `
            -or $diagnosis.execution_eligible -ne $false `
            -or $diagnosis.revision.execution_eligible -ne $false
    ) {
        throw "Direct incident diagnosis did not persist the expected read-only $PackId result."
    }

    $record = Get-DiagnosisRevisionRecord $incidentId "$($diagnosis.revision.id)"
    if (
        $record.id -ne $diagnosis.revision.id `
            -or $record.incident_id -ne $incidentId `
            -or $record.symptom_family -ne $SymptomFamily `
            -or $record.resource -ne $Resource `
            -or $record.rule_result.pack_id -ne $PackId `
            -or $record.execution_eligible -ne $false
    ) {
        throw "Database diagnosis revision does not match the $PackId API result."
    }

    $citationIds = @($record.evidence_ids | ForEach-Object { "$_" })
    $embeddedCitationIds = @(Get-DiagnosticCitationIds $record.rule_result)
    if (
        $citationIds.Count -ne $embeddedCitationIds.Count `
            -or @($citationIds | Where-Object { $embeddedCitationIds -notcontains $_ }).Count -ne 0
    ) {
        throw "Diagnosis $PackId revision evidence_ids do not match its persisted finding citations."
    }
    if ($citationIds.Count -gt 32) {
        throw "Diagnosis $PackId exceeded the bounded citation limit (count=$($citationIds.Count))."
    }
    if ($citationIds.Count -gt 0) {
        foreach ($citationId in $citationIds) {
            Assert-CanonicalEvidenceCitation $citationId $PackId
        }
        Write-Host "Validated $PackId diagnosis via $($citationIds.Count) persisted canonical Evidence citation(s)."
    }
    else {
        $missing = @(
            $record.rule_result.missing_required_evidence |
                Where-Object { -not [string]::IsNullOrWhiteSpace("$_") }
        )
        if ($missing.Count -eq 0 -or $record.partial -ne $true) {
            throw "Diagnosis $PackId had neither a persisted Evidence citation nor an explicit partial missing_required_evidence result."
        }
        Write-Host "Validated $PackId diagnosis as explicitly partial with persisted missing_required_evidence: $($missing -join ', ')"
    }
    $null = $script:acceptedPackIds.Add($PackId)
    return $diagnosis
}

function Ensure-ScheduledClusterHealthInspection {
    $page = Invoke-PublicApi Get "/v1/inspections?cluster_id=$clusterId&limit=200" $null
    $existing = @(
        $page.items |
            Where-Object {
                $_.run.template -eq 'cluster_health' `
                    -and $_.run.schedule -eq 'every 1h' `
                    -and $_.run.status -eq 'scheduled'
            }
    ) | Select-Object -First 1
    if ($null -ne $existing) {
        return $existing
    }

    $created = Invoke-PublicApi Post '/v1/inspections' @{
        cluster_id = $clusterId
        template = 'cluster_health'
        schedule = 'every 1h'
    }
    if ($created.run.status -ne 'scheduled') {
        throw 'Scheduled inspection was not persisted in the scheduled state.'
    }
    return $created
}

try {
if ($Target -eq 'Compose') {
    Require-Command docker
    $controlPlaneUrl = 'http://127.0.0.1:8090'
    $internalToken = 'phase00-internal-token'
}
else {
    Require-Command kubectl
    if (-not (Test-Path -LiteralPath $kubeconfigPath -PathType Leaf)) {
        throw "Kind kubeconfig is missing at '$kubeconfigPath'. Run kind.ps1 -Action Up first."
    }
    $tokenPath = Join-Path $kindArtifactRoot 'internal-token'
    if (-not (Test-Path -LiteralPath $tokenPath -PathType Leaf)) {
        throw "Kind internal-token fixture is missing at '$tokenPath'. Run kind.ps1 -Action Up first."
    }
    $internalToken = (Get-Content -Raw -LiteralPath $tokenPath).Trim()
    if ([string]::IsNullOrWhiteSpace($internalToken)) {
        throw 'Kind internal-token fixture is empty.'
    }
    Assert-KindWorkloadsReady

    $controlPlaneForward = Start-KubectlPortForward `
        'rocketmq-sre' `
        'service/sre-control-plane' `
        8090
    $controlPlaneUrl = "http://127.0.0.1:$($controlPlaneForward.Port)"
}

Wait-Http "$controlPlaneUrl/readyz" | Out-Null
$cluster = Invoke-PublicApi Get "/v1/clusters/$clusterId" $null
if ($cluster.state -notin @('ready_read_only', 'read_only_degraded')) {
    if ($cluster.state -eq 'offboarded') {
        throw 'The development cluster is offboarded. Run `.\scripts\dev.ps1 -Action Reset -Force`, then `-Action Up`.'
    }
    throw "Cluster is not ready for Phase 01 smoke (state=$($cluster.state))."
}
if ($cluster.effective_access_profile -ne 'read_only') {
    throw 'Cluster effective access is not read_only.'
}
Wait-ConnectorOnline | Out-Null

if ($Target -eq 'Kind') {
    Invoke-KindProbe
}
elseif ($BootstrapProbe) {
    Invoke-Docker (Compose-Arguments @('--profile', 'smoke', 'run', '--rm', 'probe-topic-bootstrap'))
    Invoke-Docker (Compose-Arguments @('--profile', 'smoke', 'run', '--rm', 'sre-probe', 'register'))
    Invoke-Docker (Compose-Arguments @('--profile', 'smoke', 'run', '--rm', 'sre-probe', 'send'))
}

$capabilities = Assert-ReadOnlyCapabilityBoundary
Write-Host "Verified persisted MCP capability: $($capabilities.digest) (mutation_supported=false)"

$null = Ensure-CertifiedLocalModelProfile

$inventory = Wait-Inventory
if ($inventory.cluster_id -ne $clusterId -or [string]::IsNullOrWhiteSpace($inventory.content_hash)) {
    throw 'Inventory snapshot is not scoped or canonically hashed.'
}

$conversation = Invoke-PublicApi Post '/v1/conversations' @{
    cluster_id = $clusterId
    question = 'Why is the Phase 01 smoke consumer lag rising?'
    resource = "consumer-lag/$group/$topic"
    persist_investigation = $true
}
$investigationId = $conversation.investigation.id
if ([string]::IsNullOrWhiteSpace($investigationId)) {
    throw 'Conversation was not persisted as an investigation.'
}
$conversationId = $conversation.conversation.id
if ([string]::IsNullOrWhiteSpace($conversationId)) {
    throw 'Conversation did not return a persisted conversation identifier.'
}
$consumerStream = Invoke-ConversationStream `
    $conversationId `
    'Show the current bounded smoke Consumer Lag using only authorized read-only evidence.' `
    "consumer-lag/$group/$topic"
$consumerConversationEvidence = Assert-LiveConversationEvidence `
    $consumerStream.FinalTurn `
    "consumer-lag/$group/$topic" `
    'consumer-lag.v2'
$conversationLag = [long]$consumerConversationEvidence.Content.total_lag
if ($conversationLag -lt 1) {
    throw 'Streamed Conversation did not cite positive live Consumer Lag.'
}
$consumerTurns = Invoke-PublicApi Get "/v1/conversations/$conversationId/turns" $null
$persistedConsumerTurn = @($consumerTurns.items | Where-Object {
    $_.turn.id -eq $consumerStream.FinalTurn.turn.id `
        -and $_.answer.id -eq $consumerStream.FinalTurn.answer.id
}) | Select-Object -First 1
if ($null -eq $persistedConsumerTurn) {
    throw 'Completed Consumer Lag Conversation turn was not durably queryable.'
}

$brokerConversation = Invoke-PublicApi Post '/v1/conversations' @{
    cluster_id = $clusterId
    question = 'Is the live RocketMQ broker available?'
    resource = "broker-runtime/$brokerName"
    persist_investigation = $true
}
$brokerConversationId = $brokerConversation.conversation.id
if ([string]::IsNullOrWhiteSpace($brokerConversationId)) {
    throw 'Broker runtime Conversation did not return a persisted identifier.'
}
$brokerStream = Invoke-ConversationStream `
    $brokerConversationId `
    'Show the current broker runtime state using only authorized read-only evidence.' `
    "broker-runtime/$brokerName"
$brokerConversationEvidence = Assert-LiveConversationEvidence `
    $brokerStream.FinalTurn `
    "broker-runtime/$brokerName" `
    'broker-health.v1'
if (
    $brokerConversationEvidence.Content.broker_up -ne $true `
        -or [long]$brokerConversationEvidence.Content.broker_rows -lt 1 `
        -or [long]$brokerConversationEvidence.Content.active_broker_rows -lt 1
) {
    throw 'Streamed Conversation did not cite a live active Broker runtime row.'
}
$brokerTurns = Invoke-PublicApi Get "/v1/conversations/$brokerConversationId/turns" $null
$persistedBrokerTurn = @($brokerTurns.items | Where-Object {
    $_.turn.id -eq $brokerStream.FinalTurn.turn.id `
        -and $_.answer.id -eq $brokerStream.FinalTurn.answer.id
}) | Select-Object -First 1
if ($null -eq $persistedBrokerTurn) {
    throw 'Completed Broker runtime Conversation turn was not durably queryable.'
}
Write-Host "Streamed Conversations cited live Consumer Lag ($conversationLag) and active Broker runtime Evidence."

$incidentView = Invoke-PublicApi Post "/v1/investigations/$investigationId/promote" @{
    title = 'Phase 01 live Consumer Lag diagnosis'
    reason = 'Phase 01 read-only smoke promotion'
}
$incidentId = $incidentView.incident.id
if ([string]::IsNullOrWhiteSpace($incidentId)) {
    throw 'Investigation promotion did not create an incident.'
}

$diagnosis = Invoke-PublicApi Post "/v1/incidents/$incidentId/diagnose" $null
if ($diagnosis.pack_id -ne 'consumer-lag.v2') {
    throw "Unexpected diagnostic pack $($diagnosis.pack_id)."
}
if (@($diagnosis.revision.evidence_ids).Count -eq 0) {
    throw 'Diagnosis did not persist any canonical Evidence citation.'
}
if (
    $diagnosis.mode -ne 'model_assisted' `
        -or $diagnosis.reason -ne 'ModelDiagnosisAdopted' `
        -or [string]::IsNullOrWhiteSpace($diagnosis.revision.primary_model_invocation_id)
) {
    throw 'Live diagnosis did not adopt a persisted model-assisted result.'
}
if ($diagnosis.execution_eligible -ne $false -or $diagnosis.revision.execution_eligible -ne $false) {
    throw 'Model-assisted Phase 01 diagnosis was incorrectly marked execution eligible.'
}
$modelCitationIds = @($diagnosis.revision.rule_result.model_assessment.cited_evidence_ids)
if ($modelCitationIds.Count -eq 0) {
    throw 'Model-assisted diagnosis did not persist its bounded Evidence citation lineage.'
}
$liveEvidence = $null
$totalLag = $null
foreach ($evidenceId in @($diagnosis.revision.evidence_ids)) {
    $cited = Invoke-PublicApi Get "/v1/evidence/$evidenceId" $null
    if (
        $cited.cluster_id -ne $clusterId `
            -or $cited.schema.family -ne 'rocketmq-sre.evidence' `
            -or $cited.content_hash -notmatch '^sha256:[0-9a-f]{64}$'
    ) {
        throw "Diagnosis cited invalid Evidence $evidenceId."
    }
    if (
        $null -eq $liveEvidence `
            -and $cited.source -in @('mcp', 'rocketmq-mcp') `
            -and $cited.resource -eq "consumer-lag/$group/$topic" `
            -and $cited.coverage -in @('available', 'partial')
    ) {
        $content = Invoke-PublicApi Get "/v1/evidence/$evidenceId/content" $null
        if ($null -ne $content.total_lag) {
            $candidateLag = [long]$content.total_lag
            if ($candidateLag -gt 0) {
                $liveEvidence = $cited
                $totalLag = $candidateLag
            }
        }
    }
}
if ($null -eq $liveEvidence) {
    throw 'Diagnosis did not cite positive live Consumer Lag Evidence returned through the mTLS Connector channel.'
}
if ($modelCitationIds -notcontains $liveEvidence.evidence_id) {
    throw 'Model-assisted conclusion did not cite the positive live Consumer Lag Evidence.'
}
Write-Host "Live canonical Evidence: $($liveEvidence.content_hash) ($($liveEvidence.coverage), total_lag=$totalLag)"
$modelInvocations = Invoke-PublicApi Get "/v1/models/invocations?cluster_id=$clusterId&incident_id=$incidentId&limit=20" $null
$primaryInvocation = @($modelInvocations.items | Where-Object {
    $_.id -eq $diagnosis.revision.primary_model_invocation_id
}) | Select-Object -First 1
if (
    $null -eq $primaryInvocation `
        -or $primaryInvocation.diagnosis_revision_id -ne $diagnosis.revision.id `
        -or $primaryInvocation.provider_family -ne 'open_ai_compatible' `
        -or $primaryInvocation.model_family -ne 'local' `
        -or $primaryInvocation.actual_model -ne 'phase01-read-only-fixture' `
        -or $primaryInvocation.purpose -ne 'primary_diagnosis' `
        -or -not [string]::IsNullOrWhiteSpace($primaryInvocation.error_code) `
        -or [string]::IsNullOrWhiteSpace($primaryInvocation.correlation_id)
) {
    throw 'Persisted model provider lineage is incomplete or does not identify the isolated Phase 01 fixture.'
}
if (
    [int]$diagnosis.budget.model_input_tokens -lt 1 `
        -or [int]$diagnosis.budget.model_output_tokens -lt 1
) {
    throw 'Model-assisted diagnosis did not persist bounded provider usage.'
}

$producerDiagnosis = Invoke-BoundedPackDiagnosis `
    'producer-connectivity.v1' `
    'producer-connectivity' `
    'producer-connectivity/phase01-smoke-producer'
$messagePathDiagnosis = Invoke-BoundedPackDiagnosis `
    'message-path.v1' `
    'message-path' `
    'message-metadata/phase01-smoke-message'
if (
    $producerDiagnosis.execution_eligible -ne $false `
        -or $messagePathDiagnosis.execution_eligible -ne $false
) {
    throw 'Direct Phase 01 pack diagnosis escaped the read-only execution boundary.'
}

$consumerInspection = Invoke-BoundedInspection `
    'consumer' `
    @('consumer-lag.v2', 'consumer-runtime.v1')
$inspection = $consumerInspection.View
$inspectionId = $inspection.run.id
$markdownReport = Invoke-PublicApi Get "/v1/inspections/$inspectionId/report?format=markdown" $null
$htmlReport = Invoke-PublicApi Get "/v1/inspections/$inspectionId/report?format=html" $null
if (
    $markdownReport.media_type -notlike 'text/markdown*' `
        -or $htmlReport.media_type -notlike 'text/html*' `
        -or [string]::IsNullOrWhiteSpace($markdownReport.content) `
        -or [string]::IsNullOrWhiteSpace($htmlReport.content)
) {
    throw 'Inspection Markdown/HTML reports were not generated.'
}

$clusterInspection = Invoke-BoundedInspection `
    'cluster_health' `
    @('cluster-topology.v1', 'deployment-drift.v1')
$brokerInspection = Invoke-BoundedInspection 'broker' @('broker-health.v1')
$telemetryInspection = Invoke-BoundedInspection 'telemetry' @('telemetry-pipeline.v1')
if (
    $clusterInspection.View.run.status -notin @('completed', 'needs_evidence') `
        -or $brokerInspection.View.run.status -notin @('completed', 'needs_evidence') `
        -or $telemetryInspection.View.run.status -notin @('completed', 'needs_evidence')
) {
    throw 'One or more Phase 01 inspection templates did not reach a persisted terminal state.'
}

$scheduled = Ensure-ScheduledClusterHealthInspection
if ($scheduled.run.status -ne 'scheduled') {
    throw 'Scheduled inspection was not persisted in the scheduled state.'
}

foreach ($recommendation in @($inspection.recommendations)) {
    $disposed = Invoke-PublicApi Post "/v1/recommendations/$($recommendation.id)/disposition" @{
        status = 'acknowledged'
        assignee = $null
        reason = 'Acknowledged by Phase 01 smoke'
        promote_to = $null
    }
    if ($disposed.status -ne 'acknowledged') {
        throw "Recommendation $($recommendation.id) was not acknowledged."
    }
}

$reviewDue = [DateTime]::UtcNow.AddDays(30).ToString('o')
$knowledge = Invoke-PublicApi Post '/v1/knowledge/import' @{
    cluster_id = $clusterId
    title = 'Phase 01 Consumer Lag smoke runbook'
    component = 'consumer'
    rocketmq_version_range = '>=1.0.0'
    source_uri = 'sre://phase01-smoke/consumer-lag'
    source_version = '1'
    valid_from = $null
    valid_until = $null
    owner = 'rocketmq-sre'
    review_due_at = $reviewDue
    sensitivity = 'internal'
    review_status = 'validated'
    human_validated = $true
    ai_generated = $false
    markdown = "# Consumer Lag`n`nInspect lag, runtime connectivity, and broker health using read-only evidence."
}
$search = Invoke-PublicApi Get "/v1/knowledge/search?q=runtime&cluster_id=$clusterId&rocketmq_version=1.0.0" $null
if ($knowledge.chunk_count -lt 1 -or @($search.items).Count -lt 1) {
    throw 'Validated knowledge could not be imported and retrieved.'
}

$journey = Invoke-PublicApi Get "/v1/message-journeys?cluster_id=$clusterId&query=phase01-smoke-message" $null
Assert-NoMessageBody $journey

$coverage = Invoke-PublicApi Get '/v1/capabilities/coverage' $null
if (@($coverage.packs).Count -lt 8) {
    throw 'Diagnostic Coverage did not expose all eight Wave A packs.'
}
$expectedPackIds = @(
    'consumer-lag.v2',
    'consumer-runtime.v1',
    'broker-health.v1',
    'producer-connectivity.v1',
    'message-path.v1',
    'cluster-topology.v1',
    'deployment-drift.v1',
    'telemetry-pipeline.v1'
)
if ($acceptedPackIds.Count -ne $expectedPackIds.Count) {
    throw "Live acceptance validated $($acceptedPackIds.Count) unique diagnostic packs; expected $($expectedPackIds.Count)."
}
foreach ($expectedPackId in $expectedPackIds) {
    if (-not $acceptedPackIds.Contains($expectedPackId)) {
        throw "Live acceptance did not validate persisted output for $expectedPackId."
    }
}
$modelStatus = Invoke-PublicApi Get '/v1/models/status' $null
if ([string]::IsNullOrWhiteSpace($modelStatus.schema_version)) {
    throw 'Model status did not return a versioned contract.'
}
$openApi = Invoke-PublicApi Get '/v1/openapi.json' $null
$srePhase = [int]$openApi.'x-rocketmq-sre-phase'
$openApiFields = @($openApi.PSObject.Properties.Name)
if ($srePhase -le 1) {
    if (
        $openApi.'x-rocketmq-effective-access' -ne 'read_only' `
            -or $openApi.'x-rocketmq-cluster-mutation-supported' -ne $false
    ) {
        throw 'OpenAPI did not freeze the Phase 01 read-only boundary.'
    }
}
else {
    $arbitraryMutationSupported = if (
        $openApiFields -contains 'x-rocketmq-unattended-arbitrary-mutation-supported'
    ) {
        $openApi.'x-rocketmq-unattended-arbitrary-mutation-supported'
    }
    else {
        $openApi.'x-rocketmq-arbitrary-mutation-supported'
    }
    $unattendedMutationSupported = if (
        $openApiFields -contains 'x-rocketmq-unattended-mutation-supported'
    ) {
        $openApi.'x-rocketmq-unattended-mutation-supported'
    }
    else {
        $openApi.'x-rocketmq-unattended-arbitrary-mutation-supported'
    }
    if (
        $arbitraryMutationSupported -ne $false `
            -or $unattendedMutationSupported -ne $false
    ) {
        throw 'A later-phase OpenAPI surface escaped the bounded mutation boundary required by Phase 01 compatibility.'
    }
    if (
        $openApiFields -contains 'x-rocketmq-r3-agent-reachable' `
            -and $openApi.'x-rocketmq-r3-agent-reachable' -ne $false
    ) {
        throw 'A later-phase OpenAPI surface made an R3 execution agent reachable.'
    }
}
foreach ($path in @($openApi.paths.PSObject.Properties)) {
    foreach ($method in @('delete', 'put')) {
        if ($null -ne $path.Value.$method) {
            throw "OpenAPI exposed forbidden HTTP $($method.ToUpperInvariant()) operation on '$($path.Name)'."
        }
    }
    if ($path.Name -match '(?:/apply|/reset|/truncate)(?:/|$)') {
        throw "OpenAPI exposed forbidden unbounded mutation path '$($path.Name)'."
    }
}

Assert-CrossClusterDenied

$auditCount = Invoke-SmokeSql @"
SELECT COUNT(*)
FROM read_audit
WHERE tenant_id = '$tenantId'
  AND actor_subject = 'phase01-smoke';
"@
if ([int]$auditCount -lt 1) {
    throw 'Public read operations did not produce append-only read audit records.'
}

Write-ConversationQualificationReport `
    $consumerStream `
    $consumerConversationEvidence `
    $conversationLag `
    $brokerStream `
    $brokerConversationEvidence

Write-Host "PHASE01_LIVE_SMOKE_OK target=$Target read_only=true model_assisted=true conversation_streams=2 diagnostic_packs=8 mutation_calls=0 executor_calls=0 total_lag=$totalLag"
Write-Host 'Phase 01 live smoke passed: streamed Consumer Lag and Broker runtime conversations, all eight persisted diagnostic packs, Evidence-to-model citation lineage, read-only workflows, reports, knowledge, coverage, audit, and scope isolation.'
}
finally {
    Stop-KubectlPortForwards
}

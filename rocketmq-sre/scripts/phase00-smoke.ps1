# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Compose', 'Kind')]
    [string]$Target
)

$ErrorActionPreference = 'Stop'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '..'))
$composeDirectory = Join-Path $sreRoot 'deploy/dev'
$composeFile = Join-Path $composeDirectory 'compose.yaml'
$requiredSignalsQualificationPath = Join-Path $sreRoot 'config/qualification/required-signals.v1.json'
$clusterId = '00000000-0000-4000-8000-000000000001'
$tenantId = '00000000-0000-4000-8000-000000000002'
$topic = 'SRE_PROBE_00000000000040008000000000000001_00000000000000000000000000000000'
$group = 'SRE_PROBE_G_C_00000000000040008000000000000001_00000000000000000000000000000000'
$internalToken = 'phase00-internal-token'

function Get-PublicApiHeaders {
    @{
        Authorization = "Bearer $internalToken"
        'x-rocketmq-tenant' = $tenantId
        'x-rocketmq-clusters' = $clusterId
        'x-rocketmq-subject' = 'phase00-smoke'
    }
}

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found."
    }
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

function Compose-Arguments([string[]]$Arguments) {
    @(
        'compose',
        '--project-directory', $composeDirectory,
        '--file', $composeFile
    ) + $Arguments
}

function Get-IdentityFixtureToken(
    [ValidateSet('wrong_audience', 'missing_read_scope', 'different_cluster')]
    [string]$Profile
) {
    $document = Invoke-Docker (Compose-Arguments @(
        'exec', '-T', 'sre-connector',
        'curl', '--fail', '--silent', '--show-error',
        '--cacert', '/etc/rocketmq/tls/ca-cert.pem',
        '--header', 'Authorization: Bearer phase00-issuer-admin',
        '--data-urlencode', "profile=$Profile",
        'https://dev-issuer-tls:8443/admin/fixture-token'
    )) -Capture
    $token = ($document | ConvertFrom-Json).access_token
    if ([string]::IsNullOrWhiteSpace($token)) {
        throw "Development issuer did not return the '$Profile' identity fixture token."
    }
    return $token
}

function Invoke-McpIdentityProbe([string]$Token, [string]$Payload) {
    $response = Invoke-Docker (Compose-Arguments @(
        'exec', '-T', 'sre-connector',
        'curl', '--silent', '--show-error', '--include',
        '--write-out', "`nPHASE00_HTTP_STATUS:%{http_code}",
        '--cacert', '/etc/rocketmq/tls/ca-cert.pem',
        '--header', "Authorization: Bearer $Token",
        '--header', 'Content-Type: application/json',
        '--header', 'Accept: application/json, text/event-stream',
        '--header', 'MCP-Protocol-Version: 2025-11-25',
        '--data', $Payload,
        'https://127.0.0.1:8089/mcp'
    )) -Capture
    $statusMatch = [regex]::Match($response, '(?m)^PHASE00_HTTP_STATUS:(\d{3})\s*$')
    if (-not $statusMatch.Success) {
        throw 'MCP identity probe did not return an HTTP status marker.'
    }
    $challengeMatch = [regex]::Match($response, '(?im)^www-authenticate:\s*([^\r\n]+)')
    $withoutStatus = [regex]::Replace($response, '(?m)\r?\nPHASE00_HTTP_STATUS:\d{3}\s*$', '')
    $sections = @([regex]::Split($withoutStatus, '\r?\n\r?\n'))
    [PSCustomObject]@{
        Status = $statusMatch.Groups[1].Value
        Challenge = if ($challengeMatch.Success) { $challengeMatch.Groups[1].Value.Trim() } else { '' }
        Body = $sections[-1].Trim()
        Raw = $response
    }
}

function Assert-McpIdentityFailClosed {
    $initializePayload = '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"phase00-identity-smoke","version":"1.0.0"}}}'

    $wrongAudienceToken = Get-IdentityFixtureToken 'wrong_audience'
    $wrongAudience = Invoke-McpIdentityProbe $wrongAudienceToken $initializePayload
    if ($wrongAudience.Status -ne '401' -or $wrongAudience.Challenge -notmatch 'error="invalid_token"') {
        throw "Wrong-audience token was not rejected as invalid_token (HTTP $($wrongAudience.Status))."
    }
    if ($wrongAudience.Raw.Contains($wrongAudienceToken) -or $wrongAudience.Body -match 'eyJ[A-Za-z0-9_-]+\.') {
        throw 'Wrong-audience rejection leaked an access token.'
    }

    $missingScopeToken = Get-IdentityFixtureToken 'missing_read_scope'
    $missingScope = Invoke-McpIdentityProbe $missingScopeToken $initializePayload
    if ($missingScope.Status -ne '403' -or $missingScope.Challenge -notmatch 'error="insufficient_scope"') {
        throw "Missing-read-scope token was not rejected as insufficient_scope (HTTP $($missingScope.Status))."
    }
    if ($missingScope.Raw.Contains($missingScopeToken) -or $missingScope.Body -match 'eyJ[A-Za-z0-9_-]+\.') {
        throw 'Missing-scope rejection leaked an access token.'
    }

    $differentClusterToken = Get-IdentityFixtureToken 'different_cluster'
    $toolPayload = '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"rocketmq_get_cluster_overview","arguments":{"cluster":"sre-dev"}}}'
    $differentCluster = Invoke-McpIdentityProbe $differentClusterToken $toolPayload
    if ($differentCluster.Status -ne '200') {
        throw "Different-cluster token did not reach the MCP authorization guard (HTTP $($differentCluster.Status))."
    }
    $toolResponse = $differentCluster.Body | ConvertFrom-Json
    $toolError = $toolResponse.result.content[0].text | ConvertFrom-Json
    if (-not $toolResponse.result.isError -or $toolError.code -ne 'cluster_not_allowed' -or $toolError.retryable) {
        throw 'Different-cluster token was not rejected with stable cluster_not_allowed semantics.'
    }
    if ($differentCluster.Raw.Contains($differentClusterToken) -or $differentCluster.Body -match 'eyJ[A-Za-z0-9_-]+\.') {
        throw 'Different-cluster rejection leaked an access token.'
    }

    Write-Host 'MCP rejected wrong audience, missing read scope, and different cluster identity fixtures.'
}

function Wait-Http([string]$Uri, [int]$Seconds = 90) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        try {
            return Invoke-RestMethod -Uri $Uri -TimeoutSec 3
        }
        catch {
            Start-Sleep -Seconds 2
        }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for $Uri"
}

function Wait-ConnectorNotReady([int]$Seconds = 45) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    $lastStatus = 'not_observed'
    do {
        $response = Invoke-Docker (Compose-Arguments @(
            'exec', '-T', 'sre-connector',
            'curl', '--silent', '--show-error',
            '--write-out', "`n%{http_code}",
            'http://127.0.0.1:8091/readyz'
        )) -Capture
        $lines = @($response -split "\r?\n")
        $lastStatus = $lines[-1]
        $body = ($lines[0..($lines.Count - 2)] -join "`n") | ConvertFrom-Json
        if ($lastStatus -eq '503' -and $body.status -eq 'not_ready') {
            return
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Connector readiness was not revoked after offboard (last HTTP $lastStatus)."
}

function Invoke-ConversationEvidence(
    [string]$Question,
    [string]$Resource,
    [int]$WindowSeconds = 600
) {
    $headers = Get-PublicApiHeaders
    $conversationBody = @{
        cluster_id = $clusterId
        question = $Question
        resource = $Resource
        persist_investigation = $false
    } | ConvertTo-Json
    $conversation = Invoke-RestMethod `
        -Method Post `
        -Uri 'http://127.0.0.1:8090/v1/conversations' `
        -Headers $headers `
        -ContentType 'application/json' `
        -Body $conversationBody `
        -TimeoutSec 30
    $conversationId = $conversation.conversation.id
    if ([string]::IsNullOrWhiteSpace($conversationId)) {
        throw 'Control Plane did not create a conversation for the bounded read query.'
    }

    $turnBody = @{
        question = $Question
        resource = $Resource
        window_seconds = $WindowSeconds
    } | ConvertTo-Json
    $turn = Invoke-RestMethod `
        -Method Post `
        -Uri "http://127.0.0.1:8090/v1/conversations/$conversationId/turns" `
        -Headers $headers `
        -ContentType 'application/json' `
        -Body $turnBody `
        -TimeoutSec 45
    $evidenceIds = @($turn.answer.evidence_ids)
    if ($turn.turn.status -ne 'answered' -or $evidenceIds.Count -ne 1) {
        throw (
            'Control Plane conversation did not return exactly one answered Evidence reference ' +
            "(status=$($turn.turn.status), evidence_count=$($evidenceIds.Count))."
        )
    }
    $evidence = Invoke-RestMethod `
        -Uri "http://127.0.0.1:8090/v1/evidence/$($evidenceIds[0])" `
        -Headers $headers `
        -TimeoutSec 30
    if ($evidence.content_hash -notmatch '^sha256:[0-9a-f]{64}$') {
        throw 'Conversation Evidence did not contain a canonical SHA-256 hash.'
    }
    if ($evidence.schema.family -ne 'rocketmq-sre.evidence') {
        throw 'Conversation Evidence schema family is not rocketmq-sre.evidence.'
    }
    return $evidence
}

function Invoke-EvidenceQuery {
    Invoke-ConversationEvidence `
        -Question "Show Consumer Lag for $group on $topic." `
        -Resource "consumer-lag/$group/$topic" `
        -WindowSeconds 300
}

function Invoke-RequiredSignalsEvidence([string]$CanonicalMetric, [int]$WindowMinutes) {
    Invoke-ConversationEvidence `
        -Question "Show the bounded range for $CanonicalMetric." `
        -Resource "metrics/range/$CanonicalMetric" `
        -WindowSeconds ($WindowMinutes * 60)
}

function Assert-RequiredSignalsQualification {
    $qualification = Get-Content -Raw -LiteralPath $requiredSignalsQualificationPath | ConvertFrom-Json
    $components = @($qualification.components)
    if (
        $qualification.production_certified `
            -or $qualification.operating_mode -ne 'read_only' `
            -or $components.Count -ne [int]$qualification.limits.maximum_components
    ) {
        throw 'Required Signals qualification contract is not bounded and read-only.'
    }

    foreach ($component in $components) {
        $deadline = [DateTime]::UtcNow.AddSeconds([int]$qualification.limits.retry_seconds)
        $lastFailure = 'no Evidence response'
        do {
            try {
                $evidence = Invoke-RequiredSignalsEvidence `
                    -CanonicalMetric $component.canonical_metric `
                    -WindowMinutes ([int]$qualification.limits.query_window_minutes)
                if ($evidence.content.storage -ne 'inline') {
                    throw 'Required Signals Evidence is not bounded inline content.'
                }
                $value = $evidence.content.value
                if (
                    $evidence.source -ne 'prometheus' `
                        -or $value.schema_version -ne 'rocketmq.prometheus-evidence.v1' `
                        -or $value.metric -ne $component.canonical_metric
                ) {
                    throw 'Required Signals metric Evidence does not match the qualification contract.'
                }
                $sampleCount = 0
                foreach ($series in @($value.series)) {
                    $sampleCount += @($series.samples).Count
                }
                if ($sampleCount -lt 1) {
                    throw "Representative metric '$($component.representative_requirement_id)' has no samples."
                }
                Write-Host (
                    "Required Signals qualified: component=$($component.query_component) " +
                    "requirement=$($component.representative_requirement_id) evidence=$($evidence.content_hash)"
                )
                $lastFailure = $null
                break
            }
            catch {
                $lastFailure = $_.Exception.Message
            }
            Start-Sleep -Seconds 2
        } while ([DateTime]::UtcNow -lt $deadline)
        if ($null -ne $lastFailure) {
            throw "Required Signals qualification failed for '$($component.query_component)': $lastFailure"
        }
    }
}

function Get-InlineLag([object]$Evidence) {
    if ($Evidence.content.storage -ne 'inline') {
        throw 'Consumer Lag Evidence was not returned as bounded inline content.'
    }
    if ($null -eq $Evidence.content.value.total_lag) {
        throw 'Consumer Lag Evidence did not contain total_lag.'
    }
    return [long]$Evidence.content.value.total_lag
}

function Wait-PositiveLag([int]$Seconds = 60) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    $lastLag = $null
    $lastFailure = 'no successful Evidence query'
    do {
        try {
            $evidence = Invoke-EvidenceQuery
            $lastLag = Get-InlineLag $evidence
            $lastFailure = "last total_lag=$lastLag"
            if ($lastLag -gt 0) {
                return [PSCustomObject]@{
                    Evidence = $evidence
                    TotalLag = $lastLag
                }
            }
        }
        catch {
            # Registration and route propagation can briefly make Lag unavailable.
            $lastFailure = $_.Exception.Message
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for positive Consumer Lag: $lastFailure."
}

function Wait-LagBelow([long]$UpperBound, [int]$Seconds = 60) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    $lastLag = $null
    $lastFailure = 'no successful Evidence query'
    do {
        try {
            $evidence = Invoke-EvidenceQuery
            $lastLag = Get-InlineLag $evidence
            $lastFailure = "last total_lag=$lastLag"
            if ($lastLag -lt $UpperBound) {
                return [PSCustomObject]@{
                    Evidence = $evidence
                    TotalLag = $lastLag
                }
            }
        }
        catch {
            # Consumer offset propagation is eventually consistent.
            $lastFailure = $_.Exception.Message
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for Consumer Lag below ${UpperBound}: $lastFailure."
}

function Drain-ProbeLag([int]$MaxBatches = 12) {
    $evidence = Invoke-EvidenceQuery
    $lag = Get-InlineLag $evidence
    for ($batch = 1; $batch -le $MaxBatches -and $lag -gt 0; $batch++) {
        Invoke-Docker (Compose-Arguments @(
            '--profile', 'smoke', 'run', '--rm',
            '-e', 'ROCKETMQ_SRE_PROBE_MAX_MESSAGES=1',
            'sre-probe', 'consume'
        )) | Out-Host
        $snapshot = Wait-LagBelow -UpperBound $lag -Seconds 30
        $evidence = $snapshot.Evidence
        $lag = $snapshot.TotalLag
    }
    if ($lag -gt 0) {
        throw "Consumer Lag remained at $lag after $MaxBatches bounded drain batches."
    }
    return [PSCustomObject]@{
        Evidence = $evidence
        TotalLag = $lag
    }
}

function Get-ClusterState {
    Invoke-RestMethod `
        -Uri "http://127.0.0.1:8090/v1/clusters/$clusterId" `
        -Headers (Get-PublicApiHeaders) `
        -TimeoutSec 15
}

function Get-ClusterCapability {
    Invoke-RestMethod `
        -Uri "http://127.0.0.1:8090/v1/clusters/$clusterId/capabilities" `
        -Headers (Get-PublicApiHeaders) `
        -TimeoutSec 15
}

function Wait-ClusterReady([int]$Seconds = 90) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        try {
            $cluster = Get-ClusterState
            if ($cluster.state -eq 'ready_read_only') {
                return $cluster
            }
        }
        catch {
            # The Connector periodically reconciles while dependencies start.
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'Timed out waiting for the persisted cluster to reach ready_read_only.'
}

function Wait-ConnectorChannelOnline([int]$Seconds = 90) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    $headers = Get-PublicApiHeaders
    do {
        try {
            $channel = Invoke-RestMethod `
                -Uri "http://127.0.0.1:8090/v1/clusters/$clusterId/connector" `
                -Headers $headers `
                -TimeoutSec 15
            if ($channel.status.liveness -eq 'online') {
                return $channel
            }
        }
        catch {
            # The HTTP/2 mTLS registration races initial MCP handshaking.
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'Timed out waiting for the authenticated Connector mTLS channel to become online.'
}

function Assert-QueryableDataSources {
    $capability = Get-ClusterCapability
    foreach ($sourceId in @('rocketmq_mcp', 'mcp_runtime', 'mcp_observability', 'prometheus', 'loki', 'tempo')) {
        $source = @($capability.data_sources | Where-Object { $_.id -eq $sourceId })
        if ($source.Count -ne 1 -or $source[0].availability -ne 'queryable') {
            throw "Data source '$sourceId' was not verified as queryable by the Connector."
        }
    }
    if (($capability.data_sources | Where-Object { $_.id -eq 'mcp_runtime' }).detail -notmatch 'rocketmq\.runtime-diagnostics\.v1') {
        throw 'Connector did not validate the versioned MCP Runtime System Resource.'
    }
    if (($capability.data_sources | Where-Object { $_.id -eq 'mcp_observability' }).detail -notmatch 'rocketmq\.observability-status\.v1') {
        throw 'Connector did not validate the versioned MCP Observability System Resource.'
    }
}

function Test-PrometheusServiceTelemetry([string]$ServiceName) {
    # The Collector's Prometheus exporter maps OTel service namespace/name to
    # `exported_job=rocketmq/<service>` rather than retaining `service_name`.
    $query = [Uri]::EscapeDataString("count({exported_job=`"rocketmq/$ServiceName`"})")
    $response = Invoke-RestMethod `
        -Uri "http://127.0.0.1:9090/api/v1/query?query=$query" `
        -TimeoutSec 10
    $result = @($response.data.result)
    return $response.status -eq 'success' `
        -and $result.Count -eq 1 `
        -and [double]$result[0].value[1] -gt 0
}

function Test-LokiServiceTelemetry([string]$ServiceName) {
    # Keep both endpoints as Int64. Without the suffix PowerShell promotes the
    # subtraction to a floating-point value and emits scientific notation,
    # which Loki correctly rejects for its nanosecond timestamp parameters.
    [long]$endNanos = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000000L
    [long]$startNanos = $endNanos - (60L * 60L * 1000000000L)
    $selector = [Uri]::EscapeDataString("{service_name=`"$ServiceName`"}")
    $response = Invoke-RestMethod `
        -Uri "http://127.0.0.1:3100/loki/api/v1/query_range?query=$selector&start=$startNanos&end=$endNanos&limit=20" `
        -TimeoutSec 10
    $streams = @($response.data.result)
    return $response.status -eq 'success' `
        -and $streams.Count -gt 0 `
        -and @($streams[0].values).Count -gt 0
}

function Test-BrokerSendTrace {
    $traceQl = [Uri]::EscapeDataString(
        '{ resource.service.name = "rocketmq-broker" && name = "RocketMQ BROKER RECEIVE_SEND" }'
    )
    $response = Invoke-RestMethod `
        -Uri "http://127.0.0.1:3200/api/search?q=$traceQl&limit=1" `
        -TimeoutSec 10
    return @($response.traces).Count -gt 0
}

function Test-McpTrace([string]$SpanName, [long]$StartUnixSeconds = 0) {
    $traceQl = [Uri]::EscapeDataString(
        "{ resource.service.name = `"rocketmq-mcp`" && name = `"$SpanName`" }"
    )
    $timeRange = ''
    if ($StartUnixSeconds -gt 0) {
        $endUnixSeconds = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds() + 5
        $timeRange = "&start=$StartUnixSeconds&end=$endUnixSeconds"
    }
    $response = Invoke-RestMethod `
        -Uri "http://127.0.0.1:3200/api/search?q=$traceQl&limit=1$timeRange" `
        -TimeoutSec 10
    return @($response.traces).Count -gt 0
}

function Wait-McpTraceSince(
    [string]$SpanName,
    [long]$StartUnixSeconds,
    [int]$Seconds = 90
) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        try {
            if (Test-McpTrace $SpanName $StartUnixSeconds) {
                return
            }
        }
        catch {
            # Tempo search can trail a newly recovered OTLP exporter.
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "MCP span '$SpanName' created after Collector recovery did not reach Tempo."
}

function Get-McpToolRequestCount {
    $query = [Uri]::EscapeDataString(
        'sum(rocketmq_mcp_requests_total{operation_kind="tool",operation="rocketmq_get_consumer_lag"})'
    )
    $response = Invoke-RestMethod `
        -Uri "http://127.0.0.1:9090/api/v1/query?query=$query" `
        -TimeoutSec 10
    $result = @($response.data.result)
    if ($response.status -ne 'success' -or $result.Count -eq 0) {
        return 0.0
    }
    return [double]$result[0].value[1]
}

function Wait-McpToolRequestCountAbove([double]$Baseline, [int]$Seconds = 90) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        try {
            if ((Get-McpToolRequestCount) -gt $Baseline) {
                return
            }
        }
        catch {
            # The Collector and Prometheus scrape pipeline recover independently.
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "MCP Tool metric did not advance beyond the pre-outage baseline $Baseline."
}

function Assert-ObservabilityQueries([int]$Seconds = 120) {
    $services = @(
        'rocketmq-broker',
        'rocketmq-namesrv',
        'rocketmq-controller',
        'rocketmq-proxy'
    )
    $metricsPending = @{}
    $logsPending = @{}
    foreach ($service in $services) {
        $metricsPending[$service] = $true
        $logsPending[$service] = $true
    }
    $brokerSendTracePending = $true
    $mcpToolTracePending = $true
    $mcpResourceTracePending = $true
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)

    do {
        foreach ($service in @($metricsPending.Keys)) {
            try {
                if (Test-PrometheusServiceTelemetry $service) {
                    $metricsPending.Remove($service)
                }
            }
            catch {
                # Export and scrape are eventually consistent during stack startup.
            }
        }
        foreach ($service in @($logsPending.Keys)) {
            try {
                if (Test-LokiServiceTelemetry $service) {
                    $logsPending.Remove($service)
                }
            }
            catch {
                # Loki indexing can trail the OTLP logs pipeline briefly.
            }
        }
        if ($brokerSendTracePending) {
            try {
                $brokerSendTracePending = -not (Test-BrokerSendTrace)
            }
            catch {
                # Tempo search can trail ingestion and block compaction briefly.
            }
        }
        if ($mcpToolTracePending) {
            try {
                $mcpToolTracePending = -not (Test-McpTrace 'RocketMQ MCP TOOL')
            }
            catch {
                # Tempo search can trail MCP Tool span ingestion briefly.
            }
        }
        if ($mcpResourceTracePending) {
            try {
                $mcpResourceTracePending = -not (Test-McpTrace 'RocketMQ MCP RESOURCE')
            }
            catch {
                # Tempo search can trail MCP Resource span ingestion briefly.
            }
        }
        if (
            $metricsPending.Count -eq 0 `
                -and $logsPending.Count -eq 0 `
                -and -not $brokerSendTracePending `
                -and -not $mcpToolTracePending `
                -and -not $mcpResourceTracePending
        ) {
            Write-Host 'Prometheus and Loki contain all four core services; Tempo contains Broker send and MCP Tool/Resource spans.'
            return
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)

    $missingMetrics = (@($metricsPending.Keys) -join ',')
    $missingLogs = (@($logsPending.Keys) -join ',')
    throw "Telemetry did not become non-empty: metrics=[$missingMetrics], logs=[$missingLogs], broker_send_trace_missing=$brokerSendTracePending, mcp_tool_trace_missing=$mcpToolTracePending, mcp_resource_trace_missing=$mcpResourceTracePending."
}

function Wait-CollectorPrometheusUp([int]$Seconds = 90) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        try {
            $response = Invoke-RestMethod `
                -Uri 'http://127.0.0.1:9090/api/v1/query?query=up%7Bjob%3D%22otel-collector%22%7D' `
                -TimeoutSec 5
            $result = @($response.data.result)
            if ($response.status -eq 'success' -and $result.Count -eq 1 -and $result[0].value[1] -eq '1') {
                return
            }
        }
        catch {
            # Prometheus may still be observing the Collector restart.
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'Collector did not return to the Prometheus up state.'
}

if ($Target -eq 'Kind') {
    Require-Command kubectl
    & (Join-Path $scriptDirectory 'kind.ps1') -Action Smoke
    if ($LASTEXITCODE -ne 0) {
        throw 'Kind parity smoke failed.'
    }
    exit 0
}

Require-Command docker
Wait-Http 'http://127.0.0.1:8090/readyz' | Out-Null
Wait-Http 'http://127.0.0.1:8091/readyz' | Out-Null
Wait-ClusterReady | Out-Null
Wait-ConnectorChannelOnline | Out-Null
Write-Host 'Connector HTTP/2 mTLS channel registered with certificate-derived identity.'
Assert-QueryableDataSources

Invoke-Docker (Compose-Arguments @('--profile', 'smoke', 'run', '--rm', 'probe-topic-bootstrap'))
Invoke-Docker (Compose-Arguments @('--profile', 'smoke', 'run', '--rm', 'sre-probe', 'register'))
Invoke-Docker (Compose-Arguments @('--profile', 'smoke', 'run', '--rm', 'sre-probe', 'send'))

$positiveSnapshot = Wait-PositiveLag
$lagEvidence = $positiveSnapshot.Evidence
$positiveLag = $positiveSnapshot.TotalLag
Write-Host "Positive-lag Evidence: $($lagEvidence.content_hash)"

Wait-Http 'http://127.0.0.1:9090/-/ready' | Out-Null
Wait-Http 'http://127.0.0.1:3100/ready' | Out-Null
Wait-Http 'http://127.0.0.1:3200/ready' | Out-Null
Assert-ObservabilityQueries
Assert-RequiredSignalsQualification
$capabilities = Get-ClusterCapability
$resources = @($capabilities.manifest.resources)
if ($resources -notcontains 'rocketmq://system/runtime/v1') {
    throw 'MCP runtime resource is missing from the verified capability surface.'
}
if ($resources -notcontains 'rocketmq://system/observability/v1') {
    throw 'MCP observability resource is missing from the verified capability surface.'
}

Invoke-Docker (Compose-Arguments @('--profile', 'smoke', 'run', '--rm', 'sre-probe', 'consume'))
$recoveredSnapshot = Wait-LagBelow -UpperBound $positiveLag
$recoveredEvidence = $recoveredSnapshot.Evidence
$recoveredLag = $recoveredSnapshot.TotalLag
Write-Host "Recovered-lag Evidence: $($recoveredEvidence.content_hash)"

Invoke-Docker (Compose-Arguments @('restart', 'postgres'))
Invoke-Docker (Compose-Arguments @('up', '--detach', '--wait', 'postgres'))
Invoke-Docker (Compose-Arguments @('restart', 'sre-control-plane'))
Wait-Http 'http://127.0.0.1:8090/readyz' | Out-Null
Invoke-Docker (Compose-Arguments @('restart', 'sre-control-plane-mtls'))
Invoke-Docker (Compose-Arguments @('up', '--detach', '--wait', 'sre-control-plane-mtls'))
Wait-Http 'http://127.0.0.1:8091/readyz' | Out-Null
$persisted = Wait-ClusterReady
Wait-ConnectorChannelOnline | Out-Null
if ($persisted.id -ne $clusterId) {
    throw 'PostgreSQL/Control Plane restart did not preserve the onboarded cluster.'
}
Assert-QueryableDataSources
Write-Host 'PostgreSQL and Control Plane restart preserved onboarding and capability state.'

$mcpToolRequestsBeforeOutage = Get-McpToolRequestCount
Invoke-Docker (Compose-Arguments @('--profile', 'observability', 'stop', 'otel-collector'))
try {
    Invoke-Docker (Compose-Arguments @('--profile', 'smoke', 'run', '--rm', 'sre-probe', 'send'))
    Invoke-EvidenceQuery | Out-Null
}
finally {
    Invoke-Docker (Compose-Arguments @('--profile', 'observability', 'start', 'otel-collector'))
}
Wait-CollectorPrometheusUp
$recoveryTraceStart = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
Invoke-EvidenceQuery | Out-Null
Wait-McpToolRequestCountAbove $mcpToolRequestsBeforeOutage
Wait-McpTraceSince 'RocketMQ MCP TOOL' $recoveryTraceStart
Write-Host 'Collector recovery exported a new MCP Tool metric and trace.'
Assert-ObservabilityQueries
Wait-Http 'http://127.0.0.1:8091/readyz' | Out-Null
$finalLagSnapshot = Drain-ProbeLag
Write-Host "Post-outage Consumer Lag recovered to $($finalLagSnapshot.TotalLag)."

Assert-McpIdentityFailClosed

$oldJwksDocument = Invoke-Docker (Compose-Arguments @(
    'exec', '-T', 'sre-connector',
    'curl', '--fail', '--silent', '--show-error',
    '--cacert', '/etc/rocketmq/tls/ca-cert.pem',
    'https://dev-issuer-tls:8443/.well-known/jwks.json'
)) -Capture
$oldJwk = @(($oldJwksDocument | ConvertFrom-Json).keys)[0]
if ([string]::IsNullOrWhiteSpace($oldJwk.kid) -or [string]::IsNullOrWhiteSpace($oldJwk.n)) {
    throw 'Development issuer did not expose a complete initial JWKS key.'
}

$tokenDocument = Invoke-Docker (Compose-Arguments @(
    'exec', '-T', 'sre-connector',
    'curl', '--fail', '--silent', '--show-error',
    '--cacert', '/etc/rocketmq/tls/ca-cert.pem',
    '--user', 'rocketmq-sre-connector:phase00-client-secret',
    '--data-urlencode', 'grant_type=client_credentials',
    '--data-urlencode', 'scope=rocketmq:read rocketmq:diagnose',
    '--data-urlencode', 'audience=rocketmq-mcp',
    'https://dev-issuer-tls:8443/oauth/token'
)) -Capture
$oldMcpToken = ($tokenDocument | ConvertFrom-Json).access_token
if ([string]::IsNullOrWhiteSpace($oldMcpToken)) {
    throw 'Development issuer did not return an access token.'
}
$rotationDocument = Invoke-Docker (Compose-Arguments @(
    'exec', '-T', 'sre-connector',
    'curl', '--fail', '--silent', '--show-error',
    '--cacert', '/etc/rocketmq/tls/ca-cert.pem',
    '--request', 'POST',
    '--header', 'Authorization: Bearer phase00-issuer-admin',
    'https://dev-issuer-tls:8443/admin/rotate'
)) -Capture
$rotatedKid = ($rotationDocument | ConvertFrom-Json).kid
$rotatedJwksDocument = Invoke-Docker (Compose-Arguments @(
    'exec', '-T', 'sre-connector',
    'curl', '--fail', '--silent', '--show-error',
    '--cacert', '/etc/rocketmq/tls/ca-cert.pem',
    'https://dev-issuer-tls:8443/.well-known/jwks.json'
)) -Capture
$rotatedJwk = @(($rotatedJwksDocument | ConvertFrom-Json).keys)[0]
if (
    [string]::IsNullOrWhiteSpace($rotatedJwk.kid) `
        -or $rotatedJwk.kid -ne $rotatedKid `
        -or $rotatedJwk.kid -eq $oldJwk.kid `
        -or $rotatedJwk.n -eq $oldJwk.n
) {
    throw 'JWKS rotation did not replace both the key identifier and RSA public key.'
}
Start-Sleep -Seconds 2
$status = Invoke-Docker (Compose-Arguments @(
    'exec', '-T', 'sre-connector',
    'curl', '--silent', '--output', '/dev/null', '--write-out', '%{http_code}',
    '--cacert', '/etc/rocketmq/tls/ca-cert.pem',
    '--header', "Authorization: Bearer $oldMcpToken",
    '--header', 'Content-Type: application/json',
    '--data', '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"phase00-smoke","version":"1.0.0"}}}',
    'https://127.0.0.1:8089/mcp'
)) -Capture
if ($status -ne '401') {
    throw "Old OAuth token was not rejected after JWKS rotation (HTTP $status)."
}
Invoke-EvidenceQuery | Out-Null
Write-Host 'JWKS rotation rejected the old token and Connector recovered once.'

$offboardBody = @{
    actor_subject = 'phase00-smoke'
    correlation_id = [Guid]::NewGuid().ToString()
    reason = 'Phase 00 offboard verification'
} | ConvertTo-Json
$offboarded = Invoke-RestMethod `
    -Method Post `
    -Uri "http://127.0.0.1:8090/v1/clusters/$clusterId/offboard" `
    -Headers (Get-PublicApiHeaders) `
    -ContentType 'application/json' `
    -Body $offboardBody `
    -TimeoutSec 15
if ($offboarded.state -ne 'offboarded') {
    throw 'Cluster did not reach offboarded state.'
}
if ($null -eq $offboarded.offboarded_at) {
    throw 'Cluster offboarding did not persist a tombstone timestamp.'
}

$offboardEnforced = $false
$deadline = [DateTime]::UtcNow.AddSeconds(45)
do {
    try {
        Invoke-EvidenceQuery | Out-Null
        Start-Sleep -Seconds 2
    }
    catch {
        $statusCode = if ($null -ne $_.Exception.Response) {
            [int]$_.Exception.Response.StatusCode
        }
        else {
            0
        }
        $errorCode = $null
        if (-not [string]::IsNullOrWhiteSpace($_.ErrorDetails.Message)) {
            try {
                $errorCode = ($_.ErrorDetails.Message | ConvertFrom-Json).code
            }
            catch {
                # The exact stable error below remains mandatory.
            }
        }
        if ($statusCode -eq 403 -and $errorCode -eq 'cluster_not_allowed') {
            Write-Host 'Offboard stopped new Evidence collection with cluster_not_allowed.'
            $offboardEnforced = $true
            break
        }
        throw "Evidence failed after offboard for an unexpected reason (HTTP $statusCode, code=$errorCode)."
    }
} while ([DateTime]::UtcNow -lt $deadline)
if (-not $offboardEnforced) {
    throw 'Connector continued collecting Evidence after offboard.'
}

$persistedOffboarded = Get-ClusterState
if ($persistedOffboarded.state -ne 'offboarded' -or $null -eq $persistedOffboarded.offboarded_at) {
    throw 'Control Plane did not retain the offboarded tombstone.'
}
Wait-Http 'http://127.0.0.1:8090/readyz' | Out-Null

Wait-ConnectorNotReady

$identityCounts = Invoke-Docker (Compose-Arguments @(
    'exec', '-T', 'postgres',
    'psql',
    '--username', 'rocketmq_sre',
    '--dbname', 'rocketmq_sre',
    '--tuples-only',
    '--no-align',
    '--command',
    "SELECT COUNT(*) FILTER (WHERE revoked_at IS NOT NULL), COUNT(*) FILTER (WHERE revoked_at IS NULL) FROM connector_identities WHERE cluster_id = '$clusterId';"
)) -Capture
$identityCountParts = @($identityCounts.Trim() -split '\|')
if (
    $identityCountParts.Count -ne 2 `
        -or [int]$identityCountParts[0] -lt 1 `
        -or [int]$identityCountParts[1] -ne 0
) {
    throw "Connector identity revocation was not persisted (counts=$identityCounts)."
}

Write-Host 'Offboard preserved Control Plane readiness, revoked Connector readiness, and persisted identity revocation.'
Write-Host 'PHASE00_COMPOSE_SMOKE_OK'

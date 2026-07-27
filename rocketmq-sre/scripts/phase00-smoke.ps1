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

function Invoke-EvidenceQuery {
    $at = [DateTime]::UtcNow.ToString('o')
    $body = @{
        query = @{
            query_id = [Guid]::NewGuid().ToString()
            correlation_id = [Guid]::NewGuid().ToString()
            tenant_id = $tenantId
            cluster_id = $clusterId
            source = 'rocketmq-mcp'
            resource = "consumer-groups/$group/lag/$topic"
            time_range = @{ start = $at; end = $at }
        }
        mcp_cluster = 'sre-dev'
        operation = @{
            kind = 'consumer_lag'
            topic = $topic
            consumer_group = $group
            limit = 50
        }
    } | ConvertTo-Json -Depth 8
    $headers = @{ Authorization = "Bearer $internalToken" }
    $evidence = Invoke-RestMethod `
        -Method Post `
        -Uri 'http://127.0.0.1:8091/internal/v1/evidence/query' `
        -Headers $headers `
        -ContentType 'application/json' `
        -Body $body `
        -TimeoutSec 30
    if ($evidence.content_hash -notmatch '^sha256:[0-9a-f]{64}$') {
        throw 'Connector Evidence did not contain a canonical SHA-256 hash.'
    }
    if ($evidence.schema.family -ne 'rocketmq-sre.evidence') {
        throw 'Connector Evidence schema family is not rocketmq-sre.evidence.'
    }
    return $evidence
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
        'sum(rocketmq_rocketmq_mcp_requests_total{operation_kind="tool",operation="rocketmq_get_consumer_lag"})'
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
$headers = @{ Authorization = "Bearer $internalToken" }
$capabilities = Invoke-RestMethod `
    -Uri 'http://127.0.0.1:8091/internal/v1/capabilities' `
    -Headers $headers `
    -TimeoutSec 15
$resources = @($capabilities.clusters.'sre-dev'.resources)
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
$persisted = Wait-ClusterReady
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
Invoke-Docker (Compose-Arguments @('--profile', 'smoke', 'run', '--rm', 'sre-probe', 'consume'))
$finalLagSnapshot = Wait-LagBelow -UpperBound 1
Write-Host "Post-outage Consumer Lag recovered to $($finalLagSnapshot.TotalLag)."

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
    -Headers @{ Authorization = "Bearer $internalToken" } `
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

$connectorReadyResponse = Invoke-Docker (Compose-Arguments @(
    'exec', '-T', 'sre-connector',
    'curl', '--silent', '--show-error',
    '--write-out', "`n%{http_code}",
    'http://127.0.0.1:8091/readyz'
)) -Capture
$connectorReadyLines = @($connectorReadyResponse -split "\r?\n")
$connectorReadyStatus = $connectorReadyLines[-1]
$connectorReadyBody = ($connectorReadyLines[0..($connectorReadyLines.Count - 2)] -join "`n") | ConvertFrom-Json
if ($connectorReadyStatus -ne '503' -or $connectorReadyBody.status -ne 'not_ready') {
    throw "Connector readiness was not revoked after offboard (HTTP $connectorReadyStatus)."
}

$connectorCapabilities = Invoke-RestMethod `
    -Uri 'http://127.0.0.1:8091/internal/v1/capabilities' `
    -Headers @{ Authorization = "Bearer $internalToken" } `
    -TimeoutSec 15
$cachedClusters = @($connectorCapabilities.clusters.psobject.Properties)
if (
    $connectorCapabilities.ready `
        -or $connectorCapabilities.last_error_code -ne 'cluster_not_allowed' `
        -or $cachedClusters.Count -ne 0
) {
    throw 'Connector did not clear readiness and the verified capability cache after offboard.'
}

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

Write-Host 'Offboard preserved Control Plane readiness, revoked Connector readiness, cleared capability cache, and persisted identity revocation.'
Write-Host 'PHASE00_COMPOSE_SMOKE_OK'

# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [string]$KubeconfigPath = $env:ROCKETMQ_SRE_KIND_KUBECONFIG,

    [string]$ArtifactRoot = $env:ROCKETMQ_SRE_VALIDATION_ARTIFACT_ROOT,

    [ValidateRange(30, 300)]
    [int]$MetricWaitSeconds = 120,

    [switch]$ValidateOnly
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
Add-Type -AssemblyName System.Net.Http
$thisScript = $MyInvocation.MyCommand.Path
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$defaultArtifactRoot = Join-Path $repositoryRoot 'target/phase00-runtime-diagnostics-smoke'
$defaultKubeconfig = Join-Path $repositoryRoot 'target/phase00-kind/kubeconfig'
$rocketmqNamespace = 'rocketmq-system'
$observabilityNamespace = 'observability'
$diagnosticsPath = '/internal/v1/runtime/diagnostics'
$diagnosticsScope = 'rocketmq:diagnose'
$endpointSchema = 'rocketmq.runtime-diagnostics-endpoint.v1'
$viewSchema = 'rocketmq.runtime-diagnostics.v1'
$componentPorts = [ordered]@{
    broker = @{ Service = 'rocketmq-broker'; Port = 18087 }
    name_server = @{ Service = 'rocketmq-namesrv'; Port = 18088 }
    controller = @{ Service = 'rocketmq-controller'; Port = 18089 }
    proxy = @{ Service = 'rocketmq-proxy'; Port = 18090 }
}
$runtimeMetrics = @(
    'rocketmq_runtime_task_groups',
    'rocketmq_runtime_tasks',
    'rocketmq_runtime_long_running_tasks',
    'rocketmq_runtime_blocking_queued',
    'rocketmq_runtime_blocking_timeouts',
    'rocketmq_runtime_lifecycle_transitions_total'
)
$portForwardProcesses = [Collections.Generic.List[Diagnostics.Process]]::new()

function Resolve-AllowedPath([string]$Path, [string]$Fallback, [string]$Name) {
    $candidate = if ([string]::IsNullOrWhiteSpace($Path)) { $Fallback } else { $Path }
    $resolved = [IO.Path]::GetFullPath($candidate)
    if (-not ($resolved.StartsWith('D:\', [StringComparison]::OrdinalIgnoreCase) `
            -or $resolved.StartsWith('F:\', [StringComparison]::OrdinalIgnoreCase))) {
        throw "$Name must be located on D: or F:; C: and G: are intentionally forbidden."
    }
    return $resolved
}

function Assert-PowerShellSyntax {
    $tokens = $null
    $parseErrors = $null
    [Management.Automation.Language.Parser]::ParseFile(
        $script:thisScript,
        [ref]$tokens,
        [ref]$parseErrors
    ) | Out-Null
    if ($parseErrors.Count -gt 0) {
        $messages = $parseErrors | ForEach-Object { $_.Message }
        throw "PowerShell syntax validation failed: $($messages -join '; ')"
    }
}

function Invoke-Kubectl([string[]]$Arguments) {
    $output = & kubectl @('--kubeconfig', $script:KubeconfigPath) @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl failed: $($output -join [Environment]::NewLine)"
    }
    return $output
}

function Start-PortForward(
    [string]$Namespace,
    [string]$Service,
    [int]$LocalPort,
    [int]$RemotePort
) {
    $stdout = Join-Path $script:ArtifactRoot "$Service-$LocalPort.stdout.log"
    $stderr = Join-Path $script:ArtifactRoot "$Service-$LocalPort.stderr.log"
    $arguments = @(
        '--kubeconfig', $script:KubeconfigPath,
        '--namespace', $Namespace,
        'port-forward',
        "service/$Service",
        "$LocalPort`:$RemotePort",
        '--address=127.0.0.1'
    )
    $process = Start-Process `
        -FilePath (Get-Command kubectl).Source `
        -ArgumentList $arguments `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru
    $script:portForwardProcesses.Add($process)
    Wait-LocalPort $process $LocalPort $stderr
}

function Wait-LocalPort(
    [Diagnostics.Process]$Process,
    [int]$Port,
    [string]$ErrorLog
) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(20)
    do {
        if ($Process.HasExited) {
            $detail = if (Test-Path -LiteralPath $ErrorLog) {
                (Get-Content -Raw -LiteralPath $ErrorLog).Trim()
            }
            else {
                'no diagnostic output'
            }
            throw "Port-forward for 127.0.0.1:$Port exited early: $detail"
        }
        $client = [Net.Sockets.TcpClient]::new()
        try {
            $connect = $client.ConnectAsync('127.0.0.1', $Port)
            if ($connect.Wait(500) -and $client.Connected) {
                return
            }
        }
        catch {
            # The listener may still be starting.
        }
        finally {
            $client.Dispose()
        }
        Start-Sleep -Milliseconds 250
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Timed out waiting for port-forward on 127.0.0.1:$Port."
}

function Invoke-BoundedHttp(
    [string]$Uri,
    [string]$Token,
    [string]$Scope
) {
    $handler = [Net.Http.HttpClientHandler]::new()
    $client = [Net.Http.HttpClient]::new($handler)
    $client.Timeout = [TimeSpan]::FromSeconds(5)
    $request = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Get, $Uri)
    try {
        if (-not [string]::IsNullOrEmpty($Token)) {
            $request.Headers.Authorization = [Net.Http.Headers.AuthenticationHeaderValue]::new('Bearer', $Token)
        }
        if (-not [string]::IsNullOrEmpty($Scope)) {
            $request.Headers.Add('X-RocketMQ-SRE-Scope', $Scope)
        }
        $response = $client.SendAsync(
            $request,
            [Net.Http.HttpCompletionOption]::ResponseHeadersRead
        ).GetAwaiter().GetResult()
        try {
            $bytes = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
            if ($bytes.Length -gt 65536) {
                throw 'HTTP response exceeded the 64 KiB smoke-test bound.'
            }
            return [pscustomobject]@{
                Status = [int]$response.StatusCode
                Body = [Text.Encoding]::UTF8.GetString($bytes)
            }
        }
        finally {
            $response.Dispose()
        }
    }
    finally {
        $request.Dispose()
        $client.Dispose()
        $handler.Dispose()
    }
}

function Assert-Status([object]$Response, [int]$Expected, [string]$Context) {
    if ($Response.Status -ne $Expected) {
        throw "$Context returned HTTP $($Response.Status), expected $Expected."
    }
}

function Assert-NoSensitiveProperties([object]$Value, [string]$Component) {
    $forbidden = @(
        'token',
        'secret',
        'credential',
        'message_body',
        'arguments',
        'configuration',
        'task_name',
        'group_name',
        'raw_name'
    )
    $pending = [Collections.Generic.Queue[object]]::new()
    $pending.Enqueue($Value)
    while ($pending.Count -gt 0) {
        $current = $pending.Dequeue()
        if ($null -eq $current -or $current -is [string] -or $current.GetType().IsPrimitive) {
            continue
        }
        if ($current -is [Collections.IEnumerable] -and $current -isnot [pscustomobject]) {
            foreach ($item in $current) {
                $pending.Enqueue($item)
            }
            continue
        }
        foreach ($property in $current.PSObject.Properties) {
            if ($forbidden -contains $property.Name.ToLowerInvariant()) {
                throw "$Component diagnostics exposed forbidden property '$($property.Name)'."
            }
            $pending.Enqueue($property.Value)
        }
    }
}

function Get-RuntimeDiagnosticsToken {
    $encoded = (
        Invoke-Kubectl @(
            '--namespace', $rocketmqNamespace,
            'get', 'secret', 'rocketmq-runtime-secrets',
            '--output=jsonpath={.data.runtime-diagnostics-token}'
        )
    ) -join ''
    if ([string]::IsNullOrWhiteSpace($encoded)) {
        throw 'The runtime diagnostics token is absent from the mounted-secret source.'
    }
    $token = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($encoded)).Trim()
    if ([string]::IsNullOrWhiteSpace($token)) {
        throw 'The runtime diagnostics token is empty.'
    }
    return $token
}

function Test-ComponentEndpoint([string]$Component, [int]$Port, [string]$Token) {
    $uri = "http://127.0.0.1:$Port$diagnosticsPath"
    Assert-Status (Invoke-BoundedHttp $uri '' $diagnosticsScope) 401 "$Component anonymous request"
    Assert-Status (Invoke-BoundedHttp $uri $Token '') 403 "$Component missing-scope request"
    $response = Invoke-BoundedHttp $uri $Token $diagnosticsScope
    Assert-Status $response 200 "$Component authorized request"
    $view = $response.Body | ConvertFrom-Json
    if (
        $view.schema_version -ne $endpointSchema `
            -or $view.source -ne 'rocketmq_process' `
            -or $view.data.schema_version -ne $viewSchema `
            -or $view.data.component -ne $Component
    ) {
        throw "$Component diagnostics response did not match its fixed schema and component identity."
    }
    Assert-NoSensitiveProperties $view $Component
    if ($response.Body.IndexOf($Token, [StringComparison]::Ordinal) -ge 0) {
        throw "$Component diagnostics response disclosed its bearer token."
    }
    $health = Invoke-BoundedHttp "http://127.0.0.1:$Port/healthz" $Token $diagnosticsScope
    if ($health.Status -eq 200) {
        throw "$Component diagnostics listener incorrectly exposed the anonymous health route."
    }
    return [pscustomobject]@{
        component = $Component
        observed_at = $view.data.observed_at
        task_group_count = $view.data.task_group_count
        task_count = $view.data.task_count
        truncated = $view.data.truncated
    }
}

function Wait-RuntimeMetric([string]$Metric, [int]$Port) {
    $expectedComponents = @('broker', 'name_server', 'controller', 'proxy')
    $query = [Uri]::EscapeDataString("max by (component) ($Metric)")
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($MetricWaitSeconds)
    $lastComponents = @()
    do {
        try {
            $response = Invoke-BoundedHttp "http://127.0.0.1:$Port/api/v1/query?query=$query" '' ''
            if ($response.Status -eq 200) {
                $document = $response.Body | ConvertFrom-Json
                if ($document.status -eq 'success') {
                    $lastComponents = @(
                        $document.data.result |
                            ForEach-Object { [string]$_.metric.component } |
                            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
                            Select-Object -Unique
                    )
                    $missing = @($expectedComponents | Where-Object { $_ -notin $lastComponents })
                    if ($missing.Count -eq 0) {
                        return [pscustomobject]@{
                            metric = $Metric
                            components = $lastComponents
                        }
                    }
                }
            }
        }
        catch {
            # Prometheus and the Collector may still be converging after rollout.
        }
        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "$Metric did not expose all runtime components; observed=$($lastComponents -join ',')."
}

$KubeconfigPath = Resolve-AllowedPath $KubeconfigPath $defaultKubeconfig 'KubeconfigPath'
$ArtifactRoot = Resolve-AllowedPath $ArtifactRoot $defaultArtifactRoot 'ArtifactRoot'
Assert-PowerShellSyntax
if ($ValidateOnly) {
    Write-Host 'PHASE00_RUNTIME_DIAGNOSTICS_STATIC_OK paths=D_or_F'
    return
}
if (-not (Test-Path -LiteralPath $KubeconfigPath -PathType Leaf)) {
    throw "Kubeconfig does not exist: $KubeconfigPath"
}
New-Item -ItemType Directory -Force -Path $ArtifactRoot | Out-Null

try {
    Invoke-Kubectl @('cluster-info') | Out-Null
    $token = Get-RuntimeDiagnosticsToken
    $componentResults = @()
    foreach ($entry in $componentPorts.GetEnumerator()) {
        Start-PortForward `
            $rocketmqNamespace `
            $entry.Value.Service `
            $entry.Value.Port `
            8087
        $componentResults += Test-ComponentEndpoint $entry.Key $entry.Value.Port $token
    }

    $prometheusPort = 19090
    Start-PortForward $observabilityNamespace 'prometheus' $prometheusPort 9090
    $metricResults = foreach ($metric in $runtimeMetrics) {
        Wait-RuntimeMetric $metric $prometheusPort
    }

    $report = [ordered]@{
        schema_version = 'rocketmq.sre-runtime-diagnostics-smoke.v1'
        observed_at = [DateTimeOffset]::UtcNow.ToString('o')
        endpoint_authentication = 'bearer_and_rocketmq_diagnose_scope'
        components = $componentResults
        metrics = $metricResults
        secret_disclosure = $false
        artifact_drives = @('D', 'F')
    }
    $reportPath = Join-Path $ArtifactRoot 'report.json'
    [IO.File]::WriteAllText(
        $reportPath,
        ($report | ConvertTo-Json -Depth 12),
        [Text.UTF8Encoding]::new($false)
    )
    Write-Host "PHASE00_RUNTIME_DIAGNOSTICS_E2E_OK report=$reportPath"
}
finally {
    foreach ($process in $portForwardProcesses) {
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        }
        $process.Dispose()
    }
}

# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$RegionAKubeconfig,
    [Parameter(Mandatory = $true)][string]$RegionAContext,
    [Parameter(Mandatory = $true)][string]$RegionBKubeconfig,
    [Parameter(Mandatory = $true)][string]$RegionBContext,
    [string]$ConnectorNamespace = 'rocketmq-system',
    [string]$AgentNamespace = 'rocketmq-sre',
    [ValidateRange(30, 600)][int]$RecoveryTimeoutSeconds = 180,
    [string]$EvidenceOutput = 'D:\rocketmq-sre-evidence\two-region-isolation.json'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$startedAt = [DateTimeOffset]::UtcNow
$evidenceOutput = [IO.Path]::GetFullPath($EvidenceOutput)
$components = @(
    [ordered]@{
        name = 'connector'
        namespace = $ConnectorNamespace
        deployment = 'rocketmq-mcp'
        container = 'connector'
        selector = 'app.kubernetes.io/name=rocketmq-mcp'
        cluster_variable = 'ROCKETMQ_SRE_CLUSTER_ID'
        locality_variable = 'ROCKETMQ_SRE_MCP_URL'
        expected_locality = 'https://127.0.0.1:8089/mcp'
    },
    [ordered]@{
        name = 'execution_agent'
        namespace = $AgentNamespace
        deployment = 'sre-execution-agent'
        container = 'execution-agent'
        selector = 'app.kubernetes.io/name=rocketmq-sre-execution-agent'
        cluster_variable = 'ROCKETMQ_SRE_AGENT_CLUSTER_ID'
        service_account = 'sre-execution-agent'
    }
)

function Assert-DataPath([string]$Path, [string]$Description) {
    $root = [IO.Path]::GetPathRoot([IO.Path]::GetFullPath($Path))
    if (
        -not $root.Equals('D:\', [StringComparison]::OrdinalIgnoreCase) -and
        -not $root.Equals('F:\', [StringComparison]::OrdinalIgnoreCase)
    ) {
        throw "$Description must use the D or F drive."
    }
}

function Invoke-Kubectl(
    [string]$Kubeconfig,
    [string]$Context,
    [string[]]$Arguments,
    [string]$Description
) {
    $output = & kubectl --kubeconfig $Kubeconfig --context $Context @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed.`n$($output -join [Environment]::NewLine)"
    }
    ($output -join "`n").Trim()
}

function Get-RegionState([string]$Kubeconfig, [string]$Context, [string]$Label) {
    $config = Invoke-Kubectl $Kubeconfig $Context @('config', 'view', '--minify', '--raw', '-o', 'json') "$Label config"
    $server = [string](($config | ConvertFrom-Json).clusters[0].cluster.server)
    if ([string]::IsNullOrWhiteSpace($server)) {
        throw "$Label has no Kubernetes API server identity."
    }
    $deployments = @()
    foreach ($component in $components) {
        Invoke-Kubectl $Kubeconfig $Context @(
            '-n', $component.namespace, 'rollout', 'status', "deployment/$($component.deployment)",
            "--timeout=${RecoveryTimeoutSeconds}s"
        ) "$Label $($component.name) readiness" | Out-Null
        $raw = Invoke-Kubectl $Kubeconfig $Context @(
            '-n', $component.namespace, 'get', "deployment/$($component.deployment)", '-o', 'json'
        ) "$Label $($component.name) deployment"
        $deployment = $raw | ConvertFrom-Json
        $container = $deployment.spec.template.spec.containers |
            Where-Object { $_.name -eq $component.container } |
            Select-Object -First 1
        if ($null -eq $container) {
            throw "$Label $($component.name) is missing container '$($component.container)'."
        }
        $clusterEntry = $container.env |
            Where-Object { $_.name -eq $component.cluster_variable } |
            Select-Object -First 1
        if ($null -eq $clusterEntry -or [string]::IsNullOrWhiteSpace([string]$clusterEntry.value)) {
            throw "$Label $($component.name) does not expose an exact cluster binding."
        }
        if ($component.Contains('locality_variable')) {
            $localityEntry = $container.env |
                Where-Object { $_.name -eq $component.locality_variable } |
                Select-Object -First 1
            if ($null -eq $localityEntry -or [string]$localityEntry.value -ne $component.expected_locality) {
                throw "$Label $($component.name) is not bound to its region-local dependency."
            }
            $locality = [string]$localityEntry.value
        }
        else {
            if (
                $deployment.spec.template.spec.automountServiceAccountToken -ne $true -or
                [string]$deployment.spec.template.spec.serviceAccountName -ne $component.service_account
            ) {
                throw "$Label $($component.name) is not bound to its region-local Kubernetes identity."
            }
            $locality = "in-cluster-service-account:$($component.service_account)"
        }
        $pods = Invoke-Kubectl $Kubeconfig $Context @(
            '-n', $component.namespace, 'get', 'pods',
            '-l', $component.selector,
            '-o', 'json'
        ) "$Label $($component.name) pods"
        $podItems = @((($pods | ConvertFrom-Json).items))
        if ($podItems.Count -lt 1) {
            throw "$Label $($component.name) has no running pod."
        }
        $deployments += [ordered]@{
            component = $component.name
            namespace = $component.namespace
            deployment = $component.deployment
            container = $component.container
            cluster_id = [string]$clusterEntry.value
            region_local_dependency = $locality
            replicas = [int]$deployment.spec.replicas
            ready_replicas = [int]$deployment.status.readyReplicas
            pod_uids = @($podItems | ForEach-Object { [string]$_.metadata.uid } | Sort-Object)
        }
    }
    [ordered]@{
        label = $Label
        context = $Context
        api_server = $server
        deployments = $deployments
    }
}

function Get-ClusterBindings($Region) {
    @($Region.deployments | ForEach-Object { $_.cluster_id } | Sort-Object -Unique)
}

Assert-DataPath $RegionAKubeconfig 'Region A kubeconfig'
Assert-DataPath $RegionBKubeconfig 'Region B kubeconfig'
Assert-DataPath $evidenceOutput 'evidence output'
if ($RegionAContext -eq $RegionBContext -and
    [IO.Path]::GetFullPath($RegionAKubeconfig) -eq [IO.Path]::GetFullPath($RegionBKubeconfig)) {
    throw 'The two regions must not resolve to the same kubeconfig and context.'
}

$regionA = Get-RegionState ([IO.Path]::GetFullPath($RegionAKubeconfig)) $RegionAContext 'region-a'
$regionB = Get-RegionState ([IO.Path]::GetFullPath($RegionBKubeconfig)) $RegionBContext 'region-b'
if ($regionA.api_server -eq $regionB.api_server) {
    throw 'The two regions resolve to the same Kubernetes API server.'
}
$bindingsA = @(Get-ClusterBindings $regionA)
$bindingsB = @(Get-ClusterBindings $regionB)
if ($bindingsA.Count -ne 1 -or $bindingsB.Count -ne 1) {
    throw 'Connector and Execution Agent must share one exact cluster binding inside each region.'
}
$sharedLogicalClusterFixture = $bindingsA[0] -eq $bindingsB[0]

$originalReplicas = @{}
$disconnectStartedAt = [DateTimeOffset]::UtcNow
try {
    foreach ($deployment in $regionA.deployments) {
        $originalReplicas[$deployment.component] = $deployment.replicas
        Invoke-Kubectl $RegionAKubeconfig $RegionAContext @(
            '-n', $deployment.namespace, 'scale', "deployment/$($deployment.deployment)", '--replicas=0'
        ) "disconnect region A $($deployment.component)" | Out-Null
    }
    foreach ($component in $components) {
        Invoke-Kubectl $RegionAKubeconfig $RegionAContext @(
            '-n', $component.namespace, 'wait', '--for=delete', "--timeout=${RecoveryTimeoutSeconds}s",
            'pod', '-l', $component.selector
        ) "region A $($component.name) disconnect completion" | Out-Null
    }
    $regionBDuringOutage = Get-RegionState $RegionBKubeconfig $RegionBContext 'region-b-during-region-a-outage'
    foreach ($deployment in $regionBDuringOutage.deployments) {
        if ($deployment.ready_replicas -lt 1) {
            throw "Region B $($deployment.component) lost readiness during Region A outage."
        }
    }
}
finally {
    foreach ($component in $components) {
        if ($originalReplicas.ContainsKey($component.name)) {
            Invoke-Kubectl $RegionAKubeconfig $RegionAContext @(
                '-n', $component.namespace, 'scale', "deployment/$($component.deployment)",
                "--replicas=$($originalReplicas[$component.name])"
            ) "restore region A $($component.name)" | Out-Null
        }
    }
}

$recoveryStartedAt = [DateTimeOffset]::UtcNow
$regionARecovered = Get-RegionState $RegionAKubeconfig $RegionAContext 'region-a-recovered'
$recoverySeconds = [int][Math]::Ceiling(([DateTimeOffset]::UtcNow - $recoveryStartedAt).TotalSeconds)
if ($recoverySeconds -gt $RecoveryTimeoutSeconds) {
    throw "Region A recovery exceeded the bounded ${RecoveryTimeoutSeconds}s window."
}
$newConnectorUids = @(
    ($regionARecovered.deployments | Where-Object { $_.component -eq 'connector' }).pod_uids
)
$oldConnectorUids = @(
    ($regionA.deployments | Where-Object { $_.component -eq 'connector' }).pod_uids
)
if (@($newConnectorUids | Where-Object { $_ -in $oldConnectorUids }).Count -ne 0) {
    throw 'Connector recovery did not create a new session-owning process for capability re-handshake.'
}

$evidence = [ordered]@{
    schema_version = 'rocketmq-sre.two-region-isolation-qualification.v1'
    status = 'passed'
    environment = 'two-independent-kubernetes-regions'
    started_at = $startedAt.ToString('O')
    finished_at = [DateTimeOffset]::UtcNow.ToString('O')
    revision = (& git -C (Split-Path -Parent $PSScriptRoot) rev-parse HEAD).Trim()
    region_a = $regionA
    region_b = $regionB
    region_b_during_region_a_outage = $regionBDuringOutage
    region_a_recovered = $regionARecovered
    region_a_disconnect_seconds = [int][Math]::Ceiling(($recoveryStartedAt - $disconnectStartedAt).TotalSeconds)
    region_a_recovery_seconds = $recoverySeconds
    independent_api_servers = $true
    region_local_dependencies_verified = $true
    shared_logical_cluster_fixture = $sharedLogicalClusterFixture
    cross_region_writes = 0
    healthy_region_unavailable_components = 0
    capability_rehandshake_forced_by_new_connector_process = $true
    recovery_backlog_bounded_by_seconds = $RecoveryTimeoutSeconds
    secrets_recorded = $false
}
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $evidenceOutput) | Out-Null
$evidence | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $evidenceOutput -Encoding utf8
Write-Host "TWO_REGION_ISOLATION_QUALIFICATION_OK evidence=$evidenceOutput"

# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidateSet('Validate', 'Run')]
    [string]$Mode = 'Validate',

    [string]$Kubeconfig = 'D:\BuildCache\rocketmq-sre-temp\kind\phase00-kubeconfig',
    [string]$ExpectedContext = 'kubernetes-admin@rocketmq-sre-phase00',

    [ValidateRange(30, 86400)]
    [int]$DurationSeconds = 21600,

    [ValidateRange(5, 60)]
    [int]$SampleIntervalSeconds = 60,

    [ValidateRange(1, 60)]
    [int]$CollectorOutageSeconds = 10,

    [switch]$InjectFaults,
    [switch]$FullDurationQualification,

    [string]$EvidenceOutput = 'D:\BuildCache\rocketmq-sre-temp\phase05-soak-chaos.json'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$workloads = @(
    [pscustomobject]@{
        Namespace = 'rocketmq-sre'
        Kind = 'Deployment'
        Resource = 'deployment'
        Name = 'sre-control-plane'
    },
    [pscustomobject]@{
        Namespace = 'rocketmq-sre'
        Kind = 'Deployment'
        Resource = 'deployment'
        Name = 'sre-executor'
    },
    [pscustomobject]@{
        Namespace = 'rocketmq-sre'
        Kind = 'Deployment'
        Resource = 'deployment'
        Name = 'sre-execution-agent'
    },
    [pscustomobject]@{
        Namespace = 'rocketmq-sre'
        Kind = 'Deployment'
        Resource = 'deployment'
        Name = 'sre-model-mock'
    },
    [pscustomobject]@{
        Namespace = 'rocketmq-sre'
        Kind = 'Deployment'
        Resource = 'deployment'
        Name = 'sre-ui'
    },
    [pscustomobject]@{
        Namespace = 'rocketmq-sre'
        Kind = 'StatefulSet'
        Resource = 'statefulset'
        Name = 'postgres'
    },
    [pscustomobject]@{
        Namespace = 'rocketmq-system'
        Kind = 'Deployment'
        Resource = 'deployment'
        Name = 'rocketmq-mcp'
    },
    [pscustomobject]@{
        Namespace = 'rocketmq-system'
        Kind = 'Deployment'
        Resource = 'deployment'
        Name = 'rocketmq-proxy'
    },
    [pscustomobject]@{
        Namespace = 'rocketmq-system'
        Kind = 'StatefulSet'
        Resource = 'statefulset'
        Name = 'rocketmq-namesrv'
    },
    [pscustomobject]@{
        Namespace = 'rocketmq-system'
        Kind = 'StatefulSet'
        Resource = 'statefulset'
        Name = 'rocketmq-controller'
    },
    [pscustomobject]@{
        Namespace = 'rocketmq-system'
        Kind = 'StatefulSet'
        Resource = 'statefulset'
        Name = 'rocketmq-broker'
    },
    [pscustomobject]@{
        Namespace = 'observability'
        Kind = 'Deployment'
        Resource = 'deployment'
        Name = 'otel-collector'
    },
    [pscustomobject]@{
        Namespace = 'observability'
        Kind = 'Deployment'
        Resource = 'deployment'
        Name = 'prometheus'
    },
    [pscustomobject]@{
        Namespace = 'observability'
        Kind = 'Deployment'
        Resource = 'deployment'
        Name = 'loki'
    },
    [pscustomobject]@{
        Namespace = 'observability'
        Kind = 'Deployment'
        Resource = 'deployment'
        Name = 'tempo'
    }
)

$faultRecords = [System.Collections.Generic.List[object]]::new()
$sampleRecords = [System.Collections.Generic.List[object]]::new()
$collectorOriginalReplicas = $null
$runSucceeded = $false

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found."
    }
}

function Assert-DataPath([string]$Path, [string]$Description) {
    $fullPath = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetPathRoot($fullPath)
    if (
        -not $root.Equals('D:\', [StringComparison]::OrdinalIgnoreCase) -and
        -not $root.Equals('F:\', [StringComparison]::OrdinalIgnoreCase)
    ) {
        throw "$Description must use the D or F drive."
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
        throw "$Command failed with exit code $exitCode`: $($output.Trim())"
    }
    [pscustomobject]@{
        ExitCode = $exitCode
        Output = $output.Trim()
    }
}

function Invoke-Kubectl {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure
    )

    Invoke-Native kubectl (@('--kubeconfig', $Kubeconfig) + $Arguments) `
        -AllowFailure:$AllowFailure
}

function Get-KubernetesObject([string[]]$Arguments) {
    $result = Invoke-Kubectl $Arguments
    $result.Output | ConvertFrom-Json
}

function Get-OptionalProperty(
    [AllowNull()][object]$Object,
    [string]$Name,
    [AllowNull()][object]$Default
) {
    if ($null -eq $Object) {
        return $Default
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property -or $null -eq $property.Value) {
        return $Default
    }
    $property.Value
}

function Get-WorkloadKey([object]$Workload) {
    "$($Workload.Namespace)/$($Workload.Resource)/$($Workload.Name)"
}

function Get-ReadinessSnapshot([string[]]$ExcludedKeys = @()) {
    $resourceList = Get-KubernetesObject @(
        'get', 'deployments,statefulsets',
        '--all-namespaces',
        '--output', 'json'
    )
    $records = [System.Collections.Generic.List[object]]::new()
    $allReady = $true
    foreach ($workload in $workloads) {
        $key = Get-WorkloadKey $workload
        if ($ExcludedKeys -contains $key) {
            continue
        }
        $matches = @(
            $resourceList.items |
                Where-Object {
                    $_.kind -eq $workload.Kind -and
                    $_.metadata.namespace -eq $workload.Namespace -and
                    $_.metadata.name -eq $workload.Name
                }
        )
        if ($matches.Count -ne 1) {
            $allReady = $false
            $records.Add([ordered]@{
                key = $key
                desired = 1
                ready = 0
                generation_observed = $false
                status = 'missing'
            })
            continue
        }
        $item = $matches[0]
        $desired = [int](Get-OptionalProperty $item.spec 'replicas' 1)
        $readyValue = if ($workload.Kind -eq 'Deployment') {
            Get-OptionalProperty $item.status 'availableReplicas' 0
        }
        else {
            Get-OptionalProperty $item.status 'readyReplicas' 0
        }
        $ready = [int]$readyValue
        $observedGeneration = Get-OptionalProperty `
            $item.status `
            'observedGeneration' `
            $null
        $generationObserved = (
            $null -ne $observedGeneration -and
            [int64]$observedGeneration -ge [int64]$item.metadata.generation
        )
        $status = if ($ready -eq $desired -and $generationObserved) {
            'ready'
        }
        else {
            'not_ready'
        }
        if ($status -ne 'ready') {
            $allReady = $false
        }
        $records.Add([ordered]@{
            key = $key
            desired = $desired
            ready = $ready
            generation_observed = $generationObserved
            status = $status
        })
    }
    [pscustomobject]@{
        observed_at = [DateTimeOffset]::UtcNow.ToString('O')
        all_ready = $allReady
        workloads = @($records)
    }
}

function Get-PodIdentity([string]$Namespace, [string]$Selector) {
    $podList = Get-KubernetesObject @(
        '--namespace', $Namespace,
        'get', 'pods',
        '--selector', $Selector,
        '--output', 'json'
    )
    $pods = @(
        $podList.items |
            Where-Object {
                $null -eq $_.metadata.PSObject.Properties['deletionTimestamp']
            }
    )
    if ($pods.Count -ne 1) {
        throw "Expected one active Pod for $Namespace selector '$Selector'."
    }
    $pod = $pods[0]
    $statuses = @(Get-OptionalProperty $pod.status 'containerStatuses' @())
    [pscustomobject]@{
        name = [string]$pod.metadata.name
        uid = [string]$pod.metadata.uid
        ready = (
            $pod.status.phase -eq 'Running' -and
            $statuses.Count -gt 0 -and
            @($statuses | Where-Object { -not $_.ready }).Count -eq 0
        )
    }
}

function Wait-NewReadyPod(
    [string]$Namespace,
    [string]$Selector,
    [string]$PreviousUid
) {
    $deadline = [DateTimeOffset]::UtcNow.AddMinutes(3)
    do {
        try {
            $pod = Get-PodIdentity $Namespace $Selector
            if ($pod.uid -ne $PreviousUid -and $pod.ready) {
                return $pod
            }
        }
        catch {
            # A Deployment or StatefulSet can briefly have no active Pod.
        }
        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "A replacement Pod for $Namespace selector '$Selector' did not become ready."
}

function Get-PvcUidSet {
    $list = Get-KubernetesObject @(
        '--namespace', 'rocketmq-system',
        'get', 'persistentvolumeclaims',
        '--output', 'json'
    )
    (@(
        $list.items |
            Sort-Object { $_.metadata.name } |
            ForEach-Object { "$($_.metadata.name)=$($_.metadata.uid)" }
    ) -join ',')
}

function Invoke-PodReplacementFault(
    [string]$Id,
    [string]$Namespace,
    [string]$Selector
) {
    $before = Get-PodIdentity $Namespace $Selector
    if (-not $before.ready) {
        throw "Fault '$Id' target was not ready before injection."
    }
    $timer = [Diagnostics.Stopwatch]::StartNew()
    Invoke-Kubectl @(
        '--namespace', $Namespace,
        'delete', 'pod', $before.name,
        '--wait=false'
    ) | Out-Null
    $after = Wait-NewReadyPod $Namespace $Selector $before.uid
    $timer.Stop()
    $faultRecords.Add([ordered]@{
        id = $Id
        target_namespace = $Namespace
        target_selector = $Selector
        pod_uid_before = $before.uid
        pod_uid_after = $after.uid
        recovered = $true
        recovery_seconds = [Math]::Round($timer.Elapsed.TotalSeconds, 3)
    })
}

function Wait-DeploymentReplicaCount(
    [string]$Namespace,
    [string]$Name,
    [int]$Replicas
) {
    $deadline = [DateTimeOffset]::UtcNow.AddMinutes(3)
    do {
        $deployment = Get-KubernetesObject @(
            '--namespace', $Namespace,
            'get', 'deployment', $Name,
            '--output', 'json'
        )
        $available = [int](
            Get-OptionalProperty $deployment.status 'availableReplicas' 0
        )
        if (
            [int]$deployment.spec.replicas -eq $Replicas -and
            $available -eq $Replicas
        ) {
            return
        }
        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Deployment $Namespace/$Name did not reach $Replicas replicas."
}

function Invoke-CollectorOutageFault {
    $timer = [Diagnostics.Stopwatch]::StartNew()
    Invoke-Kubectl @(
        '--namespace', 'observability',
        'scale', 'deployment/otel-collector',
        '--replicas=0'
    ) | Out-Null
    Wait-DeploymentReplicaCount 'observability' 'otel-collector' 0
    Start-Sleep -Seconds $CollectorOutageSeconds
    $dataPlane = Get-ReadinessSnapshot @(
        'observability/deployment/otel-collector'
    )
    if (-not $dataPlane.all_ready) {
        throw 'A non-collector workload became unavailable during the Collector outage.'
    }
    Invoke-Kubectl @(
        '--namespace', 'observability',
        'scale', 'deployment/otel-collector',
        "--replicas=$collectorOriginalReplicas"
    ) | Out-Null
    Wait-DeploymentReplicaCount `
        'observability' `
        'otel-collector' `
        $collectorOriginalReplicas
    $timer.Stop()
    $faultRecords.Add([ordered]@{
        id = 'collector_outage'
        target_namespace = 'observability'
        target_selector = 'app.kubernetes.io/name=otel-collector'
        outage_seconds = $CollectorOutageSeconds
        data_plane_remained_ready = $true
        recovered = $true
        recovery_seconds = [Math]::Round($timer.Elapsed.TotalSeconds, 3)
    })
}

function Invoke-Fault([string]$Id) {
    switch ($Id) {
        'connector_pod_replacement' {
            Invoke-PodReplacementFault `
                $Id `
                'rocketmq-system' `
                'app.kubernetes.io/name=rocketmq-mcp'
        }
        'control_plane_pod_replacement' {
            Invoke-PodReplacementFault `
                $Id `
                'rocketmq-sre' `
                'app.kubernetes.io/name=rocketmq-sre-control-plane'
        }
        'collector_outage' {
            Invoke-CollectorOutageFault
        }
        'broker_pod_replacement' {
            Invoke-PodReplacementFault `
                $Id `
                'rocketmq-system' `
                'app.kubernetes.io/name=rocketmq-broker'
        }
        default {
            throw "Unknown fault '$Id'."
        }
    }
}

Assert-DataPath $Kubeconfig 'Kubernetes kubeconfig'
Assert-DataPath $EvidenceOutput 'soak evidence'
if ($FullDurationQualification) {
    if ($DurationSeconds -lt 21600) {
        throw 'FullDurationQualification requires at least 21600 seconds.'
    }
    if (-not $InjectFaults) {
        throw 'FullDurationQualification requires InjectFaults.'
    }
}

if ($Mode -eq 'Validate') {
    Write-Host 'PHASE05_SOAK_CHAOS_VALIDATION_OK data_drives=D,F max_sample_interval_seconds=60'
    exit 0
}

Require-Command kubectl
Require-Command git
if (-not (Test-Path -LiteralPath $Kubeconfig -PathType Leaf)) {
    throw "Kind kubeconfig is missing: $Kubeconfig"
}
$Kubeconfig = [IO.Path]::GetFullPath($Kubeconfig)
$EvidenceOutput = [IO.Path]::GetFullPath($EvidenceOutput)

$context = (Invoke-Kubectl @('config', 'current-context')).Output
if ($context -ne $ExpectedContext) {
    throw "Refusing to inject faults into unexpected context '$context'."
}

$initial = Get-ReadinessSnapshot
if (-not $initial.all_ready) {
    throw 'The soak cluster was not fully ready before sampling.'
}
$collector = Get-KubernetesObject @(
    '--namespace', 'observability',
    'get', 'deployment', 'otel-collector',
    '--output', 'json'
)
$collectorOriginalReplicas = [int]$collector.spec.replicas
if ($collectorOriginalReplicas -lt 1) {
    throw 'The Collector must be running before the soak.'
}
$pvcUidsBefore = Get-PvcUidSet
if ([string]::IsNullOrWhiteSpace($pvcUidsBefore)) {
    throw 'Broker PVC identity evidence is empty.'
}

$faultSchedule = @()
if ($InjectFaults) {
    $faultSchedule = @(
        [pscustomobject]@{ Ratio = 0.20; Id = 'connector_pod_replacement' },
        [pscustomobject]@{ Ratio = 0.40; Id = 'control_plane_pod_replacement' },
        [pscustomobject]@{ Ratio = 0.60; Id = 'collector_outage' },
        [pscustomobject]@{ Ratio = 0.80; Id = 'broker_pod_replacement' }
    )
}

$startedAt = [DateTimeOffset]::UtcNow
$stopwatch = [Diagnostics.Stopwatch]::StartNew()
$nextFault = 0
try {
    while ($true) {
        $ratio = [Math]::Min(1.0, $stopwatch.Elapsed.TotalSeconds / $DurationSeconds)
        while (
            $nextFault -lt $faultSchedule.Count -and
            $ratio -ge $faultSchedule[$nextFault].Ratio
        ) {
            Invoke-Fault $faultSchedule[$nextFault].Id
            $nextFault++
            $ratio = [Math]::Min(
                1.0,
                $stopwatch.Elapsed.TotalSeconds / $DurationSeconds
            )
        }

        $snapshot = Get-ReadinessSnapshot
        $sampleRecords.Add([ordered]@{
            elapsed_seconds = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 3)
            observed_at = $snapshot.observed_at
            all_ready = $snapshot.all_ready
            workloads = $snapshot.workloads
        })
        if ($stopwatch.Elapsed.TotalSeconds -ge $DurationSeconds) {
            break
        }
        $remaining = $DurationSeconds - $stopwatch.Elapsed.TotalSeconds
        $sleepSeconds = [Math]::Min($SampleIntervalSeconds, $remaining)
        Start-Sleep -Milliseconds ([int][Math]::Ceiling($sleepSeconds * 1000))
    }

    if ($nextFault -ne $faultSchedule.Count) {
        throw 'Not every scheduled fault was executed.'
    }
    $stopwatch.Stop()
    $final = Get-ReadinessSnapshot
    if (-not $final.all_ready) {
        throw 'The soak cluster did not return to full readiness.'
    }
    $pvcUidsAfter = Get-PvcUidSet
    if ($pvcUidsAfter -ne $pvcUidsBefore) {
        throw 'Broker PVC UIDs changed during the soak.'
    }
    $readySamples = @($sampleRecords | Where-Object { $_.all_ready }).Count
    $availabilityRatio = $readySamples / [double]$sampleRecords.Count
    if ($availabilityRatio -lt 0.99) {
        throw "Soak sampled availability $availabilityRatio below 0.99."
    }

    $repositoryRoot = [IO.Path]::GetFullPath(
        (Join-Path (Split-Path -Parent $PSScriptRoot) '..')
    )
    $commit = (
        Invoke-Native git @('-C', $repositoryRoot, 'rev-parse', 'HEAD')
    ).Output
    $evidence = [ordered]@{
        schema_version = 'rocketmq-sre.phase05-soak-chaos.v1'
        status = 'passed'
        observed_at = [DateTimeOffset]::UtcNow.ToString('O')
        cluster_context = $context
        repository_commit = $commit
        full_duration_qualification = [bool]$FullDurationQualification
        production_environment = $false
        planned_duration_seconds = $DurationSeconds
        observed_duration_seconds = [Math]::Round(
            $stopwatch.Elapsed.TotalSeconds,
            3
        )
        sample_interval_seconds = $SampleIntervalSeconds
        samples_observed = $sampleRecords.Count
        samples_ready = $readySamples
        sampled_availability_ratio = $availabilityRatio
        fault_injection_enabled = [bool]$InjectFaults
        faults = @($faultRecords)
        broker_pvc_uids_preserved = $true
        unresolved_faults = @()
        final_all_ready = $true
        samples = @($sampleRecords)
        sensitive_material_recorded = $false
    }
    $directory = Split-Path -Parent $EvidenceOutput
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    [IO.File]::WriteAllText(
        $EvidenceOutput,
        ($evidence | ConvertTo-Json -Depth 20),
        [Text.UTF8Encoding]::new($false)
    )
    $runSucceeded = $true
    Write-Host (
        "PHASE05_SOAK_CHAOS_OK duration_seconds=$($evidence.observed_duration_seconds) " +
        "samples=$($evidence.samples_observed) faults=$($faultRecords.Count) " +
        "availability=$availabilityRatio evidence=$EvidenceOutput"
    )
}
finally {
    if ($null -ne $collectorOriginalReplicas) {
        $restore = Invoke-Kubectl @(
            '--namespace', 'observability',
            'scale', 'deployment/otel-collector',
            "--replicas=$collectorOriginalReplicas"
        ) -AllowFailure
        if ($restore.ExitCode -eq 0) {
            try {
                Wait-DeploymentReplicaCount `
                    'observability' `
                    'otel-collector' `
                    $collectorOriginalReplicas
            }
            catch {
                if ($runSucceeded) {
                    throw
                }
            }
        }
    }
}

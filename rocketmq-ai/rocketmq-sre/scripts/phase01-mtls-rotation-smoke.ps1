# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidatePattern('^[a-z0-9][a-z0-9-]{0,39}$')]
    [string]$ClusterName = 'rocketmq-sre-phase00',

    [string]$Kubeconfig,

    [ValidateRange(1024, 65535)]
    [int]$ControlPlaneLocalPort = 18444,

    [ValidateRange(1024, 65535)]
    [int]$ConnectorLocalPort = 18091,

    [switch]$UseExistingCertificates
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$targetRoot = [IO.Path]::GetFullPath((Join-Path $repositoryRoot 'target'))
$certificateDirectory = Join-Path $targetRoot 'phase00-certs'
$evidenceDirectory = Join-Path $targetRoot 'phase01-mtls-rotation'
$evidencePath = Join-Path $evidenceDirectory 'evidence.json'
$oldClientIdentityPath = Join-Path $evidenceDirectory 'old-client-identity.pem'
$oldServerCaPath = Join-Path $evidenceDirectory 'old-server-ca.pem'
$opensslImage = 'alpine/openssl:3.5.2@sha256:ef8657028239a006f3de0bd04529e22c073bf0ab6655ece9f25c8dde9adec146'
$rocketmqNamespace = 'rocketmq-system'
$sreNamespace = 'rocketmq-sre'
$serverSecretName = 'rocketmq-sre-control-plane-channel-server'
$clientSecretName = 'rocketmq-sre-control-plane-channel-client'
$controlPlaneDeployment = 'deployment/sre-control-plane'
$mcpDeployment = 'deployment/rocketmq-mcp'
$controlPlaneServerName = 'sre-control-plane.rocketmq-sre.svc.cluster.local'
$portForwardProcesses = [Collections.Generic.List[Diagnostics.Process]]::new()
$rotationApplied = $false
$rotationSucceeded = $false
$oldServerSecret = $null
$oldClientSecret = $null

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found."
    }
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure,
        [switch]$Sensitive
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
        $detail = if ($Sensitive) { '<redacted>' } else { $output.Trim() }
        throw "$Command failed with exit code $exitCode`: $detail"
    }
    [pscustomobject]@{
        ExitCode = $exitCode
        Output = $output.Trim()
    }
}

function Invoke-Kubectl {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure,
        [switch]$Sensitive
    )

    Invoke-Native kubectl (@('--kubeconfig', $Kubeconfig) + $Arguments) `
        -AllowFailure:$AllowFailure `
        -Sensitive:$Sensitive
}

function Invoke-KubectlInput {
    param(
        [Parameter(Mandatory = $true)][string]$InputDocument,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = $InputDocument |
            & kubectl --kubeconfig $Kubeconfig @Arguments 2>&1 |
            Out-String
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0) {
        throw "kubectl failed while applying sensitive material with exit code $exitCode`: <redacted>"
    }
    $output.Trim()
}

function Get-KubernetesObject([string[]]$Arguments) {
    (Invoke-Kubectl ($Arguments + @('--output=json')) -Sensitive).Output |
        ConvertFrom-Json
}

function Convert-SecretValue([object]$Secret, [string]$Key) {
    $property = $Secret.data.PSObject.Properties[$Key]
    if ($null -eq $property -or [string]::IsNullOrWhiteSpace([string]$property.Value)) {
        throw "Required secret key '$Key' is absent."
    }
    [Convert]::FromBase64String([string]$property.Value)
}

function New-SecretData([hashtable]$Files) {
    $data = [ordered]@{}
    foreach ($entry in $Files.GetEnumerator()) {
        if (-not (Test-Path -LiteralPath $entry.Value -PathType Leaf)) {
            throw "Required certificate file is absent: $($entry.Value)"
        }
        $data[$entry.Key] = [Convert]::ToBase64String(
            [IO.File]::ReadAllBytes($entry.Value)
        )
    }
    $data
}

function Apply-SecretData(
    [string]$Namespace,
    [string]$Name,
    [object]$Data
) {
    $manifest = [ordered]@{
        apiVersion = 'v1'
        kind = 'Secret'
        metadata = [ordered]@{
            name = $Name
            namespace = $Namespace
        }
        type = 'Opaque'
        data = $Data
    } | ConvertTo-Json -Depth 8 -Compress
    Invoke-KubectlInput $manifest @('apply', '--filename=-') | Out-Null
}

function Restart-And-Wait([string]$Namespace, [string]$Workload) {
    Invoke-Kubectl @(
        '--namespace', $Namespace,
        'rollout', 'restart', $Workload
    ) | Out-Null
    Invoke-Kubectl @(
        '--namespace', $Namespace,
        'rollout', 'status', $Workload,
        '--timeout=300s'
    ) | Out-Null
}

function Get-PodIdentity([string]$Namespace, [string]$Selector) {
    $pods = Get-KubernetesObject @(
        '--namespace', $Namespace,
        'get', 'pods',
        '--selector', $Selector
    )
    $readyPods = @(
        $pods.items |
            Where-Object {
                $_.status.phase -eq 'Running' -and
                @($_.status.containerStatuses).Count -gt 0 -and
                (@($_.status.containerStatuses | Where-Object { -not $_.ready }).Count -eq 0)
            }
    )
    if ($readyPods.Count -ne 1) {
        throw "Expected one Ready Pod for selector '$Selector' in namespace '$Namespace'."
    }
    [ordered]@{
        name = [string]$readyPods[0].metadata.name
        uid = [string]$readyPods[0].metadata.uid
    }
}

function Test-LocalPortAvailable([int]$Port) {
    $listener = [Net.Sockets.TcpListener]::new(
        [Net.IPAddress]::Loopback,
        $Port
    )
    try {
        $listener.Start()
    }
    finally {
        $listener.Stop()
    }
}

function Wait-TcpPort([int]$Port, [int]$TimeoutSeconds = 20) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        $client = [Net.Sockets.TcpClient]::new()
        try {
            $connect = $client.ConnectAsync('127.0.0.1', $Port)
            if ($connect.Wait(500) -and $client.Connected) {
                return
            }
        }
        catch {
            # The bounded retry loop owns transient port-forward startup errors.
        }
        finally {
            $client.Dispose()
        }
        Start-Sleep -Milliseconds 250
    }
    throw "Port-forward did not become ready on 127.0.0.1:$Port."
}

function Start-PortForward(
    [string]$Namespace,
    [string]$Resource,
    [string]$Mapping,
    [string]$LogPrefix,
    [int]$LocalPort
) {
    Test-LocalPortAvailable $LocalPort
    $stdout = Join-Path $evidenceDirectory "$LogPrefix.stdout.log"
    $stderr = Join-Path $evidenceDirectory "$LogPrefix.stderr.log"
    $arguments = @(
        '--kubeconfig', $Kubeconfig,
        '--namespace', $Namespace,
        'port-forward', $Resource, $Mapping
    )
    $process = Start-Process `
        -FilePath 'kubectl.exe' `
        -ArgumentList $arguments `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -WindowStyle Hidden `
        -PassThru
    $portForwardProcesses.Add($process)
    Wait-TcpPort $LocalPort
}

function Stop-PortForwards {
    foreach ($process in $portForwardProcesses) {
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
    }
    $portForwardProcesses.Clear()
}

function Test-ClientCertificate(
    [string]$CaPath,
    [string]$IdentityPath,
    [switch]$ExpectSuccess
) {
    $certificateMount = "${certificateDirectory}:/certs:ro"
    $evidenceMount = "${evidenceDirectory}:/evidence:ro"
    $caContainerPath = if ($CaPath.StartsWith(
        $certificateDirectory,
        [StringComparison]::OrdinalIgnoreCase
    )) {
        "/certs/$([IO.Path]::GetFileName($CaPath))"
    }
    else {
        "/evidence/$([IO.Path]::GetFileName($CaPath))"
    }
    $identityContainerPath = if ($IdentityPath.StartsWith(
        $certificateDirectory,
        [StringComparison]::OrdinalIgnoreCase
    )) {
        "/certs/$([IO.Path]::GetFileName($IdentityPath))"
    }
    else {
        "/evidence/$([IO.Path]::GetFileName($IdentityPath))"
    }
    $arguments = @(
        'run', '--rm', '--interactive',
        '--volume', $certificateMount,
        '--volume', $evidenceMount,
        $opensslImage,
        's_client',
        '-quiet',
        '-verify_return_error',
        '-connect', "host.docker.internal:$ControlPlaneLocalPort",
        '-servername', $controlPlaneServerName,
        '-verify_hostname', $controlPlaneServerName,
        '-CAfile', $caContainerPath,
        '-cert', $identityContainerPath,
        '-key', $identityContainerPath
    )
    $request = "GET /healthz HTTP/1.1`r`nHost: $controlPlaneServerName`r`nConnection: close`r`n`r`n"
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = $request | & docker @arguments 2>&1 | Out-String
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    $statusMatch = [regex]::Match(
        $output,
        '(?m)^HTTP/(?:1\.[01]|2) ([1-5][0-9]{2})\b'
    )
    # The mTLS proxy deliberately maps this unauthenticated path to 404 after
    # certificate verification. Nginx returns 400 before routing when client
    # certificate validation fails.
    $accepted = $statusMatch.Success -and $statusMatch.Groups[1].Value -eq '404'
    if ($ExpectSuccess -and -not $accepted) {
        $status = if ($statusMatch.Success) { $statusMatch.Groups[1].Value } else { 'none' }
        throw "Expected mTLS client certificate was rejected (HTTP $status; transport exit $exitCode)."
    }
    if (-not $ExpectSuccess -and $accepted) {
        throw 'Retired mTLS client certificate was unexpectedly accepted.'
    }
}

function Assert-ConnectorReady {
    Start-PortForward `
        $rocketmqNamespace `
        'deployment/rocketmq-mcp' `
        "${ConnectorLocalPort}:8091" `
        'connector-ready' `
        $ConnectorLocalPort
    $response = Invoke-WebRequest `
        -UseBasicParsing `
        -Uri "http://127.0.0.1:$ConnectorLocalPort/readyz" `
        -TimeoutSec 10
    if ($response.StatusCode -ne 200) {
        throw "Connector readiness returned HTTP $($response.StatusCode)."
    }
}

function Restore-OriginalSecrets {
    if ($null -eq $oldServerSecret -or $null -eq $oldClientSecret) {
        return
    }
    Apply-SecretData `
        $sreNamespace `
        $serverSecretName `
        $oldServerSecret.data
    Apply-SecretData `
        $rocketmqNamespace `
        $clientSecretName `
        $oldClientSecret.data
    Restart-And-Wait $sreNamespace $controlPlaneDeployment
    Restart-And-Wait $rocketmqNamespace $mcpDeployment
}

Require-Command kubectl
Require-Command docker
if ([string]::IsNullOrWhiteSpace($Kubeconfig)) {
    $Kubeconfig = $env:KUBECONFIG
}
if ([string]::IsNullOrWhiteSpace($Kubeconfig)) {
    $Kubeconfig = Join-Path $targetRoot 'phase00-kind/kubeconfig'
}
$Kubeconfig = [IO.Path]::GetFullPath($Kubeconfig)
if (-not (Test-Path -LiteralPath $Kubeconfig -PathType Leaf)) {
    throw "Kind kubeconfig is missing: $Kubeconfig"
}
if (-not $evidenceDirectory.StartsWith(
    $targetRoot + [IO.Path]::DirectorySeparatorChar,
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw 'mTLS rotation artifacts escaped the repository target directory.'
}
New-Item -ItemType Directory -Force -Path $evidenceDirectory | Out-Null

try {
    $oldServerSecret = Get-KubernetesObject @(
        '--namespace', $sreNamespace,
        'get', 'secret', $serverSecretName
    )
    $oldClientSecret = Get-KubernetesObject @(
        '--namespace', $rocketmqNamespace,
        'get', 'secret', $clientSecretName
    )
    [IO.File]::WriteAllBytes(
        $oldClientIdentityPath,
        (Convert-SecretValue $oldClientSecret 'connector-client-identity.pem')
    )
    [IO.File]::WriteAllBytes(
        $oldServerCaPath,
        (Convert-SecretValue $oldClientSecret 'control-plane-server-ca-cert.pem')
    )

    $beforeControlPlane = Get-PodIdentity `
        $sreNamespace `
        'app.kubernetes.io/name=rocketmq-sre-control-plane'
    $beforeMcp = Get-PodIdentity `
        $rocketmqNamespace `
        'app.kubernetes.io/name=rocketmq-mcp'

    Start-PortForward `
        $sreNamespace `
        'service/sre-control-plane' `
        "${ControlPlaneLocalPort}:8444" `
        'before-rotation' `
        $ControlPlaneLocalPort
    Test-ClientCertificate `
        $oldServerCaPath `
        $oldClientIdentityPath `
        -ExpectSuccess
    Stop-PortForwards

    if (-not $UseExistingCertificates) {
        & (Join-Path $scriptDirectory 'dev.ps1') -Action ChannelCerts
        if (-not $?) {
            throw 'Failed to generate rotated Connector-channel certificates.'
        }
    }

    $requiredFiles = @(
        'control-plane-server-ca-cert.pem',
        'control-plane-server-cert.pem',
        'control-plane-server-key.pem',
        'connector-client-ca-cert.pem',
        'connector-client-cert.pem',
        'connector-client-identity.pem'
    )
    $missingFiles = @(
        $requiredFiles |
            Where-Object {
                -not (Test-Path -LiteralPath (Join-Path $certificateDirectory $_) -PathType Leaf)
            }
    )
    if ($missingFiles.Count -gt 0) {
        throw "Rotated certificate material is incomplete: $($missingFiles -join ', ')."
    }

    $serverData = New-SecretData @{
        'control-plane-server-cert.pem' = Join-Path $certificateDirectory 'control-plane-server-cert.pem'
        'control-plane-server-key.pem' = Join-Path $certificateDirectory 'control-plane-server-key.pem'
        'connector-client-ca-cert.pem' = Join-Path $certificateDirectory 'connector-client-ca-cert.pem'
    }
    $clientData = New-SecretData @{
        'control-plane-server-ca-cert.pem' = Join-Path $certificateDirectory 'control-plane-server-ca-cert.pem'
        'connector-client-identity.pem' = Join-Path $certificateDirectory 'connector-client-identity.pem'
    }
    Apply-SecretData $sreNamespace $serverSecretName $serverData
    Apply-SecretData $rocketmqNamespace $clientSecretName $clientData
    $rotationApplied = $true

    Restart-And-Wait $sreNamespace $controlPlaneDeployment
    Restart-And-Wait $rocketmqNamespace $mcpDeployment

    $afterControlPlane = Get-PodIdentity `
        $sreNamespace `
        'app.kubernetes.io/name=rocketmq-sre-control-plane'
    $afterMcp = Get-PodIdentity `
        $rocketmqNamespace `
        'app.kubernetes.io/name=rocketmq-mcp'
    if ($beforeControlPlane.uid -eq $afterControlPlane.uid) {
        throw 'Control Plane Pod UID did not change after certificate rotation.'
    }
    if ($beforeMcp.uid -eq $afterMcp.uid) {
        throw 'MCP/Connector Pod UID did not change after certificate rotation.'
    }

    Start-PortForward `
        $sreNamespace `
        'service/sre-control-plane' `
        "${ControlPlaneLocalPort}:8444" `
        'after-rotation' `
        $ControlPlaneLocalPort
    Test-ClientCertificate `
        (Join-Path $certificateDirectory 'control-plane-server-ca-cert.pem') `
        (Join-Path $certificateDirectory 'connector-client-identity.pem') `
        -ExpectSuccess
    Test-ClientCertificate `
        (Join-Path $certificateDirectory 'control-plane-server-ca-cert.pem') `
        $oldClientIdentityPath
    Stop-PortForwards

    Assert-ConnectorReady

    $newServerSecret = Get-KubernetesObject @(
        '--namespace', $sreNamespace,
        'get', 'secret', $serverSecretName
    )
    $newClientSecret = Get-KubernetesObject @(
        '--namespace', $rocketmqNamespace,
        'get', 'secret', $clientSecretName
    )
    $evidence = [ordered]@{
        schema_version = 'rocketmq-sre.phase01-mtls-rotation.v1'
        observed_at = [DateTimeOffset]::UtcNow.ToString('O')
        cluster_name = $ClusterName
        server_secret_resource_version_before = [string]$oldServerSecret.metadata.resourceVersion
        server_secret_resource_version_after = [string]$newServerSecret.metadata.resourceVersion
        client_secret_resource_version_before = [string]$oldClientSecret.metadata.resourceVersion
        client_secret_resource_version_after = [string]$newClientSecret.metadata.resourceVersion
        control_plane_pod_uid_before = $beforeControlPlane.uid
        control_plane_pod_uid_after = $afterControlPlane.uid
        connector_pod_uid_before = $beforeMcp.uid
        connector_pod_uid_after = $afterMcp.uid
        old_certificate_accepted_before_rotation = $true
        new_certificate_accepted_after_rotation = $true
        old_certificate_rejected_after_rotation = $true
        connector_ready_after_rotation = $true
        server_certificate_sha256 = (
            Get-FileHash `
                -Algorithm SHA256 `
                -LiteralPath (Join-Path $certificateDirectory 'control-plane-server-cert.pem')
        ).Hash.ToLowerInvariant()
        client_certificate_sha256 = (
            Get-FileHash `
                -Algorithm SHA256 `
                -LiteralPath (Join-Path $certificateDirectory 'connector-client-cert.pem')
        ).Hash.ToLowerInvariant()
        sensitive_material_in_evidence = $false
    }
    [IO.File]::WriteAllText(
        $evidencePath,
        ($evidence | ConvertTo-Json -Depth 6),
        [Text.UTF8Encoding]::new($false)
    )
    $rotationSucceeded = $true
    Write-Host "PHASE01_MTLS_ROTATION_OK cluster=$ClusterName old_rejected=true connector_ready=true"
}
catch {
    if ($rotationApplied) {
        Stop-PortForwards
        Restore-OriginalSecrets
    }
    throw
}
finally {
    Stop-PortForwards
    foreach ($sensitivePath in @($oldClientIdentityPath, $oldServerCaPath)) {
        if (Test-Path -LiteralPath $sensitivePath -PathType Leaf) {
            Remove-Item -LiteralPath $sensitivePath -Force
        }
    }
    if (-not $rotationSucceeded -and (Test-Path -LiteralPath $evidencePath -PathType Leaf)) {
        Remove-Item -LiteralPath $evidencePath -Force
    }
}

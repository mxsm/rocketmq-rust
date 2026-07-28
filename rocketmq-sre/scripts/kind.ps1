# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Up', 'Down', 'Status', 'Smoke')]
    [string]$Action,

    [ValidatePattern('^[a-z0-9][a-z0-9-]{0,39}$')]
    [string]$ClusterName = 'rocketmq-sre-phase00',

    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '..'))
$kindDirectory = Join-Path $sreRoot 'deploy/kind'
$clusterConfig = Join-Path $kindDirectory 'cluster.yaml'
$chartPath = Join-Path $repositoryRoot 'distribution/helm/rocketmq-rust'
$devValues = Join-Path $chartPath 'values-dev-single.yaml'
$kindValues = Join-Path $kindDirectory 'helm-values.yaml'
$policyPath = Join-Path $repositoryRoot 'distribution/kubernetes/fault-matrix-policy.json'
$policy = Get-Content -Raw -LiteralPath $policyPath | ConvertFrom-Json
$kubeContext = "kind-$ClusterName"
$artifactRoot = [IO.Path]::GetFullPath((Join-Path $repositoryRoot 'target/phase00-kind'))
$targetRoot = [IO.Path]::GetFullPath((Join-Path $repositoryRoot 'target'))
$kubeconfigPath = Join-Path $artifactRoot 'kubeconfig'
$certificateRoot = [IO.Path]::GetFullPath((Join-Path $repositoryRoot 'target/phase00-certs'))
$rocketmqNamespace = 'rocketmq-system'
$sreNamespace = 'rocketmq-sre'
$observabilityNamespace = 'observability'

$localImages = [ordered]@{
    broker = 'rocketmq-rust/broker:local'
    namesrv = 'rocketmq-rust/namesrv:local'
    controller = 'rocketmq-rust/controller:local'
    proxy = 'rocketmq-rust/proxy:local'
    mcp = 'rocketmq-rust/mcp:local'
    'sre-control-plane' = 'rocketmq-rust/sre-control-plane:phase00-local'
    'sre-connector' = 'rocketmq-rust/sre-connector:phase00-local'
    'sre-executor' = 'rocketmq-rust/sre-executor:phase03-local'
    'sre-execution-agent' = 'rocketmq-rust/sre-execution-agent:phase03-local'
    'sre-probe' = 'rocketmq-rust/sre-probe:phase00-local'
    'sre-model-mock' = 'rocketmq-rust/sre-model-mock:phase00-local'
    'sre-ui' = 'rocketmq-rust/sre-ui:phase00-local'
    'fault-driver' = 'rocketmq-rust/fault-driver:local'
}
$supportImages = @(
    'postgres:17-alpine',
    'nginx:1.29-alpine',
    'otel/opentelemetry-collector-contrib:0.130.1',
    'prom/prometheus:v3.5.0',
    'grafana/loki:3.5.2',
    'grafana/tempo:2.8.2'
)

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found. Install the repository-pinned tool before running Kind acceptance."
    }
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure
    )

    # Windows PowerShell turns any native stderr line into a terminating
    # NativeCommandError when the script-wide preference is Stop, even when
    # the native process exits successfully (for example `kind get clusters`
    # prints its empty-state message to stderr). Capture the stream and decide
    # success exclusively from the native exit code.
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

function Assert-ArtifactRoot {
    if (-not $artifactRoot.StartsWith(
        $targetRoot + [IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase
    )) {
        throw 'Kind artifacts escaped the repository target directory.'
    }
}

function Assert-PinnedTools {
    foreach ($command in @('docker', 'kind', 'kubectl', 'helm')) {
        Require-Command $command
    }

    $kindVersion = (Invoke-Native kind @('version')).Output
    if ($kindVersion -notmatch "(^|\s)$([regex]::Escape($policy.tools.kind))(\s|$)") {
        throw "Kind $($policy.tools.kind) is required; found '$kindVersion'."
    }
    $kubectlVersion = ((Invoke-Native kubectl @('version', '--client', '--output=json')).Output | ConvertFrom-Json)
    if ($kubectlVersion.clientVersion.gitVersion -ne $policy.tools.kubectl) {
        throw "kubectl $($policy.tools.kubectl) is required; found '$($kubectlVersion.clientVersion.gitVersion)'."
    }
    $helmVersion = (Invoke-Native helm @('version', '--short')).Output
    if ($helmVersion -notmatch "^$([regex]::Escape($policy.tools.helm))(\+|$)") {
        throw "Helm $($policy.tools.helm) is required; found '$helmVersion'."
    }
    Invoke-Native docker @('info', '--format', '{{.ServerVersion}}') | Out-Null
}

function Get-KindClusters {
    Require-Command kind
    $output = (Invoke-Native kind @('get', 'clusters')).Output
    if ([string]::IsNullOrWhiteSpace($output) -or $output -eq 'No kind clusters found.') {
        return @()
    }
    @($output -split '\r?\n' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Test-ClusterExists {
    (Get-KindClusters) -contains $ClusterName
}

function Assert-ClusterExists {
    if (-not (Test-ClusterExists)) {
        throw "Kind cluster '$ClusterName' does not exist. Run -Action Up first."
    }
}

function Write-Utf8File([string]$Path, [string]$Content) {
    [IO.File]::WriteAllText($Path, $Content, [Text.UTF8Encoding]::new($false))
}

function Ensure-AdminReadCredentialFixtures {
    $accessKeyPath = Join-Path $artifactRoot 'admin-read-access-key'
    $secretKeyPath = Join-Path $artifactRoot 'admin-read-secret-key'
    if (
        (Test-Path -LiteralPath $accessKeyPath -PathType Leaf) -and
        (Test-Path -LiteralPath $secretKeyPath -PathType Leaf)
    ) {
        return
    }

    $credentialsPath = Join-Path $artifactRoot 'mcp-rmq-credentials.yml'
    if (-not (Test-Path -LiteralPath $credentialsPath -PathType Leaf)) {
        throw 'Read-only Admin credential fixtures cannot be derived because the MCP reader credential file is missing.'
    }
    $credentials = Get-Content -Raw -LiteralPath $credentialsPath
    $accessKey = [regex]::Match($credentials, '(?m)^access_key:\s*([^\s]+)\s*$')
    $secretKey = [regex]::Match($credentials, '(?m)^secret_key:\s*([^\s]+)\s*$')
    if (-not $accessKey.Success -or -not $secretKey.Success) {
        throw 'The MCP reader credential file is malformed and cannot seed the read-only Admin source.'
    }
    Write-Utf8File $accessKeyPath $accessKey.Groups[1].Value
    Write-Utf8File $secretKeyPath $secretKey.Groups[1].Value
}

function New-RandomSecret {
    $bytes = New-Object byte[] 32
    $generator = [Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($bytes)
    }
    finally {
        $generator.Dispose()
    }
    ([BitConverter]::ToString($bytes) -replace '-', '').ToLowerInvariant()
}

function Ensure-Kubeconfig {
    Assert-ArtifactRoot
    New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null
    $kubeconfig = (Invoke-Native kind @('get', 'kubeconfig', '--name', $ClusterName)).Output
    Write-Utf8File $kubeconfigPath $kubeconfig
}

function Build-Images {
    foreach ($entry in $localImages.GetEnumerator()) {
        if ($entry.Key -in @(
                'sre-control-plane',
                'sre-connector',
                'sre-executor',
                'sre-execution-agent',
                'sre-probe',
                'sre-model-mock'
            )) {
            $target = switch ($entry.Key) {
                'sre-control-plane' { 'control-plane' }
                'sre-connector' { 'connector' }
                'sre-executor' { 'executor' }
                'sre-execution-agent' { 'execution-agent' }
                'sre-probe' { 'probe' }
                'sre-model-mock' { 'model-mock' }
            }
            Invoke-Native docker @(
                'build',
                '--file', (Join-Path $sreRoot 'deploy/docker/Dockerfile'),
                '--target', $target,
                '--tag', $entry.Value,
                $repositoryRoot
            ) | Out-Null
        }
        elseif ($entry.Key -eq 'sre-ui') {
            Invoke-Native docker @(
                'build',
                '--file', (Join-Path $sreRoot 'deploy/docker/ui.Dockerfile'),
                '--build-arg', 'VITE_SRE_AUTH_MODE=development',
                '--build-arg', 'VITE_SRE_DEV_SUBJECT=phase00-kind-ui',
                '--build-arg', 'VITE_SRE_DEV_DISPLAY_NAME=RocketMQ SRE Kind Operator',
                '--build-arg', 'VITE_SRE_DEV_TENANT=00000000-0000-4000-8000-000000000002',
                '--build-arg', 'VITE_SRE_DEV_CLUSTERS=00000000-0000-4000-8000-000000000001',
                '--build-arg', 'VITE_SRE_DEV_ROLES=rocketmq:read rocketmq:diagnose operator approver',
                '--build-arg', 'VITE_SRE_DEV_TOKEN=phase00-internal-token',
                '--tag', $entry.Value,
                $repositoryRoot
            ) | Out-Null
        }
        else {
            Invoke-Native docker @(
                'build',
                '--file', (Join-Path $repositoryRoot 'docker/Dockerfile.base'),
                '--target', $entry.Key,
                '--tag', $entry.Value,
                $repositoryRoot
            ) | Out-Null
        }
    }
    foreach ($image in $supportImages) {
        Invoke-Native docker @('pull', $image) | Out-Null
    }
}

function Assert-ImagesExist {
    foreach ($image in @($localImages.Values) + $supportImages) {
        $result = Invoke-Native docker @('image', 'inspect', $image) -AllowFailure
        if ($result.ExitCode -ne 0) {
            throw "Required local image '$image' is missing. Re-run Up without -SkipBuild."
        }
    }
}

function Load-Images {
    $nodeArchitecture = (Invoke-Kubectl @(
        'get', 'nodes',
        '--output=jsonpath={.items[0].status.nodeInfo.architecture}'
    )).Output.Trim()
    if ([string]::IsNullOrWhiteSpace($nodeArchitecture)) {
        throw 'Kind node architecture could not be determined.'
    }
    $platform = "linux/$nodeArchitecture"

    # Export one node-platform archive per image. Docker Desktop's containerd
    # image store retains multi-platform indexes without every foreign
    # manifest body, while `kind load docker-image` asks containerd to import
    # all referenced platforms and fails on those intentionally absent blobs.
    foreach ($image in @($localImages.Values) + $supportImages) {
        $archiveName = ($image -replace '[^A-Za-z0-9_.-]', '_') + '.tar'
        $archivePath = Join-Path $artifactRoot $archiveName
        Write-Host "Loading Kind image $image for $platform"
        try {
            Invoke-Native docker @(
                'image', 'save',
                '--platform', $platform,
                '--output', $archivePath,
                $image
            ) | Out-Null
            Invoke-Native kind @(
                'load', 'image-archive', $archivePath,
                '--name', $ClusterName
            ) | Out-Null
        }
        finally {
            if (Test-Path -LiteralPath $archivePath -PathType Leaf) {
                Remove-Item -LiteralPath $archivePath -Force
            }
        }
    }
}

function New-SecretMaterial([switch]$ExistingCluster) {
    Assert-ArtifactRoot
    New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null

    $devScript = Join-Path $scriptDirectory 'dev.ps1'
    $missingBaseCertificates = @(
        @('ca-cert.pem', 'server-cert.pem', 'server-key.pem') |
            Where-Object { -not (Test-Path -LiteralPath (Join-Path $certificateRoot $_) -PathType Leaf) }
    )
    if ($missingBaseCertificates.Count -gt 0) {
        if ($ExistingCluster) {
            throw "Kind certificate fixtures are incomplete for existing cluster '$ClusterName'. Run -Action Down before regenerating them."
        }
        & $devScript -Action Certs
    }
    $missingChannelCertificates = @(
        @(
            'control-plane-server-ca-cert.pem',
            'control-plane-server-cert.pem',
            'control-plane-server-key.pem',
            'connector-client-ca-cert.pem',
            'connector-client-identity.pem'
        ) |
            Where-Object { -not (Test-Path -LiteralPath (Join-Path $certificateRoot $_) -PathType Leaf) }
    )
    if ($missingChannelCertificates.Count -gt 0) {
        if ($ExistingCluster) {
            throw "Kind connector-channel mTLS fixtures are incomplete for existing cluster '$ClusterName'. Run -Action Down before regenerating them."
        }
        & $devScript -Action ChannelCerts
    }

    $secretFixtureNames = @(
        'admin.identity',
        'request-policy.json',
        'broker-acl.yml',
        'proxy-acl.yml',
        'mcp-rmq-credentials.yml',
        'probe-access-key',
        'probe-secret-key',
        'bootstrap-access-key',
        'bootstrap-secret-key',
        'agent-read-access-key',
        'agent-read-secret-key',
        'agent-mutation-access-key',
        'agent-mutation-secret-key',
        'mcp-token',
        'internal-token',
        'postgres-user',
        'postgres-password',
        'postgres-db',
        'database-url'
    )
    $existingSecretFixtures = @(
        $secretFixtureNames |
            Where-Object { Test-Path -LiteralPath (Join-Path $artifactRoot $_) -PathType Leaf }
    )
    if ($existingSecretFixtures.Count -eq $secretFixtureNames.Count) {
        Ensure-AdminReadCredentialFixtures
        Write-Host "Reusing existing Kind credential fixtures from $artifactRoot."
        return
    }
    if ($ExistingCluster) {
        $missing = @(
            $secretFixtureNames |
                Where-Object { -not (Test-Path -LiteralPath (Join-Path $artifactRoot $_) -PathType Leaf) }
        )
        throw "Kind credential fixtures are incomplete for existing cluster '$ClusterName': $($missing -join ', '). Run -Action Down before regenerating them."
    }

    $mcpAccessKey = 'phase00-kind-mcp-reader'
    $mcpSecretKey = New-RandomSecret
    $probeAccessKey = 'phase00-kind-probe'
    $probeSecretKey = New-RandomSecret
    $bootstrapAccessKey = 'phase00-kind-bootstrap'
    $bootstrapSecretKey = New-RandomSecret
    $agentReadAccessKey = 'phase03-kind-agent-read'
    $agentReadSecretKey = New-RandomSecret
    $agentMutationAccessKey = 'phase03-kind-agent-mutation'
    $agentMutationSecretKey = New-RandomSecret
    $mcpToken = New-RandomSecret
    $internalToken = New-RandomSecret
    $postgresPassword = New-RandomSecret
    $probeTopic = 'SRE_PROBE_00000000000040008000000000000001_00000000000000000000000000000000'
    $probeProducerGroup = 'SRE_PROBE_G_P_00000000000040008000000000000001_00000000000000000000000000000000'
    $probeConsumerGroup = 'SRE_PROBE_G_C_00000000000040008000000000000001_00000000000000000000000000000000'
    $brokerAcl = @(
        'globalWhiteRemoteAddresses: []'
        'accounts:'
        "  - accessKey: $mcpAccessKey"
        "    secretKey: $mcpSecretKey"
        '    admin: false'
        '    defaultTopicPerm: GET'
        '    defaultGroupPerm: GET'
        '    clusterPerm: GET'
        "  - accessKey: $probeAccessKey"
        "    secretKey: $probeSecretKey"
        '    admin: false'
        '    defaultTopicPerm: DENY'
        '    defaultGroupPerm: DENY'
        '    topicPerms:'
        "      - $probeTopic=PUB|SUB"
        '    groupPerms:'
        "      - $probeProducerGroup=SUB"
        "      - $probeConsumerGroup=SUB"
        "  - accessKey: $bootstrapAccessKey"
        "    secretKey: $bootstrapSecretKey"
        '    admin: true'
        '    defaultTopicPerm: DENY'
        '    defaultGroupPerm: DENY'
        "  - accessKey: $agentReadAccessKey"
        "    secretKey: $agentReadSecretKey"
        '    admin: false'
        '    defaultTopicPerm: GET'
        '    defaultGroupPerm: GET'
        '    clusterPerm: GET'
        "  - accessKey: $agentMutationAccessKey"
        "    secretKey: $agentMutationSecretKey"
        '    admin: true'
        '    defaultTopicPerm: DENY'
        '    defaultGroupPerm: DENY'
        ''
    ) -join "`n"
    $bootstrapAcl = @(
        'globalWhiteRemoteAddresses: []'
        'accounts:'
        "  - accessKey: $bootstrapAccessKey"
        "    secretKey: $bootstrapSecretKey"
        '    admin: true'
        '    defaultTopicPerm: DENY'
        '    defaultGroupPerm: DENY'
        ''
    ) -join "`n"
    $proxyAcl = @(
        'globalWhiteRemoteAddresses: []'
        'accounts:'
        "  - accessKey: $bootstrapAccessKey"
        "    secretKey: $bootstrapSecretKey"
        '    admin: true'
        '    defaultTopicPerm: DENY'
        '    defaultGroupPerm: DENY'
        "  - accessKey: $agentReadAccessKey"
        "    secretKey: $agentReadSecretKey"
        '    admin: false'
        '    defaultTopicPerm: GET'
        '    defaultGroupPerm: GET'
        '    clusterPerm: GET'
        "  - accessKey: $agentMutationAccessKey"
        "    secretKey: $agentMutationSecretKey"
        '    admin: true'
        '    defaultTopicPerm: DENY'
        '    defaultGroupPerm: DENY'
        ''
    ) -join "`n"
    $files = @{
        'admin.identity' = 'phase00-kind-admin'
        'request-policy.json' = '{"profile":"phase00-kind-read-only"}'
        'broker-acl.yml' = $brokerAcl
        'proxy-acl.yml' = $proxyAcl
        'mcp-rmq-credentials.yml' = "access_key: $mcpAccessKey`nsecret_key: $mcpSecretKey`n"
        'admin-read-access-key' = $mcpAccessKey
        'admin-read-secret-key' = $mcpSecretKey
        'probe-access-key' = $probeAccessKey
        'probe-secret-key' = $probeSecretKey
        'bootstrap-access-key' = $bootstrapAccessKey
        'bootstrap-secret-key' = $bootstrapSecretKey
        'agent-read-access-key' = $agentReadAccessKey
        'agent-read-secret-key' = $agentReadSecretKey
        'agent-mutation-access-key' = $agentMutationAccessKey
        'agent-mutation-secret-key' = $agentMutationSecretKey
        'mcp-token' = $mcpToken
        'internal-token' = $internalToken
        'postgres-user' = 'rocketmq_sre'
        'postgres-password' = $postgresPassword
        'postgres-db' = 'rocketmq_sre'
        'database-url' = "postgres://rocketmq_sre:$postgresPassword@postgres:5432/rocketmq_sre"
    }
    foreach ($entry in $files.GetEnumerator()) {
        Write-Utf8File (Join-Path $artifactRoot $entry.Key) $entry.Value
    }
}

function Apply-GeneratedSecret {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string[]]$FileArguments
    )

    $manifest = (Invoke-Kubectl (@(
        '--namespace', $Namespace,
        'create', 'secret', 'generic', $Name
    ) + $FileArguments + @('--dry-run=client', '--output=yaml'))).Output
    $manifestPath = Join-Path $artifactRoot "$Namespace-$Name.yaml"
    Write-Utf8File $manifestPath $manifest
    Invoke-Kubectl @('apply', '--filename', $manifestPath) | Out-Null
}

function Apply-Secrets {
    Apply-GeneratedSecret $rocketmqNamespace 'rocketmq-runtime-secrets' @(
        "--from-file=ca.crt=$(Join-Path $certificateRoot 'ca-cert.pem')",
        "--from-file=tls.crt=$(Join-Path $certificateRoot 'server-cert.pem')",
        "--from-file=tls.key=$(Join-Path $certificateRoot 'server-key.pem')",
        "--from-file=admin.identity=$(Join-Path $artifactRoot 'admin.identity')",
        "--from-file=request-policy.json=$(Join-Path $artifactRoot 'request-policy.json')",
        "--from-file=broker-acl.yml=$(Join-Path $artifactRoot 'broker-acl.yml')",
        "--from-file=proxy-acl.yml=$(Join-Path $artifactRoot 'proxy-acl.yml')"
    )
    Apply-GeneratedSecret $rocketmqNamespace 'rocketmq-mcp-runtime-secrets' @(
        "--from-file=ca.crt=$(Join-Path $certificateRoot 'ca-cert.pem')",
        "--from-file=tls.crt=$(Join-Path $certificateRoot 'server-cert.pem')",
        "--from-file=tls.key=$(Join-Path $certificateRoot 'server-key.pem')",
        "--from-file=admin.identity=$(Join-Path $artifactRoot 'admin.identity')",
        "--from-file=request-policy.json=$(Join-Path $artifactRoot 'request-policy.json')",
        "--from-file=mcp-rmq-credentials.yml=$(Join-Path $artifactRoot 'mcp-rmq-credentials.yml')"
    )
    Apply-GeneratedSecret $rocketmqNamespace 'rocketmq-sre-kind-secrets' @(
        "--from-file=mcp-token=$(Join-Path $artifactRoot 'mcp-token')",
        "--from-file=internal-token=$(Join-Path $artifactRoot 'internal-token')",
        "--from-file=probe-access-key=$(Join-Path $artifactRoot 'probe-access-key')",
        "--from-file=probe-secret-key=$(Join-Path $artifactRoot 'probe-secret-key')",
        "--from-file=bootstrap-access-key=$(Join-Path $artifactRoot 'bootstrap-access-key')",
        "--from-file=bootstrap-secret-key=$(Join-Path $artifactRoot 'bootstrap-secret-key')",
        "--from-file=admin-read-access-key=$(Join-Path $artifactRoot 'admin-read-access-key')",
        "--from-file=admin-read-secret-key=$(Join-Path $artifactRoot 'admin-read-secret-key')"
    )
    Apply-GeneratedSecret $sreNamespace 'rocketmq-sre-kind-secrets' @(
        "--from-file=internal-token=$(Join-Path $artifactRoot 'internal-token')",
        "--from-file=agent-read-access-key=$(Join-Path $artifactRoot 'agent-read-access-key')",
        "--from-file=agent-read-secret-key=$(Join-Path $artifactRoot 'agent-read-secret-key')",
        "--from-file=agent-mutation-access-key=$(Join-Path $artifactRoot 'agent-mutation-access-key')",
        "--from-file=agent-mutation-secret-key=$(Join-Path $artifactRoot 'agent-mutation-secret-key')"
    )
    Apply-GeneratedSecret $sreNamespace 'rocketmq-sre-control-plane-channel-server' @(
        "--from-file=control-plane-server-cert.pem=$(Join-Path $certificateRoot 'control-plane-server-cert.pem')",
        "--from-file=control-plane-server-key.pem=$(Join-Path $certificateRoot 'control-plane-server-key.pem')",
        "--from-file=connector-client-ca-cert.pem=$(Join-Path $certificateRoot 'connector-client-ca-cert.pem')"
    )
    Apply-GeneratedSecret $rocketmqNamespace 'rocketmq-sre-control-plane-channel-client' @(
        "--from-file=control-plane-server-ca-cert.pem=$(Join-Path $certificateRoot 'control-plane-server-ca-cert.pem')",
        "--from-file=connector-client-identity.pem=$(Join-Path $certificateRoot 'connector-client-identity.pem')"
    )
    Apply-GeneratedSecret $sreNamespace 'rocketmq-sre-postgres' @(
        "--from-file=postgres-user=$(Join-Path $artifactRoot 'postgres-user')",
        "--from-file=postgres-password=$(Join-Path $artifactRoot 'postgres-password')",
        "--from-file=postgres-db=$(Join-Path $artifactRoot 'postgres-db')",
        "--from-file=database-url=$(Join-Path $artifactRoot 'database-url')"
    )
}

function Wait-Rollout([string]$Namespace, [string]$Workload, [int]$Seconds = 300) {
    Invoke-Kubectl @(
        '--namespace', $Namespace,
        'rollout', 'status', $Workload,
        "--timeout=${Seconds}s"
    ) | Out-Null
}

function Invoke-Smoke {
    Assert-ClusterExists
    Ensure-Kubeconfig
    Require-Command kubectl
    Invoke-Kubectl @(
        '--namespace', $rocketmqNamespace,
        'delete', 'job', 'rocketmq-sre-phase00-smoke',
        '--ignore-not-found=true',
        '--wait=true'
    ) | Out-Null
    Invoke-Kubectl @('apply', '--filename', (Join-Path $kindDirectory 'smoke-job.yaml')) | Out-Null
    $wait = Invoke-Kubectl @(
        '--namespace', $rocketmqNamespace,
        'wait', '--for=condition=complete',
        'job/rocketmq-sre-phase00-smoke',
        '--timeout=180s'
    ) -AllowFailure
    $logs = Invoke-Kubectl @(
        '--namespace', $rocketmqNamespace,
        'logs', 'job/rocketmq-sre-phase00-smoke'
    ) -AllowFailure
    if (-not [string]::IsNullOrWhiteSpace($logs.Output)) {
        Write-Host $logs.Output
    }
    if ($wait.ExitCode -ne 0) {
        $describe = Invoke-Kubectl @(
            '--namespace', $rocketmqNamespace,
            'describe', 'job/rocketmq-sre-phase00-smoke'
        ) -AllowFailure
        throw "Kind smoke failed.`n$($wait.Output)`n$($describe.Output)"
    }
}

switch ($Action) {
    'Down' {
        Require-Command kind
        if (Test-ClusterExists) {
            Ensure-Kubeconfig
            Invoke-Native kind @(
                'delete', 'cluster',
                '--name', $ClusterName,
                '--kubeconfig', $kubeconfigPath
            ) | Out-Null
            Write-Host "Deleted Kind cluster '$ClusterName'. Generated local fixtures remain under $artifactRoot."
        }
        else {
            Write-Host "Kind cluster '$ClusterName' does not exist."
        }
    }
    'Status' {
        Assert-ClusterExists
        Ensure-Kubeconfig
        Require-Command kubectl
        Invoke-Kubectl @('get', 'nodes', '--output=wide') | ForEach-Object { Write-Host $_.Output }
        foreach ($namespace in @($rocketmqNamespace, $sreNamespace, $observabilityNamespace)) {
            Write-Host "`n[$namespace]"
            Invoke-Kubectl @('--namespace', $namespace, 'get', 'pods', '--output=wide') |
                ForEach-Object { Write-Host $_.Output }
        }
    }
    'Smoke' {
        Invoke-Smoke
    }
    'Up' {
        Assert-PinnedTools
        Assert-ArtifactRoot
        New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null

        $renderedKind = (Invoke-Native kubectl @('kustomize', $kindDirectory)).Output
        Write-Utf8File (Join-Path $artifactRoot 'kind-rendered.yaml') $renderedKind
        Invoke-Native helm @(
            'lint', $chartPath, '--strict',
            '--values', $devValues,
            '--values', $kindValues
        ) | Out-Null
        $renderedHelm = (Invoke-Native helm @(
            'template', 'rocketmq', $chartPath,
            '--namespace', $rocketmqNamespace,
            '--values', $devValues,
            '--values', $kindValues
        )).Output
        Write-Utf8File (Join-Path $artifactRoot 'helm-rendered.yaml') $renderedHelm

        if (-not $SkipBuild) {
            Build-Images
        }
        Assert-ImagesExist

        $existingCluster = Test-ClusterExists
        if (-not $existingCluster) {
            Invoke-Native kind @(
                'create', 'cluster',
                '--name', $ClusterName,
                '--image', $policy.cluster.kind_node_image,
                '--config', $clusterConfig,
                '--kubeconfig', $kubeconfigPath,
                '--wait', '180s'
            ) | Out-Null
        }
        else {
            Write-Host "Reusing existing Kind cluster '$ClusterName'."
        }
        Ensure-Kubeconfig
        Invoke-Kubectl @('wait', '--for=condition=Ready', 'nodes', '--all', '--timeout=120s') | Out-Null
        Load-Images

        Invoke-Kubectl @('apply', '--filename', (Join-Path $kindDirectory 'namespaces.yaml')) | Out-Null
        New-SecretMaterial -ExistingCluster:$existingCluster
        Apply-Secrets
        Invoke-Kubectl @(
            '--namespace', $sreNamespace,
            'delete', 'job', 'sre-onboarding-bootstrap',
            '--ignore-not-found=true',
            '--wait=true'
        ) | Out-Null
        Invoke-Kubectl @('apply', '--kustomize', $kindDirectory) | Out-Null

        Wait-Rollout $sreNamespace 'statefulset/postgres'
        Wait-Rollout $sreNamespace 'deployment/sre-model-mock'
        Wait-Rollout $sreNamespace 'deployment/sre-control-plane'
        Wait-Rollout $sreNamespace 'deployment/sre-execution-agent'
        Wait-Rollout $sreNamespace 'deployment/sre-executor'
        Invoke-Kubectl @(
            '--namespace', $sreNamespace,
            'wait', '--for=condition=complete',
            'job/sre-onboarding-bootstrap',
            '--timeout=180s'
        ) | Out-Null
        Wait-Rollout $sreNamespace 'deployment/sre-ui'
        foreach ($component in @('otel-collector', 'prometheus', 'loki', 'tempo')) {
            Wait-Rollout $observabilityNamespace "deployment/$component"
        }

        Invoke-Native helm @(
            'upgrade', '--install', 'rocketmq', $chartPath,
            '--kubeconfig', $kubeconfigPath,
            '--kube-context', $kubeContext,
            '--namespace', $rocketmqNamespace,
            '--create-namespace',
            '--values', $devValues,
            '--values', $kindValues,
            '--force-conflicts',
            '--wait=hookOnly'
        ) | Out-Null
        Invoke-Kubectl @('apply', '--filename', (Join-Path $kindDirectory 'mcp-config.yaml')) | Out-Null
        Invoke-Kubectl @(
            '--namespace', $rocketmqNamespace,
            'patch', 'deployment', 'rocketmq-mcp',
            '--type', 'strategic',
            '--patch-file', (Join-Path $kindDirectory 'mcp-connector-patch.yaml')
        ) | Out-Null

        foreach ($component in @('broker', 'namesrv', 'controller')) {
            Wait-Rollout $rocketmqNamespace "statefulset/rocketmq-$component" 600
        }
        foreach ($component in @('proxy', 'mcp')) {
            Wait-Rollout $rocketmqNamespace "deployment/rocketmq-$component" 600
        }
        Invoke-Smoke
        Write-Host "Phase 00 Kind stack is ready in context '$kubeContext'."
    }
}

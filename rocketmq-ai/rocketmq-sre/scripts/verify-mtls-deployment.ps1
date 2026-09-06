# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [switch]$CheckCertificates
)

$ErrorActionPreference = 'Stop'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$composeDirectory = Join-Path $sreRoot 'deploy/dev'
$composeFile = Join-Path $composeDirectory 'compose.yaml'
$kindDirectory = Join-Path $sreRoot 'deploy/kind'
$targetDirectory = [IO.Path]::GetFullPath((Join-Path $repositoryRoot 'target'))
$certificateDirectory = [IO.Path]::GetFullPath((Join-Path $targetDirectory 'phase00-certs'))
$opensslImage = 'alpine/openssl:3.5.2@sha256:ef8657028239a006f3de0bd04529e22c073bf0ab6655ece9f25c8dde9adec146'
$validationEnvironmentFixtures = [ordered]@{
    'admin-read.env' = @(
        'ROCKETMQ_SRE_ADMIN_ACCESS_KEY=validation-only-denied'
        'ROCKETMQ_SRE_ADMIN_SECRET_KEY=validation-only-denied'
        ''
    ) -join "`n"
    'agent-broker.env' = @(
        'ROCKETMQ_SRE_AGENT_BROKER_READ_ACCESS_KEY=validation-only-denied'
        'ROCKETMQ_SRE_AGENT_BROKER_READ_SECRET_KEY=validation-only-denied'
        'ROCKETMQ_SRE_AGENT_BROKER_MUTATION_ACCESS_KEY=validation-only-denied'
        'ROCKETMQ_SRE_AGENT_BROKER_MUTATION_SECRET_KEY=validation-only-denied'
        ''
    ) -join "`n"
    'probe.env' = @(
        'ROCKETMQ_SRE_PROBE_ACCESS_KEY=validation-only-denied'
        'ROCKETMQ_SRE_PROBE_SECRET_KEY_FILE=/validation-only/missing'
        ''
    ) -join "`n"
    'bootstrap.env' = @(
        'ROCKETMQ_ACL_ACCESS_KEY=validation-only-denied'
        'ROCKETMQ_ACL_SECRET_KEY=validation-only-denied'
        ''
    ) -join "`n"
}

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found."
    }
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Command failed with exit code $LASTEXITCODE."
    }
}

function Assert-Contains([string]$Path, [string]$Pattern, [string]$Description) {
    $content = Get-Content -Raw -LiteralPath $Path
    if ($content -notmatch $Pattern) {
        throw "$Description is missing from $Path."
    }
}

function Assert-NotContains([string]$Path, [string]$Pattern, [string]$Description) {
    $content = Get-Content -Raw -LiteralPath $Path
    if ($content -match $Pattern) {
        throw "$Description is forbidden in $Path."
    }
}

function New-ValidationEnvironmentFixtureScope {
    $targetPrefix = $targetDirectory + [IO.Path]::DirectorySeparatorChar
    if (-not $certificateDirectory.StartsWith($targetPrefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Validation fixture output escaped the repository target directory.'
    }

    $directoryCreated = $false
    if (-not (Test-Path -LiteralPath $certificateDirectory)) {
        New-Item -ItemType Directory -Path $certificateDirectory | Out-Null
        $directoryCreated = $true
    }
    elseif (-not (Test-Path -LiteralPath $certificateDirectory -PathType Container)) {
        throw "Validation fixture directory is not a directory: $certificateDirectory"
    }

    $createdFiles = [Collections.Generic.List[string]]::new()
    try {
        foreach ($fixture in $validationEnvironmentFixtures.GetEnumerator()) {
            $path = Join-Path $certificateDirectory $fixture.Key
            if (Test-Path -LiteralPath $path) {
                if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
                    throw "Validation fixture path is not a file: $path"
                }
                continue
            }
            [IO.File]::WriteAllText($path, $fixture.Value, [Text.UTF8Encoding]::new($false))
            $createdFiles.Add($path)
        }
    }
    catch {
        foreach ($path in $createdFiles) {
            if (Test-Path -LiteralPath $path -PathType Leaf) {
                Remove-Item -LiteralPath $path -Force
            }
        }
        if ($directoryCreated -and
            -not (Get-ChildItem -LiteralPath $certificateDirectory -Force | Select-Object -First 1)) {
            Remove-Item -LiteralPath $certificateDirectory -Force
        }
        throw
    }

    [pscustomobject]@{
        CreatedFiles = $createdFiles
        DirectoryCreated = $directoryCreated
    }
}

function Remove-ValidationEnvironmentFixtureScope([pscustomobject]$Scope) {
    foreach ($path in $Scope.CreatedFiles) {
        if (Test-Path -LiteralPath $path -PathType Leaf) {
            Remove-Item -LiteralPath $path -Force
        }
    }
    if ($Scope.DirectoryCreated -and
        (Test-Path -LiteralPath $certificateDirectory -PathType Container) -and
        -not (Get-ChildItem -LiteralPath $certificateDirectory -Force | Select-Object -First 1)) {
        Remove-Item -LiteralPath $certificateDirectory -Force
    }
}

$composeProxy = Join-Path $composeDirectory 'control-plane-mtls-nginx.conf'
$kindProxy = Join-Path $kindDirectory 'control-plane-mtls-nginx.conf'
$kindConnectorPatch = Join-Path $kindDirectory 'mcp-connector-patch.yaml'
$kindConnectorRbac = Join-Path $kindDirectory 'connector-rbac.yaml'
$kindStack = Join-Path $kindDirectory 'sre-stack.yaml'
$kindNetworkPolicy = Join-Path $kindDirectory 'control-plane-network-policy.yaml'
$kindExecutionStack = Join-Path $kindDirectory 'execution-stack.yaml'
$kindExecutionRbac = Join-Path $kindDirectory 'execution-rbac.yaml'
$kindExecutionNetworkPolicy = Join-Path $kindDirectory 'execution-network-policy.yaml'
$devRunner = Join-Path $scriptDirectory 'dev.ps1'
$kindRunner = Join-Path $scriptDirectory 'kind.ps1'

foreach ($proxyConfig in @($composeProxy, $kindProxy)) {
    Assert-Contains $proxyConfig 'ssl_verify_client\s+on;' 'mandatory client-certificate verification'
    Assert-Contains $proxyConfig 'ssl_protocols\s+TLSv1\.2\s+TLSv1\.3;' 'bounded TLS protocol policy'
    Assert-Contains $proxyConfig 'client_max_body_size\s+640k;' 'bounded request body policy'
    Assert-Contains $proxyConfig 'proxy_redirect\s+off;' 'redirect suppression'
    Assert-Contains $proxyConfig 'location\s+\^~\s+/internal/v1/connectors/v1/' 'Connector-only route prefix'
    Assert-Contains $proxyConfig 'limit_except\s+GET\s+POST' 'bounded Connector channel methods'
    Assert-Contains $proxyConfig 'X-RocketMQ-Connector-Subject\s+\$ssl_client_s_dn;' 'certificate-derived connector subject'
    Assert-Contains $proxyConfig 'X-RocketMQ-Connector-Issuer\s+\$ssl_client_i_dn;' 'certificate-derived connector issuer'
}

Assert-Contains $composeFile 'ROCKETMQ_SRE_CONTROL_PLANE_URL:\s+https://sre-control-plane:8444' 'Compose HTTPS channel URL'
Assert-Contains $composeFile 'ROCKETMQ_SRE_CONTROL_PLANE_CLIENT_IDENTITY_PATH:' 'Compose client identity'
Assert-Contains $composeFile 'ROCKETMQ_SRE_ADMIN_NAMESRV_ADDR:\s+namesrv:9876' 'Compose read-only Admin source'
Assert-Contains $composeFile 'ROCKETMQ_SRE_ADMIN_ACCESS_KEY_ENV:\s+ROCKETMQ_SRE_ADMIN_ACCESS_KEY' 'Compose Admin access-key reference'
Assert-Contains $composeFile 'ROCKETMQ_SRE_ADMIN_SECRET_KEY_ENV:\s+ROCKETMQ_SRE_ADMIN_SECRET_KEY' 'Compose Admin secret-key reference'
Assert-Contains $composeFile 'env_file:\s*\r?\n\s+- .*admin-read\.env' 'Compose isolated Admin credential fixture'
Assert-Contains $composeFile 'ROCKETMQ_SRE_CONNECTOR_BIND_ADDR:\s+127\.0\.0\.1:8093' 'Compose loopback Connector listener'
Assert-Contains $composeFile 'network_mode:\s+"service:sre-control-plane"' 'Compose shared network namespace'
Assert-Contains $composeFile 'control-plane-backend:\s*\r?\n\s+name:.*\r?\n\s+internal:\s+true' 'Compose backend network isolation'
Assert-Contains $composeFile 'execution-control:\s*\r?\n\s+name:.*\r?\n\s+internal:\s+true' 'Compose Control Plane to Executor isolation'
Assert-Contains $composeFile 'executor-agent:\s*\r?\n\s+name:.*\r?\n\s+internal:\s+true' 'Compose Executor to Agent isolation'
Assert-Contains $composeFile 'ROCKETMQ_SRE_EXECUTOR_URL:\s+http://sre-executor:8094' 'Compose isolated Executor URL'
Assert-Contains $composeFile 'ROCKETMQ_SRE_EXECUTION_AGENT_URL:\s+http://sre-execution-agent:8095' 'Compose isolated Agent URL'
Assert-Contains $composeFile 'ROCKETMQ_SRE_AGENT_ACK_KEY:' 'Compose Agent fence acknowledgement key'
Assert-Contains $composeFile 'ROCKETMQ_SRE_AGENT_ENABLE_BROKER_CONFIG:\s+"true"' 'Compose explicit Broker driver enablement'
Assert-Contains $composeFile 'ROCKETMQ_SRE_AGENT_NAMESRV_ADDR:\s+namesrv:9876' 'Compose Agent Broker target'
Assert-Contains $composeFile 'env_file:\s*\r?\n\s+- .*agent-broker\.env' 'Compose isolated Agent Broker identities'
Assert-NotContains $composeFile 'ROCKETMQ_SRE_CONTROL_PLANE_URL:\s+http://' 'plain HTTP connector channel'
Assert-Contains $composeProxy 'proxy_pass\s+http://127\.0\.0\.1:8093;' 'Compose proxy loopback listener upstream'
Assert-Contains $devRunner "phase03-compose-agent-read" 'Compose dedicated Agent read identity'
Assert-Contains $devRunner "phase03-compose-agent-mutation" 'Compose dedicated Agent mutation identity'

Assert-Contains $kindConnectorPatch 'ROCKETMQ_SRE_CONTROL_PLANE_URL,\s+value:\s+"https://' 'Kind HTTPS channel URL'
Assert-Contains $kindConnectorPatch 'ROCKETMQ_SRE_CONTROL_PLANE_CLIENT_IDENTITY_PATH' 'Kind client identity'
Assert-Contains $kindConnectorPatch 'serviceAccountName:\s+rocketmq-sre-connector' 'Kind dedicated Connector ServiceAccount'
Assert-Contains $kindConnectorPatch 'automountServiceAccountToken:\s+false' 'Kind default token mount suppression'
Assert-Contains $kindConnectorPatch 'name:\s+mcp-server-ca\s*\r?\n\s+secret:' 'Kind minimal MCP CA projection'
Assert-Contains $kindConnectorPatch 'items:\s*\r?\n\s+- \{key:\s*ca\.crt,\s*path:\s*ca\.crt\}' 'Kind MCP CA-only Secret items'
Assert-Contains $kindConnectorPatch 'serviceAccountToken:\s*\r?\n\s+path:\s+token\s*\r?\n\s+expirationSeconds:\s+3600' 'Kind rotating projected ServiceAccount token'
Assert-Contains $kindConnectorPatch 'name:\s+kube-root-ca\.crt' 'Kind projected Kubernetes CA'
Assert-Contains $kindConnectorPatch 'ROCKETMQ_SRE_KUBERNETES_TOKEN_PATH' 'Kind projected-token path'
Assert-Contains $kindConnectorPatch 'ROCKETMQ_SRE_KUBERNETES_CA_PATH' 'Kind Kubernetes CA path'
Assert-NotContains $kindConnectorPatch 'ROCKETMQ_SRE_KUBERNETES_TOKEN_ENV' 'Kind static Kubernetes token'
Assert-Contains $kindConnectorPatch 'ROCKETMQ_SRE_ADMIN_NAMESRV_ADDR,\s+value:\s+"rocketmq-namesrv:9876"' 'Kind read-only Admin source'
Assert-Contains $kindConnectorPatch 'key:\s+admin-read-access-key' 'Kind Admin access-key Secret item'
Assert-Contains $kindConnectorPatch 'key:\s+admin-read-secret-key' 'Kind Admin secret-key Secret item'
Assert-NotContains $kindConnectorPatch 'ROCKETMQ_SRE_CONTROL_PLANE_URL,\s+value:\s+"http://' 'plain HTTP Kind channel'
Assert-Contains $kindStack 'name:\s+connector-mtls-proxy' 'Kind mTLS sidecar'
Assert-Contains $kindStack 'ROCKETMQ_SRE_CONNECTOR_BIND_ADDR,\s+value:\s+"127\.0\.0\.1:8093"' 'Kind loopback Connector listener'
Assert-NotContains $kindStack 'targetPort:\s+connector-api' 'Connector-only listener in the Kind Service'
Assert-Contains $kindProxy 'proxy_pass\s+http://127\.0\.0\.1:8093;' 'Kind proxy loopback listener upstream'
Assert-Contains $kindNetworkPolicy 'rocketmqrust\.com/sre-connector:' 'Kind connector-only ingress policy'
Assert-Contains $kindConnectorRbac 'kind:\s+ServiceAccount\s*\r?\nmetadata:\s*\r?\n\s+name:\s+rocketmq-sre-connector' 'Kind Connector ServiceAccount'
Assert-Contains $kindConnectorRbac 'resources:\s+\["pods",\s+"events",\s+"persistentvolumeclaims"\]' 'Kind namespaced metadata allowlist'
Assert-Contains $kindConnectorRbac 'resources:\s+\["poddisruptionbudgets"\]' 'Kind PDB allowlist'
Assert-Contains $kindConnectorRbac 'resources:\s+\["nodes"\]' 'Kind Node allowlist'
Assert-NotContains $kindConnectorRbac 'verbs:\s*\[[^\]]*(create|update|patch|delete|watch)' 'Kind RBAC mutation or watch verb'
Assert-Contains $kindExecutionStack 'name:\s+sre-executor\s*\r?\n\s+namespace:\s+rocketmq-sre\s*\r?\nautomountServiceAccountToken:\s+false' 'Kind tokenless Executor identity'
Assert-Contains $kindExecutionStack 'name:\s+sre-execution-agent\s*\r?\n\s+namespace:\s+rocketmq-sre\s*\r?\nautomountServiceAccountToken:\s+true' 'Kind Agent target identity'
Assert-Contains $kindExecutionStack 'serviceAccountName:\s+sre-executor' 'Kind dedicated Executor ServiceAccount'
Assert-Contains $kindExecutionStack 'serviceAccountName:\s+sre-execution-agent' 'Kind dedicated Agent ServiceAccount'
Assert-Contains $kindExecutionStack 'ROCKETMQ_SRE_AGENT_ENABLE_BROKER_CONFIG,\s+value:\s+"true"' 'Kind explicit Broker driver enablement'
Assert-Contains $kindExecutionStack 'ROCKETMQ_SRE_AGENT_NAMESRV_ADDR,\s+value:\s+"rocketmq-namesrv\.rocketmq-system\.svc\.cluster\.local:9876"' 'Kind Agent Broker target'
Assert-Contains $kindExecutionStack 'key:\s+agent-read-access-key' 'Kind Agent read identity'
Assert-Contains $kindExecutionStack 'key:\s+agent-mutation-access-key' 'Kind Agent mutation identity'
Assert-Contains $kindExecutionRbac 'name:\s+sre-execution-agent\s*\r?\n\s+namespace:\s+rocketmq-sre' 'Kind Agent-only mutation role binding'
Assert-NotContains $kindExecutionRbac 'name:\s+sre-executor' 'Kind Executor target role binding'
Assert-Contains $kindExecutionNetworkPolicy 'name:\s+sre-executor-isolation' 'Kind Executor network boundary'
Assert-Contains $kindExecutionNetworkPolicy 'name:\s+sre-execution-agent-isolation' 'Kind Agent network boundary'
Assert-Contains $kindRunner "phase03-kind-agent-read" 'Kind dedicated Agent read identity'
Assert-Contains $kindRunner "phase03-kind-agent-mutation" 'Kind dedicated Agent mutation identity'

$kindPatchContent = Get-Content -Raw -LiteralPath $kindConnectorPatch
$connectorContainer = [regex]::Match(
    $kindPatchContent,
    '(?ms)^\s*-\s+name:\s+connector\s*$.*\z'
).Value
if ([string]::IsNullOrWhiteSpace($connectorContainer)) {
    throw "Kind Connector container patch could not be isolated in $kindConnectorPatch."
}
if ($connectorContainer -match '(?m)^\s*-\s+\{?name:\s*runtime-secrets(?:,|\})') {
    throw 'Kind Connector container must not mount the MCP runtime Secret.'
}

$validationFixtureScope = $null
try {
    $validationFixtureScope = New-ValidationEnvironmentFixtureScope

    Require-Command docker
    Invoke-Native docker @(
        'compose',
        '--project-directory', $composeDirectory,
        '--file', $composeFile,
        '--profile', 'observability',
        'config', '--quiet'
    )

    Require-Command kubectl
    $renderedKind = & kubectl kustomize $kindDirectory
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl kustomize failed with exit code $LASTEXITCODE."
    }
    $renderedText = $renderedKind -join "`n"
    foreach ($requiredRender in @(
        'name: sre-control-plane-mtls-proxy',
        'name: connector-mtls-proxy',
        'name: rocketmq-sre-control-plane-channel-server',
        'name: rocketmq-sre-connector',
        'name: rocketmq-sre-connector-read',
        'name: rocketmq-sre-connector-node-read',
        'name: sre-executor',
        'name: sre-execution-agent',
        'name: sre-executor-isolation',
        'name: sre-execution-agent-isolation',
        'port: 8444',
        'kind: NetworkPolicy'
    )) {
        if ($renderedText -notmatch [regex]::Escape($requiredRender)) {
            throw "Rendered Kind assets do not contain '$requiredRender'."
        }
    }

    if ($CheckCertificates) {
        $requiredCertificates = @(
            'control-plane-server-ca-cert.pem',
            'control-plane-server-cert.pem',
            'connector-client-ca-cert.pem',
            'connector-client-cert.pem',
            'connector-client-identity.pem'
        )
        $missing = @(
            $requiredCertificates |
                Where-Object { -not (Test-Path -LiteralPath (Join-Path $certificateDirectory $_) -PathType Leaf) }
        )
        if ($missing.Count -gt 0) {
            throw "Generated mTLS material is incomplete: $($missing -join ', '). Run dev.ps1 -Action Certs."
        }

        $mount = "${certificateDirectory}:/certs:ro"
        Invoke-Native docker @(
            'run', '--rm', '--volume', $mount, $opensslImage,
            'verify', '-CAfile', '/certs/control-plane-server-ca-cert.pem',
            '-purpose', 'sslserver', '/certs/control-plane-server-cert.pem'
        )
        Invoke-Native docker @(
            'run', '--rm', '--volume', $mount, $opensslImage,
            'verify', '-CAfile', '/certs/connector-client-ca-cert.pem',
            '-purpose', 'sslclient', '/certs/connector-client-cert.pem'
        )
        Invoke-Native docker @(
            'run', '--rm', '--volume', $mount, $opensslImage,
            'x509', '-in', '/certs/control-plane-server-cert.pem',
            '-noout', '-checkhost', 'sre-control-plane.rocketmq-sre.svc.cluster.local'
        )
        Invoke-Native docker @(
            'run', '--rm', '--volume', $mount, $opensslImage,
            'pkey', '-in', '/certs/connector-client-identity.pem', '-check', '-noout'
        )
        foreach ($proxyConfig in @($composeProxy, $kindProxy)) {
            Invoke-Native docker @(
                'run', '--rm',
                '--user', '10001:10001',
                '--read-only',
                '--tmpfs', '/tmp:size=16m,mode=1777',
                '--cap-drop', 'ALL',
                '--security-opt', 'no-new-privileges:true',
                '--volume', "${proxyConfig}:/etc/nginx/nginx.conf:ro",
                '--volume', "$(Join-Path $certificateDirectory 'control-plane-server-cert.pem'):/etc/rocketmq/sre-channel/control-plane-server-cert.pem:ro",
                '--volume', "$(Join-Path $certificateDirectory 'control-plane-server-key.pem'):/etc/rocketmq/sre-channel/control-plane-server-key.pem:ro",
                '--volume', "$(Join-Path $certificateDirectory 'connector-client-ca-cert.pem'):/etc/rocketmq/sre-channel/connector-client-ca-cert.pem:ro",
                '--entrypoint', 'nginx',
                'nginx:1.29-alpine',
                '-t', '-c', '/etc/nginx/nginx.conf'
            )
        }
    }
}
finally {
    if ($null -ne $validationFixtureScope) {
        Remove-ValidationEnvironmentFixtureScope $validationFixtureScope
    }
}

Write-Host 'Control Plane to Connector mTLS deployment contract is valid.'

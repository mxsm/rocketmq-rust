# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Up', 'Down', 'Status', 'Certs', 'ChannelCerts', 'Reset')]
    [string]$Action,

    [switch]$Force
)

$ErrorActionPreference = 'Stop'
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '..'))
$composeDirectory = Join-Path $sreRoot 'deploy/dev'
$composeFile = Join-Path $composeDirectory 'compose.yaml'
$certificateDirectory = [IO.Path]::GetFullPath((Join-Path $repositoryRoot 'target/phase00-certs'))
$expectedCertificateRoot = [IO.Path]::GetFullPath((Join-Path $repositoryRoot 'target'))
$opensslImage = 'alpine/openssl:3.5.2@sha256:ef8657028239a006f3de0bd04529e22c073bf0ab6655ece9f25c8dde9adec146'
$requiredBaseDevelopmentMaterial = @(
    'ca-cert.pem',
    'server-cert.pem',
    'server-key.pem',
    'admin.identity',
    'request-policy.json',
    'broker-acl.yml',
    'mcp-rmq-credentials.yml',
    'admin-read.env',
    'probe-secret-key',
    'probe.env',
    'bootstrap.env'
)
$requiredControlPlaneChannelMaterial = @(
    'control-plane-server-ca-cert.pem',
    'control-plane-server-cert.pem',
    'control-plane-server-key.pem',
    'connector-client-ca-cert.pem',
    'connector-client-cert.pem',
    'connector-client-identity.pem'
)
function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found."
    }
}

function Invoke-Docker([string[]]$Arguments) {
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

function Assert-CertificateDirectory {
    if (-not $certificateDirectory.StartsWith(
        $expectedCertificateRoot + [IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Certificate output escaped the repository target directory."
    }
}

function Ensure-AdminReadEnvironmentFixture {
    $environmentPath = Join-Path $certificateDirectory 'admin-read.env'
    if (Test-Path -LiteralPath $environmentPath -PathType Leaf) {
        return
    }
    $credentialsPath = Join-Path $certificateDirectory 'mcp-rmq-credentials.yml'
    if (-not (Test-Path -LiteralPath $credentialsPath -PathType Leaf)) {
        return
    }
    $credentials = Get-Content -Raw -LiteralPath $credentialsPath
    $accessKey = [regex]::Match($credentials, '(?m)^access_key:\s*([^\s]+)\s*$')
    $secretKey = [regex]::Match($credentials, '(?m)^secret_key:\s*([^\s]+)\s*$')
    if (-not $accessKey.Success -or -not $secretKey.Success) {
        throw 'The MCP reader credential file is malformed and cannot seed the read-only Admin source.'
    }
    [IO.File]::WriteAllText(
        $environmentPath,
        "ROCKETMQ_SRE_ADMIN_ACCESS_KEY=$($accessKey.Groups[1].Value)`nROCKETMQ_SRE_ADMIN_SECRET_KEY=$($secretKey.Groups[1].Value)`n",
        [Text.UTF8Encoding]::new($false)
    )
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

function New-ControlPlaneChannelCertificates {
    Assert-CertificateDirectory
    New-Item -ItemType Directory -Force -Path $certificateDirectory | Out-Null
    foreach ($channelFile in @(
        'control-plane-server-ca-key.pem',
        'control-plane-server-ca-cert.pem',
        'control-plane-server-key.pem',
        'control-plane-server-cert.pem',
        'connector-client-ca-key.pem',
        'connector-client-ca-cert.pem',
        'connector-client-key.pem',
        'connector-client-cert.pem',
        'connector-client-identity.pem',
        'control-plane-server.csr',
        'control-plane-server.ext',
        'control-plane-server-ca-cert.srl',
        'connector-client.csr',
        'connector-client.ext',
        'connector-client-ca-cert.srl'
    )) {
        $channelPath = Join-Path $certificateDirectory $channelFile
        if (Test-Path -LiteralPath $channelPath -PathType Leaf) {
            Remove-Item -LiteralPath $channelPath -Force
        }
    }

    $mount = "${certificateDirectory}:/certs"
    $base = @('run', '--rm', '--volume', $mount, $opensslImage)
    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'control-plane-server.ext'),
        "basicConstraints=critical,CA:FALSE`nkeyUsage=critical,digitalSignature,keyEncipherment`nextendedKeyUsage=serverAuth`nsubjectAltName=DNS:sre-control-plane,DNS:sre-control-plane-mtls,DNS:sre-control-plane.rocketmq-sre,DNS:sre-control-plane.rocketmq-sre.svc,DNS:sre-control-plane.rocketmq-sre.svc.cluster.local,DNS:localhost,IP:127.0.0.1`n",
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker ($base + @('genrsa', '-out', '/certs/control-plane-server-ca-key.pem', '3072'))
    Invoke-Docker ($base + @(
        'req', '-x509', '-new', '-sha256',
        '-key', '/certs/control-plane-server-ca-key.pem',
        '-days', '7',
        '-subj', '/CN=RocketMQ SRE Control Plane Development CA',
        '-out', '/certs/control-plane-server-ca-cert.pem'
    ))
    Invoke-Docker ($base + @('genrsa', '-out', '/certs/control-plane-server-key.pem', '3072'))
    Invoke-Docker ($base + @(
        'req', '-new', '-sha256',
        '-key', '/certs/control-plane-server-key.pem',
        '-subj', '/CN=sre-control-plane',
        '-out', '/certs/control-plane-server.csr'
    ))
    Invoke-Docker ($base + @(
        'x509', '-req', '-sha256',
        '-in', '/certs/control-plane-server.csr',
        '-CA', '/certs/control-plane-server-ca-cert.pem',
        '-CAkey', '/certs/control-plane-server-ca-key.pem',
        '-CAcreateserial',
        '-days', '7',
        '-extfile', '/certs/control-plane-server.ext',
        '-out', '/certs/control-plane-server-cert.pem'
    ))

    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'connector-client.ext'),
        "basicConstraints=critical,CA:FALSE`nkeyUsage=critical,digitalSignature,keyEncipherment`nextendedKeyUsage=clientAuth`n",
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker ($base + @('genrsa', '-out', '/certs/connector-client-ca-key.pem', '3072'))
    Invoke-Docker ($base + @(
        'req', '-x509', '-new', '-sha256',
        '-key', '/certs/connector-client-ca-key.pem',
        '-days', '7',
        '-subj', '/CN=RocketMQ SRE Connector Development CA',
        '-out', '/certs/connector-client-ca-cert.pem'
    ))
    Invoke-Docker ($base + @('genrsa', '-out', '/certs/connector-client-key.pem', '3072'))
    Invoke-Docker ($base + @(
        'req', '-new', '-sha256',
        '-key', '/certs/connector-client-key.pem',
        '-subj', '/CN=rocketmq-sre-connector',
        '-out', '/certs/connector-client.csr'
    ))
    Invoke-Docker ($base + @(
        'x509', '-req', '-sha256',
        '-in', '/certs/connector-client.csr',
        '-CA', '/certs/connector-client-ca-cert.pem',
        '-CAkey', '/certs/connector-client-ca-key.pem',
        '-CAcreateserial',
        '-days', '7',
        '-extfile', '/certs/connector-client.ext',
        '-out', '/certs/connector-client-cert.pem'
    ))
    Invoke-Docker @(
        'run', '--rm',
        '--volume', $mount,
        '--entrypoint', '/bin/chmod',
        $opensslImage,
        '0644',
        '/certs/connector-client-cert.pem',
        '/certs/connector-client-key.pem'
    )
    $clientIdentity = [IO.File]::ReadAllText(
        (Join-Path $certificateDirectory 'connector-client-cert.pem')
    ) + [IO.File]::ReadAllText(
        (Join-Path $certificateDirectory 'connector-client-key.pem')
    )
    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'connector-client-identity.pem'),
        $clientIdentity,
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker @(
        'run', '--rm',
        '--volume', $mount,
        '--entrypoint', '/bin/sh',
        $opensslImage,
        '-ec',
        'chown 10001:10001 /certs/control-plane-server-ca-cert.pem /certs/control-plane-server-cert.pem /certs/control-plane-server-key.pem /certs/connector-client-ca-cert.pem /certs/connector-client-cert.pem /certs/connector-client-key.pem /certs/connector-client-identity.pem; chmod 0444 /certs/control-plane-server-ca-cert.pem /certs/control-plane-server-cert.pem /certs/connector-client-ca-cert.pem /certs/connector-client-cert.pem; chmod 0400 /certs/control-plane-server-key.pem /certs/connector-client-key.pem /certs/connector-client-identity.pem'
    )
    foreach ($temporaryFile in @(
        'control-plane-server.csr',
        'control-plane-server.ext',
        'control-plane-server-ca-cert.srl',
        'connector-client.csr',
        'connector-client.ext',
        'connector-client-ca-cert.srl'
    )) {
        $temporaryPath = Join-Path $certificateDirectory $temporaryFile
        if (Test-Path -LiteralPath $temporaryPath -PathType Leaf) {
            Remove-Item -LiteralPath $temporaryPath -Force
        }
    }
    Write-Host "Control Plane connector-channel mTLS fixtures written under $certificateDirectory"
}

function New-DevelopmentCertificates {
    Assert-CertificateDirectory
    New-Item -ItemType Directory -Force -Path $certificateDirectory | Out-Null
    Get-ChildItem -LiteralPath $certificateDirectory -File -ErrorAction SilentlyContinue |
        Remove-Item -Force

    $extensionFile = Join-Path $certificateDirectory 'server.ext'
    [IO.File]::WriteAllText(
        $extensionFile,
        "subjectAltName=DNS:mcp,DNS:rocketmq-mcp,DNS:dev-issuer-tls,DNS:localhost,IP:127.0.0.1`nextendedKeyUsage=serverAuth`n",
        [Text.UTF8Encoding]::new($false)
    )
    $mount = "${certificateDirectory}:/certs"
    $base = @('run', '--rm', '--volume', $mount, $opensslImage)

    Invoke-Docker ($base + @('genrsa', '-out', '/certs/ca-key.pem', '2048'))
    Invoke-Docker ($base + @(
        'req', '-x509', '-new', '-sha256',
        '-key', '/certs/ca-key.pem',
        '-days', '7',
        '-subj', '/CN=RocketMQ SRE Phase00 Development CA',
        '-out', '/certs/ca-cert.pem'
    ))
    Invoke-Docker ($base + @('genrsa', '-out', '/certs/server-key.pem', '2048'))
    Invoke-Docker ($base + @(
        'req', '-new',
        '-key', '/certs/server-key.pem',
        '-subj', '/CN=mcp',
        '-out', '/certs/server.csr'
    ))
    Invoke-Docker ($base + @(
        'x509', '-req', '-sha256',
        '-in', '/certs/server.csr',
        '-CA', '/certs/ca-cert.pem',
        '-CAkey', '/certs/ca-key.pem',
        '-CAcreateserial',
        '-days', '7',
        '-extfile', '/certs/server.ext',
        '-out', '/certs/server-cert.pem'
    ))

    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'control-plane-server.ext'),
        "basicConstraints=critical,CA:FALSE`nkeyUsage=critical,digitalSignature,keyEncipherment`nextendedKeyUsage=serverAuth`nsubjectAltName=DNS:sre-control-plane,DNS:sre-control-plane-mtls,DNS:sre-control-plane.rocketmq-sre,DNS:sre-control-plane.rocketmq-sre.svc,DNS:sre-control-plane.rocketmq-sre.svc.cluster.local,DNS:localhost,IP:127.0.0.1`n",
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker ($base + @('genrsa', '-out', '/certs/control-plane-server-ca-key.pem', '3072'))
    Invoke-Docker ($base + @(
        'req', '-x509', '-new', '-sha256',
        '-key', '/certs/control-plane-server-ca-key.pem',
        '-days', '7',
        '-subj', '/CN=RocketMQ SRE Control Plane Development CA',
        '-out', '/certs/control-plane-server-ca-cert.pem'
    ))
    Invoke-Docker ($base + @('genrsa', '-out', '/certs/control-plane-server-key.pem', '3072'))
    Invoke-Docker ($base + @(
        'req', '-new', '-sha256',
        '-key', '/certs/control-plane-server-key.pem',
        '-subj', '/CN=sre-control-plane',
        '-out', '/certs/control-plane-server.csr'
    ))
    Invoke-Docker ($base + @(
        'x509', '-req', '-sha256',
        '-in', '/certs/control-plane-server.csr',
        '-CA', '/certs/control-plane-server-ca-cert.pem',
        '-CAkey', '/certs/control-plane-server-ca-key.pem',
        '-CAcreateserial',
        '-days', '7',
        '-extfile', '/certs/control-plane-server.ext',
        '-out', '/certs/control-plane-server-cert.pem'
    ))

    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'connector-client.ext'),
        "basicConstraints=critical,CA:FALSE`nkeyUsage=critical,digitalSignature,keyEncipherment`nextendedKeyUsage=clientAuth`n",
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker ($base + @('genrsa', '-out', '/certs/connector-client-ca-key.pem', '3072'))
    Invoke-Docker ($base + @(
        'req', '-x509', '-new', '-sha256',
        '-key', '/certs/connector-client-ca-key.pem',
        '-days', '7',
        '-subj', '/CN=RocketMQ SRE Connector Development CA',
        '-out', '/certs/connector-client-ca-cert.pem'
    ))
    Invoke-Docker ($base + @('genrsa', '-out', '/certs/connector-client-key.pem', '3072'))
    Invoke-Docker ($base + @(
        'req', '-new', '-sha256',
        '-key', '/certs/connector-client-key.pem',
        '-subj', '/CN=rocketmq-sre-connector',
        '-out', '/certs/connector-client.csr'
    ))
    Invoke-Docker ($base + @(
        'x509', '-req', '-sha256',
        '-in', '/certs/connector-client.csr',
        '-CA', '/certs/connector-client-ca-cert.pem',
        '-CAkey', '/certs/connector-client-ca-key.pem',
        '-CAcreateserial',
        '-days', '7',
        '-extfile', '/certs/connector-client.ext',
        '-out', '/certs/connector-client-cert.pem'
    ))
    Invoke-Docker @(
        'run', '--rm',
        '--volume', $mount,
        '--entrypoint', '/bin/chmod',
        $opensslImage,
        '0644',
        '/certs/connector-client-cert.pem',
        '/certs/connector-client-key.pem'
    )
    $clientIdentity = [IO.File]::ReadAllText(
        (Join-Path $certificateDirectory 'connector-client-cert.pem')
    ) + [IO.File]::ReadAllText(
        (Join-Path $certificateDirectory 'connector-client-key.pem')
    )
    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'connector-client-identity.pem'),
        $clientIdentity,
        [Text.UTF8Encoding]::new($false)
    )
    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'admin.identity'),
        "phase00-compose-admin`n",
        [Text.UTF8Encoding]::new($false)
    )
    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'request-policy.json'),
        "{`"profile`":`"phase00-compose-read-only`"}`n",
        [Text.UTF8Encoding]::new($false)
    )
    $mcpAccessKey = 'phase00-compose-mcp-reader'
    $mcpSecretKey = New-RandomSecret
    $probeAccessKey = 'phase00-compose-probe'
    $probeSecretKey = New-RandomSecret
    $bootstrapAccessKey = 'phase00-compose-bootstrap'
    $bootstrapSecretKey = New-RandomSecret
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
        ''
    ) -join "`n"
    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'broker-acl.yml'),
        $brokerAcl,
        [Text.UTF8Encoding]::new($false)
    )
    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'mcp-rmq-credentials.yml'),
        "access_key: $mcpAccessKey`nsecret_key: $mcpSecretKey`n",
        [Text.UTF8Encoding]::new($false)
    )
    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'admin-read.env'),
        "ROCKETMQ_SRE_ADMIN_ACCESS_KEY=$mcpAccessKey`nROCKETMQ_SRE_ADMIN_SECRET_KEY=$mcpSecretKey`n",
        [Text.UTF8Encoding]::new($false)
    )
    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'probe-secret-key'),
        "$probeSecretKey`n",
        [Text.UTF8Encoding]::new($false)
    )
    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'probe.env'),
        "ROCKETMQ_SRE_PROBE_ACCESS_KEY=$probeAccessKey`nROCKETMQ_SRE_PROBE_SECRET_KEY_FILE=/var/run/secrets/rocketmq/probe-secret-key`n",
        [Text.UTF8Encoding]::new($false)
    )
    [IO.File]::WriteAllText(
        (Join-Path $certificateDirectory 'bootstrap.env'),
        "ROCKETMQ_ACL_ACCESS_KEY=$bootstrapAccessKey`nROCKETMQ_ACL_SECRET_KEY=$bootstrapSecretKey`n",
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker @(
        'run', '--rm',
        '--volume', $mount,
        '--entrypoint', '/bin/sh',
        $opensslImage,
        '-ec',
        'chown 10001:10001 /certs/ca-cert.pem /certs/server-cert.pem /certs/server-key.pem /certs/control-plane-server-ca-cert.pem /certs/control-plane-server-cert.pem /certs/control-plane-server-key.pem /certs/connector-client-ca-cert.pem /certs/connector-client-cert.pem /certs/connector-client-key.pem /certs/connector-client-identity.pem /certs/admin.identity /certs/request-policy.json /certs/broker-acl.yml /certs/mcp-rmq-credentials.yml /certs/admin-read.env /certs/probe-secret-key; chmod 0444 /certs/ca-cert.pem /certs/server-cert.pem /certs/control-plane-server-ca-cert.pem /certs/control-plane-server-cert.pem /certs/connector-client-ca-cert.pem /certs/connector-client-cert.pem /certs/admin.identity /certs/request-policy.json; chmod 0400 /certs/server-key.pem /certs/control-plane-server-key.pem /certs/connector-client-key.pem /certs/connector-client-identity.pem /certs/broker-acl.yml /certs/mcp-rmq-credentials.yml /certs/admin-read.env /certs/probe-secret-key'
    )
    foreach ($temporaryFile in @(
        'server.csr',
        'server.ext',
        'ca-cert.srl',
        'control-plane-server.csr',
        'control-plane-server.ext',
        'control-plane-server-ca-cert.srl',
        'connector-client.csr',
        'connector-client.ext',
        'connector-client-ca-cert.srl'
    )) {
        $temporaryPath = Join-Path $certificateDirectory $temporaryFile
        if (Test-Path -LiteralPath $temporaryPath -PathType Leaf) {
            Remove-Item -LiteralPath $temporaryPath -Force
        }
    }
    Write-Host "Development TLS fixtures written under $certificateDirectory"
}

Require-Command docker
if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) {
    throw "Compose file was not found at $composeFile"
}

switch ($Action) {
    'Certs' {
        New-DevelopmentCertificates
    }
    'ChannelCerts' {
        New-ControlPlaneChannelCertificates
    }
    'Up' {
        Ensure-AdminReadEnvironmentFixture
        $missingBaseMaterial = @(
            $requiredBaseDevelopmentMaterial |
                Where-Object { -not (Test-Path -LiteralPath (Join-Path $certificateDirectory $_) -PathType Leaf) }
        )
        if ($missingBaseMaterial.Count -gt 0) {
            New-DevelopmentCertificates
        }
        else {
            $missingChannelMaterial = @(
                $requiredControlPlaneChannelMaterial |
                    Where-Object {
                        -not (Test-Path -LiteralPath (Join-Path $certificateDirectory $_) -PathType Leaf)
                    }
            )
            if ($missingChannelMaterial.Count -gt 0) {
                New-ControlPlaneChannelCertificates
            }
        }
        Invoke-Docker (Compose-Arguments @('--profile', 'observability', 'config', '--quiet'))
        Invoke-Docker (Compose-Arguments @(
            '--profile', 'observability',
            'up', '--build', '--detach', '--wait'
        ))
        Write-Host 'Phase 00 stack is ready: UI http://127.0.0.1:3004'
    }
    'Down' {
        Invoke-Docker (Compose-Arguments @(
            '--profile', 'observability',
            'down', '--remove-orphans'
        ))
    }
    'Status' {
        Invoke-Docker (Compose-Arguments @(
            '--profile', 'observability',
            'ps'
        ))
    }
    'Reset' {
        if (-not $Force) {
            throw 'Reset removes the Phase 00 PostgreSQL and observability volumes. Re-run with -Force.'
        }
        Invoke-Docker (Compose-Arguments @(
            '--profile', 'observability',
            'down', '--volumes', '--remove-orphans'
        ))
        Assert-CertificateDirectory
        if (Test-Path -LiteralPath $certificateDirectory) {
            Get-ChildItem -LiteralPath $certificateDirectory -File -ErrorAction SilentlyContinue |
                Remove-Item -Force
        }
        Write-Host 'Phase 00 containers, volumes, and generated certificate files were removed.'
    }
}

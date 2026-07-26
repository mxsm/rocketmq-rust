# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Up', 'Down', 'Status', 'Certs', 'Reset')]
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
$requiredDevelopmentMaterial = @(
    'ca-cert.pem',
    'server-cert.pem',
    'server-key.pem',
    'admin.identity',
    'request-policy.json',
    'broker-acl.yml',
    'mcp-rmq-credentials.yml',
    'probe-secret-key',
    'probe.env',
    'bootstrap.env'
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
        'chown 10001:10001 /certs/ca-cert.pem /certs/server-cert.pem /certs/server-key.pem /certs/admin.identity /certs/request-policy.json /certs/broker-acl.yml /certs/mcp-rmq-credentials.yml /certs/probe-secret-key; chmod 0444 /certs/ca-cert.pem /certs/server-cert.pem /certs/admin.identity /certs/request-policy.json; chmod 0400 /certs/server-key.pem /certs/broker-acl.yml /certs/mcp-rmq-credentials.yml /certs/probe-secret-key'
    )
    Remove-Item -LiteralPath (Join-Path $certificateDirectory 'server.csr') -Force
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
    'Up' {
        $missingMaterial = @(
            $requiredDevelopmentMaterial |
                Where-Object { -not (Test-Path -LiteralPath (Join-Path $certificateDirectory $_) -PathType Leaf) }
        )
        if ($missingMaterial.Count -gt 0) {
            New-DevelopmentCertificates
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

# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

[CmdletBinding()]
param(
    [string]$CargoTargetDir = 'D:\cargo-targets\rocketmq-sre-enterprise-smoke',
    [string]$EvidenceOutput = 'D:\rocketmq-sre-evidence\enterprise-integrations-smoke.json',
    [switch]$KeepRunning
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$composeDirectory = Join-Path $sreRoot 'deploy/dev'
$composeFile = Join-Path $composeDirectory 'compose.yaml'
$enterpriseComposeFile = Join-Path $composeDirectory 'compose.enterprise.yaml'
$certificateDirectory = Join-Path $repositoryRoot 'target/phase00-certs'
$secretDirectory = Join-Path $repositoryRoot 'target/enterprise-secrets'
$caPath = Join-Path $certificateDirectory 'ca-cert.pem'
$manifestPath = Join-Path $sreRoot 'Cargo.toml'
$composePrefix = @(
    'compose',
    '--project-directory', $composeDirectory,
    '--file', $composeFile,
    '--file', $enterpriseComposeFile,
    '--profile', 'enterprise',
    '--profile', 'observability'
)

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

function Invoke-Native(
    [string]$Command,
    [string[]]$Arguments,
    [string]$Description
) {
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

function Invoke-Compose([string[]]$Arguments, [string]$Description) {
    Invoke-Native docker ($composePrefix + $Arguments) $Description
}

function Wait-CompletedService([string]$Service) {
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        $containerId = & docker @($composePrefix + @('ps', '--all', '--quiet', $Service))
        if ($LASTEXITCODE -ne 0) {
            throw "cannot inspect bootstrap service $Service"
        }
        if (-not [string]::IsNullOrWhiteSpace($containerId)) {
            $state = & docker inspect $containerId --format '{{.State.Status}} {{.State.ExitCode}}'
            if ($LASTEXITCODE -ne 0) {
                throw "cannot inspect bootstrap container for $Service"
            }
            if ($state -eq 'exited 0') {
                return
            }
            if ($state.StartsWith('exited ')) {
                throw "bootstrap service $Service failed: $state"
            }
        }
        Start-Sleep -Seconds 1
    }
    throw "bootstrap service $Service did not complete in time"
}

function Invoke-BoundedGet([string]$Url, [switch]$Tls) {
    $arguments = @('--fail', '--silent', '--show-error', '--max-time', '10')
    if ($Tls) {
        $arguments += @('--ssl-no-revoke', '--cacert', $caPath)
    }
    $result = & curl.exe @arguments $Url
    if ($LASTEXITCODE -ne 0) {
        throw "bounded backend query failed for $Url"
    }
    if (($result | Out-String).Length -gt 1MB) {
        throw "backend query exceeded the 1 MiB qualification limit for $Url"
    }
    $result
}

function ConvertFrom-JwtPart([string]$Part) {
    $encoded = $Part.Replace('-', '+').Replace('_', '/')
    switch ($encoded.Length % 4) {
        2 { $encoded += '==' }
        3 { $encoded += '=' }
    }
    [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($encoded)) |
        ConvertFrom-Json
}

foreach ($path in @(
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $EvidenceOutput; Description = 'evidence output' }
)) {
    Assert-DataPath $path.Value $path.Description
}

$savedEnvironment = @{}
$environmentNames = @(
    'CARGO_TARGET_DIR',
    'ROCKETMQ_SRE_OBJECT_STORE_ENDPOINT',
    'ROCKETMQ_SRE_OBJECT_STORE_BUCKET',
    'ROCKETMQ_SRE_OBJECT_STORE_REGION',
    'ROCKETMQ_SRE_OBJECT_STORE_CA_PATH',
    'ROCKETMQ_SRE_OBJECT_STORE_VAULT_AGENT_ROOT',
    'ROCKETMQ_SRE_OBJECT_STORE_SECRET_NAMESPACE',
    'ROCKETMQ_SRE_OBJECT_STORE_ACCESS_KEY_REF',
    'ROCKETMQ_SRE_OBJECT_STORE_SECRET_KEY_REF',
    'ROCKETMQ_SRE_OBJECT_STORE_SECRET_CACHE_SECONDS'
)
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$services = @(
    'keycloak', 'keycloak-tls', 'vault', 'vault-render',
    'minio', 'minio-bootstrap', 'minio-tls',
    'prometheus', 'loki', 'tempo', 'alertmanager'
)

try {
    New-Item -ItemType Directory -Force -Path $CargoTargetDir, $certificateDirectory, $secretDirectory | Out-Null
    & (Join-Path $scriptDirectory 'dev.ps1') -Action Certs -Force
    Invoke-Compose @('config', '--quiet') 'enterprise Compose validation'
    Invoke-Compose (@('up', '-d') + $services) 'enterprise backend startup'
    Wait-CompletedService 'vault-render'
    Wait-CompletedService 'minio-bootstrap'

    $discovery = Invoke-BoundedGet `
        'https://localhost:8445/realms/rocketmq-sre/.well-known/openid-configuration' `
        -Tls | ConvertFrom-Json
    if ($discovery.issuer -ne 'https://localhost:8445/realms/rocketmq-sre') {
        throw 'OIDC discovery issuer mismatch.'
    }
    Invoke-BoundedGet 'https://localhost:9443/minio/health/ready' -Tls | Out-Null
    Invoke-BoundedGet 'http://localhost:8200/v1/sys/health' | Out-Null

    $tokenResponse = & curl.exe `
        --fail --silent --show-error --ssl-no-revoke --cacert $caPath `
        -X POST `
        'https://localhost:8445/realms/rocketmq-sre/protocol/openid-connect/token' `
        -H 'Content-Type: application/x-www-form-urlencoded' `
        --data-urlencode 'grant_type=password' `
        --data-urlencode 'client_id=rocketmq-sre-qualification' `
        --data-urlencode 'client_secret=qualification-client-secret' `
        --data-urlencode 'username=sre-operator' `
        --data-urlencode 'password=qualification-only'
    if ($LASTEXITCODE -ne 0) {
        throw 'OIDC qualification token request failed.'
    }
    $accessToken = ($tokenResponse | ConvertFrom-Json).access_token
    $claims = ConvertFrom-JwtPart $accessToken.Split('.')[1]
    if (@($claims.aud) -notcontains 'rocketmq-sre-control-plane') {
        throw 'OIDC audience mismatch.'
    }
    if ($claims.rocketmq_tenant -ne '00000000-0000-4000-8000-000000000002') {
        throw 'OIDC tenant claim mismatch.'
    }
    if (@($claims.rocketmq_clusters) -notcontains '00000000-0000-4000-8000-000000000001') {
        throw 'OIDC cluster claim mismatch.'
    }
    foreach ($scope in @('rocketmq:read', 'rocketmq:diagnose')) {
        if (($claims.scope -split ' ') -notcontains $scope) {
            throw "OIDC token is missing $scope."
        }
    }
    $realmRoles = @($claims.realm_access.roles)
    foreach ($role in @('operator', 'approver', 'model-governance')) {
        if ($realmRoles -notcontains $role) {
            throw "OIDC token is missing the standard realm role $role."
        }
    }

    $beforeKeys = @((Invoke-BoundedGet `
        'https://localhost:8445/realms/rocketmq-sre/protocol/openid-connect/certs' `
        -Tls | ConvertFrom-Json).keys.kid)
    Invoke-Compose @('up', '-d', '--force-recreate', 'keycloak', 'keycloak-tls') 'OIDC signing-key rotation'
    $afterKeys = @((Invoke-BoundedGet `
        'https://localhost:8445/realms/rocketmq-sre/protocol/openid-connect/certs' `
        -Tls | ConvertFrom-Json).keys.kid)
    if (-not ($afterKeys | Where-Object { $beforeKeys -notcontains $_ })) {
        throw 'OIDC issuer did not publish a new signing key.'
    }

    foreach ($endpoint in @(
        'http://localhost:9090/api/v1/query?query=up',
        'http://localhost:3100/loki/api/v1/labels',
        'http://localhost:3200/api/search?limit=1',
        'http://localhost:9093/api/v2/status'
    )) {
        Invoke-BoundedGet $endpoint | Out-Null
    }

    $renderedAccessKey = Join-Path $secretDirectory 'object-store/access-key'
    $beforeRotation = (Get-Item -LiteralPath $renderedAccessKey).LastWriteTimeUtc
    Start-Sleep -Milliseconds 1100
    Invoke-Compose @('run', '--rm', '--no-deps', 'vault-render') 'Vault secret rotation render'
    $afterRotation = (Get-Item -LiteralPath $renderedAccessKey).LastWriteTimeUtc
    if ($afterRotation -le $beforeRotation) {
        throw 'Vault-rendered secret version did not advance.'
    }

    $env:CARGO_TARGET_DIR = [IO.Path]::GetFullPath($CargoTargetDir)
    $env:ROCKETMQ_SRE_OBJECT_STORE_ENDPOINT = 'https://localhost:9443'
    $env:ROCKETMQ_SRE_OBJECT_STORE_BUCKET = 'rocketmq-sre-evidence'
    $env:ROCKETMQ_SRE_OBJECT_STORE_REGION = 'us-east-1'
    $env:ROCKETMQ_SRE_OBJECT_STORE_CA_PATH = [IO.Path]::GetFullPath($caPath)
    $env:ROCKETMQ_SRE_OBJECT_STORE_VAULT_AGENT_ROOT = [IO.Path]::GetFullPath($secretDirectory)
    $env:ROCKETMQ_SRE_OBJECT_STORE_SECRET_NAMESPACE = 'object-store'
    $env:ROCKETMQ_SRE_OBJECT_STORE_ACCESS_KEY_REF = 'external://object-store/access-key'
    $env:ROCKETMQ_SRE_OBJECT_STORE_SECRET_KEY_REF = 'external://object-store/secret-key'
    $env:ROCKETMQ_SRE_OBJECT_STORE_SECRET_CACHE_SECONDS = '0'
    Invoke-Native cargo @(
        'test', '--manifest-path', $manifestPath, '--locked',
        '-p', 'rocketmq-sre-control-plane',
        'evidence::blob::tests::s3_compatible_https_live_round_trip_uses_external_secret_references',
        '--', '--ignored', '--exact'
    ) 'HTTPS S3-compatible Evidence Store smoke'

    $evidence = [ordered]@{
        schema_version = 'rocketmq-sre.enterprise-integrations-smoke.v1'
        status = 'passed'
        observed_at = [DateTimeOffset]::UtcNow.ToString('O')
        oidc = [ordered]@{
            authorization_code_pkce_profile = $true
            issuer = $discovery.issuer
            audience_verified = $true
            tenant_cluster_scope_verified = $true
            standard_realm_role_mapping_verified = $true
            signing_key_rotation_verified = $true
        }
        secrets = [ordered]@{
            backend = 'vault'
            external_references_only = $true
            rendered_rotation_path_verified = $true
            secret_values_recorded = $false
        }
        evidence_store = [ordered]@{
            backend = 's3-compatible'
            https_put_get_hash_cleanup = $true
            credentials_from_secret_reference = $true
        }
        observability = @('prometheus', 'loki', 'tempo', 'alertmanager')
    }
    $evidenceDirectory = Split-Path -Parent ([IO.Path]::GetFullPath($EvidenceOutput))
    New-Item -ItemType Directory -Force -Path $evidenceDirectory | Out-Null
    $evidence | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $EvidenceOutput -Encoding utf8
    Write-Host "ENTERPRISE_INTEGRATIONS_SMOKE_OK evidence=$EvidenceOutput"
}
finally {
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
    if (-not $KeepRunning) {
        Invoke-Compose @('down', '--volumes', '--remove-orphans') 'enterprise backend cleanup'
    }
}

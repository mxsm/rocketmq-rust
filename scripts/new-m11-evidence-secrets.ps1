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
    [ValidateSet("Validate", "Generate")]
    [string]$Mode = "Validate",

    [string]$OutputDirectory = "target/m11-evidence-inputs",
    [string]$Namespace = "rocketmq-system"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ExpectedFiles = @(
    "runtime-secret.yaml",
    "rotated-runtime-secret.yaml",
    "baseline-driver-secret.yaml",
    "rotated-driver-secret.yaml"
)

function ConvertTo-Base64 {
    param([Parameter(Mandatory)][AllowEmptyString()][string]$Value)

    [Convert]::ToBase64String([Text.UTF8Encoding]::new($false).GetBytes($Value))
}

function New-RandomHex {
    param([ValidateRange(8, 128)][int]$ByteCount)

    $bytes = [byte[]]::new($ByteCount)
    $generator = [Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($bytes)
    } finally {
        $generator.Dispose()
    }
    ([BitConverter]::ToString($bytes)).Replace("-", "").ToLowerInvariant()
}

function ConvertTo-DerLength {
    param([ValidateRange(0, [int]::MaxValue)][int]$Length)

    if ($Length -lt 128) {
        return ,([byte[]]@([byte]$Length))
    }
    $octets = [System.Collections.Generic.List[byte]]::new()
    $remaining = $Length
    while ($remaining -gt 0) {
        $octets.Insert(0, [byte]($remaining -band 0xff))
        $remaining = $remaining -shr 8
    }
    $encoded = [System.Collections.Generic.List[byte]]::new()
    $encoded.Add([byte](0x80 -bor $octets.Count))
    $encoded.AddRange($octets)
    return ,$encoded.ToArray()
}

function ConvertTo-DerInteger {
    param([Parameter(Mandatory)][byte[]]$Value)

    $offset = 0
    while ($offset -lt $Value.Length - 1 -and $Value[$offset] -eq 0) {
        $offset++
    }
    [byte[]]$content = $Value[$offset..($Value.Length - 1)]
    if (($content[0] -band 0x80) -ne 0) {
        [byte[]]$content = @([byte]0) + $content
    }
    $encoded = [System.Collections.Generic.List[byte]]::new()
    $encoded.Add([byte]0x02)
    $encoded.AddRange([byte[]](ConvertTo-DerLength $content.Length))
    $encoded.AddRange($content)
    return ,$encoded.ToArray()
}

function ConvertTo-Pkcs1PrivateKey {
    param([Parameter(Mandatory)][Security.Cryptography.RSAParameters]$Parameters)

    $content = [System.Collections.Generic.List[byte]]::new()
    foreach ($value in @(
            [byte[]]@(0),
            $Parameters.Modulus,
            $Parameters.Exponent,
            $Parameters.D,
            $Parameters.P,
            $Parameters.Q,
            $Parameters.DP,
            $Parameters.DQ,
            $Parameters.InverseQ
        )) {
        $content.AddRange([byte[]](ConvertTo-DerInteger $value))
    }
    $encoded = [System.Collections.Generic.List[byte]]::new()
    $encoded.Add([byte]0x30)
    $encoded.AddRange([byte[]](ConvertTo-DerLength $content.Count))
    $encoded.AddRange($content)
    return ,$encoded.ToArray()
}

function ConvertTo-Pem {
    param(
        [Parameter(Mandatory)][string]$Label,
        [Parameter(Mandatory)][byte[]]$Der
    )

    $payload = [Convert]::ToBase64String(
        $Der,
        [Base64FormattingOptions]::InsertLineBreaks
    ).Replace("`r`n", "`n")
    "-----BEGIN $Label-----`n$payload`n-----END $Label-----`n"
}

function New-TlsIdentity {
    param([Parameter(Mandatory)][string]$CommonName)

    $rsa = [Security.Cryptography.RSA]::Create(2048)
    $certificate = $null
    try {
        $request = [Security.Cryptography.X509Certificates.CertificateRequest]::new(
            "CN=$CommonName",
            $rsa,
            [Security.Cryptography.HashAlgorithmName]::SHA256,
            [Security.Cryptography.RSASignaturePadding]::Pkcs1
        )
        $certificate = $request.CreateSelfSigned(
            [DateTimeOffset]::UtcNow.AddMinutes(-5),
            [DateTimeOffset]::UtcNow.AddDays(2)
        )
        @{
            Certificate = ConvertTo-Pem "CERTIFICATE" $certificate.RawData
            PrivateKey = ConvertTo-Pem "RSA PRIVATE KEY" (ConvertTo-Pkcs1PrivateKey $rsa.ExportParameters($true))
        }
    } finally {
        if ($null -ne $certificate) {
            $certificate.Dispose()
        }
        $rsa.Dispose()
    }
}

function Write-SecretManifest {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][hashtable]$Data
    )

    $lines = [System.Collections.Generic.List[string]]::new()
    foreach ($line in @(
            "apiVersion: v1",
            "kind: Secret",
            "metadata:",
            "  name: $Name",
            "  namespace: $Namespace",
            "type: Opaque",
            "data:"
        )) {
        $lines.Add($line)
    }
    foreach ($key in ($Data.Keys | Sort-Object)) {
        $lines.Add("  ${key}: $(ConvertTo-Base64 ([string]$Data[$key]))")
    }
    [IO.File]::WriteAllText(
        $Path,
        (($lines -join "`n") + "`n"),
        [Text.UTF8Encoding]::new($false)
    )
}

function New-RuntimeSecretData {
    param(
        [Parameter(Mandatory)][string]$AccessKey,
        [Parameter(Mandatory)][string]$SecretKey,
        [Parameter(Mandatory)][string]$Nonce,
        [Parameter(Mandatory)][hashtable]$TlsIdentity
    )

    $acl = @"
globalWhiteRemoteAddresses: []
accounts:
  - accessKey: '$AccessKey'
    secretKey: '$SecretKey'
    admin: true
    defaultTopicPerm: PUB|SUB
    defaultGroupPerm: PUB|SUB
"@
    @{
        "admin.identity" = "m11-evidence-admin-$Nonce"
        "broker-acl.yml" = $acl
        "ca.crt" = "m11-evidence-trust-anchor-$Nonce"
        "proxy-acl.yml" = $acl
        "request-policy.json" = '{"profile":"m11-ephemeral-evidence"}'
        "tls.crt" = $TlsIdentity.Certificate
        "tls.key" = $TlsIdentity.PrivateKey
    }
}

if ($Mode -eq "Validate") {
    Write-Output "M11_EVIDENCE_SECRET_GENERATOR_OK files=$($ExpectedFiles.Count)"
    exit 0
}

if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    throw "OutputDirectory is required in Generate mode"
}
if ([string]::IsNullOrWhiteSpace($Namespace)) {
    throw "Namespace is required in Generate mode"
}

$resolvedOutput = [IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Force -Path $resolvedOutput | Out-Null

$nonce = New-RandomHex 12
$baselineAccessKey = "M11B$(New-RandomHex 10)"
$baselineSecretKey = New-RandomHex 32
$rotatedAccessKey = "M11R$(New-RandomHex 10)"
$rotatedSecretKey = New-RandomHex 32
$tlsIdentity = New-TlsIdentity "rocketmq-mcp.$Namespace.svc.cluster.local"

Write-SecretManifest `
    -Path (Join-Path $resolvedOutput "runtime-secret.yaml") `
    -Name "rocketmq-runtime-secrets" `
    -Data (New-RuntimeSecretData $baselineAccessKey $baselineSecretKey $nonce $tlsIdentity)
Write-SecretManifest `
    -Path (Join-Path $resolvedOutput "rotated-runtime-secret.yaml") `
    -Name "rocketmq-runtime-secrets" `
    -Data (New-RuntimeSecretData $rotatedAccessKey $rotatedSecretKey $nonce $tlsIdentity)
Write-SecretManifest `
    -Path (Join-Path $resolvedOutput "baseline-driver-secret.yaml") `
    -Name "rocketmq-fault-driver-baseline" `
    -Data @{
        ROCKETMQ_ACL_ACCESS_KEY = $baselineAccessKey
        ROCKETMQ_ACL_SECRET_KEY = $baselineSecretKey
    }
Write-SecretManifest `
    -Path (Join-Path $resolvedOutput "rotated-driver-secret.yaml") `
    -Name "rocketmq-fault-driver-rotated" `
    -Data @{
        ROCKETMQ_ACL_ACCESS_KEY = $rotatedAccessKey
        ROCKETMQ_ACL_SECRET_KEY = $rotatedSecretKey
    }

foreach ($file in $ExpectedFiles) {
    $path = Join-Path $resolvedOutput $file
    if (-not (Test-Path -LiteralPath $path -PathType Leaf) -or (Get-Item -LiteralPath $path).Length -eq 0) {
        throw "failed to create evidence input manifest: $file"
    }
}

Write-Output "M11_EPHEMERAL_SECRET_MANIFESTS_OK files=$($ExpectedFiles.Count)"

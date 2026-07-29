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
    [string]$Kubeconfig = 'G:\rocketmq-sre-phase2-temp\kind-access\rocketmq-sre-phase00.kubeconfig',
    [string]$CargoHome = 'G:\rocketmq-sre-phase1-cargo-home',
    [string]$CargoTargetDir = 'G:\rocketmq-sre-phase2-cargo-target',
    [string]$TempDir = 'G:\rocketmq-sre-phase2-temp',
    [string]$AdminCliPath = '',
    [ValidateRange(1024, 65535)]
    [int]$PostgresLocalPort = 60032,
    [ValidateRange(1024, 65535)]
    [int]$ExecutorLocalPort = 60094,
    [ValidateRange(1024, 65535)]
    [int]$AgentLocalPort = 60095,
    [ValidateRange(1024, 65535)]
    [int]$NameServerLocalPort = 60876,
    [ValidateRange(1024, 65535)]
    [int]$BrokerLocalPort = 60911
)

$ErrorActionPreference = 'Stop'
$sreRoot = Split-Path -Parent $PSScriptRoot
$repositoryRoot = Split-Path -Parent $sreRoot
$manifestPath = Join-Path $sreRoot 'Cargo.toml'
$namespace = 'rocketmq-sre'
$credentialSet = 'broker-admin'
$activeSecret = 'broker-admin-credential-v1'
$candidateSecret = 'broker-admin-credential-v2'
$selector = 'broker-admin-credential-selector'
$probeTopic = 'SRE_PROBE_CREDENTIAL_ROTATION'

function Assert-NonSystemBuildPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Description
    )
    $resolved = [IO.Path]::GetFullPath($Path)
    if ($resolved.StartsWith('C:\', [StringComparison]::OrdinalIgnoreCase)) {
        throw "$Description must not use the C drive: $resolved"
    }
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Executable,
        [Parameter(Mandatory = $true)]
        [object[]]$Arguments,
        [Parameter(Mandatory = $true)]
        [string]$Description
    )
    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

function Assert-PortAvailable {
    param([Parameter(Mandatory = $true)][int]$Port)
    if (Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue) {
        throw "Local port $Port is already in use."
    }
}

function Start-KubePortForward {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetNamespace,
        [Parameter(Mandatory = $true)]
        [string]$Resource,
        [Parameter(Mandatory = $true)]
        [int]$LocalPort,
        [Parameter(Mandatory = $true)]
        [int]$RemotePort,
        [Parameter(Mandatory = $true)]
        [string]$LogRoot
    )
    $safeName = $Resource.Replace('/', '-')
    $stdout = Join-Path $LogRoot "$safeName-$LocalPort.out.log"
    $stderr = Join-Path $LogRoot "$safeName-$LocalPort.err.log"
    $process = Start-Process `
        -FilePath kubectl `
        -ArgumentList @(
            '--kubeconfig', $Kubeconfig,
            '-n', $TargetNamespace,
            'port-forward', $Resource,
            "${LocalPort}:${RemotePort}"
        ) `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -WindowStyle Hidden `
        -PassThru
    $deadline = (Get-Date).AddSeconds(30)
    do {
        if ($process.HasExited) {
            $diagnostic = if (Test-Path -LiteralPath $stderr) {
                Get-Content -LiteralPath $stderr -Raw
            } else {
                'no port-forward diagnostic'
            }
            throw "Port-forward $Resource exited before readiness: $diagnostic"
        }
        if (Get-NetTCPConnection -State Listen -LocalPort $LocalPort -ErrorAction SilentlyContinue) {
            return $process
        }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)
    throw "Port-forward $Resource did not listen on $LocalPort before the deadline."
}

function Stop-OwnedProcess {
    param([Diagnostics.Process]$Process)
    if ($null -ne $Process -and -not $Process.HasExited) {
        Stop-Process -Id $Process.Id -Force
        $Process.WaitForExit(10000)
    }
}

function Assert-ResourceAbsent {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Kind,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )
    $existing = kubectl --kubeconfig $Kubeconfig -n $namespace get "$Kind/$Name" --ignore-not-found -o name
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to inspect $Kind/$Name."
    }
    if (-not [string]::IsNullOrWhiteSpace($existing)) {
        throw "Refusing to overwrite pre-existing test resource $Kind/$Name."
    }
}

function Apply-JsonResource {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Resource,
        [Parameter(Mandatory = $true)]
        [string]$Description
    )
    $Resource |
        ConvertTo-Json -Depth 10 -Compress |
        kubectl --kubeconfig $Kubeconfig apply -f -
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed."
    }
}

function Remove-TestResources {
    kubectl --kubeconfig $Kubeconfig -n $namespace delete `
        configmap/$selector `
        secret/$activeSecret `
        secret/$candidateSecret `
        --ignore-not-found `
        --wait=true `
        --timeout=60s | Out-Host
}

foreach ($path in @(
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $TempDir; Description = 'temporary directory' }
)) {
    Assert-NonSystemBuildPath $path.Value $path.Description
}
$Kubeconfig = [IO.Path]::GetFullPath($Kubeconfig)
if (-not (Test-Path -LiteralPath $Kubeconfig -PathType Leaf)) {
    throw "Kubeconfig does not exist: $Kubeconfig"
}
if ([string]::IsNullOrWhiteSpace($AdminCliPath)) {
    $AdminCliPath = Join-Path $CargoTargetDir 'debug/rocketmq-admin-cli.exe'
}
$AdminCliPath = [IO.Path]::GetFullPath($AdminCliPath)
Assert-NonSystemBuildPath $AdminCliPath 'Admin CLI'
if (-not (Test-Path -LiteralPath $AdminCliPath -PathType Leaf)) {
    throw "A prebuilt RocketMQ Admin CLI is required: $AdminCliPath"
}
foreach ($port in @(
    $PostgresLocalPort,
    $ExecutorLocalPort,
    $AgentLocalPort,
    $NameServerLocalPort,
    $BrokerLocalPort
)) {
    Assert-PortAvailable $port
}
foreach ($resource in @(
    @{ Kind = 'secret'; Name = $activeSecret },
    @{ Kind = 'secret'; Name = $candidateSecret },
    @{ Kind = 'configmap'; Name = $selector }
)) {
    Assert-ResourceAbsent $resource.Kind $resource.Name
}

New-Item -ItemType Directory -Force -Path $CargoHome, $CargoTargetDir, $TempDir | Out-Null
$targetDrive = Get-PSDrive -Name (
    [IO.Path]::GetPathRoot([IO.Path]::GetFullPath($CargoTargetDir)).Substring(0, 1)
)
if (($targetDrive.Free / 1GB) -lt 15) {
    Invoke-Native cargo @(
        '+1.95.0', 'clean',
        '--manifest-path', $manifestPath,
        '--target-dir', $CargoTargetDir
    ) 'low-space SRE Cargo cleanup'
    throw 'Cargo cleanup removed the required prebuilt Admin CLI; rebuild it outside C: and rerun.'
}

$runRoot = [IO.Path]::GetFullPath(
    (Join-Path $TempDir "phase03-credential-supervised-$([Guid]::NewGuid().ToString('N'))")
)
$expectedTempRoot = [IO.Path]::GetFullPath($TempDir).TrimEnd('\') + '\'
if (-not $runRoot.StartsWith($expectedTempRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Credential E2E runtime directory escaped the configured temporary root.'
}
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

$environmentNames = @(
    'CARGO_HOME',
    'CARGO_TARGET_DIR',
    'TEMP',
    'TMP',
    'KUBECONFIG',
    'ROCKETMQ_ACL_ACCESS_KEY',
    'ROCKETMQ_ACL_SECRET_KEY',
    'ROCKETMQ_SRE_PHASE3_DATABASE_URL',
    'ROCKETMQ_SRE_PHASE3_EXECUTOR_URL',
    'ROCKETMQ_SRE_PHASE3_AGENT_URL',
    'ROCKETMQ_SRE_PHASE3_WORKLOAD_TOKEN',
    'ROCKETMQ_SRE_PHASE3_SIGNING_KEY'
)
$savedEnvironment = @{}
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$forwards = [Collections.Generic.List[Diagnostics.Process]]::new()
$resourcesCreated = $false
$succeeded = $false
try {
    $env:KUBECONFIG = $Kubeconfig
    $forwards.Add((Start-KubePortForward $namespace 'statefulset/postgres' $PostgresLocalPort 5432 $runRoot))
    $forwards.Add((Start-KubePortForward $namespace 'service/sre-executor' $ExecutorLocalPort 8094 $runRoot))
    $forwards.Add((Start-KubePortForward $namespace 'service/sre-execution-agent' $AgentLocalPort 8095 $runRoot))
    $forwards.Add(
        (Start-KubePortForward 'rocketmq-system' 'service/rocketmq-namesrv' $NameServerLocalPort 9876 $runRoot)
    )
    $forwards.Add((Start-KubePortForward 'rocketmq-system' 'service/rocketmq-broker' $BrokerLocalPort 10911 $runRoot))

    $kindSecret = kubectl --kubeconfig $Kubeconfig -n $namespace get secret rocketmq-sre-kind-secrets -o json |
        ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to read the Kind workload credential references.'
    }
    $env:ROCKETMQ_ACL_ACCESS_KEY = [Text.Encoding]::UTF8.GetString(
        [Convert]::FromBase64String($kindSecret.data.'agent-mutation-access-key')
    )
    $env:ROCKETMQ_ACL_SECRET_KEY = [Text.Encoding]::UTF8.GetString(
        [Convert]::FromBase64String($kindSecret.data.'agent-mutation-secret-key')
    )
    Invoke-Native $AdminCliPath @(
        'topic', 'updateTopic',
        '-n', "127.0.0.1:$NameServerLocalPort",
        '-b', "127.0.0.1:$BrokerLocalPort",
        '-t', $probeTopic,
        '-r', '1',
        '-w', '1',
        '-p', '6'
    ) 'dedicated credential validation Topic bootstrap'
    Remove-Item Env:\ROCKETMQ_ACL_ACCESS_KEY -ErrorAction SilentlyContinue
    Remove-Item Env:\ROCKETMQ_ACL_SECRET_KEY -ErrorAction SilentlyContinue

    $readAccessKey = $kindSecret.data.'agent-read-access-key'
    $readSecretKey = $kindSecret.data.'agent-read-secret-key'
    $resourcesCreated = $true
    foreach ($secretSpec in @(
        @{ Name = $activeSecret; Version = 'v1' },
        @{ Name = $candidateSecret; Version = 'v2' }
    )) {
        Apply-JsonResource ([ordered]@{
            apiVersion = 'v1'
            kind = 'Secret'
            metadata = [ordered]@{
                name = $secretSpec.Name
                namespace = $namespace
                annotations = [ordered]@{
                    'rocketmq.apache.org/sre-credential-set' = $credentialSet
                    'rocketmq.apache.org/sre-credential-version' = $secretSpec.Version
                }
            }
            immutable = $true
            type = 'Opaque'
            data = [ordered]@{
                'access-key' = $readAccessKey
                'secret-key' = $readSecretKey
            }
        }) "$($secretSpec.Name) creation"
    }
    Apply-JsonResource ([ordered]@{
        apiVersion = 'v1'
        kind = 'ConfigMap'
        metadata = [ordered]@{
            name = $selector
            namespace = $namespace
            annotations = [ordered]@{
                'rocketmq.apache.org/sre-credential-set' = $credentialSet
                'rocketmq.apache.org/sre-active-credential-version' = 'v1'
                'rocketmq.apache.org/sre-active-credential-ref' =
                    "kubernetes://$namespace/$activeSecret"
                'rocketmq.apache.org/sre-candidate-probe-healthy' = 'false'
            }
        }
        data = [ordered]@{
            description = 'Phase 03 supervised credential selector fixture'
        }
    }) 'credential selector creation'

    $databaseUrlEncoded = kubectl --kubeconfig $Kubeconfig -n $namespace get `
        secret rocketmq-sre-postgres -o jsonpath='{.data.database-url}'
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to read the Kind PostgreSQL connection reference.'
    }
    $databaseUrl = [Text.Encoding]::UTF8.GetString(
        [Convert]::FromBase64String($databaseUrlEncoded)
    )
    $databaseUri = [UriBuilder]$databaseUrl
    $databaseUri.Host = '127.0.0.1'
    $databaseUri.Port = $PostgresLocalPort
    $workloadToken = [Text.Encoding]::UTF8.GetString(
        [Convert]::FromBase64String($kindSecret.data.'internal-token')
    )
    $env:ROCKETMQ_SRE_PHASE3_DATABASE_URL = $databaseUri.Uri.AbsoluteUri
    $env:ROCKETMQ_SRE_PHASE3_EXECUTOR_URL = "http://127.0.0.1:$ExecutorLocalPort"
    $env:ROCKETMQ_SRE_PHASE3_AGENT_URL = "http://127.0.0.1:$AgentLocalPort"
    $env:ROCKETMQ_SRE_PHASE3_WORKLOAD_TOKEN = $workloadToken
    $env:ROCKETMQ_SRE_PHASE3_SIGNING_KEY = $workloadToken
    $env:CARGO_HOME = $CargoHome
    $env:CARGO_TARGET_DIR = $CargoTargetDir
    $env:TEMP = $TempDir
    $env:TMP = $TempDir
    Set-Location $repositoryRoot
    Invoke-Native cargo @(
        '+1.95.0', 'test',
        '--manifest-path', $manifestPath,
        '--locked',
        '--package', 'rocketmq-sre-control-plane',
        'supervised_execution::credential_rotation_e2e_tests::real_kind_supervised_credential_overlap_passes_critic_and_verification',
        '--',
        '--ignored',
        '--exact',
        '--nocapture',
        '--test-threads=1'
    ) 'formal supervised credential rotation E2E'

    $selectorView = kubectl --kubeconfig $Kubeconfig -n $namespace get configmap $selector -o json |
        ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to verify the credential selector after execution.'
    }
    $annotations = $selectorView.metadata.annotations
    if (
        $annotations.'rocketmq.apache.org/sre-active-credential-version' -ne 'v2' -or
        $annotations.'rocketmq.apache.org/sre-retiring-credential-version' -ne 'v1' -or
        $annotations.'rocketmq.apache.org/sre-candidate-probe-healthy' -ne 'true'
    ) {
        throw 'The supervised execution did not leave the expected bounded overlap state.'
    }
    $succeeded = $true
    Write-Host (
        'PHASE03_CREDENTIAL_SUPERVISED_E2E_OK ' +
        'critic=heterogeneous approval=independent execution=succeeded ' +
        'candidate_active=true previous_retiring=true journaled=true'
    )
}
finally {
    if ($resourcesCreated) {
        Remove-TestResources
    }
    foreach ($process in $forwards) {
        Stop-OwnedProcess $process
    }
    foreach ($name in $environmentNames) {
        $value = $savedEnvironment[$name]
        if ($null -eq $value) {
            [Environment]::SetEnvironmentVariable($name, $null, 'Process')
        } else {
            [Environment]::SetEnvironmentVariable($name, $value, 'Process')
        }
    }
    Set-Location $repositoryRoot
    if (
        (Test-Path -LiteralPath $runRoot -PathType Container) -and
        $runRoot.StartsWith($expectedTempRoot, [StringComparison]::OrdinalIgnoreCase)
    ) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
    if (-not $succeeded) {
        Write-Warning 'Phase 03 supervised credential rotation E2E did not complete successfully.'
    }
}

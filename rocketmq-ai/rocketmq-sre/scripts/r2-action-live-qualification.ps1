# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [ValidatePattern('^[a-z0-9][a-z0-9-]{0,39}$')]
    [string]$ClusterName = 'rocketmq-sre-r2-qualification',

    [string]$CargoHome = 'D:\BuildCache\rocketmq-sre-cargo-home',

    [string]$CargoTargetDir = 'F:\BuildCache\rocketmq-sre-r2-action-qualification',

    [string]$AdminCargoTargetDir = 'D:\BuildCache\rocketmq-sre-r2-admin-cli',

    [string]$TemporaryRoot = 'D:\BuildCache\rocketmq-sre-temp',

    [string]$EvidenceRoot = 'D:\rocketmq-sre-evidence',

    [ValidateRange(1024, 65535)]
    [int]$PostgresLocalPort = 35442,

    [ValidateRange(1024, 65535)]
    [int]$ExecutorLocalPort = 58106,

    [ValidateRange(1024, 65535)]
    [int]$AgentLocalPort = 58107,

    [switch]$SkipImageBuild
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$sreRoot = [IO.Path]::GetFullPath((Join-Path $scriptDirectory '..'))
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $sreRoot '../..'))
$repositoryTarget = [IO.Path]::GetFullPath((Join-Path $repositoryRoot 'target'))
$kindArtifacts = [IO.Path]::GetFullPath((Join-Path $repositoryTarget 'phase00-kind'))
$certificateArtifacts = [IO.Path]::GetFullPath((Join-Path $repositoryTarget 'phase00-certs'))
$kubeconfig = Join-Path $kindArtifacts 'kubeconfig'
$manifestPath = Join-Path $sreRoot 'config\qualification\r2-actions.v1.json'
$checkerPath = Join-Path $scriptDirectory 'check_r2_action_qualification.py'
$kindScript = Join-Path $scriptDirectory 'kind.ps1'
$waveScript = Join-Path $scriptDirectory 'phase03-wave-actions-supervised-e2e.ps1'
$credentialScript = Join-Path $scriptDirectory 'phase03-credential-supervised-e2e.ps1'
$cargoManifest = Join-Path $sreRoot 'Cargo.toml'
$rootCargoManifest = Join-Path $repositoryRoot 'Cargo.toml'

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

function Test-KindCluster([string]$Name) {
    $savedErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'SilentlyContinue'
        $clusters = & kind get clusters 2>$null
        $status = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $savedErrorActionPreference
    }
    if ($status -ne 0) {
        return $false
    }
    return @($clusters | Where-Object { $_.Trim() -eq $Name }).Count -eq 1
}

function Remove-OwnedArtifacts {
    foreach ($path in @($kindArtifacts, $certificateArtifacts)) {
        if (-not $path.StartsWith($repositoryTarget + '\', [StringComparison]::OrdinalIgnoreCase)) {
            throw "Owned runtime artifact escaped the repository target directory: $path"
        }
        if (Test-Path -LiteralPath $path) {
            Remove-Item -LiteralPath $path -Recurse -Force
        }
    }
}

function Get-ImageRepository([string]$Image) {
    $withoutDigest = $Image.Split('@')[0]
    $slash = $withoutDigest.LastIndexOf('/')
    $colon = $withoutDigest.LastIndexOf(':')
    if ($colon -gt $slash) {
        return $withoutDigest.Substring(0, $colon)
    }
    return $withoutDigest
}

foreach ($path in @(
    @{ Value = $CargoHome; Description = 'Cargo home' },
    @{ Value = $CargoTargetDir; Description = 'Cargo target directory' },
    @{ Value = $AdminCargoTargetDir; Description = 'Admin CLI Cargo target directory' },
    @{ Value = $TemporaryRoot; Description = 'temporary directory' },
    @{ Value = $EvidenceRoot; Description = 'qualification Evidence root' }
)) {
    Assert-DataPath $path.Value $path.Description
}
if (@(@($PostgresLocalPort, $ExecutorLocalPort, $AgentLocalPort) | Select-Object -Unique).Count -ne 3) {
    throw 'Qualification loopback ports must be distinct.'
}

$resolvedCargoHome = [IO.Path]::GetFullPath($CargoHome)
$resolvedCargoTarget = [IO.Path]::GetFullPath($CargoTargetDir)
$resolvedAdminCargoTarget = [IO.Path]::GetFullPath($AdminCargoTargetDir)
$resolvedTemporaryRoot = [IO.Path]::GetFullPath($TemporaryRoot)
$resolvedEvidenceRoot = [IO.Path]::GetFullPath($EvidenceRoot).TrimEnd('\')
$allowedEvidenceRoots = @(
    [IO.Path]::GetFullPath('D:\rocketmq-sre-evidence').TrimEnd('\'),
    [IO.Path]::GetFullPath('F:\rocketmq-sre-evidence').TrimEnd('\')
)
if (-not ($allowedEvidenceRoots -contains $resolvedEvidenceRoot)) {
    throw 'Qualification reports must use D:\rocketmq-sre-evidence or F:\rocketmq-sre-evidence.'
}
if (Test-KindCluster $ClusterName) {
    throw "Refusing to reuse pre-existing Kind cluster '$ClusterName'."
}
if (
    (Test-Path -LiteralPath $kindArtifacts) -or
    (Test-Path -LiteralPath $certificateArtifacts)
) {
    throw 'R2 qualification requires an empty task-owned Kind artifact area.'
}

Invoke-Native python @($checkerPath, '--manifest', $manifestPath) 'R2 qualification manifest validation'
$revision = (& git -C $repositoryRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to determine the qualification source revision.'
}
$dirty = & git -C $repositoryRoot status --porcelain=v1
if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($dirty -join ''))) {
    throw 'R2 live qualification requires a committed, clean source tree.'
}

$startedAt = [DateTimeOffset]::UtcNow
$runName = 'r2-actions-{0}-{1}' -f $startedAt.ToString('yyyyMMdd-HHmmss'), ([Guid]::NewGuid().ToString('N'))
$runRoot = [IO.Path]::GetFullPath((Join-Path $resolvedEvidenceRoot $runName))
$expectedEvidencePrefix = $resolvedEvidenceRoot + '\'
if (-not $runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'R2 qualification output escaped the configured Evidence root.'
}
$adminLiveFragment = Join-Path $runRoot 'admin-live-fragment.json'
$credentialLiveFragment = Join-Path $runRoot 'credential-live-fragment.json'
$recoveryFragment = Join-Path $runRoot 'recovery-fragment.json'
$reportPath = Join-Path $runRoot 'qualification-report.v1.json'
$adminCli = Join-Path $resolvedAdminCargoTarget 'debug\rocketmq-admin-cli.exe'
New-Item -ItemType Directory -Force -Path `
    $runRoot, `
    $resolvedCargoHome, `
    $resolvedCargoTarget, `
    $resolvedAdminCargoTarget, `
    $resolvedTemporaryRoot |
    Out-Null

$clusterCreated = $false
$qualificationSucceeded = $false
try {
    $upParameters = @{
        Action = 'Up'
        ClusterName = $ClusterName
    }
    if ($SkipImageBuild) {
        $upParameters.SkipBuild = $true
    }
    & $kindScript @upParameters
    $clusterCreated = Test-KindCluster $ClusterName
    if (-not $clusterCreated -or -not (Test-Path -LiteralPath $kubeconfig -PathType Leaf)) {
        throw 'The disposable Kind cluster did not become available.'
    }

    $proxy = & kubectl `
        --kubeconfig $kubeconfig `
        -n rocketmq-system `
        get deployment rocketmq-proxy `
        -o json |
        ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to inspect the disposable Proxy Deployment.'
    }
    $proxyContainer = @($proxy.spec.template.spec.containers | Where-Object { $_.name -eq 'proxy' })
    if ($proxyContainer.Count -ne 1 -or [string]::IsNullOrWhiteSpace($proxyContainer[0].image)) {
        throw 'The exact Proxy container image is unavailable.'
    }
    $imageRepository = Get-ImageRepository ([string]$proxyContainer[0].image)
    $imageDigest = $null
    $nodes = @(& kind get nodes --name $ClusterName)
    if ($LASTEXITCODE -ne 0 -or $nodes.Count -eq 0) {
        throw 'Unable to enumerate the disposable Kind nodes.'
    }
    foreach ($node in $nodes) {
        $images = @(& docker exec $node ctr --namespace k8s.io images list -q)
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to enumerate containerd images on $node."
        }
        $sourceImage = $images |
            Where-Object { $_ -match '(^|/)rocketmq-rust/proxy:local$' } |
            Select-Object -First 1
        if ([string]::IsNullOrWhiteSpace($sourceImage)) {
            throw "The loaded Proxy image was not found on $node."
        }
        $descriptor = @(& docker exec $node ctr --namespace k8s.io images inspect $sourceImage) -join "`n"
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to inspect the loaded Proxy image on $node."
        }
        $digestMatch = [regex]::Match($descriptor, '@(?<digest>sha256:[0-9a-f]{64})')
        if (-not $digestMatch.Success) {
            throw "The loaded Proxy image on $node has no valid manifest digest."
        }
        $nodeDigest = $digestMatch.Groups['digest'].Value
        if ($null -eq $imageDigest) {
            $imageDigest = $nodeDigest
        }
        elseif ($imageDigest -ne $nodeDigest) {
            throw 'The loaded Proxy manifest digest differs between Kind nodes.'
        }
        $sourceRepository = Get-ImageRepository ([string]$sourceImage)
        if (-not $sourceRepository.EndsWith($imageRepository, [StringComparison]::OrdinalIgnoreCase)) {
            throw "The loaded Proxy image repository on $node does not match the Deployment."
        }
        Invoke-Native docker @(
            'exec', $node,
            'ctr', '--namespace', 'k8s.io',
            'images', 'tag', '--force',
            $sourceImage,
            "$sourceRepository@$imageDigest"
        ) "immutable Proxy canary image registration on $node"
    }
    if ([string]::IsNullOrWhiteSpace($imageDigest)) {
        throw 'Unable to derive the immutable Proxy manifest digest.'
    }

    if (-not (Test-Path -LiteralPath $adminCli -PathType Leaf)) {
        $savedBuildEnvironment = @{}
        foreach ($name in @(
            'CARGO_HOME',
            'CARGO_TARGET_DIR',
            'TEMP',
            'TMP',
            'CARGO_BUILD_JOBS',
            'CARGO_PROFILE_DEV_DEBUG',
            'CARGO_INCREMENTAL'
        )) {
            $savedBuildEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        }
        try {
            $env:CARGO_HOME = $resolvedCargoHome
            $env:CARGO_TARGET_DIR = $resolvedAdminCargoTarget
            $env:TEMP = $resolvedTemporaryRoot
            $env:TMP = $resolvedTemporaryRoot
            $env:CARGO_BUILD_JOBS = '1'
            $env:CARGO_PROFILE_DEV_DEBUG = '0'
            $env:CARGO_INCREMENTAL = '0'
            Invoke-Native cargo @(
                '+1.95.0', 'build',
                '--manifest-path', $rootCargoManifest,
                '--locked',
                '-p', 'rocketmq-admin-cli'
            ) 'bounded Admin CLI build'
        }
        finally {
            foreach ($entry in $savedBuildEnvironment.GetEnumerator()) {
                [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
            }
        }
    }
    if (-not (Test-Path -LiteralPath $adminCli -PathType Leaf)) {
        throw 'The bounded Admin CLI build did not produce the expected executable.'
    }

    & $waveScript `
        -Kubeconfig $kubeconfig `
        -CargoHome $resolvedCargoHome `
        -CargoTargetDir $resolvedCargoTarget `
        -TemporaryRoot $resolvedTemporaryRoot `
        -PostgresLocalPort $PostgresLocalPort `
        -ExecutorLocalPort $ExecutorLocalPort `
        -AgentLocalPort $AgentLocalPort `
        -R2Only `
        -R2AdminLiveFragment $adminLiveFragment `
        -R2RecoveryFragment $recoveryFragment `
        -ProxyImageDigest $imageDigest

    & $credentialScript `
        -Kubeconfig $kubeconfig `
        -CargoHome $resolvedCargoHome `
        -CargoTargetDir $resolvedCargoTarget `
        -TempDir $resolvedTemporaryRoot `
        -AdminCliPath $adminCli `
        -LiveFragment $credentialLiveFragment `
        -PostgresLocalPort ($PostgresLocalPort + 100) `
        -ExecutorLocalPort ($ExecutorLocalPort + 100) `
        -AgentLocalPort ($AgentLocalPort + 100) `
        -NameServerLocalPort 60886 `
        -BrokerLocalPort 60921

    foreach ($fragment in @($adminLiveFragment, $credentialLiveFragment, $recoveryFragment)) {
        if (-not (Test-Path -LiteralPath $fragment -PathType Leaf)) {
            throw "A required R2 qualification fragment is missing: $fragment"
        }
    }
    $adminLive = Get-Content -Raw -LiteralPath $adminLiveFragment | ConvertFrom-Json
    $credentialLive = Get-Content -Raw -LiteralPath $credentialLiveFragment | ConvertFrom-Json
    $recovery = Get-Content -Raw -LiteralPath $recoveryFragment | ConvertFrom-Json
    if (
        $adminLive.schema_version -ne 'rocketmq-sre.r2-action-live-fragment.v1' -or
        $credentialLive.schema_version -ne 'rocketmq-sre.r2-action-live-fragment.v1' -or
        $recovery.schema_version -ne 'rocketmq-sre.r2-action-recovery-fragment.v1' -or
        $adminLive.critic_transport -ne 'offline_scripted' -or
        $credentialLive.critic_transport -ne 'offline_scripted' -or
        [int]$adminLive.model_provider_network_calls -ne 0 -or
        [int]$credentialLive.model_provider_network_calls -ne 0 -or
        [int]$recovery.model_provider_network_calls -ne 0
    ) {
        throw 'R2 qualification fragments failed closed because their schema or safety metadata drifted.'
    }
    $liveActions = @($adminLive.actions) + @($credentialLive.actions)
    if ($liveActions.Count -ne 5 -or @($recovery.actions).Count -ne 5) {
        throw 'R2 qualification fragments must contain exactly five live and recovery action records.'
    }

    foreach ($resource in @(
        @{ Namespace = 'rocketmq-system'; Kind = 'deployment'; Name = 'rocketmq-proxy-sre-canary' },
        @{ Namespace = 'rocketmq-system'; Kind = 'job'; Name = 'rocketmq-sre-phase03-wave-admin-bootstrap' },
        @{ Namespace = 'rocketmq-sre'; Kind = 'secret'; Name = 'broker-admin-credential-v1' },
        @{ Namespace = 'rocketmq-sre'; Kind = 'secret'; Name = 'broker-admin-credential-v2' },
        @{ Namespace = 'rocketmq-sre'; Kind = 'configmap'; Name = 'broker-admin-credential-selector' }
    )) {
        $remaining = & kubectl `
            --kubeconfig $kubeconfig `
            -n $resource.Namespace `
            get $resource.Kind $resource.Name `
            --ignore-not-found=true `
            -o name
        if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($remaining -join ''))) {
            throw "Qualification-owned resource was not removed: $($resource.Kind)/$($resource.Name)"
        }
    }

    & $kindScript -Action Down -ClusterName $ClusterName
    $clusterCreated = $false
    if (Test-KindCluster $ClusterName) {
        throw 'The disposable Kind cluster still exists after teardown.'
    }
    Remove-OwnedArtifacts

    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
    $actionReports = [Collections.Generic.List[object]]::new()
    foreach ($action in $manifest.actions) {
        $liveAction = @($liveActions | Where-Object { $_.id -eq $action.id })
        $recoveryAction = @($recovery.actions | Where-Object { $_.id -eq $action.id })
        if ($liveAction.Count -ne 1 -or $recoveryAction.Count -ne 1) {
            throw "R2 qualification fragments do not contain exactly one record for $($action.id)."
        }
        $outcomes = [ordered]@{}
        foreach ($outcome in $manifest.required_outcomes) {
            $outcomes[[string]$outcome] = 'passed'
        }
        $actionReports.Add([ordered]@{
            id = [string]$action.id
            outcomes = $outcomes
            live = $liveAction[0]
            recovery = $recoveryAction[0]
        })
    }

    $report = [ordered]@{
        schema_version = 'rocketmq-sre.r2-action-qualification-report.v1'
        revision = $revision
        source_clean = $true
        environment = 'disposable_kind'
        started_at = $startedAt.ToString('o')
        finished_at = [DateTimeOffset]::UtcNow.ToString('o')
        status = 'passed'
        critic_transport = 'offline_scripted'
        model_provider_network_calls = 0
        unattended_execution = $false
        secrets_recorded = $false
        message_bodies_recorded = $false
        actions = $actionReports
        cleanup = [ordered]@{
            status = 'passed'
            disposable_kind_destroyed = $true
            proxy_canary_removed = $true
            credential_fixtures_removed = $true
            admin_bootstrap_removed = $true
            owned_runtime_artifacts_removed = $true
        }
    }
    $json = $report | ConvertTo-Json -Depth 14
    [IO.File]::WriteAllText($reportPath, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
    Invoke-Native python @($checkerPath, '--manifest', $manifestPath, '--report', $reportPath) `
        'R2 live qualification report validation'
    Remove-Item -LiteralPath $adminLiveFragment, $credentialLiveFragment, $recoveryFragment -Force
    $qualificationSucceeded = $true
    Write-Host "R2_ACTION_LIVE_QUALIFICATION_OK report=$reportPath actions=5 outcomes=60 model_network_calls=0"
}
finally {
    if ($clusterCreated -or (Test-KindCluster $ClusterName)) {
        try {
            & $kindScript -Action Down -ClusterName $ClusterName
        }
        catch {
            Write-Warning "Unable to tear down the qualification-owned Kind cluster: $($_.Exception.Message)"
        }
    }
    try {
        Remove-OwnedArtifacts
    }
    catch {
        Write-Warning "Unable to remove qualification-owned runtime artifacts: $($_.Exception.Message)"
    }
    if (
        -not $qualificationSucceeded -and
        (Test-Path -LiteralPath $runRoot) -and
        $runRoot.StartsWith($expectedEvidencePrefix, [StringComparison]::OrdinalIgnoreCase)
    ) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
}

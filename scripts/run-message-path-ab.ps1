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
    [ValidateSet("Validate", "Plan", "Run")]
    [string]$Mode = "Validate",
    [ValidateSet("kind", "target")]
    [string]$Backend = "kind",
    [string]$BaselineCommit,
    [string]$CandidateCommit,
    [string]$BaselineImageMap,
    [string]$CandidateImageMap,
    [string]$ImageProvenance,
    [string]$EffectiveConfig,
    [string]$TargetId,
    [string]$ClusterUid,
    [string]$DurabilityContract,
    [string]$Namespace = "rocketmq-system",
    [string]$ReleaseName = "rocketmq",
    [string]$NamesrvAddress = "127.0.0.1:19876",
    [int]$NamesrvLocalPort = 19876,
    [string]$TopicPrefix = "MessagePathAB",
    [int]$Repetitions = 5,
    [int]$Seed = 20260812,
    [string]$RunId,
    [string]$OutputRoot = "target/message-path-ab",
    [string]$Kubectl = "kubectl",
    [int]$MinimumFreeDiskGiB = 50,
    [int]$StableSeconds = 15,
    [switch]$AllowBusyHost
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$PythonScript = Join-Path $PSScriptRoot "message_path_ab.py"
$PolicyPath = Join-Path $PSScriptRoot "message-path-qualification-policy.json"
$Services = @("broker", "namesrv", "controller", "proxy", "mcp")

function Require-Command {
    param([Parameter(Mandatory)][string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "required command is unavailable: $Name"
    }
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory)][string]$Executable,
        [Parameter(Mandatory)][string[]]$Arguments,
        [switch]$AllowFailure
    )
    $content = (& $Executable @Arguments 2>&1 | Out-String).TrimEnd()
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "$Executable failed with exit code $exitCode`n$content"
    }
    [pscustomobject]@{ ExitCode = $exitCode; Output = $content }
}

function Resolve-RepositoryOutput {
    param([Parameter(Mandatory)][string]$Path)
    $resolved = if ([IO.Path]::IsPathRooted($Path)) {
        [IO.Path]::GetFullPath($Path)
    } else {
        [IO.Path]::GetFullPath((Join-Path $RepositoryRoot $Path))
    }
    $prefix = $RepositoryRoot.TrimEnd("\", "/") + [IO.Path]::DirectorySeparatorChar
    if (-not $resolved.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "OutputRoot must remain inside the repository"
    }
    $resolved
}

function Require-File {
    param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)][string]$Label)
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Label file is required"
    }
    (Resolve-Path -LiteralPath $Path).Path
}

function Read-ImageMap {
    param([Parameter(Mandatory)][string]$Path)
    $map = Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json -AsHashtable
    if ((@($map.Keys | Sort-Object) -join ",") -ne (@($Services | Sort-Object) -join ",")) {
        throw "image map must contain exactly five RocketMQ services"
    }
    $map
}

function Get-RunDirectory {
    param([Parameter(Mandatory)][string]$Root, [Parameter(Mandatory)][string]$Identity)
    Join-Path $Root $Identity
}

function Save-CommandOutput {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [Parameter(Mandatory)][string]$Path,
        [switch]$Required
    )
    $result = Invoke-Checked -Executable $Kubectl -Arguments $Arguments -AllowFailure
    [IO.File]::WriteAllText($Path, $result.Output + "`n", [Text.UTF8Encoding]::new($false))
    if ($result.ExitCode -ne 0) {
        [IO.File]::WriteAllText("$Path.failed", "$($result.ExitCode)`n", [Text.UTF8Encoding]::new($false))
        if ($Required) { throw "required Kubernetes evidence command failed: $($Arguments -join ' ')" }
    }
}

function Save-ClusterSnapshot {
    param([Parameter(Mandatory)][string]$Directory, [Parameter(Mandatory)][string]$Prefix)
    New-Item -ItemType Directory -Force -Path $Directory | Out-Null
    $podsPath = Join-Path $Directory "$Prefix-pods.json"
    Save-CommandOutput @("-n", $Namespace, "get", "pods", "-o", "json") $podsPath -Required
    Save-CommandOutput @("-n", $Namespace, "get", "statefulset,deployment", "-o", "json") (Join-Path $Directory "$Prefix-workloads.json") -Required
    Save-CommandOutput @("-n", $Namespace, "top", "pods", "--containers") (Join-Path $Directory "$Prefix-pod-resources.txt")
    Save-CommandOutput @("get", "nodes", "-o", "wide") (Join-Path $Directory "$Prefix-nodes.txt") -Required
    $pods = Get-Content -Raw -LiteralPath $podsPath | ConvertFrom-Json
    $resourceCommand = 'printf "memory_current="; cat /sys/fs/cgroup/memory.current 2>/dev/null || true; cat /proc/1/status; set -- /proc/1/task/*; echo task_count=$#; set -- /proc/1/fd/*; echo fd_count=$#'
    foreach ($pod in $pods.items) {
        $name = [string]$pod.metadata.name
        Save-CommandOutput @(
            "-n", $Namespace, "exec", $name, "--", "/bin/sh", "-c", $resourceCommand
        ) (Join-Path $Directory "$Prefix-$name-process.txt") -Required
    }
}

function Assert-NoContainerRestart {
    $pods = (Invoke-Checked $Kubectl @("-n", $Namespace, "get", "pods", "-o", "json")).Output | ConvertFrom-Json
    foreach ($pod in $pods.items) {
        foreach ($status in @($pod.status.containerStatuses)) {
            if ([int]$status.restartCount -ne 0) {
                throw "container restart detected before measurement: $($pod.metadata.name)/$($status.name)"
            }
        }
    }
}

function Wait-WorkloadsStable {
    $workloads = @(
        "statefulset/$ReleaseName-broker",
        "statefulset/$ReleaseName-namesrv",
        "statefulset/$ReleaseName-controller",
        "deployment/$ReleaseName-proxy",
        "deployment/$ReleaseName-mcp"
    )
    foreach ($workload in $workloads) {
        Invoke-Checked $Kubectl @("-n", $Namespace, "rollout", "status", $workload, "--timeout=300s") | Out-Null
    }
    $first = (Invoke-Checked $Kubectl @("-n", $Namespace, "get", "statefulset,deployment", "-o", "json")).Output
    Start-Sleep -Seconds $StableSeconds
    $second = (Invoke-Checked $Kubectl @("-n", $Namespace, "get", "statefulset,deployment", "-o", "json")).Output
    $firstState = $first | ConvertFrom-Json
    $secondState = $second | ConvertFrom-Json
    foreach ($state in @($firstState.items) + @($secondState.items)) {
        if ([int]$state.status.readyReplicas -ne [int]$state.spec.replicas) {
            throw "workload is not fully Ready: $($state.metadata.name)"
        }
    }
    Assert-NoContainerRestart
}

function Set-SubjectImages {
    param([Parameter(Mandatory)][hashtable]$Images)
    foreach ($service in @("broker", "namesrv", "controller")) {
        Invoke-Checked $Kubectl @(
            "-n", $Namespace, "set", "image", "statefulset/$ReleaseName-$service", "$service=$($Images[$service])"
        ) | Out-Null
    }
    foreach ($service in @("proxy", "mcp")) {
        Invoke-Checked $Kubectl @(
            "-n", $Namespace, "set", "image", "deployment/$ReleaseName-$service", "$service=$($Images[$service])"
        ) | Out-Null
    }
    Wait-WorkloadsStable
    foreach ($service in $Services) {
        $kind = if ($service -in @("broker", "namesrv", "controller")) { "statefulset" } else { "deployment" }
        $actual = (Invoke-Checked $Kubectl @(
            "-n", $Namespace, "get", "$kind/$ReleaseName-$service", "-o", "jsonpath={.spec.template.spec.containers[?(@.name=='$service')].image}"
        )).Output
        if ($actual -ne $Images[$service]) {
            throw "$service workload image differs from the requested immutable reference"
        }
    }
}

function Assert-HostReady {
    $drive = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($RepositoryRoot))
    $freeGiB = [math]::Floor($drive.AvailableFreeSpace / 1GB)
    if ($freeGiB -lt $MinimumFreeDiskGiB) {
        throw "free disk ${freeGiB}GiB is below the ${MinimumFreeDiskGiB}GiB A/B minimum"
    }
    if (-not $AllowBusyHost -and $IsWindows) {
        $cpu = Get-CimInstance Win32_Processor | Measure-Object -Property LoadPercentage -Average
        if ($cpu.Average -gt 20) {
            throw "host CPU is not idle enough for paired A/B measurement: $([math]::Round($cpu.Average, 2))%"
        }
    }
    if ($Backend -eq "kind") {
        Require-Command "docker"
        Invoke-Checked "docker" @("info") | Out-Null
    }
}

function Wait-TcpPort {
    param([Parameter(Mandatory)][int]$Port)
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(30)
    do {
        $client = [Net.Sockets.TcpClient]::new()
        try {
            $pending = $client.ConnectAsync("127.0.0.1", $Port)
            if ($pending.Wait(500) -and $client.Connected) { return }
        } catch {
        } finally {
            $client.Dispose()
        }
        Start-Sleep -Milliseconds 250
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "NameServer port-forward did not become ready on 127.0.0.1:$Port"
}

Require-Command "python"
if ($Mode -eq "Validate") {
    Invoke-Checked "python" @($PythonScript, "--help") | Out-Null
    Invoke-Checked "python" @((Join-Path $PSScriptRoot "message_path_qualification.py"), "validate-policy") | Out-Null
    Write-Host "MESSAGE_PATH_AB_RUNNER_VALID"
    return
}

if ([string]::IsNullOrWhiteSpace($RunId)) {
    $RunId = "message-path-ab-$([DateTimeOffset]::UtcNow.ToString('yyyyMMddTHHmmssZ'))"
}
$baselineMapPath = Require-File $BaselineImageMap "baseline image map"
$candidateMapPath = Require-File $CandidateImageMap "candidate image map"
$provenancePath = Require-File $ImageProvenance "image provenance"
$configPath = Require-File $EffectiveConfig "effective configuration"
$outputBase = Resolve-RepositoryOutput $OutputRoot
$runDirectory = Get-RunDirectory $outputBase $RunId
if (Test-Path -LiteralPath $runDirectory) {
    throw "A/B run directory already exists and will not be overwritten: $runDirectory"
}
New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null
$planPath = Join-Path $runDirectory "ab-plan.json"

if ($Mode -eq "Run") {
    if ($PSVersionTable.PSVersion.Major -lt 7) { throw "Run mode requires PowerShell 7 or newer" }
    Require-Command $Kubectl
    Assert-HostReady
    Invoke-Checked $Kubectl @("cluster-info") | Out-Null
    Invoke-Checked $Kubectl @("-n", $Namespace, "get", "statefulset/$ReleaseName-broker") | Out-Null
    if ([string]::IsNullOrWhiteSpace($ClusterUid)) {
        $ClusterUid = (Invoke-Checked $Kubectl @("get", "namespace", $Namespace, "-o", "jsonpath={.metadata.uid}")).Output
    }
}
if ([string]::IsNullOrWhiteSpace($ClusterUid)) { throw "ClusterUid is required in Plan mode" }
if ([string]::IsNullOrWhiteSpace($TargetId)) { throw "TargetId is required" }
if ([string]::IsNullOrWhiteSpace($DurabilityContract)) { throw "DurabilityContract is required" }
$driverCommit = (Invoke-Checked "git" @("rev-parse", "HEAD")).Output.Trim()
$dirty = (Invoke-Checked "git" @("status", "--porcelain")).Output
if (-not [string]::IsNullOrWhiteSpace($dirty)) { throw "paired A/B requires a clean benchmark-driver worktree" }

Invoke-Checked "python" @(
    $PythonScript, "plan",
    "--run-id", $RunId,
    "--baseline-commit", $BaselineCommit,
    "--candidate-commit", $CandidateCommit,
    "--driver-commit", $driverCommit,
    "--baseline-image-map", $baselineMapPath,
    "--candidate-image-map", $candidateMapPath,
    "--image-provenance", $provenancePath,
    "--effective-config", $configPath,
    "--target-id", $TargetId,
    "--cluster-uid", $ClusterUid,
    "--namesrv", $NamesrvAddress,
    "--topic-prefix", $TopicPrefix,
    "--durability-contract", $DurabilityContract,
    "--repetitions", "$Repetitions",
    "--seed", "$Seed",
    "--output", $planPath
) | Out-Null
if ($Mode -eq "Plan") {
    Write-Host "MESSAGE_PATH_AB_PLAN_READY plan=$planPath"
    return
}

$baselineImages = Read-ImageMap $baselineMapPath
$candidateImages = Read-ImageMap $candidateMapPath
$plan = Get-Content -Raw -LiteralPath $planPath | ConvertFrom-Json
$portForwardOut = Join-Path $runDirectory "port-forward.stdout.log"
$portForwardErr = Join-Path $runDirectory "port-forward.stderr.log"
$startParameters = @{
    FilePath = $Kubectl
    ArgumentList = @("-n", $Namespace, "port-forward", "service/$ReleaseName-namesrv-discovery", "${NamesrvLocalPort}:9876")
    PassThru = $true
    RedirectStandardOutput = $portForwardOut
    RedirectStandardError = $portForwardErr
}
if ($IsWindows) { $startParameters.WindowStyle = "Hidden" }
$portForward = Start-Process @startParameters
try {
    Wait-TcpPort $NamesrvLocalPort
    $currentRole = ""
    foreach ($arm in $plan.arms) {
        $role = [string]$arm.role
        if ($role -ne $currentRole) {
            $images = if ($role -eq "baseline") { $baselineImages } else { $candidateImages }
            Set-SubjectImages $images
            $currentRole = $role
        }
        $armDirectory = Join-Path $runDirectory ("arms/{0:D3}-{1}-{2}-{3}" -f $arm.index, $role, $arm.phase, $arm.ordinal)
        Save-ClusterSnapshot $armDirectory "before"
        $execution = Invoke-Checked "python" @(
            $PythonScript, "execute-arm", "--plan", $planPath,
            "--arm-index", "$($arm.index)", "--output-root", $runDirectory
        ) -AllowFailure
        [IO.File]::WriteAllText((Join-Path $armDirectory "runner.log"), $execution.Output + "`n", [Text.UTF8Encoding]::new($false))
        Save-ClusterSnapshot $armDirectory "after"
        Assert-NoContainerRestart
        if ($execution.ExitCode -ne 0) {
            throw "A/B arm $($arm.index) failed; its evidence was retained and will not be retried"
        }
    }
    Invoke-Checked "python" @($PythonScript, "assemble", "--plan", $planPath, "--output-root", $runDirectory) | Out-Null
    Invoke-Checked "python" @($PythonScript, "validate", "--plan", $planPath, "--output-root", $runDirectory) | Out-Null
    Write-Host "MESSAGE_PATH_AB_COMPLETE output=$runDirectory"
} finally {
    if ($null -ne $portForward -and -not $portForward.HasExited) {
        Stop-Process -Id $portForward.Id -Force -ErrorAction SilentlyContinue
        $portForward.WaitForExit(5000) | Out-Null
    }
}

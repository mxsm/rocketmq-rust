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
    [ValidateSet("Plan", "Smoke", "Full")]
    [string]$Mode = "Plan",
    [string]$JavaRocketmqHome = "",
    [string]$JavaHome = $env:JAVA_HOME,
    [ValidateRange(0, 168)]
    [int]$SteadyReadHours = 24,
    [string]$TlsReloadScript = "",
    [ValidateSet(0, 100, 1000)]
    [int]$TlsReloadIterations = 0,
    [string]$ChunkScenarioScript = "",
    [string]$OutputRoot = "target/namesrv-soak"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$outputRootPath = [System.IO.Path]::GetFullPath((Join-Path $workspaceRoot $OutputRoot))
$runRoot = Join-Path $outputRootPath "$($Mode.ToLowerInvariant())-$runStamp"
$routeScript = Join-Path $PSScriptRoot "run_namesrv_route_e2e_bench.ps1"
$mixedScript = Join-Path $PSScriptRoot "run_namesrv_mixed_parity_matrix.ps1"
$results = [System.Collections.Generic.List[object]]::new()

$scenarioPlan = @(
    [ordered]@{ name = "registration-chunks"; topics = 70000; brokers = 300; driver = "external-broker" },
    [ordered]@{ name = "expiry"; brokers = 10000; expiredPercent = @(10, 50, 100); driver = "criterion" },
    [ordered]@{ name = "route-capacity"; topics = @(20000, 100000); connections = @(64, 256, 1024); zonePercent = 10; driver = "route-e2e" },
    [ordered]@{ name = "kv-faults"; failures = @("create", "write", "fsync", "replace"); driver = "cargo-test" },
    [ordered]@{ name = "tls-reload"; iterations = @(100, 1000); driver = "deployment-specific" },
    [ordered]@{ name = "steady-read"; hours = $SteadyReadHours; driver = "route-e2e-repeat" }
)

function Add-Result {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Status,
        [string]$Detail = ""
    )

    $results.Add([ordered]@{
            name = $Name
            status = $Status
            detail = $Detail
            completedAtUtc = [DateTime]::UtcNow.ToString("o")
        }) | Out-Null
}

function Invoke-CheckedNative {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    Write-Host "[$Name] $FilePath $($Arguments -join ' ')"
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        Add-Result -Name $Name -Status "failed" -Detail "exit code $LASTEXITCODE"
        throw "$Name failed with exit code $LASTEXITCODE"
    }
    Add-Result -Name $Name -Status "passed"
}

function Invoke-RouteProfile {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][ValidateSet("rust", "java")][string]$Server,
        [Parameter(Mandatory = $true)][string]$Profile,
        [Parameter(Mandatory = $true)][string]$Workload
    )

    $arguments = @{
        Server = $Server
        Profile = $Profile
        Workload = $Workload
        JavaHome = $JavaHome
        OutputRoot = (Join-Path $runRoot "route")
    }
    if ($Server -eq "java") {
        $arguments.JavaRocketmqHome = $JavaRocketmqHome
    }
    Write-Host "[$Name] route E2E $Server/$Profile/$Workload"
    & $routeScript @arguments
    if ($LASTEXITCODE -ne 0) {
        Add-Result -Name $Name -Status "failed" -Detail "exit code $LASTEXITCODE"
        throw "$Name failed with exit code $LASTEXITCODE"
    }
    Add-Result -Name $Name -Status "passed"
}

if ($Mode -eq "Plan") {
    [ordered]@{
        mode = $Mode
        workspace = $workspaceRoot
        scenarios = $scenarioPlan
        note = "Plan mode performs no load and creates no artifacts."
    } | ConvertTo-Json -Depth 8
    return
}

New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
$startedAt = [DateTime]::UtcNow

try {
    Invoke-CheckedNative -Name "namesrv-tests" -FilePath "cargo" -Arguments @(
        "test", "-p", "rocketmq-namesrv", "--lib", "--tests"
    )
    Invoke-CheckedNative -Name "observability-tests" -FilePath "cargo" -Arguments @(
        "test", "-p", "rocketmq-observability", "namesrv"
    )
    Invoke-RouteProfile -Name "rust-route-smoke" -Server "rust" -Profile "p3" -Workload "smoke"

    if (-not [string]::IsNullOrWhiteSpace($JavaRocketmqHome)) {
        Invoke-RouteProfile -Name "java-route-smoke" -Server "java" -Profile "java-5.5.0" -Workload "smoke"
        Write-Host "[mixed-parity] running both Java/Rust directions"
        & $mixedScript -Mode "rust-namesrv-java-broker" -JavaRocketmqHome $JavaRocketmqHome -JavaHome $JavaHome
        if ($LASTEXITCODE -ne 0) { throw "rust-namesrv-java-broker parity failed with exit code $LASTEXITCODE" }
        & $mixedScript -Mode "java-namesrv-rust-broker" -JavaRocketmqHome $JavaRocketmqHome -JavaHome $JavaHome
        if ($LASTEXITCODE -ne 0) { throw "java-namesrv-rust-broker parity failed with exit code $LASTEXITCODE" }
        Add-Result -Name "mixed-parity" -Status "passed"
    }
    else {
        Add-Result -Name "java-and-mixed-parity" -Status "skipped" -Detail "JavaRocketmqHome was not supplied"
    }

    if ($Mode -eq "Full") {
        foreach ($workload in @("topics-20k-width-1", "topics-100k-width-4", "topics-100k-width-16")) {
            Invoke-RouteProfile -Name "rust-route-$workload" -Server "rust" -Profile "p3" -Workload $workload
            if (-not [string]::IsNullOrWhiteSpace($JavaRocketmqHome)) {
                Invoke-RouteProfile -Name "java-route-$workload" -Server "java" -Profile "java-5.5.0" -Workload $workload
            }
        }

        Invoke-CheckedNative -Name "expiry-10-50-100" -FilePath "cargo" -Arguments @(
            "bench", "-p", "rocketmq-namesrv", "--bench", "namesrv_write_recovery_bench"
        )
        Invoke-CheckedNative -Name "kv-durable-fault-tests" -FilePath "cargo" -Arguments @(
            "test", "-p", "rocketmq-namesrv", "kvconfig::persistence::tests"
        )

        if (-not [string]::IsNullOrWhiteSpace($ChunkScenarioScript)) {
            $resolvedChunkScript = (Resolve-Path -LiteralPath $ChunkScenarioScript).Path
            & $resolvedChunkScript -TopicCount 70000 -BrokerCount 300 -OutputRoot $runRoot
            if ($LASTEXITCODE -ne 0) { throw "chunk scenario failed with exit code $LASTEXITCODE" }
            Add-Result -Name "registration-chunks" -Status "passed"
        }
        else {
            Add-Result -Name "registration-chunks" -Status "skipped" -Detail "deployment Broker chunk driver was not supplied"
        }

        if ($TlsReloadIterations -gt 0 -and -not [string]::IsNullOrWhiteSpace($TlsReloadScript)) {
            $resolvedTlsScript = (Resolve-Path -LiteralPath $TlsReloadScript).Path
            & $resolvedTlsScript -Iterations $TlsReloadIterations -OutputRoot $runRoot
            if ($LASTEXITCODE -ne 0) { throw "TLS reload scenario failed with exit code $LASTEXITCODE" }
            Add-Result -Name "tls-reload-$TlsReloadIterations" -Status "passed"
        }
        else {
            Add-Result -Name "tls-reload" -Status "skipped" -Detail "hot-reload driver/iteration count was not supplied"
        }

        if ($SteadyReadHours -gt 0) {
            $steadyDeadline = (Get-Date).AddHours($SteadyReadHours)
            $steadyIteration = 0
            while ((Get-Date) -lt $steadyDeadline) {
                $steadyIteration++
                Invoke-RouteProfile -Name "steady-read-$steadyIteration" -Server "rust" -Profile "p3-steady" -Workload "topics-20k-width-1"
            }
            Add-Result -Name "steady-read" -Status "passed" -Detail "$SteadyReadHours hour target; $steadyIteration completed profiles"
        }
        else {
            Add-Result -Name "steady-read" -Status "skipped" -Detail "SteadyReadHours is zero"
        }
    }
}
finally {
    $commit = (& git -C $workspaceRoot rev-parse HEAD 2>$null)
    $summary = [ordered]@{
        schemaVersion = 1
        mode = $Mode
        commit = if ($LASTEXITCODE -eq 0) { "$commit".Trim() } else { "unknown" }
        startedAtUtc = $startedAt.ToString("o")
        completedAtUtc = [DateTime]::UtcNow.ToString("o")
        javaRocketmqHome = if ([string]::IsNullOrWhiteSpace($JavaRocketmqHome)) { $null } else { $JavaRocketmqHome }
        steadyReadHours = $SteadyReadHours
        scenarios = $scenarioPlan
        results = $results
        releaseGateNote = "Skipped external scenarios are not release passes. Review raw route, Criterion, process, metric, and digest artifacts against the NameServer SLO."
    }
    $summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $runRoot "soak-summary.json") -Encoding utf8
}

Write-Host "NameServer $Mode run artifacts: $runRoot"

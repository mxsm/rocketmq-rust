[CmdletBinding()]
param(
    [string]$OutputDirectory = "target/runtime-audit",
    [string]$BaselineDirectory = "target/runtime-baseline/before",
    [string]$BoundaryBaselineFile = "scripts/runtime-audit-baseline.json",
    [switch]$EnforceBoundaryBaseline,
    [switch]$SkipBaseline
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$auditRoot = Join-Path $workspaceRoot $OutputDirectory
$baselineRoot = Join-Path $workspaceRoot $BaselineDirectory
$boundaryBaselinePath = if ([System.IO.Path]::IsPathRooted($BoundaryBaselineFile)) {
    $BoundaryBaselineFile
}
else {
    Join-Path $workspaceRoot $BoundaryBaselineFile
}
$script:testScopeCache = @{}
$script:benchmarkScopeCache = @{}

function New-DirectoryIfMissing {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function Invoke-Ripgrep {
    param([Parameter(Mandatory = $true)][string]$Pattern)

    $output = & rg --json --glob "*.rs" --glob "!target/**" $Pattern $workspaceRoot 2>$null
    if ($LASTEXITCODE -gt 1) {
        throw "rg failed while scanning pattern: $Pattern"
    }
    return @($output)
}

function Get-BraceDelta {
    param([AllowNull()][string]$Line)

    if ($null -eq $Line) {
        return 0
    }
    return ([regex]::Matches($Line, "\{").Count - [regex]::Matches($Line, "\}").Count)
}

function Add-TestScopeRange {
    param(
        [System.Collections.Generic.List[object]]$Ranges,
        [Parameter(Mandatory = $true)][int]$Start,
        [Parameter(Mandatory = $true)][int]$End
    )

    $Ranges.Add([pscustomobject]@{
            Start = $Start
            End = $End
        }) | Out-Null
}

function Get-SourceTestScopeRanges {
    param([Parameter(Mandatory = $true)][string]$RelativePath)

    if ($script:testScopeCache.ContainsKey($RelativePath)) {
        return @($script:testScopeCache[$RelativePath])
    }

    $ranges = New-Object System.Collections.Generic.List[object]
    $absolutePath = Join-Path $workspaceRoot $RelativePath
    if (-not (Test-Path -LiteralPath $absolutePath)) {
        $script:testScopeCache[$RelativePath] = @()
        return @()
    }

    $lines = @(Get-Content -LiteralPath $absolutePath -Encoding utf8)
    $pendingCfgTest = $false
    $pendingTestAttribute = $false
    $activeStart = $null
    $activeDepth = 0
    $activeSawBrace = $false

    for ($index = 0; $index -lt $lines.Count; $index++) {
        $line = $lines[$index]
        $lineNumber = $index + 1

        if ($null -ne $activeStart) {
            if ($line.Contains("{")) {
                $activeSawBrace = $true
            }
            if ($activeSawBrace) {
                $activeDepth += Get-BraceDelta -Line $line
                if ($activeDepth -le 0) {
                    Add-TestScopeRange -Ranges $ranges -Start $activeStart -End $lineNumber
                    $activeStart = $null
                    $activeDepth = 0
                    $activeSawBrace = $false
                }
            }
            continue
        }

        if ($line -match "#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]") {
            $pendingCfgTest = $true
            continue
        }
        if ($line -match "#\s*\[\s*(tokio::)?test\b") {
            $pendingTestAttribute = $true
            continue
        }

        if ($pendingCfgTest -and $line -match "\bmod\s+\w+") {
            $activeStart = $lineNumber
            $activeDepth = 0
            $activeSawBrace = $false
            if ($line.Contains("{")) {
                $activeSawBrace = $true
                $activeDepth += Get-BraceDelta -Line $line
                if ($activeDepth -le 0) {
                    Add-TestScopeRange -Ranges $ranges -Start $activeStart -End $lineNumber
                    $activeStart = $null
                    $activeSawBrace = $false
                }
            }
            $pendingCfgTest = $false
            continue
        }

        if ($pendingTestAttribute -and $line -match "\b(async\s+)?fn\s+\w+") {
            $activeStart = $lineNumber
            $activeDepth = 0
            $activeSawBrace = $false
            if ($line.Contains("{")) {
                $activeSawBrace = $true
                $activeDepth += Get-BraceDelta -Line $line
                if ($activeDepth -le 0) {
                    Add-TestScopeRange -Ranges $ranges -Start $activeStart -End $lineNumber
                    $activeStart = $null
                    $activeSawBrace = $false
                }
            }
            $pendingTestAttribute = $false
            continue
        }

        if ($pendingCfgTest -and -not [string]::IsNullOrWhiteSpace($line) -and $line -notmatch "^\s*#") {
            $pendingCfgTest = $false
        }
        if ($pendingTestAttribute -and -not [string]::IsNullOrWhiteSpace($line) -and $line -notmatch "^\s*#") {
            $pendingTestAttribute = $false
        }
    }

    if ($null -ne $activeStart) {
        Add-TestScopeRange -Ranges $ranges -Start $activeStart -End $lines.Count
    }

    $result = @($ranges.ToArray())
    $script:testScopeCache[$RelativePath] = $result
    return $result
}

function Test-LineInTestScope {
    param(
        [Parameter(Mandatory = $true)][string]$RelativePath,
        [Parameter(Mandatory = $true)][int]$Line
    )

    foreach ($range in (Get-SourceTestScopeRanges -RelativePath $RelativePath)) {
        if ($Line -ge $range.Start -and $Line -le $range.End) {
            return $true
        }
    }
    return $false
}

function Get-SourceBenchmarkScopeRanges {
    param([Parameter(Mandatory = $true)][string]$RelativePath)

    if ($script:benchmarkScopeCache.ContainsKey($RelativePath)) {
        return @($script:benchmarkScopeCache[$RelativePath])
    }

    $ranges = New-Object System.Collections.Generic.List[object]
    $absolutePath = Join-Path $workspaceRoot $RelativePath
    if (-not (Test-Path -LiteralPath $absolutePath)) {
        $script:benchmarkScopeCache[$RelativePath] = @()
        return @()
    }

    $lines = @(Get-Content -LiteralPath $absolutePath -Encoding utf8)
    $activeStart = $null
    $activeDepth = 0
    $activeSawBrace = $false

    for ($index = 0; $index -lt $lines.Count; $index++) {
        $line = $lines[$index]
        $lineNumber = $index + 1

        if ($null -ne $activeStart) {
            if ($line.Contains("{")) {
                $activeSawBrace = $true
            }
            if ($activeSawBrace) {
                $activeDepth += Get-BraceDelta -Line $line
                if ($activeDepth -le 0) {
                    Add-TestScopeRange -Ranges $ranges -Start $activeStart -End $lineNumber
                    $activeStart = $null
                    $activeDepth = 0
                    $activeSawBrace = $false
                }
            }
            continue
        }

        if (($line -match "\b(pub(\([^)]*\))?\s+)?mod\s+bench_support\b" -and $line -notmatch ";\s*$") -or
            $line -match "\b(pub(\([^)]*\))?\s+)?(async\s+)?fn\s+run_\w*_lifecycle_probe\b") {
            $activeStart = $lineNumber
            $activeDepth = 0
            $activeSawBrace = $false
            if ($line.Contains("{")) {
                $activeSawBrace = $true
                $activeDepth += Get-BraceDelta -Line $line
                if ($activeDepth -le 0) {
                    Add-TestScopeRange -Ranges $ranges -Start $activeStart -End $lineNumber
                    $activeStart = $null
                    $activeSawBrace = $false
                }
            }
        }
    }

    if ($null -ne $activeStart) {
        Add-TestScopeRange -Ranges $ranges -Start $activeStart -End $lines.Count
    }

    $result = @($ranges.ToArray())
    $script:benchmarkScopeCache[$RelativePath] = $result
    return $result
}

function Test-LineInBenchmarkScope {
    param(
        [Parameter(Mandatory = $true)][string]$RelativePath,
        [Parameter(Mandatory = $true)][int]$Line
    )

    foreach ($range in (Get-SourceBenchmarkScopeRanges -RelativePath $RelativePath)) {
        if ($Line -ge $range.Start -and $Line -le $range.End) {
            return $true
        }
    }
    return $false
}

function Get-RuntimeAuditScope {
    param(
        [Parameter(Mandatory = $true)][string]$RelativePath,
        [Parameter(Mandatory = $true)][int]$Line
    )

    $normalized = $RelativePath.Replace("\", "/")
    if ($normalized -match "(^|/)benches/") {
        return "benchmark"
    }
    if (Test-LineInBenchmarkScope -RelativePath $normalized -Line $Line) {
        return "benchmark"
    }
    if ($normalized -match "(^|/)tests/") {
        return "test"
    }
    if ($normalized -match "(^|/)examples/") {
        return "example"
    }
    if (Test-LineInTestScope -RelativePath $normalized -Line $Line) {
        return "test"
    }
    return "production"
}

function Test-RustCommentOnlyLine {
    param([AllowNull()][string]$Line)

    if ($null -eq $Line) {
        return $false
    }

    $trimmed = $Line.TrimStart()
    return $trimmed.StartsWith("//") -or
        $trimmed.StartsWith("/*") -or
        $trimmed.StartsWith("*") -or
        $trimmed.StartsWith("*/")
}

function Convert-RgJsonLine {
    param([Parameter(Mandatory = $true)][string]$Line)

    if ([string]::IsNullOrWhiteSpace($Line)) {
        return $null
    }

    $event = $Line | ConvertFrom-Json
    if ($event.type -ne "match") {
        return $null
    }

    $path = $event.data.path.text
    $lineNumber = [int]$event.data.line_number
    $lineText = ($event.data.lines.text -replace "`r?`n$", "").Trim()
    if (Test-RustCommentOnlyLine -Line $lineText) {
        return $null
    }
    $resolvedPath = (Resolve-Path -LiteralPath $path).Path
    $relativePath = $resolvedPath
    if ($resolvedPath.StartsWith($workspaceRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        $relativePath = $resolvedPath.Substring($workspaceRoot.Length).TrimStart("\", "/")
    }
    $relativePath = $relativePath.Replace("\", "/")
    $crate = ($relativePath -split "/")[0]
    $scope = Get-RuntimeAuditScope -RelativePath $relativePath -Line $lineNumber

    [pscustomobject]@{
        Crate = $crate
        Scope = $scope
        Path = $relativePath
        Line = $lineNumber
        Text = $lineText
    }
}

function Get-RuntimeMatches {
    param([Parameter(Mandatory = $true)][string]$Pattern)

    $matches = @()
    foreach ($line in (Invoke-Ripgrep -Pattern $Pattern)) {
        $match = Convert-RgJsonLine -Line $line
        if ($null -ne $match) {
            $matches += $match
        }
    }
    return @($matches)
}

function ConvertTo-MarkdownInlineCode {
    param([AllowNull()][string]$Text)

    $tick = [char]0x60
    $escaped = if ($null -eq $Text) { "" } else { $Text.Replace("|", "\|").Replace("$tick", "$tick$tick") }
    return "$tick$escaped$tick"
}

function Write-MarkdownReport {
    param(
        [Parameter(Mandatory = $true)][string]$Title,
        [AllowNull()][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $Matches = @($Matches | Where-Object { $null -ne $_ })

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# $Title")
    $lines.Add("")
    $lines.Add("Generated: $(Get-Date -Format o)")
    $lines.Add("")
    $lines.Add("Total matches: $($Matches.Count)")
    $lines.Add("")

    if ($Matches.Count -gt 0) {
        $lines.Add("| Scope | Crate | File | Line | Code |")
        $lines.Add("|---|---|---|---:|---|")
        foreach ($match in $Matches) {
            $fileCell = ConvertTo-MarkdownInlineCode -Text $match.Path
            $code = ConvertTo-MarkdownInlineCode -Text $match.Text
            $lines.Add("| $($match.Scope) | $($match.Crate) | $fileCell | $($match.Line) | $code |")
        }
    }

    $lines -join [Environment]::NewLine | Out-File -Encoding utf8 $Path
}

function Get-ProductionMatches {
    param([AllowNull()][array]$Matches)

    $Matches = @($Matches | Where-Object { $null -ne $_ })
    return @($Matches | Where-Object { $_.Scope -eq "production" })
}

function Get-CountForCrate {
    param(
        [AllowNull()][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Crate
    )

    $Matches = @($Matches | Where-Object { $null -ne $_ })
    return @($Matches | Where-Object { $_.Crate -eq $Crate }).Count
}

function Get-CountForScope {
    param(
        [AllowNull()][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Scope
    )

    $Matches = @($Matches | Where-Object { $null -ne $_ })
    return @($Matches | Where-Object { $_.Scope -eq $Scope }).Count
}

function Get-CountForCrateAndScope {
    param(
        [AllowNull()][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Crate,
        [Parameter(Mandatory = $true)][string]$Scope
    )

    $Matches = @($Matches | Where-Object { $null -ne $_ })
    return @($Matches | Where-Object { $_.Crate -eq $Crate -and $_.Scope -eq $Scope }).Count
}

function Write-BaselineJson {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][hashtable]$Metrics
    )

    $payload = [ordered]@{
        generated_at = (Get-Date -Format o)
        phase = "before"
        source = "scripts/runtime-audit.ps1"
        metrics = $Metrics
    }

    $payload | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 $Path
}

function Get-StringSha256Prefix {
    param(
        [AllowNull()][string]$Text,
        [int]$Length = 16
    )

    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($(if ($null -eq $Text) { "" } else { $Text }))
        $hash = $sha.ComputeHash($bytes)
        return ([System.BitConverter]::ToString($hash)).Replace("-", "").ToLowerInvariant().Substring(0, $Length)
    }
    finally {
        $sha.Dispose()
    }
}

function Get-BoundaryKind {
    param([Parameter(Mandatory = $true)][string]$Category)

    switch ($Category) {
        "task-group-root-sites" { return "task-group-root" }
        "current-runtime-adapter-sites" { return "current-runtime-adapter" }
        "raw-tokio-spawn-sites" { return "raw-tokio-spawn" }
        "raw-blocking-executor-sites" { return "raw-blocking-executor" }
        "scheduler-sites" { return "scheduler-site" }
        "dedicated-thread-sites" { return "dedicated-thread" }
        default { return $Category }
    }
}

function Get-MigrationPhaseForPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $normalized = $Path.Replace("\", "/")
    if ($normalized -match "^rocketmq-runtime/") { return "PR-2-runtime-primitives" }
    if ($normalized -match "^rocketmq-client/") { return "PR-3-service-context-api" }
    if ($normalized -match "^rocketmq-remoting/") { return "PR-4-remoting-lifecycle" }
    if ($normalized -match "^rocketmq-namesrv/") { return "PR-5-namesrv-shutdown" }
    if ($normalized -match "^rocketmq-broker/") { return "PR-6-broker-report-tree" }
    if ($normalized -match "^rocketmq-(store|controller|auth|common|tieredstore)/") { return "PR-7-store-controller-auth-blocking" }
    return "legacy-compatibility"
}

function Get-BoundaryFingerprint {
    param(
        [Parameter(Mandatory = $true)][string]$Category,
        [Parameter(Mandatory = $true)]$Match,
        [Parameter(Mandatory = $true)][string]$Kind
    )

    $normalizedText = ($Match.Text -replace "\s+", " ").Trim()
    $textHash = Get-StringSha256Prefix -Text $normalizedText
    return "$Category|$Kind|$($Match.Path)|$textHash"
}

function New-BoundaryDisposition {
    param(
        [Parameter(Mandatory = $true)][string]$Disposition,
        [Parameter(Mandatory = $true)][bool]$ActionRequired,
        [Parameter(Mandatory = $true)][string]$Reason,
        [Parameter(Mandatory = $true)][string]$MigrationPhase
    )

    [pscustomobject]@{
        Disposition = $Disposition
        ActionRequired = $ActionRequired
        Reason = $Reason
        MigrationPhase = $MigrationPhase
    }
}

function Get-BoundaryDisposition {
    param(
        [Parameter(Mandatory = $true)][string]$Category,
        [Parameter(Mandatory = $true)]$Match
    )

    $path = $Match.Path.Replace("\", "/")
    $phase = Get-MigrationPhaseForPath -Path $path

    if ($Match.Scope -ne "production") {
        return New-BoundaryDisposition `
            -Disposition "test-or-benchmark-only" `
            -ActionRequired $false `
            -Reason "Non-production runtime boundary site; tracked for visibility but not enforced as production debt." `
            -MigrationPhase $phase
    }

    if ($path -match "^rocketmq-runtime/src/") {
        return New-BoundaryDisposition `
            -Disposition "runtime-primitive" `
            -ActionRequired $false `
            -Reason "Allowed inside rocketmq-runtime primitive implementation." `
            -MigrationPhase "PR-2-runtime-primitives"
    }

    switch ($Category) {
        "scheduler-sites" {
            $schedulerDisposition = Get-SchedulerDisposition -Match $Match
            return New-BoundaryDisposition `
                -Disposition $schedulerDisposition.Disposition `
                -ActionRequired ([bool]$schedulerDisposition.ActionRequired) `
                -Reason $schedulerDisposition.Reason `
                -MigrationPhase $phase
        }
        "task-group-root-sites" {
            if ($path -eq "rocketmq-broker/src/broker_runtime.rs" -and $Match.Text -match "TaskGroup::root\(name, runtime\)") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "BrokerRuntime compatibility helper falls back to the current Tokio runtime only when no ServiceContext was injected." `
                    -MigrationPhase "PR-3-service-context-api"
            }

            if ($path -eq "rocketmq-namesrv/src/bootstrap.rs" -and $Match.Text -match 'TaskGroup::root\("rocketmq-namesrv"') {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "NameServerRuntimeInner::task_group compatibility helper falls back to the current Tokio runtime only when no ServiceContext was injected." `
                    -MigrationPhase "PR-5-namesrv-shutdown"
            }

            if ($path -eq "rocketmq-broker/src/latency/broker_fast_failure.rs" -and $Match.Text -match 'TaskGroup::root\("rocketmq-broker\.fast-failure"') {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "BrokerFastFailure::new compatibility path falls back to the current Tokio runtime; BrokerRuntime injects a parent task group when ServiceContext is available." `
                    -MigrationPhase "PR-6-broker-report-tree"
            }

            if ($path -eq "rocketmq-broker/src/processor/pop_message_processor.rs" -and $Match.Text -match 'TaskGroup::root\("rocketmq-broker\.pop\.queue-lock"') {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "QueueLockManager::new compatibility path falls back to the current Tokio runtime; broker processors inject a parent task group when ServiceContext is available." `
                    -MigrationPhase "PR-6-broker-report-tree"
            }

            if ($path -eq "rocketmq-broker/src/topic/manager/topic_queue_mapping_manager.rs" -and $Match.Text -match "TaskGroup::root") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "TopicQueueMappingManager::new compatibility path falls back to the current Tokio runtime; BrokerRuntime injects a parent task group when ServiceContext is available." `
                    -MigrationPhase "PR-6-broker-report-tree"
            }

            if ($path -eq "rocketmq-controller/src/storage/rocksdb_backend.rs" -and $Match.Text -match "TaskGroup::root") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "RocksDBBackend::new compatibility path falls back to the current Tokio runtime; new_with_parent_task_group injects a parent task group when available." `
                    -MigrationPhase "PR-7-store-controller-auth-blocking"
            }

            if ($path -eq "rocketmq-auth/src/runtime_bridge.rs" -and $Match.Text -match "TaskGroup::root") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "AuthBlockingExecutor keeps a lazy current-runtime fallback for file-backed auth metadata compatibility; runtime bridge counters and shutdown reports expose the boundary." `
                    -MigrationPhase "PR-7-store-controller-auth-blocking"
            }

            if ($path -eq "rocketmq-auth/src/runtime.rs" -and $Match.Text -match "TaskGroup::root") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "ACL file watcher is a compatibility path that binds the current Tokio runtime when auth hot reload is enabled." `
                    -MigrationPhase "PR-7-store-controller-auth-blocking"
            }

            if ($path -eq "rocketmq-remoting/src/tls.rs" -and $Match.Text -match 'TaskGroup::root\("rocketmq-remoting\.tls", runtime\)') {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "TlsServerRuntime::new compatibility helper falls back to the current Tokio runtime; new code should use new_with_service_context." `
                    -MigrationPhase "PR-4-remoting-lifecycle"
            }

            if ($path -eq "rocketmq-remoting/src/connection_v2.rs" -and $Match.Text -match "TaskGroup::root") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "ConcurrentConnection::try_new compatibility helper falls back to the current Tokio runtime; new code should use try_new_with_task_group." `
                    -MigrationPhase "PR-4-remoting-lifecycle"
            }

            if ($path -eq "rocketmq-remoting/src/net/channel.rs" -and $Match.Text -match 'TaskGroup::root\("rocketmq-remoting\.channel"') {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "ChannelInner::try_new compatibility helper falls back to the current Tokio runtime; new code should use try_new_with_task_group." `
                    -MigrationPhase "PR-4-remoting-lifecycle"
            }

            if ($path -in @(
                    "rocketmq/src/schedule.rs",
                    "rocketmq/src/schedule/executor.rs",
                    "rocketmq/src/schedule/scheduler.rs",
                    "rocketmq/src/task/service_task.rs"
                )) {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "Legacy rocketmq scheduling and service-task constructors retain current-runtime fallbacks; new *_with_task_group constructors attach tasks to an injected parent TaskGroup." `
                    -MigrationPhase "legacy-compatibility"
            }

            if ($path -in @(
                    "rocketmq-client/src/runtime.rs",
                    "rocketmq-remoting/src/clients/client.rs",
                    "rocketmq-remoting/src/clients/connection_pool.rs",
                    "rocketmq-remoting/src/clients/rocketmq_tokio_client.rs",
                    "rocketmq-remoting/src/remoting_server/rocketmq_tokio_server.rs"
                )) {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "Client and remoting compatibility constructors retain current-runtime fallbacks; ServiceContext-based APIs attach new tasks to the caller's service tree." `
                    -MigrationPhase "PR-4-remoting-lifecycle"
            }

            if ($path -in @(
                    "rocketmq-common/src/common/statistics/statistics_manager.rs",
                    "rocketmq-common/src/common/stats/moment_stats_item.rs",
                    "rocketmq-common/src/common/stats/moment_stats_item_set.rs",
                    "rocketmq-controller/src/controller/controller_manager.rs",
                    "rocketmq-controller/src/controller/open_raft_controller.rs",
                    "rocketmq-controller/src/heartbeat/default_broker_heartbeat_manager.rs",
                    "rocketmq-controller/src/metadata/broker.rs",
                    "rocketmq-controller/src/rpc/server.rs",
                    "rocketmq-store/src/runtime.rs",
                    "rocketmq-store/src/rocksdb/runtime.rs",
                    "rocketmq-tieredstore/src/runtime.rs"
                )) {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "Store, controller, tieredstore, and common statistics compatibility helpers retain current-runtime fallbacks; parent TaskGroup APIs are available for structured service-tree ownership." `
                    -MigrationPhase "PR-7-store-controller-auth-blocking"
            }

            if ($path -in @(
                    "rocketmq-observability/src/exporter/prometheus.rs",
                    "rocketmq-proxy/src/grpc/server.rs",
                    "rocketmq-proxy/src/grpc/service.rs"
                )) {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "Proxy and observability compatibility entrypoints retain current-runtime fallbacks; *_with_task_group APIs attach background tasks to an injected parent TaskGroup." `
                    -MigrationPhase "legacy-compatibility"
            }

            return New-BoundaryDisposition `
                -Disposition "unparented-task-group-root-risk" `
                -ActionRequired $true `
                -Reason "Production code creates a TaskGroup root outside rocketmq-runtime; migrate to injected ServiceContext or parent TaskGroup." `
                -MigrationPhase $phase
        }
        "current-runtime-adapter-sites" {
            if ($path -match "^rocketmq-tools/" -or $path -match "^rocketmq-.+/src/(bin|main)\.rs$") {
                return New-BoundaryDisposition `
                    -Disposition "top-level-owned-runtime-boundary" `
                    -ActionRequired $false `
                    -Reason "Allowed process or tool entrypoint runtime boundary." `
                    -MigrationPhase $phase
            }

            if ($path -eq "rocketmq-common/src/utils/http_tiny_client.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "sync-runtime-guard" `
                    -ActionRequired $false `
                    -Reason "HttpTinyClient's deprecated blocking API checks for an active Tokio runtime only to reject nested blocking calls and direct callers to the async API." `
                    -MigrationPhase "PR-7-store-controller-auth-blocking"
            }

            if ($path -eq "rocketmq-namesrv/src/bootstrap.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "NameServerRuntimeInner::task_group binds the current Tokio runtime only when no ServiceContext was injected." `
                    -MigrationPhase "PR-5-namesrv-shutdown"
            }

            if ($path -eq "rocketmq-broker/src/broker_runtime.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "BrokerRuntimeInner::broker_task_group_or_current binds the current Tokio runtime only when no ServiceContext was injected." `
                    -MigrationPhase "PR-6-broker-report-tree"
            }

            if ($path -eq "rocketmq-broker/src/latency/broker_fast_failure.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "BrokerFastFailure::new compatibility path binds the current Tokio runtime only when no parent TaskGroup was injected." `
                    -MigrationPhase "PR-6-broker-report-tree"
            }

            if ($path -eq "rocketmq-broker/src/processor/pop_message_processor.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "QueueLockManager::new compatibility path binds the current Tokio runtime only when no parent TaskGroup was injected." `
                    -MigrationPhase "PR-6-broker-report-tree"
            }

            if ($path -eq "rocketmq-broker/src/topic/manager/topic_queue_mapping_manager.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "TopicQueueMappingManager::new compatibility path binds the current Tokio runtime only when no parent TaskGroup was injected." `
                    -MigrationPhase "PR-6-broker-report-tree"
            }

            if ($path -eq "rocketmq-controller/src/storage/rocksdb_backend.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "RocksDBBackend::new compatibility path binds the current Tokio runtime only when no parent TaskGroup was injected." `
                    -MigrationPhase "PR-7-store-controller-auth-blocking"
            }

            if ($path -in @(
                    "rocketmq/src/schedule.rs",
                    "rocketmq/src/schedule/executor.rs",
                    "rocketmq/src/schedule/scheduler.rs",
                    "rocketmq/src/task/service_task.rs"
                )) {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "Legacy rocketmq scheduling and service-task constructors bind the current Tokio runtime only through compatibility fallbacks; new *_with_task_group constructors attach tasks to an injected parent TaskGroup." `
                    -MigrationPhase "legacy-compatibility"
            }

            if ($path -in @(
                    "rocketmq-client/src/runtime.rs",
                    "rocketmq-remoting/src/clients/client.rs",
                    "rocketmq-remoting/src/clients/connection_pool.rs",
                    "rocketmq-remoting/src/clients/rocketmq_tokio_client.rs",
                    "rocketmq-remoting/src/remoting_server/rocketmq_tokio_server.rs"
                )) {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "Client and remoting compatibility constructors bind the current Tokio runtime only through fallback paths; ServiceContext-based APIs attach new tasks to the caller's service tree." `
                    -MigrationPhase "PR-4-remoting-lifecycle"
            }

            if ($path -in @(
                    "rocketmq-common/src/common/statistics/statistics_manager.rs",
                    "rocketmq-common/src/common/stats/moment_stats_item.rs",
                    "rocketmq-common/src/common/stats/moment_stats_item_set.rs",
                    "rocketmq-controller/src/controller/controller_manager.rs",
                    "rocketmq-controller/src/controller/open_raft_controller.rs",
                    "rocketmq-controller/src/heartbeat/default_broker_heartbeat_manager.rs",
                    "rocketmq-controller/src/metadata/broker.rs",
                    "rocketmq-controller/src/rpc/server.rs",
                    "rocketmq-store/src/runtime.rs",
                    "rocketmq-store/src/rocksdb/runtime.rs",
                    "rocketmq-tieredstore/src/runtime.rs"
                )) {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "Store, controller, tieredstore, and common statistics compatibility helpers bind the current Tokio runtime only through fallback paths; parent TaskGroup APIs are available for structured service-tree ownership." `
                    -MigrationPhase "PR-7-store-controller-auth-blocking"
            }

            if ($path -in @(
                    "rocketmq-observability/src/exporter/prometheus.rs",
                    "rocketmq-proxy/src/bootstrap.rs",
                    "rocketmq-proxy/src/grpc/server.rs",
                    "rocketmq-proxy/src/grpc/service.rs"
                )) {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "Proxy and observability compatibility entrypoints bind the current Tokio runtime only through fallback paths; injected ServiceContext and *_with_task_group APIs attach background tasks to a managed service tree." `
                    -MigrationPhase "legacy-compatibility"
            }

            if ($path -eq "rocketmq-auth/src/runtime_bridge.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "Auth sync and blocking bridges bind the current Tokio runtime only through documented compatibility paths with counters." `
                    -MigrationPhase "PR-7-store-controller-auth-blocking"
            }

            if ($path -eq "rocketmq-auth/src/runtime.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "ACL file watcher binds the current Tokio runtime only through the auth hot-reload compatibility path." `
                    -MigrationPhase "PR-7-store-controller-auth-blocking"
            }

            if ($path -eq "rocketmq-remoting/src/tls.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "TlsServerRuntime::new compatibility helper binds the current Tokio runtime only when no ServiceContext was injected." `
                    -MigrationPhase "PR-4-remoting-lifecycle"
            }

            if ($path -eq "rocketmq-remoting/src/connection_v2.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "ConcurrentConnection::try_new compatibility helper binds the current Tokio runtime only when no parent TaskGroup was provided." `
                    -MigrationPhase "PR-4-remoting-lifecycle"
            }

            if ($path -eq "rocketmq-remoting/src/net/channel.rs" -and $Match.Text -match "Handle::try_current") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "ChannelInner::try_new compatibility helper binds the current Tokio runtime only when no parent TaskGroup was provided." `
                    -MigrationPhase "PR-4-remoting-lifecycle"
            }

            return New-BoundaryDisposition `
                -Disposition "bare-current-runtime-risk" `
                -ActionRequired $true `
                -Reason "Production code binds to the current Tokio runtime outside an approved top-level adapter." `
                -MigrationPhase $phase
        }
        "raw-tokio-spawn-sites" {
            return New-BoundaryDisposition `
                -Disposition "raw-spawn-risk" `
                -ActionRequired $true `
                -Reason "Production code spawns directly instead of routing through TaskGroup or a documented runtime primitive." `
                -MigrationPhase $phase
        }
        "raw-blocking-executor-sites" {
            if ($path -eq "rocketmq-auth/src/runtime_bridge.rs") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "Auth sync bridge uses block_in_place as a documented compatibility path." `
                    -MigrationPhase "PR-7-store-controller-auth-blocking"
            }

            if ($path -eq "rocketmq-client/src/producer/producer_impl/default_mq_producer_impl.rs") {
                return New-BoundaryDisposition `
                    -Disposition "service-context-parented" `
                    -ActionRequired $false `
                    -Reason "Client producer callback offload is routed through the client runtime blocking wrapper." `
                    -MigrationPhase "PR-3-service-context-api"
            }

            return New-BoundaryDisposition `
                -Disposition "raw-blocking-risk" `
                -ActionRequired $true `
                -Reason "Production code uses raw blocking offload; migrate to BlockingExecutor or document the compatibility boundary." `
                -MigrationPhase $phase
        }
        "legacy-runtime-api-sites" {
            if ($path -match "^rocketmq-tools/" -or $path -match "^rocketmq-.+/src/(bin|main)\.rs$") {
                return New-BoundaryDisposition `
                    -Disposition "top-level-owned-runtime-boundary" `
                    -ActionRequired $false `
                    -Reason "Allowed process or tool entrypoint legacy runtime ownership boundary." `
                    -MigrationPhase $phase
            }

            if ($path -eq "rocketmq-client/src/producer/transaction_mq_produce_builder.rs") {
                return New-BoundaryDisposition `
                    -Disposition "current-runtime-compat-adapter" `
                    -ActionRequired $false `
                    -Reason "Transaction producer builder keeps RocketMQRuntime as a legacy check-runtime compatibility adapter." `
                    -MigrationPhase "legacy-compatibility"
            }

            return New-BoundaryDisposition `
                -Disposition "unclassified-boundary-risk" `
                -ActionRequired $true `
                -Reason "RocketMQRuntime usage must be a runtime primitive, top-level owned runtime boundary, or current-runtime compatibility adapter." `
                -MigrationPhase $phase
        }
        "dedicated-thread-sites" {
            if ($path -eq "rocketmq-client/src/runtime.rs") {
                return New-BoundaryDisposition `
                    -Disposition "documented-dedicated-thread" `
                    -ActionRequired $false `
                    -Reason "Client fallback idle reaper is a documented dedicated OS thread." `
                    -MigrationPhase "PR-3-service-context-api"
            }

            if ($path -match "^rocketmq-common/src/common/thread/thread_service_(tokio|std)\.rs$") {
                return New-BoundaryDisposition `
                    -Disposition "documented-dedicated-thread" `
                    -ActionRequired $false `
                    -Reason "Common ServiceThread compatibility abstraction owns a documented dedicated thread." `
                    -MigrationPhase "legacy-compatibility"
            }

            if ($path -eq "rocketmq-store-local/src/base/allocate_mapped_file_service.rs") {
                return New-BoundaryDisposition `
                    -Disposition "documented-dedicated-thread" `
                    -ActionRequired $false `
                    -Reason "Local mapped-file allocation runs on a documented dedicated OS thread." `
                    -MigrationPhase "PR-7-store-controller-auth-blocking"
            }

            if ($path -eq "rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/main.rs") {
                return New-BoundaryDisposition `
                    -Disposition "top-level-owned-runtime-boundary" `
                    -ActionRequired $false `
                    -Reason "CLI entrypoint owns a dedicated host thread for stack sizing." `
                    -MigrationPhase "legacy-compatibility"
            }

            return New-BoundaryDisposition `
                -Disposition "undocumented-dedicated-thread-risk" `
                -ActionRequired $true `
                -Reason "Production code creates a dedicated thread without a boundary disposition in the runtime audit." `
                -MigrationPhase $phase
        }
        default {
            return New-BoundaryDisposition `
                -Disposition "unclassified-boundary-risk" `
                -ActionRequired $true `
                -Reason "Runtime boundary category has no disposition rule." `
                -MigrationPhase $phase
        }
    }
}

function Add-BoundaryDisposition {
    param(
        [Parameter(Mandatory = $true)][string]$Category,
        [AllowNull()][array]$Matches
    )

    $classified = @()
    $kind = Get-BoundaryKind -Category $Category
    $Matches = @($Matches | Where-Object { $null -ne $_ })
    foreach ($match in $Matches) {
        $disposition = Get-BoundaryDisposition -Category $Category -Match $match
        $fingerprint = Get-BoundaryFingerprint -Category $Category -Match $match -Kind $kind
        $classified += [pscustomobject]@{
            Scope = $match.Scope
            Crate = $match.Crate
            Path = $match.Path
            Line = $match.Line
            Kind = $kind
            Text = $match.Text
            Disposition = $disposition.Disposition
            ActionRequired = [bool]$disposition.ActionRequired
            Reason = $disposition.Reason
            MigrationPhase = $disposition.MigrationPhase
            Fingerprint = $fingerprint
        }
    }
    return @($classified)
}

function Write-BoundaryDispositionReport {
    param(
        [Parameter(Mandatory = $true)][string]$Title,
        [AllowNull()][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $Matches = @($Matches | Where-Object { $null -ne $_ })

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# $Title")
    $lines.Add("")
    $lines.Add("Generated: $(Get-Date -Format o)")
    $lines.Add("")
    $lines.Add("Total matches: $($Matches.Count)")
    $lines.Add("Action required: $(@($Matches | Where-Object { $_.ActionRequired }).Count)")
    $lines.Add("Allowed or documented: $(@($Matches | Where-Object { -not $_.ActionRequired }).Count)")
    $lines.Add("")

    if ($Matches.Count -gt 0) {
        $lines.Add("| Crate | File | Line | Kind | Disposition | Action required | Migration phase | Reason | Fingerprint | Code |")
        $lines.Add("|---|---|---:|---|---|---|---|---|---|---|")
        foreach ($match in $Matches) {
            $fileCell = ConvertTo-MarkdownInlineCode -Text $match.Path
            $code = ConvertTo-MarkdownInlineCode -Text $match.Text
            $reason = $match.Reason.Replace("|", "\|")
            $fingerprint = ConvertTo-MarkdownInlineCode -Text $match.Fingerprint
            $lines.Add("| $($match.Crate) | $fileCell | $($match.Line) | $($match.Kind) | $($match.Disposition) | $($match.ActionRequired) | $($match.MigrationPhase) | $reason | $fingerprint | $code |")
        }
    }

    $lines -join [Environment]::NewLine | Out-File -Encoding utf8 $Path
}

function Get-DispositionCounts {
    param([AllowNull()][array]$Matches)

    $counts = [ordered]@{}
    $Matches = @($Matches | Where-Object { $null -ne $_ })
    foreach ($disposition in ($Matches | Select-Object -ExpandProperty Disposition -Unique | Sort-Object)) {
        $counts[$disposition] = @($Matches | Where-Object { $_.Disposition -eq $disposition }).Count
    }
    return $counts
}

function New-BoundaryBaselineDocument {
    param([Parameter(Mandatory = $true)][hashtable]$BoundaryDispositionByCategory)

    $document = [ordered]@{
        version = 1
        generated_at = (Get-Date -Format o)
        source = "scripts/runtime-audit.ps1"
        policy = "fail on new runtime boundary debt; existing debt must not grow"
        categories = [ordered]@{}
    }

    foreach ($category in ($BoundaryDispositionByCategory.Keys | Sort-Object)) {
        $categoryMatches = @($BoundaryDispositionByCategory[$category] | Where-Object { $null -ne $_ })
        $required = @($categoryMatches | Where-Object { $_.ActionRequired })
        $fingerprints = @()
        foreach ($match in ($required | Sort-Object Path, Line, Kind)) {
            $fingerprints += [ordered]@{
                fingerprint = $match.Fingerprint
                path = $match.Path
                kind = $match.Kind
                reason = $match.Reason
                migration_phase = $match.MigrationPhase
            }
        }

        $document.categories[$category] = [ordered]@{
            action_required_max = $required.Count
            fingerprints = $fingerprints
        }
    }

    return $document
}

function Write-BoundaryBaselineTemplate {
    param(
        [Parameter(Mandatory = $true)][hashtable]$BoundaryDispositionByCategory,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $document = New-BoundaryBaselineDocument -BoundaryDispositionByCategory $BoundaryDispositionByCategory
    $document | ConvertTo-Json -Depth 12 | Out-File -Encoding utf8 $Path
}

function Get-BaselineCategory {
    param(
        [Parameter(Mandatory = $true)]$Baseline,
        [Parameter(Mandatory = $true)][string]$Category
    )

    $property = $Baseline.categories.PSObject.Properties[$Category]
    if ($null -eq $property) {
        return $null
    }
    return $property.Value
}

function Get-BaselineFingerprintSet {
    param([AllowNull()]$CategoryBaseline)

    $set = @{}
    if ($null -eq $CategoryBaseline -or $null -eq $CategoryBaseline.fingerprints) {
        return $set
    }

    foreach ($entry in @($CategoryBaseline.fingerprints)) {
        $fingerprint = if ($entry -is [string]) { $entry } else { $entry.fingerprint }
        if (-not [string]::IsNullOrWhiteSpace($fingerprint)) {
            $set[$fingerprint] = $true
        }
    }
    return $set
}

function Compare-BoundaryBaseline {
    param(
        [Parameter(Mandatory = $true)][hashtable]$BoundaryDispositionByCategory,
        [Parameter(Mandatory = $true)][string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return [pscustomobject]@{
            Status = "missing"
            Failures = @("Boundary baseline file not found: $Path")
        }
    }

    $baseline = Get-Content -Raw -LiteralPath $Path -Encoding utf8 | ConvertFrom-Json
    $failures = New-Object System.Collections.Generic.List[string]

    foreach ($category in ($BoundaryDispositionByCategory.Keys | Sort-Object)) {
        $categoryBaseline = Get-BaselineCategory -Baseline $baseline -Category $category
        if ($null -eq $categoryBaseline) {
            $failures.Add("Missing baseline category: $category")
            continue
        }

        $categoryMatches = @($BoundaryDispositionByCategory[$category] | Where-Object { $null -ne $_ })
        $required = @($categoryMatches | Where-Object { $_.ActionRequired })
        $max = [int]$categoryBaseline.action_required_max
        if ($required.Count -gt $max) {
            $failures.Add("$category action-required count $($required.Count) exceeds baseline max $max")
        }

        $knownFingerprints = Get-BaselineFingerprintSet -CategoryBaseline $categoryBaseline
        foreach ($match in $required) {
            if (-not $knownFingerprints.ContainsKey($match.Fingerprint)) {
                $failures.Add("$category has new action-required fingerprint $($match.Fingerprint) at $($match.Path):$($match.Line)")
            }
        }
    }

    $status = if ($failures.Count -eq 0) { "passed" } else { "failed" }
    return [pscustomobject]@{
        Status = $status
        Failures = @($failures.ToArray())
    }
}

function Write-RuntimeBoundarySummary {
    param(
        [Parameter(Mandatory = $true)][hashtable]$BoundaryDispositionByCategory,
        [AllowNull()]$BaselineComparison,
        [Parameter(Mandatory = $true)][string]$MarkdownPath,
        [Parameter(Mandatory = $true)][string]$JsonPath
    )

    $summary = [ordered]@{
        generated_at = (Get-Date -Format o)
        baseline_status = if ($null -eq $BaselineComparison) { "not-checked" } else { $BaselineComparison.Status }
        categories = [ordered]@{}
        top_action_required = @()
        baseline_failures = if ($null -eq $BaselineComparison) { @() } else { @($BaselineComparison.Failures) }
    }

    $top = @()
    foreach ($category in ($BoundaryDispositionByCategory.Keys | Sort-Object)) {
        $matches = @($BoundaryDispositionByCategory[$category] | Where-Object { $null -ne $_ })
        $required = @($matches | Where-Object { $_.ActionRequired })
        $summary.categories[$category] = [ordered]@{
            total = $matches.Count
            action_required = $required.Count
            allowed_or_documented = @($matches | Where-Object { -not $_.ActionRequired }).Count
            by_disposition = Get-DispositionCounts -Matches $matches
        }
        $top += @($required | Select-Object -First 10)
    }

    $summary.top_action_required = @(
        $top |
            Sort-Object MigrationPhase, Path, Line |
            Select-Object -First 10 |
            ForEach-Object {
                [ordered]@{
                    category = $_.Kind
                    crate = $_.Crate
                    path = $_.Path
                    line = $_.Line
                    disposition = $_.Disposition
                    migration_phase = $_.MigrationPhase
                    reason = $_.Reason
                    fingerprint = $_.Fingerprint
                }
            }
    )

    $summary | ConvertTo-Json -Depth 12 | Out-File -Encoding utf8 $JsonPath

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# Runtime Boundary Summary")
    $lines.Add("")
    $lines.Add("Generated: $(Get-Date -Format o)")
    $lines.Add("")
    $lines.Add("Baseline status: $($summary.baseline_status)")
    $lines.Add("")
    $lines.Add("| Boundary source | Total | Action required | Allowed or documented |")
    $lines.Add("|---|---:|---:|---:|")
    foreach ($category in ($BoundaryDispositionByCategory.Keys | Sort-Object)) {
        $entry = $summary.categories[$category]
        $lines.Add("| $category | $($entry.total) | $($entry.action_required) | $($entry.allowed_or_documented) |")
    }
    $lines.Add("")
    $lines.Add("## Top Action-Required Runtime Boundary Risks")
    $lines.Add("")
    if ($summary.top_action_required.Count -eq 0) {
        $lines.Add("No action-required runtime boundary risks.")
    }
    else {
        $lines.Add("| Kind | Crate | File | Line | Disposition | Migration phase | Reason | Fingerprint |")
        $lines.Add("|---|---|---|---:|---|---|---|---|")
        foreach ($match in $summary.top_action_required) {
            $fileCell = ConvertTo-MarkdownInlineCode -Text $match.path
            $reason = $match.reason.Replace("|", "\|")
            $fingerprint = ConvertTo-MarkdownInlineCode -Text $match.fingerprint
            $lines.Add("| $($match.category) | $($match.crate) | $fileCell | $($match.line) | $($match.disposition) | $($match.migration_phase) | $reason | $fingerprint |")
        }
    }

    if ($null -ne $BaselineComparison -and $BaselineComparison.Failures.Count -gt 0) {
        $lines.Add("")
        $lines.Add("## Baseline Failures")
        $lines.Add("")
        foreach ($failure in $BaselineComparison.Failures) {
            $lines.Add("- $failure")
        }
    }

    $lines -join [Environment]::NewLine | Out-File -Encoding utf8 $MarkdownPath
}

function Get-RuntimeSpawnDisposition {
    param([Parameter(Mandatory = $true)]$Match)

    $path = $Match.Path.Replace("\", "/")
    $text = $Match.Text

    if ($path -match "^rocketmq-runtime/src/") {
        return [pscustomobject]@{
            Disposition = "runtime-primitive"
            ActionRequired = $false
            Reason = "Allowed only inside rocketmq-runtime primitives; production callers should use TaskGroup, ScheduledTaskGroup, RuntimeOwner, or RuntimeHandle wrappers."
        }
    }

    if ($path -eq "rocketmq-client/src/runtime.rs" -and $text -match "thread::Builder::new") {
        return [pscustomobject]@{
            Disposition = "dedicated-idle-reaper-thread"
            ActionRequired = $false
            Reason = "Allowed dedicated OS thread for fallback runtime idle shutdown; it does not occupy Tokio worker or blocking pools."
        }
    }

    if ($path -match "^rocketmq-common/src/common/thread/thread_service_(tokio|std)\.rs$") {
        return [pscustomobject]@{
            Disposition = "dedicated-service-thread"
            ActionRequired = $false
            Reason = "Allowed ServiceThread compatibility abstraction; long blocking loops should remain isolated from Tokio blocking pools."
        }
    }

    if ($path -eq "rocketmq-store-local/src/base/allocate_mapped_file_service.rs") {
        return [pscustomobject]@{
            Disposition = "dedicated-store-allocation-thread"
            ActionRequired = $false
            Reason = "Allowed store file allocation loop; mmap allocation and warmup are isolated on a dedicated OS thread."
        }
    }

    if ($path -eq "rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/main.rs" -and $text -match "thread::Builder::new") {
        return [pscustomobject]@{
            Disposition = "tooling-entrypoint-stack-thread"
            ActionRequired = $false
            Reason = "Allowed CLI entrypoint host thread for explicit stack sizing; the application runtime is created at the process boundary."
        }
    }

    if ($path -match "^rocketmq-tools/") {
        return [pscustomobject]@{
            Disposition = "tooling-follow-up"
            ActionRequired = $true
            Reason = "Tooling applications are lower risk than broker/client runtime paths, but should still avoid exposed Tokio JoinHandle or ad hoc runtime creation where practical."
        }
    }

    return [pscustomobject]@{
        Disposition = "unclassified-follow-up"
        ActionRequired = $true
        Reason = "Manual review required before this site can be treated as compliant with the runtime model."
    }
}

function Add-RuntimeSpawnDisposition {
    param([Parameter(Mandatory = $true)][array]$Matches)

    $classified = @()
    foreach ($match in $Matches) {
        $disposition = Get-RuntimeSpawnDisposition -Match $match
        $classified += [pscustomobject]@{
            Scope = $match.Scope
            Crate = $match.Crate
            Path = $match.Path
            Line = $match.Line
            Text = $match.Text
            Disposition = $disposition.Disposition
            ActionRequired = [bool]$disposition.ActionRequired
            Reason = $disposition.Reason
        }
    }
    return @($classified)
}

function Write-RuntimeSpawnDispositionReport {
    param(
        [Parameter(Mandatory = $true)][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# Production Runtime Spawn Disposition")
    $lines.Add("")
    $lines.Add("Generated: $(Get-Date -Format o)")
    $lines.Add("")
    $lines.Add("Total matches: $($Matches.Count)")
    $lines.Add("Action required: $(@($Matches | Where-Object { $_.ActionRequired }).Count)")
    $lines.Add("Allowed or documented: $(@($Matches | Where-Object { -not $_.ActionRequired }).Count)")
    $lines.Add("")

    if ($Matches.Count -gt 0) {
        $lines.Add("| Crate | File | Line | Disposition | Action required | Reason | Code |")
        $lines.Add("|---|---|---:|---|---|---|---|")
        foreach ($match in $Matches) {
            $fileCell = ConvertTo-MarkdownInlineCode -Text $match.Path
            $code = ConvertTo-MarkdownInlineCode -Text $match.Text
            $reason = $match.Reason.Replace("|", "\|")
            $lines.Add("| $($match.Crate) | $fileCell | $($match.Line) | $($match.Disposition) | $($match.ActionRequired) | $reason | $code |")
        }
    }

    $lines -join [Environment]::NewLine | Out-File -Encoding utf8 $Path
}

function Get-RuntimeCreationDisposition {
    param([Parameter(Mandatory = $true)]$Match)

    $path = $Match.Path.Replace("\", "/")
    $text = $Match.Text

    if ($path -match "^rocketmq-runtime/src/") {
        return [pscustomobject]@{
            Disposition = "runtime-primitive"
            ActionRequired = $false
            Reason = "Allowed inside rocketmq-runtime ownership, context, handle, legacy, and actor primitives."
        }
    }

    if ($path -eq "rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/main.rs") {
        return [pscustomobject]@{
            Disposition = "tooling-entrypoint-runtime"
            ActionRequired = $false
            Reason = "Allowed CLI process entrypoint runtime; application entry points own Tokio runtimes."
        }
    }

    if ($path -eq "rocketmq-auth/src/runtime_bridge.rs" -or
        ($path -match "^rocketmq-auth/src/(authentication|authorization)/" -and $text -match "block_on_sync_bridge|block_on_authentication_provider|block_on_base_authorization")) {
        return [pscustomobject]@{
            Disposition = "auth-sync-bridge"
            ActionRequired = $false
            Reason = "Documented compatibility sync bridge with counters and shared fallback runtime policy."
        }
    }

    if ($path -eq "rocketmq-common/src/utils/http_tiny_client.rs") {
        return [pscustomobject]@{
            Disposition = "common-http-sync-bridge"
            ActionRequired = $false
            Reason = "Documented blocking HTTP compatibility bridge backed by shared runtime owner."
        }
    }

    if ($path -eq "rocketmq-common/src/thread_pool.rs") {
        return [pscustomobject]@{
            Disposition = "common-runtime-owner-facade"
            ActionRequired = $false
            Reason = "Common executor facade delegates runtime ownership and block_on through RuntimeOwner."
        }
    }

    if ($path -eq "rocketmq-client/src/runtime.rs") {
        return [pscustomobject]@{
            Disposition = "client-fallback-runtime"
            ActionRequired = $false
            Reason = "Documented client shared fallback runtime and shutdown bridge."
        }
    }

    if ($path -eq "rocketmq-client/src/factory/mq_client_instance.rs" -and $text -match "futures::executor::block_on") {
        return [pscustomobject]@{
            Disposition = "client-sync-block-on-follow-up"
            ActionRequired = $true
            Reason = "Synchronous factory path still uses futures::executor::block_on and needs a focused compatibility review."
        }
    }

    if ($path -eq "rocketmq-dashboard/rocketmq-dashboard-web/backend/src/service/dashboard_service.rs" -and
        $text -match "Handle::try_current") {
        return [pscustomobject]@{
            Disposition = "dashboard-current-runtime-boundary"
            ActionRequired = $false
            Reason = "Dashboard web backend binds to the Axum application runtime before spawning the history collector."
        }
    }

    if ($path -eq "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/topic/service.rs" -and
        $text -match "runtime\.block_on") {
        return [pscustomobject]@{
            Disposition = "dashboard-transaction-sync-bridge"
            ActionRequired = $false
            Reason = "Tauri topic transaction send runs inside spawn_blocking and uses the shared RuntimeOwner compatibility bridge."
        }
    }

    if ($path -match "^rocketmq-dashboard/") {
        return [pscustomobject]@{
            Disposition = "dashboard-standalone-follow-up"
            ActionRequired = $true
            Reason = "Standalone dashboard projects are outside root workspace validation and need separate runtime-model review."
        }
    }

    if ($path -match "^rocketmq-auth/src/runtime\.rs$") {
        return [pscustomobject]@{
            Disposition = "auth-runtime-boundary"
            ActionRequired = $false
            Reason = "Auth runtime binds to the current Tokio runtime before creating TaskGroup-backed services."
        }
    }

    if ($path -match "^rocketmq-common/src/common/(stats|statistics)/") {
        return [pscustomobject]@{
            Disposition = "common-scheduler-runtime-boundary"
            ActionRequired = $false
            Reason = "Common scheduled stats components bind to the current runtime before using ScheduledTaskGroup."
        }
    }

    if ($path -match "^rocketmq/src/") {
        return [pscustomobject]@{
            Disposition = "rocketmq-runtime-boundary"
            ActionRequired = $false
            Reason = "RocketMQ scheduler/service abstractions bind to the current runtime before TaskGroup-backed lifecycle management."
        }
    }

    if ($path -match "^rocketmq-namesrv/") {
        return [pscustomobject]@{
            Disposition = "namesrv-runtime-boundary"
            ActionRequired = $false
            Reason = "NameSrv bootstrap binds to the application runtime at startup."
        }
    }

    if ($path -eq "rocketmq-broker/src/topic/manager/topic_queue_mapping_manager.rs" -and $text -match "Handle::try_current") {
        return [pscustomobject]@{
            Disposition = "broker-blocking-executor-boundary"
            ActionRequired = $false
            Reason = "Topic queue mapping persistence lazily binds a bounded BlockingExecutor to the current broker Tokio runtime."
        }
    }

    if ($path -match "^rocketmq-broker/") {
        return [pscustomobject]@{
            Disposition = "broker-runtime-boundary"
            ActionRequired = $false
            Reason = "Broker runtime and migrated services bind to the application runtime at startup/lifecycle boundaries."
        }
    }

    if ($path -match "^rocketmq-controller/") {
        return [pscustomobject]@{
            Disposition = "controller-runtime-boundary"
            ActionRequired = $false
            Reason = "Controller services bind to the current runtime before TaskGroup/ScheduledTaskGroup-managed lifecycle."
        }
    }

    if ($path -match "^rocketmq-store/src/(runtime|rocksdb/runtime)\.rs$" -or $path -match "^rocketmq-tieredstore/src/runtime\.rs$") {
        return [pscustomobject]@{
            Disposition = "store-runtime-boundary"
            ActionRequired = $false
            Reason = "Store runtime adapter binds to the current runtime for store-managed async services."
        }
    }

    if ($path -match "^rocketmq-remoting/src/clients/blocking_client\.rs$") {
        return [pscustomobject]@{
            Disposition = "remoting-blocking-client-compat"
            ActionRequired = $false
            Reason = "Documented blocking remoting client compatibility wrapper around async client operations."
        }
    }

    if ($path -match "^rocketmq-remoting/") {
        return [pscustomobject]@{
            Disposition = "remoting-runtime-boundary"
            ActionRequired = $false
            Reason = "Remoting client/server/TLS components bind to the current runtime before TaskGroup-backed lifecycle management."
        }
    }

    if ($path -match "^rocketmq-proxy/") {
        return [pscustomobject]@{
            Disposition = "proxy-runtime-boundary"
            ActionRequired = $false
            Reason = "Proxy entrypoint and gRPC services bind to the application runtime."
        }
    }

    if ($path -match "^rocketmq-observability/") {
        return [pscustomobject]@{
            Disposition = "observability-runtime-boundary"
            ActionRequired = $false
            Reason = "Observability exporter setup binds metrics/exporter runtime handles during startup."
        }
    }

    if ($path -match "^rocketmq-tools/") {
        return [pscustomobject]@{
            Disposition = "tooling-runtime-boundary"
            ActionRequired = $false
            Reason = "Admin tooling binds to the current runtime or process entrypoint runtime."
        }
    }

    return [pscustomobject]@{
        Disposition = "unclassified-follow-up"
        ActionRequired = $true
        Reason = "Manual review required before this runtime creation site can be treated as compliant."
    }
}

function Add-RuntimeCreationDisposition {
    param([Parameter(Mandatory = $true)][array]$Matches)

    $classified = @()
    foreach ($match in $Matches) {
        $disposition = Get-RuntimeCreationDisposition -Match $match
        $classified += [pscustomobject]@{
            Scope = $match.Scope
            Crate = $match.Crate
            Path = $match.Path
            Line = $match.Line
            Text = $match.Text
            Disposition = $disposition.Disposition
            ActionRequired = [bool]$disposition.ActionRequired
            Reason = $disposition.Reason
        }
    }
    return @($classified)
}

function Write-RuntimeCreationDispositionReport {
    param(
        [Parameter(Mandatory = $true)][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# Production Runtime Creation Disposition")
    $lines.Add("")
    $lines.Add("Generated: $(Get-Date -Format o)")
    $lines.Add("")
    $lines.Add("Total matches: $($Matches.Count)")
    $lines.Add("Action required: $(@($Matches | Where-Object { $_.ActionRequired }).Count)")
    $lines.Add("Allowed or documented: $(@($Matches | Where-Object { -not $_.ActionRequired }).Count)")
    $lines.Add("")

    if ($Matches.Count -gt 0) {
        $lines.Add("| Crate | File | Line | Disposition | Action required | Reason | Code |")
        $lines.Add("|---|---|---:|---|---|---|---|")
        foreach ($match in $Matches) {
            $fileCell = ConvertTo-MarkdownInlineCode -Text $match.Path
            $code = ConvertTo-MarkdownInlineCode -Text $match.Text
            $reason = $match.Reason.Replace("|", "\|")
            $lines.Add("| $($match.Crate) | $fileCell | $($match.Line) | $($match.Disposition) | $($match.ActionRequired) | $reason | $code |")
        }
    }

    $lines -join [Environment]::NewLine | Out-File -Encoding utf8 $Path
}

function Get-SchedulerDisposition {
    param([Parameter(Mandatory = $true)]$Match)

    $path = $Match.Path.Replace("\", "/")

    if ($path -match "^rocketmq-runtime/src/") {
        return [pscustomobject]@{
            Disposition = "runtime-scheduler-primitive"
            ActionRequired = $false
            Reason = "Allowed inside rocketmq-runtime scheduler primitives; these sites implement cancellable sleeps, fixed-delay, fixed-rate, and no-overlap semantics."
        }
    }

    if ($path -match "^rocketmq/src/schedule") {
        return [pscustomobject]@{
            Disposition = "rocketmq-scheduler-facade"
            ActionRequired = $false
            Reason = "Allowed inside the RocketMQ scheduler facade; driver and run tasks are tracked by TaskGroup and provide bounded shutdown reports."
        }
    }

    if ($path -eq "rocketmq-common/src/thread_pool.rs") {
        return [pscustomobject]@{
            Disposition = "common-scheduler-facade"
            ActionRequired = $false
            Reason = "Allowed compatibility facade backed by RuntimeOwner and tracked task groups."
        }
    }

    if ($path -match "^rocketmq-client/src/factory/mq_client_instance\.rs$") {
        return [pscustomobject]@{
            Disposition = "client-scheduler-boundary"
            ActionRequired = $false
            Reason = "Documented client scheduling boundary using ScheduledTaskManager and cancellable delayed runtime actions."
        }
    }

    if ($path -match "^rocketmq-client/src/producer/produce_accumulator\.rs$") {
        return [pscustomobject]@{
            Disposition = "client-producer-batch-drain"
            ActionRequired = $false
            Reason = "Allowed production periodic batch-drain timer; it is business scheduling rather than runtime ownership."
        }
    }

    if ($path -match "^rocketmq-client/src/trace/async_trace_dispatcher\.rs$") {
        return [pscustomobject]@{
            Disposition = "client-trace-batch-flush"
            ActionRequired = $false
            Reason = "Allowed trace batch flush cadence; shutdown flush no longer depends on a fixed blocking delay."
        }
    }

    if ($path -match "^rocketmq-client/src/runtime\.rs$") {
        return [pscustomobject]@{
            Disposition = "client-delayed-runtime-action"
            ActionRequired = $false
            Reason = "Allowed client runtime helper delay for scheduled actions; runtime ownership remains centralized in client runtime wrappers."
        }
    }

    if ($path -match "^rocketmq-client/src/consumer/") {
        return [pscustomobject]@{
            Disposition = "client-consumer-protocol-delay"
            ActionRequired = $false
            Reason = "Allowed consumer protocol retry, suspend, and pull delay semantics; these are bounded business waits rather than ad hoc runtime management."
        }
    }

    if ($path -match "^rocketmq-broker/src/broker_runtime\.rs$") {
        return [pscustomobject]@{
            Disposition = "broker-scheduler-boundary"
            ActionRequired = $false
            Reason = "Documented broker runtime scheduling boundary; migrated background tasks are rooted in broker lifecycle ownership."
        }
    }

    if ($path -eq "rocketmq-broker/src/broker/broker_control_plane/bootstrap.rs") {
        return [pscustomobject]@{
            Disposition = "broker-controller-bootstrap-delay"
            ActionRequired = $false
            Reason = "Allowed bounded controller bootstrap observation delay; periodic controller work remains rooted in the broker ScheduledTaskManager lifecycle."
        }
    }

    if ($path -match "^rocketmq-broker/src/(schedule|transaction|long_polling|processor|topic)/") {
        return [pscustomobject]@{
            Disposition = "broker-protocol-timer"
            ActionRequired = $false
            Reason = "Allowed broker protocol timer, long-poll timeout, retry, yield, or delayed-message semantics; these waits are not runtime creation or detached task ownership."
        }
    }

    if ($path -match "^rocketmq-controller/src/") {
        return [pscustomobject]@{
            Disposition = "controller-retry-backoff"
            ActionRequired = $false
            Reason = "Allowed controller startup or RPC retry/backoff timing; scheduled lifecycle jobs use ScheduledTaskGroup where they are long-running services."
        }
    }

    if ($path -match "^rocketmq-remoting/src/remoting_server/") {
        return [pscustomobject]@{
            Disposition = "remoting-idle-timeout"
            ActionRequired = $false
            Reason = "Allowed per-connection idle timeout inside the remoting server task lifecycle."
        }
    }

    if ($path -match "^rocketmq-store/src/stats/") {
        return [pscustomobject]@{
            Disposition = "store-stats-scheduler-boundary"
            ActionRequired = $false
            Reason = "Documented store stats scheduling boundary backed by ScheduledTaskManager."
        }
    }

    if ($path -match "^rocketmq-store/src/") {
        return [pscustomobject]@{
            Disposition = "store-storage-timer"
            ActionRequired = $false
            Reason = "Allowed storage protocol, HA, flush, and checkpoint timing semantics; long-running scheduled services have migration benchmarks and shutdown artifacts."
        }
    }

    if ($path -match "^rocketmq-tools/") {
        return [pscustomobject]@{
            Disposition = "tooling-user-driven-polling"
            ActionRequired = $false
            Reason = "Allowed admin tooling polling or refresh interval outside broker/client hot runtime paths."
        }
    }

    if ($path -match "^rocketmq-dashboard/") {
        return [pscustomobject]@{
            Disposition = "dashboard-standalone-refresh-loop"
            ActionRequired = $false
            Reason = "Allowed standalone dashboard refresh loop; dashboard projects require separate validation from the root workspace."
        }
    }

    return [pscustomobject]@{
        Disposition = "unclassified-follow-up"
        ActionRequired = $true
        Reason = "Manual review required before this scheduler site can be treated as compliant."
    }
}

function Add-SchedulerDisposition {
    param([Parameter(Mandatory = $true)][array]$Matches)

    $classified = @()
    foreach ($match in $Matches) {
        $disposition = Get-SchedulerDisposition -Match $match
        $classified += [pscustomobject]@{
            Scope = $match.Scope
            Crate = $match.Crate
            Path = $match.Path
            Line = $match.Line
            Text = $match.Text
            Disposition = $disposition.Disposition
            ActionRequired = [bool]$disposition.ActionRequired
            Reason = $disposition.Reason
        }
    }
    return @($classified)
}

function Write-SchedulerDispositionReport {
    param(
        [Parameter(Mandatory = $true)][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# Production Scheduler Disposition")
    $lines.Add("")
    $lines.Add("Generated: $(Get-Date -Format o)")
    $lines.Add("")
    $lines.Add("Total matches: $($Matches.Count)")
    $lines.Add("Action required: $(@($Matches | Where-Object { $_.ActionRequired }).Count)")
    $lines.Add("Allowed or documented: $(@($Matches | Where-Object { -not $_.ActionRequired }).Count)")
    $lines.Add("")

    if ($Matches.Count -gt 0) {
        $lines.Add("| Crate | File | Line | Disposition | Action required | Reason | Code |")
        $lines.Add("|---|---|---:|---|---|---|---|")
        foreach ($match in $Matches) {
            $fileCell = ConvertTo-MarkdownInlineCode -Text $match.Path
            $code = ConvertTo-MarkdownInlineCode -Text $match.Text
            $reason = $match.Reason.Replace("|", "\|")
            $lines.Add("| $($match.Crate) | $fileCell | $($match.Line) | $($match.Disposition) | $($match.ActionRequired) | $reason | $code |")
        }
    }

    $lines -join [Environment]::NewLine | Out-File -Encoding utf8 $Path
}

function Get-BlockingDisposition {
    param([Parameter(Mandatory = $true)]$Match)

    $path = $Match.Path.Replace("\", "/")

    if ($path -eq "rocketmq-runtime/src/blocking.rs") {
        return [pscustomobject]@{
            Disposition = "runtime-blocking-primitive"
            ActionRequired = $false
            Reason = "Allowed only inside BlockingExecutor; it bounds concurrency, observes queue/task timeouts, and tracks still-running tasks."
        }
    }

    if ($path -eq "rocketmq-runtime/src/shutdown_report.rs") {
        return [pscustomobject]@{
            Disposition = "runtime-blocking-report-field"
            ActionRequired = $false
            Reason = "Report text and fields for BlockingExecutor shutdown evidence, not a blocking operation."
        }
    }

    if ($path -match "^rocketmq-store/src/") {
        return [pscustomobject]@{
            Disposition = "store-storage-blocking-domain"
            ActionRequired = $false
            Reason = "Allowed store local-file, mmap, RocksDB, HA, index, timer, and checkpoint blocking domain; long loops use dedicated services and store benchmarks cover shutdown behavior."
        }
    }

    if ($path -match "^rocketmq-broker/src/bin/") {
        return [pscustomobject]@{
            Disposition = "broker-entrypoint-config-io"
            ActionRequired = $false
            Reason = "Allowed process bootstrap configuration file read before broker services enter hot async paths."
        }
    }

    if ($path -match "^rocketmq-broker/src/(broker_runtime\.rs|config\.rs|config/|offset/|pop|processor/|subscription/|topic/)") {
        return [pscustomobject]@{
            Disposition = "broker-persistent-state-domain"
            ActionRequired = $false
            Reason = "Allowed broker persistent state, RocksDB metadata, admin persistence, or storage-backed processor path; direct async blocking offload sites are separately migrated to BlockingExecutor."
        }
    }

    if ($path -match "^rocketmq-controller/src/(storage|openraft/storage)") {
        return [pscustomobject]@{
            Disposition = "controller-storage-domain"
            ActionRequired = $false
            Reason = "Allowed controller storage/RocksDB domain outside Tokio runtime ownership; controller scheduling lifecycle is covered by separate scheduler disposition."
        }
    }

    if ($path -match "^rocketmq-client/src/(admin|implementation|producer|consumer)/") {
        return [pscustomobject]@{
            Disposition = "client-admin-or-callback-blocking-boundary"
            ActionRequired = $false
            Reason = "Allowed client admin metadata, protocol naming, or callback offload boundary; real user callback blocking paths use the client BlockingExecutor wrapper."
        }
    }

    if ($path -match "^rocketmq-common/src/") {
        return [pscustomobject]@{
            Disposition = "common-sync-file-or-config-boundary"
            ActionRequired = $false
            Reason = "Allowed shared synchronous file/config utility boundary; async callers should wrap hot-path file I/O with a BlockingExecutor."
        }
    }

    if ($path -match "^rocketmq-auth/src/") {
        return [pscustomobject]@{
            Disposition = "auth-local-metadata-file-io"
            ActionRequired = $false
            Reason = "Allowed local ACL/auth metadata file I/O compatibility path; auth sync bridges are separately classified in runtime creation disposition."
        }
    }

    if ($path -match "^rocketmq-remoting/src/protocol/") {
        return [pscustomobject]@{
            Disposition = "remoting-protocol-field-false-positive"
            ActionRequired = $false
            Reason = "Protocol field or header text containing blocking-related words, not a runtime blocking operation."
        }
    }

    if ($path -match "^rocketmq-namesrv/src/bin/") {
        return [pscustomobject]@{
            Disposition = "namesrv-entrypoint-config-io"
            ActionRequired = $false
            Reason = "Allowed process bootstrap configuration file read before NameSrv services enter hot async paths."
        }
    }

    if ($path -match "^rocketmq-tools/") {
        return [pscustomobject]@{
            Disposition = "tooling-blocking-io"
            ActionRequired = $false
            Reason = "Allowed admin tooling export, catalog, RocksDB inspection, or command polling outside broker/client hot runtime paths."
        }
    }

    if ($path -match "^rocketmq-observability/src/") {
        return [pscustomobject]@{
            Disposition = "observability-metric-name-false-positive"
            ActionRequired = $false
            Reason = "Metric names and semantic labels for RocksDB/storage reporting, not Tokio runtime blocking."
        }
    }

    if ($path -match "^rocketmq-dashboard/") {
        return [pscustomobject]@{
            Disposition = "dashboard-standalone-blocking-boundary"
            ActionRequired = $false
            Reason = "Standalone dashboard blocking bridge outside the root workspace runtime model; validate in the dashboard project when it is changed."
        }
    }

    return [pscustomobject]@{
        Disposition = "unclassified-follow-up"
        ActionRequired = $true
        Reason = "Manual review required before this blocking site can be treated as compliant."
    }
}

function Add-BlockingDisposition {
    param([Parameter(Mandatory = $true)][array]$Matches)

    $classified = @()
    foreach ($match in $Matches) {
        $disposition = Get-BlockingDisposition -Match $match
        $classified += [pscustomobject]@{
            Scope = $match.Scope
            Crate = $match.Crate
            Path = $match.Path
            Line = $match.Line
            Text = $match.Text
            Disposition = $disposition.Disposition
            ActionRequired = [bool]$disposition.ActionRequired
            Reason = $disposition.Reason
        }
    }
    return @($classified)
}

function Write-BlockingDispositionReport {
    param(
        [Parameter(Mandatory = $true)][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# Production Blocking Disposition")
    $lines.Add("")
    $lines.Add("Generated: $(Get-Date -Format o)")
    $lines.Add("")
    $lines.Add("Total matches: $($Matches.Count)")
    $lines.Add("Action required: $(@($Matches | Where-Object { $_.ActionRequired }).Count)")
    $lines.Add("Allowed or documented: $(@($Matches | Where-Object { -not $_.ActionRequired }).Count)")
    $lines.Add("")

    if ($Matches.Count -gt 0) {
        $lines.Add("| Crate | File | Line | Disposition | Action required | Reason | Code |")
        $lines.Add("|---|---|---:|---|---|---|---|")
        foreach ($match in $Matches) {
            $fileCell = ConvertTo-MarkdownInlineCode -Text $match.Path
            $code = ConvertTo-MarkdownInlineCode -Text $match.Text
            $reason = $match.Reason.Replace("|", "\|")
            $lines.Add("| $($match.Crate) | $fileCell | $($match.Line) | $($match.Disposition) | $($match.ActionRequired) | $reason | $code |")
        }
    }

    $lines -join [Environment]::NewLine | Out-File -Encoding utf8 $Path
}

function Get-ShutdownDisposition {
    param([Parameter(Mandatory = $true)]$Match)

    $path = $Match.Path.Replace("\", "/")

    if ($path -match "^rocketmq-runtime/src/") {
        return [pscustomobject]@{
            Disposition = "runtime-shutdown-primitive"
            ActionRequired = $false
            Reason = "Allowed inside runtime owner, task group, scheduler, shutdown report, and legacy runtime compatibility primitives."
        }
    }

    if ($path -match "^rocketmq/src/") {
        return [pscustomobject]@{
            Disposition = "rocketmq-core-lifecycle"
            ActionRequired = $false
            Reason = "Allowed core task, scheduler, blocking queue, and service lifecycle boundary with tracked shutdown reporting."
        }
    }

    if ($path -match "^rocketmq-common/src/") {
        return [pscustomobject]@{
            Disposition = "common-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed common executor, scheduler, service thread, stats, and compatibility runtime lifecycle boundary."
        }
    }

    if ($path -match "^rocketmq-client/src/") {
        return [pscustomobject]@{
            Disposition = "client-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed client task, consumer, producer, trace, stats, remoting, and fallback runtime lifecycle boundary after migrated tracked-task shutdown stages."
        }
    }

    if ($path -match "^rocketmq-namesrv/src/") {
        return [pscustomobject]@{
            Disposition = "namesrv-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed NameSrv bootstrap, route manager, remoting, and batch unregistration lifecycle boundary with shutdown drain benchmarks."
        }
    }

    if ($path -match "^rocketmq-broker/src/") {
        return [pscustomobject]@{
            Disposition = "broker-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed broker runtime, scheduler, long-polling, processor, admin, client housekeeping, and storage-related lifecycle boundary after migrated service stages."
        }
    }

    if ($path -match "^rocketmq-controller/src/") {
        return [pscustomobject]@{
            Disposition = "controller-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed controller metadata, heartbeat, OpenRaft, leadership, RPC, and lifecycle boundary after scheduled-service migration."
        }
    }

    if ($path -match "^rocketmq-remoting/src/") {
        return [pscustomobject]@{
            Disposition = "remoting-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed remoting client, server, connection pool, TLS reload, and callback worker lifecycle boundary."
        }
    }

    if ($path -match "^rocketmq-store/src/") {
        return [pscustomobject]@{
            Disposition = "store-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed store service, HA, flush, timer, stats, RocksDB, compaction, allocation, and local-file lifecycle boundary with storage-specific shutdown behavior."
        }
    }

    if ($path -match "^rocketmq-tieredstore/src/") {
        return [pscustomobject]@{
            Disposition = "tieredstore-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed tiered-store dispatcher, cleanup, runtime, and storage lifecycle boundary covered by cleanup scheduler migration."
        }
    }

    if ($path -match "^rocketmq-proxy(?:-core|-cluster|-local)?/src/") {
        return [pscustomobject]@{
            Disposition = "proxy-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed facade, Core ingress, or Cluster adapter lifecycle boundary backed by injected task ownership and reportable shutdown."
        }
    }

    if ($path -match "^rocketmq-auth/src/") {
        return [pscustomobject]@{
            Disposition = "auth-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed auth runtime, ACL watcher, local metadata, and compatibility bridge lifecycle boundary."
        }
    }

    if ($path -match "^rocketmq-observability/src/") {
        return [pscustomobject]@{
            Disposition = "observability-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed observability exporter, metrics initialization, and shutdown labeling lifecycle boundary."
        }
    }

    if ($path -match "^rocketmq-tools/") {
        return [pscustomobject]@{
            Disposition = "tooling-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed admin CLI/TUI command, request, and local runtime lifecycle boundary outside broker/client hot paths."
        }
    }

    if ($path -match "^rocketmq-dashboard/") {
        return [pscustomobject]@{
            Disposition = "dashboard-standalone-lifecycle-boundary"
            ActionRequired = $false
            Reason = "Allowed standalone dashboard lifecycle boundary; dashboard projects require separate validation when changed."
        }
    }

    if ($path -match "^rocketmq-error/src/") {
        return [pscustomobject]@{
            Disposition = "error-type-field"
            ActionRequired = $false
            Reason = "Error enum field or message containing shutdown-related wording, not a runtime lifecycle operation."
        }
    }

    return [pscustomobject]@{
        Disposition = "unclassified-follow-up"
        ActionRequired = $true
        Reason = "Manual review required before this shutdown site can be treated as compliant."
    }
}

function Add-ShutdownDisposition {
    param([Parameter(Mandatory = $true)][array]$Matches)

    $classified = @()
    foreach ($match in $Matches) {
        $disposition = Get-ShutdownDisposition -Match $match
        $classified += [pscustomobject]@{
            Scope = $match.Scope
            Crate = $match.Crate
            Path = $match.Path
            Line = $match.Line
            Text = $match.Text
            Disposition = $disposition.Disposition
            ActionRequired = [bool]$disposition.ActionRequired
            Reason = $disposition.Reason
        }
    }
    return @($classified)
}

function Write-ShutdownDispositionReport {
    param(
        [Parameter(Mandatory = $true)][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# Production Shutdown Disposition")
    $lines.Add("")
    $lines.Add("Generated: $(Get-Date -Format o)")
    $lines.Add("")
    $lines.Add("Total matches: $($Matches.Count)")
    $lines.Add("Action required: $(@($Matches | Where-Object { $_.ActionRequired }).Count)")
    $lines.Add("Allowed or documented: $(@($Matches | Where-Object { -not $_.ActionRequired }).Count)")
    $lines.Add("")

    if ($Matches.Count -gt 0) {
        $lines.Add("| Crate | File | Line | Disposition | Action required | Reason | Code |")
        $lines.Add("|---|---|---:|---|---|---|---|")
        foreach ($match in $Matches) {
            $fileCell = ConvertTo-MarkdownInlineCode -Text $match.Path
            $code = ConvertTo-MarkdownInlineCode -Text $match.Text
            $reason = $match.Reason.Replace("|", "\|")
            $lines.Add("| $($match.Crate) | $fileCell | $($match.Line) | $($match.Disposition) | $($match.ActionRequired) | $reason | $code |")
        }
    }

    $lines -join [Environment]::NewLine | Out-File -Encoding utf8 $Path
}

New-DirectoryIfMissing -Path $auditRoot
if (-not $SkipBaseline) {
    New-DirectoryIfMissing -Path $baselineRoot
}

$patterns = [ordered]@{
    "runtime-spawn-sites" = "tokio::spawn|std::thread::spawn|thread::Builder::new|JoinHandle|JoinSet"
    "runtime-creation-sites" = "Runtime::new|RocketMQRuntime::new|Builder::new_multi_thread|Builder::new_current_thread|Handle::try_current|Handle::current|block_on|block_in_place"
    "blocking-sites" = "spawn_blocking|blocking_recv|blocking_send|std::fs::|RocksDB|rocksdb|thread::sleep"
    "scheduler-sites" = "schedule_at_fixed_rate|ScheduledTaskManager|FixedRate|FixedDelay|FixedRateNoOverlap|tokio::time::interval|tokio::time::sleep"
    "shutdown-sites" = "abort\(|shutdown_timeout|shutdown_background|CancellationToken|cancelled\(|Notify|wait_for_server_task|shutdown\("
    "task-group-root-sites" = "TaskGroup::root"
    "current-runtime-adapter-sites" = "Handle::try_current|Handle::current|RuntimeContext::from_current|RuntimeContext::try_from_current"
    "raw-tokio-spawn-sites" = "tokio::spawn|JoinSet::spawn"
    "raw-blocking-executor-sites" = "spawn_blocking|block_in_place"
    "legacy-runtime-api-sites" = "\bRocketMQRuntime\b"
    "dedicated-thread-sites" = "std::thread::spawn|thread::Builder::new"
}

$boundaryKeys = @(
    "task-group-root-sites",
    "current-runtime-adapter-sites",
    "raw-tokio-spawn-sites",
    "raw-blocking-executor-sites",
    "scheduler-sites",
    "legacy-runtime-api-sites",
    "dedicated-thread-sites"
)

$allMatches = @{}
foreach ($entry in $patterns.GetEnumerator()) {
    $matches = @(Get-RuntimeMatches -Pattern $entry.Value)
    $allMatches[$entry.Key] = $matches
    Write-MarkdownReport `
        -Title ($entry.Key -replace "-", " ") `
        -Matches $matches `
        -Path (Join-Path $auditRoot "$($entry.Key).md")
    Write-MarkdownReport `
        -Title ("production " + ($entry.Key -replace "-", " ")) `
        -Matches (Get-ProductionMatches -Matches $matches) `
        -Path (Join-Path $auditRoot "production-$($entry.Key).md")
}

$productionRuntimeSpawnDisposition = Add-RuntimeSpawnDisposition -Matches (Get-ProductionMatches -Matches $allMatches["runtime-spawn-sites"])
Write-RuntimeSpawnDispositionReport `
    -Matches $productionRuntimeSpawnDisposition `
    -Path (Join-Path $auditRoot "production-runtime-spawn-disposition.md")

$productionRuntimeCreationDisposition = Add-RuntimeCreationDisposition -Matches (Get-ProductionMatches -Matches $allMatches["runtime-creation-sites"])
Write-RuntimeCreationDispositionReport `
    -Matches $productionRuntimeCreationDisposition `
    -Path (Join-Path $auditRoot "production-runtime-creation-disposition.md")

$productionSchedulerDisposition = Add-SchedulerDisposition -Matches (Get-ProductionMatches -Matches $allMatches["scheduler-sites"])
Write-SchedulerDispositionReport `
    -Matches $productionSchedulerDisposition `
    -Path (Join-Path $auditRoot "production-scheduler-disposition.md")

$productionBlockingDisposition = Add-BlockingDisposition -Matches (Get-ProductionMatches -Matches $allMatches["blocking-sites"])
Write-BlockingDispositionReport `
    -Matches $productionBlockingDisposition `
    -Path (Join-Path $auditRoot "production-blocking-disposition.md")

$productionShutdownDisposition = Add-ShutdownDisposition -Matches (Get-ProductionMatches -Matches $allMatches["shutdown-sites"])
Write-ShutdownDispositionReport `
    -Matches $productionShutdownDisposition `
    -Path (Join-Path $auditRoot "production-shutdown-disposition.md")

$productionBoundaryDispositionByCategory = @{}
foreach ($key in $boundaryKeys) {
    $classified = Add-BoundaryDisposition `
        -Category $key `
        -Matches (Get-ProductionMatches -Matches $allMatches[$key])
    $productionBoundaryDispositionByCategory[$key] = $classified

    $reportName = switch ($key) {
        "task-group-root-sites" { "production-task-group-root-disposition.md" }
        "current-runtime-adapter-sites" { "production-current-runtime-adapter-disposition.md" }
        "raw-tokio-spawn-sites" { "production-raw-tokio-spawn-disposition.md" }
        "raw-blocking-executor-sites" { "production-raw-blocking-executor-disposition.md" }
        "scheduler-sites" { "production-scheduler-boundary-disposition.md" }
        "dedicated-thread-sites" { "production-dedicated-thread-disposition.md" }
        default { "production-$key-disposition.md" }
    }

    Write-BoundaryDispositionReport `
        -Title ("Production " + ($key -replace "-", " ") + " Disposition") `
        -Matches $classified `
        -Path (Join-Path $auditRoot $reportName)
}

$boundaryBaselineTemplatePath = Join-Path $auditRoot "runtime-boundary-baseline-template.json"
Write-BoundaryBaselineTemplate `
    -BoundaryDispositionByCategory $productionBoundaryDispositionByCategory `
    -Path $boundaryBaselineTemplatePath

$boundaryBaselineComparison = $null
if ($EnforceBoundaryBaseline) {
    $boundaryBaselineComparison = Compare-BoundaryBaseline `
        -BoundaryDispositionByCategory $productionBoundaryDispositionByCategory `
        -Path $boundaryBaselinePath
}
elseif (Test-Path -LiteralPath $boundaryBaselinePath) {
    $boundaryBaselineComparison = Compare-BoundaryBaseline `
        -BoundaryDispositionByCategory $productionBoundaryDispositionByCategory `
        -Path $boundaryBaselinePath
}

Write-RuntimeBoundarySummary `
    -BoundaryDispositionByCategory $productionBoundaryDispositionByCategory `
    -BaselineComparison $boundaryBaselineComparison `
    -MarkdownPath (Join-Path $auditRoot "runtime-boundary-summary.md") `
    -JsonPath (Join-Path $auditRoot "runtime-boundary-summary.json")

if ($EnforceBoundaryBaseline -and $null -ne $boundaryBaselineComparison -and $boundaryBaselineComparison.Status -ne "passed") {
    throw "Runtime boundary baseline enforcement failed. See $(Join-Path $auditRoot "runtime-boundary-summary.md")."
}

$classificationLines = @(
    "# Runtime Task Classification",
    "",
    "Generated: $(Get-Date -Format o)",
    "",
    "| Classification | Migration target | Audit source |",
    "|---|---|---|",
    "| short async task | RuntimeHandle::spawn + tracing span | runtime-spawn-sites.md |",
    "| long service task | TaskGroup::spawn_service | runtime-spawn-sites.md, shutdown-sites.md |",
    "| scheduled task | ScheduledTaskGroup | scheduler-sites.md |",
    "| connection task | remoting connection TaskGroup | runtime-spawn-sites.md |",
    "| blocking task | BlockingExecutor | blocking-sites.md |",
    "| legacy runtime compatibility | RuntimeOwner or RuntimeContext compatibility adapter | legacy-runtime-api-sites.md |",
    "| dedicated loop | dedicated thread + CancellationToken | runtime-creation-sites.md, shutdown-sites.md |",
    "| detached task | DetachedTaskPolicy or eliminate | runtime-spawn-sites.md |",
    "",
    "Each audit row includes a Scope column. Scope is production, test, benchmark, or example.",
    "Benchmark scope includes benches/ paths, inline modules named bench_support, and diagnostic functions named run_*_lifecycle_probe.",
    "Test scope includes tests/ paths and best-effort source ranges under #[cfg(test)] or #[tokio::test].",
    "Production-only reports are emitted as production-*.md so migration planning can ignore test harness noise.",
    '`production-runtime-spawn-disposition.md` separates allowed runtime primitives and dedicated OS threads from remaining follow-up items.',
    '`production-runtime-creation-disposition.md` separates entrypoint runtimes, runtime primitives, documented compatibility bridges, and remaining follow-up items.',
    '`production-scheduler-disposition.md` separates scheduler primitives, documented business timers, protocol timeouts, and remaining follow-up items.',
    '`production-blocking-disposition.md` separates blocking primitives, storage domains, bootstrap/config I/O, tooling, metric/protocol false positives, and remaining follow-up items.',
    '`production-shutdown-disposition.md` separates runtime primitives, crate lifecycle boundaries, tool/dashboard boundaries, error-field false positives, and remaining follow-up items.',
    '`runtime-boundary-summary.md` separates parented runtime boundaries, compatibility adapters, and unparented runtime debt guarded by baseline enforcement.',
    "Comment-only Rust lines are omitted so documentation examples do not count as runtime sites.",
    "",
    "Manual review is still required before migrating each site."
)
$classificationLines -join [Environment]::NewLine | Out-File -Encoding utf8 (Join-Path $auditRoot "task-classification.md")

$summary = [ordered]@{
    generated_at = (Get-Date -Format o)
    total = [ordered]@{}
    production_total = [ordered]@{}
    by_scope = [ordered]@{}
    by_crate = [ordered]@{}
    production_by_crate = [ordered]@{}
}

$scopes = @("production", "test", "benchmark", "example")
foreach ($key in $patterns.Keys) {
    $summary.total[$key] = $allMatches[$key].Count
    $summary.production_total[$key] = Get-CountForScope -Matches $allMatches[$key] -Scope "production"
    $summary.by_scope[$key] = [ordered]@{}
    foreach ($scope in $scopes) {
        $summary.by_scope[$key][$scope] = Get-CountForScope -Matches $allMatches[$key] -Scope $scope
    }
}

$crates = $allMatches.Values | ForEach-Object { $_ } | Select-Object -ExpandProperty Crate -Unique | Sort-Object
foreach ($crate in $crates) {
    $summary.by_crate[$crate] = [ordered]@{}
    $summary.production_by_crate[$crate] = [ordered]@{}
    foreach ($key in $patterns.Keys) {
        $summary.by_crate[$crate][$key] = Get-CountForCrate -Matches $allMatches[$key] -Crate $crate
        $summary.production_by_crate[$crate][$key] = Get-CountForCrateAndScope `
            -Matches $allMatches[$key] `
            -Crate $crate `
            -Scope "production"
    }
}

$spawnDispositionByKind = [ordered]@{}
foreach ($disposition in ($productionRuntimeSpawnDisposition | Select-Object -ExpandProperty Disposition -Unique | Sort-Object)) {
    $spawnDispositionByKind[$disposition] = @($productionRuntimeSpawnDisposition | Where-Object { $_.Disposition -eq $disposition }).Count
}
$summary.production_runtime_spawn_disposition = [ordered]@{
    total = $productionRuntimeSpawnDisposition.Count
    action_required = @($productionRuntimeSpawnDisposition | Where-Object { $_.ActionRequired }).Count
    allowed_or_documented = @($productionRuntimeSpawnDisposition | Where-Object { -not $_.ActionRequired }).Count
    by_disposition = $spawnDispositionByKind
}
$creationDispositionByKind = [ordered]@{}
foreach ($disposition in ($productionRuntimeCreationDisposition | Select-Object -ExpandProperty Disposition -Unique | Sort-Object)) {
    $creationDispositionByKind[$disposition] = @($productionRuntimeCreationDisposition | Where-Object { $_.Disposition -eq $disposition }).Count
}
$summary.production_runtime_creation_disposition = [ordered]@{
    total = $productionRuntimeCreationDisposition.Count
    action_required = @($productionRuntimeCreationDisposition | Where-Object { $_.ActionRequired }).Count
    allowed_or_documented = @($productionRuntimeCreationDisposition | Where-Object { -not $_.ActionRequired }).Count
    by_disposition = $creationDispositionByKind
}
$schedulerDispositionByKind = [ordered]@{}
foreach ($disposition in ($productionSchedulerDisposition | Select-Object -ExpandProperty Disposition -Unique | Sort-Object)) {
    $schedulerDispositionByKind[$disposition] = @($productionSchedulerDisposition | Where-Object { $_.Disposition -eq $disposition }).Count
}
$summary.production_scheduler_disposition = [ordered]@{
    total = $productionSchedulerDisposition.Count
    action_required = @($productionSchedulerDisposition | Where-Object { $_.ActionRequired }).Count
    allowed_or_documented = @($productionSchedulerDisposition | Where-Object { -not $_.ActionRequired }).Count
    by_disposition = $schedulerDispositionByKind
}
$blockingDispositionByKind = [ordered]@{}
foreach ($disposition in ($productionBlockingDisposition | Select-Object -ExpandProperty Disposition -Unique | Sort-Object)) {
    $blockingDispositionByKind[$disposition] = @($productionBlockingDisposition | Where-Object { $_.Disposition -eq $disposition }).Count
}
$summary.production_blocking_disposition = [ordered]@{
    total = $productionBlockingDisposition.Count
    action_required = @($productionBlockingDisposition | Where-Object { $_.ActionRequired }).Count
    allowed_or_documented = @($productionBlockingDisposition | Where-Object { -not $_.ActionRequired }).Count
    by_disposition = $blockingDispositionByKind
}
$shutdownDispositionByKind = [ordered]@{}
foreach ($disposition in ($productionShutdownDisposition | Select-Object -ExpandProperty Disposition -Unique | Sort-Object)) {
    $shutdownDispositionByKind[$disposition] = @($productionShutdownDisposition | Where-Object { $_.Disposition -eq $disposition }).Count
}
$summary.production_shutdown_disposition = [ordered]@{
    total = $productionShutdownDisposition.Count
    action_required = @($productionShutdownDisposition | Where-Object { $_.ActionRequired }).Count
    allowed_or_documented = @($productionShutdownDisposition | Where-Object { -not $_.ActionRequired }).Count
    by_disposition = $shutdownDispositionByKind
}
$summary.production_runtime_boundary_disposition = [ordered]@{}
foreach ($key in ($boundaryKeys | Sort-Object)) {
    $matches = @($productionBoundaryDispositionByCategory[$key])
    $summary.production_runtime_boundary_disposition[$key] = [ordered]@{
        total = $matches.Count
        action_required = @($matches | Where-Object { $_.ActionRequired }).Count
        allowed_or_documented = @($matches | Where-Object { -not $_.ActionRequired }).Count
        by_disposition = Get-DispositionCounts -Matches $matches
    }
}

$summary | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 (Join-Path $auditRoot "summary.json")

$riskSummaryLines = New-Object System.Collections.Generic.List[string]
$riskSummaryLines.Add("# Production Runtime Risk Summary")
$riskSummaryLines.Add("")
$riskSummaryLines.Add("Generated: $(Get-Date -Format o)")
$riskSummaryLines.Add("")
$riskSummaryLines.Add("| Audit source | All matches | Production | Test | Benchmark | Example |")
$riskSummaryLines.Add("|---|---:|---:|---:|---:|---:|")
foreach ($key in $patterns.Keys) {
    $riskSummaryLines.Add(
        "| $key | $($allMatches[$key].Count) | $(Get-CountForScope -Matches $allMatches[$key] -Scope "production") | $(Get-CountForScope -Matches $allMatches[$key] -Scope "test") | $(Get-CountForScope -Matches $allMatches[$key] -Scope "benchmark") | $(Get-CountForScope -Matches $allMatches[$key] -Scope "example") |"
    )
}
$riskSummaryLines.Add("")
$riskSummaryLines.Add("Runtime spawn disposition: $(@($productionRuntimeSpawnDisposition | Where-Object { $_.ActionRequired }).Count) action-required, $(@($productionRuntimeSpawnDisposition | Where-Object { -not $_.ActionRequired }).Count) allowed-or-documented.")
$riskSummaryLines.Add("Runtime creation disposition: $(@($productionRuntimeCreationDisposition | Where-Object { $_.ActionRequired }).Count) action-required, $(@($productionRuntimeCreationDisposition | Where-Object { -not $_.ActionRequired }).Count) allowed-or-documented.")
$riskSummaryLines.Add("Scheduler disposition: $(@($productionSchedulerDisposition | Where-Object { $_.ActionRequired }).Count) action-required, $(@($productionSchedulerDisposition | Where-Object { -not $_.ActionRequired }).Count) allowed-or-documented.")
$riskSummaryLines.Add("Blocking disposition: $(@($productionBlockingDisposition | Where-Object { $_.ActionRequired }).Count) action-required, $(@($productionBlockingDisposition | Where-Object { -not $_.ActionRequired }).Count) allowed-or-documented.")
$riskSummaryLines.Add("Shutdown disposition: $(@($productionShutdownDisposition | Where-Object { $_.ActionRequired }).Count) action-required, $(@($productionShutdownDisposition | Where-Object { -not $_.ActionRequired }).Count) allowed-or-documented.")
$riskSummaryLines.Add("")
$riskSummaryLines.Add("Runtime boundary baseline status: $(if ($null -eq $boundaryBaselineComparison) { "not-checked" } else { $boundaryBaselineComparison.Status }).")
foreach ($key in ($boundaryKeys | Sort-Object)) {
    $matches = @($productionBoundaryDispositionByCategory[$key])
    $riskSummaryLines.Add("$key boundary disposition: $(@($matches | Where-Object { $_.ActionRequired }).Count) action-required, $(@($matches | Where-Object { -not $_.ActionRequired }).Count) allowed-or-documented.")
}
$riskSummaryLines -join [Environment]::NewLine | Out-File -Encoding utf8 (Join-Path $auditRoot "production-risk-summary.md")

if (-not $SkipBaseline) {
    Write-BaselineJson -Path (Join-Path $baselineRoot "client-runtime-spawn.json") -Metrics @{
        spawn_sites = Get-CountForCrate -Matches $allMatches["runtime-spawn-sites"] -Crate "rocketmq-client"
        production_spawn_sites = Get-CountForCrateAndScope -Matches $allMatches["runtime-spawn-sites"] -Crate "rocketmq-client" -Scope "production"
        runtime_creation_sites = Get-CountForCrate -Matches $allMatches["runtime-creation-sites"] -Crate "rocketmq-client"
        production_runtime_creation_sites = Get-CountForCrateAndScope -Matches $allMatches["runtime-creation-sites"] -Crate "rocketmq-client" -Scope "production"
    }
    Write-BaselineJson -Path (Join-Path $baselineRoot "namesrv-shutdown.json") -Metrics @{
        shutdown_sites = Get-CountForCrate -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-namesrv"
        production_shutdown_sites = Get-CountForCrateAndScope -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-namesrv" -Scope "production"
        scheduler_sites = Get-CountForCrate -Matches $allMatches["scheduler-sites"] -Crate "rocketmq-namesrv"
        production_scheduler_sites = Get-CountForCrateAndScope -Matches $allMatches["scheduler-sites"] -Crate "rocketmq-namesrv" -Scope "production"
    }
    Write-BaselineJson -Path (Join-Path $baselineRoot "broker-runtime-lifecycle.json") -Metrics @{
        spawn_sites = Get-CountForCrate -Matches $allMatches["runtime-spawn-sites"] -Crate "rocketmq-broker"
        production_spawn_sites = Get-CountForCrateAndScope -Matches $allMatches["runtime-spawn-sites"] -Crate "rocketmq-broker" -Scope "production"
        runtime_creation_sites = Get-CountForCrate -Matches $allMatches["runtime-creation-sites"] -Crate "rocketmq-broker"
        production_runtime_creation_sites = Get-CountForCrateAndScope -Matches $allMatches["runtime-creation-sites"] -Crate "rocketmq-broker" -Scope "production"
        shutdown_sites = Get-CountForCrate -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-broker"
        production_shutdown_sites = Get-CountForCrateAndScope -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-broker" -Scope "production"
    }
    Write-BaselineJson -Path (Join-Path $baselineRoot "remoting-connection-lifecycle.json") -Metrics @{
        spawn_sites = Get-CountForCrate -Matches $allMatches["runtime-spawn-sites"] -Crate "rocketmq-remoting"
        production_spawn_sites = Get-CountForCrateAndScope -Matches $allMatches["runtime-spawn-sites"] -Crate "rocketmq-remoting" -Scope "production"
        shutdown_sites = Get-CountForCrate -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-remoting"
        production_shutdown_sites = Get-CountForCrateAndScope -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-remoting" -Scope "production"
    }
    Write-BaselineJson -Path (Join-Path $baselineRoot "scheduler.json") -Metrics @{
        rocketmq_scheduler_sites = Get-CountForCrate -Matches $allMatches["scheduler-sites"] -Crate "rocketmq"
        rocketmq_production_scheduler_sites = Get-CountForCrateAndScope -Matches $allMatches["scheduler-sites"] -Crate "rocketmq" -Scope "production"
        namesrv_scheduler_sites = Get-CountForCrate -Matches $allMatches["scheduler-sites"] -Crate "rocketmq-namesrv"
        namesrv_production_scheduler_sites = Get-CountForCrateAndScope -Matches $allMatches["scheduler-sites"] -Crate "rocketmq-namesrv" -Scope "production"
        broker_scheduler_sites = Get-CountForCrate -Matches $allMatches["scheduler-sites"] -Crate "rocketmq-broker"
        broker_production_scheduler_sites = Get-CountForCrateAndScope -Matches $allMatches["scheduler-sites"] -Crate "rocketmq-broker" -Scope "production"
    }
    Write-BaselineJson -Path (Join-Path $baselineRoot "store-blocking.json") -Metrics @{
        blocking_sites = Get-CountForCrate -Matches $allMatches["blocking-sites"] -Crate "rocketmq-store"
        production_blocking_sites = Get-CountForCrateAndScope -Matches $allMatches["blocking-sites"] -Crate "rocketmq-store" -Scope "production"
        shutdown_sites = Get-CountForCrate -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-store"
        production_shutdown_sites = Get-CountForCrateAndScope -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-store" -Scope "production"
    }
}

Write-Host "Runtime audit written to $auditRoot"
if (-not $SkipBaseline) {
    Write-Host "Baseline JSON written to $baselineRoot"
}

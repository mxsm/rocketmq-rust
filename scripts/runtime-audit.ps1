[CmdletBinding()]
param(
    [string]$OutputDirectory = "target/runtime-audit",
    [string]$BaselineDirectory = "target/runtime-baseline/before",
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
        [Parameter(Mandatory = $true)][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Path
    )

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
    param([Parameter(Mandatory = $true)][array]$Matches)

    return @($Matches | Where-Object { $_.Scope -eq "production" })
}

function Get-CountForCrate {
    param(
        [Parameter(Mandatory = $true)][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Crate
    )

    return @($Matches | Where-Object { $_.Crate -eq $Crate }).Count
}

function Get-CountForScope {
    param(
        [Parameter(Mandatory = $true)][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Scope
    )

    return @($Matches | Where-Object { $_.Scope -eq $Scope }).Count
}

function Get-CountForCrateAndScope {
    param(
        [Parameter(Mandatory = $true)][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Crate,
        [Parameter(Mandatory = $true)][string]$Scope
    )

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

    if ($path -eq "rocketmq-store/src/base/allocate_mapped_file_service.rs") {
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
}

$allMatches = @{}
foreach ($entry in $patterns.GetEnumerator()) {
    $matches = Get-RuntimeMatches -Pattern $entry.Value
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

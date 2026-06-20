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

function Get-RuntimeAuditScope {
    param(
        [Parameter(Mandatory = $true)][string]$RelativePath,
        [Parameter(Mandatory = $true)][int]$Line
    )

    $normalized = $RelativePath.Replace("\", "/")
    if ($normalized -match "(^|/)benches/") {
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
    "Test scope includes tests/ paths and best-effort source ranges under #[cfg(test)] or #[tokio::test].",
    "Production-only reports are emitted as production-*.md so migration planning can ignore test harness noise.",
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

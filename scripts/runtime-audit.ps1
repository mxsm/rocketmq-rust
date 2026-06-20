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
    $resolvedPath = (Resolve-Path -LiteralPath $path).Path
    $relativePath = $resolvedPath
    if ($resolvedPath.StartsWith($workspaceRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        $relativePath = $resolvedPath.Substring($workspaceRoot.Length).TrimStart("\", "/")
    }
    $relativePath = $relativePath.Replace("\", "/")
    $crate = ($relativePath -split "/")[0]

    [pscustomobject]@{
        Crate = $crate
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
        $lines.Add("| Crate | File | Line | Code |")
        $lines.Add("|---|---|---:|---|")
        foreach ($match in $Matches) {
            $fileCell = ConvertTo-MarkdownInlineCode -Text $match.Path
            $code = ConvertTo-MarkdownInlineCode -Text $match.Text
            $lines.Add("| $($match.Crate) | $fileCell | $($match.Line) | $code |")
        }
    }

    $lines -join [Environment]::NewLine | Out-File -Encoding utf8 $Path
}

function Get-CountForCrate {
    param(
        [Parameter(Mandatory = $true)][array]$Matches,
        [Parameter(Mandatory = $true)][string]$Crate
    )

    return @($Matches | Where-Object { $_.Crate -eq $Crate }).Count
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
    "Manual review is still required before migrating each site."
)
$classificationLines -join [Environment]::NewLine | Out-File -Encoding utf8 (Join-Path $auditRoot "task-classification.md")

$summary = [ordered]@{
    generated_at = (Get-Date -Format o)
    total = [ordered]@{}
    by_crate = [ordered]@{}
}

foreach ($key in $patterns.Keys) {
    $summary.total[$key] = $allMatches[$key].Count
}

$crates = $allMatches.Values | ForEach-Object { $_ } | Select-Object -ExpandProperty Crate -Unique | Sort-Object
foreach ($crate in $crates) {
    $summary.by_crate[$crate] = [ordered]@{}
    foreach ($key in $patterns.Keys) {
        $summary.by_crate[$crate][$key] = Get-CountForCrate -Matches $allMatches[$key] -Crate $crate
    }
}

$summary | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 (Join-Path $auditRoot "summary.json")

if (-not $SkipBaseline) {
    Write-BaselineJson -Path (Join-Path $baselineRoot "client-runtime-spawn.json") -Metrics @{
        spawn_sites = Get-CountForCrate -Matches $allMatches["runtime-spawn-sites"] -Crate "rocketmq-client"
        runtime_creation_sites = Get-CountForCrate -Matches $allMatches["runtime-creation-sites"] -Crate "rocketmq-client"
    }
    Write-BaselineJson -Path (Join-Path $baselineRoot "namesrv-shutdown.json") -Metrics @{
        shutdown_sites = Get-CountForCrate -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-namesrv"
        scheduler_sites = Get-CountForCrate -Matches $allMatches["scheduler-sites"] -Crate "rocketmq-namesrv"
    }
    Write-BaselineJson -Path (Join-Path $baselineRoot "broker-runtime-lifecycle.json") -Metrics @{
        spawn_sites = Get-CountForCrate -Matches $allMatches["runtime-spawn-sites"] -Crate "rocketmq-broker"
        runtime_creation_sites = Get-CountForCrate -Matches $allMatches["runtime-creation-sites"] -Crate "rocketmq-broker"
        shutdown_sites = Get-CountForCrate -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-broker"
    }
    Write-BaselineJson -Path (Join-Path $baselineRoot "remoting-connection-lifecycle.json") -Metrics @{
        spawn_sites = Get-CountForCrate -Matches $allMatches["runtime-spawn-sites"] -Crate "rocketmq-remoting"
        shutdown_sites = Get-CountForCrate -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-remoting"
    }
    Write-BaselineJson -Path (Join-Path $baselineRoot "scheduler.json") -Metrics @{
        rocketmq_scheduler_sites = Get-CountForCrate -Matches $allMatches["scheduler-sites"] -Crate "rocketmq"
        namesrv_scheduler_sites = Get-CountForCrate -Matches $allMatches["scheduler-sites"] -Crate "rocketmq-namesrv"
        broker_scheduler_sites = Get-CountForCrate -Matches $allMatches["scheduler-sites"] -Crate "rocketmq-broker"
    }
    Write-BaselineJson -Path (Join-Path $baselineRoot "store-blocking.json") -Metrics @{
        blocking_sites = Get-CountForCrate -Matches $allMatches["blocking-sites"] -Crate "rocketmq-store"
        shutdown_sites = Get-CountForCrate -Matches $allMatches["shutdown-sites"] -Crate "rocketmq-store"
    }
}

Write-Host "Runtime audit written to $auditRoot"
if (-not $SkipBaseline) {
    Write-Host "Baseline JSON written to $baselineRoot"
}

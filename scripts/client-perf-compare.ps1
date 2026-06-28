[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Baseline,
    [Parameter(Mandatory = $true)]
    [string]$Current,
    [double]$MaxRegressionPercent = 5.0,
    [string]$OutputFile = "",
    [string]$MarkdownFile = "",
    [switch]$WarnOnlyMissing
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Resolve-RepoPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return $Path
    }
    return Join-Path $workspaceRoot $Path
}

function Read-Report {
    param([Parameter(Mandatory = $true)][string]$Path)

    $resolved = Resolve-RepoPath -Path $Path
    if (-not (Test-Path $resolved)) {
        throw "Performance report does not exist: $Path"
    }
    return Get-Content -Path $resolved -Raw | ConvertFrom-Json
}

function ConvertTo-ResultMap {
    param([AllowNull()]$Results)

    $map = @{}
    if ($null -eq $Results) {
        return $map
    }

    foreach ($result in @($Results)) {
        if ($null -eq $result) {
            continue
        }
        $idProperty = $result.PSObject.Properties["id"]
        if ($null -ne $idProperty -and
            $null -ne $idProperty.Value -and
            -not [string]::IsNullOrWhiteSpace([string]$idProperty.Value)) {
            $map[[string]$idProperty.Value] = $result
        }
    }
    return $map
}

function Get-MetricPairs {
    param(
        [string]$Prefix,
        [AllowNull()]$Value
    )

    $pairs = @()
    if ($null -eq $Value) {
        return $pairs
    }

    if ($Value -is [System.Array]) {
        for ($index = 0; $index -lt $Value.Count; $index++) {
            $name = if ([string]::IsNullOrWhiteSpace($Prefix)) { "[$index]" } else { "$Prefix[$index]" }
            $pairs += Get-MetricPairs -Prefix $name -Value $Value[$index]
        }
        return $pairs
    }

    if ($Value -is [hashtable]) {
        foreach ($key in $Value.Keys) {
            $name = if ([string]::IsNullOrWhiteSpace($Prefix)) { [string]$key } else { "$Prefix.$key" }
            $pairs += Get-MetricPairs -Prefix $name -Value $Value[$key]
        }
        return $pairs
    }

    if ($Value -is [pscustomobject]) {
        foreach ($property in $Value.PSObject.Properties) {
            $name = if ([string]::IsNullOrWhiteSpace($Prefix)) { $property.Name } else { "$Prefix.$($property.Name)" }
            $pairs += Get-MetricPairs -Prefix $name -Value $property.Value
        }
        return $pairs
    }

    return @([ordered]@{ name = $Prefix; value = $Value })
}

function Try-GetDouble {
    param(
        [AllowNull()]$Value,
        [ref]$Number
    )

    if ($null -eq $Value) {
        return $false
    }
    return [double]::TryParse(
        [string]$Value,
        [System.Globalization.NumberStyles]::Float,
        [System.Globalization.CultureInfo]::InvariantCulture,
        $Number
    )
}

function Test-FailureMetric {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [AllowNull()]$Value
    )

    if ($Name -match "(^|[.])healthy$" -and $Value -is [bool] -and -not $Value) {
        return "healthy=false"
    }

    $number = 0.0
    if (Try-GetDouble -Value $Value -Number ([ref]$number)) {
        if ($number -gt 0 -and $Name -match "(leak|leaked|still_running|timed_out|failed|failure)") {
            return "$Name=$number"
        }
    }

    return ""
}

$baselineReport = Read-Report -Path $Baseline
$currentReport = Read-Report -Path $Current

$baselineMap = ConvertTo-ResultMap -Results $baselineReport.criterion_results
$currentMap = ConvertTo-ResultMap -Results $currentReport.criterion_results

$comparisons = @()
$failures = @()
$warnings = @()

foreach ($id in ($baselineMap.Keys | Sort-Object)) {
    if (-not $currentMap.ContainsKey($id)) {
        $message = "Missing current Criterion result for $id"
        if ($WarnOnlyMissing) {
            $warnings += $message
        }
        else {
            $failures += $message
        }
        continue
    }

    $baselineValue = [double]$baselineMap[$id].mean_point_estimate
    $currentValue = [double]$currentMap[$id].mean_point_estimate
    if ($baselineValue -le 0) {
        $warnings += "Skipping $id because baseline mean is not positive: $baselineValue"
        continue
    }

    $changePercent = (($currentValue - $baselineValue) / $baselineValue) * 100.0
    $status = if ($changePercent -gt $MaxRegressionPercent) { "fail" } elseif ($changePercent -lt 0) { "improved" } else { "pass" }

    if ($status -eq "fail") {
        $failures += ("Criterion regression for {0}: {1:N2}% > {2:N2}%" -f $id, $changePercent, $MaxRegressionPercent)
    }

    $comparisons += [ordered]@{
        id = $id
        unit = "ns"
        baseline_mean = $baselineValue
        current_mean = $currentValue
        change_percent = [math]::Round($changePercent, 4)
        status = $status
    }
}

foreach ($id in ($currentMap.Keys | Sort-Object)) {
    if (-not $baselineMap.ContainsKey($id)) {
        $warnings += "New current Criterion result without baseline: $id"
    }
}

$artifactChecks = @()
foreach ($artifact in @($currentReport.artifact_results)) {
    if ($null -eq $artifact) {
        continue
    }
    $pathProperty = $artifact.PSObject.Properties["path"]
    $artifactPath = if ($null -ne $pathProperty -and $null -ne $pathProperty.Value) { [string]$pathProperty.Value } else { "artifact" }
    $metricsProperty = $artifact.PSObject.Properties["metrics"]
    $metricsValue = if ($null -ne $metricsProperty) { $metricsProperty.Value } else { $null }
    foreach ($metric in Get-MetricPairs -Prefix "" -Value $metricsValue) {
        $failureReason = Test-FailureMetric -Name $metric.name -Value $metric.value
        if (-not [string]::IsNullOrWhiteSpace($failureReason)) {
            $message = "$artifactPath reports $failureReason"
            $failures += $message
            $artifactChecks += [ordered]@{
                artifact = $artifactPath
                metric = $metric.name
                value = $metric.value
                status = "fail"
                reason = $failureReason
            }
        }
    }
}

if (@($comparisons).Count -eq 0) {
    $warnings += "No matching Criterion results were compared."
}

$status = if (@($failures).Count -gt 0) { "fail" } else { "pass" }

if ([string]::IsNullOrWhiteSpace($OutputFile)) {
    $outputRoot = Join-Path $workspaceRoot "target/client-perf"
    New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
    $OutputFile = Join-Path $outputRoot "compare-$(Get-Date -Format 'yyyyMMdd-HHmmss').json"
}
elseif (-not [System.IO.Path]::IsPathRooted($OutputFile)) {
    $OutputFile = Join-Path $workspaceRoot $OutputFile
}

if ([string]::IsNullOrWhiteSpace($MarkdownFile)) {
    $MarkdownFile = [System.IO.Path]::ChangeExtension($OutputFile, ".md")
}
elseif (-not [System.IO.Path]::IsPathRooted($MarkdownFile)) {
    $MarkdownFile = Join-Path $workspaceRoot $MarkdownFile
}

$result = [ordered]@{
    schema_version = 1
    source = "scripts/client-perf-compare.ps1"
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    status = $status
    max_regression_percent = $MaxRegressionPercent
    baseline = [ordered]@{
        path = $Baseline
        commit = $baselineReport.git.short_commit
        generated_at = $baselineReport.generated_at
    }
    current = [ordered]@{
        path = $Current
        commit = $currentReport.git.short_commit
        generated_at = $currentReport.generated_at
    }
    compared_criterion_count = @($comparisons).Count
    failures = $failures
    warnings = $warnings
    comparisons = $comparisons
    artifact_checks = $artifactChecks
}

$result | ConvertTo-Json -Depth 24 | Out-File -Encoding utf8 -FilePath $OutputFile

$markdown = @(
    "# Client Performance Compare",
    "",
    "- Status: $status",
    "- Max regression: $MaxRegressionPercent%",
    "- Baseline: `$Baseline` ($($result.baseline.commit))",
    "- Current: `$Current` ($($result.current.commit))",
    "- Compared Criterion results: $(@($comparisons).Count)",
    "- Failures: $(@($failures).Count)",
    "- Warnings: $(@($warnings).Count)",
    "",
    "## Criterion",
    "",
    "| Benchmark | Change % | Status | Baseline ns | Current ns |",
    "|---|---:|---|---:|---:|"
)

foreach ($comparison in $comparisons) {
    $markdown += "| $($comparison.id) | $($comparison.change_percent) | $($comparison.status) | $($comparison.baseline_mean) | $($comparison.current_mean) |"
}

if (@($failures).Count -gt 0) {
    $markdown += @("", "## Failures", "")
    foreach ($failure in $failures) {
        $markdown += "- $failure"
    }
}

if (@($warnings).Count -gt 0) {
    $markdown += @("", "## Warnings", "")
    foreach ($warning in $warnings) {
        $markdown += "- $warning"
    }
}

$markdown | Set-Content -Encoding utf8 -Path $MarkdownFile

Write-Host "Client performance compare JSON report: $OutputFile"
Write-Host "Client performance compare Markdown report: $MarkdownFile"
Write-Host "Client performance compare status: $status"

if ($status -ne "pass") {
    exit 1
}

param(
    [Parameter(Mandatory = $true)] [string] $PrometheusBaseUrl,
    [Parameter(Mandatory = $true)] [string] $ThroughputQuery,
    [Parameter(Mandatory = $true)] [string] $P99Query,
    [Parameter(Mandatory = $true)] [ValidateSet("baseline", "candidate")] [string] $Phase,
    [ValidateRange(60, 86400)] [int] $DurationSeconds = 900,
    [ValidateRange(1, 300)] [int] $SampleIntervalSeconds = 15,
    [string] $BaselineSummary = "",
    [double] $MaxThroughputDecreasePercent = 1.0,
    [double] $MaxP99IncreasePercent = 2.0,
    [string] $EvidenceDirectory = "target/log-filter-evidence"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ($Phase -eq "candidate" -and [string]::IsNullOrWhiteSpace($BaselineSummary)) {
    throw "BaselineSummary is required for the candidate phase"
}
$evidenceRoot = [System.IO.Path]::GetFullPath($EvidenceDirectory)
[System.IO.Directory]::CreateDirectory($evidenceRoot) | Out-Null

function Invoke-PrometheusScalar {
    param([Parameter(Mandatory = $true)] [string] $Query)

    $base = $PrometheusBaseUrl.TrimEnd('/')
    $uri = "$base/api/v1/query?query=$([uri]::EscapeDataString($Query))"
    $response = Invoke-RestMethod -Method Get -Uri $uri -TimeoutSec 30
    if ($response.status -ne "success" -or $response.data.result.Count -ne 1) {
        throw "Prometheus query must return exactly one time series: $Query"
    }
    $value = [double]$response.data.result[0].value[1]
    if ([double]::IsNaN($value) -or [double]::IsInfinity($value)) {
        throw "Prometheus query returned a non-finite value: $Query"
    }
    return $value
}

$samples = [System.Collections.Generic.List[object]]::new()
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
do {
    $samples.Add([pscustomobject]@{
        timestamp = [DateTimeOffset]::UtcNow.ToString("O")
        throughput = Invoke-PrometheusScalar -Query $ThroughputQuery
        p99 = Invoke-PrometheusScalar -Query $P99Query
    })
    if ($stopwatch.Elapsed.TotalSeconds -lt $DurationSeconds) {
        Start-Sleep -Seconds $SampleIntervalSeconds
    }
} while ($stopwatch.Elapsed.TotalSeconds -lt $DurationSeconds)
$stopwatch.Stop()

if ($samples.Count -lt 2) {
    throw "At least two canary samples are required"
}
$throughputAverage = ($samples | Measure-Object -Property throughput -Average).Average
$p99Average = ($samples | Measure-Object -Property p99 -Average).Average
$summary = [ordered]@{
    phase = $Phase
    started_at = $samples[0].timestamp
    completed_at = [DateTimeOffset]::UtcNow.ToString("O")
    duration_seconds = [math]::Round($stopwatch.Elapsed.TotalSeconds, 3)
    sample_interval_seconds = $SampleIntervalSeconds
    sample_count = $samples.Count
    throughput_query = $ThroughputQuery
    p99_query = $P99Query
    throughput_average = $throughputAverage
    p99_average = $p99Average
    throughput_change_percent = $null
    p99_change_percent = $null
    gate_passed = $true
}

if ($Phase -eq "candidate") {
    if (-not (Test-Path -LiteralPath $BaselineSummary -PathType Leaf)) {
        throw "Baseline summary does not exist: $BaselineSummary"
    }
    $baseline = Get-Content -LiteralPath $BaselineSummary -Raw | ConvertFrom-Json
    if ([double]$baseline.throughput_average -le 0 -or [double]$baseline.p99_average -le 0) {
        throw "Baseline averages must both be greater than zero"
    }
    $throughputChange = (($throughputAverage / [double]$baseline.throughput_average) - 1.0) * 100.0
    $p99Change = (($p99Average / [double]$baseline.p99_average) - 1.0) * 100.0
    $summary.throughput_change_percent = $throughputChange
    $summary.p99_change_percent = $p99Change
    $summary.gate_passed = $throughputChange -ge -$MaxThroughputDecreasePercent -and $p99Change -le $MaxP99IncreasePercent
}

$stamp = [DateTimeOffset]::UtcNow.ToString("yyyyMMdd-HHmmss")
$samplePath = Join-Path $evidenceRoot "log-filter-canary-$Phase-$stamp.csv"
$summaryPath = Join-Path $evidenceRoot "log-filter-canary-$Phase-$stamp.json"
$samples | Export-Csv -LiteralPath $samplePath -NoTypeInformation -Encoding utf8
$summary | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $summaryPath -Encoding utf8
if (-not $summary.gate_passed) {
    throw "Canary performance gate failed; evidence: $summaryPath"
}
Write-Host "Canary performance evidence collected. Summary: $summaryPath"

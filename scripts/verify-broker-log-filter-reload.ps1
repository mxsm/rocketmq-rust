param(
    [Parameter(Mandatory = $true)] [string] $AdminExecutable,
    [Parameter(Mandatory = $true)] [string] $BrokerAddress,
    [Parameter(Mandatory = $true)] [string] $AuditPath,
    [string] $NamesrvAddress = "",
    [string] $Filter = "info,rocketmq_broker=debug",
    [string] $Reason = "production log-filter verification",
    [string] $RequestId = "",
    [ValidateRange(60, 7200)] [int] $TtlSeconds = 60,
    [ValidateSet("ttl", "restore")] [string] $RestoreMode = "ttl",
    [ValidateRange(1, 600)] [int] $CommandTimeoutSeconds = 30,
    [ValidateRange(1, 10000)] [int] $MaxReloadLatencyMilliseconds = 100,
    [string] $EvidenceDirectory = "target/log-filter-evidence"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if (-not (Test-Path -LiteralPath $AdminExecutable -PathType Leaf)) {
    throw "Admin executable does not exist: $AdminExecutable"
}
if ([string]::IsNullOrWhiteSpace($env:ROCKETMQ_ACL_ACCESS_KEY) -or
    [string]::IsNullOrWhiteSpace($env:ROCKETMQ_ACL_SECRET_KEY)) {
    throw "ROCKETMQ_ACL_ACCESS_KEY and ROCKETMQ_ACL_SECRET_KEY must be set"
}
if ([string]::IsNullOrWhiteSpace($RequestId)) {
    $RequestId = "log-filter-$([guid]::NewGuid().ToString('N'))"
}

$evidenceRoot = [System.IO.Path]::GetFullPath($EvidenceDirectory)
[System.IO.Directory]::CreateDirectory($evidenceRoot) | Out-Null
$resolvedAuditPath = [System.IO.Path]::GetFullPath($AuditPath)

function Invoke-AdminUpdate {
    param(
        [Parameter(Mandatory = $true)] [string] $Label,
        [Parameter(Mandatory = $true)] [hashtable] $Properties
    )

    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = [System.IO.Path]::GetFullPath($AdminExecutable)
    $startInfo.WorkingDirectory = (Get-Location).Path
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    if (-not [string]::IsNullOrWhiteSpace($NamesrvAddress)) {
        $startInfo.ArgumentList.Add("-n")
        $startInfo.ArgumentList.Add($NamesrvAddress)
    }
    foreach ($argument in @("broker", "updateBrokerConfig", "-b", $BrokerAddress)) {
        $startInfo.ArgumentList.Add($argument)
    }
    foreach ($key in ($Properties.Keys | Sort-Object)) {
        $startInfo.ArgumentList.Add("-p")
        $startInfo.ArgumentList.Add("$key=$($Properties[$key])")
    }
    $startInfo.ArgumentList.Add("--yes")

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    if (-not $process.Start()) {
        throw "Failed to start admin CLI for $Label"
    }
    $stdoutRead = $process.StandardOutput.ReadToEndAsync()
    $stderrRead = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit($CommandTimeoutSeconds * 1000)) {
        $process.Kill($true)
        $process.WaitForExit()
        throw "Admin CLI timed out during $Label"
    }
    $stopwatch.Stop()
    $stdout = $stdoutRead.GetAwaiter().GetResult()
    $stderr = $stderrRead.GetAwaiter().GetResult()
    [System.IO.File]::WriteAllText((Join-Path $evidenceRoot "$Label.stdout.log"), $stdout)
    [System.IO.File]::WriteAllText((Join-Path $evidenceRoot "$Label.stderr.log"), $stderr)
    if ($process.ExitCode -ne 0) {
        throw "Admin CLI failed during $Label (exit=$($process.ExitCode)); inspect evidence logs"
    }
    [pscustomobject]@{
        label = $Label
        client_round_trip_milliseconds = $stopwatch.ElapsedMilliseconds
        exit_code = $process.ExitCode
    }
}

function Read-AuditRecords {
    if (-not (Test-Path -LiteralPath $resolvedAuditPath -PathType Leaf)) {
        return @()
    }
    $records = @()
    foreach ($line in [System.IO.File]::ReadAllLines($resolvedAuditPath)) {
        if (-not [string]::IsNullOrWhiteSpace($line)) {
            try {
                $records += $line | ConvertFrom-Json
            } catch {
                throw "Audit file contains invalid JSONL: $resolvedAuditPath"
            }
        }
    }
    return $records
}

function Wait-AuditPhase {
    param(
        [Parameter(Mandatory = $true)] [string] $ExpectedRequestId,
        [Parameter(Mandatory = $true)] [string] $Phase,
        [Parameter(Mandatory = $true)] [DateTimeOffset] $Deadline
    )

    do {
        $match = Read-AuditRecords |
            Where-Object { $_.request_id -eq $ExpectedRequestId -and $_.phase -eq $Phase } |
            Select-Object -Last 1
        if ($null -ne $match) {
            return $match
        }
        Start-Sleep -Milliseconds 200
    } while ([DateTimeOffset]::UtcNow -lt $Deadline)
    throw "Timed out waiting for audit phase '$Phase' for request '$ExpectedRequestId'"
}

$applyResult = Invoke-AdminUpdate -Label "$RequestId-apply" -Properties @{
    logFilter = $Filter
    logFilterReason = $Reason
    logFilterRequestId = $RequestId
    logFilterTtlSeconds = $TtlSeconds
}
$auditDeadline = [DateTimeOffset]::UtcNow.AddSeconds($CommandTimeoutSeconds)
$intent = Wait-AuditPhase -ExpectedRequestId $RequestId -Phase "intent" -Deadline $auditDeadline
$success = Wait-AuditPhase -ExpectedRequestId $RequestId -Phase "success" -Deadline $auditDeadline
if ($success.result -ne "success") {
    throw "Broker audit does not report a successful reload for '$RequestId'"
}
$reloadLatency = [int64]$success.reload_duration_millis
if ($reloadLatency -lt 0 -or $reloadLatency -gt $MaxReloadLatencyMilliseconds) {
    throw "Broker reload latency gate failed: ${reloadLatency}ms (limit ${MaxReloadLatencyMilliseconds}ms)"
}

$restoreRequestId = $null
$restoreResult = $null
if ($RestoreMode -eq "restore") {
    $restoreRequestId = "$RequestId-restore"
    $restoreResult = Invoke-AdminUpdate -Label "$restoreRequestId-apply" -Properties @{
        logFilterReason = "early restore after $RequestId"
        logFilterRequestId = $restoreRequestId
        logFilterRestore = "true"
    }
    $restoreAudit = Wait-AuditPhase -ExpectedRequestId $restoreRequestId -Phase "success" -Deadline ([DateTimeOffset]::UtcNow.AddSeconds($CommandTimeoutSeconds))
} else {
    $ttlDeadline = [DateTimeOffset]::UtcNow.AddSeconds($TtlSeconds + $CommandTimeoutSeconds)
    $restoreAudit = Wait-AuditPhase -ExpectedRequestId $RequestId -Phase "ttl_restore" -Deadline $ttlDeadline
}
if ($restoreAudit.result -ne "success") {
    throw "Broker did not restore the startup log-filter baseline successfully"
}

$summary = [pscustomobject]@{
    broker_address = $BrokerAddress
    request_id = $RequestId
    filter = $Filter
    ttl_seconds = $TtlSeconds
    restore_mode = $RestoreMode
    restore_request_id = $restoreRequestId
    broker_reload_latency_milliseconds = $reloadLatency
    max_reload_latency_milliseconds = $MaxReloadLatencyMilliseconds
    apply_client_round_trip_milliseconds = $applyResult.client_round_trip_milliseconds
    restore_client_round_trip_milliseconds = if ($null -eq $restoreResult) { $null } else { $restoreResult.client_round_trip_milliseconds }
    audit_path = $resolvedAuditPath
    verified_at = [DateTimeOffset]::UtcNow.ToString("O")
}
$summaryPath = Join-Path $evidenceRoot "$RequestId-summary.json"
$summary | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $summaryPath -Encoding utf8
Write-Host "Broker log-filter reload and restore gates passed. Evidence: $summaryPath"

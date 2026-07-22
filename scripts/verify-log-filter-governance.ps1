param(
    [Parameter(Mandatory = $true)] [ValidateSet("namesrv", "broker", "controller", "proxy")] [string] $Service,
    [Parameter(Mandatory = $true)] [string] $Executable,
    [string[]] $CommonArguments = @(),
    [string] $TargetFilter = "",
    [string] $TargetName = "",
    [int] $SampleSeconds = 60,
    [string] $EvidenceDirectory = "target/log-filter-evidence"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ($SampleSeconds -lt 1) {
    throw "SampleSeconds must be at least 1"
}
if (-not (Test-Path -LiteralPath $Executable -PathType Leaf)) {
    throw "Executable does not exist: $Executable"
}
if ([string]::IsNullOrWhiteSpace($TargetFilter)) {
    $TargetFilter = "info,rocketmq_$Service=debug"
}
if ([string]::IsNullOrWhiteSpace($TargetName)) {
    $TargetName = "rocketmq_$Service"
}

$evidenceRoot = [System.IO.Path]::GetFullPath($EvidenceDirectory)
[System.IO.Directory]::CreateDirectory($evidenceRoot) | Out-Null

function Invoke-LogProbe {
    param(
        [Parameter(Mandatory = $true)] [string] $Name,
        [Parameter(Mandatory = $true)] [AllowEmptyString()] [string] $RustLog,
        [string[]] $ExtraArguments = @(),
        [switch] $ExpectStartupFailure
    )

    $stdoutPath = Join-Path $evidenceRoot "$Service-$Name.stdout.log"
    $stderrPath = Join-Path $evidenceRoot "$Service-$Name.stderr.log"
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = [System.IO.Path]::GetFullPath($Executable)
    $startInfo.WorkingDirectory = (Get-Location).Path
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in @($CommonArguments) + @($ExtraArguments)) {
        $startInfo.ArgumentList.Add($argument)
    }
    if ([string]::IsNullOrEmpty($RustLog)) {
        $startInfo.Environment.Remove("RUST_LOG") | Out-Null
    } else {
        $startInfo.Environment["RUST_LOG"] = $RustLog
    }

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    if (-not $process.Start()) {
        throw "Failed to start $Service probe '$Name'"
    }
    $stdoutRead = $process.StandardOutput.ReadToEndAsync()
    $stderrRead = $process.StandardError.ReadToEndAsync()
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($SampleSeconds)
    while (-not $process.HasExited -and [DateTimeOffset]::UtcNow -lt $deadline) {
        Start-Sleep -Milliseconds 200
    }

    $wasRunning = -not $process.HasExited
    if ($wasRunning) {
        $process.Kill($true)
        $process.WaitForExit()
    }
    $stdout = $stdoutRead.GetAwaiter().GetResult()
    $stderr = $stderrRead.GetAwaiter().GetResult()
    [System.IO.File]::WriteAllText($stdoutPath, $stdout)
    [System.IO.File]::WriteAllText($stderrPath, $stderr)
    $combined = "$stdout`n$stderr"
    $debugCount = ([regex]::Matches($combined, "(?m)\bDEBUG\b")).Count
    $infoCount = ([regex]::Matches($combined, "(?m)\bINFO\b")).Count
    $nonTargetDebugCount = ([regex]::Matches($combined, "(?m)^(?=.*\bDEBUG\b)(?!.*$([regex]::Escape($TargetName))).*$")).Count
    $exitCode = if ($process.HasExited) { $process.ExitCode } else { $null }

    if ($ExpectStartupFailure) {
        if ($wasRunning -or $exitCode -eq 0) {
            throw "$Service invalid-filter probe did not fail before lifecycle startup"
        }
    } elseif (-not $wasRunning) {
        throw "$Service exited before the $SampleSeconds-second sampling window (exit=$exitCode)"
    }

    [pscustomobject]@{
        service = $Service
        probe = $Name
        rust_log = if ([string]::IsNullOrEmpty($RustLog)) { $null } else { $RustLog }
        debug_count = $debugCount
        info_count = $infoCount
        non_target_debug_count = $nonTargetDebugCount
        remained_running = $wasRunning
        exit_code = $exitCode
        stdout = $stdoutPath
        stderr = $stderrPath
        captured_at = [DateTimeOffset]::UtcNow.ToString("O")
    }
}

$results = @()
$results += Invoke-LogProbe -Name "default-info" -RustLog ""
if ($results[-1].debug_count -ne 0 -or $results[-1].info_count -le 0) {
    throw "$Service default gate failed: DEBUG must be 0 and INFO must be greater than 0"
}

$results += Invoke-LogProbe -Name "target-debug" -RustLog $TargetFilter
if ($results[-1].debug_count -le 0 -or $results[-1].non_target_debug_count -ne 0) {
    throw "$Service target gate failed: target DEBUG must be observable and non-target DEBUG must remain 0"
}

$results += Invoke-LogProbe -Name "invalid-filter" -RustLog "" -ExtraArguments @("--log-filter", "rocketmq==debug") -ExpectStartupFailure

$evidencePath = Join-Path $evidenceRoot "$Service-summary.json"
$results | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $evidencePath -Encoding utf8
Write-Host "Log-filter governance gates passed for $Service. Evidence: $evidencePath"

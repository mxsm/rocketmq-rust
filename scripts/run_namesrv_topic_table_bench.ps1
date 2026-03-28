[CmdletBinding()]
param(
    [string]$OutputFile = "docs/namesrv-topic-table-benchmark-raw.txt"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$resolvedOutput = Join-Path $workspaceRoot $OutputFile
$outputDirectory = Split-Path -Parent $resolvedOutput

if (-not (Test-Path $outputDirectory)) {
    New-Item -ItemType Directory -Path $outputDirectory | Out-Null
}

Push-Location $workspaceRoot
try {
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = "cmd.exe"
    $startInfo.Arguments = "/c cargo bench -p rocketmq-namesrv --bench topic_table_hot_path_bench"
    $startInfo.WorkingDirectory = $workspaceRoot
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.CreateNoWindow = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()

    $combined = ($stdout, $stderr) -join ""
    $combined | Tee-Object -FilePath $resolvedOutput | Out-Host

    if ($process.ExitCode -ne 0) {
        throw "topic_table_hot_path_bench failed"
    }

    Write-Host "Benchmark output saved to $resolvedOutput"
}
finally {
    Pop-Location
}

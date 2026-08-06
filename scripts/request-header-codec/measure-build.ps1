# Copyright 2023 The RocketMQ Rust Authors
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
    [Parameter(Mandatory = $true)]
    [ValidateSet('clean', 'incremental')]
    [string]$Mode,

    [Parameter(Mandatory = $true)]
    [string]$Variant,

    [Parameter(Mandatory = $true)]
    [string]$Output,

    [string]$Repository,

    [int]$Runs = 0,

    [switch]$DiagnosticAllowRecipeOverride,

    [switch]$KeepTargets
)

$ErrorActionPreference = 'Stop'
$scriptRoot = Split-Path -Parent $PSCommandPath
$repoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path -LiteralPath (Join-Path $scriptRoot '..\..')).Path
}
$recipePath = Join-Path $scriptRoot 'perf-build-recipe-v1.json'
$recipe = Get-Content -LiteralPath $recipePath -Raw | ConvertFrom-Json
$recipeRuns = [int]$recipe.repetitions
if ($Runs -eq 0) {
    $Runs = $recipeRuns
}
if ($Runs -ne $recipeRuns -and -not $DiagnosticAllowRecipeOverride) {
    throw "Release build evidence requires exactly $recipeRuns repetitions; requested $Runs"
}
$outputPath = [System.IO.Path]::GetFullPath($Output)
$targetRoot = Join-Path $repoRoot "target\request-header-codec-perf\build\$Variant"
$targetRootFull = [System.IO.Path]::GetFullPath($targetRoot).TrimEnd([System.IO.Path]::DirectorySeparatorChar)
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

function Get-ProcessTreeWorkingSet([int]$RootProcessId) {
    $processRows = @(Get-CimInstance Win32_Process | Select-Object ProcessId, ParentProcessId)
    $ids = New-Object 'System.Collections.Generic.HashSet[int]'
    [void]$ids.Add($RootProcessId)
    $changed = $true
    while ($changed) {
        $changed = $false
        foreach ($row in $processRows) {
            if ($ids.Contains([int]$row.ParentProcessId) -and $ids.Add([int]$row.ProcessId)) {
                $changed = $true
            }
        }
    }
    $sum = 0L
    foreach ($id in $ids) {
        $process = Get-Process -Id $id -ErrorAction SilentlyContinue
        if ($null -ne $process) {
            $sum += [long]$process.WorkingSet64
        }
    }
    return $sum
}

function Get-StringSha256([string]$Value) {
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Value)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        return ([System.BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
    } finally {
        $sha.Dispose()
    }
}

function Get-CanonicalTextSha256([string]$Path) {
    $text = [System.IO.File]::ReadAllText($Path).Replace("`r`n", "`n").Replace("`r", "`n")
    return Get-StringSha256 $text
}

function Get-Median([long[]]$Values) {
    $sorted = @($Values | Sort-Object)
    if ($sorted.Count -eq 0) {
        throw 'Cannot aggregate an empty measurement set'
    }
    $middle = [int][Math]::Floor($sorted.Count / 2)
    if (($sorted.Count % 2) -eq 1) {
        return [long]$sorted[$middle]
    }
    return [long](($sorted[$middle - 1] + $sorted[$middle]) / 2)
}

function Invoke-MeasuredCargo([string]$TargetDirectory, [bool]$Incremental, [bool]$AllowExisting) {
    if ((Test-Path -LiteralPath $TargetDirectory) -and -not $AllowExisting) {
        throw "Measurement target must be fresh: $TargetDirectory"
    }
    New-Item -ItemType Directory -Force -Path $TargetDirectory | Out-Null
    $stdout = Join-Path $TargetDirectory 'cargo.stdout.log'
    $stderr = Join-Path $TargetDirectory 'cargo.stderr.log'
    $oldTarget = $env:CARGO_TARGET_DIR
    $oldIncremental = $env:CARGO_INCREMENTAL
    try {
        $env:CARGO_TARGET_DIR = $TargetDirectory
        $env:CARGO_INCREMENTAL = if ($Incremental) { '1' } else { '0' }
        $startInfo = New-Object System.Diagnostics.ProcessStartInfo
        $startInfo.FileName = (Get-Command cargo).Source
        $startInfo.Arguments = 'build --locked --release -p rocketmq-protocol --bench request_header_codec'
        $startInfo.WorkingDirectory = $repoRoot
        $startInfo.UseShellExecute = $false
        $startInfo.CreateNoWindow = $true
        $startInfo.RedirectStandardOutput = $true
        $startInfo.RedirectStandardError = $true
        $process = New-Object System.Diagnostics.Process
        $process.StartInfo = $startInfo
        $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        [void]$process.Start()
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        $peak = 0L
        while (-not $process.HasExited) {
            $peak = [Math]::Max($peak, (Get-ProcessTreeWorkingSet $process.Id))
            Start-Sleep -Milliseconds $recipe.cleanBuild.processTreeMemorySampleIntervalMillis
            $process.Refresh()
        }
        $process.WaitForExit()
        $stdoutTask.Wait()
        $stderrTask.Wait()
        $stopwatch.Stop()
        [System.IO.File]::WriteAllText($stdout, $stdoutTask.Result, $utf8NoBom)
        [System.IO.File]::WriteAllText($stderr, $stderrTask.Result, $utf8NoBom)
        $exitCode = $process.ExitCode
        if ($exitCode -ne 0) {
            throw "Cargo build failed; inspect $stderr"
        }
        return [ordered]@{
            wallTimeNanos = [long]($stopwatch.Elapsed.TotalMilliseconds * 1000000)
            processTreePeakWorkingSetBytes = $peak
            targetDirectory = $TargetDirectory
            stdout = $stdout
            stderr = $stderr
            stdoutSha256 = (Get-FileHash -LiteralPath $stdout -Algorithm SHA256).Hash.ToLowerInvariant()
            stderrSha256 = (Get-FileHash -LiteralPath $stderr -Algorithm SHA256).Hash.ToLowerInvariant()
        }
    } finally {
        $env:CARGO_TARGET_DIR = $oldTarget
        $env:CARGO_INCREMENTAL = $oldIncremental
    }
}

function Remove-MeasurementTarget([string]$TargetDirectory) {
    if ($KeepTargets) {
        return
    }
    $resolved = [System.IO.Path]::GetFullPath($TargetDirectory)
    $requiredPrefix = $targetRootFull + [System.IO.Path]::DirectorySeparatorChar
    if (-not $resolved.StartsWith($requiredPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove measurement target outside $targetRootFull`: $resolved"
    }
    if (Test-Path -LiteralPath $resolved) {
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

$samples = @()
for ($run = 1; $run -le $Runs; $run++) {
    $runId = '{0:D2}-{1}' -f $run, ([Guid]::NewGuid().ToString('N'))
    $target = Join-Path $targetRoot $runId
    if ($Mode -eq 'clean') {
        $measurement = Invoke-MeasuredCargo $target $false $false
        $artifacts = @(Get-ChildItem -LiteralPath (Join-Path $target 'release\deps') -Filter 'request_header_codec-*.exe')
        if ($artifacts.Count -ne 1) {
            throw "Expected exactly one benchmark artifact in $target, found $($artifacts.Count)"
        }
        $artifactJson = & python (Join-Path $scriptRoot 'measure_artifact.py') $artifacts[0].FullName
        if ($LASTEXITCODE -ne 0) {
            throw 'Artifact measurement failed'
        }
        $measurement.artifact = $artifactJson | ConvertFrom-Json
        $samples += $measurement
        Remove-MeasurementTarget $target
        continue
    }

    [void](Invoke-MeasuredCargo $target $true $false)
    $source = Join-Path $repoRoot $recipe.incrementalBuild.edit.file
    $backup = Join-Path $target 'incremental-probe.backup'
    try {
        & python (Join-Path $scriptRoot 'apply-incremental-probe.py') apply --source $source --backup $backup
        if ($LASTEXITCODE -ne 0) {
            throw 'Failed to apply incremental probe'
        }
        $samples += Invoke-MeasuredCargo $target $true $true
    } finally {
        if (Test-Path -LiteralPath $backup) {
            & python (Join-Path $scriptRoot 'apply-incremental-probe.py') restore --source $source --backup $backup
        }
        Remove-MeasurementTarget $target
    }
}

$rustcIdentity = (& rustc -Vv) -join "`n"
$cargoIdentity = (& cargo -V)
$toolIdentity = [ordered]@{
    rustc = $rustcIdentity
    cargo = $cargoIdentity
}
$templateJson = $recipe.environmentTemplate | ConvertTo-Json -Depth 8 -Compress
$toolJson = $toolIdentity | ConvertTo-Json -Depth 8 -Compress
$helperHashes = [ordered]@{}
foreach ($helper in @('measure-build.ps1', 'measure_artifact.py', 'apply-incremental-probe.py')) {
    $helperHashes[$helper] = (Get-FileHash -LiteralPath (Join-Path $scriptRoot $helper) -Algorithm SHA256).Hash.ToLowerInvariant()
}
$powerPolicy = (& powercfg /getactivescheme 2>$null) -join "`n"
$document = [ordered]@{
    schemaVersion = 1
    sourceCommit = (& git -C $repoRoot rev-parse HEAD).Trim()
    buildRecipe = [ordered]@{
        id = $recipe.id
        sha256 = Get-CanonicalTextSha256 $recipePath
    }
    mode = $Mode
    variant = $Variant
    aggregate = 'median'
    releasable = ($Runs -eq $recipeRuns)
    repetitions = $Runs
    resolvedCommand = @($recipe.cleanBuild.command)
    normalizedEnvironmentTemplateSha256 = Get-StringSha256 $templateJson
    toolIdentitySha256 = Get-StringSha256 $toolJson
    measurementHelperSha256 = $helperHashes
    samples = $samples
    medians = [ordered]@{
        wallTimeNanos = Get-Median @($samples | ForEach-Object { [long]$_.wallTimeNanos })
        processTreePeakWorkingSetBytes = Get-Median @($samples | ForEach-Object { [long]$_.processTreePeakWorkingSetBytes })
        artifactBytes = if ($Mode -eq 'clean') { Get-Median @($samples | ForEach-Object { [long]$_.artifact.artifactBytes }) } else { $null }
        textBytes = if ($Mode -eq 'clean') { Get-Median @($samples | ForEach-Object { [long]$_.artifact.textBytes }) } else { $null }
    }
    rawEnvironment = [ordered]@{
        rustc = $rustcIdentity
        cargo = $cargoIdentity
        os = [System.Environment]::OSVersion.VersionString
        processorCount = [System.Environment]::ProcessorCount
        powerPolicy = $powerPolicy
    }
}
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $outputPath) | Out-Null
[System.IO.File]::WriteAllText($outputPath, (($document | ConvertTo-Json -Depth 12) + "`n"), $utf8NoBom)
Write-Output "Build evidence written to $outputPath"

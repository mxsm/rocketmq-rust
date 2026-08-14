# Copyright 2026 The RocketMQ Rust Authors
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
    [string]$JavaRepo,

    [Parameter(Mandatory = $true)]
    [string]$Output,

    [ValidateRange(10, 100)]
    [int]$RustProcessSamples = 10,

    [switch]$Resume
)

$ErrorActionPreference = 'Stop'
$scriptRoot = Split-Path -Parent $PSCommandPath
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $scriptRoot '..\..')).Path
$javaRoot = (Resolve-Path -LiteralPath $JavaRepo).Path
$outputRoot = [System.IO.Path]::GetFullPath($Output)
$profilePath = Join-Path $scriptRoot 'profile-v1.json'
$profile = Get-Content -Raw -LiteralPath $profilePath | ConvertFrom-Json
$corpusPath = Join-Path $repoRoot 'scripts\request-header-codec\perf-corpus-v1.json'
$fixtureManifest = Join-Path $repoRoot 'rocketmq-protocol\tests\fixtures\request_header_codec\manifest.json'
$fixtures = Split-Path -Parent $fixtureManifest
$harnessPom = Join-Path $repoRoot 'scripts\request-header-codec\java-harness\pom.xml'
$harnessJar = Join-Path $repoRoot 'scripts\request-header-codec\java-harness\target\request-header-codec-benchmarks.jar'
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

if ($RustProcessSamples -lt [int]$profile.rustProcessSamples) {
    throw "At least $($profile.rustProcessSamples) Rust process samples are required"
}
if ((Test-Path -LiteralPath $outputRoot) -and -not $Resume) {
    throw "Baseline output must be a new directory: $outputRoot"
}

function Assert-CleanWorktree([string]$Repository, [string]$Description) {
    $status = (& git -C $Repository status --short) -join "`n"
    if ($LASTEXITCODE -ne 0) {
        throw "$Description is not a Git worktree"
    }
    if ($status) {
        throw "$Description must be clean:`n$status"
    }
}

function Invoke-Logged([string]$Description, [string]$Log, [scriptblock]$Operation) {
    $parent = Split-Path -Parent $Log
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
    $previousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & $Operation 2>&1 | Tee-Object -FilePath $Log | Out-Host
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    if ($exitCode -ne 0) {
        throw "$Description failed with exit code $exitCode"
    }
}

function Get-ProcessOutput([string]$FileName, [string]$Arguments) {
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $FileName
    $startInfo.Arguments = $Arguments
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    if ($process.ExitCode -ne 0) {
        throw "$FileName $Arguments failed with exit code $($process.ExitCode)"
    }
    return (($stdout.Trim(), $stderr.Trim()) | Where-Object { $_ }) -join "`n"
}

function Write-Json([string]$Path, [object]$Value, [int]$Depth = 12) {
    $parent = Split-Path -Parent $Path
    if ($parent) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    [System.IO.File]::WriteAllText($Path, (($Value | ConvertTo-Json -Depth $Depth) + "`n"), $utf8NoBom)
}

Assert-CleanWorktree $repoRoot 'Rust baseline worktree'
Assert-CleanWorktree $javaRoot 'Java oracle worktree'

[xml]$javaPom = Get-Content -Raw -LiteralPath (Join-Path $javaRoot 'pom.xml')
[xml]$benchmarkPom = Get-Content -Raw -LiteralPath $harnessPom
$javaProjectVersion = $javaPom.SelectSingleNode("//*[local-name()='properties']/*[local-name()='revision']").InnerText
$harnessRocketMQVersion = $benchmarkPom.SelectSingleNode(
    "//*[local-name()='properties']/*[local-name()='rocketmq.version']"
).InnerText
if ($javaProjectVersion -ne $harnessRocketMQVersion) {
    throw "Java oracle version $javaProjectVersion does not match benchmark dependency $harnessRocketMQVersion"
}

$rustRevision = (& git -C $repoRoot rev-parse HEAD).Trim()
$javaRevision = (& git -C $javaRoot rev-parse HEAD).Trim()
$corpus = Get-Content -Raw -LiteralPath $corpusPath | ConvertFrom-Json
$caseIds = @($corpus.cases | ForEach-Object { [string]$_.id })
$uniqueCaseIds = @($caseIds | Sort-Object -Unique)
if ($caseIds.Count -ne [int]$profile.caseCount -or $uniqueCaseIds.Count -ne $caseIds.Count) {
    throw "Expected $($profile.caseCount) unique request-header cases; found $($caseIds.Count)/$($uniqueCaseIds.Count)"
}

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
[System.IO.File]::WriteAllLines((Join-Path $outputRoot 'cases.txt'), $caseIds, $utf8NoBom)
[System.IO.File]::WriteAllText(
    (Join-Path $outputRoot 'commands.txt'),
    (@(
        'cargo bench --locked -p rocketmq-protocol --bench request_header_codec -- --warm-up-time 5 --measurement-time 10 --sample-size 100',
        'cargo bench --locked -p rocketmq-protocol --bench remoting_command_hot_paths -- --warm-up-time 5 --measurement-time 10 --sample-size 100',
        'cargo bench --locked -p rocketmq-transport --features test-support --bench p0_overhead -- --warm-up-time 5 --measurement-time 10 --sample-size 100',
        'cargo bench --locked -p rocketmq-transport --features test-support --bench frame_write -- --warm-up-time 5 --measurement-time 10 --sample-size 100',
        'cargo bench --locked -p rocketmq-transport --features test-support --bench write_pipeline -- --warm-up-time 5 --measurement-time 10 --sample-size 100',
        'cargo bench --locked -p rocketmq-transport --features test-support --bench admission_pending_hooks -- --warm-up-time 5 --measurement-time 10 --sample-size 100',
        'mvn -f <java-oracle>/pom.xml -pl remoting -am install -Dmaven.test.skip=true -Dcheckstyle.skip=true -Drat.skip=true -Dspotbugs.skip=true -Dspotless.check.skip=true',
        'mvn -f scripts/request-header-codec/java-harness/pom.xml -DskipTests package',
        'java -jar request-header-codec-benchmarks.jar HeaderCodecBenchmark -f 5 -wi 10 -w 1s -i 15 -r 1s -prof gc -rf json'
    ) -join "`n") + "`n",
    $utf8NoBom
)

$processor = Get-CimInstance Win32_Processor | Select-Object -First 1
$computer = Get-CimInstance Win32_ComputerSystem
$currentProcess = [System.Diagnostics.Process]::GetCurrentProcess()
$powerSchemeOutput = (& powercfg /getactivescheme 2>$null) -join "`n"
$powerSchemeMatch = [regex]::Match($powerSchemeOutput, '[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}')
$powerSchemeGuid = if ($powerSchemeMatch.Success) { $powerSchemeMatch.Value.ToLowerInvariant() } else { 'unknown' }
$metadata = [ordered]@{
    schemaVersion = 1
    baselineId = [string]$profile.baselineId
    startedUtc = [DateTime]::UtcNow.ToString('o')
    rustRevision = $rustRevision
    javaRevision = $javaRevision
    javaProjectVersion = $javaProjectVersion
    rustProcessSamples = $RustProcessSamples
    os = [System.Environment]::OSVersion.VersionString
    processor = $processor.Name.Trim()
    physicalCores = [int]$processor.NumberOfCores
    logicalProcessors = [int]$processor.NumberOfLogicalProcessors
    memoryBytes = [long]$computer.TotalPhysicalMemory
    processAffinity = ('0x{0:x}' -f $currentProcess.ProcessorAffinity.ToInt64())
    allocator = 'system'
    rustc = (& rustc -Vv) -join "`n"
    cargo = (& cargo -V)
    java = Get-ProcessOutput (Get-Command java).Source '-version'
    maven = (& mvn -version) -join "`n"
    powerSchemeGuid = $powerSchemeGuid
    profile = $profile
}
$metadataPath = Join-Path $outputRoot 'metadata.json'
Write-Json $metadataPath $metadata 16

$javaOutput = Join-Path $outputRoot 'java'
$javaRaw = Join-Path $javaOutput 'jmh-result.json'
$javaNormalized = Join-Path $javaOutput 'java-normalized.json'
if (-not ($Resume -and (Test-Path -LiteralPath $javaNormalized))) {
    Invoke-Logged 'Java remoting oracle build' (Join-Path $javaOutput 'maven-remoting.log') {
        & mvn -f (Join-Path $javaRoot 'pom.xml') -pl remoting -am install `
            '-Dmaven.test.skip=true' '-Dcheckstyle.skip=true' '-Drat.skip=true' `
            '-Dspotbugs.skip=true' '-Dspotless.check.skip=true'
    }
    Invoke-Logged 'Java benchmark harness build' (Join-Path $javaOutput 'maven-harness.log') {
        & mvn -f $harnessPom '-DskipTests' package
    }
    $forkArguments = "-Dheader.fixtureDirectory=$fixtures -Xms2g -Xmx2g -XX:+AlwaysPreTouch"
    Invoke-Logged 'Java JMH baseline' (Join-Path $javaOutput 'jmh-console.log') {
        & java -jar $harnessJar HeaderCodecBenchmark `
            -f ([int]$profile.javaJmh.forks) `
            -wi ([int]$profile.javaJmh.warmupIterations) -w "$($profile.javaJmh.warmupSeconds)s" `
            -i ([int]$profile.javaJmh.measurementIterations) -r "$($profile.javaJmh.measurementSeconds)s" `
            -prof ([string]$profile.javaJmh.profiler) -rf json -rff $javaRaw `
            -jvmArgsAppend $forkArguments
    }
    & python (Join-Path $repoRoot 'scripts\request-header-codec\normalize_jmh.py') `
        --raw $javaRaw --corpus $corpusPath --fixture-manifest $fixtureManifest `
        --runner $metadataPath --output $javaNormalized --commit $javaRevision --profile gate `
        --forks ([int]$profile.javaJmh.forks) `
        --warmup-iterations ([int]$profile.javaJmh.warmupIterations) `
        --measurement-iterations ([int]$profile.javaJmh.measurementIterations)
    if ($LASTEXITCODE -ne 0) {
        throw 'Java JMH normalization failed'
    }
}

$targets = @(
    [pscustomobject]@{ Name = 'request_header_codec'; Package = 'rocketmq-protocol'; Features = $null; Kind = 'header' },
    [pscustomobject]@{ Name = 'remoting_command_hot_paths'; Package = 'rocketmq-protocol'; Features = $null; Kind = 'remoting' },
    [pscustomobject]@{ Name = 'p0_overhead'; Package = 'rocketmq-transport'; Features = 'test-support'; Kind = 'criterion' },
    [pscustomobject]@{ Name = 'frame_write'; Package = 'rocketmq-transport'; Features = 'test-support'; Kind = 'criterion' },
    [pscustomobject]@{ Name = 'write_pipeline'; Package = 'rocketmq-transport'; Features = 'test-support'; Kind = 'criterion' },
    [pscustomobject]@{ Name = 'admission_pending_hooks'; Package = 'rocketmq-transport'; Features = 'test-support'; Kind = 'criterion' }
)

$oldCriterionHome = $env:CRITERION_HOME
$oldHeaderCriterion = $env:ROCKETMQ_HEADER_CODEC_CRITERION_DIR
$oldHeaderAlloc = $env:ROCKETMQ_HEADER_CODEC_ALLOC_OUTPUT
$oldHeaderAllocSamples = $env:ROCKETMQ_HEADER_CODEC_ALLOC_SAMPLES
$oldRemotingCriterion = $env:ROCKETMQ_REMOTING_COMMAND_CRITERION_DIR
$oldRemotingEvidence = $env:ROCKETMQ_REMOTING_COMMAND_EVIDENCE
$oldBaselineWarmup = $env:ROCKETMQ_REMOTING_COMMAND_BASELINE_WARMUP_SECONDS
$oldBaselineMeasurement = $env:ROCKETMQ_REMOTING_COMMAND_BASELINE_MEASUREMENT_SECONDS
$oldBaselineSamples = $env:ROCKETMQ_REMOTING_COMMAND_BASELINE_SAMPLE_SIZE
try {
    $env:ROCKETMQ_REMOTING_COMMAND_BASELINE_WARMUP_SECONDS = [string]$profile.criterion.warmupSeconds
    $env:ROCKETMQ_REMOTING_COMMAND_BASELINE_MEASUREMENT_SECONDS = [string]$profile.criterion.measurementSeconds
    $env:ROCKETMQ_REMOTING_COMMAND_BASELINE_SAMPLE_SIZE = [string]$profile.criterion.sampleSize
    for ($sample = 1; $sample -le $RustProcessSamples; $sample++) {
        $sampleName = 'sample-{0:d2}' -f $sample
        $offset = ($sample - 1) % $targets.Count
        $orderedTargets = @($targets[$offset..($targets.Count - 1)] + $targets[0..($offset - 1)])
        if ($offset -eq 0) {
            $orderedTargets = $targets
        }
        foreach ($target in $orderedTargets) {
            $destination = Join-Path $outputRoot "rust\$sampleName\$($target.Name)"
            $criterion = Join-Path $destination 'criterion'
            $completion = Join-Path $destination 'complete.marker'
            if ($Resume -and (Test-Path -LiteralPath $completion)) {
                continue
            }
            New-Item -ItemType Directory -Force -Path $destination | Out-Null
            $env:CRITERION_HOME = $criterion
            $env:ROCKETMQ_HEADER_CODEC_CRITERION_DIR = $null
            $env:ROCKETMQ_HEADER_CODEC_ALLOC_OUTPUT = $null
            $env:ROCKETMQ_HEADER_CODEC_ALLOC_SAMPLES = $null
            $env:ROCKETMQ_REMOTING_COMMAND_CRITERION_DIR = $null
            $env:ROCKETMQ_REMOTING_COMMAND_EVIDENCE = $null
            if ($target.Kind -eq 'header') {
                $env:ROCKETMQ_HEADER_CODEC_CRITERION_DIR = $criterion
                $env:ROCKETMQ_HEADER_CODEC_ALLOC_OUTPUT = Join-Path $destination 'allocations.json'
                $env:ROCKETMQ_HEADER_CODEC_ALLOC_SAMPLES = '32'
            } elseif ($target.Kind -eq 'remoting') {
                $env:ROCKETMQ_REMOTING_COMMAND_CRITERION_DIR = $criterion
                $env:ROCKETMQ_REMOTING_COMMAND_EVIDENCE = Join-Path $destination 'evidence.json'
            }
            $cargoArguments = @('bench', '--locked', '-p', $target.Package)
            if ($target.Features) {
                $cargoArguments += @('--features', $target.Features)
            }
            $cargoArguments += @(
                '--bench', $target.Name, '--',
                '--warm-up-time', [string]$profile.criterion.warmupSeconds,
                '--measurement-time', [string]$profile.criterion.measurementSeconds,
                '--sample-size', [string]$profile.criterion.sampleSize
            )
            Invoke-Logged "$sampleName $($target.Name)" (Join-Path $destination 'console.log') {
                & cargo @cargoArguments
            }
            [System.IO.File]::WriteAllText($completion, "complete`n", $utf8NoBom)
        }
    }
} finally {
    $env:CRITERION_HOME = $oldCriterionHome
    $env:ROCKETMQ_HEADER_CODEC_CRITERION_DIR = $oldHeaderCriterion
    $env:ROCKETMQ_HEADER_CODEC_ALLOC_OUTPUT = $oldHeaderAlloc
    $env:ROCKETMQ_HEADER_CODEC_ALLOC_SAMPLES = $oldHeaderAllocSamples
    $env:ROCKETMQ_REMOTING_COMMAND_CRITERION_DIR = $oldRemotingCriterion
    $env:ROCKETMQ_REMOTING_COMMAND_EVIDENCE = $oldRemotingEvidence
    $env:ROCKETMQ_REMOTING_COMMAND_BASELINE_WARMUP_SECONDS = $oldBaselineWarmup
    $env:ROCKETMQ_REMOTING_COMMAND_BASELINE_MEASUREMENT_SECONDS = $oldBaselineMeasurement
    $env:ROCKETMQ_REMOTING_COMMAND_BASELINE_SAMPLE_SIZE = $oldBaselineSamples
}

$summaryOutput = Join-Path $outputRoot 'summary'
& python (Join-Path $scriptRoot 'summarize.py') `
    --artifact-root $outputRoot --profile $profilePath --java-normalized $javaNormalized `
    --metadata $metadataPath --output $summaryOutput
if ($LASTEXITCODE -ne 0) {
    throw 'Baseline summary generation failed'
}

$metadata.completedUtc = [DateTime]::UtcNow.ToString('o')
Write-Json $metadataPath $metadata 16
Write-Output "Baseline complete: $outputRoot"

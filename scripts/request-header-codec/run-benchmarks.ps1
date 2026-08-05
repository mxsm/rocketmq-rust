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
    [string]$JavaRepo,

    [Parameter(Mandatory = $true)]
    [string]$Corpus,

    [Parameter(Mandatory = $true)]
    [string]$Gates,

    [Parameter(Mandatory = $true)]
    [string]$Output,

    [ValidateSet('PostP0', 'Phase1', 'Release')]
    [string]$Mode = 'Release',

    [string]$V2Worktree,

    [string]$V2Manifest,

    [switch]$Quick,

    [switch]$PublishBaseline,

    [switch]$Resume
)

$ErrorActionPreference = 'Stop'
$expectedJavaCommit = '2daf0e2ca91a1592d18235d43e5d709d1c35d15f'
$scriptRoot = Split-Path -Parent $PSCommandPath
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $scriptRoot '..\..')).Path
$javaRoot = (Resolve-Path -LiteralPath $JavaRepo).Path
$corpusPath = (Resolve-Path -LiteralPath $Corpus).Path
$gatesPath = (Resolve-Path -LiteralPath $Gates).Path
$outputRoot = [System.IO.Path]::GetFullPath($Output)
$fixtures = Join-Path $repoRoot 'rocketmq-protocol\tests\fixtures\request_header_codec'
$fixtureManifest = Join-Path $fixtures 'manifest.json'
$harnessPom = Join-Path $scriptRoot 'java-harness\pom.xml'
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

if ((Test-Path -LiteralPath $outputRoot) -and -not $Resume) {
    throw "Performance output must be a new directory: $outputRoot"
}
if ($Quick -and $PublishBaseline) {
    throw 'Diagnostic quick runs cannot publish a checked-in baseline'
}
if ($Mode -eq 'Release' -and (-not $V2Worktree -or -not $V2Manifest)) {
    throw 'Release mode requires -V2Worktree and -V2Manifest for same-run replay'
}

function Write-Json([string]$Path, [object]$Value, [int]$Depth = 16) {
    $parent = Split-Path -Parent $Path
    if ($parent) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    [System.IO.File]::WriteAllText($Path, (($Value | ConvertTo-Json -Depth $Depth) + "`n"), $utf8NoBom)
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

function Invoke-Checked([string]$Description, [scriptblock]$Operation) {
    & $Operation | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE"
    }
}

function Assert-CleanCommit([string]$Repository, [string]$Expected, [string]$Description) {
    $actual = (& git -C $Repository rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $actual -ne $Expected) {
        throw "$Description HEAD must be $Expected; found $actual"
    }
    if ((& git -C $Repository status --short) -join "`n") {
        throw "$Description worktree must be clean"
    }
}

function New-RunnerFingerprint([string]$Path) {
    $processor = Get-CimInstance Win32_Processor | Select-Object -First 1
    $powerPolicy = (& powercfg /getactivescheme 2>$null) -join "`n"
    $identity = [ordered]@{
        schemaVersion = 1
        platform = 'windows-x86_64-msvc'
        os = [System.Environment]::OSVersion.VersionString
        processor = $processor.Name.Trim()
        physicalCores = [int]$processor.NumberOfCores
        logicalProcessors = [int]$processor.NumberOfLogicalProcessors
        memoryBytes = [long](Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
        rustc = (& rustc -Vv) -join "`n"
        cargo = (& cargo -V)
        java = Get-ProcessOutput (Get-Command java).Source '-version'
        maven = (& mvn -version) -join "`n"
        powerPolicy = $powerPolicy
    }
    $identityJson = $identity | ConvertTo-Json -Depth 8 -Compress
    $slug = ($processor.Name.ToLowerInvariant() -replace '[^a-z0-9]+', '-').Trim('-')
    $identity['id'] = "windows-$slug-$((Get-StringSha256 $identityJson).Substring(0, 12))"
    Write-Json $Path $identity
    return $identity
}

function Invoke-JavaBenchmark([string]$RunnerPath, [string]$Destination) {
    Invoke-Checked 'Pinned Java remoting build' {
        & mvn -f (Join-Path $javaRoot 'pom.xml') -pl remoting -am install '-Dmaven.test.skip=true' `
            '-Dcheckstyle.skip=true' '-Drat.skip=true' '-Dspotbugs.skip=true' '-Dspotless.check.skip=true'
    }
    Invoke-Checked 'Java benchmark harness package' {
        & mvn -f $harnessPom package '-DskipTests'
    }
    $jar = Join-Path $scriptRoot 'java-harness\target\request-header-codec-benchmarks.jar'
    $raw = Join-Path $Destination 'jmh-result.json'
    $console = Join-Path $Destination 'jmh-console.txt'
    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    if ($Quick) {
        $forks = 1
        $warmupIterations = 1
        $measurementIterations = 1
        $warmupTime = '200ms'
        $measurementTime = '200ms'
        $jvmMemory = '-Xms512m -Xmx512m'
        $profile = 'diagnostic'
    } else {
        $forks = 5
        $warmupIterations = 10
        $measurementIterations = 15
        $warmupTime = '1s'
        $measurementTime = '1s'
        $jvmMemory = '-Xms2g -Xmx2g -XX:+AlwaysPreTouch'
        $profile = 'gate'
    }
    $forkArguments = "-Dheader.fixtureDirectory=$fixtures $jvmMemory"
    $arguments = @(
        '-jar', $jar, 'HeaderCodecBenchmark', '-f', $forks, '-wi', $warmupIterations,
        '-i', $measurementIterations, '-w', $warmupTime, '-r', $measurementTime,
        '-rf', 'json', '-rff', $raw, '-prof', 'gc', '-jvmArgsAppend', $forkArguments
    )
    $previousErrorPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & java @arguments 2>&1 | Tee-Object -FilePath $console | Out-Host
        $jmhExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorPreference
    }
    if ($jmhExitCode -ne 0) {
        throw "Java JMH failed with exit code $jmhExitCode"
    }
    $normalized = Join-Path $Destination 'java.json'
    $normalizeArguments = @(
        (Join-Path $scriptRoot 'normalize_jmh.py'), '--raw', $raw, '--corpus', $corpusPath,
        '--fixture-manifest', $fixtureManifest, '--runner', $RunnerPath, '--output', $normalized,
        '--commit', $expectedJavaCommit, '--profile', $profile, '--forks', $forks,
        '--warmup-iterations', $warmupIterations, '--measurement-iterations', $measurementIterations
    )
    & python @normalizeArguments | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw 'Java JMH normalization failed'
    }
    return $normalized
}

function Invoke-RustBenchmark(
    [string]$Repository,
    [string]$RunnerPath,
    [string]$Destination,
    [string]$Role,
    [string]$BaselineManifestPath
) {
    $raw = Join-Path $Destination 'criterion-raw'
    $allocations = Join-Path $Destination 'allocations.json'
    $normalized = Join-Path $Destination "$Role.json"
    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    $oldWarmup = $env:ROCKETMQ_HEADER_CODEC_WARMUP_SECONDS
    $oldMeasurement = $env:ROCKETMQ_HEADER_CODEC_MEASUREMENT_SECONDS
    $oldSamples = $env:ROCKETMQ_HEADER_CODEC_SAMPLES
    $oldCriterion = $env:ROCKETMQ_HEADER_CODEC_CRITERION_DIR
    $oldAllocationSamples = $env:ROCKETMQ_HEADER_CODEC_ALLOC_SAMPLES
    $oldAllocationOutput = $env:ROCKETMQ_HEADER_CODEC_ALLOC_OUTPUT
    try {
        if ($Quick) {
            $env:ROCKETMQ_HEADER_CODEC_WARMUP_SECONDS = '1'
            $env:ROCKETMQ_HEADER_CODEC_MEASUREMENT_SECONDS = '1'
            $env:ROCKETMQ_HEADER_CODEC_SAMPLES = '10'
            $env:ROCKETMQ_HEADER_CODEC_ALLOC_SAMPLES = '2'
            $profile = 'diagnostic'
        } else {
            $env:ROCKETMQ_HEADER_CODEC_WARMUP_SECONDS = '5'
            $env:ROCKETMQ_HEADER_CODEC_MEASUREMENT_SECONDS = '10'
            $env:ROCKETMQ_HEADER_CODEC_SAMPLES = '100'
            $env:ROCKETMQ_HEADER_CODEC_ALLOC_SAMPLES = '32'
            $profile = 'gate'
        }
        $env:ROCKETMQ_HEADER_CODEC_CRITERION_DIR = $raw
        $env:ROCKETMQ_HEADER_CODEC_ALLOC_OUTPUT = $allocations
        Push-Location $Repository
        try {
            $previousErrorPreference = $ErrorActionPreference
            try {
                $ErrorActionPreference = 'Continue'
                & cargo bench --locked -p rocketmq-protocol --bench request_header_codec 2>&1 | Out-Host
                $criterionExitCode = $LASTEXITCODE
            } finally {
                $ErrorActionPreference = $previousErrorPreference
            }
            if ($criterionExitCode -ne 0) {
                throw "Rust Criterion failed with exit code $criterionExitCode"
            }
        } finally {
            Pop-Location
        }
    } finally {
        $env:ROCKETMQ_HEADER_CODEC_WARMUP_SECONDS = $oldWarmup
        $env:ROCKETMQ_HEADER_CODEC_MEASUREMENT_SECONDS = $oldMeasurement
        $env:ROCKETMQ_HEADER_CODEC_SAMPLES = $oldSamples
        $env:ROCKETMQ_HEADER_CODEC_CRITERION_DIR = $oldCriterion
        $env:ROCKETMQ_HEADER_CODEC_ALLOC_SAMPLES = $oldAllocationSamples
        $env:ROCKETMQ_HEADER_CODEC_ALLOC_OUTPUT = $oldAllocationOutput
    }
    $commit = (& git -C $Repository rev-parse HEAD).Trim()
    $normalizeArguments = @(
        (Join-Path $scriptRoot 'normalize_criterion.py'), '--raw-dir', $raw, '--corpus', $corpusPath,
        '--fixture-manifest', $fixtureManifest, '--runner', $RunnerPath, '--output', $normalized,
        '--role', $Role, '--commit', $commit, '--profile', $profile, '--allocations', $allocations
    )
    if ($BaselineManifestPath) {
        $normalizeArguments += @('--baseline-manifest', $BaselineManifestPath)
    }
    & python @normalizeArguments | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw 'Rust Criterion normalization failed'
    }
    return $normalized
}

function Invoke-BuildEvidence([string]$Repository, [string]$Variant, [string]$Destination) {
    $measureScript = Join-Path $Repository 'scripts\request-header-codec\measure-build.ps1'
    if (-not (Test-Path -LiteralPath $measureScript)) {
        throw "Build measurement helper is absent from $Repository"
    }
    $clean = Join-Path $Destination "$Variant-clean.json"
    $incremental = Join-Path $Destination "$Variant-incremental.json"
    if ($Resume -and (Test-Path -LiteralPath $clean) -and (Test-Path -LiteralPath $incremental)) {
        return @($clean, $incremental)
    }
    $common = @{
        Variant = $Variant
    }
    if ($Quick) {
        $common['Runs'] = 1
        $common['DiagnosticAllowRecipeOverride'] = $true
    }
    & $measureScript -Mode clean -Output $clean @common | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "$Variant clean-build measurement failed"
    }
    & $measureScript -Mode incremental -Output $incremental @common | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "$Variant incremental-build measurement failed"
    }
    return @($clean, $incremental)
}

function Add-FileIdentity([System.Collections.Specialized.OrderedDictionary]$Files, [string]$Name, [string]$Path) {
    $resolved = (Resolve-Path -LiteralPath $Path).Path
    $relative = $resolved.Substring($outputRoot.Length).TrimStart('\', '/') -replace '\\', '/'
    $Files[$Name] = [ordered]@{
        path = $relative
        sha256 = (Get-FileHash -LiteralPath $resolved -Algorithm SHA256).Hash.ToLowerInvariant()
    }
}

Assert-CleanCommit $javaRoot $expectedJavaCommit 'Java oracle'
$gatesDocument = Get-Content -LiteralPath $gatesPath -Raw | ConvertFrom-Json
$recipePath = Join-Path $repoRoot $gatesDocument.buildRecipe.path
$recipeHash = (Get-FileHash -LiteralPath $recipePath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($recipeHash -ne $gatesDocument.buildRecipe.sha256) {
    throw 'perf-gates.json references a stale build recipe digest'
}

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
$runnerPath = Join-Path $outputRoot 'runner.json'
$runner = if ($Resume -and (Test-Path -LiteralPath $runnerPath)) {
    Get-Content -LiteralPath $runnerPath -Raw | ConvertFrom-Json
} else {
    New-RunnerFingerprint $runnerPath
}
$inputDirectory = Join-Path $outputRoot 'inputs'
New-Item -ItemType Directory -Force -Path $inputDirectory | Out-Null
$corpusCopy = Join-Path $inputDirectory 'perf-corpus-v1.json'
Copy-Item -LiteralPath $corpusPath -Destination $corpusCopy -Force

$javaNormalized = Join-Path $outputRoot 'java\java.json'
if (-not ($Resume -and (Test-Path -LiteralPath $javaNormalized))) {
    $javaNormalized = Invoke-JavaBenchmark $runnerPath (Join-Path $outputRoot 'java')
}
$files = [ordered]@{}
Add-FileIdentity $files 'corpus' $corpusCopy
Add-FileIdentity $files 'runner' $runnerPath
Add-FileIdentity $files 'java' $javaNormalized

if ($Mode -eq 'Release') {
    $v2Root = (Resolve-Path -LiteralPath $V2Worktree).Path
    $v2ManifestPath = (Resolve-Path -LiteralPath $V2Manifest).Path
    Assert-CleanCommit $v2Root ((Get-Content -LiteralPath $v2ManifestPath -Raw | ConvertFrom-Json).commit) 'V2 replay'
    $v3Normalized = Join-Path $outputRoot 'rust\v3\release-candidate.json'
    if (-not ($Resume -and (Test-Path -LiteralPath $v3Normalized))) {
        $v3Normalized = Invoke-RustBenchmark $repoRoot $runnerPath (Join-Path $outputRoot 'rust\v3') 'release-candidate' $null
    }
    $v2Normalized = Join-Path $outputRoot 'rust\v2\v2-replay.json'
    if (-not ($Resume -and (Test-Path -LiteralPath $v2Normalized))) {
        $v2Normalized = Invoke-RustBenchmark $v2Root $runnerPath (Join-Path $outputRoot 'rust\v2') 'v2-replay' $v2ManifestPath
    }
    $v3Build = Invoke-BuildEvidence $repoRoot 'v3' (Join-Path $outputRoot 'build')
    $v2Build = Invoke-BuildEvidence $v2Root 'v2-replay' (Join-Path $outputRoot 'build')
    $baselineCopy = Join-Path $outputRoot 'baseline\v2-phase1.json'
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $baselineCopy) | Out-Null
    Copy-Item -LiteralPath $v2ManifestPath -Destination $baselineCopy
    Add-FileIdentity $files 'v3' $v3Normalized
    Add-FileIdentity $files 'v2Replay' $v2Normalized
    Add-FileIdentity $files 'v2Manifest' $baselineCopy
    Add-FileIdentity $files 'v3CleanBuild' $v3Build[0]
    Add-FileIdentity $files 'v3IncrementalBuild' $v3Build[1]
    Add-FileIdentity $files 'v2CleanBuild' $v2Build[0]
    Add-FileIdentity $files 'v2IncrementalBuild' $v2Build[1]
    $manifestMode = 'release'
} else {
    $role = if ($Mode -eq 'PostP0') { 'post-p0' } else { 'phase1-hardened' }
    $rustNormalized = Join-Path $outputRoot "rust\$role.json"
    if (-not ($Resume -and (Test-Path -LiteralPath $rustNormalized))) {
        $rustNormalized = Invoke-RustBenchmark $repoRoot $runnerPath (Join-Path $outputRoot 'rust') $role $null
    }
    $build = Invoke-BuildEvidence $repoRoot $role (Join-Path $outputRoot 'build')
    Add-FileIdentity $files 'v2' $rustNormalized
    Add-FileIdentity $files 'v2CleanBuild' $build[0]
    Add-FileIdentity $files 'v2IncrementalBuild' $build[1]
    $manifestMode = $role

    if ($PublishBaseline) {
        $baselineDirectory = Join-Path $scriptRoot "perf-baselines\$($runner.id)"
        New-Item -ItemType Directory -Force -Path $baselineDirectory | Out-Null
        Copy-Item -LiteralPath $runnerPath -Destination (Join-Path $baselineDirectory 'runner.json') -Force
        Copy-Item -LiteralPath $javaNormalized -Destination (Join-Path $baselineDirectory 'java.json') -Force
        $baseline = Get-Content -LiteralPath $rustNormalized -Raw | ConvertFrom-Json
        $baseline | Add-Member -NotePropertyName buildRecipe -NotePropertyValue ([ordered]@{
            id = $gatesDocument.buildRecipe.id
            sha256 = $gatesDocument.buildRecipe.sha256
        })
        $baseline | Add-Member -NotePropertyName cleanBuild -NotePropertyValue (Get-Content -LiteralPath $build[0] -Raw | ConvertFrom-Json)
        $baseline | Add-Member -NotePropertyName incrementalBuild -NotePropertyValue (Get-Content -LiteralPath $build[1] -Raw | ConvertFrom-Json)
        $baselineName = if ($Mode -eq 'PostP0') { 'v2-post-p0.json' } else { 'v2-phase1.json' }
        Write-Json (Join-Path $baselineDirectory $baselineName) $baseline 32
    }
}

$manifest = [ordered]@{
    schemaVersion = 1
    mode = $manifestMode
    quickDiagnostic = [bool]$Quick
    gatesSha256 = (Get-FileHash -LiteralPath $gatesPath -Algorithm SHA256).Hash.ToLowerInvariant()
    buildRecipe = [ordered]@{
        id = $gatesDocument.buildRecipe.id
        sha256 = $gatesDocument.buildRecipe.sha256
    }
    runnerId = $runner.id
    files = $files
}
Write-Json (Join-Path $outputRoot 'manifest.json') $manifest 20
Write-Output "Request-header performance evidence written to $outputRoot"

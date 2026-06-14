[CmdletBinding()]
param(
    [string]$OutputDir = "target\e2e",
    [string]$BrokerDataRoot = "",
    [int]$MaxDataDriveUsedPercent = 85,
    [string]$JavaRocketMQRoot = "D:\Github\Java\rocketmq",
    [string]$JavaRocketmqHome = "D:\Github\Java\rocketmq\distribution\target\rocketmq-5.5.0\rocketmq-5.5.0",
    [string]$JavaHome = $env:JAVA_HOME,
    [string]$RocketMQHome = "",
    [string]$NamesrvHost = "127.0.0.1",
    [int]$NamesrvPort = 19876,
    [int]$BrokerPort = 10911,
    [string]$Topic = "RustFuncTestTopic",
    [string]$ClusterName = "RustFuncCluster",
    [string]$BrokerName = "rustFuncBroker",
    [int]$StartupTimeoutSeconds = 300,
    [int]$CommandTimeoutSeconds = 1800,
    [int]$BrokerSmokeTimeoutSeconds = 600,
    [int]$PerBrokerSmokeTimeoutSeconds = 180,
    [string[]]$BrokerSmokeTests = @(),
    [switch]$SkipBaseGates,
    [switch]$SkipRustClientJavaBroker,
    [switch]$SkipRustClientRustBroker,
    [switch]$SkipJavaClientRustBroker,
    [switch]$SkipMixedMatrix,
    [switch]$RunProductionReadiness,
    [switch]$UseExistingJavaClasses,
    [bool]$EnableCreateTopicSmoke = $true,
    [switch]$EnableAclSmoke,
    [string]$AclAccessKey = $(if ([string]::IsNullOrWhiteSpace($env:ROCKETMQ_ACL_ACCESS_KEY)) { "rocketmqRustSmokeAk" } else { $env:ROCKETMQ_ACL_ACCESS_KEY }),
    [string]$AclSecretKey = $(if ([string]::IsNullOrWhiteSpace($env:ROCKETMQ_ACL_SECRET_KEY)) { "rocketmqRustSmokeSk123456" } else { $env:ROCKETMQ_ACL_SECRET_KEY }),
    [switch]$EnableTraceSmoke,
    [switch]$EnableRecallSmoke,
    [switch]$EnableTlsSmoke,
    [switch]$IncludeAdvancedBrokerSmokes,
    [switch]$IncludeSlowBrokerSmokes,
    [switch]$RequireExternalGates,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

if ($PSBoundParameters.ContainsKey("BrokerSmokeTimeoutSeconds") -and
    -not $PSBoundParameters.ContainsKey("PerBrokerSmokeTimeoutSeconds")) {
    $PerBrokerSmokeTimeoutSeconds = $BrokerSmokeTimeoutSeconds
}

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runRoot = Join-Path $workspaceRoot (Join-Path $OutputDir "$runStamp-$PID")
$logRoot = Join-Path $runRoot "logs"
$defaultDataRoot = Join-Path $runRoot "data"
$dataRoot = $defaultDataRoot
$configRoot = Join-Path $runRoot "config"
$summaryFile = Join-Path $runRoot "summary.txt"
$javaRuntimeHome = Join-Path $runRoot "java-rocketmq-home"
$javaTlsConfigPath = Join-Path $configRoot "java-tls.properties"
$rustNamesrvConfigPath = Join-Path $configRoot "rust-namesrv.toml"
$rustAclFilePath = Join-Path $configRoot "plain_acl.yml"
$processes = @()
$externalGates = [System.Collections.Generic.List[string]]::new()
$script:ResolvedJavaHome = ""
$namesrvAddr = "$NamesrvHost`:$NamesrvPort"
$brokerAddr = "$NamesrvHost`:$BrokerPort"
if ([string]::IsNullOrWhiteSpace($RocketMQHome)) {
    $RocketMQHome = Join-Path $runRoot "rocketmq-home"
}

function Format-CommandArgument {
    param([Parameter(Mandatory = $true)][string]$Value)
    return '"' + $Value.Replace('"', '\"') + '"'
}

function New-Directory {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function ConvertTo-FullPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }

    return [System.IO.Path]::GetFullPath((Join-Path $workspaceRoot $Path))
}

function Get-PathDriveUsedPercent {
    param([Parameter(Mandatory = $true)][string]$Path)

    try {
        $fullPath = ConvertTo-FullPath -Path $Path
        $driveRoot = [System.IO.Path]::GetPathRoot($fullPath)
        if ([string]::IsNullOrWhiteSpace($driveRoot)) {
            return $null
        }

        $driveInfo = [System.IO.DriveInfo]::new($driveRoot)
        if (-not $driveInfo.IsReady -or $driveInfo.TotalSize -le 0) {
            return $null
        }

        return [math]::Round((($driveInfo.TotalSize - $driveInfo.AvailableFreeSpace) * 100.0) / $driveInfo.TotalSize, 2)
    }
    catch {
        return $null
    }
}

function Resolve-TestDataRoot {
    if (-not [string]::IsNullOrWhiteSpace($BrokerDataRoot)) {
        return ConvertTo-FullPath -Path $BrokerDataRoot
    }

    $defaultUsedPercent = Get-PathDriveUsedPercent -Path $defaultDataRoot
    if ($null -eq $defaultUsedPercent -or $defaultUsedPercent -le $MaxDataDriveUsedPercent) {
        return $defaultDataRoot
    }

    $candidateRoots = @()
    if (-not [string]::IsNullOrWhiteSpace($env:ROCKETMQ_E2E_DATA_ROOT)) {
        $candidateRoots += $env:ROCKETMQ_E2E_DATA_ROOT
    }
    if (-not [string]::IsNullOrWhiteSpace($env:TEMP)) {
        $candidateRoots += (Join-Path $env:TEMP "rocketmq-rust-e2e-data")
    }

    $driveCandidates = [System.IO.DriveInfo]::GetDrives() |
        Where-Object { $_.DriveType -eq [System.IO.DriveType]::Fixed -and $_.IsReady -and $_.TotalSize -gt 0 } |
        Sort-Object AvailableFreeSpace -Descending
    foreach ($drive in $driveCandidates) {
        $candidateRoots += (Join-Path $drive.RootDirectory.FullName "rocketmq-rust-e2e-data")
    }

    foreach ($candidate in $candidateRoots) {
        if ([string]::IsNullOrWhiteSpace($candidate)) {
            continue
        }

        $candidateUsedPercent = Get-PathDriveUsedPercent -Path $candidate
        if ($null -ne $candidateUsedPercent -and $candidateUsedPercent -le $MaxDataDriveUsedPercent) {
            return Join-Path (ConvertTo-FullPath -Path $candidate) "$runStamp-$PID"
        }
    }

    return $defaultDataRoot
}

function Normalize-BrokerSmokeTests {
    $normalized = @()
    foreach ($testName in $BrokerSmokeTests) {
        if ([string]::IsNullOrWhiteSpace($testName)) {
            continue
        }

        foreach ($splitTestName in ($testName -split ",")) {
            $trimmedTestName = $splitTestName.Trim()
            if (-not [string]::IsNullOrWhiteSpace($trimmedTestName)) {
                $normalized += $trimmedTestName
            }
        }
    }
    return $normalized
}

function Write-AsciiFile {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Content
    )
    [System.IO.File]::WriteAllText($Path, $Content, [System.Text.Encoding]::ASCII)
}

function Add-ExternalGate {
    param(
        [System.Collections.Generic.List[string]]$Gates,
        [Parameter(Mandatory = $true)][string]$Gate
    )
    if (-not $Gates.Contains($Gate)) {
        $Gates.Add($Gate) | Out-Null
    }
}

function Test-EnvNonEmpty {
    param([Parameter(Mandatory = $true)][string]$Name)
    return -not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($Name))
}

function Test-AclSmokeEnabled {
    return $EnableAclSmoke -or
        ((Test-EnvNonEmpty "ROCKETMQ_ACL_ACCESS_KEY") -and (Test-EnvNonEmpty "ROCKETMQ_ACL_SECRET_KEY"))
}

function ConvertTo-YamlSingleQuoted {
    param([Parameter(Mandatory = $true)][string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function ConvertTo-TomlBasicString {
    param([Parameter(Mandatory = $true)][string]$Value)
    return '"' + $Value.Replace('\', '\\').Replace('"', '\"') + '"'
}

function Write-PlainAclFile {
    param([Parameter(Mandatory = $true)][string]$Path)

    $content = @"
globalWhiteRemoteAddresses: []
accounts:
  - accessKey: $(ConvertTo-YamlSingleQuoted $AclAccessKey)
    secretKey: $(ConvertTo-YamlSingleQuoted $AclSecretKey)
    admin: true
    defaultTopicPerm: PUB|SUB
    defaultGroupPerm: PUB|SUB
"@
    Write-AsciiFile -Path $Path -Content $content
}

function Write-ToolsAclFile {
    param([Parameter(Mandatory = $true)][string]$Path)

    $content = @"
accessKey: $(ConvertTo-YamlSingleQuoted $AclAccessKey)
secretKey: $(ConvertTo-YamlSingleQuoted $AclSecretKey)
"@
    Write-AsciiFile -Path $Path -Content $content
}

function Write-JavaTlsConfig {
    param([Parameter(Mandatory = $true)][string]$Path)

    $content = @"
tls.test.mode.enable=true
tls.server.need.client.auth=none
tls.server.authClient=false
tls.client.authServer=false
"@
    Write-AsciiFile -Path $Path -Content $content
}

function Initialize-OptionalGateFiles {
    if (Test-AclSmokeEnabled) {
        $javaConfDir = Join-Path $javaRuntimeHome "conf"
        New-Directory $javaRuntimeHome
        New-Directory $javaConfDir
        Write-PlainAclFile -Path (Join-Path $javaConfDir "plain_acl.yml")
        Write-ToolsAclFile -Path (Join-Path $javaConfDir "tools.yml")
        Write-PlainAclFile -Path $rustAclFilePath
    }

    if ($EnableTlsSmoke) {
        Write-JavaTlsConfig -Path $javaTlsConfigPath
    }
}

function Get-JavaOptValue {
    $items = @()
    if (-not [string]::IsNullOrWhiteSpace($env:JAVA_OPT)) {
        $items += $env:JAVA_OPT
    }
    if (Test-AclSmokeEnabled) {
        $items += "-Drocketmq.home.dir=$($javaRuntimeHome -replace '\\','/')"
    }
    if ($EnableTlsSmoke) {
        $items += "-Dtls.enable=true"
        $items += "-Dtls.server.mode=permissive"
        $items += "-Dtls.config.file=$($javaTlsConfigPath -replace '\\','/')"
        $items += "-Dtls.test.mode.enable=true"
        $items += "-Dtls.client.authServer=false"
    }

    return ($items -join " ").Trim()
}

function Resolve-JavaHome {
    param([string]$Candidate)

    if (-not [string]::IsNullOrWhiteSpace($Candidate)) {
        $javaExe = Join-Path $Candidate "bin\java.exe"
        if (Test-Path $javaExe) {
            return (Resolve-Path $Candidate).Path
        }
    }

    $javaCommand = Get-Command java.exe -ErrorAction SilentlyContinue
    if ($null -ne $javaCommand) {
        $startInfo = New-Object System.Diagnostics.ProcessStartInfo
        $startInfo.FileName = $javaCommand.Source
        $startInfo.Arguments = "-XshowSettings:properties -version"
        $startInfo.UseShellExecute = $false
        $startInfo.RedirectStandardOutput = $true
        $startInfo.RedirectStandardError = $true
        $startInfo.CreateNoWindow = $true
        $process = New-Object System.Diagnostics.Process
        $process.StartInfo = $startInfo
        [void]$process.Start()
        $outputText = $process.StandardOutput.ReadToEnd() + [Environment]::NewLine + $process.StandardError.ReadToEnd()
        $process.WaitForExit()
        $output = $outputText -split "\r?\n"
        foreach ($line in $output) {
            if ($line -match '^\s*java\.home\s*=\s*(.+?)\s*$') {
                $candidateHome = $Matches[1].Trim()
                if (Test-Path (Join-Path $candidateHome "bin\java.exe")) {
                    return (Resolve-Path $candidateHome).Path
                }
            }
        }
    }

    throw "JAVA_HOME is not set and java.home could not be inferred from java.exe. Pass -JavaHome."
}

function Stop-ProcessTree {
    param([Parameter(Mandatory = $true)][int]$ProcessId)

    $children = Get-CimInstance Win32_Process -Filter "ParentProcessId = $ProcessId" -ErrorAction SilentlyContinue
    foreach ($child in $children) {
        Stop-ProcessTree -ProcessId ([int]$child.ProcessId)
    }

    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

function Stop-StartedProcesses {
    foreach ($process in $script:processes) {
        if ($null -ne $process -and -not $process.HasExited) {
            Stop-ProcessTree -ProcessId $process.Id
        }
    }
    $script:processes = @()
}

function Write-OutputFileDelta {
    param(
        [Parameter(Mandatory = $true)][string]$OutputFile,
        [Parameter(Mandatory = $true)][ref]$Position
    )

    if (-not (Test-Path $OutputFile)) {
        return
    }

    $stream = [System.IO.File]::Open($OutputFile, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    try {
        if ($stream.Length -lt $Position.Value) {
            $Position.Value = 0
        }
        [void]$stream.Seek([int64]$Position.Value, [System.IO.SeekOrigin]::Begin)
        $reader = New-Object System.IO.StreamReader($stream, [System.Text.Encoding]::UTF8, $true, 4096, $true)
        try {
            $text = $reader.ReadToEnd()
            $Position.Value = $stream.Position
        } finally {
            $reader.Dispose()
        }
    } finally {
        $stream.Dispose()
    }

    if (-not [string]::IsNullOrEmpty($text)) {
        Write-Host -NoNewline $text
    }
}

function Invoke-LoggedCommand {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory,
        [Parameter(Mandatory = $true)][string]$OutputFile,
        [int]$TimeoutSeconds = 0,
        [hashtable]$Environment = @{}
    )

    Write-Host "==> $Name"
    Write-Host "    $Command"
    "$Name=$OutputFile" | Add-Content -Path $summaryFile

    if ($DryRun) {
        Write-Host "[dry-run] $Command"
        return
    }

    Set-Content -Path $OutputFile -Value "" -NoNewline
    $quotedOutputFile = Format-CommandArgument $OutputFile
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = "cmd.exe"
    $startInfo.Arguments = "/c $Command > $quotedOutputFile 2>&1"
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $false
    $startInfo.RedirectStandardError = $false
    $startInfo.CreateNoWindow = $true

    foreach ($entry in $Environment.GetEnumerator()) {
        $startInfo.EnvironmentVariables[$entry.Key] = [string]$entry.Value
    }

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    [void]$process.Start()

    $position = 0
    $deadline = $null
    if ($TimeoutSeconds -gt 0) {
        $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    }

    while (-not $process.WaitForExit(500)) {
        Write-OutputFileDelta -OutputFile $OutputFile -Position ([ref]$position)
        if ($null -ne $deadline -and (Get-Date) -gt $deadline) {
            Stop-ProcessTree -ProcessId $process.Id
            $process.WaitForExit()
            Write-OutputFileDelta -OutputFile $OutputFile -Position ([ref]$position)
            throw "$Name timed out after $TimeoutSeconds seconds. See $OutputFile"
        }
    }

    Write-OutputFileDelta -OutputFile $OutputFile -Position ([ref]$position)

    if ($process.ExitCode -ne 0) {
        throw "$Name failed with exit code $($process.ExitCode). See $OutputFile"
    }
}

function Invoke-LoggedCommandUntilMatch {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory,
        [Parameter(Mandatory = $true)][string]$OutputFile,
        [Parameter(Mandatory = $true)][string[]]$RequiredValues,
        [Parameter(Mandatory = $true)][string]$Description,
        [int]$TimeoutSeconds = 0,
        [hashtable]$Environment = @{},
        [int]$MaxAttempts = 12,
        [int]$DelaySeconds = 5
    )

    $lastError = ""
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        if ($attempt -gt 1) {
            Write-Host "Retrying $Description ($attempt/$MaxAttempts)..."
        }

        try {
            Invoke-LoggedCommand `
                -Name $Name `
                -Command $Command `
                -WorkingDirectory $WorkingDirectory `
                -OutputFile $OutputFile `
                -TimeoutSeconds $TimeoutSeconds `
                -Environment $Environment

            $output = if (Test-Path $OutputFile) { Get-Content -Raw -Path $OutputFile } else { "" }
            $missingValues = @()
            foreach ($value in $RequiredValues) {
                if ($output -notmatch [regex]::Escape($value)) {
                    $missingValues += $value
                }
            }

            if ($missingValues.Count -eq 0) {
                return $output
            }

            $lastError = "$Description output does not contain required value(s): $($missingValues -join ', ')."
        }
        catch {
            $lastError = $_.Exception.Message
        }

        if ($attempt -lt $MaxAttempts) {
            Start-Sleep -Seconds $DelaySeconds
        }
    }

    throw $lastError
}

function Start-LoggedProcess {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory,
        [Parameter(Mandatory = $true)][string]$LogPath,
        [hashtable]$Environment = @{}
    )

    $display = "$FilePath $($ArgumentList -join ' ')"
    Write-Host "==> start $Name"
    Write-Host "    $display"
    "$Name=$LogPath" | Add-Content -Path $summaryFile

    if ($DryRun) {
        Write-Host "[dry-run] $display"
        return $null
    }

    $originalEnvironment = @{}
    foreach ($entry in $Environment.GetEnumerator()) {
        $originalEnvironment[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, "Process")
        [Environment]::SetEnvironmentVariable($entry.Key, [string]$entry.Value, "Process")
    }

    try {
        $process = Start-Process `
            -FilePath $FilePath `
            -ArgumentList $ArgumentList `
            -WorkingDirectory $WorkingDirectory `
            -RedirectStandardOutput $LogPath `
            -RedirectStandardError "$LogPath.err" `
            -WindowStyle Hidden `
            -PassThru
    }
    finally {
        foreach ($entry in $Environment.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($entry.Key, $originalEnvironment[$entry.Key], "Process")
        }
    }

    Write-Host "[started] pid=$($process.Id) $Name"
    return $process
}

function Wait-ForTcpPort {
    param(
        [Parameter(Mandatory = $true)][string]$EndpointHost,
        [Parameter(Mandatory = $true)][int]$Port,
        [int]$TimeoutSeconds = 180
    )

    if ($DryRun) {
        Write-Host "[dry-run] wait for $EndpointHost`:$Port"
        return
    }

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $client = New-Object System.Net.Sockets.TcpClient
        try {
            $asyncResult = $client.BeginConnect($EndpointHost, $Port, $null, $null)
            if ($asyncResult.AsyncWaitHandle.WaitOne(1000) -and $client.Connected) {
                $client.EndConnect($asyncResult)
                $client.Dispose()
                return
            }
        }
        catch {
        }
        finally {
            $client.Dispose()
        }
        Start-Sleep -Milliseconds 500
    }

    throw "Timed out waiting for $EndpointHost`:$Port"
}

function Write-JavaNamesrvConfig {
    param([Parameter(Mandatory = $true)][string]$Path)

    $content = @"
listenPort=$NamesrvPort
kvConfigPath=$((Join-Path $dataRoot 'java-namesrv-kv.json') -replace '\\','/')
configStorePath=$($Path -replace '\\','/')
"@
    Write-AsciiFile -Path $Path -Content $content
}

function Write-JavaBrokerConfig {
    param([Parameter(Mandatory = $true)][string]$Path)

    $javaBrokerDataRoot = Join-Path $dataRoot "java-broker"
    New-Directory $javaBrokerDataRoot
    $content = @"
brokerClusterName=$ClusterName
brokerName=$BrokerName
brokerId=0
brokerIP1=$NamesrvHost
listenPort=$BrokerPort
namesrvAddr=$namesrvAddr
autoCreateTopicEnable=true
autoCreateSubscriptionGroup=true
traceTopicEnable=true
recallMessageEnable=true
deleteWhen=04
fileReservedTime=1
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH
enablePropertyFilter=true
storePathRootDir=$($javaBrokerDataRoot -replace '\\','/')
storePathCommitLog=$((Join-Path $javaBrokerDataRoot 'commitlog') -replace '\\','/')
storePathConsumeQueue=$((Join-Path $javaBrokerDataRoot 'consumequeue') -replace '\\','/')
"@
    if (Test-AclSmokeEnabled) {
        $innerCredentials = "{""accessKey"":""$AclAccessKey"",""secretKey"":""$AclSecretKey""}"
        $content += @"

authenticationEnabled=true
authorizationEnabled=true
authenticationMetadataProvider=org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider
authorizationMetadataProvider=org.apache.rocketmq.auth.authorization.provider.LocalAuthorizationMetadataProvider
migrateAuthFromV1Enabled=true
innerClientAuthenticationCredentials=$innerCredentials
"@
    }
    Write-AsciiFile -Path $Path -Content $content
}

function Write-RustNamesrvConfig {
    param([Parameter(Mandatory = $true)][string]$Path)

    $content = ""
    if ($EnableTlsSmoke) {
        $content += @"
tls.enable = true
tls.test.mode.enable = true
tls.server.mode = "permissive"
tls.server.need.client.auth = "none"
tls.server.authClient = false
tls.client.authServer = false
"@
    }
    Write-AsciiFile -Path $Path -Content $content
}

function Write-RustBrokerConfig {
    param([Parameter(Mandatory = $true)][string]$Path)

    $rustBrokerDataRoot = Join-Path $dataRoot "rust-broker"
    New-Directory $rustBrokerDataRoot
    $content = @"
namesrvAddr = "$namesrvAddr"
brokerIp1 = "$NamesrvHost"
listenPort = $BrokerPort
storePathRootDir = "$($rustBrokerDataRoot -replace '\\','/')"
storePathCommitLog = "$((Join-Path $rustBrokerDataRoot 'commitlog') -replace '\\','/')"
mappedFileSizeCommitLog = 67108864
enableControllerMode = false
autoCreateTopicEnable = true
autoCreateSubscriptionGroup = true
traceTopicEnable = true
recallMessageEnable = true
"@
    if (Test-AclSmokeEnabled) {
        $innerCredentials = "{""accessKey"":""$AclAccessKey"",""secretKey"":""$AclSecretKey""}"
        $content += @"

authenticationEnabled = true
authorizationEnabled = true
aclFile = $(ConvertTo-TomlBasicString ($rustAclFilePath -replace '\\','/'))
innerClientAuthenticationCredentials = $(ConvertTo-TomlBasicString $innerCredentials)
"@
    }
    if ($EnableTlsSmoke) {
        $content += @"

tls.enable = true
tls.test.mode.enable = true
tls.server.mode = "permissive"
tls.server.need.client.auth = "none"
tls.server.authClient = false
tls.client.authServer = false
"@
    }
    $content += @"

[brokerServerConfig]
listenPort = $BrokerPort
bindAddress = "0.0.0.0"

[brokerIdentity]
brokerName = "$BrokerName"
brokerClusterName = "$ClusterName"
brokerId = 0
"@
    Write-AsciiFile -Path $Path -Content $content
}

function Get-SmokeEnvironment {
    $environment = @{
        ROCKETMQ_HOME = $RocketMQHome
        ROCKETMQ_NAMESRV_ADDR = $namesrvAddr
        ROCKETMQ_TEST_TOPIC = $Topic
        ROCKETMQ_REQUIRE_BROKER_BACKED_SMOKE = "true"
    }
    if ($EnableCreateTopicSmoke) {
        $environment["ROCKETMQ_ENABLE_CREATE_TOPIC_SMOKE"] = "true"
    }
    if ($EnableTraceSmoke) {
        $environment["ROCKETMQ_ENABLE_TRACE_SMOKE"] = "true"
    }
    if ($EnableRecallSmoke) {
        $environment["ROCKETMQ_ENABLE_RECALL_SMOKE"] = "true"
    }
    if (Test-AclSmokeEnabled) {
        $environment["ROCKETMQ_ACL_ACCESS_KEY"] = $AclAccessKey
        $environment["ROCKETMQ_ACL_SECRET_KEY"] = $AclSecretKey
    }
    if ($EnableTlsSmoke) {
        $environment["ROCKETMQ_ENABLE_TLS_SMOKE"] = "true"
        $environment["ROCKETMQ_TLS_TEST_MODE_ENABLE"] = "true"
        $environment["ROCKETMQ_TLS_CLIENT_AUTH_SERVER"] = "false"
    }
    return $environment
}

function Add-OptionalSmokeGates {
    if ($BrokerSmokeTests.Count -gt 0) {
        Add-ExternalGate -Gates $externalGates -Gate "broker smoke scenario filter used: $($BrokerSmokeTests -join ', ')"
    }
    if (-not $EnableCreateTopicSmoke) {
        Add-ExternalGate -Gates $externalGates -Gate "create topic broker smoke disabled by -EnableCreateTopicSmoke:`$false"
    }
    if (-not $IncludeAdvancedBrokerSmokes) {
        Add-ExternalGate -Gates $externalGates -Gate "advanced broker smokes not run; pass -IncludeAdvancedBrokerSmokes for retry/orderly/request-reply coverage"
    }
    if (-not $IncludeSlowBrokerSmokes) {
        Add-ExternalGate -Gates $externalGates -Gate "slow broker admin smoke not run; pass -IncludeSlowBrokerSmokes for query/view/min/max/search offset coverage"
    }
    if (-not (Test-AclSmokeEnabled)) {
        Add-ExternalGate -Gates $externalGates -Gate "ACL broker smoke not enabled; pass -EnableAclSmoke or set ROCKETMQ_ACL_ACCESS_KEY/ROCKETMQ_ACL_SECRET_KEY"
    }
    if (-not $EnableTlsSmoke) {
        Add-ExternalGate -Gates $externalGates -Gate "TLS broker smoke not enabled; pass -EnableTlsSmoke"
    }
    if (-not $EnableTraceSmoke) {
        Add-ExternalGate -Gates $externalGates -Gate "Trace broker smoke not enabled; pass -EnableTraceSmoke"
    }
    if (-not $EnableRecallSmoke) {
        Add-ExternalGate -Gates $externalGates -Gate "Recall broker smoke not enabled; pass -EnableRecallSmoke"
    }
}

function Get-BrokerSmokeScenarioTimeoutSeconds {
    param([Parameter(Mandatory = $true)][string]$TestName)

    switch ($TestName) {
        "broker_backed_push_consumer_retry_smoke" { return [Math]::Max($PerBrokerSmokeTimeoutSeconds, 180) }
        "broker_backed_producer_mq_admin_offsets_smoke" { return [Math]::Max($PerBrokerSmokeTimeoutSeconds, 300) }
        "broker_backed_trace_producer_send_smoke" { return [Math]::Max($PerBrokerSmokeTimeoutSeconds, 180) }
        default { return $PerBrokerSmokeTimeoutSeconds }
    }
}

function Get-RustBrokerBackedSmokeScenarios {
    if ($BrokerSmokeTests.Count -gt 0) {
        $selectedScenarios = @()
        foreach ($testName in $BrokerSmokeTests) {
            if ([string]::IsNullOrWhiteSpace($testName)) {
                continue
            }
            $trimmedTestName = $testName.Trim()
            $selectedScenarios += @{
                Test = $trimmedTestName
                TimeoutSeconds = Get-BrokerSmokeScenarioTimeoutSeconds -TestName $trimmedTestName
            }
        }
        return $selectedScenarios
    }

    $scenarios = @(
        @{
            Test = "broker_backed_producer_sync_oneway_batch_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        },
        @{
            Test = "broker_backed_producer_async_callback_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        },
        @{
            Test = "broker_backed_transaction_producer_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        },
        @{
            Test = "broker_backed_lite_pull_assign_offset_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        },
        @{
            Test = "broker_backed_lite_pull_subscribe_poll_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        },
        @{
            Test = "broker_backed_push_consumer_concurrent_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        }
    )

    if ($EnableCreateTopicSmoke) {
        $scenarios += @{
            Test = "broker_backed_producer_create_topic_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        }
    }
    if ($IncludeAdvancedBrokerSmokes) {
        $scenarios += @{
            Test = "broker_backed_push_consumer_retry_smoke"
            TimeoutSeconds = ([Math]::Max($PerBrokerSmokeTimeoutSeconds, 180))
        }
        $scenarios += @{
            Test = "broker_backed_push_consumer_orderly_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        }
        $scenarios += @{
            Test = "broker_backed_producer_request_reply_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        }
    }
    if ($IncludeSlowBrokerSmokes) {
        $scenarios += @{
            Test = "broker_backed_producer_mq_admin_offsets_smoke"
            TimeoutSeconds = ([Math]::Max($PerBrokerSmokeTimeoutSeconds, 300))
        }
    }
    if (Test-AclSmokeEnabled) {
        $scenarios += @{
            Test = "broker_backed_acl_producer_send_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        }
    }
    if ($EnableTlsSmoke) {
        $scenarios += @{
            Test = "broker_backed_tls_producer_send_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        }
    }
    if ($EnableTraceSmoke) {
        $scenarios += @{
            Test = "broker_backed_trace_producer_send_smoke"
            TimeoutSeconds = ([Math]::Max($PerBrokerSmokeTimeoutSeconds, 180))
        }
    }
    if ($EnableRecallSmoke) {
        $scenarios += @{
            Test = "broker_backed_producer_recall_smoke"
            TimeoutSeconds = $PerBrokerSmokeTimeoutSeconds
        }
    }

    return $scenarios
}

function Assert-JavaInputs {
    if (-not (Test-Path (Join-Path $JavaRocketMQRoot "pom.xml"))) {
        throw "Java RocketMQ root does not contain pom.xml: $JavaRocketMQRoot"
    }
    if (-not (Test-Path (Join-Path $JavaRocketmqHome "bin\mqadmin.cmd"))) {
        throw "JavaRocketmqHome does not look like a RocketMQ distribution home: $JavaRocketmqHome"
    }
    if ([string]::IsNullOrWhiteSpace($script:ResolvedJavaHome)) {
        $script:ResolvedJavaHome = Resolve-JavaHome -Candidate $JavaHome
    }
}

function Get-JavaEnvironment {
    param([string]$RocketHome = $JavaRocketmqHome)

    Assert-JavaInputs
    $environment = @{
        ROCKETMQ_HOME = $RocketHome
        JAVA_HOME = $script:ResolvedJavaHome
    }
    $javaOpt = Get-JavaOptValue
    if (-not [string]::IsNullOrWhiteSpace($javaOpt)) {
        $environment["JAVA_OPT"] = $javaOpt
    }
    return $environment
}

function Invoke-AdminTopicGates {
    param([Parameter(Mandatory = $true)][string]$NamePrefix)

    Assert-JavaInputs
    $javaBinRoot = Join-Path $JavaRocketmqHome "bin"
    $adminEnv = Get-JavaEnvironment
    $null = Invoke-LoggedCommandUntilMatch `
        -Name "$NamePrefix-cluster-list" `
        -Command "cmd /c $(Format-CommandArgument (Join-Path $javaBinRoot 'mqadmin.cmd')) clusterList -n $namesrvAddr" `
        -WorkingDirectory $javaBinRoot `
        -OutputFile (Join-Path $logRoot "$NamePrefix-cluster-list.log") `
        -TimeoutSeconds $CommandTimeoutSeconds `
        -Environment $adminEnv `
        -RequiredValues @($ClusterName, $BrokerName, $brokerAddr) `
        -Description "$NamePrefix clusterList"
    $null = Invoke-LoggedCommandUntilMatch `
        -Name "$NamePrefix-update-topic" `
        -Command "cmd /c $(Format-CommandArgument (Join-Path $javaBinRoot 'mqadmin.cmd')) updateTopic -n $namesrvAddr -b $brokerAddr -t $Topic -r 4 -w 4" `
        -WorkingDirectory $javaBinRoot `
        -OutputFile (Join-Path $logRoot "$NamePrefix-update-topic.log") `
        -TimeoutSeconds $CommandTimeoutSeconds `
        -Environment $adminEnv `
        -RequiredValues @($Topic) `
        -Description "$NamePrefix updateTopic"
    $null = Invoke-LoggedCommandUntilMatch `
        -Name "$NamePrefix-topic-route" `
        -Command "cmd /c $(Format-CommandArgument (Join-Path $javaBinRoot 'mqadmin.cmd')) topicRoute -n $namesrvAddr -t $Topic" `
        -WorkingDirectory $javaBinRoot `
        -OutputFile (Join-Path $logRoot "$NamePrefix-topic-route.log") `
        -TimeoutSeconds $CommandTimeoutSeconds `
        -Environment $adminEnv `
        -RequiredValues @($BrokerName, $brokerAddr) `
        -Description "$NamePrefix topicRoute"
}

function Start-JavaTopology {
    Assert-JavaInputs
    $javaBinRoot = Join-Path $JavaRocketmqHome "bin"
    $javaNamesrvConfig = Join-Path $configRoot "java-namesrv.properties"
    $javaBrokerConfig = Join-Path $configRoot "java-broker.properties"
    Write-JavaNamesrvConfig -Path $javaNamesrvConfig
    Write-JavaBrokerConfig -Path $javaBrokerConfig

    $script:processes += Start-LoggedProcess `
        -Name "java-namesrv" `
        -FilePath "cmd.exe" `
        -ArgumentList @("/c", (Join-Path $javaBinRoot "mqnamesrv.cmd"), "-c", $javaNamesrvConfig) `
        -WorkingDirectory $javaBinRoot `
        -LogPath (Join-Path $logRoot "java-namesrv.log") `
        -Environment (Get-JavaEnvironment)
    Wait-ForTcpPort -EndpointHost $NamesrvHost -Port $NamesrvPort -TimeoutSeconds $StartupTimeoutSeconds

    $script:processes += Start-LoggedProcess `
        -Name "java-broker" `
        -FilePath "cmd.exe" `
        -ArgumentList @("/c", (Join-Path $javaBinRoot "mqbroker.cmd"), "-c", $javaBrokerConfig, "-n", $namesrvAddr) `
        -WorkingDirectory $javaBinRoot `
        -LogPath (Join-Path $logRoot "java-broker.log") `
        -Environment (Get-JavaEnvironment)
    Wait-ForTcpPort -EndpointHost $NamesrvHost -Port $BrokerPort -TimeoutSeconds $StartupTimeoutSeconds
    Invoke-AdminTopicGates -NamePrefix "java-topology"
}

function Start-RustTopology {
    $rustBrokerConfig = Join-Path $configRoot "rust-broker.toml"
    Write-RustBrokerConfig -Path $rustBrokerConfig
    if ($EnableTlsSmoke) {
        Write-RustNamesrvConfig -Path $rustNamesrvConfigPath
    }
    New-Directory $RocketMQHome

    $rustNamesrvArgs = @(
        "run", "-p", "rocketmq-namesrv", "--bin", "rocketmq-namesrv-rust", "--"
    )
    if ($EnableTlsSmoke) {
        $rustNamesrvArgs += @("-c", $rustNamesrvConfigPath)
    }
    $rustNamesrvArgs += @(
        "--listenPort", $NamesrvPort,
        "--bindAddress", $NamesrvHost,
        "--rocketmqHome", $RocketMQHome
    )

    $script:processes += Start-LoggedProcess `
        -Name "rust-namesrv" `
        -FilePath "cargo" `
        -ArgumentList $rustNamesrvArgs `
        -WorkingDirectory $workspaceRoot `
        -LogPath (Join-Path $logRoot "rust-namesrv.log") `
        -Environment @{ ROCKETMQ_HOME = $RocketMQHome }
    Wait-ForTcpPort -EndpointHost $NamesrvHost -Port $NamesrvPort -TimeoutSeconds $StartupTimeoutSeconds

    $script:processes += Start-LoggedProcess `
        -Name "rust-broker" `
        -FilePath "cargo" `
        -ArgumentList @(
            "run", "-p", "rocketmq-broker", "--bin", "rocketmq-broker-rust", "--",
            "-c", $rustBrokerConfig,
            "-n", $namesrvAddr
        ) `
        -WorkingDirectory $workspaceRoot `
        -LogPath (Join-Path $logRoot "rust-broker.log") `
        -Environment @{ ROCKETMQ_HOME = $RocketMQHome }
    Wait-ForTcpPort -EndpointHost $NamesrvHost -Port $BrokerPort -TimeoutSeconds $StartupTimeoutSeconds
    Invoke-AdminTopicGates -NamePrefix "rust-topology"
}

function Invoke-RustBrokerBackedSmoke {
    param([Parameter(Mandatory = $true)][string]$Name)

    $environment = Get-SmokeEnvironment
    $scenarios = @(Get-RustBrokerBackedSmokeScenarios)
    if ($scenarios.Count -eq 0) {
        throw "No broker-backed smoke scenarios are enabled for $Name"
    }

    foreach ($scenario in $scenarios) {
        $testName = $scenario.Test
        $timeoutSeconds = $scenario.TimeoutSeconds
        $outputFile = Join-Path $logRoot "$Name-$testName.log"
        Invoke-LoggedCommand `
            -Name "$Name-$testName" `
            -Command "cargo test -p rocketmq-client-rust --test broker_backed_smoke $testName -- --exact --nocapture --test-threads=1" `
            -WorkingDirectory $workspaceRoot `
            -OutputFile $outputFile `
            -TimeoutSeconds $timeoutSeconds `
            -Environment $environment

        $output = if (Test-Path $outputFile) { Get-Content -Raw -Path $outputFile } else { "" }
        if ($output -match "running 0 tests") {
            throw "$Name-$testName matched zero tests. Check -BrokerSmokeTests spelling and argument splitting. See $outputFile"
        }
        $explicitOkLine = $output -match "test\s+$([regex]::Escape($testName))\s+\.\.\.\s+ok"
        $singleTestOkResult = $output -match "test result:\s+ok\.\s+1 passed;"
        if (-not ($explicitOkLine -or $singleTestOkResult)) {
            throw "$Name-$testName did not report a single-test ok result. See $outputFile"
        }
    }
}

function Invoke-JavaClientGate {
    $javaGateScript = Join-Path $PSScriptRoot "run_client_java_gate.ps1"
    $command = "powershell -NoProfile -ExecutionPolicy Bypass -File $(Format-CommandArgument $javaGateScript) " +
        "-Mode Compatibility -JavaRocketMQRoot $(Format-CommandArgument $JavaRocketMQRoot) " +
        "-NamesrvAddr $(Format-CommandArgument $namesrvAddr) -Topic $(Format-CommandArgument $Topic) -Quick"
    if ($UseExistingJavaClasses) {
        $command = "$command -UseExistingJavaClasses"
    }
    if (Test-AclSmokeEnabled) {
        $command = "$command -Acl -AccessKey $(Format-CommandArgument $AclAccessKey) -SecretKey $(Format-CommandArgument $AclSecretKey)"
    }
    if ($EnableTraceSmoke) {
        $command = "$command -Trace"
    }
    if ($EnableRecallSmoke) {
        $command = "$command -Recall"
    }
    if ($EnableTlsSmoke) {
        $command = "$command -Tls"
    }
    $javaGateEnv = Get-SmokeEnvironment
    $javaEnv = Get-JavaEnvironment
    $javaGateEnv["JAVA_HOME"] = $javaEnv["JAVA_HOME"]
    Invoke-LoggedCommand `
        -Name "java-client-rust-broker" `
        -Command $command `
        -WorkingDirectory $workspaceRoot `
        -OutputFile (Join-Path $logRoot "java-client-rust-broker.log") `
        -TimeoutSeconds $CommandTimeoutSeconds `
        -Environment $javaGateEnv
}

function Invoke-BaseGates {
    $baseEnv = @{
        ROCKETMQ_NAMESRV_ADDR = ""
        ROCKETMQ_REQUIRE_BROKER_BACKED_SMOKE = ""
    }
    $commands = @(
        @{ Name = "cargo-fmt-check"; Command = "cargo fmt --all -- --check"; Timeout = $CommandTimeoutSeconds },
        @{ Name = "workspace-clippy"; Command = "cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings"; Timeout = $CommandTimeoutSeconds },
        @{ Name = "client-lib-tests"; Command = "cargo test -p rocketmq-client-rust --lib"; Timeout = $CommandTimeoutSeconds },
        @{ Name = "client-integration-tests"; Command = "cargo test -p rocketmq-client-rust --tests"; Timeout = $CommandTimeoutSeconds },
        @{ Name = "broker-lib-bin-tests"; Command = "cargo test -p rocketmq-broker --lib --bins"; Timeout = $CommandTimeoutSeconds },
        @{ Name = "remoting-lib-tests"; Command = "cargo test -p rocketmq-remoting --lib"; Timeout = $CommandTimeoutSeconds },
        @{ Name = "store-integration-tests"; Command = "cargo test -p rocketmq-store --tests"; Timeout = $CommandTimeoutSeconds }
    )

    foreach ($entry in $commands) {
        Invoke-LoggedCommand `
            -Name $entry.Name `
            -Command $entry.Command `
            -WorkingDirectory $workspaceRoot `
            -OutputFile (Join-Path $logRoot "$($entry.Name).log") `
            -TimeoutSeconds $entry.Timeout `
            -Environment $baseEnv
    }
}

function Invoke-MixedMatrix {
    $matrixScript = Join-Path $PSScriptRoot "run_namesrv_mixed_parity_matrix.ps1"
    Assert-JavaInputs
    $commonArgs = "-JavaRocketmqHome $(Format-CommandArgument $JavaRocketmqHome) " +
        "-JavaHome $(Format-CommandArgument $script:ResolvedJavaHome) " +
        "-MaxDataDriveUsedPercent $MaxDataDriveUsedPercent " +
        "-NamesrvHost $NamesrvHost -NamesrvPort $NamesrvPort -BrokerPort $BrokerPort " +
        "-Topic $(Format-CommandArgument $Topic) -ClusterName $(Format-CommandArgument $ClusterName) " +
        "-BrokerName $(Format-CommandArgument $BrokerName)"

    Invoke-LoggedCommand `
        -Name "mixed-rust-namesrv-java-broker" `
        -Command "powershell -NoProfile -ExecutionPolicy Bypass -File $(Format-CommandArgument $matrixScript) -Mode rust-namesrv-java-broker $commonArgs" `
        -WorkingDirectory $workspaceRoot `
        -OutputFile (Join-Path $logRoot "mixed-rust-namesrv-java-broker.log") `
        -TimeoutSeconds $CommandTimeoutSeconds `
        -Environment @{ ROCKETMQ_HOME = $RocketMQHome; JAVA_HOME = $script:ResolvedJavaHome }

    Invoke-LoggedCommand `
        -Name "mixed-java-namesrv-rust-broker" `
        -Command "powershell -NoProfile -ExecutionPolicy Bypass -File $(Format-CommandArgument $matrixScript) -Mode java-namesrv-rust-broker $commonArgs" `
        -WorkingDirectory $workspaceRoot `
        -OutputFile (Join-Path $logRoot "mixed-java-namesrv-rust-broker.log") `
        -TimeoutSeconds $CommandTimeoutSeconds `
        -Environment @{ ROCKETMQ_HOME = $RocketMQHome; JAVA_HOME = $script:ResolvedJavaHome }
}

function Invoke-ProductionReadiness {
    $readinessScript = Join-Path $PSScriptRoot "run_client_production_readiness.ps1"
    $command = "powershell -NoProfile -ExecutionPolicy Bypass -File $(Format-CommandArgument $readinessScript) " +
        "-BrokerSmoke Require -JavaRocketMQRoot $(Format-CommandArgument $JavaRocketMQRoot) " +
        "-UseExistingJavaClassesForJavaGate"
    Invoke-LoggedCommand `
        -Name "client-production-readiness" `
        -Command $command `
        -WorkingDirectory $workspaceRoot `
        -OutputFile (Join-Path $logRoot "client-production-readiness.log") `
        -TimeoutSeconds $CommandTimeoutSeconds `
        -Environment (Get-SmokeEnvironment)
}

$BrokerSmokeTests = @(Normalize-BrokerSmokeTests)
$dataRoot = Resolve-TestDataRoot
New-Directory $runRoot
New-Directory $logRoot
New-Directory $dataRoot
New-Directory $configRoot
New-Directory $RocketMQHome
Initialize-OptionalGateFiles

"RocketMQ Rust client/broker functional test run" | Set-Content -Path $summaryFile
"timestamp=$runStamp" | Add-Content -Path $summaryFile
"workspace=$workspaceRoot" | Add-Content -Path $summaryFile
"javaRoot=$JavaRocketMQRoot" | Add-Content -Path $summaryFile
"javaHome=$JavaRocketmqHome" | Add-Content -Path $summaryFile
"javaHomeOverride=$JavaHome" | Add-Content -Path $summaryFile
"rocketmqHome=$RocketMQHome" | Add-Content -Path $summaryFile
"dataRoot=$dataRoot" | Add-Content -Path $summaryFile
"defaultDataRoot=$defaultDataRoot" | Add-Content -Path $summaryFile
"namesrv=$namesrvAddr" | Add-Content -Path $summaryFile
"broker=$brokerAddr" | Add-Content -Path $summaryFile
"topic=$Topic" | Add-Content -Path $summaryFile
"brokerSmokeTimeoutSeconds=$BrokerSmokeTimeoutSeconds" | Add-Content -Path $summaryFile
"perBrokerSmokeTimeoutSeconds=$PerBrokerSmokeTimeoutSeconds" | Add-Content -Path $summaryFile
"brokerSmokeTests=$($BrokerSmokeTests -join ',')" | Add-Content -Path $summaryFile
"includeAdvancedBrokerSmokes=$IncludeAdvancedBrokerSmokes" | Add-Content -Path $summaryFile
"includeSlowBrokerSmokes=$IncludeSlowBrokerSmokes" | Add-Content -Path $summaryFile
"enableAclSmoke=$(Test-AclSmokeEnabled)" | Add-Content -Path $summaryFile
"aclAccessKey=$AclAccessKey" | Add-Content -Path $summaryFile
"javaRuntimeHome=$javaRuntimeHome" | Add-Content -Path $summaryFile
"enableTlsSmoke=$EnableTlsSmoke" | Add-Content -Path $summaryFile
"dryRun=$DryRun" | Add-Content -Path $summaryFile
"" | Add-Content -Path $summaryFile

Add-OptionalSmokeGates

try {
    if (-not $SkipBaseGates) {
        Invoke-BaseGates
    } else {
        Add-ExternalGate -Gates $externalGates -Gate "base Cargo gates skipped by -SkipBaseGates"
    }

    if (-not $SkipRustClientJavaBroker) {
        try {
            Start-JavaTopology
            Invoke-RustBrokerBackedSmoke -Name "rust-client-java-broker"
        }
        finally {
            Stop-StartedProcesses
        }
    } else {
        Add-ExternalGate -Gates $externalGates -Gate "Rust client against Java broker skipped by -SkipRustClientJavaBroker"
    }

    if ((-not $SkipRustClientRustBroker) -or (-not $SkipJavaClientRustBroker)) {
        try {
            Start-RustTopology
            if (-not $SkipRustClientRustBroker) {
                Invoke-RustBrokerBackedSmoke -Name "rust-client-rust-broker"
            } else {
                Add-ExternalGate -Gates $externalGates -Gate "Rust client against Rust broker skipped by -SkipRustClientRustBroker"
            }
            if (-not $SkipJavaClientRustBroker) {
                Invoke-JavaClientGate
            } else {
                Add-ExternalGate -Gates $externalGates -Gate "Java client against Rust broker skipped by -SkipJavaClientRustBroker"
            }
        }
        finally {
            Stop-StartedProcesses
        }
    } else {
        Add-ExternalGate -Gates $externalGates -Gate "Rust client against Rust broker skipped by -SkipRustClientRustBroker"
        Add-ExternalGate -Gates $externalGates -Gate "Java client against Rust broker skipped by -SkipJavaClientRustBroker"
    }

    if (-not $SkipMixedMatrix) {
        Invoke-MixedMatrix
    } else {
        Add-ExternalGate -Gates $externalGates -Gate "mixed namesrv/broker matrix skipped by -SkipMixedMatrix"
    }

    if ($RunProductionReadiness) {
        Invoke-ProductionReadiness
    } else {
        Add-ExternalGate -Gates $externalGates -Gate "production readiness wrapper not run; pass -RunProductionReadiness"
    }

    "" | Add-Content -Path $summaryFile
    "External gates still required:" | Add-Content -Path $summaryFile
    if ($externalGates.Count -eq 0) {
        "none" | Add-Content -Path $summaryFile
        Write-Host "All configured functional gates passed."
    } else {
        foreach ($gate in $externalGates) {
            "- $gate" | Add-Content -Path $summaryFile
        }
        Write-Host "Configured gates completed. External gates still required:"
        foreach ($gate in $externalGates) {
            Write-Host " - $gate"
        }
        if ($RequireExternalGates) {
            throw "Functional test external gates are still open. See $summaryFile"
        }
    }

    Write-Host "Functional test summary saved to $summaryFile"
}
finally {
    Stop-StartedProcesses
}

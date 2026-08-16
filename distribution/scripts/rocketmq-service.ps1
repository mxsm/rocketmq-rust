param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('start', 'stop', 'status')]
    [string]$Action,
    [Parameter(Mandatory = $true, Position = 1)]
    [ValidateSet('namesrv', 'broker', 'controller', 'proxy')]
    [string]$Service
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$workdir = if ($env:ROCKETMQ_WORKDIR) { $env:ROCKETMQ_WORKDIR } else { Join-Path (Get-Location) 'work' }
$binaryNames = @{
    namesrv = 'rocketmq-namesrv-rust.exe'
    broker = 'rocketmq-broker-rust.exe'
    controller = 'rocketmq-controller-rust.exe'
    proxy = 'rocketmq-proxy-rust.exe'
}
$runDir = Join-Path $workdir 'run'
$logDir = Join-Path $workdir 'logs'
$dataDir = Join-Path $workdir "data/$Service"
New-Item -ItemType Directory -Force -Path $runDir, $logDir, $dataDir | Out-Null
$pidFile = Join-Path $runDir "$Service.pid"
$logFile = Join-Path $logDir "$Service.log"

function Get-ServiceProcess {
    if (-not (Test-Path -LiteralPath $pidFile)) { return $null }
    $processId = [int](Get-Content -LiteralPath $pidFile -Raw)
    return Get-Process -Id $processId -ErrorAction SilentlyContinue
}

$process = Get-ServiceProcess
switch ($Action) {
    'start' {
        if ($process) { Write-Output "$Service already running"; exit 0 }
        $env:ROCKETMQ_HOME = $workdir
        $arguments = @('-c', (Join-Path $root "conf/$Service.toml"))
        $process = Start-Process -FilePath (Join-Path $root "bin/$($binaryNames[$Service])") `
            -ArgumentList $arguments -RedirectStandardOutput $logFile -RedirectStandardError "$logFile.err" `
            -WindowStyle Hidden -PassThru
        Set-Content -LiteralPath $pidFile -Value $process.Id
    }
    'stop' {
        if (-not $process) { Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue; exit 0 }
        Stop-Process -Id $process.Id
        Remove-Item -LiteralPath $pidFile -Force
    }
    'status' {
        if ($process) { Write-Output "$Service running"; exit 0 }
        Write-Output "$Service stopped"
        exit 3
    }
}

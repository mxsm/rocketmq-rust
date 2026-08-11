[CmdletBinding()]
param(
    [ValidateSet("send", "store", "ha", "consume", "proxy", "all")]
    [string]$Scope = "all",
    [switch]$ListOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$workspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$testCases = @(
    [pscustomobject]@{
        Scope = "send"
        Arguments = @("test", "-p", "rocketmq-protocol", "--test", "request_header_java_compatibility")
    },
    [pscustomobject]@{
        Scope = "store"
        Arguments = @("test", "-p", "rocketmq-store", "--test", "timer_java_compat_tests")
    },
    [pscustomobject]@{
        Scope = "ha"
        Arguments = @("test", "-p", "rocketmq-store", "--test", "ha_transfer_engine")
    },
    [pscustomobject]@{
        Scope = "consume"
        Arguments = @("test", "-p", "rocketmq-client-rust", "--test", "pull_message_service_test")
    },
    [pscustomobject]@{
        Scope = "consume"
        Arguments = @("test", "-p", "rocketmq-client-rust", "--test", "lite_pull_capability_tests")
    },
    [pscustomobject]@{
        Scope = "consume"
        Arguments = @("test", "-p", "rocketmq-client-rust", "--test", "lite_pull_assignment_registry_tests")
    },
    [pscustomobject]@{
        Scope = "proxy"
        Arguments = @("test", "-p", "rocketmq-proxy", "--test", "grpc_ingress")
    },
    [pscustomobject]@{
        Scope = "proxy"
        Arguments = @("test", "-p", "rocketmq-proxy-cluster")
    },
    [pscustomobject]@{
        Scope = "proxy"
        Arguments = @("test", "-p", "rocketmq-proxy-local")
    }
)

function Format-CommandLine {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $formatted = $Arguments | ForEach-Object {
        if ($_ -match '\s') {
            '"' + $_.Replace('"', '\"') + '"'
        }
        else {
            $_
        }
    }
    return "cargo " + ($formatted -join " ")
}

$selectedTests = @($testCases | Where-Object { $Scope -eq "all" -or $_.Scope -eq $Scope })
if ($selectedTests.Count -eq 0) {
    throw "No focused tests are registered for scope '$Scope'."
}

if (-not $ListOnly) {
    Get-Command cargo -ErrorAction Stop | Out-Null
}

Push-Location $workspaceRoot
try {
    foreach ($testCase in $selectedTests) {
        $commandLine = Format-CommandLine -Arguments $testCase.Arguments
        Write-Host "[$($testCase.Scope)] $commandLine"
        if ($ListOnly) {
            continue
        }

        & cargo @($testCase.Arguments)
        if ($LASTEXITCODE -ne 0) {
            throw "Focused test failed with exit code $LASTEXITCODE`: $commandLine"
        }
    }
}
finally {
    Pop-Location
}

if ($ListOnly) {
    Write-Host "Listed $($selectedTests.Count) focused test command(s)."
}
else {
    Write-Host "Completed $($selectedTests.Count) focused test command(s)."
}

# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$KubectlPath,
    [Parameter(Mandatory = $true)][string]$KubeconfigPath,
    [Parameter(Mandatory = $true)][string]$KubeContext,
    [Parameter(Mandatory = $true)][string]$Namespace,
    [Parameter(Mandatory = $true)][string]$Service,
    [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$LocalPort,
    [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$RemotePort
)

$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'
$forward = "${LocalPort}:${RemotePort}"

while (Test-Path -LiteralPath $KubeconfigPath -PathType Leaf) {
    $arguments = @(
        '--kubeconfig', $KubeconfigPath,
        '--context', $KubeContext,
        '--namespace', $Namespace,
        'port-forward', "service/$Service", $forward,
        '--address=127.0.0.1'
    )
    & $KubectlPath @arguments
    Start-Sleep -Seconds 2
}

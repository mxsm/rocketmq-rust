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
    [ValidateSet("Library", "Validate")]
    [string]$HelperMode = "Validate"
)

function Assert-LiveFaultIdentity {
    param(
        [Parameter(Mandatory)][ValidatePattern('^[a-z0-9][a-z0-9-]{2,62}$')][string]$RunToken,
        [Parameter(Mandatory)][ValidatePattern('^[a-z0-9][a-z0-9.-]{0,62}$')][string]$Namespace,
        [Parameter(Mandatory)][ValidatePattern('^[a-z0-9][a-z0-9.-]{0,62}$')][string]$Pod
    )
    [pscustomobject]@{ RunToken = $RunToken; Namespace = $Namespace; Pod = $Pod }
}

function Invoke-LiveFaultNative {
    param(
        [Parameter(Mandatory)][string]$Executable,
        [Parameter(Mandatory)][string[]]$Arguments,
        [switch]$AllowFailure
    )
    if (Get-Command Invoke-Native -ErrorAction SilentlyContinue) {
        return Invoke-Native $Executable $Arguments -AllowFailure:$AllowFailure
    }
    $output = (& $Executable @Arguments 2>&1 | Out-String).TrimEnd()
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "$Executable failed with exit code $exitCode`n$output"
    }
    [pscustomobject]@{ ExitCode = $exitCode; Output = $output }
}

function Get-LiveBrokerRoleSnapshot {
    param([Parameter(Mandatory)][string]$Namespace)
    $records = [System.Collections.Generic.List[object]]::new()
    foreach ($ordinal in 0..2) {
        $pod = "rocketmq-broker-$ordinal"
        $logs = Invoke-LiveFaultNative kubectl @('-n', $Namespace, 'logs', $pod, '--timestamps', '--tail=2000') -AllowFailure
        $roleLines = @($logs.Output -split "`r?`n" | Where-Object {
            $_ -match 'Apply controller role change' -or $_ -match 'new_role=(SyncMaster|Slave)'
        })
        $lastRoleLine = if ($roleLines.Count -eq 0) { '' } else { [string]$roleLines[-1] }
        $role = if ($lastRoleLine -match 'new_role=SyncMaster') {
            'SyncMaster'
        } elseif ($lastRoleLine -match 'new_role=Slave') {
            'Slave'
        } else {
            'Unknown'
        }
        $controllerIdMatch = [regex]::Match($lastRoleLine, 'broker_controller_id=(-?\d+)')
        $controllerId = if ($controllerIdMatch.Success) { [int64]$controllerIdMatch.Groups[1].Value } else { -1 }
        $state = Invoke-LiveFaultNative kubectl @('-n', $Namespace, 'get', 'pod', $pod, '-o', 'json') -AllowFailure
        $ready = $false
        $uid = ''
        $node = ''
        $ip = ''
        if ($state.ExitCode -eq 0) {
            $podState = $state.Output | ConvertFrom-Json
            $ready = @($podState.status.conditions | Where-Object {
                $_.type -eq 'Ready' -and $_.status -eq 'True'
            }).Count -eq 1
            $uid = [string]$podState.metadata.uid
            $node = [string]$podState.spec.nodeName
            $ip = [string]$podState.status.podIP
        }
        $records.Add([pscustomobject]@{
            Pod = $pod; Role = $role; ControllerId = $controllerId; Ready = $ready; Uid = $uid; Node = $node; PodIp = $ip
            RoleEvidence = $lastRoleLine
        })
    }
    $masters = @($records | Where-Object { $_.Role -eq 'SyncMaster' -and $_.Ready })
    [pscustomobject]@{
        Records = @($records)
        Masters = $masters
        Output = ($records | ConvertTo-Json -Depth 5)
    }
}

function Wait-LiveSingleMaster {
    param(
        [Parameter(Mandatory)][string]$Namespace,
        [string]$ExcludedPod = '',
        [ValidateRange(1, 600)][int]$TimeoutSeconds = 180
    )
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $last = $null
    do {
        $last = Get-LiveBrokerRoleSnapshot -Namespace $Namespace
        $eligible = @($last.Masters | Where-Object { [string]::IsNullOrWhiteSpace($ExcludedPod) -or $_.Pod -ne $ExcludedPod })
        if ($last.Masters.Count -eq 1 -and $eligible.Count -eq 1) {
            return [pscustomobject]@{ Master = $eligible[0]; Snapshot = $last }
        }
        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    $details = if ($null -eq $last) { 'no Broker role sample' } else { $last.Output }
    throw "Broker group did not converge to exactly one live master before the deadline`n$details"
}

function Get-LivePodMetrics {
    param(
        [Parameter(Mandatory)][string]$Namespace,
        [Parameter(Mandatory)][string]$Pod,
        [ValidateRange(1, 65535)][int]$Port = 5557,
        [ValidatePattern('^/[a-zA-Z0-9_./-]*$')][string]$Path = '/metrics'
    )
    $request = "exec 3<>/dev/tcp/127.0.0.1/$Port; printf 'GET $Path HTTP/1.0\r\nHost: localhost\r\n\r\n' >&3; cat <&3"
    $result = Invoke-LiveFaultNative kubectl @(
        '-n', $Namespace, 'exec', $Pod, '--', '/bin/bash', '-c', $request
    ) -AllowFailure
    if ($result.ExitCode -ne 0 -or $result.Output -notmatch 'HTTP/1\.[01] 200') {
        throw "cannot read live metrics from $Pod"
    }
    $result
}

function Set-LivePodPortImpairment {
    param(
        [Parameter(Mandatory)][ValidatePattern('^[a-zA-Z0-9_.-]+$')][string]$Node,
        [Parameter(Mandatory)][ValidatePattern('^(?:\d{1,3}\.){3}\d{1,3}$')][string]$PodIp,
        [Parameter(Mandatory)][ValidateRange(1, 65535)][int]$Port,
        [Parameter(Mandatory)][ValidateRange(0, 60000)][int]$DelayMilliseconds,
        [Parameter(Mandatory)][ValidateRange(0, 100)][int]$LossPercent,
        [Parameter(Mandatory)][ValidatePattern('^[a-z0-9-]{3,32}$')][string]$RuleTag
    )
    $before = Invoke-LiveFaultNative docker @('exec', $Node, 'tc', 'qdisc', 'show', 'dev', 'eth0')
    $commands = @(
        "tc qdisc replace dev eth0 root handle 1: prio",
        "tc qdisc replace dev eth0 parent 1:3 handle 30: netem delay ${DelayMilliseconds}ms loss ${LossPercent}%",
        "tc filter replace dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst $PodIp/32 match ip dport $Port 0xffff flowid 1:3"
    )
    foreach ($command in $commands) {
        Invoke-LiveFaultNative docker @('exec', $Node, '/bin/sh', '-c', $command) | Out-Null
    }
    $after = Invoke-LiveFaultNative docker @('exec', $Node, 'tc', 'qdisc', 'show', 'dev', 'eth0')
    if ($after.Output -notmatch '\bnetem\b') { throw "live port impairment did not install netem" }
    [pscustomobject]@{
        Kind = 'pod-port-netem'; RuleTag = $RuleTag; Node = $Node; PodIp = $PodIp; Port = $Port
        Before = $before.Output; After = $after.Output
    }
}

function Clear-LivePodPortImpairment {
    param(
        [Parameter(Mandatory)][ValidatePattern('^[a-zA-Z0-9_.-]+$')][string]$Node,
        [Parameter(Mandatory)][ValidatePattern('^[a-z0-9-]{3,32}$')][string]$RuleTag
    )
    $cleanup = Invoke-LiveFaultNative docker @('exec', $Node, 'tc', 'qdisc', 'del', 'dev', 'eth0', 'root') -AllowFailure
    $state = Invoke-LiveFaultNative docker @('exec', $Node, 'tc', 'qdisc', 'show', 'dev', 'eth0')
    if ($state.Output -match '\bnetem\b') { throw "live port impairment cleanup left netem installed: $RuleTag" }
    [pscustomobject]@{ ExitCode = $cleanup.ExitCode; Output = "$($cleanup.Output)`n$($state.Output)" }
}

function Start-LiveBrokerDiskFull {
    param(
        [Parameter(Mandatory)][string]$Namespace,
        [Parameter(Mandatory)][string]$Pod,
        [Parameter(Mandatory)][string]$RunToken,
        [ValidateRange(8, 256)][int]$ReserveMiB = 16,
        [ValidateRange(256, 4096)][int]$MaximumFillMiB = 2048
    )
    $null = Assert-LiveFaultIdentity -RunToken $RunToken -Namespace $Namespace -Pod $Pod
    $path = "/var/lib/rocketmq/.live-disk-full-$RunToken"
    $probe = Invoke-LiveFaultNative kubectl @(
        '-n', $Namespace, 'exec', $Pod, '--', '/bin/sh', '-c', 'df -Pk /var/lib/rocketmq | tail -1'
    )
    $fields = @($probe.Output -split '\s+' | Where-Object { $_ })
    if ($fields.Count -lt 4 -or $fields[3] -notmatch '^\d+$') { throw "cannot parse Broker PVC free space" }
    $availableMiB = [math]::Floor([int64]$fields[3] / 1024)
    if ($availableMiB -gt ($MaximumFillMiB + $ReserveMiB)) {
        throw "Broker PVC is too large for bounded disk-full injection: available=${availableMiB}MiB"
    }
    $fillMiB = [math]::Max(1, $availableMiB - $ReserveMiB)
    $script = "rm -f '$path'; dd if=/dev/zero of='$path' bs=1048576 count=$fillMiB conv=fsync; sync"
    $write = Invoke-LiveFaultNative kubectl @(
        '-n', $Namespace, 'exec', $Pod, '--', '/bin/sh', '-c', $script
    ) -AllowFailure
    $after = Invoke-LiveFaultNative kubectl @(
        '-n', $Namespace, 'exec', $Pod, '--', '/bin/sh', '-c', 'df -Pk /var/lib/rocketmq | tail -1'
    )
    $afterFields = @($after.Output -split '\s+' | Where-Object { $_ })
    $remainingMiB = [math]::Floor([int64]$afterFields[3] / 1024)
    if ($remainingMiB -gt ($ReserveMiB + 2)) { throw "bounded disk-full injection did not consume the target PVC" }
    [pscustomobject]@{
        Kind = 'broker-pvc-disk-full'; Namespace = $Namespace; Pod = $Pod; Path = $path
        AvailableBeforeMiB = $availableMiB; RemainingMiB = $remainingMiB
        FillMiB = $fillMiB; WriteExitCode = $write.ExitCode; Output = "$($probe.Output)`n$($write.Output)`n$($after.Output)"
    }
}

function Clear-LiveBrokerDiskFull {
    param(
        [Parameter(Mandatory)][string]$Namespace,
        [Parameter(Mandatory)][string]$Pod,
        [Parameter(Mandatory)][string]$RunToken
    )
    $null = Assert-LiveFaultIdentity -RunToken $RunToken -Namespace $Namespace -Pod $Pod
    $path = "/var/lib/rocketmq/.live-disk-full-$RunToken"
    $script = "rm -f '$path'; sync; test ! -e '$path'; df -Pk /var/lib/rocketmq | tail -1"
    Invoke-LiveFaultNative kubectl @('-n', $Namespace, 'exec', $Pod, '--', '/bin/sh', '-c', $script)
}

function Get-LiveSnapshotObservation {
    param(
        [Parameter(Mandatory)][string]$Namespace,
        [Parameter(Mandatory)][string]$Pod,
        [string]$SinceTime = '',
        [ValidateRange(10, 5000)][int]$TailLines = 1000
    )
    $logArguments = @('-n', $Namespace, 'logs', $Pod, "--tail=$TailLines")
    if (-not [string]::IsNullOrWhiteSpace($SinceTime)) { $logArguments += "--since-time=$SinceTime" }
    $logs = Invoke-LiveFaultNative kubectl $logArguments -AllowFailure
    $files = Invoke-LiveFaultNative kubectl @(
        '-n', $Namespace, 'exec', $Pod, '--', '/bin/sh', '-c',
        'find /var/lib/rocketmq/controller -maxdepth 4 -type f -print 2>/dev/null | sort'
    ) -AllowFailure
    $combined = "$($logs.Output)`n$($files.Output)"
    [pscustomobject]@{
        SnapshotObserved = $combined -match '(?i)snapshot'; InstallObserved = $combined -match '(?i)install[_ -]?snapshot'
        Logs = $logs.Output; Files = $files.Output
    }
}

function Wait-LiveSnapshotInstall {
    param(
        [Parameter(Mandatory)][string]$Namespace,
        [Parameter(Mandatory)][string]$Pod,
        [Parameter(Mandatory)][string]$SinceTime,
        [ValidateRange(1, 300)][int]$TimeoutSeconds = 120
    )
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $last = $null
    do {
        $last = Get-LiveSnapshotObservation `
            -Namespace $Namespace `
            -Pod $Pod `
            -SinceTime $SinceTime `
            -TailLines 2000
        if ($last.InstallObserved) { return $last }
        Start-Sleep -Milliseconds 250
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    $details = if ($null -eq $last) { 'no snapshot observation' } else { $last.Logs }
    throw "Controller follower did not start live snapshot installation before the deadline`n$details"
}

function Assert-LiveFaultCleanup {
    param(
        [Parameter(Mandatory)][string]$Namespace,
        [Parameter(Mandatory)][string]$RunToken,
        [string[]]$Nodes = @(),
        [string[]]$BrokerPods = @('rocketmq-broker-0', 'rocketmq-broker-1', 'rocketmq-broker-2')
    )
    $residue = [System.Collections.Generic.List[string]]::new()
    foreach ($node in $Nodes) {
        $qdisc = Invoke-LiveFaultNative docker @('exec', $node, 'tc', 'qdisc', 'show', 'dev', 'eth0') -AllowFailure
        if ($qdisc.Output -match '\bnetem\b') { $residue.Add("netem:$node") }
    }
    foreach ($pod in $BrokerPods) {
        $files = Invoke-LiveFaultNative kubectl @(
            '-n', $Namespace, 'exec', $pod, '--', '/bin/sh', '-c',
            "find /var/lib/rocketmq -maxdepth 1 -name '.live-*-$RunToken' -print"
        ) -AllowFailure
        if (-not [string]::IsNullOrWhiteSpace($files.Output)) { $residue.Add("files:${pod}:$($files.Output)") }
    }
    if ($residue.Count -ne 0) { throw "live fault cleanup residue: $($residue -join ', ')" }
    [pscustomobject]@{ ExitCode = 0; Output = 'live fault cleanup verified' }
}

if ($HelperMode -eq "Validate") {
    Write-Host "LIVE_KUBERNETES_FAULT_HELPERS_VALID"
}

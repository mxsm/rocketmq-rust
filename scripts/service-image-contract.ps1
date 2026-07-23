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
    [string]$OutputDirectory = "target/service-images"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        # PowerShell abbreviates named parameters, so `Command` would consume
        # native `-c` arguments intended for tools such as /bin/sh.
        [string]$Executable,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )

    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Executable failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Captured {
    param(
        [Parameter(Mandatory = $true)]
        # Keep native switches distinct from this wrapper's named parameters.
        [string]$Executable,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )

    $output = (& $Executable @Arguments | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "$Executable failed with exit code $LASTEXITCODE"
    }
    return $output
}

function Format-CriticalVulnerabilitySummary {
    param(
        [object[]]$Vulnerabilities
    )

    $summaries = @(
        $Vulnerabilities |
            Select-Object -First 10 |
            ForEach-Object {
                $fixedVersionProperty = $_.PSObject.Properties["FixedVersion"]
                $fixedVersion = if (
                    $null -eq $fixedVersionProperty -or
                    [string]::IsNullOrWhiteSpace([string]$fixedVersionProperty.Value)
                ) {
                    "unfixed"
                }
                else {
                    [string]$fixedVersionProperty.Value
                }
                "$($_.VulnerabilityID) $($_.PkgName) $($_.InstalledVersion) -> $fixedVersion"
            }
    )
    return $summaries -join "; "
}

foreach ($command in @("docker", "syft", "trivy", "cosign", "git")) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "required command is unavailable: $command"
    }
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$policyPath = Join-Path $root "docker/container-policy.json"
$policy = Get-Content -Raw -LiteralPath $policyPath | ConvertFrom-Json
$tmpfsOptions = "$($policy.runtime.tmpfs_path):rw,noexec,nosuid,size=16m,uid=$($policy.runtime.uid),gid=$($policy.runtime.gid),mode=0700"
$dockerfilePath = (Resolve-Path (Join-Path $root $policy.foundation_dockerfile)).Path
$smokeConfigPath = (Resolve-Path (Join-Path $root $policy.smoke_config_directory)).Path
$outputPath = Join-Path $root $OutputDirectory
New-Item -ItemType Directory -Force -Path $outputPath | Out-Null

$sourceCommit = (Invoke-Captured git -C $root rev-parse HEAD).Trim()
$sourceEpoch = [long](Invoke-Captured git -C $root show -s --format=%ct HEAD).Trim()
$sourceVersion = "1.0.0-$($sourceCommit.Substring(0, 12))"
$env:SOURCE_DATE_EPOCH = $sourceEpoch

$keyPrefix = Join-Path $outputPath "ephemeral-service-images"
$privateKey = "$keyPrefix.key"
$publicKey = "$keyPrefix.pub"
$oldPassword = $env:COSIGN_PASSWORD
$randomBytes = New-Object byte[] 32
$random = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$evidence = [System.Collections.Generic.List[object]]::new()

try {
    $random.GetBytes($randomBytes)
    $env:COSIGN_PASSWORD = [Convert]::ToBase64String($randomBytes)
    Invoke-Checked cosign generate-key-pair --output-key-prefix $keyPrefix

    foreach ($serviceProperty in $policy.services.PSObject.Properties) {
        $serviceName = $serviceProperty.Name
        $service = $serviceProperty.Value
        $imageRef = "rocketmq-rust/$($serviceName):verification"

        Invoke-Checked docker buildx build --load --file $dockerfilePath --target $service.target --tag $imageRef --build-arg "SOURCE_REVISION=$sourceCommit" --build-arg "SOURCE_VERSION=$sourceVersion" $root

        $configuredUser = (Invoke-Captured docker image inspect --format "{{.Config.User}}" $imageRef).Trim()
        if ($configuredUser -ne "$($policy.runtime.uid):$($policy.runtime.gid)") {
            throw "$serviceName runtime image user mismatch: $configuredUser"
        }

        $entrypoint = @((Invoke-Captured docker image inspect --format "{{json .Config.Entrypoint}}" $imageRef) | ConvertFrom-Json)
        $expectedEntrypoint = "/usr/local/bin/$($service.binary)"
        if ($entrypoint.Count -ne 1 -or $entrypoint[0] -ne $expectedEntrypoint) {
            throw "$serviceName entrypoint mismatch: $($entrypoint -join ',')"
        }

        $command = @((Invoke-Captured docker image inspect --format "{{json .Config.Cmd}}" $imageRef) | ConvertFrom-Json)
        $expectedCommand = @($service.command)
        if (($command -join "`n") -ne ($expectedCommand -join "`n")) {
            throw "$serviceName command mismatch"
        }

        $labels = (Invoke-Captured docker image inspect --format "{{json .Config.Labels}}" $imageRef) | ConvertFrom-Json
        $expectedLabels = [ordered]@{
            "io.rocketmq.image.role" = $serviceName
            "io.rocketmq.service.binary" = $service.binary
            "io.rocketmq.service.config-path" = $service.config_path
            "io.rocketmq.service.data-path" = $service.data_path
            "io.rocketmq.service.ports" = (@($service.ports) -join ",")
            "io.rocketmq.service.signal" = "SIGTERM"
        }
        foreach ($label in $expectedLabels.GetEnumerator()) {
            if ($labels.PSObject.Properties[$label.Key].Value -ne $label.Value) {
                throw "$serviceName label mismatch: $($label.Key)"
            }
        }

        $exposedPorts = (Invoke-Captured docker image inspect --format "{{json .Config.ExposedPorts}}" $imageRef) | ConvertFrom-Json
        foreach ($port in @($service.ports)) {
            if ($null -eq $exposedPorts.PSObject.Properties["$port/tcp"]) {
                throw "$serviceName does not expose required TCP port $port"
            }
        }

        $ownedBinaries = (Invoke-Captured docker run --rm --entrypoint /bin/sh $imageRef -c "find /usr/local/bin -maxdepth 1 -type f -name 'rocketmq-*' -printf '%f\n' | sort").Split("`n", [System.StringSplitOptions]::RemoveEmptyEntries)
        if ($ownedBinaries.Count -ne 1 -or $ownedBinaries[0] -ne $service.binary) {
            throw "$serviceName runtime image contains binaries outside its owner boundary: $($ownedBinaries -join ',')"
        }

        & docker run --rm --network none --read-only --tmpfs $tmpfsOptions $imageRef *> $null
        if ($LASTEXITCODE -eq 0) {
            throw "$serviceName must fail closed when its required config mount is absent"
        }

        $permissionSmoke = @(
            "set -eu",
            ('test "$(id -u)" = {0}' -f $policy.runtime.uid),
            ('test "$(id -g)" = {0}' -f $policy.runtime.gid),
            "! touch /opt/rocketmq/rootfs-must-stay-read-only 2>/dev/null",
            "touch $($service.data_path)/data-write",
            "touch $($policy.runtime.tmpfs_path)/tmp-write"
        ) -join "; "
        Invoke-Checked docker run --rm --network none --read-only --mount "type=volume,destination=$($policy.runtime.writable_data_path)" --tmpfs $tmpfsOptions --entrypoint /bin/sh $imageRef -c $permissionSmoke

        $containerId = ""
        try {
            $containerId = (Invoke-Captured docker run --detach --interactive --network none --read-only --mount "type=volume,destination=$($policy.runtime.writable_data_path)" --mount "type=bind,source=$smokeConfigPath,target=/etc/rocketmq,readonly" --tmpfs $tmpfsOptions $imageRef).Trim()
            Start-Sleep -Seconds 5
            $running = (Invoke-Captured docker inspect --format "{{.State.Running}}" $containerId).Trim()
            if ($running -ne "true") {
                $logs = Invoke-Captured docker logs $containerId
                throw "$serviceName exited before SIGTERM smoke: $logs"
            }
            Invoke-Checked docker stop --signal SIGTERM --timeout 30 $containerId
            $exitCode = [int](Invoke-Captured docker inspect --format "{{.State.ExitCode}}" $containerId).Trim()
            if ($exitCode -ne 0) {
                $logs = Invoke-Captured docker logs $containerId
                throw "$serviceName SIGTERM exit code was $exitCode`: $logs"
            }
        }
        finally {
            if ($containerId) {
                & docker rm --force $containerId *> $null
            }
        }

        $sbomPath = Join-Path $outputPath "$serviceName.cdx.json"
        $trivyPath = Join-Path $outputPath "$serviceName.trivy.json"
        $bundlePath = Join-Path $outputPath "$serviceName.sbom.sigstore.json"
        Invoke-Checked syft scan "docker:$imageRef" --scope all-layers --output "cyclonedx-json=$sbomPath"
        Invoke-Checked trivy image --scanners vuln --severity CRITICAL --exit-code 0 --format json --output $trivyPath $imageRef
        Invoke-Checked cosign sign-blob --yes --key $privateKey --bundle $bundlePath $sbomPath
        Invoke-Checked cosign verify-blob --key $publicKey --bundle $bundlePath $sbomPath

        $sbom = Get-Content -Raw -LiteralPath $sbomPath | ConvertFrom-Json
        if ($sbom.bomFormat -ne "CycloneDX") {
            throw "$serviceName Syft output is not a CycloneDX SBOM"
        }
        $trivyReport = Get-Content -Raw -LiteralPath $trivyPath | ConvertFrom-Json
        $criticalVulnerabilities = @(
            foreach ($result in @($trivyReport.Results)) {
                $vulnerabilities = $result.PSObject.Properties["Vulnerabilities"]
                if ($null -ne $vulnerabilities) {
                    foreach ($vulnerability in @($vulnerabilities.Value)) {
                        if ($vulnerability.Severity -eq "CRITICAL") {
                            $vulnerability
                        }
                    }
                }
            }
        )
        $criticalFindings = $criticalVulnerabilities.Count
        if ($criticalFindings -gt $policy.supply_chain.critical_vulnerability_policy.maximum_findings) {
            $criticalSummary = Format-CriticalVulnerabilitySummary -Vulnerabilities $criticalVulnerabilities
            throw "$serviceName critical vulnerability policy failed: $criticalFindings findings; $criticalSummary"
        }

        $evidence.Add([ordered]@{
            service = $serviceName
            target = $service.target
            package = $service.package
            binary = $service.binary
            image_ref = $imageRef
            image_id = (Invoke-Captured docker image inspect --format "{{.Id}}" $imageRef).Trim()
            configured_user = $configuredUser
            config_path = $service.config_path
            data_path = $service.data_path
            ports = @($service.ports)
            signal = "SIGTERM"
            critical_vulnerability_findings = $criticalFindings
            sbom_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $sbomPath).Hash.ToLowerInvariant()
            trivy_report_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $trivyPath).Hash.ToLowerInvariant()
            signature_bundle_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $bundlePath).Hash.ToLowerInvariant()
        })
    }
}
finally {
    $random.Dispose()
    $env:COSIGN_PASSWORD = $oldPassword
    if (Test-Path -LiteralPath $privateKey) {
        Remove-Item -LiteralPath $privateKey -Force
    }
}

if (Test-Path -LiteralPath $privateKey) {
    throw "ephemeral service-image signing private key was not deleted"
}

$provenance = [ordered]@{
    schema_version = 1
    milestone = $policy.milestone
    source_commit = $sourceCommit
    source_date_epoch = $sourceEpoch
    builder_image = $policy.base_images.builder.reference
    runtime_image = $policy.base_images.runtime.reference
    rust_toolchain = $policy.build.rust_toolchain
    debian_snapshot = $policy.build.debian_snapshot
    private_key_retained = $false
    services = @($evidence)
}
$provenance | ConvertTo-Json -Depth 8 | Set-Content -Encoding utf8 -LiteralPath (Join-Path $outputPath "provenance.json")
Write-Host "SERVICE_IMAGE_CONTRACT_OK services=$($evidence.Count) output=$OutputDirectory"

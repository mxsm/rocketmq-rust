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

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "docker" / "container-policy.json"

EXPECTED_SERVICES: dict[str, dict[str, Any]] = {
    "broker": {
        "target": "broker",
        "package": "rocketmq-broker",
        "binary": "rocketmq-broker-rust",
        "config_path": "/etc/rocketmq/broker.toml",
        "data_path": "/var/lib/rocketmq/broker",
        "ports": [8088, 10911, 10912],
        "command": ["--configFile", "/etc/rocketmq/broker.toml"],
    },
    "namesrv": {
        "target": "namesrv",
        "package": "rocketmq-namesrv",
        "binary": "rocketmq-namesrv-rust",
        "config_path": "/etc/rocketmq/namesrv.toml",
        "data_path": "/var/lib/rocketmq/namesrv",
        "ports": [8088, 9876],
        "command": ["--configFile", "/etc/rocketmq/namesrv.toml"],
    },
    "controller": {
        "target": "controller",
        "package": "rocketmq-controller",
        "binary": "rocketmq-controller-rust",
        "config_path": "/etc/rocketmq/controller.toml",
        "data_path": "/var/lib/rocketmq/controller",
        "ports": [8088, 60109, 60110],
        "command": ["--config-file", "/etc/rocketmq/controller.toml"],
    },
    "proxy": {
        "target": "proxy",
        "package": "rocketmq-proxy",
        "binary": "rocketmq-proxy-rust",
        "config_path": "/etc/rocketmq/proxy.toml",
        "data_path": "/var/lib/rocketmq/proxy",
        "ports": [8080, 8081, 8088],
        "command": ["--config", "/etc/rocketmq/proxy.toml"],
    },
    "mcp": {
        "target": "mcp",
        "package": "rocketmq-mcp",
        "binary": "rocketmq-mcp",
        "config_path": "/etc/rocketmq/mcp.toml",
        "data_path": "/var/lib/rocketmq/mcp",
        "ports": [8088, 8089],
        "command": ["--config", "/etc/rocketmq/mcp.toml", "--transport", "stdio"],
    },
}


def load_policy(path: Path = POLICY_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def audit_foundation(
    policy: dict[str, Any],
    dockerfile: str,
    workflow: str,
    supply_script: str,
    dockerfiles: set[str],
    service_script: str = "",
    signal_sources: dict[str, str] | None = None,
    dockerignore: str = "",
) -> list[str]:
    findings: list[str] = []
    if policy.get("schema_version") != 1:
        findings.append("container policy schema_version must be 1")
    if policy.get("milestone") != "M11-10":
        findings.append("container policy milestone must be M11-10")

    foundation = policy.get("foundation_dockerfile")
    exceptions = policy.get("compatibility_exceptions", [])
    registered = {foundation}
    if exceptions:
        findings.append("M11-10 must retire all legacy Dockerfile compatibility exceptions")
    for exception in exceptions:
        registered.add(exception.get("path"))
    for path in sorted(dockerfiles - registered):
        findings.append(f"unregistered Dockerfile: {path}")
    for path in sorted(registered - dockerfiles):
        findings.append(f"registered Dockerfile does not exist: {path}")
    if re.search(r"(?m)^\s*(?:tests/|\*\*/tests/)\s*$", dockerignore):
        findings.append("Docker build context must preserve explicit Cargo test targets")

    builder_ref = policy["base_images"]["builder"]["reference"]
    runtime_ref = policy["base_images"]["runtime"]["reference"]
    for name, reference in (("builder", builder_ref), ("runtime", runtime_ref)):
        if not re.fullmatch(r"[^\s@]+:[^\s@]+@sha256:[0-9a-f]{64}", reference):
            findings.append(f"{name} base image must use a tag plus sha256 manifest digest")
        if ":latest@" in reference:
            findings.append(f"{name} base image must not use latest")
    required_dockerfile_fragments = [
        f"ARG BUILDER_IMAGE={builder_ref}",
        f"ARG RUNTIME_IMAGE={runtime_ref}",
        f"ARG DEBIAN_SNAPSHOT={policy['build']['debian_snapshot']}",
        f"ARG ROCKETMQ_RUST_TOOLCHAIN={policy['build']['rust_toolchain']}",
        "AS builder-base",
        "AS runtime-base",
        "AS runtime-base-smoke",
        "snapshot.debian.org/archive/debian/",
        "snapshot.debian.org/archive/debian-security/",
        "Acquire::Check-Valid-Until=false",
        "apt-get install -y --no-install-recommends ca-certificates",
        "sed -i 's#URIs: http://#URIs: https://#' /etc/apt/sources.list.d/rocketmq-snapshot.sources",
        "test -f /usr/include/google/protobuf/duration.proto",
        "test -f /usr/include/google/protobuf/timestamp.proto",
        "protoc --version",
        "COPY --from=ca-bundle /etc/ssl/certs/ /etc/ssl/certs/",
        f"USER {policy['runtime']['uid']}:{policy['runtime']['gid']}",
        'CMD ["/bin/true"]',
        "STOPSIGNAL SIGTERM",
    ]
    for fragment in required_dockerfile_fragments:
        if fragment not in dockerfile:
            findings.append(f"foundation Dockerfile missing: {fragment}")
    for package in policy["build"]["builder_packages"]:
        package_line = re.search(rf"(?m)^\s*{re.escape(package)}(?:\s|\\|$)", dockerfile)
        inline_install = re.search(
            rf"(?m)^\s*apt-get install [^\n]*\b{re.escape(package)}\b",
            dockerfile,
        )
        if package_line is None and inline_install is None:
            findings.append(f"foundation Dockerfile missing pinned-snapshot package: {package}")
    expected_runtime_packages = {
        "coreutils",
        "dash",
        "findutils",
        "libc6",
        "libgcc-s1",
        "libssl3t64",
        "passwd",
    }
    if policy["build"].get("runtime_dependency_source") != "pinned-base-image-with-snapshot-ca-bundle":
        findings.append("runtime dependency source must remain the pinned base image plus snapshot CA bundle")
    if set(policy["build"].get("runtime_base_packages", [])) != expected_runtime_packages:
        findings.append("runtime pinned-base package contract drifted")
    foundation_only = dockerfile.split("FROM builder-base AS service-builder", maxsplit=1)[0]
    if re.search(r"(?im)^\s*ENTRYPOINT\b", foundation_only):
        findings.append("foundation Dockerfile must not define a shell or service entrypoint")
    if re.search(r"(?im)^\s*COPY\s+\.\s", foundation_only):
        findings.append("foundation runtime must not copy the repository source")
    if re.search(r"(?i)(?:^|[^a-z])latest(?:[^a-z]|$)", dockerfile):
        findings.append("foundation Dockerfile contains a mutable latest tag")
    for label, value in policy["runtime"]["required_labels"].items():
        if f'{label}="{value}"' not in dockerfile:
            findings.append(f"foundation Dockerfile missing required label {label}={value}")

    runtime = policy["runtime"]
    if runtime.get("uid") == 0 or runtime.get("gid") == 0:
        findings.append("container runtime identity must be non-root")
    if runtime.get("read_only_rootfs_required") is not True:
        findings.append("container policy must require a read-only rootfs")
    if runtime.get("default_command") != ["/bin/true"]:
        findings.append("container foundation default command must remain /bin/true")

    services = policy.get("services")
    if services != EXPECTED_SERVICES:
        findings.append("five-service package/binary/config/data/port/command contract drifted")
    if policy.get("smoke_config_directory") != "docker/smoke-config":
        findings.append("service smoke config directory drifted")
    if policy.get("service_contract_script") != "scripts/service-image-contract.ps1":
        findings.append("service image contract script path drifted")
    if policy.get("build", {}).get("service_builder_target") != "service-builder":
        findings.append("service builder target drifted")
    if policy.get("build", {}).get("service_runtime_target") != "service-runtime":
        findings.append("service runtime target drifted")
    if policy.get("build", {}).get("default_target") != "container-contract-default":
        findings.append("container default target drifted")
    if policy.get("build", {}).get("snapshot_bootstrap_transport") != "http-with-signed-release-then-https":
        findings.append("signed snapshot trust-bootstrap transport drifted")
    if dockerfile.count("URIs: http://snapshot.debian.org/") != 2:
        findings.append("builder stage must bootstrap CA from the signed snapshot over HTTP")
    if dockerfile.count("sed -i 's#URIs: http://#URIs: https://#'") != 1:
        findings.append("builder stage must switch snapshot transport back to HTTPS")
    runtime_section = dockerfile.split("FROM ${RUNTIME_IMAGE} AS runtime-base", maxsplit=1)[1].split(
        "FROM runtime-base AS runtime-base-smoke", maxsplit=1
    )[0]
    if re.search(r"(?m)^\s*(?:apt-get|apt)\s", runtime_section):
        findings.append("runtime stage must not resolve mutable packages")
    for fragment in (
        "FROM builder-base AS service-builder",
        "FROM runtime-base AS service-runtime",
        "FROM runtime-base-smoke AS container-contract-default",
        'VOLUME ["/var/lib/rocketmq"]',
        "USER 10001:10001",
    ):
        if fragment not in dockerfile:
            findings.append(f"service Dockerfile missing: {fragment}")
    if "ROCKETMQ_COMPONENT" in dockerfile:
        findings.append("service images must not use component-selector dispatch")
    for service_name, contract in EXPECTED_SERVICES.items():
        target = contract["target"]
        match = re.search(
            rf"(?ms)^FROM service-runtime AS {re.escape(target)}\s*(.*?)(?=^FROM |\Z)",
            dockerfile,
        )
        if match is None:
            findings.append(f"service Dockerfile missing independent target: {target}")
            continue
        section = match.group(1)
        binary = contract["binary"]
        ports = " ".join(str(port) for port in contract["ports"])
        required_service_fragments = [
            f"COPY --from=service-builder --chmod=0555 /opt/rocketmq-binaries/{binary} /usr/local/bin/{binary}",
            f'io.rocketmq.image.role="{service_name}"',
            f'io.rocketmq.service.binary="{binary}"',
            f'io.rocketmq.service.config-path="{contract["config_path"]}"',
            f'io.rocketmq.service.data-path="{contract["data_path"]}"',
            f'io.rocketmq.service.ports="{",".join(str(port) for port in contract["ports"])}"',
            'io.rocketmq.service.signal="SIGTERM"',
            f"EXPOSE {ports}",
            f'ENTRYPOINT ["/usr/local/bin/{binary}"]',
            f"CMD {json.dumps(contract['command'])}",
        ]
        for fragment in required_service_fragments:
            if fragment not in section:
                findings.append(f"{service_name} image target missing: {fragment}")
        if re.search(r"(?im)^\s*ENTRYPOINT\s+.*(?:/bin/(?:sh|bash)|rocketmq-start)", section):
            findings.append(f"{service_name} image must use its direct binary entrypoint")
        if re.search(
            r"(?im)^\s*(?:ENTRYPOINT|CMD)\s+.*(?:password|secret|token|access[-_]?key)",
            section,
        ):
            findings.append(f"{service_name} image command must not contain secret arguments")
        if re.search(r"(?i)(?:password|secret|token|access[-_]?key)", " ".join(contract["command"])):
            findings.append(f"{service_name} image command must not contain secret arguments")
        build_pattern = (
            rf"(?m)^cargo build .*--package {re.escape(contract['package'])}"
            rf" .*--bin {re.escape(binary)}$"
        )
        if not re.search(build_pattern, dockerfile):
            findings.append(f"service builder missing owner pair: {contract['package']}/{binary}")

    expected_signal_sources = {
        "broker": "rocketmq-broker/src/broker_bootstrap.rs",
        "namesrv": "rocketmq-namesrv/src/bootstrap.rs",
        "controller": "rocketmq-controller/src/bin/controller_bootstrap.rs",
        "proxy": "rocketmq-proxy/src/bootstrap.rs",
        "mcp_stdio": "rocketmq-tools/rocketmq-mcp/src/main.rs",
        "mcp_http": "rocketmq-tools/rocketmq-mcp/src/transport/streamable_http.rs",
    }
    if policy.get("signal_sources") != expected_signal_sources:
        findings.append("service signal source registry drifted")
    for name, source in (signal_sources or {}).items():
        if "wait_for_shutdown_signal" not in source and "wait_for_signal_result" not in source:
            findings.append(f"{name} entrypoint must use the shared lifecycle SIGINT/SIGTERM waiter")
        if "tokio::signal::ctrl_c" in source:
            findings.append(f"{name} entrypoint must not use a Ctrl-C-only signal boundary")

    naming = policy["naming"]
    if naming.get("repository_prefix") != "ghcr.io/mxsm/rocketmq-rust":
        findings.append("container repository prefix drifted")
    if set(naming.get("services", [])) != {"broker", "namesrv", "controller", "proxy", "mcp"}:
        findings.append("container service naming set must contain the five M11 services")
    try:
        tag_pattern = re.compile(naming["immutable_tag_pattern"])
    except (KeyError, re.error):
        findings.append("container immutable tag pattern is invalid")
    else:
        if not tag_pattern.fullmatch("1.0.0-0123456789ab") or tag_pattern.fullmatch("latest"):
            findings.append("container immutable tag pattern is not strict")
    if set(naming.get("forbidden_tags", [])) != {"latest", "main", "master"}:
        findings.append("container mutable tag deny-list drifted")

    supply_chain = policy["supply_chain"]
    vulnerability = supply_chain["critical_vulnerability_policy"]
    if vulnerability != {
        "severity": "CRITICAL",
        "maximum_findings": 0,
        "ignore_unfixed": False,
    }:
        findings.append("container critical vulnerability policy was weakened")
    signature = supply_chain["signature"]
    if signature.get("format") != "sigstore-bundle" or signature.get("private_key_must_be_deleted") is not True:
        findings.append("container signature bundle/private-key policy was weakened")
    expected_digest_pattern = (
        r"^ghcr\.io/mxsm/rocketmq-rust/(broker|namesrv|controller|proxy|mcp)@sha256:[0-9a-f]{64}$"
    )
    if signature.get("published_digest_pattern") != expected_digest_pattern:
        findings.append("published container signatures must require an approved GHCR service digest")
    if signature.get("certificate_oidc_issuer") != "https://token.actions.githubusercontent.com":
        findings.append("published container signatures must require the GitHub Actions OIDC issuer")

    if "pull_request_target:" in workflow:
        findings.append("container workflow must not use pull_request_target")
    if "permissions:\n  contents: read" not in workflow:
        findings.append("container workflow must declare contents: read")
    for forbidden_permission in ("id-token: write", "packages: write", "contents: write"):
        if forbidden_permission in workflow:
            findings.append(f"container foundation workflow must not request {forbidden_permission}")
    for action, sha in policy["workflow"]["actions"].items():
        if f"uses: {action}@{sha}" not in workflow:
            findings.append(f"container workflow must pin {action}@{sha}")
    for version_key, input_name in (
        ("syft_version", "syft-version"),
        ("trivy_version", "version"),
        ("cosign_version", "cosign-release"),
    ):
        version = policy["supply_chain"][version_key]
        if f"{input_name}: {version}" not in workflow:
            findings.append(f"container workflow must pin {input_name}: {version}")
    for fragment in (
        "python scripts/container_image_guard.py",
        "scripts/container-supply-chain.ps1",
        "scripts/service-image-contract.ps1",
        "target/container-foundation",
        "target/service-images",
        "if: always()",
    ):
        if fragment not in workflow:
            findings.append(f"container workflow missing: {fragment}")

    script_fragments = [
        "--read-only",
        "--network",
        "none",
        "--tmpfs",
        "--mount",
        "$policy.runtime.tmpfs_path",
        "$policy.runtime.writable_data_path",
        "cyclonedx-json",
        "--severity",
        "CRITICAL",
        "--exit-code 0",
        "cosign",
        "sign-blob",
        "verify-blob",
        "--bundle",
        "Remove-Item -LiteralPath $privateKey",
        "cosign sign --yes $PublishedImageDigest",
        "cosign verify --certificate-identity-regexp",
        "$policy.supply_chain.signature.published_digest_pattern",
        'PSObject.Properties["FixedVersion"]',
    ]
    for fragment in script_fragments:
        if fragment not in supply_script:
            findings.append(f"container supply-chain script missing: {fragment}")
    tmpfs_ownership = (
        "uid=$($policy.runtime.uid)",
        "gid=$($policy.runtime.gid)",
        "mode=0700",
    )
    if any(fragment not in supply_script for fragment in tmpfs_ownership):
        findings.append("container supply-chain tmpfs must be owned by the non-root runtime identity")
    if supply_script.count("[string]$Executable") < 1 or "[string]$Command" in supply_script:
        findings.append("container supply-chain native runner must reserve -c for executable arguments")
    if "--ignore-unfixed" in supply_script:
        findings.append("critical vulnerability gate must not ignore unfixed findings")
    if "$_.FixedVersion" in supply_script:
        findings.append("container vulnerability summary must tolerate a missing FixedVersion property")

    service_script_fragments = [
        "docker buildx build",
        "--target $service.target",
        "--read-only",
        "must fail closed when its required config mount is absent",
        "docker stop --signal SIGTERM --timeout 30",
        "{{.State.ExitCode}}",
        "$service.data_path",
        "find /usr/local/bin",
        "cyclonedx-json",
        "--severity CRITICAL",
        "--exit-code 0",
        "cosign sign-blob",
        "cosign verify-blob",
        "Remove-Item -LiteralPath $privateKey",
        'PSObject.Properties["FixedVersion"]',
    ]
    for fragment in service_script_fragments:
        if fragment not in service_script:
            findings.append(f"service image contract script missing: {fragment}")
    if any(fragment not in service_script for fragment in tmpfs_ownership):
        findings.append("service image tmpfs must be owned by the non-root runtime identity")
    if service_script.count("[string]$Executable") < 2 or "[string]$Command" in service_script:
        findings.append("service image native runners must reserve -c for executable arguments")
    if "--ignore-unfixed" in service_script:
        findings.append("service image CRITICAL vulnerability gate must not ignore unfixed findings")
    if "$_.FixedVersion" in service_script:
        findings.append("service vulnerability summary must tolerate a missing FixedVersion property")
    return findings


def repository_dockerfiles(root: Path = ROOT) -> set[str]:
    return {
        path.relative_to(root).as_posix()
        for path in (root / "docker").glob("Dockerfile*")
        if path.is_file()
    }


def main() -> int:
    try:
        policy = load_policy()
        dockerfile = (ROOT / policy["foundation_dockerfile"]).read_text(encoding="utf-8")
        workflow = (ROOT / policy["workflow"]["path"]).read_text(encoding="utf-8")
        supply_script = (ROOT / "scripts" / "container-supply-chain.ps1").read_text(encoding="utf-8")
        service_script = (ROOT / policy["service_contract_script"]).read_text(encoding="utf-8")
        signal_sources = {
            name: (ROOT / path).read_text(encoding="utf-8") for name, path in policy["signal_sources"].items()
        }
        dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
        findings = audit_foundation(
            policy,
            dockerfile,
            workflow,
            supply_script,
            repository_dockerfiles(),
            service_script,
            signal_sources,
            dockerignore,
        )
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        print(f"CONTAINER_IMAGE_GUARD_FAILED {error}", file=sys.stderr)
        return 2
    if findings:
        for finding in findings:
            print(f"CONTAINER_IMAGE_GUARD_FINDING {finding}", file=sys.stderr)
        return 1
    print(
        "CONTAINER_IMAGE_GUARD_OK "
        f"dockerfiles={len(repository_dockerfiles())} "
        f"exceptions={len(policy['compatibility_exceptions'])} "
        f"services={len(policy['services'])} "
        f"milestone={policy['milestone']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

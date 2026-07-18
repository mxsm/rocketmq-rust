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


def load_policy(path: Path = POLICY_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def audit_foundation(
    policy: dict[str, Any],
    dockerfile: str,
    workflow: str,
    supply_script: str,
    dockerfiles: set[str],
) -> list[str]:
    findings: list[str] = []
    if policy.get("schema_version") != 1:
        findings.append("container policy schema_version must be 1")
    if policy.get("milestone") != "M11-07":
        findings.append("container policy milestone must be M11-07")

    foundation = policy.get("foundation_dockerfile")
    exceptions = policy.get("compatibility_exceptions", [])
    registered = {foundation}
    for exception in exceptions:
        path = exception.get("path")
        registered.add(path)
        if exception.get("expires_at") != "M11-08":
            findings.append(f"compatibility exception {path} must expire at M11-08")
        if not str(exception.get("reason", "")).strip():
            findings.append(f"compatibility exception {path} requires a reason")
    for path in sorted(dockerfiles - registered):
        findings.append(f"unregistered Dockerfile: {path}")
    for path in sorted(registered - dockerfiles):
        findings.append(f"registered Dockerfile does not exist: {path}")

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
        f"USER {policy['runtime']['uid']}:{policy['runtime']['gid']}",
        'CMD ["/bin/true"]',
        "STOPSIGNAL SIGTERM",
    ]
    for fragment in required_dockerfile_fragments:
        if fragment not in dockerfile:
            findings.append(f"foundation Dockerfile missing: {fragment}")
    for package in policy["build"]["builder_packages"] + policy["build"]["runtime_packages"]:
        if not re.search(rf"(?m)^\s*{re.escape(package)}(?:\s|\\|$)", dockerfile):
            findings.append(f"foundation Dockerfile missing pinned-snapshot package: {package}")
    if re.search(r"(?im)^\s*ENTRYPOINT\b", dockerfile):
        findings.append("foundation Dockerfile must not define a shell or service entrypoint")
    if re.search(r"(?im)^\s*COPY\s+\.\s", dockerfile):
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
        "target/container-foundation",
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
        "--exit-code",
        "cosign",
        "sign-blob",
        "verify-blob",
        "--bundle",
        "Remove-Item -LiteralPath $privateKey",
        "cosign sign --yes $PublishedImageDigest",
        "cosign verify --certificate-identity-regexp",
        "$policy.supply_chain.signature.published_digest_pattern",
    ]
    for fragment in script_fragments:
        if fragment not in supply_script:
            findings.append(f"container supply-chain script missing: {fragment}")
    if "--ignore-unfixed" in supply_script:
        findings.append("critical vulnerability gate must not ignore unfixed findings")
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
        findings = audit_foundation(policy, dockerfile, workflow, supply_script, repository_dockerfiles())
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
        f"milestone={policy['milestone']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

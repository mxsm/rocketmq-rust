#!/usr/bin/env python3
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

"""Qualify Docker Hub release digests and mirror only their version tags to GHCR."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib
from urllib.parse import quote

from publish_dockerhub_images import (IDENTITY, ISSUER, ReleaseError, check_identity,
                                      qualify, registry_reference, run, verify_registry_evidence)


ROOT = Path(__file__).resolve().parents[1]
IMAGE_TYPES = {"application/vnd.oci.image.manifest.v1+json",
               "application/vnd.docker.distribution.manifest.v2+json"}


def inspect(reference: str, *, missing_ok: bool = False) -> dict | None:
    reference = registry_reference(reference)
    result = subprocess.run(["crane", "digest", reference], text=True, capture_output=True, check=False)
    if result.returncode:
        # A transport, authorization, or rate-limit failure is never an absent image.
        if missing_ok and re.search(r"\b(MANIFEST_UNKNOWN|NAME_UNKNOWN)\b", result.stderr):
            return None
        raise ReleaseError(f"cannot inspect {reference}: {result.stderr.strip()}")
    digest = result.stdout.strip()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ReleaseError(f"invalid image digest for {reference}")
    repository = reference.split("@")[0].rsplit(":", 1)[0]
    immutable = f"{repository}@{digest}"
    manifest = json.loads(run(["crane", "manifest", immutable], capture=True))
    if manifest.get("mediaType") not in IMAGE_TYPES or "manifests" in manifest:
        raise ReleaseError(f"{reference} must be a single image, without embedded attestation manifests")
    config = json.loads(run(["crane", "config", immutable], capture=True))
    if config.get("os") != "linux" or config.get("architecture") != "amd64":
        raise ReleaseError(f"{reference} is not linux/amd64")
    return {"digest": digest, "labels": config.get("config", {}).get("Labels", {})}


def destination_state(owner: str, name: str, version: str) -> dict | None:
    # GHCR's token endpoint can return DENIED for a repository that has never existed.
    # Confirm package absence through GitHub's authenticated API instead of treating DENIED as absence.
    package = quote(f"rocketmq-rust/{name}", safe="")
    result = subprocess.run(["gh", "api", f"users/{owner}/packages/container/{package}"],
                            text=True, capture_output=True, check=False)
    if result.returncode:
        if "HTTP 404" in result.stderr:
            return None
        raise ReleaseError(f"cannot check destination package {name}: {result.stderr.strip()}")
    return inspect(f"ghcr.io/{owner}/rocketmq-rust/{name}:{version}", missing_ok=True)


def verify_sbom_subject(attestations: str, repository: str, digest: str) -> None:
    # Cosign canonicalizes docker.io to index.docker.io in the statement subject.
    names = {repository, repository.replace("docker.io/", "index.docker.io/", 1),
             registry_reference(repository)}
    for line in attestations.splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)["payload"]
        statement = json.loads(base64.b64decode(payload + "=" * (-len(payload) % 4), validate=True))
        subjects = statement.get("subject")
        predicate = statement.get("predicate")
        if (statement.get("_type") in {"https://in-toto.io/Statement/v0.1", "https://in-toto.io/Statement/v1"}
                and statement.get("predicateType") == "https://cyclonedx.org/bom"
                and isinstance(subjects, list) and len(subjects) == 1
                and subjects[0].get("name") in names
                and subjects[0].get("digest") == {"sha256": digest.removeprefix("sha256:")}
                and isinstance(predicate, dict) and predicate.get("bomFormat") == "CycloneDX"
                and isinstance(predicate.get("components"), list)):
            return
    raise ReleaseError("verified source SBOM does not bind the exact repository/digest")


def verify_source(repository: str, digest: str, commit: str, name: str, prefix: Path) -> None:
    reference = registry_reference(f"{repository}@{digest}")
    verification = ["--certificate-identity-regexp", IDENTITY, "--certificate-oidc-issuer", ISSUER]
    signature = verify_registry_evidence([
        "cosign", "verify", *verification, "--annotations", f"source_commit={commit}",
        "--annotations", f"component={name}", reference,
    ])
    attestation = verify_registry_evidence([
        "cosign", "verify-attestation", *verification, "--type", "cyclonedx", reference,
    ])
    verify_sbom_subject(attestation, repository, digest)
    prefix.with_suffix(".source-signature.json").write_text(signature, encoding="utf-8")
    prefix.with_suffix(".source-attestation.jsonl").write_text(attestation, encoding="utf-8")


def scan(reference: str, prefix: Path) -> None:
    qualify(reference, prefix, remote=True)
    findings = json.loads(prefix.with_suffix(".trivy.json").read_text(encoding="utf-8"))
    if any(vulnerability.get("Severity") == "CRITICAL"
           for result in findings.get("Results", []) for vulnerability in result.get("Vulnerabilities", [])):
        raise ReleaseError(f"CRITICAL findings block {reference}")


def selected_images(group: str) -> list[dict]:
    policy = json.loads((ROOT / "docker/release-images.json").read_text(encoding="utf-8"))
    if policy["schema_version"] != 1 or policy["platform"] != "linux/amd64":
        raise ReleaseError("unsupported release image policy")
    groups = policy["groups"] if group == "all" else {group: policy["groups"][group]}
    images = [image for entries in groups.values() for image in entries]
    names = [image["name"] for image in images]
    if not images or len(names) != len(set(names)):
        raise ReleaseError("release catalog must contain unique components")
    for image in images:
        if (not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", image["name"])
                or image["repository"] != f"rocketmq-rust-{image['name']}"):
            raise ReleaseError("invalid release repository mapping")
    return images


def validate_source(version: str, commit: str) -> None:
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ReleaseError("version must be stable semver")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ReleaseError("source commit must be a full lowercase SHA")
    actual = run(["git", "rev-parse", f"refs/tags/v{version}^{{commit}}"], capture=True).strip()
    run(["git", "merge-base", "--is-ancestor", commit, "refs/remotes/origin/main"])
    manifest = tomllib.loads(run(["git", "show", f"{commit}:Cargo.toml"], capture=True))
    if actual != commit or manifest["workspace"]["package"]["version"] != version:
        raise ReleaseError("release tag, source commit, and workspace version disagree")


def execute(group: str, version: str, commit: str, namespace: str, owner: str,
            publish: bool, output: Path) -> dict:
    validate_source(version, commit)
    if any(not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", value) for value in (namespace, owner)):
        raise ReleaseError("registry namespace/owner must be lowercase")
    images = selected_images(group)
    output.mkdir(parents=True, exist_ok=True)
    result = {"schema_version": 1, "version": version, "source_commit": commit,
              "tooling_commit": run(["git", "rev-parse", "HEAD"], capture=True).strip(),
              "group": group, "platform": "linux/amd64", "dry_run": not publish,
              "expected_components": [image["name"] for image in images],
              "complete": False, "images": [], "artifacts": {}}

    def save() -> None:
        (output / "publication.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")

    save()
    plan = []
    for image in images:
        name = image["name"]
        source = f"docker.io/{namespace}/{image['repository']}"
        destination = f"ghcr.io/{owner}/rocketmq-rust/{name}"
        state = inspect(f"{source}:{version}")
        check_identity(state, version, commit, name)
        existing = destination_state(owner, name, version) if publish else None
        if existing and existing["digest"] != state["digest"]:
            raise ReleaseError(f"refusing to replace conflicting release {destination}:{version}")
        if existing:
            check_identity(existing, version, commit, name)
        plan.append((name, source, destination, state["digest"], existing is not None))

    # Every selected source passes identity, signature, SBOM and fresh scan checks before any copy.
    for name, source, _, digest, _ in plan:
        print(f"Qualifying {name} {digest}", flush=True)
        verify_source(source, digest, commit, name, output / name)
        scan(f"{source}@{digest}", output / name)
    for name, source, destination, digest, present in plan:
        reference = f"{destination}:{version}"
        if publish:
            if not present:
                run(["crane", "copy", registry_reference(f"{source}@{digest}"), reference])
            final = inspect(reference)
            check_identity(final, version, commit, name)
            if final["digest"] != digest:
                raise ReleaseError(f"registry copy changed the digest for {name}")
            # No staging/commit tags, referrer bundles, or image-index wrappers are written to GHCR.
            tags = run(["crane", "ls", destination], capture=True).splitlines()
            if version not in tags:
                raise ReleaseError(f"published version tag is missing for {name}")
        result["images"].append({"component": name, "source": f"{source}@{digest}",
                                 "destination": reference, "digest": digest,
                                 "tags": [version], "published": publish, "checks": "passed"})
        save()
    result["artifacts"] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                           for path in sorted(output.glob("*")) if path.is_file()
                           and path.name not in {"publication.json", "publication.sigstore.json"}}
    result["complete"] = True
    save()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", default="all")
    parser.add_argument("--version", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--owner", default="mxsm")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--output", type=Path, default=ROOT / "target/release/ghcr")
    args = parser.parse_args()
    try:
        result = execute(args.group, args.version, args.source_commit, args.namespace,
                         args.owner, args.publish, args.output)
    except (ReleaseError, OSError, KeyError, ValueError, TypeError, subprocess.CalledProcessError) as error:
        print(f"GHCR_RELEASE_FAILED: {error}", file=sys.stderr)
        return 1
    print(f"GHCR_RELEASE_OK dry_run={result['dry_run']} images={len(result['images'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

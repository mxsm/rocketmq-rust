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

"""Build, scan, and optionally publish one group of Docker Hub release images."""

from __future__ import annotations

import argparse
import base64
from collections.abc import Callable
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tomllib

from dockerhub_tags import DockerHubTags, HubError


ROOT = Path(os.environ.get("RELEASE_SOURCE_ROOT", Path(__file__).resolve().parents[1])).resolve()
SOURCE = "https://github.com/mxsm/rocketmq-rust"
IDENTITY = r"^https://github\.com/mxsm/rocketmq-rust/\.github/workflows/release\.yml@refs/heads/main$"
ISSUER = "https://token.actions.githubusercontent.com"


class ReleaseError(ValueError):
    """Publication cannot proceed without changing an existing release or bypassing a check."""


def run(command: list[str], *, capture: bool = False) -> str:
    result = subprocess.run(command, cwd=ROOT, check=True, text=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout if capture else ""


def upload_cosign_evidence(command: list[str]) -> None:
    """Retry idempotent evidence uploads only for registry throttling or server failures."""
    for attempt in range(5):
        result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
        if result.stdout:
            print(result.stdout, end="", flush=True)
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr, flush=True)
        if result.returncode == 0:
            return
        transient = re.search(r"unexpected status code (?:429|5[0-9]{2})\b", result.stderr)
        if not transient or attempt == 4:
            raise subprocess.CalledProcessError(
                result.returncode, command, output=result.stdout, stderr=result.stderr,
            )
        delay = min(15 * 2 ** attempt, 60)
        print(f"Registry evidence upload temporarily unavailable; retry {attempt + 1}/4 in {delay}s",
              file=sys.stderr, flush=True)
        time.sleep(delay)


def verify_registry_evidence(command: list[str], *, validate: Callable[[str], None] | None = None) -> str:
    """Wait for newly uploaded evidence to become discoverable; verification stays mandatory."""
    for attempt in range(10):
        result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr, flush=True)
        if result.returncode == 0:
            try:
                if validate is not None:
                    validate(result.stdout)
            except ReleaseError:
                # Re-signing an immutable image leaves older valid attestations discoverable
                # while the registry indexes the new one. Only the requested payload satisfies us.
                if attempt == 9:
                    raise
            else:
                return result.stdout
        else:
            lines = result.stderr.strip().splitlines()
            message = lines[-1] if lines else ""
            message = message.removeprefix("error during command execution: ").removeprefix("Error: ")
            unavailable = message in {
                "no signatures found",
                "no attestations found",
                "no valid bundles exist in registry",
                "no matching attestations: no valid bundles exist in registry",
            } or message.startswith("none of the attestations matched the predicate type: ")
            unavailable = unavailable or bool(re.search(
                r"unexpected status code (?:429|5[0-9]{2})\b|\bTOOMANYREQUESTS:", message,
            ))
            if not unavailable or attempt == 9:
                raise subprocess.CalledProcessError(
                    result.returncode, command, output=result.stdout, stderr=result.stderr,
                )
        delay = min(5 * 2 ** attempt, 60)
        print(f"Requested registry evidence not discoverable yet; retry {attempt + 1}/9 in {delay}s",
              file=sys.stderr, flush=True)
        time.sleep(delay)
    raise ReleaseError("registry evidence verification did not complete")


def inspect(reference: str, *, missing_ok: bool = False) -> dict | None:
    command = ["docker", "buildx", "imagetools", "inspect", reference,
               "--format", "{{json .Manifest}}"]
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
    if result.returncode:
        # Authentication, rate limits, and transport failures must never mean 'tag absent'.
        message = result.stderr.strip()
        absent = message in {f"{reference}: not found", f"ERROR: {reference}: not found"}
        if missing_ok and (absent or re.search(r"(?i)\bmanifest unknown\b", message)):
            return None
        raise ReleaseError(f"cannot inspect {reference}: {result.stderr.strip()}")
    manifest = json.loads(result.stdout)
    digest = manifest["digest"]
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ReleaseError(f"invalid registry digest for {reference}")
    immutable = f"{reference.split('@')[0].rsplit(':', 1)[0]}@{digest}"
    config = json.loads(run(["docker", "buildx", "imagetools", "inspect", immutable,
                            "--format", "{{json .Image}}"], capture=True))
    if config.get("os") != "linux" or config.get("architecture") != "amd64":
        raise ReleaseError(f"{reference} is not a single linux/amd64 image")
    return {"digest": digest, "labels": config.get("config", {}).get("Labels", {})}


def labels(version: str, commit: str, name: str) -> dict[str, str]:
    return {"org.opencontainers.image.source": SOURCE,
            "org.opencontainers.image.version": version,
            "org.opencontainers.image.revision": commit,
            "io.rocketmq.release.component": name}


def check_identity(state: dict, version: str, commit: str, name: str) -> None:
    if any(state["labels"].get(key) != value for key, value in labels(version, commit, name).items()):
        raise ReleaseError(f"existing {name} {version} belongs to a different release source")


def existing_release(repository: str, version: str, commit: str, name: str) -> dict | None:
    # Older publishers may have completed only the commit alias. Reuse and validate it
    # during migration, but promotion never creates a commit alias again.
    states = [inspect(f"{repository}:{tag}", missing_ok=True)
              for tag in (version, f"{version}-{commit[:12]}")]
    for state in states:
        if state:
            check_identity(state, version, commit, name)
    present = [state for state in states if state]
    if len(present) == 2 and present[0]["digest"] != present[1]["digest"]:
        raise ReleaseError(f"existing version and legacy commit tags disagree for {name}")
    return present[0] if present else None


def build(image: dict, local: str, version: str, commit: str, platform: str) -> None:
    command = ["docker", "buildx", "build", "--load", "--platform", platform,
               "--provenance=false", "--sbom=false", "--file", image["dockerfile"], "--tag", local,
               "--build-arg", f"SOURCE_REVISION={commit}", "--build-arg", f"SOURCE_VERSION={version}"]
    if image.get("target"):
        command.extend(["--target", image["target"]])
    arguments = dict(image.get("build_args", {}))
    for key, value in arguments.items():
        command.extend(["--build-arg", f"{key}={value}"])
    for key, value in labels(version, commit, image["name"]).items():
        command.extend(["--label", f"{key}={value}"])
    command.append(".")
    run(command)


def qualify(reference: str, prefix: Path, *, remote: bool) -> Path:
    sbom = prefix.with_suffix(".cdx.json")
    run(["syft", f"{'registry' if remote else 'docker'}:{reference}",
         "--output", f"cyclonedx-json={sbom}"])
    run(["trivy", "image", "--no-progress", "--skip-version-check",
         "--image-src", "remote" if remote else "docker", "--scanners", "vuln",
         "--severity", "CRITICAL", "--exit-code", "1", "--format", "json",
         "--output", str(prefix.with_suffix(".trivy.json")), reference])
    return sbom


def sign(reference: str, sbom: Path, commit: str, name: str, prefix: Path) -> None:
    annotations = ["--annotations", f"source_commit={commit}", "--annotations", f"component={name}"]
    upload_cosign_evidence(["cosign", "sign", "--yes", "--bundle",
                           str(prefix.with_suffix(".signature-bundle.json")), *annotations, reference])
    verification = ["--certificate-identity-regexp", IDENTITY, "--certificate-oidc-issuer", ISSUER]
    signature = verify_registry_evidence(["cosign", "verify", *verification, *annotations, reference])
    prefix.with_suffix(".signature.json").write_text(signature, encoding="utf-8")
    upload_cosign_evidence(["cosign", "attest", "--yes", "--bundle",
                           str(prefix.with_suffix(".attestation-bundle.json")),
                           "--type", "cyclonedx", "--predicate", str(sbom), reference])
    expected = json.loads(sbom.read_text(encoding="utf-8"))
    digest = reference.rsplit("@sha256:", 1)[1]

    def validate_sbom(attestation: str) -> None:
        for line in attestation.splitlines():
            if not line.strip():
                continue
            envelope = json.loads(line)
            encoded = envelope["payload"]
            statement = json.loads(base64.b64decode(encoded + "=" * (-len(encoded) % 4), validate=True))
            if (statement.get("predicate") == expected
                    and statement.get("predicateType") == "https://cyclonedx.org/bom"
                    and any(subject.get("digest", {}).get("sha256") == digest
                            for subject in statement.get("subject", []))):
                return
        raise ReleaseError(f"verified attestation does not bind the scanned SBOM to {name}")

    attestation = verify_registry_evidence(["cosign", "verify-attestation", *verification,
                                          "--type", "cyclonedx", reference], validate=validate_sbom)
    prefix.with_suffix(".attestation.jsonl").write_text(attestation, encoding="utf-8")


def promote(repository: str, digest: str, version: str, commit: str, name: str) -> None:
    reference = f"{repository}@{digest}"
    alias = f"{repository}:{version}"
    state = inspect(alias, missing_ok=True)
    if state:
        check_identity(state, version, commit, name)
        if state["digest"] != digest:
            raise ReleaseError(f"refusing to overwrite {alias} with a different digest")
    else:
        run(["docker", "buildx", "imagetools", "create", "--prefer-index=false", "--tag", alias, reference])
    if inspect(alias)["digest"] != digest:
        raise ReleaseError(f"digest changed while promoting {alias}")


def cleanup_aliases(client: DockerHubTags, namespace: str, image: dict, version: str,
                    commit: str, digest: str) -> list[str]:
    repository = f"docker.io/{namespace}/{image['repository']}"
    stable = inspect(f"{repository}:{version}")
    check_identity(stable, version, commit, image["name"])
    if stable["digest"] != digest:
        raise ReleaseError("stable digest changed before tag cleanup")
    candidates = [tag for tag in client.tags(namespace, image["repository"])
                  if tag["name"] == f"{version}-{commit[:12]}"
                  or re.fullmatch(rf"staging-{commit[:12]}-(?:[1-9][0-9]*|local)-[1-9][0-9]*", tag["name"])]
    # Validate every candidate before deleting one; preserve other releases and signature referrers.
    for tag in candidates:
        if tag.get("digest") != digest:
            raise ReleaseError(f"refusing to delete conflicting alias {repository}:{tag['name']}")
    removed = []
    for tag in candidates:
        client.delete(namespace, image["repository"], tag["name"])
        removed.append(tag["name"])
    if inspect(f"{repository}:{version}")["digest"] != digest:
        raise ReleaseError("stable digest changed after tag cleanup")
    return removed


def cleanup_run_tags(group: str, commit: str, namespace: str, staging_run: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", commit) or not re.fullmatch(r"[1-9][0-9]*-[1-9][0-9]*", staging_run):
        raise ReleaseError("cleanup requires an exact source commit and positive run ID/attempt")
    policy = json.loads((ROOT / "docker/release-images.json").read_text(encoding="utf-8"))
    images = policy["groups"][group]
    client = DockerHubTags()
    tag = f"staging-{commit[:12]}-{staging_run}"
    for image in images:
        client.delete(namespace, image["repository"], tag)


def execute(group: str, version: str, commit: str, namespace: str, publish: bool, output: Path,
            *, staging_run: str | None = None) -> dict:
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ReleaseError("version must be stable semver")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ReleaseError("source commit must be a full lowercase commit SHA")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", namespace):
        raise ReleaseError("Docker Hub namespace must be a lowercase user or organization name")
    if staging_run and not re.fullmatch(r"[1-9][0-9]*-[1-9][0-9]*", staging_run):
        raise ReleaseError("staging run must be a positive GitHub run ID and attempt separated by a hyphen")
    actual = run(["git", "rev-parse", "HEAD"], capture=True).strip()
    workspace = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    if actual != commit or workspace["workspace"]["package"]["version"] != version:
        raise ReleaseError("checkout does not match the requested release source/version")
    policy = json.loads((ROOT / "docker/release-images.json").read_text(encoding="utf-8"))
    images = policy["groups"][group]
    client = DockerHubTags() if publish else None
    if publish:
        # Check tag deletion rights before building/pushing; an absent owned run tag is
        # idempotent. This also removes remnants when the same helper step is retried.
        owned_tag = f"staging-{commit[:12]}-{os.environ.get('GITHUB_RUN_ID', 'local')}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
        for image in images:
            client.delete(namespace, image["repository"], owned_tag)
    output.mkdir(parents=True, exist_ok=True)
    result = {"version": version, "source_commit": commit,
              "tooling_commit": os.environ.get("RELEASE_TOOLING_COMMIT"), "group": group,
              "dry_run": not publish, "expected_components": [image["name"] for image in images],
              "complete": False, "images": []}
    (output / "publication.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    plan = []
    # Check every existing alias before building or pushing any member of this group.
    for image in images:
        repository = f"docker.io/{namespace}/{image['repository']}"
        state = existing_release(repository, version, commit, image["name"]) if publish else None
        if publish and state is None and staging_run:
            staging = f"{repository}:staging-{commit[:12]}-{staging_run}"
            state = inspect(staging, missing_ok=True)
            if state:
                check_identity(state, version, commit, image["name"])
        plan.append((image, repository, state))
    qualified = []
    for image, repository, state in plan:
        name = image["name"]
        prefix = output / name
        local = f"rocketmq-release/{name}:{version}-{commit[:12]}"
        if state:
            reference = f"{repository}@{state['digest']}"
            sbom = qualify(reference, prefix, remote=True)
        else:
            build(image, local, version, commit, policy["platform"])
            sbom = qualify(local, prefix, remote=False)
            reference = local
        qualified.append((image, repository, state, reference, sbom))
    # All members passed scans before the first registry write.
    for image, repository, state, reference, sbom in qualified:
        name = image["name"]
        record = {"component": name, "repository": repository, "checks": "passed"}
        if publish:
            temporary = None
            try:
                if not state:
                    temporary = owned_tag
                    staging = f"{repository}:{temporary}"
                    run(["docker", "tag", reference, staging])
                    run(["docker", "push", staging])
                    state = inspect(staging)
                    check_identity(state, version, commit, name)
                    reference = f"{repository}@{state['digest']}"
                    sbom = qualify(reference, output / f"{name}-registry", remote=True)
                sign(reference, sbom, commit, name, output / name)
                promote(repository, state["digest"], version, commit, name)
            except (ReleaseError, OSError, ValueError, subprocess.CalledProcessError) as error:
                if temporary:
                    try:
                        client.delete(namespace, image["repository"], temporary)
                    except HubError as cleanup_error:
                        raise ReleaseError(f"{error}; temporary tag cleanup also failed: {cleanup_error}") from error
                raise
            if temporary:
                client.delete(namespace, image["repository"], temporary)
            removed = ([temporary] if temporary else []) + cleanup_aliases(
                client, namespace, image, version, commit, state["digest"],
            )
            record.update(digest=state["digest"], tags=[version], published=True, removed_tags=removed)
        result["images"].append(record)
        (output / "publication.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    result["complete"] = True
    (output / "publication.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--publish", action="store_true", help="push/sign after checks (default: local dry-run)")
    parser.add_argument("--staging-run", help="reuse staged images from this failed GitHub run ID and attempt")
    parser.add_argument("--cleanup-staging-only", action="store_true", help="remove only this exact run's staging tags")
    parser.add_argument("--output", type=Path, default=ROOT / "target/release/images")
    args = parser.parse_args()
    try:
        if args.cleanup_staging_only:
            if args.publish or not args.staging_run:
                raise ReleaseError("staging cleanup requires --staging-run and cannot publish")
            cleanup_run_tags(args.group, args.source_commit, args.namespace, args.staging_run)
            print(f"STAGING_CLEANUP_OK group={args.group} run={args.staging_run}")
            return 0
        result = execute(args.group, args.version, args.source_commit, args.namespace, args.publish, args.output,
                         staging_run=args.staging_run)
    except (ReleaseError, HubError, OSError, KeyError, ValueError, subprocess.CalledProcessError) as error:
        print(f"IMAGE_RELEASE_FAILED: {error}", file=sys.stderr)
        return 1
    print(f"IMAGE_RELEASE_OK group={args.group} dry_run={result['dry_run']} images={len(result['images'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

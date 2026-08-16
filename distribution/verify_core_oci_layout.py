#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Semantically verify local core OCI layouts without remote registry access."""

from __future__ import annotations

import argparse
import json
from pathlib import Path, PurePosixPath
import stat
import subprocess
import sys
import tarfile
import tempfile


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from build_core_oci_layout import LINUX_TARGET, SERVICES
from release_archive_common import (
    ArchiveError,
    add_unique_record,
    artifact_id,
    candidate_relative,
    draft_partial_path,
    load_candidate,
    load_layout,
    read_policy_json,
    resolve_candidate_path,
    save_draft,
    write_json,
)
from release_state import read_json, resolve_existing_file


def _blob(layout: Path, digest: str) -> Path:
    algorithm, separator, value = digest.partition(":")
    if algorithm != "sha256" or not separator or len(value) != 64:
        raise ArchiveError(f"OCI descriptor is malformed: {digest}")
    return resolve_existing_file(layout / "blobs" / "sha256" / value, "OCI blob")


def _safe_member(member: tarfile.TarInfo) -> PurePosixPath:
    path = PurePosixPath(member.name)
    if path.is_absolute() or ".." in path.parts or member.issym() or member.islnk():
        raise ArchiveError(f"OCI layer contains an unsafe member: {member.name}")
    return path


def verify_layouts(candidate_manifest: Path, *, smoke: bool) -> Path:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    policy = read_json(ROOT / "docker" / "core-container-policy.json")
    release_layout = load_layout()
    binary_specs = {entry["id"]: entry for entry in release_layout["binaries"]}
    results = []
    for service in SERVICES:
        layout = resolve_candidate_path(root, f"oci-layout/{service}", f"{service} OCI layout")
        if not layout.is_dir():
            raise ArchiveError(f"OCI layout is missing: {service}")
        candidate_record = read_json(layout / "OCI_CANDIDATE_MANIFEST.json")
        expected_identity = (
            candidate["candidate_id"],
            candidate["version"],
            candidate["run_id"],
            candidate["attempt"],
            service,
            "not-executed",
        )
        actual_identity = (
            candidate_record.get("candidate_id"),
            candidate_record.get("version"),
            candidate_record.get("run_id"),
            candidate_record.get("attempt"),
            candidate_record.get("service"),
            candidate_record.get("remote_publication"),
        )
        if actual_identity != expected_identity:
            raise ArchiveError(f"OCI candidate identity mismatch: {service}")
        index = read_json(layout / "index.json")
        if len(index.get("manifests", [])) != 1:
            raise ArchiveError(f"OCI index must select exactly one manifest: {service}")
        manifest = json.loads(_blob(layout, index["manifests"][0]["digest"]).read_bytes())
        config = json.loads(_blob(layout, manifest["config"]["digest"]).read_bytes())
        if len(manifest.get("layers", [])) != 1:
            raise ArchiveError(f"OCI image must contain exactly one layer: {service}")
        labels = config.get("config", {}).get("Labels", {})
        for label in policy["required_labels"]:
            if not labels.get(label):
                raise ArchiveError(f"OCI image label is missing for {service}: {label}")
        spec = binary_specs[service]
        archive_binary = spec.get("archive_binary", spec["binary"])
        expected_labels = {
            "org.opencontainers.image.version": candidate["version"],
            "io.rocketmq.service": service,
            "io.rocketmq.build.run-id": candidate["run_id"],
            "io.rocketmq.build.artifact-id": artifact_id(candidate, LINUX_TARGET, service),
            "io.rocketmq.build.requested-features": ",".join(spec["requested_features"]),
            "io.rocketmq.build.effective-features": ",".join(spec["effective_features"]),
            "io.rocketmq.remote-publication": "not-executed",
        }
        if any(labels.get(key) != value for key, value in expected_labels.items()):
            raise ArchiveError(f"OCI image metadata differs from the Linux archive: {service}")
        runtime = config["config"]
        if runtime.get("User") != policy["runtime"]["user"]:
            raise ArchiveError(f"OCI service is not configured as non-root: {service}")
        expected_entrypoint = [f"/usr/local/bin/{archive_binary}"]
        if runtime.get("Entrypoint") != expected_entrypoint:
            raise ArchiveError(f"OCI entrypoint is incorrect: {service}")
        if runtime.get("Cmd") != ["-c", "/etc/rocketmq/service.toml"]:
            raise ArchiveError(f"OCI configuration mount contract is incorrect: {service}")
        layer = _blob(layout, manifest["layers"][0]["digest"])
        expected_members = {
            f"usr/local/bin/{archive_binary}",
            "etc/rocketmq/service.toml",
            "LICENSE-APACHE",
            "NOTICE",
        }
        with tarfile.open(layer, "r") as archive:
            members = archive.getmembers()
            names = {_safe_member(member).as_posix() for member in members}
            if names != expected_members:
                raise ArchiveError(f"OCI layer contents changed: {service}")
            binary_member = archive.getmember(f"usr/local/bin/{archive_binary}")
            source = archive.extractfile(binary_member)
            if source is None:
                raise ArchiveError(f"OCI binary is unreadable: {service}")
            binary_bytes = source.read()
        stdout = ""
        if smoke:
            with tempfile.TemporaryDirectory() as temporary:
                binary = Path(temporary) / archive_binary
                binary.write_bytes(binary_bytes)
                binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
                completed = subprocess.run(
                    [str(binary), "--version", "--verbose"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if completed.returncode != 0:
                    raise ArchiveError(f"OCI binary smoke failed: {service}: {completed.stderr}")
                required = [
                    f"version={candidate['version']}",
                    f"artifact_id={expected_labels['io.rocketmq.build.artifact-id']}",
                    f"requested_features={expected_labels['io.rocketmq.build.requested-features']}",
                    f"effective_features={expected_labels['io.rocketmq.build.effective-features']}",
                ]
                if any(value not in completed.stdout for value in required):
                    raise ArchiveError(f"OCI binary version metadata mismatch: {service}")
                stdout = completed.stdout
        results.append({"service": service, "status": "passed", "stdout": stdout})
    output = root / "evidence" / LINUX_TARGET / "CORE_OCI_SMOKE.json"
    write_json(
        output,
        {
            "schema_version": 1,
            "candidate_id": candidate["candidate_id"],
            "target": LINUX_TARGET,
            "results": results,
            "status": "passed",
            "remote_publication": "not-executed",
        },
    )
    partial = read_policy_json(draft_partial_path(root, LINUX_TARGET), "Linux candidate partial draft")
    add_unique_record(
        partial,
        "artifacts",
        {
            "id": "oci-smoke",
            "kind": "oci-smoke",
            "path": candidate_relative(root, output, "OCI smoke evidence"),
        },
    )
    save_draft(root, LINUX_TARGET, partial)
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--target", default=LINUX_TARGET, choices=[LINUX_TARGET])
    parser.add_argument("--smoke", action="store_true", required=True)
    args = parser.parse_args(argv)
    try:
        output = verify_layouts(args.candidate_manifest, smoke=args.smoke)
        print(f"CORE_OCI_VERIFY_OK output={output} remote_publication=not-executed")
        return 0
    except (ArchiveError, OSError, KeyError, json.JSONDecodeError, tarfile.TarError) as error:
        print(f"CORE_OCI_VERIFY_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

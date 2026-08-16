#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Build local OCI image layouts from already staged Linux service binaries."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import (
    ArchiveError,
    add_unique_record,
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


LINUX_TARGET = "x86_64-unknown-linux-gnu"
SERVICES = ("namesrv", "broker", "controller", "proxy")


def _blob(layout: Path, content: bytes) -> tuple[str, int]:
    # OCI requires digest-addressed blob names. The descriptor is protocol
    # structure only; candidate acceptance is based on semantic verification.
    digest = hashlib.sha256(content).hexdigest()
    path = layout / "blobs" / "sha256" / digest
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return f"sha256:{digest}", len(content)


def _layer(binary_name: str, binary: bytes, config: bytes) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for name, content, mode in (
            (f"usr/local/bin/{binary_name}", binary, 0o555),
            ("etc/rocketmq/service.toml", config, 0o444),
            ("LICENSE-APACHE", (ROOT / "LICENSE-APACHE").read_bytes(), 0o444),
            ("NOTICE", (ROOT / "NOTICE").read_bytes(), 0o444),
        ):
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = mode
            info.mtime = 0
            archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def _archive_manifest(root: Path, version: str) -> dict:
    expected = root / "archives" / f"rocketmq-rust-{version}-{LINUX_TARGET}.manifest.json"
    value = read_json(resolve_existing_file(expected, "Linux release archive manifest"))
    if value.get("target") != LINUX_TARGET or value.get("version") != version:
        raise ArchiveError("Linux archive manifest identity is inconsistent")
    return value


def build_layouts(candidate_manifest: Path) -> list[Path]:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    policy = read_json(ROOT / "docker" / "core-container-policy.json")
    release_layout = load_layout()
    archive_manifest = _archive_manifest(root, candidate["version"])
    archive_records = {entry["component"]: entry for entry in archive_manifest["binaries"]}
    binary_specs = {entry["id"]: entry for entry in release_layout["binaries"]}
    partial = read_policy_json(draft_partial_path(root, LINUX_TARGET), "Linux candidate partial draft")
    outputs: list[Path] = []
    for service in SERVICES:
        spec = binary_specs[service]
        record = archive_records.get(service)
        if record is None:
            raise ArchiveError(f"Linux archive has no service binary: {service}")
        binary = resolve_candidate_path(root, record["path"], f"{service} binary")
        config = root / "staging" / LINUX_TARGET / f"rocketmq-rust-{candidate['version']}" / "conf" / f"{service}.toml"
        config = resolve_existing_file(config, f"{service} staged config")
        output = root / "oci-layout" / service
        if output.exists():
            raise ArchiveError(f"OCI layout already exists: {output}")
        output.mkdir(parents=True)
        archive_binary = spec.get("archive_binary", spec["binary"])
        layer_digest, layer_size = _blob(output, _layer(archive_binary, binary.read_bytes(), config.read_bytes()))
        labels = {
            "org.opencontainers.image.version": candidate["version"],
            "org.opencontainers.image.source": "https://github.com/mxsm/rocketmq-rust",
            "org.opencontainers.image.created": candidate["created_at"],
            "io.rocketmq.service": service,
            "io.rocketmq.build.run-id": candidate["run_id"],
            "io.rocketmq.build.artifact-id": record["artifact_id"],
            "io.rocketmq.build.requested-features": ",".join(record["requested_features"]),
            "io.rocketmq.build.effective-features": ",".join(record["effective_features"]),
            "io.rocketmq.remote-publication": "not-executed",
        }
        config_value = {
            "architecture": "amd64",
            "os": "linux",
            "config": {
                "User": policy["runtime"]["user"],
                "Entrypoint": [f"/usr/local/bin/{archive_binary}"],
                "Cmd": ["-c", "/etc/rocketmq/service.toml"],
                "WorkingDir": policy["runtime"]["work_root"],
                "Labels": labels,
                "Healthcheck": {"Test": ["CMD", f"/usr/local/bin/{archive_binary}", "--version"]},
            },
            "rootfs": {"type": "layers", "diff_ids": [layer_digest]},
            "history": [{"created": candidate["created_at"], "created_by": "local candidate layout"}],
        }
        config_bytes = json.dumps(config_value, separators=(",", ":"), sort_keys=True).encode()
        config_digest, config_size = _blob(output, config_bytes)
        manifest_value = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest,
                "size": config_size,
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar",
                    "digest": layer_digest,
                    "size": layer_size,
                }
            ],
            "annotations": {"org.opencontainers.image.ref.name": f"{candidate['version']}-{candidate['run_id']}"},
        }
        manifest_bytes = json.dumps(manifest_value, separators=(",", ":"), sort_keys=True).encode()
        manifest_digest, manifest_size = _blob(output, manifest_bytes)
        write_json(output / "oci-layout", {"imageLayoutVersion": "1.0.0"})
        write_json(
            output / "index.json",
            {
                "schemaVersion": 2,
                "manifests": [
                    {
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "digest": manifest_digest,
                        "size": manifest_size,
                        "annotations": {"org.opencontainers.image.ref.name": f"{candidate['version']}-{candidate['run_id']}"},
                    }
                ],
            },
        )
        candidate_record = {
            "schema_version": 1,
            "candidate_id": candidate["candidate_id"],
            "version": candidate["version"],
            "run_id": candidate["run_id"],
            "attempt": candidate["attempt"],
            "target": LINUX_TARGET,
            "service": service,
            "artifact_id": record["artifact_id"],
            "layout": candidate_relative(root, output, "OCI layout"),
            "entrypoint": config_value["config"]["Entrypoint"],
            "labels": labels,
            "remote_publication": "not-executed",
        }
        write_json(output / "OCI_CANDIDATE_MANIFEST.json", candidate_record)
        add_unique_record(
            partial,
            "artifacts",
            {"id": f"oci-{service}", "kind": "oci-layout", "component": service, "path": candidate_record["layout"]},
        )
        add_unique_record(
            partial,
            "artifacts",
            {
                "id": f"oci-manifest-{service}",
                "kind": "oci-manifest",
                "component": service,
                "path": candidate_relative(root, output / "OCI_CANDIDATE_MANIFEST.json", "OCI candidate manifest"),
            },
        )
        outputs.append(output)
    save_draft(root, LINUX_TARGET, partial)
    return outputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--target", default=LINUX_TARGET, choices=[LINUX_TARGET])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.dry_run:
            _manifest, candidate, _root = load_candidate(args.candidate_manifest)
            print(f"CORE_OCI_DRY_RUN candidate={candidate['candidate_id']} services={','.join(SERVICES)}")
            return 0
        outputs = build_layouts(args.candidate_manifest)
        print(f"CORE_OCI_LAYOUT_OK services={len(outputs)} remote_publication=not-executed")
        return 0
    except (ArchiveError, OSError, KeyError, json.JSONDecodeError) as error:
        print(f"CORE_OCI_LAYOUT_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

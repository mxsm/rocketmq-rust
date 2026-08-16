# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import io
from pathlib import Path
import tarfile
from typing import Any
import zipfile

from scripts.tests.release_archive_test_support import create_candidate
from scripts.tests.release_test_support import ROOT, read_json, write_json


def _tar_member(archive: tarfile.TarFile, name: str, content: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(content)
    info.mode = 0o644
    info.mtime = 0
    archive.addfile(info, io.BytesIO(content))


def seed_complete_candidate(base: Path) -> tuple[Path, dict[str, Any]]:
    candidate = create_candidate(base)
    root = candidate.parent
    candidate_value = read_json(candidate)
    scope = read_json(ROOT / "scripts" / "core-release-scope.json")
    packages = [
        entry["name"]
        for entry in scope["core_packages"]
        if entry["classification"] == "registry-publish"
    ]
    if len(packages) != 24:
        raise AssertionError("fixture requires the frozen 24-package denominator")
    staged = []
    for name in packages:
        crate = root / "crate-packages" / f"{name}-1.0.0.crate"
        crate.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(crate, "w:gz") as archive:
            package_root = f"{name}-1.0.0"
            _tar_member(archive, f"{package_root}/Cargo.toml", f'[package]\nname = "{name}"\nversion = "1.0.0"\n'.encode())
            _tar_member(archive, f"{package_root}/LICENSE-APACHE", b"Apache License\n")
            _tar_member(archive, f"{package_root}/NOTICE", b"RocketMQ Rust\n")
        staged.append(
            {
                "name": name,
                "version": "1.0.0",
                "crate_path": crate.relative_to(root).as_posix(),
                "legal_files": ["LICENSE-APACHE", "NOTICE"],
                "status": "packaged-locally",
            }
        )
    write_json(
        root / "PACKAGE_PLAN.json",
        {
            "schema_version": 1,
            "candidate_id": candidate_value["candidate_id"],
            "version": "1.0.0",
            "run_id": candidate_value["run_id"],
            "attempt": candidate_value["attempt"],
            "mode": "package-only",
            "registry_publish_count": 24,
            "staged_packages": staged,
            "remote_publication": {"status": "not-executed"},
        },
    )
    release_layout = read_json(ROOT / "distribution" / "release-layout.json")
    binary_records = [
        {
            "component": entry["id"],
            "requested_features": entry["requested_features"],
            "effective_features": entry["effective_features"],
        }
        for entry in release_layout["binaries"]
    ]
    for target, target_spec in release_layout["targets"].items():
        extension = ".zip" if target_spec["archive_format"] == "zip" else ".tar.gz"
        archive_path = root / "archives" / f"rocketmq-rust-1.0.0-{target}{extension}"
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        package_root = "rocketmq-rust-1.0.0"
        if extension == ".zip":
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr(f"{package_root}/LICENSE-APACHE", "Apache License\n")
                archive.writestr(f"{package_root}/NOTICE", "RocketMQ Rust\n")
        else:
            with tarfile.open(archive_path, "w:gz") as archive:
                _tar_member(archive, f"{package_root}/LICENSE-APACHE", b"Apache License\n")
                _tar_member(archive, f"{package_root}/NOTICE", b"RocketMQ Rust\n")
        write_json(
            root / "archives" / f"rocketmq-rust-1.0.0-{target}.manifest.json",
            {
                "schema_version": 1,
                "candidate_id": candidate_value["candidate_id"],
                "version": "1.0.0",
                "run_id": candidate_value["run_id"],
                "attempt": candidate_value["attempt"],
                "target": target,
                "archive": archive_path.relative_to(root).as_posix(),
                "binaries": binary_records,
                "remote_publication": "not-executed",
            },
        )
    for service in ("namesrv", "broker", "controller", "proxy"):
        layout = root / "oci-layout" / service
        blobs = layout / "blobs" / "sha256"
        blobs.mkdir(parents=True, exist_ok=True)
        manifest_name = "a" * 64
        layer_name = "b" * 64
        write_json(
            layout / "index.json",
            {"schemaVersion": 2, "manifests": [{"digest": f"sha256:{manifest_name}"}]},
        )
        write_json(
            blobs / manifest_name,
            {"schemaVersion": 2, "layers": [{"digest": f"sha256:{layer_name}"}]},
        )
        with tarfile.open(blobs / layer_name, "w") as archive:
            _tar_member(archive, "LICENSE-APACHE", b"Apache License\n")
            _tar_member(archive, "NOTICE", b"RocketMQ Rust\n")
        write_json(
            layout / "OCI_CANDIDATE_MANIFEST.json",
            {
                "schema_version": 1,
                "candidate_id": candidate_value["candidate_id"],
                "version": "1.0.0",
                "run_id": candidate_value["run_id"],
                "attempt": candidate_value["attempt"],
                "target": "x86_64-unknown-linux-gnu",
                "service": service,
                "artifact_id": f"{candidate_value['candidate_id']}.x86_64-unknown-linux-gnu.{service}",
                "remote_publication": "not-executed",
            },
        )
    write_json(
        root / "ARTIFACT_INDEX.json",
        {
            "schema_version": 1,
            "candidate_id": candidate_value["candidate_id"],
            "version": "1.0.0",
            "run_id": candidate_value["run_id"],
            "attempt": candidate_value["attempt"],
            "artifacts": [],
            "remote_publication": "not-executed",
        },
    )
    write_json(root / "events" / "prepare.completed.json", {"route_id": "R06-prepare", "worker_id": "aggregate", "exit_code": 0, "status": "passed"})
    write_json(root / "contexts" / "aggregate.json", {"worker_id": "aggregate", "executor": "local"})
    metadata_packages = []
    metadata_nodes = []
    shared_id = "registry+fixture#shared@1.0.0"
    transitive_id = "registry+fixture#transitive@1.0.0"
    metadata_packages.append({"id": shared_id, "name": "shared", "version": "1.0.0", "license": "Apache-2.0"})
    metadata_packages.append(
        {"id": transitive_id, "name": "transitive", "version": "1.0.0", "license": "Apache-2.0"}
    )
    metadata_nodes.append({"id": shared_id, "deps": [{"pkg": transitive_id}]})
    metadata_nodes.append({"id": transitive_id, "deps": []})
    for name in packages:
        package_id = f"path+fixture#{name}@1.0.0"
        metadata_packages.append({"id": package_id, "name": name, "version": "1.0.0", "license": "Apache-2.0"})
        metadata_nodes.append({"id": package_id, "deps": [{"pkg": shared_id}]})
    metadata = {"packages": metadata_packages, "resolve": {"nodes": metadata_nodes}}
    return candidate, metadata

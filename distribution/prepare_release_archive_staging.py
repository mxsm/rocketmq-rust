#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import stat
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import (
    ArchiveError,
    add_unique_record,
    candidate_relative,
    draft_partial_path,
    file_inventory,
    load_candidate,
    load_layout,
    read_policy_json,
    resolve_candidate_path,
    save_draft,
    target_layout,
)
from release_state import read_json, resolve_existing_file, resolve_within


def _copy_archive_config(service: str, source: Path, destination: Path) -> None:
    value = source.read_text(encoding="utf-8")
    replacements = {
        "${user.home}/rocketmq": "./work/namesrv/rocketmq",
        "${user.home}/namesrv": "./work/namesrv",
        "/opt/data/rocketmq/store": "./work/broker/store",
    }
    for old, new in replacements.items():
        value = value.replace(old, new)
    if "${user.home}" in value or "/opt/data" in value:
        raise ArchiveError(f"{service} config retains a development-machine path")
    destination.write_text(value, encoding="utf-8", newline="\n")


def prepare_staging(candidate_manifest: Path, target: str, common_inputs: Path) -> Path:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    layout = load_layout()
    target_spec = target_layout(layout, target)
    partial = read_policy_json(draft_partial_path(root, target), "candidate partial draft")
    common_inputs = resolve_within(root, common_inputs, "common release inputs")
    common_manifest = read_json(common_inputs / "COMMON_RELEASE_INPUTS.json")
    actual_inventory = [
        entry for entry in file_inventory(common_inputs) if entry["path"] != "COMMON_RELEASE_INPUTS.json"
    ]
    if actual_inventory != common_manifest["files"]:
        raise ArchiveError("common release inputs changed after their manifest was written")
    package_root = root / "staging" / target / f"rocketmq-rust-{candidate['version']}"
    if package_root.exists():
        raise ArchiveError(f"archive staging already exists: {package_root}")
    for directory in layout["archive_directories"]:
        (package_root / directory).mkdir(parents=True, exist_ok=True)
    for name in layout["common_files"]:
        shutil.copyfile(resolve_existing_file(common_inputs / name, name), package_root / name)
    suffix = target_spec["executable_suffix"]
    binary_records = {
        entry["component"]: entry
        for entry in partial["artifacts"]
        if entry.get("kind") == "binary"
    }
    for binary in layout["binaries"]:
        record = binary_records.get(binary["id"])
        if record is None:
            raise ArchiveError(f"candidate partial has no binary: {binary['id']}")
        source = resolve_candidate_path(root, record["path"], "release binary")
        archive_name = binary.get("archive_binary", binary["binary"])
        destination = package_root / "bin" / f"{archive_name}{suffix}"
        shutil.copyfile(source, destination)
        destination.chmod(destination.stat().st_mode | stat.S_IXUSR)
    for service, source_value in layout["configs"].items():
        source = resolve_existing_file(ROOT / source_value, f"{service} config")
        _copy_archive_config(service, source, package_root / "conf" / f"{service}.toml")
    for script in ("rocketmq-service.ps1", "rocketmq-service.sh"):
        source = resolve_existing_file(ROOT / "distribution" / "scripts" / script, script)
        destination = package_root / "scripts" / script
        shutil.copyfile(source, destination)
        destination.chmod(destination.stat().st_mode | stat.S_IXUSR)
    add_unique_record(
        partial,
        "artifacts",
        {
            "id": "common-inputs",
            "kind": "common-inputs",
            "path": candidate_relative(root, common_inputs, "common release inputs"),
        },
    )
    save_draft(root, target, partial)
    return package_root


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--common-inputs", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        output = prepare_staging(args.candidate_manifest, args.target, args.common_inputs)
        print(f"RELEASE_ARCHIVE_STAGING_OK target={args.target} output={output}")
        return 0
    except (ArchiveError, OSError, KeyError) as error:
        print(f"RELEASE_ARCHIVE_STAGING_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

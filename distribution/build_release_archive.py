#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import (
    ArchiveError,
    add_unique_record,
    artifact_id,
    candidate_relative,
    draft_partial_path,
    file_inventory,
    load_candidate,
    load_layout,
    read_policy_json,
    save_draft,
    target_layout,
    write_json,
)


def build_archive(candidate_manifest: Path, target: str) -> tuple[Path, Path]:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    layout = load_layout()
    target_spec = target_layout(layout, target)
    package_name = f"rocketmq-rust-{candidate['version']}"
    staging = root / "staging" / target / package_name
    required = [
        *(staging / name for name in layout["common_files"]),
        *(staging / "conf" / f"{service}.toml" for service in layout["configs"]),
        staging / "sbom" / "components.cdx.json",
        staging / "scripts" / "rocketmq-service.ps1",
        staging / "scripts" / "rocketmq-service.sh",
    ]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise ArchiveError("archive staging is incomplete: " + ", ".join(missing))
    output_root = root / "archives"
    output_root.mkdir(parents=True, exist_ok=True)
    base = output_root / f"{package_name}-{target}"
    if target_spec["archive_format"] == "zip":
        archive = Path(shutil.make_archive(str(base), "zip", root_dir=staging.parent, base_dir=package_name))
    else:
        archive = Path(
            shutil.make_archive(str(base), "gztar", root_dir=staging.parent, base_dir=package_name)
        )
    manifest_path = output_root / f"{package_name}-{target}.manifest.json"
    binary_records = [
        entry
        for entry in read_policy_json(
            draft_partial_path(root, target), "candidate partial draft"
        )["artifacts"]
        if entry.get("kind") == "binary"
    ]
    value = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "target": target,
        "artifact_id": artifact_id(candidate, target, "archive"),
        "archive": candidate_relative(root, archive, "release archive"),
        "files": file_inventory(staging),
        "binaries": binary_records,
        "remote_publication": "not-executed",
    }
    write_json(manifest_path, value)
    partial = read_policy_json(draft_partial_path(root, target), "candidate partial draft")
    add_unique_record(
        partial,
        "artifacts",
        {
            "id": "archive",
            "kind": "release-archive",
            "artifact_id": value["artifact_id"],
            "path": value["archive"],
        },
    )
    add_unique_record(
        partial,
        "artifacts",
        {
            "id": "archive-manifest",
            "kind": "archive-manifest",
            "path": candidate_relative(root, manifest_path, "archive manifest"),
        },
    )
    save_draft(root, target, partial)
    return archive, manifest_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--target", required=True)
    args = parser.parse_args(argv)
    try:
        archive, manifest = build_archive(args.candidate_manifest, args.target)
        print(f"RELEASE_ARCHIVE_OK target={args.target} archive={archive} manifest={manifest}")
        return 0
    except (ArchiveError, OSError, KeyError) as error:
        print(f"RELEASE_ARCHIVE_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

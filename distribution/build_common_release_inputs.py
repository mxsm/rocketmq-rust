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

from release_archive_common import ArchiveError, file_inventory, load_candidate, write_json
from release_state import resolve_existing_file, resolve_within


SOURCES = {
    "LICENSE-APACHE": ROOT / "LICENSE-APACHE",
    "NOTICE": ROOT / "NOTICE",
    "README.md": ROOT / "README.md",
    "release-identity.json": ROOT / "distribution" / "release-identity.json",
    "core-release-scope.json": ROOT / "scripts" / "core-release-scope.json",
    "release-layout.json": ROOT / "distribution" / "release-layout.json",
}


def build_inputs(candidate_manifest: Path, output: Path) -> Path:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    output = resolve_within(root, output, "common inputs output")
    if output.exists():
        raise ArchiveError(f"common inputs output already exists: {output}")
    notes = resolve_existing_file(
        root / "common-input-source" / "RELEASE_NOTES.md", "candidate release notes"
    )
    output.mkdir(parents=True)
    for name, source in SOURCES.items():
        shutil.copyfile(resolve_existing_file(source, name), output / name)
    shutil.copyfile(notes, output / "RELEASE_NOTES.md")
    inventory = file_inventory(output)
    manifest = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "files": inventory,
        "remote_publication": "not-executed",
    }
    write_json(output / "COMMON_RELEASE_INPUTS.json", manifest)
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        output = build_inputs(args.candidate_manifest, args.output)
        print(f"COMMON_RELEASE_INPUTS_OK output={output}")
        return 0
    except (ArchiveError, OSError) as error:
        print(f"COMMON_RELEASE_INPUTS_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

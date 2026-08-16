#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any


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
    save_draft,
    target_layout,
)


def _metadata() -> dict[str, Any]:
    completed = subprocess.run(
        ["cargo", "metadata", "--locked", "--format-version", "1", "--no-deps"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise ArchiveError(f"cargo metadata failed: {completed.stderr.strip()}")
    return json.loads(completed.stdout)


def generate_sbom(candidate_manifest: Path, target: str, toolchain: Path) -> Path:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    layout = load_layout()
    target_layout(layout, target)
    policy = read_policy_json(toolchain, "SBOM toolchain")
    staging = root / "staging" / target / f"rocketmq-rust-{candidate['version']}"
    if not staging.is_dir():
        raise ArchiveError(f"archive staging is missing: {staging}")
    metadata = _metadata()
    packages = {package["name"]: package for package in metadata["packages"]}
    components = []
    for binary in layout["binaries"]:
        package = packages.get(binary["package"])
        if package is None:
            raise ArchiveError(f"SBOM package is absent from Cargo metadata: {binary['package']}")
        components.append(
            {
                "type": "application" if binary["kind"] == "service" else "library",
                "name": binary["binary"],
                "version": package["version"],
                "properties": [
                    {"name": "rocketmq:package", "value": binary["package"]},
                    {"name": "rocketmq:target", "value": target},
                    {
                        "name": "rocketmq:effective-features",
                        "value": ",".join(binary["effective_features"]),
                    },
                ],
            }
        )
    value = {
        "bomFormat": policy["format"],
        "specVersion": policy["spec_version"],
        "version": 1,
        "metadata": {
            "component": {
                "type": "application",
                "name": "rocketmq-rust-community-distribution",
                "version": candidate["version"],
            },
            "properties": [
                {"name": "rocketmq:candidate-id", "value": candidate["candidate_id"]},
                {"name": "rocketmq:staging-file-count", "value": str(len(file_inventory(staging)))},
            ],
        },
        "components": components,
    }
    output = staging / "sbom" / "components.cdx.json"
    if output.exists():
        raise ArchiveError(f"component SBOM already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8", newline="\n")
    partial = read_policy_json(draft_partial_path(root, target), "candidate partial draft")
    add_unique_record(
        partial,
        "artifacts",
        {
            "id": "component-sbom",
            "kind": "component-sbom",
            "path": candidate_relative(root, output, "component SBOM"),
        },
    )
    save_draft(root, target, partial)
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--toolchain", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        output = generate_sbom(args.candidate_manifest, args.target, args.toolchain)
        print(f"COMPONENT_SBOM_OK target={args.target} output={output}")
        return 0
    except (ArchiveError, OSError, json.JSONDecodeError, KeyError) as error:
        print(f"COMPONENT_SBOM_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

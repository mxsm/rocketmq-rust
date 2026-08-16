#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Generate candidate-scoped local provenance without digest readiness fields."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import (
    ArchiveError,
    candidate_relative,
    load_candidate,
    load_layout,
    write_json,
)
from release_artifact_index import register_artifacts
from release_state import ensure_no_digest_fields, read_json, resolve_existing_file, utc_now


def _event_records(root: Path) -> list[dict]:
    values = []
    for path in sorted((root / "events").rglob("*.completed.json")):
        event = read_json(path)
        values.append(
            {
                "path": candidate_relative(root, path, "execution event"),
                "route_id": event.get("route_id", path.name.removesuffix(".completed.json")),
                "worker_id": event.get("worker_id", "unrecorded"),
                "exit_code": event.get("exit_code"),
                "status": event.get("status", "completed"),
            }
        )
    if not values:
        raise ArchiveError("candidate provenance has no completed execution events")
    return values


def _context_records(root: Path) -> list[dict]:
    values = []
    for path in sorted((root / "contexts").rglob("*.json")):
        context = read_json(path)
        values.append(
            {
                "path": candidate_relative(root, path, "execution context"),
                "worker_id": context.get("worker_id", "unrecorded"),
                "executor": context.get("executor", context.get("execution_mode", "local")),
            }
        )
    if not values:
        raise ArchiveError("candidate provenance has no execution contexts")
    return values


def generate(candidate_manifest: Path) -> Path:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    plan = read_json(resolve_existing_file(root / "PACKAGE_PLAN.json", "package-only report"))
    if plan.get("registry_publish_count") != 24 or len(plan.get("staged_packages", [])) != 24:
        raise ArchiveError("provenance package input denominator is incomplete")
    sbom_index = read_json(resolve_existing_file(root / "sbom" / "SBOM_INDEX.json", "release SBOM index"))
    if len(sbom_index.get("outputs", [])) != 31:
        raise ArchiveError("provenance SBOM input denominator is incomplete")
    artifact_index_path = resolve_existing_file(root / "ARTIFACT_INDEX.json", "candidate artifact index")
    artifact_index = read_json(artifact_index_path)
    if artifact_index.get("candidate_id") != candidate["candidate_id"]:
        raise ArchiveError("provenance artifact index identity mismatch")
    toolchain = read_json(ROOT / "distribution" / "sbom-toolchain.json")
    rust_toolchain = tomllib.loads((ROOT / "rust-toolchain.toml").read_text(encoding="utf-8"))
    outputs = []
    output_roots = ("crate-packages", "archives", "oci-layout", "helm", "sbom")
    for directory in output_roots:
        for path in sorted((root / directory).rglob("*")):
            if path.is_symlink():
                raise ArchiveError(f"provenance output contains a link: {path}")
            if path.is_file():
                relative = candidate_relative(root, path, "provenance output")
                outputs.append(
                    {
                        "id": relative.replace("/", ":"),
                        "kind": directory,
                        "path": relative,
                        "size": path.stat().st_size,
                    }
                )
    outputs.append(
        {
            "id": "artifact-index",
            "kind": "manifest",
            "path": candidate_relative(root, artifact_index_path, "candidate artifact index"),
            "size": artifact_index_path.stat().st_size,
        }
    )
    if not outputs:
        raise ArchiveError("candidate provenance has no registered outputs")
    package_versions = [
        {"name": entry["name"], "version": entry["version"]}
        for entry in plan["staged_packages"]
    ]
    value = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "generated_at": utc_now(),
        "distribution": "unofficial-community",
        "execution_route": "workflow" if os.environ.get("GITHUB_ACTIONS") == "true" else "local",
        "targets": sorted(load_layout()["targets"]),
        "toolchain": {
            "rust": rust_toolchain["toolchain"]["channel"],
            "sbom_generator": toolchain["generator"],
            "sbom_generator_version": toolchain["generator_version"],
        },
        "lockfile_mode": "locked",
        "input_versions": package_versions,
        "execution_events": _event_records(root),
        "execution_contexts": _context_records(root),
        "outputs": outputs,
        "remote_publication": "not-executed",
    }
    ensure_no_digest_fields(value)
    output = root / "provenance" / "RELEASE_PROVENANCE.json"
    if output.exists():
        raise ArchiveError(f"release provenance already exists: {output}")
    write_json(output, value)
    register_artifacts(
        candidate_manifest,
        [{"id": "release-provenance", "kind": "provenance", "path": output}],
    )
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        output = generate(args.candidate_manifest)
        print(f"RELEASE_PROVENANCE_OK output={output} remote_publication=not-executed")
        return 0
    except (ArchiveError, OSError, KeyError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        print(f"RELEASE_PROVENANCE_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

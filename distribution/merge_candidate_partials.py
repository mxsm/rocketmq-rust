#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import (
    ArchiveError,
    load_candidate,
    load_layout,
    require_relative_path,
    resolve_candidate_path,
    write_json,
)
from release_state import read_json


def _bundle_root(partial_path: Path) -> Path:
    for parent in partial_path.parents:
        if (parent / "CANDIDATE_TRANSFER.json").is_file():
            return parent
    if partial_path.parent.name == "partials":
        return partial_path.parent.parent
    raise ArchiveError(f"cannot locate target bundle root for partial: {partial_path}")


def _copy_entry(source_root: Path, candidate_root: Path, relative: str) -> None:
    posix = require_relative_path(relative, "merged bundle path")
    source = source_root.joinpath(*posix.parts)
    destination = resolve_candidate_path(candidate_root, relative, "merged candidate path")
    if not source.exists() or source.is_symlink():
        raise ArchiveError(f"target bundle entry is missing or unsafe: {relative}")
    sources = [path for path in source.rglob("*") if path.is_file()] if source.is_dir() else [source]
    for path in sources:
        if path.is_symlink():
            raise ArchiveError(f"target bundle entry is a link: {path}")
        suffix = path.relative_to(source)
        output = destination / suffix if source.is_dir() else destination
        if output.exists():
            if not output.is_file() or output.read_bytes() != path.read_bytes():
                raise ArchiveError(f"target bundle would overwrite a different file: {relative}")
            continue
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.merge.tmp")
        if temporary.exists():
            raise ArchiveError(f"stale merge temporary file exists: {temporary}")
        shutil.copyfile(path, temporary)
        os.replace(temporary, output)


def merge_partials(candidate_manifest: Path, download_root: Path, targets: list[str]) -> Path:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    layout = load_layout()
    download_root = download_root.resolve()
    partials: list[dict] = []
    for target in targets:
        matches = list(download_root.rglob(f"CANDIDATE_PARTIAL.{target}.json"))
        if len(matches) != 1:
            raise ArchiveError(f"expected one sealed partial for {target}, found {len(matches)}")
        partial_path = matches[0]
        partial = read_json(partial_path)
        identity = (
            partial.get("candidate_id"),
            partial.get("version"),
            partial.get("run_id"),
            partial.get("attempt"),
            partial.get("target"),
            partial.get("sealed"),
        )
        expected = (
            candidate["candidate_id"],
            candidate["version"],
            candidate["run_id"],
            candidate["attempt"],
            target,
            True,
        )
        if identity != expected:
            raise ArchiveError(f"sealed partial identity mismatch: {target}")
        if partial.get("worker_id") != f"release-{target}":
            raise ArchiveError(f"sealed partial worker mismatch: {target}")
        identifiers = {
            entry.get("id") for entry in partial.get("artifacts", []) if isinstance(entry, dict)
        }
        required = {f"binary-{entry['id']}" for entry in layout["binaries"]}
        required.update({"archive", "archive-manifest", "common-inputs", "component-sbom", "host-smoke"})
        if not required.issubset(identifiers):
            raise ArchiveError(f"sealed partial artifact denominator is incomplete: {target}")
        if not partial.get("events") or not partial.get("execution_contexts"):
            raise ArchiveError(f"sealed partial evidence denominator is incomplete: {target}")
        source_root = _bundle_root(partial_path)
        for artifact in partial.get("artifacts", []):
            _copy_entry(source_root, root, artifact.get("path"))
        for event in partial.get("events", []):
            _copy_entry(source_root, root, event.get("started"))
            _copy_entry(source_root, root, event.get("completed"))
            completed = source_root.joinpath(
                *require_relative_path(event.get("completed"), "completed event").parts
            )
            if read_json(completed).get("exit_code") != 0:
                raise ArchiveError(f"sealed partial contains a failed event: {event.get('id')}")
        for context in partial.get("execution_contexts", []):
            _copy_entry(source_root, root, context.get("path"))
            context_path = source_root.joinpath(
                *require_relative_path(context.get("path"), "execution context").parts
            )
            if read_json(context_path).get("worker_id") != partial["worker_id"]:
                raise ArchiveError(f"sealed partial context worker mismatch: {context.get('id')}")
        partials.append(partial)
    artifact_ids: set[str] = set()
    artifacts: list[dict] = []
    event_ids: set[str] = set()
    context_ids: set[str] = set()
    for partial in partials:
        for artifact in partial["artifacts"]:
            identifier = f"{partial['target']}:{artifact['id']}"
            if identifier in artifact_ids:
                raise ArchiveError(f"merged artifact identifier is duplicated: {identifier}")
            artifact_ids.add(identifier)
            require_relative_path(artifact["path"], "merged artifact path")
            artifacts.append({"target": partial["target"], **artifact})
        for event in partial["events"]:
            identifier = f"{partial['target']}:{event['id']}"
            if identifier in event_ids:
                raise ArchiveError(f"merged event identifier is duplicated: {identifier}")
            event_ids.add(identifier)
        for context in partial["execution_contexts"]:
            identifier = f"{partial['target']}:{context['id']}"
            if identifier in context_ids:
                raise ArchiveError(f"merged context identifier is duplicated: {identifier}")
            context_ids.add(identifier)
    value = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "targets": targets,
        "artifacts": artifacts,
        "remote_publication": "not-executed",
    }
    output = root / "ARTIFACT_INDEX.json"
    if output.exists():
        raise ArchiveError(f"candidate artifact index already exists: {output}")
    write_json(output, value)
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--download-root", type=Path, required=True)
    parser.add_argument("--require-targets", required=True)
    args = parser.parse_args(argv)
    try:
        targets = [value for value in args.require_targets.split(",") if value]
        if len(targets) != len(set(targets)):
            raise ArchiveError("required target list is empty or duplicated")
        output = merge_partials(args.candidate_manifest, args.download_root, targets)
        print(f"CANDIDATE_PARTIAL_MERGE_OK targets={len(targets)} output={output}")
        return 0
    except (ArchiveError, OSError) as error:
        print(f"CANDIDATE_PARTIAL_MERGE_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

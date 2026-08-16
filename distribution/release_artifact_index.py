#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Candidate artifact-index updates shared by local release evidence generators."""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import ArchiveError, candidate_relative, load_candidate
from release_state import atomic_write_json, exclusive_lock, read_json, utc_now


def register_artifacts(candidate_manifest: Path, records: list[dict[str, Any]]) -> Path:
    manifest, candidate, root = load_candidate(candidate_manifest)
    index_path = root / "ARTIFACT_INDEX.json"
    with exclusive_lock(root / ".artifact-index.lock"):
        if index_path.exists():
            index = read_json(index_path)
        else:
            index = {
                "schema_version": 1,
                "candidate_id": candidate["candidate_id"],
                "version": candidate["version"],
                "run_id": candidate["run_id"],
                "attempt": candidate["attempt"],
                "artifacts": [],
                "remote_publication": "not-executed",
            }
        if index.get("candidate_id") != candidate["candidate_id"]:
            raise ArchiveError("candidate artifact index identity mismatch")
        artifacts = index.get("artifacts")
        if not isinstance(artifacts, list):
            raise ArchiveError("candidate artifact index has no artifacts list")
        identifiers = {entry.get("id") for entry in artifacts if isinstance(entry, dict)}
        for record in records:
            identifier = record.get("id")
            path = record.get("path")
            if not isinstance(identifier, str) or not isinstance(path, Path) or not path.is_file():
                raise ArchiveError("candidate artifact registration is incomplete")
            if identifier in identifiers:
                raise ArchiveError(f"candidate artifact is already registered: {identifier}")
            artifacts.append(
                {
                    "id": identifier,
                    "kind": record["kind"],
                    "path": candidate_relative(root, path, identifier),
                }
            )
            identifiers.add(identifier)
        atomic_write_json(index_path, index)
        relative_index = candidate_relative(root, index_path, "candidate artifact index")
        if candidate.get("artifact_index") not in (None, relative_index):
            raise ArchiveError("candidate points to a different artifact index")
        candidate["artifact_index"] = relative_index
        candidate["generation"] += 1
        candidate["updated_at"] = utc_now()
        atomic_write_json(manifest, candidate)
    return index_path

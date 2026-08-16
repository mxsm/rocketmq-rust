# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import stat
from typing import Any

from scripts.tests.release_test_support import write_json


def create_candidate(root: Path, *, version: str = "1.0.0") -> Path:
    candidate_root = root / version / "local" / "attempt-1"
    manifest = candidate_root / "CANDIDATE_RUN.json"
    series = root / "RELEASE_SERIES.json"
    write_json(
        series,
        {
            "schema_version": 1,
            "series_id": "community-v1",
            "generation": 1,
            "head_manifest": str(manifest.resolve()),
        },
    )
    write_json(
        manifest,
        {
            "schema_version": 1,
            "candidate_id": f"{version}-runlocal-attempt1-ordinal1",
            "candidate_kind": "rc" if "-rc." in version else "final",
            "version": version,
            "run_id": "local",
            "attempt": 1,
            "ordinal": 1,
            "candidate_root": str(candidate_root.resolve()),
            "series_manifest": str(series.resolve()),
            "series_id": "community-v1",
            "series_generation": 1,
            "parent_manifest": None,
            "state": "development",
            "sealed": False,
            "outcome": None,
            "rejection_reason": None,
            "known_issues": [],
            "generation": 0,
            "build_source_bundle": None,
            "source_snapshot": None,
            "artifact_index": None,
            "evidence_index": None,
            "event_index": None,
            "execution_context_index": None,
            "creation_operation_id": "fixture",
            "created_at": "2026-08-16T00:00:00Z",
            "updated_at": "2026-08-16T00:00:00Z",
        },
    )
    return manifest


def seed_binary_partial(common: Any, candidate_manifest: Path, target: str) -> dict[str, Any]:
    _manifest, candidate, root = common.load_candidate(candidate_manifest)
    layout = common.load_layout()
    target_spec = common.target_layout(layout, target)
    partial = common.create_partial(candidate, target)
    context = root / "contexts" / target / "context.json"
    write_json(context, {"candidate_id": candidate["candidate_id"], "worker_id": partial["worker_id"]})
    partial["execution_contexts"].append(
        {"id": "context", "path": common.candidate_relative(root, context, "context")}
    )
    started = root / "events" / target / "build.started.json"
    completed = root / "events" / target / "build.completed.json"
    write_json(started, {"status": "started"})
    write_json(completed, {"status": "passed", "exit_code": 0})
    partial["events"].append(
        {
            "id": "event",
            "started": common.candidate_relative(root, started, "started event"),
            "completed": common.candidate_relative(root, completed, "completed event"),
        }
    )
    suffix = target_spec["executable_suffix"]
    for binary in layout["binaries"]:
        path = root / "cargo-target" / target / "release" / f"{binary['binary']}{suffix}"
        path.parent.mkdir(parents=True, exist_ok=True)
        requested = ",".join(binary["requested_features"])
        effective = ",".join(binary["effective_features"])
        path.write_text(
            "#!/usr/bin/env sh\n"
            f"echo component={binary['id']}\n"
            f"echo version={candidate['version']}\n"
            f"echo artifact_id={common.artifact_id(candidate, target, binary['id'])}\n"
            f"echo requested_features={requested}\n"
            f"echo effective_features={effective}\n",
            encoding="utf-8",
            newline="\n",
        )
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
        partial["artifacts"].append(
            {
                "id": f"binary-{binary['id']}",
                "kind": "binary",
                "component": binary["id"],
                "artifact_id": common.artifact_id(candidate, target, binary["id"]),
                "path": common.candidate_relative(root, path, "binary"),
                "requested_features": binary["requested_features"],
                "effective_features": binary["effective_features"],
                "required_dependencies": binary.get("required_dependencies", []),
                "command": ["cargo", "build", "--locked"],
                "exit_code": 0,
            }
        )
    common.save_draft(root, target, partial)
    return partial

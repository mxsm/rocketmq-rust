#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import (
    ArchiveError,
    draft_partial_path,
    load_candidate,
    load_layout,
    read_policy_json,
    resolve_candidate_path,
    sealed_partial_path,
)
from release_state import atomic_write_json, read_json


def seal_partial(candidate_manifest: Path, target: str) -> Path:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    layout = load_layout()
    if target not in layout["targets"]:
        raise ArchiveError(f"unsupported release target: {target}")
    draft_path = draft_partial_path(root, target)
    sealed_path = sealed_partial_path(root, target)
    if sealed_path.exists():
        raise ArchiveError(f"candidate partial is already sealed: {sealed_path}")
    partial = read_policy_json(draft_path, "candidate partial draft")
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
        False,
    )
    if identity != expected:
        raise ArchiveError("candidate partial identity does not match the active run")
    artifacts = partial.get("artifacts", [])
    identifiers = [entry.get("id") for entry in artifacts if isinstance(entry, dict)]
    required = {f"binary-{entry['id']}" for entry in layout["binaries"]}
    required.update({"archive", "archive-manifest", "common-inputs", "component-sbom", "host-smoke"})
    missing = sorted(required - set(identifiers))
    if missing:
        raise ArchiveError(f"candidate partial is missing artifacts: {', '.join(missing)}")
    if len(identifiers) != len(set(identifiers)):
        raise ArchiveError("candidate partial artifact identifiers are duplicated")
    for artifact in artifacts:
        path = resolve_candidate_path(root, artifact.get("path"), "partial artifact")
        if not path.exists():
            raise ArchiveError(f"candidate partial artifact is missing: {path}")
    events = partial.get("events")
    contexts = partial.get("execution_contexts")
    if not events or not contexts:
        raise ArchiveError("candidate partial has no event/context evidence")
    for event in events:
        started = resolve_candidate_path(root, event.get("started"), "partial started event")
        completed = resolve_candidate_path(root, event.get("completed"), "partial completed event")
        if not started.is_file() or not completed.is_file():
            raise ArchiveError(f"candidate partial event pair is incomplete: {event.get('id')}")
        completed_value = read_json(completed)
        if completed_value.get("exit_code") != 0:
            raise ArchiveError(f"candidate partial event did not succeed: {event.get('id')}")
    for context in contexts:
        path = resolve_candidate_path(root, context.get("path"), "partial execution context")
        if not path.is_file():
            raise ArchiveError(f"candidate partial context is missing: {context.get('id')}")
    partial["sealed"] = True
    atomic_write_json(draft_path, partial)
    os.replace(draft_path, sealed_path)
    return sealed_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--target", required=True)
    args = parser.parse_args(argv)
    try:
        output = seal_partial(args.candidate_manifest, args.target)
        print(f"CANDIDATE_PARTIAL_SEALED target={args.target} partial={output}")
        return 0
    except (ArchiveError, OSError) as error:
        print(f"CANDIDATE_PARTIAL_SEAL_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

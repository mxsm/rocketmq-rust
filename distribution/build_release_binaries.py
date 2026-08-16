#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import argparse
from contextlib import contextmanager
import os
from pathlib import Path
import sys
from typing import Any, Iterator


ROOT = Path(__file__).resolve().parents[1]
for module_root in (ROOT / "distribution", ROOT / "scripts"):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

import capture_candidate_execution_context
import release_candidate_command
from release_archive_common import (
    ArchiveError,
    add_unique_record,
    artifact_id,
    candidate_relative,
    load_candidate,
    load_layout,
    load_or_create_draft,
    save_draft,
    target_layout,
)


def build_command(
    candidate_root: Path,
    target: str,
    binary: dict[str, Any],
) -> list[str]:
    command = [
        "cargo",
        "build",
        "--locked",
        "--release",
        "--target",
        target,
        "--target-dir",
        str(candidate_root / "cargo-target" / target),
        "--package",
        binary["package"],
        "--bin",
        binary["binary"],
    ]
    features = binary.get("requested_features", [])
    if features:
        command.extend(["--features", ",".join(features)])
    return command


@contextmanager
def _build_environment(values: dict[str, str]) -> Iterator[None]:
    previous = {name: os.environ.get(name) for name in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def build_binaries(candidate_manifest: Path, target: str) -> Path:
    manifest, candidate, root = load_candidate(candidate_manifest)
    layout = load_layout()
    target_spec = target_layout(layout, target)
    draft = load_or_create_draft(root, candidate, target)
    context = capture_candidate_execution_context.capture_context(
        manifest,
        draft["worker_id"],
        root / "contexts" / target,
    )
    add_unique_record(
        draft,
        "execution_contexts",
        {
            "id": f"context-{target}",
            "path": candidate_relative(root, context, "execution context"),
        },
    )
    suffix = target_spec["executable_suffix"]
    for binary in layout["binaries"]:
        component = binary["id"]
        route_id = f"R05-build-{component}-{target}"
        command = build_command(root, target, binary)
        environment = {
            "ROCKETMQ_RELEASE_ARTIFACT_ID": artifact_id(candidate, target, component),
            "ROCKETMQ_RELEASE_REQUESTED_FEATURES": ",".join(binary["requested_features"]),
            "ROCKETMQ_RELEASE_EFFECTIVE_FEATURES": ",".join(binary["effective_features"]),
        }
        with _build_environment(environment):
            exit_code = release_candidate_command.run_command(
                manifest,
                route_id=route_id,
                worker_id=draft["worker_id"],
                context_path=context,
                event_root=root / "events" / target,
                command=command,
            )
        if exit_code != 0:
            raise ArchiveError(f"release binary build failed: {component}")
        built = root / "cargo-target" / target / "release" / f"{binary['binary']}{suffix}"
        if not built.is_file():
            raise ArchiveError(f"release binary output is missing: {built}")
        add_unique_record(
            draft,
            "artifacts",
            {
                "id": f"binary-{component}",
                "kind": "binary",
                "component": component,
                "artifact_id": environment["ROCKETMQ_RELEASE_ARTIFACT_ID"],
                "path": candidate_relative(root, built, "release binary"),
                "requested_features": binary["requested_features"],
                "effective_features": binary["effective_features"],
                "required_dependencies": binary.get("required_dependencies", []),
                "command": command,
                "exit_code": exit_code,
            },
        )
        add_unique_record(
            draft,
            "events",
            {
                "id": f"event-{component}",
                "started": candidate_relative(
                    root, root / "events" / target / f"{route_id}.started.json", "started event"
                ),
                "completed": candidate_relative(
                    root,
                    root / "events" / target / f"{route_id}.completed.json",
                    "completed event",
                ),
            },
        )
    save_draft(root, target, draft)
    return root / "partials" / f"CANDIDATE_PARTIAL.{target}.draft.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--target", required=True)
    args = parser.parse_args(argv)
    try:
        output = build_binaries(args.candidate_manifest, args.target)
        print(f"RELEASE_BINARY_BUILD_OK target={args.target} partial={output}")
        return 0
    except (ArchiveError, OSError) as error:
        print(f"RELEASE_BINARY_BUILD_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

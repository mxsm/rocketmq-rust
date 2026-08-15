#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run one candidate command with exactly-once started/completed event fragments."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]
DISTRIBUTION = ROOT / "distribution"
if str(DISTRIBUTION) not in sys.path:
    sys.path.insert(0, str(DISTRIBUTION))

from release_state import (
    ReleaseStateError,
    atomic_write_json,
    read_json,
    require_safe_id,
    resolve_existing_file,
    utc_now,
    validate_candidate,
)


class CommandError(ReleaseStateError):
    """Raised when a candidate route is duplicated or not safely attributable."""


SENSITIVE_OPTION_PARTS = {"password", "token", "secret", "credential"}
SENSITIVE_KEY_OPTIONS = {"key", "private-key", "api-key", "access-key", "secret-key", "encryption-key"}


def _sensitive_option(item: str) -> bool:
    name = item.split("=", 1)[0].lstrip("-").lower().replace("_", "-")
    return name in SENSITIVE_KEY_OPTIONS or any(
        part in SENSITIVE_OPTION_PARTS for part in name.split("-")
    )


def _redact_command(command: Sequence[str]) -> list[str]:
    rendered: list[str] = []
    redact_next = False
    for item in command:
        if redact_next:
            rendered.append("<redacted>")
            redact_next = False
            continue
        if item.startswith("-") and "=" not in item and _sensitive_option(item):
            rendered.append(item)
            redact_next = True
        elif item.startswith("-") and "=" in item and _sensitive_option(item):
            rendered.append(item.split("=", 1)[0] + "=<redacted>")
        else:
            rendered.append(item)
    return rendered


def run_command(
    candidate_manifest: Path,
    *,
    route_id: str,
    worker_id: str,
    context_path: Path,
    event_root: Path,
    command: Sequence[str],
) -> int:
    require_safe_id(route_id, "route_id")
    require_safe_id(worker_id, "worker_id")
    if not command:
        raise CommandError("candidate command cannot be empty")
    candidate_manifest = resolve_existing_file(candidate_manifest, "candidate_manifest")
    candidate = read_json(candidate_manifest)
    validate_candidate(candidate)
    if candidate["sealed"]:
        raise CommandError("sealed candidates cannot run new evidence-producing routes")
    context_path = resolve_existing_file(context_path, "execution_context")
    context = read_json(context_path)
    if context.get("candidate_id") != candidate["candidate_id"] or context.get("worker_id") != worker_id:
        raise CommandError("execution context does not match candidate and worker")
    event_root = event_root.resolve()
    started_path = event_root / f"{route_id}.started.json"
    completed_path = event_root / f"{route_id}.completed.json"
    if started_path.exists() or completed_path.exists():
        raise CommandError(f"candidate route already has an event reservation: {route_id}")
    started = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "route_id": route_id,
        "worker_id": worker_id,
        "context_path": str(context_path),
        "status": "started",
        "command": _redact_command(command),
        "started_at": utc_now(),
    }
    atomic_write_json(started_path, started)
    environment = os.environ.copy()
    environment.update(
        {
            "RELEASE_CANDIDATE_MANIFEST": str(candidate_manifest),
            "RELEASE_CANDIDATE_ROUTE_ID": route_id,
            "RELEASE_CANDIDATE_STARTED_EVENT": str(started_path),
            "RELEASE_CANDIDATE_COMPLETED_EVENT": str(completed_path),
        }
    )
    try:
        result = subprocess.run(list(command), check=False, env=environment)
        exit_code = result.returncode
    except OSError as error:
        exit_code = 127
        launch_error = str(error)
    else:
        launch_error = None
    if not completed_path.exists():
        completed = {
            "schema_version": 1,
            "candidate_id": candidate["candidate_id"],
            "route_id": route_id,
            "worker_id": worker_id,
            "context_path": str(context_path),
            "status": "passed" if exit_code == 0 else "failed",
            "exit_code": exit_code,
            "started_at": started["started_at"],
            "completed_at": utc_now(),
            "launch_error": launch_error,
        }
        atomic_write_json(completed_path, completed)
    else:
        completed = read_json(completed_path)
        identity = (
            completed.get("candidate_id"),
            completed.get("route_id"),
            completed.get("worker_id"),
            completed.get("context_path"),
        )
        expected_identity = (
            candidate["candidate_id"],
            route_id,
            worker_id,
            str(context_path),
        )
        if identity != expected_identity:
            raise CommandError("child-produced lifecycle completion event is not attributable to this route")
        recorded_exit = completed.get("exit_code")
        if not isinstance(recorded_exit, int) or isinstance(recorded_exit, bool):
            raise CommandError("child-produced lifecycle completion event has no integer exit code")
        expected_status = "passed" if recorded_exit == 0 else "failed"
        if completed.get("status") != expected_status or recorded_exit != exit_code:
            raise CommandError("child-produced lifecycle completion event disagrees with process exit")
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="subcommand", required=True)
    run = subcommands.add_parser("run")
    run.add_argument("--candidate-manifest", type=Path, required=True)
    run.add_argument("--route-id", required=True)
    run.add_argument("--worker-id", required=True)
    run.add_argument("--context", type=Path, required=True)
    run.add_argument("--event-root", type=Path, required=True)
    run.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command and args.command[0] == "--" else args.command
    try:
        return run_command(
            args.candidate_manifest,
            route_id=args.route_id,
            worker_id=args.worker_id,
            context_path=args.context,
            event_root=args.event_root,
            command=command,
        )
    except ReleaseStateError as error:
        print(f"RELEASE_CANDIDATE_COMMAND_FAILED detail={error}", file=sys.stderr)
        return 125


if __name__ == "__main__":
    raise SystemExit(main())

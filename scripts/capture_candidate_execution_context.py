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

"""Capture a non-sensitive worker-scoped execution context for one candidate."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import platform
import sys


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


class ContextError(ReleaseStateError):
    """Raised when an execution context cannot be captured safely."""


PUBLISHING_CREDENTIAL_NAMES = {
    "CARGO_REGISTRY_TOKEN",
    "CRATES_IO_TOKEN",
    "DOCKER_PASSWORD",
    "GHCR_TOKEN",
    "HELM_REGISTRY_PASSWORD",
}


def capture_context(candidate_manifest: Path, worker_id: str, output_root: Path) -> Path:
    require_safe_id(worker_id, "worker_id")
    candidate_manifest = resolve_existing_file(candidate_manifest, "candidate_manifest")
    candidate = read_json(candidate_manifest)
    validate_candidate(candidate)
    if candidate["sealed"]:
        raise ContextError("sealed candidates cannot capture new execution contexts")
    output = (output_root / f"{worker_id}.json").resolve()
    if output.exists():
        raise ContextError(f"execution context already exists for worker {worker_id}")
    credential_names = sorted(name for name in PUBLISHING_CREDENTIAL_NAMES if os.environ.get(name))
    value = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "worker_id": worker_id,
        "platform": sys.platform,
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
        "ci": os.environ.get("CI", "").lower() == "true",
        "workflow_event": os.environ.get("GITHUB_EVENT_NAME", "local"),
        "workflow_name": os.environ.get("GITHUB_WORKFLOW"),
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID"),
        "workflow_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "publish_input": os.environ.get("INPUT_PUBLISH", "false").lower() == "true",
        "publishing_credentials_provided": bool(credential_names),
        "publishing_credential_names": credential_names,
        "captured_at": utc_now(),
    }
    atomic_write_json(output, value)
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        output = capture_context(args.candidate_manifest, args.worker_id, args.output_root)
    except ReleaseStateError as error:
        print(f"CANDIDATE_EXECUTION_CONTEXT_FAILED detail={error}", file=sys.stderr)
        return 1
    print(f"CANDIDATE_EXECUTION_CONTEXT_OK output={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

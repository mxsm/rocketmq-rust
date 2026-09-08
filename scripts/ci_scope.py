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

"""Select root CI work from changed paths without builds or baseline checks."""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
from pathlib import Path
import subprocess
import tomllib


ROOT = Path(__file__).resolve().parents[1]


def workspace_members(root: Path) -> tuple[str, ...]:
    manifest = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    # Keep declared paths/patterns even if a package or its manifest was deleted.
    return tuple(pattern.rstrip("/") for pattern in manifest["workspace"]["members"])


def classify(paths: list[str], members: tuple[str, ...], *, full: bool = False) -> dict[str, bool]:
    paths = [path.replace("\\", "/").removeprefix("./") for path in paths]
    # Markdown can be embedded with include_str! or used as runtime prompt data.
    code = [path for path in paths if path.rsplit("/", 1)[-1] != "AGENTS.md"]

    def under(path: str, roots: tuple[str, ...]) -> bool:
        return any(fnmatch.fnmatchcase(path, root) or fnmatch.fnmatchcase(path, root + "/**") for root in roots)

    global_config = any(
        path in {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "rust-toolchain"}
        or path.startswith(".cargo/")
        for path in code
    )
    member_manifest = any(path.endswith("/Cargo.toml") and under(path, members) for path in code)
    feature_config = global_config or member_manifest
    root_workflow = ".github/workflows/rocketmq-rust-ci.yaml" in paths

    return {
        "full_features": full or feature_config,
        "automation": full or any(path in {
            ".github/workflows/auto_approve_pull_requests.yml",
            "scripts/tests/test_auto_approve.cjs",
        } for path in paths),
        "rust": full or global_config or root_workflow
        or any(path in {"rustfmt.toml", ".clippy.toml"} or under(path, members) for path in code),
        "observability": full or feature_config or any(
            under(path, ("rocketmq-observability", "rocketmq-runtime", "rocketmq-error", "rocketmq-model"))
            or (path.startswith("rocketmq-broker/") and "observability" in path)
            for path in code
        ),
        "rocksdb": full or feature_config or any(
            under(path, ("rocketmq-store", "rocketmq-store-rocksdb", "rocketmq-store-api",
                         "rocketmq-store-local", "rocketmq-broker"))
            for path in code
        ),
        "header": full or global_config or any(
            under(path, ("rocketmq-protocol", "rocketmq-macros", "scripts/request-header-codec"))
            for path in code
        ),
        "errors": full or global_config or any(
            under(path, ("rocketmq-error",))
            or path in {"scripts/error_architecture_guard.py", "scripts/check-error-hygiene.ps1",
                        "scripts/tests/test_error_architecture_guard.py"}
            for path in code
        ),
        "routing": full or any(
            path == "AGENTS.md" or path.endswith(("/AGENTS.md", "/Cargo.toml", "/package.json"))
            or path.startswith(".github/workflows/")
            or path in {"Cargo.toml", "package.json", "scripts/ci_scope.py",
                        "scripts/tests/test_ci_scope.py", "scripts/tests/test_agents_routing.py",
                        "scripts/check-agents-routing.ps1", "scripts/check-agents-routing.sh",
                        "rocketmq-doc/en/agents-routing-validation-adr.md",
                        "rocketmq-doc/en/agent-validation-reference.md"}
            for path in paths
        ),
    }


def changed_paths(root: Path, base: str) -> list[str]:
    # Include both old and new paths when a file moves between projects.
    result = subprocess.run(
        ["git", "diff", "--no-renames", "--name-only", "-z", base, "HEAD", "--"],
        cwd=root, check=True, capture_output=True,
    )
    return [os.fsdecode(path) for path in result.stdout.split(b"\0") if path]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths", nargs="*", help="Explicit paths for local scope inspection")
    args = parser.parse_args()
    event_name = os.environ.get("GITHUB_EVENT_NAME", "")
    full = event_name in {"schedule", "workflow_dispatch"}
    if args.paths is not None:
        paths = args.paths
    elif full:
        paths = []
    else:
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8"))
        if event_name == "pull_request":
            base = event["pull_request"]["base"]["sha"]
        elif event_name == "push":
            base = event["before"]
        else:
            raise ValueError(f"Unsupported CI event: {event_name}")
        if base and set(base) == {"0"}:
            full, paths = True, []
        else:
            paths = changed_paths(ROOT, base)

    scope = classify(paths, workspace_members(ROOT), full=full)
    print(json.dumps({"changed_files": len(paths), "checks": scope}, sort_keys=True))
    if output := os.environ.get("GITHUB_OUTPUT"):
        with Path(output).open("a", encoding="utf-8") as stream:
            for name, enabled in scope.items():
                stream.write(f"{name}={str(enabled).lower()}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

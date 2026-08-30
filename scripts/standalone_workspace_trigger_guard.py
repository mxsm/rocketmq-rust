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

"""Verify standalone Cargo workspace consumers are covered by CI path filters."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REQUIRED_EVENTS = ("push", "pull_request")
ON_PATTERN = re.compile(r"^(?P<indent>\s*)[\"']?on[\"']?\s*:\s*(?:#.*)?$")
EVENT_PATTERN = re.compile(
    r"^(?P<indent>\s*)(?P<event>push|pull_request)\s*:\s*(?:#.*)?$"
)
PATHS_PATTERN = re.compile(r"^(?P<indent>\s*)paths\s*:\s*(?:#.*)?$")
LIST_ITEM_PATTERN = re.compile(r"^\s*-\s*(?P<value>.+?)\s*$")


@dataclass(frozen=True)
class WorkspaceSpec:
    name: str
    manifest: str
    workflow: str


WORKSPACES = (
    WorkspaceSpec(
        name="rocketmq-sre",
        manifest="rocketmq-ai/rocketmq-sre/Cargo.toml",
        workflow=".github/workflows/rocketmq-sre-ci.yml",
    ),
    WorkspaceSpec(
        name="rocketmq-mcp",
        manifest="rocketmq-ai/rocketmq-mcp/Cargo.toml",
        workflow=".github/workflows/rocketmq-mcp-ci.yaml",
    ),
)


class GuardConfigurationError(RuntimeError):
    """Raised when metadata or workflow input cannot be inspected safely."""


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def repository_path_dependency_roots(
    metadata: dict[str, Any],
    repository_root: Path,
) -> set[str]:
    """Return repository-relative roots for path packages outside the workspace."""

    repository = repository_root.resolve()
    raw_workspace_root = metadata.get("workspace_root")
    if not isinstance(raw_workspace_root, str) or not raw_workspace_root:
        raise GuardConfigurationError("cargo metadata did not provide workspace_root")
    workspace_root = Path(raw_workspace_root).resolve()
    raw_packages = metadata.get("packages")
    if not isinstance(raw_packages, list):
        raise GuardConfigurationError("cargo metadata did not provide packages")

    roots: set[str] = set()
    for package in raw_packages:
        if not isinstance(package, dict) or package.get("source") is not None:
            continue
        manifest_path = package.get("manifest_path")
        if not isinstance(manifest_path, str) or not manifest_path:
            raise GuardConfigurationError("path package did not provide manifest_path")
        package_root = Path(manifest_path).resolve().parent
        if not _is_within(package_root, repository):
            continue
        if _is_within(package_root, workspace_root):
            continue
        roots.add(package_root.relative_to(repository).as_posix())
    return roots


def _indent_width(value: str) -> int:
    return len(value.expandtabs(8))


def _list_value(line: str) -> str | None:
    match = LIST_ITEM_PATTERN.match(line)
    if match is None:
        return None
    value = match.group("value").strip()
    if " #" in value:
        value = value.split(" #", 1)[0].rstrip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return value.replace("\\", "/").removeprefix("./")


def workflow_event_paths(workflow_text: str) -> dict[str, set[str]]:
    """Extract GitHub Actions path filters for push and pull_request."""

    lines = workflow_text.splitlines()
    result = {event: set() for event in REQUIRED_EVENTS}
    on_index = None
    on_indent = 0
    for index, line in enumerate(lines):
        match = ON_PATTERN.match(line)
        if match is not None:
            on_index = index
            on_indent = _indent_width(match.group("indent"))
            break
    if on_index is None:
        return result

    index = on_index + 1
    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            index += 1
            continue
        current_indent = _indent_width(line[: len(line) - len(line.lstrip())])
        if current_indent <= on_indent:
            break
        event_match = EVENT_PATTERN.match(line)
        if event_match is None:
            index += 1
            continue
        event = event_match.group("event")
        event_indent = _indent_width(event_match.group("indent"))
        index += 1
        while index < len(lines):
            event_line = lines[index]
            event_stripped = event_line.strip()
            if not event_stripped or event_stripped.startswith("#"):
                index += 1
                continue
            nested_indent = _indent_width(
                event_line[: len(event_line) - len(event_line.lstrip())]
            )
            if nested_indent <= event_indent:
                break
            paths_match = PATHS_PATTERN.match(event_line)
            if paths_match is None:
                index += 1
                continue
            paths_indent = _indent_width(paths_match.group("indent"))
            index += 1
            while index < len(lines):
                path_line = lines[index]
                path_stripped = path_line.strip()
                if not path_stripped or path_stripped.startswith("#"):
                    index += 1
                    continue
                path_indent = _indent_width(
                    path_line[: len(path_line) - len(path_line.lstrip())]
                )
                if path_indent <= paths_indent:
                    break
                value = _list_value(path_line)
                if value is not None:
                    result[event].add(value)
                index += 1
    return result


def _pattern_covers_root(pattern: str, dependency_root: str) -> bool:
    normalized = pattern.replace("\\", "/").removeprefix("./").rstrip("/")
    root = dependency_root.replace("\\", "/").strip("/")
    if normalized in {"**", "**/*"}:
        return True
    for suffix in ("/**/*", "/**"):
        if normalized.endswith(suffix):
            prefix = normalized[: -len(suffix)].rstrip("/")
            return root == prefix or root.startswith(f"{prefix}/")
    return False


def missing_workflow_triggers(
    metadata: dict[str, Any],
    workflow_text: str,
    repository_root: Path,
) -> dict[str, set[str]]:
    """Return path dependency roots missing from each required workflow event."""

    dependency_roots = repository_path_dependency_roots(metadata, repository_root)
    event_paths = workflow_event_paths(workflow_text)
    return {
        event: {
            root
            for root in dependency_roots
            if not any(
                _pattern_covers_root(pattern, root)
                for pattern in event_paths.get(event, set())
            )
        }
        for event in REQUIRED_EVENTS
    }


def _load_metadata(repository_root: Path, manifest: Path) -> dict[str, Any]:
    cargo = os.environ.get("CARGO", "cargo")
    result = subprocess.run(
        [
            cargo,
            "metadata",
            "--manifest-path",
            str(manifest),
            "--locked",
            "--format-version",
            "1",
        ],
        cwd=repository_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise GuardConfigurationError(
            f"cargo metadata failed for {manifest}: {detail}"
        )
    try:
        metadata = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise GuardConfigurationError(
            f"cargo metadata returned invalid JSON for {manifest}: {error}"
        ) from error
    if not isinstance(metadata, dict):
        raise GuardConfigurationError(
            f"cargo metadata returned a non-object for {manifest}"
        )
    return metadata


def run_guard(repository_root: Path) -> int:
    repository = repository_root.resolve()
    failures: list[str] = []
    inspected_roots = 0
    for workspace in WORKSPACES:
        manifest = repository / workspace.manifest
        workflow = repository / workspace.workflow
        if not manifest.is_file():
            failures.append(
                f"MISSING_STANDALONE_MANIFEST workspace={workspace.name} "
                f"path={workspace.manifest}"
            )
            continue
        if not workflow.is_file():
            failures.append(
                f"MISSING_STANDALONE_WORKFLOW workspace={workspace.name} "
                f"path={workspace.workflow}"
            )
            continue
        try:
            metadata = _load_metadata(repository, manifest)
            dependency_roots = repository_path_dependency_roots(
                metadata,
                repository,
            )
            missing = missing_workflow_triggers(
                metadata,
                workflow.read_text(encoding="utf-8"),
                repository,
            )
        except (GuardConfigurationError, OSError) as error:
            failures.append(
                f"STANDALONE_TRIGGER_INSPECTION_FAILED "
                f"workspace={workspace.name} detail={error}"
            )
            continue
        inspected_roots += len(dependency_roots)
        for event in REQUIRED_EVENTS:
            for root in sorted(missing[event]):
                failures.append(
                    f"MISSING_WORKFLOW_TRIGGER workspace={workspace.name} "
                    f"event={event} dependency={root} expected={root}/** "
                    f"workflow={workspace.workflow}"
                )

    if failures:
        for failure in failures:
            print(failure)
        return 1
    print(
        "STANDALONE_WORKSPACE_TRIGGER_GUARD_OK "
        f"workspaces={len(WORKSPACES)} dependency_roots={inspected_roots}"
    )
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify standalone workspace Cargo path dependencies are covered "
            "by push and pull_request workflow path filters."
        )
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root (defaults to the parent of scripts/).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    return run_guard(args.repo_root)


if __name__ == "__main__":
    raise SystemExit(main())

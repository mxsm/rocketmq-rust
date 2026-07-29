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

"""Collect a sanitized, reproducible implementation-baseline manifest."""

from __future__ import annotations

import argparse
import ctypes
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
from typing import Any

import architecture_dependency_guard as dependency_guard


ROOT = Path(__file__).resolve().parents[1]
INVENTORY_PATH = ROOT / "scripts/architecture-validation-inventory.json"


def run(command: list[str]) -> str:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"{' '.join(command)} failed: {detail}")
    return completed.stdout.strip()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def file_sha256(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def memory_bytes() -> int:
    if os.name == "nt":
        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("length", ctypes.c_ulong),
                ("memory_load", ctypes.c_ulong),
                ("total_physical", ctypes.c_ulonglong),
                ("available_physical", ctypes.c_ulonglong),
                ("total_page_file", ctypes.c_ulonglong),
                ("available_page_file", ctypes.c_ulonglong),
                ("total_virtual", ctypes.c_ulonglong),
                ("available_virtual", ctypes.c_ulonglong),
                ("available_extended_virtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatus()
        status.length = ctypes.sizeof(MemoryStatus)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return int(status.total_physical)
    if hasattr(os, "sysconf"):
        page_size = os.sysconf("SC_PAGE_SIZE")
        pages = os.sysconf("SC_PHYS_PAGES")
        if isinstance(page_size, int) and isinstance(pages, int):
            return page_size * pages
    return 0


def filesystem_name(root: Path) -> str:
    if os.name != "nt":
        return "platform-reported"
    volume_path = ctypes.create_unicode_buffer(261)
    if not ctypes.windll.kernel32.GetVolumePathNameW(str(root), volume_path, len(volume_path)):
        return "unknown"
    filesystem = ctypes.create_unicode_buffer(261)
    if not ctypes.windll.kernel32.GetVolumeInformationW(
        volume_path.value,
        None,
        0,
        None,
        None,
        None,
        filesystem,
        len(filesystem),
    ):
        return "unknown"
    return filesystem.value or "unknown"


def powershell_version() -> str:
    executable = shutil.which("pwsh") or shutil.which("powershell")
    if executable is None:
        return "unavailable"
    return run([executable, "-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"])


def relative_status() -> tuple[bool, list[str]]:
    lines = run(["git", "status", "--short", "--untracked-files=all"]).splitlines()
    return bool(lines), lines


def collect(output: Path) -> dict[str, Any]:
    inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    policy = inventory["implementation_baseline"]
    metadata = dependency_guard.read_metadata(None)
    packages = dependency_guard.workspace_packages(metadata)
    library_targets = sum(
        bool({"lib", "proc-macro"}.intersection(target.get("kind", [])))
        for package in packages
        for target in package.get("targets", [])
    )
    dirty, status = relative_status()
    tracked_diff = subprocess.run(
        ["git", "diff", "--binary", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        check=True,
    ).stdout
    disk = shutil.disk_usage(ROOT)
    artifacts: list[dict[str, str | int]] = []
    for relative in policy["required_evidence"]:
        path = ROOT / relative
        if not path.is_file():
            raise RuntimeError(f"required baseline evidence is missing: {relative}")
        artifacts.append(
            {
                "path": relative,
                "bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )

    collected_at = datetime.now(timezone.utc).isoformat()
    return {
        "schema_version": 1,
        "baseline_id": policy["id"],
        "classification": "dirty-implementation-baseline" if dirty else "clean-implementation-baseline",
        "candidate_eligible": not dirty,
        "collected_at_utc": collected_at,
        "historical_context": {
            "review_commit": policy["historical_review_commit"],
            "planning_snapshot_commit": policy["planning_snapshot_commit"],
            "statement": policy["historical_difference"],
        },
        "git": {
            "commit": run(["git", "rev-parse", "HEAD"]),
            "dirty": dirty,
            "status": status,
            "tracked_diff_sha256": sha256_bytes(tracked_diff),
        },
        "toolchain": {
            "rustc": run(["rustc", "--version", "--verbose"]),
            "cargo": run(["cargo", "--version"]),
            "python": sys.version.splitlines()[0],
            "powershell": powershell_version(),
        },
        "hardware": {
            "os": platform.system(),
            "os_release": platform.release(),
            "architecture": platform.machine(),
            "cpu_model": os.environ.get("PROCESSOR_IDENTIFIER") or platform.processor() or "unknown",
            "logical_cpus": os.cpu_count() or 0,
            "memory_bytes": memory_bytes(),
            "filesystem": filesystem_name(ROOT),
            "disk_total_bytes": disk.total,
            "disk_free_bytes": disk.free,
            "runner_label": os.environ.get("RUNNER_OS") or "local",
        },
        "cargo_metadata": {
            "command": "cargo metadata --format-version 1 --no-deps",
            "normalized_sha256": dependency_guard.normalized_metadata_sha256(metadata, ROOT),
            "workspace_packages": len(packages),
            "library_targets": library_targets,
        },
        "projects": {
            "root_manifest": inventory["root"]["manifest"],
            "standalone": [
                {
                    "id": entry["id"],
                    "manifest": entry["manifest"],
                    "instructions": entry["instructions"],
                }
                for entry in inventory["standalone"]
            ],
            "node": [
                {"id": entry["id"], "path": entry["path"]}
                for entry in inventory["node_projects"]
            ],
        },
        "evidence": artifacts,
        "commands": policy["commands"],
        "privacy": {
            "hostname_recorded": False,
            "absolute_repository_path_recorded": False,
            "credentials_recorded": False,
            "message_bodies_recorded": False,
        },
        "output": output.relative_to(ROOT).as_posix(),
    }


def main() -> int:
    inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    default_output = ROOT / inventory["implementation_baseline"]["output"]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=default_output)
    args = parser.parse_args()
    output = args.output.resolve()
    try:
        output.relative_to(ROOT)
    except ValueError:
        print("IMPLEMENTATION_BASELINE_FAILED output must remain inside the repository", file=sys.stderr)
        return 1
    try:
        manifest = collect(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"IMPLEMENTATION_BASELINE_FAILED {error}", file=sys.stderr)
        return 1
    print(
        f"IMPLEMENTATION_BASELINE_OK id={manifest['baseline_id']} "
        f"dirty={str(manifest['git']['dirty']).lower()} output={manifest['output']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

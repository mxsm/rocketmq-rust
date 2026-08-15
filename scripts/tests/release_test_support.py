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

from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path
import sys
import tarfile
from typing import Any


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, relative: str):
    path = ROOT / relative
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8", newline="\n")


def write_gate_evidence(path: Path, candidate_id: str, *, complete: bool = True) -> Path:
    required_capabilities = [
        *(f"F-{index:02d}" for index in range(1, 19)),
        *(f"G-{index:02d}" for index in range(1, 6)),
    ]
    required_release_results = [
        "R01-RELEASE-VERSION",
        "R01-CANDIDATE-LIFECYCLE",
        "R01-CORE-IMAGE-WORKFLOW",
    ]
    capability_results = {capability: "passed" for capability in required_capabilities}
    release_results = {result: "passed" for result in required_release_results}
    if not complete:
        capability_results["F-01"] = "failed"
    write_json(
        path,
        {
            "schema_version": 1,
            "candidate_id": candidate_id,
            "all_required_passed": complete,
            "failed_result_ids": [] if complete else ["F-01"],
            "capability_results": capability_results,
            "release_result_ids": release_results,
        },
    )
    return path


def create_source_bundle(
    path: Path,
    *,
    version: str,
    run_id: str,
    attempt: int,
    files: dict[str, bytes] | None = None,
) -> Path:
    files = files or {"Cargo.toml": b"[workspace]\n", "src/lib.rs": b"pub fn ready() {}\n"}
    manifest = {
        "schema_version": 1,
        "version": version,
        "run_id": run_id,
        "attempt": attempt,
        "files": [
            {"path": name, "type": "file", "size": len(content)}
            for name, content in sorted(files.items())
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w") as archive:
        rendered = (json.dumps(manifest, indent=2) + "\n").encode()
        info = tarfile.TarInfo("CORE_SOURCE_MANIFEST.json")
        info.size = len(rendered)
        archive.addfile(info, io.BytesIO(rendered))
        for name, content in sorted(files.items()):
            info = tarfile.TarInfo(f"source/{name}")
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
    return path

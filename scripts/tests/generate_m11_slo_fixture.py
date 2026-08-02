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

"""Regenerate hashes and the embedded fault snapshot in the M11-12 fixture."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def canonical_json_sha256(value: dict[str, object]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def canonical_text_sha256(path: Path) -> str:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(text.encode()).hexdigest()


def raw_file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: dict[str, object]) -> None:
    path.write_text(
        json.dumps(value, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    root = args.root.resolve()
    fixture = root / "scripts/tests/fixtures/m11-slo/pass"
    fault_path = fixture / "artifacts/fault-run.json"
    run_path = fixture / "run.json"
    fault_policy = json.loads(
        (root / "distribution/kubernetes/fault-matrix-policy.json").read_text(encoding="utf-8")
    )
    readiness_policy = json.loads(
        (root / "distribution/config/architecture-production-readiness-policy.json").read_text(
            encoding="utf-8"
        )
    )

    fault = json.loads(fault_path.read_text(encoding="utf-8"))
    run = json.loads(run_path.read_text(encoding="utf-8"))
    run["category"] = "ha_soak_rpo_rto"
    run["source"] = "architecture-slo-evidence"
    run["status"] = "not-run"
    fault["candidate_commit"] = run["candidate_commit"]
    fault["scenarios"] = [{"id": item["id"]} for item in fault_policy["scenarios"]]
    write_json(fault_path, fault)

    run["policy_sha256"] = canonical_json_sha256(readiness_policy)
    fault_hash = raw_file_sha256(fault_path)
    run["fault_evidence"]["sha256"] = fault_hash
    for relative in run["release_artifacts"]:
        run["release_artifacts"][relative] = canonical_text_sha256(root / relative)
    for artifact in run["artifacts"]:
        artifact["sha256"] = raw_file_sha256(fixture / artifact["path"])
    write_json(run_path, run)
    print(f"generated {len(fault['scenarios'])} embedded fault scenarios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

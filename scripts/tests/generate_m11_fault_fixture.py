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

"""Regenerate the deterministic non-production M11-11 fault guard fixture."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def canonical_json_sha256(value: dict[str, object]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    root = args.root.resolve()
    policy = json.loads(
        (root / "distribution/kubernetes/fault-matrix-policy.json").read_text(encoding="utf-8")
    )
    fixture = root / "scripts/tests/fixtures/m11-fault-matrix/pass"
    artifact_directory = fixture / "artifacts"
    artifact_directory.mkdir(parents=True, exist_ok=True)

    scenario_records: list[dict[str, object]] = []
    artifact_records: list[dict[str, str]] = []
    for scenario in policy["scenarios"]:
        scenario_id = scenario["id"]
        relative = f"artifacts/{scenario_id}.txt"
        content = f"M11-11 guard fixture only: {scenario_id}\n"
        artifact = fixture / relative
        artifact.write_text(content, encoding="utf-8", newline="\n")
        artifact_records.append(
            {"path": relative, "sha256": hashlib.sha256(content.encode()).hexdigest()}
        )
        scenario_records.append(
            {
                "id": scenario_id,
                "status": "passed",
                "assertions": {name: True for name in scenario["required_assertions"]},
                "evidence": {name: relative for name in scenario["required_evidence"]},
            }
        )

    run = {
        "schema_version": 1,
        "milestone": "M11-11",
        "policy_sha256": canonical_json_sha256(policy),
        "run_id": "fixture-m11-11-positive",
        "candidate_commit": "0123456789abcdef0123456789abcdef01234567",
        "started_at": "2026-07-17T01:00:00Z",
        "finished_at": "2026-07-17T01:16:00Z",
        "backend": "kind",
        "dynamic_execution": False,
        "fixture": True,
        "cluster_profile": {
            "control_plane_nodes": 1,
            "worker_nodes": 3,
            "broker_replicas": 3,
            "controller_replicas": 3,
            "storage_class": "fixture-standard",
            "nodes": [
                "fixture-control-plane",
                "fixture-worker-a",
                "fixture-worker-b",
                "fixture-worker-c",
            ],
        },
        "tool_versions": {
            "docker": "fixture",
            "kind": "v0.27.0-fixture",
            "k3d": "not-used-fixture",
            "kubectl": "v1.32.2-fixture",
            "helm": "v4.2.3-fixture",
        },
        "chart_sha256": "e9b88b4ee95b18c706839c28d3a0220e5bc470e9cd9262410c90793c45ff8b7c",
        "overlay_sha256": "029a7f0f4e1932c52a0476cf02a0fd855c0bb85694b82c338fc648dcb53a819d",
        "baseline_images": {
            "broker": "example.invalid/broker@sha256:99e09cb2284e2ddbb73a995deee3e91783fd04d177602ccf6eab326d778ee777",
            "namesrv": "example.invalid/namesrv@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818",
            "controller": "example.invalid/controller@sha256:f226345927d7e348497136874b6d207e0b32cc52154ad8323129352923a3142f",
            "proxy": "example.invalid/proxy@sha256:a6875aaea358acf0ac07786b1a6755d08fd640f4c79b7a2e46681cc13f49a04b",
            "mcp": "example.invalid/mcp@sha256:06d8f25bc3a971c4eb29e0ff08429b180402db0f4dec838c9eac427e296800a0",
        },
        "candidate_images": {
            "broker": "example.invalid/broker@sha256:3a88693bc57dd4c3d5f37e2acb1dc7e35daa0e578ca6c8538ed3fc4534de4373",
            "namesrv": "example.invalid/namesrv@sha256:536532dedeeed56d3f086f49bfb898babf09e36f7d5ac1ee5efc374e59ff7688",
            "controller": "example.invalid/controller@sha256:d7ac38651c788f9e8745b1a183b894aa4f5acd428ea4a2cebdf31c5c18352e5d",
            "proxy": "example.invalid/proxy@sha256:9e29c84a61a63655bff5c10fd9ff3153336b16caa0ed5c149950c06a349d72c1",
            "mcp": "example.invalid/mcp@sha256:4df393a882f7367f20eb8e91411e9f31df2aecef2b57e4748b2879ea4f1fd9c7",
        },
        "global_assertions": {name: True for name in policy["global_assertions"]},
        "unresolved_faults": [],
        "scenarios": scenario_records,
        "artifacts": artifact_records,
    }
    (fixture / "run.json").write_text(
        json.dumps(run, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"generated {len(scenario_records)} M11-11 fixture scenarios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

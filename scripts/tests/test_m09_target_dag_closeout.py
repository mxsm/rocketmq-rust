# Copyright 2023 The RocketMQ Rust Authors
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

import json
import subprocess
import sys
import tomllib
import unittest
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GUARD = ROOT / "scripts" / "architecture_dependency_guard.py"
POLICY = ROOT / "scripts" / "architecture-dependency-policy.json"
BASELINE = ROOT / "scripts" / "architecture-dependency-baseline.json"


class TargetDagCloseoutTests(unittest.TestCase):
    def test_target_guard_closes_unapproved_edges_and_reports_debt_separately(self) -> None:
        result = subprocess.run(
            [sys.executable, str(GUARD), "--mode", "target"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("TARGET_COMPATIBILITY_LEDGER active_edges=38 entries=38", result.stdout)
        self.assertIn("TARGET_TEST_DEPENDENCIES active_edges=3 entries=3", result.stdout)
        self.assertIn("ARCHITECTURE_DEPENDENCY_GUARD_OK mode=target", result.stdout)
        self.assertNotIn("VIOLATION", result.stdout)

    def test_workspace_target_is_exact_and_standalone_projects_are_external(self) -> None:
        policy = json.loads(POLICY.read_text(encoding="utf-8"))
        metadata = json.loads(
            subprocess.run(
                ["cargo", "metadata", "--format-version", "1", "--no-deps"],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=True,
            ).stdout
        )
        members = set(metadata["workspace_members"])
        packages = {item["name"] for item in metadata["packages"] if item["id"] in members}

        self.assertEqual(32, len(packages))
        self.assertEqual(set(policy["target_dag"]), packages)
        self.assertNotIn("rocketmq-example", packages)
        self.assertFalse(any(name.startswith("rocketmq-dashboard-") and name != "rocketmq-dashboard-common" for name in packages))

    def test_temporary_exceptions_are_empty_and_removal_windows_are_current(self) -> None:
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
        ledger = baseline["compatibility_manifest_exceptions"]
        identities = {
            (item["caller"], item["target"], item["kind"], item["path"], item["alias"])
            for item in ledger
        }

        self.assertEqual([], baseline["manifest_exceptions"])
        self.assertEqual([], baseline["source_exceptions"])
        self.assertEqual(38, len(ledger))
        self.assertEqual(38, len(identities))
        self.assertEqual(
            Counter({"R1": 29, "next-major": 4, "M09-04": 3, "long-term": 2}),
            Counter(item["remove_by"] for item in ledger),
        )

    def test_test_only_edges_are_exact_and_cannot_hide_production_edges(self) -> None:
        policy = json.loads(POLICY.read_text(encoding="utf-8"))
        edges = policy["test_dependency_policy"]["allowed_edges"]

        self.assertEqual(3, len(edges))
        self.assertEqual({"dev"}, {item["kind"] for item in edges})
        self.assertEqual(
            {
                ("rocketmq-broker", "rocketmq-controller"),
                ("rocketmq-broker", "rocketmq-namesrv"),
                ("rocketmq-rust", "rocketmq-observability"),
            },
            {(item["caller"], item["target"]) for item in edges},
        )

    def test_boundary_type_owner_is_model_and_legacy_paths_are_reexports(self) -> None:
        tiered = tomllib.loads((ROOT / "rocketmq-tieredstore" / "Cargo.toml").read_text(encoding="utf-8"))
        protocol = (ROOT / "rocketmq-protocol" / "src" / "common" / "boundary_type.rs").read_text(
            encoding="utf-8"
        )
        common = (ROOT / "rocketmq-common" / "src" / "common" / "boundary_type.rs").read_text(
            encoding="utf-8"
        )
        tiered_sources = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / "rocketmq-tieredstore").rglob("*.rs")
        )

        self.assertIn("rocketmq-model", tiered["dependencies"])
        self.assertNotIn("rocketmq-common", tiered["dependencies"])
        self.assertIn("pub use rocketmq_model::boundary_type::*;", protocol)
        self.assertIn("pub use rocketmq_protocol::common::boundary_type::*;", common)
        self.assertNotIn("rocketmq_common::common::boundary_type::BoundaryType", tiered_sources)


if __name__ == "__main__":
    unittest.main()

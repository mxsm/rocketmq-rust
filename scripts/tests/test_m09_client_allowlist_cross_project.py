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

import json
import subprocess
import sys
import tomllib
import unittest
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
POLICY = ROOT / "scripts" / "architecture-dependency-policy.json"
BASELINE = ROOT / "scripts" / "architecture-dependency-baseline.json"
GUARD = ROOT / "scripts" / "architecture_dependency_guard.py"
MCP = ROOT / "rocketmq-tools" / "rocketmq-mcp"
EVIDENCE = (
    ROOT
    / "docs"
    / "plans"
    / "architecture-refactor-migration"
    / "phase-2-core-boundaries"
    / "09-client-allowlist-cross-project-evidence.md"
)
DESIGN = ROOT / "docs" / "architecture-refactor-design.md"


def manifest(relative: str) -> dict:
    return tomllib.loads((ROOT / relative / "Cargo.toml").read_text(encoding="utf-8"))


def rocketmq_dependencies(relative: str) -> set[str]:
    return {
        name
        for name in manifest(relative).get("dependencies", {})
        if name.startswith("rocketmq-")
    }


class ClientAllowlistCrossProjectTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = json.loads(POLICY.read_text(encoding="utf-8"))
        cls.baseline = json.loads(BASELINE.read_text(encoding="utf-8"))

    def test_client_allowlist_is_exactly_workspace_two_plus_standalone_one(self) -> None:
        client_policy = self.policy["client_policy"]
        manifest_identities = {
            (entry["caller"], entry["kind"], entry["path"], entry["alias"])
            for entry in client_policy["target_manifest_allowlist"]
        }
        source_identities = {
            (entry["caller"], entry["path_prefix"], tuple(entry["aliases"]))
            for entry in client_policy["target_source_allowlist"]
        }

        self.assertEqual(
            {
                (
                    "rocketmq-admin-core",
                    "normal",
                    "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/Cargo.toml",
                    "rocketmq_client_rust",
                ),
                (
                    "rocketmq-proxy-cluster",
                    "normal",
                    "rocketmq-proxy-cluster/Cargo.toml",
                    "rocketmq_client_rust",
                ),
                ("rocketmq-example", "dev", "rocketmq-example/Cargo.toml", "rocketmq_client_rust"),
            },
            manifest_identities,
        )
        self.assertEqual(
            {
                (
                    "rocketmq-admin-core",
                    "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/client_adapter/",
                    ("rocketmq_client_rust",),
                ),
                ("rocketmq-proxy-cluster", "rocketmq-proxy-cluster/src/", ("rocketmq_client_rust",)),
                ("rocketmq-example", "rocketmq-example/examples/", ("rocketmq_client_rust",)),
            },
            source_identities,
        )

    def test_server_core_and_local_normal_closures_forbid_client(self) -> None:
        expected_callers = {
            "rocketmq-broker",
            "rocketmq-namesrv",
            "rocketmq-proxy-core",
            "rocketmq-proxy-local",
            "rocketmq-common",
            "rocketmq-remoting",
        }
        matches = [
            rule
            for rule in self.policy["closure_rules"]
            if set(rule["callers"]) == expected_callers
            and rule["forbidden_targets"] == ["rocketmq-client-rust"]
        ]

        self.assertEqual(1, len(matches))

    def test_mcp_removes_unused_edges_and_keeps_owned_runtime_lifecycle(self) -> None:
        mcp_manifest = manifest("rocketmq-tools/rocketmq-mcp")
        dependencies = rocketmq_dependencies("rocketmq-tools/rocketmq-mcp")
        runtime_sources = {
            path.relative_to(ROOT).as_posix()
            for path in (MCP / "src").rglob("*.rs")
            if "rocketmq_runtime::" in path.read_text(encoding="utf-8")
        }

        self.assertEqual(
            {"rocketmq-admin-core", "rocketmq-observability", "rocketmq-runtime"},
            dependencies,
        )
        self.assertEqual([], mcp_manifest["features"]["auth"])
        self.assertIn("auth", mcp_manifest["features"]["streamable-http"])
        self.assertEqual(
            {
                "rocketmq-tools/rocketmq-mcp/src/app.rs",
                "rocketmq-tools/rocketmq-mcp/src/guard/audit.rs",
            },
            runtime_sources,
        )
        self.assertEqual(
            {
                "rocketmq-admin-core",
                "rocketmq-security-api",
                "rocketmq-observability",
                "rocketmq-runtime",
            },
            set(self.policy["target_dag"]["rocketmq-mcp"]),
        )
        design = DESIGN.read_text(encoding="utf-8")
        self.assertIn(
            "admin-core、security-api、observability，以及 runtime 的 owned lifecycle/BlockingExecutor",
            design,
        )

    def test_mcp_and_dashboards_reach_client_only_through_admin_core(self) -> None:
        forbidden = {"rocketmq-client-rust", "rocketmq-common", "rocketmq-remoting"}
        projects = (
            "rocketmq-tools/rocketmq-mcp",
            "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri",
            "rocketmq-dashboard/rocketmq-dashboard-web/backend",
        )
        for project in projects:
            with self.subTest(project=project):
                parsed = manifest(project)
                dependencies = set(parsed["dependencies"])
                self.assertIn("rocketmq-admin-core", dependencies)
                self.assertEqual(set(), dependencies & forbidden)
                admin = parsed["dependencies"]["rocketmq-admin-core"]
                self.assertIs(admin.get("default-features"), False)
                self.assertIn("client-adapter", admin.get("features", []))
                self.assertNotIn("legacy-common-compat", admin.get("features", []))

    def test_m09_04_ledger_is_zero_and_target_guard_passes(self) -> None:
        ledger = self.baseline["compatibility_manifest_exceptions"]
        self.assertEqual(35, len(ledger))
        self.assertEqual(Counter({"R1": 29, "next-major": 4, "long-term": 2}), Counter(
            item["remove_by"] for item in ledger
        ))
        self.assertFalse(any(item["remove_by"] == "M09-04" for item in ledger))

        result = subprocess.run(
            [sys.executable, str(GUARD), "--mode", "target"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("TARGET_COMPATIBILITY_LEDGER active_edges=35 entries=35", result.stdout)
        self.assertNotIn("VIOLATION", result.stdout)

    def test_evidence_records_cross_project_results_and_remaining_goal(self) -> None:
        evidence = EVIDENCE.read_text(encoding="utf-8")

        self.assertIn("workspace 2 + standalone 1", evidence)
        self.assertIn("M09-04：3 → 0", evidence)
        self.assertIn("35", evidence)
        self.assertIn("57/82", evidence)
        self.assertIn("PR-M09-05", evidence)


if __name__ == "__main__":
    unittest.main()

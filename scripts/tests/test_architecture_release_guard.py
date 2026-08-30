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

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import architecture_release_guard as guard  # noqa: E402


class ArchitectureReleaseGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = json.loads(guard.PLAN_PATH.read_text(encoding="utf-8"))
        self.policy = json.loads(guard.POLICY_PATH.read_text(encoding="utf-8"))
        self.baseline = json.loads(guard.BASELINE_PATH.read_text(encoding="utf-8"))

    def test_live_release_package_passes(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPTS / "architecture_release_guard.py")],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertNotIn("Traceback", result.stdout + result.stderr)
        self.assertIn("ARCHITECTURE_RELEASE_GUARD_OK", result.stdout)

    def test_structural_core_mode_requires_semantic_release_routes(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS / "architecture_release_guard.py"),
                "--scope",
                "core-release",
                "--mode",
                "structural",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("mode=structural", result.stdout)

    def test_standalone_mcp_is_discovered_without_becoming_a_root_member(self) -> None:
        findings: list[guard.Finding] = []

        inventory = guard.discover_release_inventory(ROOT, findings)

        self.assertEqual([], findings)
        self.assertIn("rocketmq-ai/rocketmq-mcp/Cargo.toml", inventory.standalone_projects)
        self.assertIn("rocketmq-mcp", inventory.governance_targets)
        self.assertNotIn("rocketmq-mcp", inventory.root_members)

    def test_plan_contains_no_deleted_legacy_resource(self) -> None:
        source = guard.PLAN_PATH.read_text(encoding="utf-8")
        for legacy in ("rocketmq-common", "rocketmq-remoting", '"rocketmq-rust"'):
            self.assertNotIn(legacy, source)
        self.assertIn("rocketmq-doc/en/", self.plan["design_source"])

    def test_plan_delegates_publish_order_to_locked_metadata_planner(self) -> None:
        scope = json.loads((SCRIPTS / "core-release-scope.json").read_text(encoding="utf-8"))
        metadata = guard.package_publish_workspace.collect_metadata(ROOT)
        package_plan = guard.package_publish_workspace.build_plan(metadata, scope, selector=None)

        topology = self.plan["release_topology"]
        self.assertNotIn("publish_order", topology)
        self.assertEqual(
            "cargo metadata --locked --format-version 1 --no-deps",
            topology["metadata_command"],
        )
        self.assertEqual(24, len(package_plan["packages"]))
        self.assertEqual(3, len(package_plan["skipped_packages"]))
        package_names = {entry["name"] for entry in package_plan["packages"]}
        self.assertNotIn("rocketmq-dashboard-common", package_names)
        self.assertNotIn("rocketmq-mcp", package_names)

    def test_plan_requires_semantic_core_routes_instead_of_legacy_modes(self) -> None:
        commands = {entry["id"]: entry["command"] for entry in self.plan["semantic_release_routes"]}

        self.assertIn("--mode structural --scope core-release", commands["dependency"])
        self.assertIn("--mode semantic --scope core-release", commands["documentation"])
        self.assertIn("--scope core-release", commands["public-api-intent"])
        self.assertIn("--scope core-release --mode structural", commands["release"])
        serialized = json.dumps(self.plan).lower()
        self.assertNotIn("transition_debt", self.plan)
        for legacy in ('"required_mode": "baseline"', '"required_mode": "transition"', '"required_mode": "target"'):
            self.assertNotIn(legacy, serialized)

    def test_ci_validation_rejects_a_missing_required_core_static_route(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow = root / ".github/workflows/rocketmq-rust-ci.yaml"
            workflow.parent.mkdir(parents=True)
            workflow.write_text("jobs: {}\n", encoding="utf-8")
            findings: list[guard.Finding] = []

            guard.validate_ci(root, findings)

        self.assertIn("ci-command-missing", {finding.code for finding in findings})

    def test_ci_validation_rejects_blocking_legacy_identity_routes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow = root / ".github/workflows/rocketmq-rust-ci.yaml"
            workflow.parent.mkdir(parents=True)
            workflow.write_text(
                """
jobs:
  core:
    steps:
      - name: Core semantic guards
        run: python scripts/core_release_static_guard.py
      - name: Legacy dependency baseline
        run: python scripts/architecture_dependency_guard.py --mode baseline
""",
                encoding="utf-8",
            )
            findings: list[guard.Finding] = []

            guard.validate_ci(root, findings)

        self.assertIn("ci-legacy-route-blocking", {finding.code for finding in findings})

    def test_missing_design_source_is_a_structured_finding(self) -> None:
        invalid = copy.deepcopy(self.plan)
        invalid["design_source"] = "rocketmq-doc/en/missing.md#release-topology"
        findings = guard.validate(
            invalid,
            self.policy,
            self.baseline,
            check_ci=False,
        )
        self.assertTrue(any(item.code == "design-source-missing" for item in findings))

    def test_package_planner_resource_violation_is_a_structured_finding(self) -> None:
        invalid = copy.deepcopy(self.plan)
        invalid["release_topology"]["planner"] = "distribution/deleted-planner.py"
        findings = guard.validate(
            invalid,
            self.policy,
            self.baseline,
            check_ci=False,
        )
        self.assertTrue(any(item.code == "package-planner-resource-missing" for item in findings))

    def test_missing_manifest_and_section_do_not_raise(self) -> None:
        edge = {
            "caller": "fixture",
            "target": "rocketmq-error",
            "kind": "normal",
            "path": "fixture/Cargo.toml",
            "alias": "rocketmq_error",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            findings: list[guard.Finding] = []
            guard.manifest_has_edge(edge, root, findings)
            self.assertTrue(any(item.code == "manifest-invalid" for item in findings))

            manifest = root / "fixture" / "Cargo.toml"
            manifest.parent.mkdir()
            manifest.write_text('[package]\nname = "fixture"\nversion = "0.1.0"\n', encoding="utf-8")
            findings = []
            guard.manifest_has_edge(edge, root, findings)
            self.assertTrue(any(item.code == "manifest-section-missing" for item in findings))

    def test_unknown_package_classification_is_a_finding(self) -> None:
        invalid = copy.deepcopy(self.plan)
        invalid["release_topology"]["registry_publish_classification"] = "deleted"
        findings = guard.validate(
            invalid,
            self.policy,
            self.baseline,
            check_ci=False,
        )
        self.assertTrue(any(item.code == "package-planner-contract-invalid" for item in findings))
        self.assertTrue(any(item.code == "package-planner-scope-mismatch" for item in findings))

    def test_malformed_json_is_a_structured_input_finding(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "bad.json"
            path.write_text("{", encoding="utf-8")
            findings: list[guard.Finding] = []
            self.assertIsNone(guard.load_json(path, "fixture", findings))
            self.assertEqual(["input-invalid"], [item.code for item in findings])

    def test_compatibility_windows_exactly_match_baseline(self) -> None:
        invalid = copy.deepcopy(self.plan)
        invalid["compatibility_windows"]["preserved_edges"][0]["alias"] = "renamed_store"
        findings = guard.validate(
            invalid,
            self.policy,
            self.baseline,
            check_ci=False,
        )
        self.assertTrue(any(item.code == "compatibility-window-mismatch" for item in findings))


if __name__ == "__main__":
    unittest.main()

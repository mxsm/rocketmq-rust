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

    def test_plan_contains_no_deleted_legacy_resource(self) -> None:
        source = guard.PLAN_PATH.read_text(encoding="utf-8")
        for legacy in ("rocketmq-common", "rocketmq-remoting", '"rocketmq-rust"'):
            self.assertNotIn(legacy, source)
        self.assertIn("rocketmq-doc/en/", self.plan["design_source"])

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

    def test_publish_order_violation_is_a_structured_finding(self) -> None:
        invalid = copy.deepcopy(self.plan)
        order = invalid["release_topology"]["publish_order"]
        caller = order.index("rocketmq-model")
        dependency = order.index("rocketmq-error")
        order[caller], order[dependency] = order[dependency], order[caller]
        findings = guard.validate(
            invalid,
            self.policy,
            self.baseline,
            check_ci=False,
        )
        self.assertTrue(any(item.code == "publish-order-violation" for item in findings))

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

    def test_unknown_dependency_package_is_a_finding(self) -> None:
        invalid = copy.deepcopy(self.plan)
        invalid["release_topology"]["publish_order"][-1] = "rocketmq-deleted"
        findings = guard.validate(
            invalid,
            self.policy,
            self.baseline,
            check_ci=False,
        )
        self.assertTrue(any(item.code == "publish-package-mismatch" for item in findings))

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

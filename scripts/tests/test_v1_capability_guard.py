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
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
MANIFEST_PATH = SCRIPTS / "v1-capability-manifest.json"
SCHEMA_PATH = SCRIPTS / "v1-capability-manifest.schema.json"


class V1CapabilityGuardTests(unittest.TestCase):
    def load_manifest(self) -> dict[str, object]:
        self.assertTrue(MANIFEST_PATH.is_file(), "capability manifest is missing")
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    def validate_fixture(self, manifest: dict[str, object], *, phase: int = 0) -> list[dict[str, str]]:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            code = """
import json
from pathlib import Path
import sys
import v1_capability_guard as guard

manifest = guard.load_manifest(Path(sys.argv[1]))
findings = guard.validate_manifest(manifest, root=Path(sys.argv[2]), phase=int(sys.argv[3]))
print(json.dumps([finding.as_dict() for finding in findings]))
"""
            completed = subprocess.run(
                [sys.executable, "-c", code, str(path), str(ROOT), str(phase)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                env={**os.environ, "PYTHONPATH": str(SCRIPTS)},
            )
        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        return json.loads(completed.stdout)

    def capability(self, manifest: dict[str, object], capability_id: str) -> dict[str, object]:
        return next(item for item in manifest["capabilities"] if item["capability_id"] == capability_id)

    def test_live_manifest_and_schema_pass_the_phase_zero_guard(self) -> None:
        self.assertTrue(SCHEMA_PATH.is_file(), "capability schema is missing")
        completed = subprocess.run(
            [sys.executable, str(SCRIPTS / "v1_capability_guard.py"), "--phase", "0"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        self.assertIn(
            "V1_CAPABILITY_GUARD_OK phase=0 capabilities=26 active=24 deferred=2 exclusions=4",
            completed.stdout,
        )

    def test_manifest_contains_every_required_capability_and_unique_test_id(self) -> None:
        manifest = self.load_manifest()
        expected = {f"F-{number:02d}" for number in range(1, 19)} | {
            f"G-{number:02d}" for number in range(1, 9)
        }
        capabilities = manifest["capabilities"]
        test_ids = [test_id for item in capabilities for test_id in item["test_ids"]]

        self.assertEqual(expected, {item["capability_id"] for item in capabilities})
        self.assertEqual(len(test_ids), len(set(test_ids)))
        for capability in capabilities:
            expected = "deferred" if capability["capability_id"] in {"G-07", "G-08"} else "1.0.0-rc.1"
            self.assertEqual(expected, capability["target_rc"])

    def test_missing_required_fields_are_structured_findings(self) -> None:
        manifest = self.load_manifest()
        capability = manifest["capabilities"][0]
        del capability["ownership"]["dri"]
        capability["test_ids"] = []
        capability["commands"] = []
        capability["expected_results"] = []
        del capability["target_phase"]
        del capability["target_rc"]

        findings = self.validate_fixture(manifest)
        codes = {item["code"] for item in findings}
        self.assertTrue(
            {
                "ownership-invalid",
                "test-ids-missing",
                "commands-missing",
                "expected-results-missing",
                "target-phase-invalid",
                "target-rc-invalid",
            }.issubset(codes),
            findings,
        )

    def test_test_ids_and_commands_have_one_to_one_route_cardinality(self) -> None:
        manifest = self.load_manifest()
        capability = self.capability(manifest, "F-01")
        capability["commands"].append("cargo test -p rocketmq-broker --test extra-route")

        findings = self.validate_fixture(manifest)

        self.assertIn(
            "test-command-cardinality-mismatch",
            {item["code"] for item in findings},
        )

    def test_core_capability_cannot_be_intentionally_unsupported(self) -> None:
        manifest = self.load_manifest()
        capability = self.capability(manifest, "F-01")
        capability["implementation_status"] = "intentionally-unsupported"
        capability["completion_status"] = "intentionally-unsupported"
        capability["exclusion"] = {"reason": "too difficult"}

        findings = self.validate_fixture(manifest)
        self.assertIn("unsupported-core-capability", {item["code"] for item in findings})

    def test_controller_capability_cannot_claim_java_internal_wire_compatibility(self) -> None:
        manifest = self.load_manifest()
        controller = self.capability(manifest, "F-09")
        controller["compatibility_mode"] = "wire"
        controller["java_baseline"]["scope"] = "controller-internal"

        findings = self.validate_fixture(manifest)
        self.assertIn("controller-java-internal-wire", {item["code"] for item in findings})

    def test_missing_required_capability_is_a_finding(self) -> None:
        manifest = self.load_manifest()
        manifest["capabilities"] = [
            item for item in manifest["capabilities"] if item["capability_id"] != "F-18"
        ]

        findings = self.validate_fixture(manifest)
        self.assertIn("required-capability-missing", {item["code"] for item in findings})

    def test_excluded_products_cannot_enter_the_development_denominator(self) -> None:
        manifest = self.load_manifest()
        excluded = copy.deepcopy(manifest["capabilities"][0])
        excluded["capability_id"] = "F-19"
        excluded["title"] = "OpenMessaging compatibility layer"
        excluded["test_ids"] = ["F19-OPENMESSAGING"]
        manifest["capabilities"].append(excluded)

        findings = self.validate_fixture(manifest)
        self.assertIn("excluded-capability-in-denominator", {item["code"] for item in findings})

    def test_rust_native_alternative_can_be_equivalent_with_system_evidence(self) -> None:
        manifest = self.load_manifest()
        controller = self.capability(manifest, "F-09")
        controller["implementation_status"] = "implemented"
        controller["evidence_status"] = "functional-system"
        controller["completion_status"] = "alternative-equivalent"
        controller["variance_class"] = "rust-enhancement"
        controller["artifacts"] = [
            {"path": "target/v1-evidence/F09-controller-ha.json", "run_id": "fixture-run"}
        ]

        findings = self.validate_fixture(manifest)
        self.assertEqual([], [item for item in findings if item["path"].startswith("F-09")])

    def test_deferred_status_is_restricted_to_approved_long_running_items(self) -> None:
        manifest = self.load_manifest()
        capability = self.capability(manifest, "F-01")
        capability["completion_status"] = "deferred-by-scope"
        capability["deferred_reference"] = "D-F01"

        findings = self.validate_fixture(manifest)
        self.assertIn("deferred-capability-not-approved", {item["code"] for item in findings})

    def test_active_capability_cannot_enter_development_without_a_target_rc(self) -> None:
        manifest = self.load_manifest()
        capability = self.capability(manifest, "F-01")
        capability["implementation_status"] = "partial"
        capability["target_rc"] = "deferred"

        findings = self.validate_fixture(manifest)
        self.assertIn("active-target-rc-missing", {item["code"] for item in findings})

    def test_scope_document_records_compatibility_boundaries(self) -> None:
        path = ROOT / "rocketmq-doc/en/release/1.0/scope-and-compatibility.md"
        self.assertTrue(path.is_file(), "1.0 scope document is missing")
        document = path.read_text(encoding="utf-8")
        for required in (
            "Pure Rust Controller",
            "Rust-native Timer and POP",
            "Conditional Java data migration",
            "OpenMessaging",
            "BrokerContainer",
            "DLedger CommitLog",
            "not production-certified",
        ):
            self.assertIn(required, document)

    def test_phase_six_rejects_every_active_blocked_capability(self) -> None:
        findings = self.validate_fixture(self.load_manifest(), phase=6)
        blocked = {item["path"] for item in findings if item["code"] == "release-capability-blocked"}

        self.assertEqual(24, len(blocked))

    def test_schema_and_manifest_do_not_define_content_digest_fields(self) -> None:
        self.assertTrue(MANIFEST_PATH.is_file(), "capability manifest is missing")
        self.assertTrue(SCHEMA_PATH.is_file(), "capability schema is missing")
        manifest_text = MANIFEST_PATH.read_text(encoding="utf-8").lower()
        schema_text = SCHEMA_PATH.read_text(encoding="utf-8").lower()

        for forbidden in ('"sha', '"hash', '"digest'):
            self.assertNotIn(forbidden, manifest_text)
            self.assertNotIn(forbidden, schema_text)


if __name__ == "__main__":
    unittest.main()

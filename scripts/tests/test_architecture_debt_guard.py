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

from scripts import architecture_debt_guard as guard


ROOT = Path(__file__).resolve().parents[2]


class ArchitectureDebtGuardTests(unittest.TestCase):
    def test_core_allow_debt_excludes_non_core_projects(self) -> None:
        source = """
INTERNAL_ERROR_ALLOWLIST = ("rocketmq-broker/src/",)
ANYHOW_RESULT_ALLOWLIST = {
    "rocketmq-client/src/runtime.rs": "core",
    "rocketmq-dashboard/app/src/main.rs": "excluded",
}
PROCESSOR_GENERIC_RESPONSE_ALLOWLIST = {}
SOURCE_STRINGIFICATION_ALLOWLIST = {}
"""

        self.assertEqual(2, guard.error_allowlist_count(source, scope="core-release"))
        self.assertEqual(3, guard.error_allowlist_count(source, scope="repo-global"))

    def test_live_core_scope_check_is_an_independent_gate(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/architecture_debt_guard.py"),
                "--check",
                "--scope",
                "core-release",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        self.assertIn("scope=core-release", completed.stdout)

    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = guard.load_registry(ROOT)

    def test_repository_registry_is_complete(self) -> None:
        self.assertEqual([], guard.validate_registry(ROOT, self.registry))
        self.assertEqual([], guard.validate_specialist_ledgers(ROOT, self.registry))

    def test_protocol_unsafe_debt_is_registered_with_its_exact_active_scope(self) -> None:
        hygiene = json.loads((ROOT / "scripts/rust-hygiene-baseline.json").read_text(encoding="utf-8"))
        unsafe_entries = [entry for entry in hygiene["entries"] if entry["kind"].startswith("unsafe_")]
        registry_entry = next(entry for entry in self.registry["entries"] if entry["id"] == "ARC-UNSAFE-001")

        self.assertEqual(0, len(unsafe_entries))
        self.assertTrue(all(entry["path"].startswith("rocketmq-protocol/") for entry in unsafe_entries))
        self.assertEqual(len(unsafe_entries), registry_entry["scope_count"])

    def test_active_entry_requires_owner_and_removal_contract(self) -> None:
        registry = copy.deepcopy(self.registry)
        registry["entries"][0]["owner"] = ""
        del registry["entries"][0]["removal_condition"]

        findings = guard.validate_registry(ROOT, registry)

        self.assertIn("entry-schema", {finding.code for finding in findings})

    def test_missing_adr_fails_closed(self) -> None:
        registry = copy.deepcopy(self.registry)
        registry["entries"][0]["adr"] = "rocketmq-doc/en/missing-architecture-decision.md"

        findings = guard.validate_registry(ROOT, registry)

        self.assertIn("adr-missing", {finding.code for finding in findings})

    def test_resolved_source_check_rejects_regression(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.rs").write_text("ClientRuntime::new(config)", encoding="utf-8")
            (root / "adr.md").write_text("- Status: Accepted\n", encoding="utf-8")
            entry = {
                "id": "ARC-TEST-001",
                "class": "runtime_adapter",
                "owner": "runtime",
                "status": "resolved",
                "reason": "test",
                "adr": "adr.md",
                "removal_condition": "stays absent",
                "target_release": "2.0.0",
                "evidence": ["source.rs"],
                "scope_count": 0,
                "source_checks": [
                    {
                        "pattern": "ClientRuntime::new",
                        "max_count": 0,
                        "paths": ["source.rs"],
                    }
                ],
            }
            entries = [copy.deepcopy(value) for value in self.registry["entries"] if value["status"] == "active"]
            entries.append(entry)
            registry = {
                "schema_version": 1,
                "release_boundary": "2.0.0",
                "generated_document": "debt.md",
                "entries": entries,
            }
            for value in entries[:-1]:
                value["adr"] = "adr.md"
                value["evidence"] = ["source.rs"]

            findings = guard.validate_registry(root, registry)

        self.assertIn("resolved-debt-regressed", {finding.code for finding in findings})

    def test_render_is_deterministic_and_declares_real_boundaries(self) -> None:
        first = guard.render_document(self.registry)
        second = guard.render_document(self.registry)

        self.assertEqual(first, second)
        self.assertIn("Protocol, wire, persisted-layout", first)
        self.assertIn("2.0.0", first)


if __name__ == "__main__":
    unittest.main()

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
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import generate_telemetry_semantic_registry as generator  # noqa: E402
import telemetry_semantic_guard as guard  # noqa: E402


class TelemetrySemanticGuardTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = guard.load_json(guard.DEFAULT_REGISTRY)
        cls.inventory = guard.build_source_inventory()
        cls.fixtures = guard.load_json(
            SCRIPTS / "telemetry-semantic-guard-violations.json"
        )["fixtures"]

    def test_live_registry_matches_every_rust_signal(self) -> None:
        self.assertEqual([], guard.validate_registry(self.registry, self.inventory))
        self.assertEqual(125, len(self.inventory["metrics"]))
        self.assertEqual(4, len(self.inventory["spans"]))
        self.assertEqual(7, len(self.inventory["events"]))
        self.assertEqual(136, len(self.registry["signals"]))

    def test_log_filter_metrics_have_stable_registry_contracts(self) -> None:
        symbols = {
            "LOG_FILTER_RELOAD_TOTAL",
            "LOG_FILTER_ACTIVE",
            "LOG_FILTER_EXPIRY_TIMESTAMP_SECONDS",
            "LOG_FILTER_AUDIT_FAILURE_TOTAL",
            "LOG_FILTER_AUTO_RESTORE_FAILURE_TOTAL",
            "LOG_FILTER_ROLLBACK_FAILURE_TOTAL",
        }
        signals = {
            signal["source_symbol"]: signal
            for signal in self.registry["signals"]
            if signal["source_symbol"] in symbols
        }

        self.assertEqual(symbols, set(signals))
        for signal in signals.values():
            self.assertEqual("metric", signal["signal_type"])
            self.assertEqual("observability", signal["owner"])
            self.assertEqual("stable", signal["stability"])
            self.assertEqual("operational", signal["privacy"])
            self.assertEqual([], signal["attributes"])
            self.assertEqual(1, signal["cardinality_budget"])
            self.assertEqual({"strategy": "aggregate"}, signal["sampling"])

    def test_generator_is_deterministic(self) -> None:
        self.assertEqual(self.registry, generator.build_registry())

    def test_cli_reports_registered_inventory(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPTS / "telemetry_semantic_guard.py")],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("metrics=125 spans=4 logs=7 attributes=68", result.stdout)

    def test_committed_violation_fixtures_are_rejected(self) -> None:
        fixture_ids = {fixture["id"] for fixture in self.fixtures}
        self.assertEqual(
            {
                "unknown-signal",
                "undeclared-attribute",
                "high-cardinality-default",
                "unbounded-event-rate",
                "invalid-deprecation-window",
                "blocking-outage-enqueue",
                "weakened-byte-bound",
            },
            fixture_ids,
        )
        for fixture in self.fixtures:
            with self.subTest(fixture=fixture["id"]):
                invalid = guard.apply_fixture(self.registry, fixture)
                findings = guard.validate_registry(invalid, self.inventory)
                self.assertTrue(
                    any(fixture["expected"] in finding for finding in findings),
                    findings,
                )

    def test_missing_required_family_is_rejected(self) -> None:
        invalid = copy.deepcopy(self.registry)
        for signal in invalid["signals"]:
            if signal["family"] == "auth-mcp":
                signal["family"] = "request"
        findings = guard.validate_registry(invalid, self.inventory)
        self.assertTrue(any("missing required families" in finding for finding in findings))

    def test_sensitive_attribute_cannot_disable_redaction(self) -> None:
        invalid = copy.deepcopy(self.registry)
        attribute = next(
            item for item in invalid["attributes"] if item["privacy"] == "sensitive"
        )
        attribute["redaction"] = "none"
        findings = guard.validate_registry(invalid, self.inventory)
        self.assertTrue(any("requires hash or drop redaction" in finding for finding in findings))

    def test_forbidden_payload_attribute_is_rejected(self) -> None:
        invalid = copy.deepcopy(self.registry)
        invalid["attributes"].append(
            {
                "id": "message.body",
                "cardinality": "bounded",
                "privacy": "sensitive",
                "max_distinct": 1,
                "max_value_bytes": 64,
                "default_enabled": False,
                "redaction": "drop",
            }
        )
        findings = guard.validate_registry(invalid, self.inventory)
        self.assertTrue(any("forbidden sensitive attribute" in finding for finding in findings))


if __name__ == "__main__":
    unittest.main()

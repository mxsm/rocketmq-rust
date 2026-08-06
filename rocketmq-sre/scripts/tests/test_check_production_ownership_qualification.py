#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for target-environment production ownership qualification."""

from __future__ import annotations

import copy
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "check_production_ownership_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_production_ownership_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ProductionOwnershipQualificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)
        now = datetime.now(UTC).replace(microsecond=0)
        assignments = []
        for index, target in enumerate(self.manifest["required_targets"], 1):
            for responsibility in target["responsibilities"]:
                assignments.append(
                    {
                        "target_kind": target["kind"],
                        "target_id": target["id"],
                        "responsibility": responsibility,
                        "owner_ref": f"owner://team/{target['id']}/{responsibility}",
                        "approval_ref": f"approval://change/{index}/{responsibility}",
                        "on_call_ref": f"oncall://schedule/{target['id']}",
                        "verified": True,
                    }
                )
        self.report = {
            "schema_version": MODULE.REPORT_SCHEMA,
            "status": "passed",
            "source_revision": "a" * 40,
            "environment_ref": "environment://production/primary",
            "observed_at": now.isoformat().replace("+00:00", "Z"),
            "expires_at": (now + timedelta(days=30)).isoformat().replace("+00:00", "Z"),
            "ownership_qualified": True,
            "production_certified": False,
            "grants_execution_authority": False,
            "personal_data_recorded": False,
            "secrets_recorded": False,
            "message_bodies_recorded": False,
            "assignments": assignments,
        }

    def test_committed_manifest_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])

    def test_complete_target_report_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_report(self.report, self.manifest), [])

    def test_missing_duplicate_and_unexpected_bindings_fail_closed(self) -> None:
        report = copy.deepcopy(self.report)
        removed = report["assignments"].pop()
        report["assignments"].append(copy.deepcopy(report["assignments"][0]))
        unexpected = copy.deepcopy(removed)
        unexpected["target_id"] = "unregistered-target"
        report["assignments"].append(unexpected)

        findings = MODULE.validate_report(report, self.manifest)

        self.assertTrue(any("missing required ownership bindings" in finding for finding in findings))
        self.assertTrue(any("duplicate ownership binding" in finding for finding in findings))
        self.assertTrue(any("unexpected ownership binding" in finding for finding in findings))

    def test_placeholder_personal_and_sensitive_values_are_rejected(self) -> None:
        report = copy.deepcopy(self.report)
        report["assignments"][0]["owner_ref"] = "owner://team/TBD"
        report["assignments"][1]["approval_ref"] = "operator@example.com"
        report["note"] = "Bearer qualification-token"

        findings = MODULE.validate_report(report, self.manifest)

        self.assertTrue(any("placeholder" in finding for finding in findings))
        self.assertTrue(any("opaque reference" in finding for finding in findings))
        self.assertIn("report contains credential-like material", findings)

    def test_expired_or_overlong_attestation_is_rejected(self) -> None:
        report = copy.deepcopy(self.report)
        now = datetime.now(UTC).replace(microsecond=0)
        report["observed_at"] = (now - timedelta(days=100)).isoformat().replace("+00:00", "Z")
        report["expires_at"] = (now - timedelta(days=1)).isoformat().replace("+00:00", "Z")

        findings = MODULE.validate_report(report, self.manifest, now=now)

        self.assertTrue(any("validity exceeds" in finding for finding in findings))
        self.assertTrue(any("attestation is expired" in finding for finding in findings))

    def test_ownership_report_cannot_certify_or_authorize_the_product(self) -> None:
        report = copy.deepcopy(self.report)
        report["production_certified"] = True
        report["grants_execution_authority"] = True

        findings = MODULE.validate_report(report, self.manifest)

        self.assertIn("production_certified must remain False", findings)
        self.assertIn("grants_execution_authority must remain False", findings)

    def test_manifest_drift_and_malformed_targets_are_rejected(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["production_certified"] = True
        manifest["required_targets"][0] = "malformed"
        manifest["required_targets"][2]["responsibilities"] = [{}]
        manifest["required_targets"].append(copy.deepcopy(manifest["required_targets"][1]))
        manifest["report_contract"]["allowed_roots"] = [r"C:\temp"]

        findings = MODULE.validate_manifest(manifest)

        self.assertIn("production_certified must remain False", findings)
        self.assertIn("required_targets[0] must be an object", findings)
        self.assertIn("required_targets[2] is malformed", findings)
        self.assertTrue(any("duplicate required target" in finding for finding in findings))
        self.assertIn("required target ownership matrix drifted", findings)
        self.assertIn("report_contract drifted from the machine-local privacy boundary", findings)

    def test_malformed_report_fields_are_rejected(self) -> None:
        report = copy.deepcopy(self.report)
        report["source_revision"] = "short"
        report["environment_ref"] = "production-primary"
        report["observed_at"] = "2026-08-07T00:00:00"
        report["expires_at"] = "not-a-time"
        report["assignments"] = [
            "malformed",
            {"target_kind": "action"},
            {
                "target_kind": "action",
                "target_id": "proxy.restart_one.v1",
                "responsibility": "sre_owner",
                "owner_ref": "bad-owner",
                "approval_ref": "bad-approval",
                "on_call_ref": "bad-oncall",
                "verified": False,
            },
        ]

        findings = MODULE.validate_report(report, self.manifest)

        self.assertIn("source_revision must be a full Git revision", findings)
        self.assertIn("environment_ref must be an opaque environment:// reference", findings)
        self.assertIn("observed_at must include a timezone", findings)
        self.assertIn("expires_at must be an RFC 3339 timestamp", findings)
        self.assertIn("assignments[0] must be an object", findings)
        self.assertIn("assignments[1] has a malformed ownership binding", findings)
        self.assertIn("assignments[2].verified must be true", findings)

    def test_cli_accepts_manifest_only_and_rejects_unsafe_report_path(self) -> None:
        output = io.StringIO()
        with patch.object(sys, "argv", [str(SCRIPT)]), redirect_stdout(output):
            self.assertEqual(MODULE.main(), 0)
        self.assertIn("report=not_provided", output.getvalue())

        output = io.StringIO()
        with (
            patch.object(sys, "argv", [str(SCRIPT), "--report", r"C:\temp\ownership.json"]),
            redirect_stdout(output),
        ):
            self.assertEqual(MODULE.main(), 1)
        self.assertIn("restricted to the D: or F:", output.getvalue())

    def test_cli_rejects_non_object_manifest(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as temporary:
            manifest = Path(temporary) / "invalid.json"
            manifest.write_text(json.dumps([]), encoding="utf-8")
            output = io.StringIO()
            with (
                patch.object(sys, "argv", [str(SCRIPT), "--manifest", str(manifest)]),
                redirect_stdout(output),
            ):
                self.assertEqual(MODULE.main(), 1)
        self.assertIn("unable_to_load_manifest", output.getvalue())

    def test_machine_local_report_paths_are_restricted_to_data_drives(self) -> None:
        self.assertEqual(MODULE.validate_report_path(r"D:\rocketmq-sre-evidence\ownership.json"), [])
        self.assertEqual(MODULE.validate_report_path(r"F:\rocketmq-sre-evidence\ownership.json"), [])
        self.assertTrue(MODULE.validate_report_path(r"C:\temp\ownership.json"))
        self.assertTrue(MODULE.validate_report_path("rocketmq-sre/config/ownership.json"))
        self.assertTrue(
            MODULE.validate_report_path(r"D:\rocketmq-sre-evidence\..\escaped\ownership.json")
        )


if __name__ == "__main__":
    unittest.main()

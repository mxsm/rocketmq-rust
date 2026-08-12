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

import argparse
import copy
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import message_path_ab as ab  # noqa: E402
import message_path_qualification as qualification  # noqa: E402


SERVICES = ("broker", "namesrv", "controller", "proxy", "mcp")


class MessagePathAbTest(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = qualification.load_json(qualification.DEFAULT_POLICY)

    def create_inputs(self, root: Path) -> argparse.Namespace:
        baseline_map = {
            service: f"registry.local/rocketmq/{service}@sha256:{str(index + 1) * 64}"
            for index, service in enumerate(SERVICES)
        }
        candidate_map = {
            service: f"registry.local/rocketmq/{service}@sha256:{chr(97 + index) * 64}"
            for index, service in enumerate(SERVICES)
        }
        baseline_path = root / "baseline-images.json"
        candidate_path = root / "candidate-images.json"
        qualification.write_json(baseline_path, baseline_map)
        qualification.write_json(candidate_path, candidate_map)
        effective_config = root / "effective-config.json"
        effective_config.write_text('{"flush":"sync"}\n', encoding="utf-8")
        provenance = {
            "schema_version": 1,
            "artifact_kind": "rocketmq_local_evidence_image_provenance",
            "baseline": {
                "commit": "1" * 40,
                "image_map_sha256": ab.digest(baseline_path),
                "deployment_digest": "sha256:" + "3" * 64,
            },
            "candidate": {
                "commit": "2" * 40,
                "image_map_sha256": ab.digest(candidate_path),
                "deployment_digest": "sha256:" + "4" * 64,
            },
        }
        provenance_path = root / "image-provenance.json"
        qualification.write_json(provenance_path, provenance)
        return argparse.Namespace(
            policy=qualification.DEFAULT_POLICY,
            run_id="paired-ab-test",
            baseline_commit="1" * 40,
            candidate_commit="2" * 40,
            driver_commit="5" * 40,
            baseline_image_map=baseline_path,
            candidate_image_map=candidate_path,
            image_provenance=provenance_path,
            effective_config=effective_config,
            target_id="kind-engineering-rehearsal",
            cluster_uid="cluster-uid-1",
            namesrv="127.0.0.1:19876",
            topic_prefix="MessagePathAB",
            durability_contract="strict-sync-required-ack-clean-election",
            repetitions=5,
            seed=42,
        )

    def test_plan_is_deterministic_and_pairs_every_subject(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            args = self.create_inputs(Path(temporary))
            first = ab.create_plan(args)
            second = ab.create_plan(args)

        self.assertEqual(first["arms"], second["arms"])
        self.assertEqual(12, len(first["arms"]))
        self.assertEqual([], ab.validate_plan(first, self.policy))
        for phase, count in (("warmup", 1), ("sample", 5)):
            for ordinal in range(1, count + 1):
                roles = [
                    arm["role"]
                    for arm in first["arms"]
                    if arm["phase"] == phase and arm["ordinal"] == ordinal
                ]
                self.assertCountEqual(("baseline", "candidate"), roles)

    def test_plan_rejects_mutable_image_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = self.create_inputs(root)
            image_map = qualification.load_json(args.baseline_image_map)
            image_map["broker"] = "registry.local/rocketmq/broker:latest"
            qualification.write_json(args.baseline_image_map, image_map)

            with self.assertRaisesRegex(ab.AbError, "manifest digest"):
                ab.create_plan(args)

    @staticmethod
    def measurement(plan: dict, arm: dict, workload: dict, value: float) -> dict:
        topic = f"{plan['target']['topic_prefix']}_{arm['role'][0]}{arm['phase'][0]}{arm['ordinal']}_{workload['id'].replace('-', '_')}"
        sample_id = f"{plan['run_id']}-{arm['role']}-{arm['phase']}-{arm['ordinal']}-{workload['id']}"
        count = workload["message_count"]
        return {
            "schema_version": 1,
            "artifact_kind": "rocketmq_message_path_measurement",
            "run_id": sample_id,
            "scenario": workload["scenario"],
            "operation": "consume" if workload["scenario"] == "lite-pull" else "send",
            "target": {"namesrv_addr": plan["target"]["namesrv_addr"], "topic": topic},
            "workload": {
                "message_count": count,
                "message_size_bytes": workload["message_size_bytes"],
                "batch_size": workload["batch_size"],
            },
            "result": {
                "duration_us": 1_000_000,
                "success_count": count,
                "send_failed_count": 0,
                "response_failed_count": 0,
                "throughput_messages_per_second": value,
                "payload_mib_per_second": value / 1000,
                "latency_us": {
                    "samples": count,
                    "average": value,
                    "p50": value,
                    "p95": value,
                    "p99": value,
                    "p999": value,
                    "max": value,
                },
            },
        }

    def write_arms(self, plan: dict, plan_path: Path, output: Path) -> None:
        for arm in plan["arms"]:
            value = 1000.0 + arm["ordinal"] + (20.0 if arm["role"] == "candidate" else 0.0)
            records = [
                {
                    "workload": workload["id"],
                    "measurement": self.measurement(plan, arm, workload, value),
                }
                for workload in self.policy["modes"]["release"]["workloads"]
            ]
            report = {
                "schema_version": 1,
                "artifact_kind": "rocketmq_message_path_ab_arm",
                "plan_sha256": ab.digest(plan_path),
                "arm": arm,
                "status": "pass",
                "failures": [],
                "records": records,
            }
            qualification.write_json(ab.arm_directory(output, arm) / "arm-report.json", report)

    def test_assemble_requires_all_arms_and_binds_report_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = self.create_inputs(root)
            plan = ab.create_plan(args)
            plan_path = root / "ab-plan.json"
            qualification.write_json(plan_path, plan)
            output = root / "evidence"
            self.write_arms(plan, plan_path, output)
            with mock.patch.object(
                qualification,
                "environment_record",
                return_value={"hardware_id": "sha256:" + "6" * 64},
            ):
                comparison, comparison_path = ab.assemble(plan, plan_path, output)

            baseline_path = output / "baseline" / "measurement-set.json"
            candidate_path = output / "candidate" / "measurement-set.json"
            self.assertEqual("pass", comparison["status"])
            self.assertTrue(comparison["release_comparison_qualified"])
            self.assertEqual(ab.digest(plan_path), comparison["ab_plan_sha256"])
            self.assertEqual(ab.digest(baseline_path), comparison["baseline"]["report_sha256"])
            self.assertEqual(ab.digest(candidate_path), comparison["candidate"]["report_sha256"])
            self.assertTrue(comparison_path.is_file())
            self.assertEqual(9, len(comparison["comparisons"]))
            self.assertIn("paired_bootstrap_95_ci", comparison["comparisons"][0])
            baseline = qualification.load_json(baseline_path)
            self.assertTrue(any(item["path"].endswith("arm-report.json") for item in baseline["artifacts"]))

    def test_assemble_does_not_replace_a_missing_or_failed_arm(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = self.create_inputs(root)
            plan = ab.create_plan(args)
            plan_path = root / "ab-plan.json"
            qualification.write_json(plan_path, plan)
            output = root / "evidence"
            self.write_arms(plan, plan_path, output)
            missing = ab.arm_directory(output, plan["arms"][-1]) / "arm-report.json"
            missing.unlink()

            with self.assertRaisesRegex(ab.AbError, "missing arm report"):
                ab.assemble(plan, plan_path, output)

    def test_validation_rejects_plan_with_duplicate_arm(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            plan = ab.create_plan(self.create_inputs(Path(temporary)))
        plan["arms"][-1] = copy.deepcopy(plan["arms"][0])
        plan["arms"][-1]["index"] = len(plan["arms"]) - 1

        findings = ab.validate_plan(plan, self.policy)

        self.assertTrue(any("every required arm" in finding for finding in findings))

    def test_powershell_runner_exposes_non_destructive_validation(self) -> None:
        powershell = shutil.which("pwsh") or shutil.which("powershell")
        if powershell is None:
            self.skipTest("PowerShell is unavailable")
        result = subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(ROOT / "scripts" / "run-message-path-ab.ps1"),
                "-Mode",
                "Validate",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("MESSAGE_PATH_AB_RUNNER_VALID", result.stdout)


if __name__ == "__main__":
    unittest.main()

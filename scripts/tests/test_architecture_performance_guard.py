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
import math
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import architecture_performance_guard as guard  # noqa: E402


class ArchitecturePerformanceGuardTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = guard.load_json(guard.DEFAULT_PROFILES)
        cls.empty_exceptions = guard.load_json(guard.DEFAULT_EXCEPTIONS)

    def build_report(self, commit: str, *, kind: str = "fixture") -> dict:
        report = {
            "schema_version": 1,
            "run_id": f"fixture-{commit[:8]}",
            "report_kind": kind,
            "profile_schema_sha256": guard.canonical_sha256(self.policy),
            "generated_at": "2026-07-18T00:00:00Z",
            "git": {"commit": commit, "dirty": False},
            "environment": {
                "hardware_id": "fixture-host-sha256:0123456789abcdef",
                "os": "fixture-os",
                "kernel": "fixture-kernel",
                "architecture": "x86_64",
                "cpu_model": "fixture-cpu",
                "logical_cpus": 16,
                "memory_bytes": 34359738368,
                "filesystem": "fixture-fs",
                "rustc": "rustc fixture",
                "cargo": "cargo fixture",
                "build_profile": "release",
                "features": ["fixture"],
                "measurement_method": "synthetic guard fixture; not benchmark evidence",
            },
            "correctness_checks": [
                {"id": check_id, "status": "pass", "evidence_sha256": "a" * 64}
                for check_id in self.policy["required_correctness_checks"]
            ],
            "profiles": [],
        }
        for profile in self.policy["profiles"]:
            metrics = guard.expanded_metrics(self.policy, profile)
            report_profile = {"id": profile["id"], "measurement_status": "measured", "variants": []}
            hypothesis_values = {
                hypothesis["metric"]: float(hypothesis["value"])
                for hypothesis in profile.get("hypotheses", [])
                if hypothesis["operator"] in {"max", "min"}
            }
            for variant in profile["variants"]:
                measurements = {}
                for metric in metrics:
                    value = 100.0 if metric["comparison"] == "relative" else hypothesis_values.get(metric["id"], 1.0)
                    measurements[metric["id"]] = {"samples": [value] * self.policy["minimum_samples"]}
                report_profile["variants"].append(
                    {
                        "id": variant["id"],
                        "parameters": copy.deepcopy(variant["parameters"]),
                        "raw_data": {
                            "path": f"target/architecture-refactor/M10/{profile['id']}-{variant['id']}.json",
                            "sha256": "b" * 64,
                        },
                        "metrics": measurements,
                    }
                )
            report["profiles"].append(report_profile)
        return report

    @staticmethod
    def metric(report: dict, profile: str, variant: str, metric: str) -> dict:
        report_profile = next(item for item in report["profiles"] if item["id"] == profile)
        report_variant = next(item for item in report_profile["variants"] if item["id"] == variant)
        return report_variant["metrics"][metric]

    def evaluate(self, baseline: dict, candidate: dict, exceptions: dict | None = None) -> dict:
        return guard.evaluate_documents(
            self.policy,
            baseline,
            candidate,
            exceptions or self.empty_exceptions,
            allow_fixture=True,
            now=datetime(2026, 7, 18, tzinfo=timezone.utc),
        )

    def test_live_profile_inventory_and_cli_validation_pass(self) -> None:
        self.assertEqual([], guard.validate_profile_policy(self.policy))
        self.assertEqual(guard.REQUIRED_PROFILE_IDS, {profile["id"] for profile in self.policy["profiles"]})
        self.assertEqual(11, sum(len(profile["variants"]) for profile in self.policy["profiles"]))

        result = subprocess.run(
            [sys.executable, str(SCRIPTS / "architecture_performance_guard.py"), "--validate-profiles"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("profiles=8 variants=11 metric_contracts=50", result.stdout)

    def test_collection_contract_preserves_target_hardware_requirement(self) -> None:
        invalid = copy.deepcopy(self.policy)
        invalid["profiles"][0]["collection"]["status"] = "measured"
        findings = guard.validate_profile_policy(invalid)
        self.assertTrue(any("target-hardware requirement" in finding for finding in findings))

        invalid = copy.deepcopy(self.policy)
        invalid["profiles"][0]["collection"]["required_outputs"].remove("raw_samples")
        findings = guard.validate_profile_policy(invalid)
        self.assertTrue(any("evidence contract" in finding for finding in findings))

    def test_all_profiles_pass_with_stable_samples_and_non_gating_hypotheses(self) -> None:
        baseline = self.build_report("1" * 40)
        candidate = self.build_report("2" * 40)
        for profile in self.policy["profiles"]:
            contracts = guard.expanded_metrics(self.policy, profile)
            for variant in profile["variants"]:
                for contract in contracts:
                    if contract["comparison"] != "relative":
                        continue
                    value = 98.0 if contract["direction"] == "higher" else 102.0
                    self.metric(candidate, profile["id"], variant["id"], contract["id"])["samples"] = [value] * 5

        result = self.evaluate(baseline, candidate)
        self.assertEqual("pass", result["status"], result["failures"])
        self.assertEqual(66, len(result["comparisons"]))
        self.assertTrue(any(item["status"] == "missed" for item in result["hypotheses"]))
        self.assertTrue(all(item["gate"] is False for item in result["hypotheses"]))

    def test_regression_over_five_percent_fails_in_both_directions(self) -> None:
        baseline = self.build_report("1" * 40)
        candidate = self.build_report("2" * 40)
        self.metric(candidate, "local-append", "producers-1", "throughput_per_second")["samples"] = [94.0] * 5
        self.metric(candidate, "local-append", "producers-1", "p99_latency_us")["samples"] = [106.0] * 5

        result = self.evaluate(baseline, candidate)
        self.assertEqual("fail", result["status"])
        self.assertEqual(2, len([item for item in result["comparisons"] if item["status"] == "fail-regression"]))

    def test_noise_is_a_failure_and_cannot_expand_the_budget(self) -> None:
        baseline = self.build_report("1" * 40)
        candidate = self.build_report("2" * 40)
        self.metric(candidate, "local-append", "producers-1", "throughput_per_second")["samples"] = [50, 75, 100, 125, 150]

        result = self.evaluate(baseline, candidate)
        self.assertEqual("fail", result["status"])
        self.assertTrue(any("noise stability limits" in finding for finding in result["failures"]))

        candidate = self.build_report("2" * 40)
        self.metric(candidate, "local-append", "producers-1", "throughput_per_second")["samples"] = [
            100,
            100,
            100,
            100,
            200,
        ]
        result = self.evaluate(baseline, candidate)
        self.assertEqual("fail", result["status"])
        self.assertTrue(any("sample deviation" in finding for finding in result["failures"]))

    def test_environment_and_required_metadata_must_match(self) -> None:
        baseline = self.build_report("1" * 40)
        candidate = self.build_report("2" * 40)
        candidate["environment"]["filesystem"] = "different-fs"
        result = self.evaluate(baseline, candidate)
        self.assertEqual("fail", result["status"])
        self.assertIn("baseline and candidate environments differ", result["failures"][0])

        invalid = self.build_report("2" * 40)
        del invalid["environment"]["kernel"]
        self.metric(invalid, "local-append", "producers-1", "throughput_per_second")["samples"][0] = math.inf
        result = self.evaluate(baseline, invalid)
        self.assertEqual("fail", result["status"])
        self.assertTrue(any("environment.kernel is required" in finding for finding in result["failures"]))
        self.assertTrue(any("non-finite samples" in finding for finding in result["failures"]))

    def test_correctness_failure_is_never_suppressed_by_performance_exception(self) -> None:
        baseline = self.build_report("1" * 40)
        candidate = self.build_report("2" * 40)
        candidate["correctness_checks"][0]["status"] = "fail"
        exception = self.exception_document(allowed=100.0)

        result = self.evaluate(baseline, candidate, exception)
        self.assertEqual("fail", result["status"])
        self.assertTrue(any("correctness-first check" in finding for finding in result["failures"]))

    def exception_document(self, *, allowed: float = 10.0, expires: str = "2099-01-01T00:00:00Z") -> dict:
        return {
            "schema_version": 1,
            "exceptions": [
                {
                    "profile": "local-append",
                    "variant": "producers-1",
                    "metric": "throughput_per_second",
                    "owner": "storage-performance",
                    "reason": "temporary measured regression",
                    "approved_by": "human-architect",
                    "expires_at": expires,
                    "rollback_config": "disable candidate append adapter",
                    "allowed_regression_percent": allowed,
                }
            ],
        }

    def test_scoped_unexpired_exception_is_reported_and_expired_exception_fails(self) -> None:
        baseline = self.build_report("1" * 40)
        candidate = self.build_report("2" * 40)
        self.metric(candidate, "local-append", "producers-1", "throughput_per_second")["samples"] = [92.0] * 5

        result = self.evaluate(baseline, candidate, self.exception_document())
        self.assertEqual("pass", result["status"], result["failures"])
        comparison = next(
            item
            for item in result["comparisons"]
            if item["profile"] == "local-append"
            and item["variant"] == "producers-1"
            and item["metric"] == "throughput_per_second"
        )
        self.assertEqual("approved-exception", comparison["status"])
        self.assertEqual(1, len(result["approved_exceptions"]))

        expired = self.evaluate(
            baseline,
            candidate,
            self.exception_document(expires="2026-07-17T23:59:59Z"),
        )
        self.assertEqual("fail", expired["status"])
        self.assertTrue(any("expired" in finding for finding in expired["failures"]))

        invalid_target = self.exception_document()
        invalid_target["exceptions"][0]["metric"] = "fsync_per_ack"
        invalid_target["exceptions"][0]["profile"] = "sync-flush"
        invalid_target["exceptions"][0]["variant"] = "concurrency-64"
        result = self.evaluate(baseline, candidate, invalid_target)
        self.assertEqual("fail", result["status"])
        self.assertTrue(any("non-regression observation" in finding for finding in result["failures"]))

    def test_fixture_reports_require_explicit_opt_in(self) -> None:
        baseline = self.build_report("1" * 40)
        candidate = self.build_report("2" * 40)
        result = guard.evaluate_documents(self.policy, baseline, candidate, self.empty_exceptions)
        self.assertEqual("fail", result["status"])
        self.assertTrue(any("--allow-fixture" in finding for finding in result["failures"]))

    def test_missing_profile_raw_hash_dirty_measurement_and_unknown_exception_fail(self) -> None:
        baseline = self.build_report("1" * 40)
        candidate = self.build_report("2" * 40)
        candidate["profiles"].pop()
        result = self.evaluate(baseline, candidate)
        self.assertEqual("fail", result["status"])
        self.assertTrue(any("profile inventory mismatch" in finding for finding in result["failures"]))

        candidate = self.build_report("2" * 40)
        candidate["profiles"][0]["variants"][0]["raw_data"]["sha256"] = "not-a-hash"
        result = self.evaluate(baseline, candidate)
        self.assertEqual("fail", result["status"])
        self.assertTrue(any("raw_data.sha256 is invalid" in finding for finding in result["failures"]))

        measurement = self.build_report("2" * 40, kind="measurement")
        measurement["git"]["dirty"] = True
        result = guard.evaluate_documents(self.policy, baseline, measurement, self.empty_exceptions, allow_fixture=True)
        self.assertEqual("fail", result["status"])
        self.assertTrue(any("clean Git tree" in finding for finding in result["failures"]))

        unknown = self.exception_document()
        unknown["exceptions"][0]["profile"] = "missing-profile"
        result = self.evaluate(baseline, self.build_report("2" * 40), unknown)
        self.assertEqual("fail", result["status"])
        self.assertTrue(any("unknown profile" in finding for finding in result["failures"]))

    def test_cli_writes_hash_bound_comparison_report(self) -> None:
        baseline = self.build_report("1" * 40)
        candidate = self.build_report("2" * 40)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline_path = root / "baseline.json"
            candidate_path = root / "candidate.json"
            output_path = root / "comparison.json"
            baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
            candidate_path.write_text(json.dumps(candidate), encoding="utf-8")
            exit_code = guard.main(
                [
                    "--baseline",
                    str(baseline_path),
                    "--candidate",
                    str(candidate_path),
                    "--output",
                    str(output_path),
                    "--allow-fixture",
                ]
            )
            self.assertEqual(0, exit_code)
            output = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual("pass", output["status"])
            self.assertRegex(output["baseline_sha256"], r"^[0-9a-f]{64}$")
            self.assertRegex(output["candidate_sha256"], r"^[0-9a-f]{64}$")
            self.assertRegex(output["environment_sha256"], r"^[0-9a-f]{64}$")


if __name__ == "__main__":
    unittest.main()

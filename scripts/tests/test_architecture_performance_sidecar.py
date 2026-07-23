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
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import architecture_performance_guard as guard  # noqa: E402
import architecture_performance_sidecar as sidecar  # noqa: E402


class ArchitecturePerformanceSidecarTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = guard.load_json(guard.DEFAULT_PROFILES)

    def build_manifest(self) -> dict:
        return {
            "schema_version": 1,
            "environment": {
                "hardware_id": "target-host-a-2026",
                "filesystem": "ext4 /benchmark",
                "build_profile": "release",
                "features": ["rocksdb_store", "tieredstore"],
                "measurement_method": "target hardware sidecar protocol v1",
            },
            "correctness_checks": [
                {
                    "id": check_id,
                    "command": ["runner", "correctness", check_id],
                    "timeout_seconds": 60,
                }
                for check_id in self.policy["required_correctness_checks"]
            ],
            "profiles": [
                {
                    "id": profile["id"],
                    "variants": [
                        {
                            "id": variant["id"],
                            "command": [
                                "runner",
                                "measurement",
                                profile["id"],
                                variant["id"],
                            ],
                            "timeout_seconds": 60,
                        }
                        for variant in profile["variants"]
                    ],
                }
                for profile in self.policy["profiles"]
            ],
        }

    @staticmethod
    def environment() -> dict:
        return {
            "hardware_id": "target-host-a-2026",
            "os": "Test OS",
            "kernel": "test-kernel",
            "architecture": "x86_64",
            "cpu_model": "test-cpu",
            "logical_cpus": 16,
            "memory_bytes": 34_359_738_368,
            "filesystem": "ext4 /benchmark",
            "rustc": "rustc 1.94.0",
            "cargo": "cargo 1.94.0",
            "build_profile": "release",
            "features": ["rocksdb_store", "tieredstore"],
            "measurement_method": "target hardware sidecar protocol v1",
        }

    def successful_executor(self, calls: list[list[str]]) -> sidecar.CommandExecutor:
        profile_map = guard.policy_maps(self.policy)

        def execute(command: list[str], _cwd: Path, _timeout: int) -> sidecar.CommandResult:
            calls.append(command)
            if command[1] == "correctness":
                stdout = "correctness passed"
            else:
                profile_id = command[2]
                variant_id = command[3]
                metrics = {
                    metric["id"]: {
                        "samples": [100.0] * self.policy["minimum_samples"],
                    }
                    for metric in guard.expanded_metrics(
                        self.policy,
                        profile_map[profile_id],
                    )
                }
                stdout = json.dumps(
                    {
                        "schema_version": 1,
                        "profile": profile_id,
                        "variant": variant_id,
                        "metrics": metrics,
                    }
                )
            return sidecar.CommandResult(
                exit_code=0,
                stdout=stdout,
                stderr="",
                started_at="2026-07-23T00:00:00Z",
                finished_at="2026-07-23T00:00:01Z",
                duration_ms=1_000,
            )

        return execute

    def test_complete_manifest_collects_correctness_before_all_variants(self) -> None:
        manifest = self.build_manifest()
        self.assertEqual([], sidecar.validate_manifest(manifest, self.policy))
        calls: list[list[str]] = []
        with tempfile.TemporaryDirectory() as directory:
            repository_root = Path(directory)
            audit_root = repository_root / "target" / "architecture-refactor" / "M10"
            report, report_path = sidecar.collect_report(
                policy=self.policy,
                manifest=manifest,
                run_id="candidate-target-a",
                output_dir=audit_root / "candidate-target-a",
                audit_root=audit_root,
                repository_root=repository_root,
                git_commit="2" * 40,
                git_dirty=False,
                environment=self.environment(),
                executor=self.successful_executor(calls),
            )

            check_count = len(self.policy["required_correctness_checks"])
            variant_count = sum(len(profile["variants"]) for profile in self.policy["profiles"])
            self.assertEqual(4, check_count)
            self.assertEqual(11, variant_count)
            self.assertTrue(all(command[1] == "correctness" for command in calls[:check_count]))
            self.assertTrue(all(command[1] == "measurement" for command in calls[check_count:]))
            self.assertEqual(check_count + variant_count, len(calls))
            self.assertEqual([], guard.validate_report(report, self.policy, "sidecar", False))
            self.assertTrue(report_path.is_file())
            for profile in report["profiles"]:
                for variant in profile["variants"]:
                    raw_path = repository_root / variant["raw_data"]["path"]
                    self.assertEqual(guard.file_sha256(raw_path), variant["raw_data"]["sha256"])

    def test_manifest_rejects_missing_variant_and_placeholder_commands(self) -> None:
        invalid = self.build_manifest()
        invalid["profiles"][0]["variants"].pop()
        findings = sidecar.validate_manifest(invalid, self.policy)
        self.assertTrue(any("variant inventory mismatch" in finding for finding in findings))

        template = sidecar.manifest_template(self.policy)
        findings = sidecar.validate_manifest(template, self.policy)
        self.assertTrue(any("placeholder or fixture marker" in finding for finding in findings))

    def test_correctness_failure_stops_before_measurement(self) -> None:
        calls: list[list[str]] = []

        def fail_first(command: list[str], _cwd: Path, _timeout: int) -> sidecar.CommandResult:
            calls.append(command)
            return sidecar.CommandResult(
                exit_code=1,
                stdout="",
                stderr="failed",
                started_at="2026-07-23T00:00:00Z",
                finished_at="2026-07-23T00:00:01Z",
                duration_ms=1_000,
            )

        with tempfile.TemporaryDirectory() as directory:
            repository_root = Path(directory)
            audit_root = repository_root / "target" / "architecture-refactor" / "M10"
            output_dir = audit_root / "failed-correctness"
            with self.assertRaisesRegex(sidecar.SidecarError, "correctness-first"):
                sidecar.collect_report(
                    policy=self.policy,
                    manifest=self.build_manifest(),
                    run_id="failed-correctness",
                    output_dir=output_dir,
                    audit_root=audit_root,
                    repository_root=repository_root,
                    git_commit="2" * 40,
                    git_dirty=False,
                    environment=self.environment(),
                    executor=fail_first,
                )
            self.assertEqual(1, len(calls))
            self.assertEqual("correctness", calls[0][1])
            self.assertFalse((output_dir / "measurement-report.json").exists())

    def test_malformed_measurement_is_retained_but_not_reported_as_valid(self) -> None:
        calls: list[list[str]] = []
        successful = self.successful_executor(calls)

        def malformed(command: list[str], cwd: Path, timeout: int) -> sidecar.CommandResult:
            result = successful(command, cwd, timeout)
            if command[1] != "measurement":
                return result
            return sidecar.CommandResult(
                exit_code=0,
                stdout=json.dumps(
                    {
                        "schema_version": 1,
                        "profile": command[2],
                        "variant": command[3],
                        "metrics": {},
                    }
                ),
                stderr="",
                started_at=result.started_at,
                finished_at=result.finished_at,
                duration_ms=result.duration_ms,
            )

        with tempfile.TemporaryDirectory() as directory:
            repository_root = Path(directory)
            audit_root = repository_root / "target" / "architecture-refactor" / "M10"
            output_dir = audit_root / "malformed"
            with self.assertRaisesRegex(sidecar.SidecarError, "metric inventory mismatch"):
                sidecar.collect_report(
                    policy=self.policy,
                    manifest=self.build_manifest(),
                    run_id="malformed-measurement",
                    output_dir=output_dir,
                    audit_root=audit_root,
                    repository_root=repository_root,
                    git_commit="2" * 40,
                    git_dirty=False,
                    environment=self.environment(),
                    executor=malformed,
                )
            raw = (
                output_dir
                / "raw"
                / "profiles"
                / self.policy["profiles"][0]["id"]
                / f"{self.policy['profiles'][0]['variants'][0]['id']}.json"
            )
            self.assertTrue(raw.is_file())
            self.assertFalse((output_dir / "measurement-report.json").exists())

    def test_dirty_git_and_non_audit_output_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository_root = Path(directory)
            audit_root = repository_root / "target" / "architecture-refactor" / "M10"
            common = {
                "policy": self.policy,
                "manifest": self.build_manifest(),
                "run_id": "candidate-target-a",
                "audit_root": audit_root,
                "repository_root": repository_root,
                "git_commit": "2" * 40,
                "environment": self.environment(),
                "executor": self.successful_executor([]),
            }
            with self.assertRaisesRegex(sidecar.SidecarError, "clean Git tree"):
                sidecar.collect_report(
                    **common,
                    output_dir=audit_root / "dirty",
                    git_dirty=True,
                )
            self.assertFalse((audit_root / "dirty").exists())

            with self.assertRaisesRegex(sidecar.SidecarError, "ignored audit root"):
                sidecar.collect_report(
                    **common,
                    output_dir=repository_root / "tracked-evidence",
                    git_dirty=False,
                )

    def test_git_state_is_rechecked_after_collection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository_root = Path(directory)
            audit_root = repository_root / "target" / "architecture-refactor" / "M10"
            output_dir = audit_root / "commit-drift"
            with self.assertRaisesRegex(sidecar.SidecarError, "changed during measurement"):
                sidecar.collect_report(
                    policy=self.policy,
                    manifest=self.build_manifest(),
                    run_id="commit-drift",
                    output_dir=output_dir,
                    audit_root=audit_root,
                    repository_root=repository_root,
                    git_commit="2" * 40,
                    git_dirty=False,
                    environment=self.environment(),
                    executor=self.successful_executor([]),
                    post_collection_git_snapshot=lambda _root: ("3" * 40, False),
                )
            self.assertFalse((output_dir / "measurement-report.json").exists())

    def test_sidecar_protocol_cannot_be_removed_from_policy(self) -> None:
        invalid = copy.deepcopy(self.policy)
        del invalid["sidecar_protocol"]
        findings = guard.validate_profile_policy(invalid)
        self.assertIn(
            "performance sidecar protocol differs from the approved fail-closed contract",
            findings,
        )
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(guard, "ROOT", Path(directory)):
                findings = guard.validate_profile_policy(self.policy)
        self.assertIn("performance sidecar runner is missing", findings)

    def test_command_executor_terminates_a_timed_out_process_group(self) -> None:
        result = sidecar.execute_command(
            [sys.executable, "-c", "import time; time.sleep(10)"],
            ROOT,
            1,
        )
        self.assertTrue(result.timed_out)
        self.assertEqual(124, result.exit_code)
        self.assertLess(result.duration_ms, 5_000)


if __name__ == "__main__":
    unittest.main()

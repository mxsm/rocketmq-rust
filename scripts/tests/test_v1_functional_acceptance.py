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

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]


def load_runner():
    path = ROOT / "scripts/v1_functional_acceptance.py"
    spec = importlib.util.spec_from_file_location("v1_functional_acceptance", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class V1FunctionalAcceptanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = load_runner()

    def test_repository_matrix_has_the_closed_functional_denominator(self) -> None:
        matrix = self.runner.load_matrix(ROOT / "scripts/v1-functional-test-matrix.json")
        self.assertEqual(self.runner.REQUIRED_RESULT_IDS, tuple(route["id"] for route in matrix.routes))
        self.assertEqual(33, len(matrix.routes))
        self.assertEqual(12, sum(route["kind"] == "profile" for route in matrix.routes))
        self.assertEqual("aggregate", matrix.route("U01")["kind"])
        lifecycle = json.loads((ROOT / "distribution/config/release-lifecycle.json").read_text(encoding="utf-8"))
        covered = {capability for route in matrix.routes for capability in route.get("capability_ids", [])}
        self.assertEqual(set(lifecycle["required_capabilities"]), covered)

    def test_profile_scenario_and_all_selection_are_exact(self) -> None:
        matrix = self.runner.load_matrix(ROOT / "scripts/v1-functional-test-matrix.json")
        self.assertEqual(("P04",), self.runner.select_routes(matrix, profile="P04"))
        self.assertEqual(("I03",), self.runner.select_routes(matrix, scenario="I03"))
        self.assertEqual(self.runner.REQUIRED_RESULT_IDS, self.runner.select_routes(matrix, all_scenarios=True))
        with self.assertRaisesRegex(self.runner.AcceptanceError, "exactly one"):
            self.runner.select_routes(matrix, profile="P01", scenario="I01")
        with self.assertRaisesRegex(self.runner.AcceptanceError, "unknown profile"):
            self.runner.select_routes(matrix, profile="P99")

    def test_candidate_manifest_is_the_only_identity_and_root_selector(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            candidate_root = root / "candidate"
            candidate_root.mkdir()
            manifest = candidate_root / "CANDIDATE_RUN.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "candidate_id": "rc1-local-attempt1-ordinal1",
                        "candidate_kind": "rc",
                        "version": "1.0.0-rc.1",
                        "run_id": "local",
                        "attempt": 1,
                        "ordinal": 1,
                        "candidate_root": str(candidate_root),
                        "series_manifest": str(root / "RELEASE_SERIES.json"),
                        "series_id": "community-v1",
                        "series_generation": 1,
                        "parent_manifest": None,
                        "state": "staged-rc",
                        "sealed": False,
                        "outcome": None,
                        "known_issues": [],
                        "generation": 1,
                        "artifact_index": "ARTIFACT_INDEX.json",
                        "route_denominator": {
                            "schema_version": 1,
                            "audit_points": {"functional-full-matrix": ["P01"]},
                        },
                    }
                ),
                encoding="utf-8",
            )
            (candidate_root / "ARTIFACT_INDEX.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "candidate_id": "rc1-local-attempt1-ordinal1",
                        "version": "1.0.0-rc.1",
                        "run_id": "local",
                        "attempt": 1,
                        "remote_publication": "not-executed",
                        "artifacts": [],
                    }
                ),
                encoding="utf-8",
            )
            loaded = self.runner.load_candidate(manifest)
            self.assertEqual(candidate_root, loaded.root)
            forged = json.loads(manifest.read_text(encoding="utf-8"))
            forged["candidate_root"] = str(root / "other")
            manifest.write_text(json.dumps(forged), encoding="utf-8")
            with self.assertRaisesRegex(self.runner.AcceptanceError, "live at the candidate root"):
                self.runner.load_candidate(manifest)

    def test_archive_resolution_rejects_mixed_candidate_and_unregistered_paths(self) -> None:
        candidate = {
            "candidate_id": "candidate-a",
            "version": "1.0.0-rc.1",
            "run_id": "run-a",
            "attempt": 1,
        }
        index = {
            "candidate_id": "candidate-b",
            "version": "1.0.0-rc.1",
            "run_id": "run-a",
            "attempt": 1,
            "remote_publication": "not-executed",
            "artifacts": [],
        }
        with self.assertRaisesRegex(self.runner.AcceptanceError, "identity"):
            self.runner.archive_record(candidate, index, "x86_64-pc-windows-msvc")
        index["candidate_id"] = "candidate-a"
        with self.assertRaisesRegex(self.runner.AcceptanceError, "exactly one registered release archive"):
            self.runner.archive_record(candidate, index, "x86_64-pc-windows-msvc")

    def test_driver_command_can_only_reference_packaged_binaries(self) -> None:
        route = {
            "id": "P01",
            "command": ["{broker}", "--config", "{config_root}/broker.toml"],
        }
        package_root = Path("C:/candidate/archive")
        command = self.runner.expand_driver_command(
            route,
            package_root,
            {"broker": package_root / "bin/rocketmq-broker-rust.exe"},
            package_root / "work",
            package_root / "result.json",
            [12000],
        )
        self.assertEqual(package_root / "bin/rocketmq-broker-rust.exe", Path(command[0]))
        with self.assertRaisesRegex(self.runner.AcceptanceError, "source build tool"):
            self.runner.expand_driver_command(
                {"id": "P01", "command": ["cargo", "run"]},
                package_root,
                {},
                package_root / "work",
                package_root / "result.json",
                [12000],
            )

    def test_result_validation_rejects_zero_tests_ignored_and_mixed_identity(self) -> None:
        expected = self.runner.ResultIdentity("candidate-a", "1.0.0-rc.1", "run-a", 1, "P01")
        base = {
            "schema_version": 1,
            "candidate_id": "candidate-a",
            "version": "1.0.0-rc.1",
            "run_id": "run-a",
            "attempt": 1,
            "result_id": "P01",
            "status": "passed",
            "matched_test_count": 1,
            "executed_test_count": 1,
            "passed_test_count": 1,
            "failed_test_count": 0,
            "ignored_test_count": 0,
            "readiness_check_count": 1,
            "teardown_completed": True,
            "remote_publication": "not-executed",
        }
        self.runner.validate_driver_result(base, expected)
        for change, message in (
            ({"matched_test_count": 0, "executed_test_count": 0, "passed_test_count": 0}, "zero tests"),
            ({"ignored_test_count": 1, "matched_test_count": 2}, "ignored"),
            ({"candidate_id": "candidate-b"}, "identity"),
        ):
            with self.subTest(change=change), self.assertRaisesRegex(self.runner.AcceptanceError, message):
                self.runner.validate_driver_result({**base, **change}, expected)

    def test_timeout_terminates_the_process_group_and_records_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            marker = root / "child.txt"
            script = root / "hang.py"
            script.write_text(
                textwrap.dedent(
                    f"""
                    import subprocess, sys, time
                    subprocess.Popen([sys.executable, '-c', "import pathlib,time; time.sleep(2); pathlib.Path({str(marker)!r}).write_text('orphan')"])
                    time.sleep(30)
                    """
                ),
                encoding="utf-8",
            )
            with self.assertRaises(subprocess.TimeoutExpired):
                self.runner.run_bounded_process(
                    [sys.executable, str(script)], root, root / "stdout.log", root / "stderr.log", 1
                )
            import time

            time.sleep(2.2)
            self.assertFalse(marker.exists(), "timed-out descendants must not survive teardown")
            self.assertTrue((root / "stdout.log").is_file())
            self.assertTrue((root / "stderr.log").is_file())

    def test_shell_entrypoints_share_the_candidate_only_python_runner(self) -> None:
        powershell = (ROOT / "scripts/run-v1-functional-acceptance.ps1").read_text(encoding="utf-8")
        bash = (ROOT / "scripts/run-v1-functional-acceptance.sh").read_text(encoding="utf-8")
        for source in (powershell, bash):
            self.assertIn("v1_functional_acceptance.py", source)
            self.assertIn("candidate-manifest", source.lower())
            self.assertNotIn("ArtifactRoot", source)
            self.assertNotIn("cargo build", source)

    def test_selected_run_fails_fast_after_a_route_failure(self) -> None:
        matrix = self.runner.load_matrix(ROOT / "scripts/v1-functional-test-matrix.json")
        candidate = mock.Mock()
        candidate.root = Path("C:/candidate")
        selected = ("P01", "P02", "P03", "P04", "P05")
        calls: list[str] = []

        def wrapped(_candidate, _matrix, result_id, _target):
            calls.append(result_id)
            return 1 if result_id == "P01" else 0

        with mock.patch.object(self.runner, "_wrapped_one", side_effect=wrapped):
            result = self.runner.run_selected(
                candidate,
                matrix,
                ROOT / "scripts/v1-functional-test-matrix.json",
                selected,
                "x86_64-pc-windows-msvc",
            )
        self.assertEqual(1, result)
        self.assertNotIn("P05", calls, "dependent work must not start after the batch failed")

    def test_failure_result_is_durable_and_candidate_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "P01.json"
            identity = self.runner.ResultIdentity("candidate-a", "1.0.0-rc.1", "run-a", 1, "P01")
            self.runner._write_failure(
                output,
                identity,
                self.runner.AcceptanceError("driver failed"),
                ["driver", "--result", "P01"],
                ["F-01"],
            )
            result = json.loads(output.read_text(encoding="utf-8"))
            from scripts import release_evidence_guard

            self.assertEqual(release_evidence_guard.RESULT_FIELDS, set(result))
            self.assertEqual(("candidate-a", "P01", "failed", 1), (
                result["candidate_id"], result["result_id"], result["status"], result["failed_test_count"]
            ))
            self.assertEqual(["F-01"], result["capability_ids"])
            diagnostics = json.loads(output.with_suffix(".diagnostics.json").read_text(encoding="utf-8"))
            self.assertEqual("not-executed", diagnostics["remote_publication"])

    def test_normalized_success_is_accepted_by_the_release_evidence_guard(self) -> None:
        from scripts import release_evidence_guard

        identity = self.runner.ResultIdentity("candidate-a", "1.0.0-rc.1", "run-a", 1, "P01")
        result = self.runner._evidence_record(
            identity,
            status="passed",
            command=["driver", "--result", "P01"],
            exit_code=0,
            matched=2,
            executed=2,
            passed=2,
            failed=0,
            ignored=0,
            capability_ids=["F-01"],
            result_path="P01.json",
        )
        release_evidence_guard._validate_result(
            result,
            {"candidate_id": "candidate-a", "version": "1.0.0-rc.1", "run_id": "run-a", "attempt": 1},
        )


if __name__ == "__main__":
    unittest.main()

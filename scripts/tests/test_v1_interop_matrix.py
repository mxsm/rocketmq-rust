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
import os
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = ROOT / "scripts" / "interop" / "run_v1_interop.py"
MATRIX_PATH = ROOT / "scripts" / "interop" / "v1-interop-matrix.json"


def load_runner():
    spec = importlib.util.spec_from_file_location("run_v1_interop", RUNNER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load v1 interop runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class V1InteropMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(RUNNER_PATH.is_file(), "the v1 interop runner must be implemented")
        self.runner = load_runner()
        self.matrix = self.runner.load_matrix(MATRIX_PATH)

    def test_matrix_freezes_java_version_scope_and_five_result_ids(self) -> None:
        self.assertEqual("5.5.0", self.matrix["javaVersion"])
        self.assertEqual(["I01", "I02", "I03", "I04", "I05"], self.matrix["resultIds"])
        self.assertEqual({item["id"] for item in self.matrix["scenarios"]}, set(self.matrix["resultIds"]))
        self.assertEqual(
            {"Java Controller", "Java AutoSwitchHA", "DLedger CommitLog"},
            set(self.matrix["excludedModes"]),
        )

    def test_matrix_rejects_duplicate_ids_or_missing_negative_coverage(self) -> None:
        duplicate = json.loads(json.dumps(self.matrix))
        duplicate["scenarios"][1]["id"] = "I01"
        with self.assertRaisesRegex(ValueError, "unique"):
            self.runner.validate_matrix(duplicate)

        incomplete = json.loads(json.dumps(self.matrix))
        incomplete["scenarios"][0]["negativeCases"] = []
        with self.assertRaisesRegex(ValueError, "negativeCases"):
            self.runner.validate_matrix(incomplete)

        capability_drift = json.loads(json.dumps(self.matrix))
        capability_drift["scenarios"][0]["capabilities"].remove("send")
        with self.assertRaisesRegex(ValueError, "required capabilities"):
            self.runner.validate_matrix(capability_drift)

    def test_scenario_selector_never_turns_one_result_into_aggregate_success(self) -> None:
        selected = self.runner.select_scenarios(self.matrix, scenario_id="I03", run_all=False)
        self.assertEqual(["I03"], [item["id"] for item in selected])
        selected = self.runner.select_scenarios(self.matrix, scenario_id=None, run_all=True)
        self.assertEqual(self.matrix["resultIds"], [item["id"] for item in selected])
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.runner.select_scenarios(self.matrix, scenario_id=None, run_all=False)

    def test_single_scenario_executes_real_child_and_validates_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = self._write_candidate(root)
            driver = self._write_driver(root)
            output = root / "output"

            exit_code = self.runner.main(
                [
                    "--candidate-manifest",
                    str(candidate),
                    "--matrix",
                    str(MATRIX_PATH),
                    "--scenario",
                    "I01",
                    "--case-driver",
                    str(driver),
                    "--output",
                    str(output),
                    "--timeout-seconds",
                    "2",
                ]
            )

            self.assertEqual(0, exit_code)
            run = json.loads((output / "run.json").read_text(encoding="utf-8"))
            self.assertEqual("passed", run["status"])
            self.assertEqual(["I01"], run["scenarioIds"])
            self.assertTrue((output / "scenarios" / "I01" / "result.json").is_file())

    def test_missing_skipped_failed_and_timed_out_children_fail_closed(self) -> None:
        for mode, message in [
            ("missing", "missing result"),
            ("skipped", "status must be passed"),
            ("exit", "exited with 7"),
            ("timeout", "timed out"),
        ]:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                candidate = self._write_candidate(root)
                driver = self._write_driver(root)
                output = root / "output"
                with patch.dict(os.environ, {"FAKE_INTEROP_MODE": mode}):
                    exit_code = self.runner.main(
                        [
                            "--candidate-manifest",
                            str(candidate),
                            "--matrix",
                            str(MATRIX_PATH),
                            "--scenario",
                            "I02",
                            "--case-driver",
                            str(driver),
                            "--output",
                            str(output),
                            "--timeout-seconds",
                            "1",
                        ]
                    )
                self.assertEqual(1, exit_code)
                run = json.loads((output / "run.json").read_text(encoding="utf-8"))
                self.assertEqual("failed", run["status"])
                self.assertIn(message, run["error"])

    def test_all_requires_five_independent_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = self._write_candidate(root)
            driver = self._write_driver(root)
            output = root / "output"
            exit_code = self.runner.main(
                [
                    "--candidate-manifest",
                    str(candidate),
                    "--matrix",
                    str(MATRIX_PATH),
                    "--all",
                    "--case-driver",
                    str(driver),
                    "--output",
                    str(output),
                    "--timeout-seconds",
                    "2",
                    "--max-workers",
                    "2",
                ]
            )
            self.assertEqual(0, exit_code)
            run = json.loads((output / "run.json").read_text(encoding="utf-8"))
            self.assertEqual(self.matrix["resultIds"], run["scenarioIds"])
            self.assertEqual(5, len(run["results"]))

    @staticmethod
    def _write_candidate(root: Path) -> Path:
        candidate_root = root / "candidate"
        candidate_root.mkdir()
        manifest = candidate_root / "CANDIDATE_RUN.json"
        manifest.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "candidate_id": "1.0.0-rc.1-local-attempt1",
                    "version": "1.0.0-rc.1",
                    "run_id": "local",
                    "attempt": 1,
                    "candidate_root": str(candidate_root),
                }
            ),
            encoding="utf-8",
        )
        return manifest

    @staticmethod
    def _write_driver(root: Path) -> Path:
        driver = root / "fake_interop_driver.py"
        driver.write_text(
            textwrap.dedent(
                """
                import argparse
                import json
                import os
                import sys
                import time
                from pathlib import Path

                parser = argparse.ArgumentParser()
                parser.add_argument("--contract", type=Path, required=True)
                parser.add_argument("--candidate-manifest", type=Path, required=True)
                parser.add_argument("--result", type=Path, required=True)
                parser.add_argument("--work-dir", type=Path, required=True)
                args = parser.parse_args()
                mode = os.environ.get("FAKE_INTEROP_MODE", "passed")
                if mode == "timeout":
                    time.sleep(5)
                if mode == "exit":
                    raise SystemExit(7)
                if mode == "missing":
                    raise SystemExit(0)
                contract = json.loads(args.contract.read_text(encoding="utf-8"))
                candidate = json.loads(args.candidate_manifest.read_text(encoding="utf-8"))
                evidence = args.work_dir / "evidence"
                evidence.mkdir(parents=True, exist_ok=True)
                for name in contract["requiredEvidence"]:
                    (evidence / f"{name}.txt").write_text(name, encoding="utf-8")
                result = {
                    "schemaVersion": 1,
                    "resultId": contract["id"],
                    "candidateId": candidate["candidate_id"],
                    "version": candidate["version"],
                    "runId": candidate["run_id"],
                    "attempt": candidate["attempt"],
                    "javaVersion": "5.5.0",
                    "status": "skipped" if mode == "skipped" else "passed",
                    "assertions": {name: True for name in contract["requiredAssertions"]},
                    "evidence": {name: f"work/evidence/{name}.txt" for name in contract["requiredEvidence"]},
                    "remotePublication": "not-executed",
                }
                args.result.write_text(json.dumps(result), encoding="utf-8")
                """
            ),
            encoding="utf-8",
        )
        return driver


if __name__ == "__main__":
    unittest.main()

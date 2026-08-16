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
RUNNER_PATH = ROOT / "scripts" / "interop" / "run_v1_storage_fault.py"
MATRIX_PATH = ROOT / "scripts" / "interop" / "v1-storage-fault-matrix.json"
RESULT_IDS = [
    "U01-LF",
    "U01-MP",
    "U01-RDB",
    "U01-CMP",
    "U01-POP",
    "U01-TMR",
    "U01-TRD",
    "U01-CTL",
    "U01-UPG",
]


def load_runner():
    spec = importlib.util.spec_from_file_location("run_v1_storage_fault", RUNNER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load storage-fault runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class V1StorageFaultMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(RUNNER_PATH.is_file(), "the storage-fault runner must be implemented")
        self.runner = load_runner()
        self.matrix = self.runner.load_matrix(MATRIX_PATH)

    def test_matrix_freezes_nine_result_ids_scope_and_aggregate(self) -> None:
        self.assertEqual(RESULT_IDS, self.matrix["resultIds"])
        self.assertEqual("U01", self.matrix["aggregateId"])
        self.assertEqual(RESULT_IDS, [scenario["id"] for scenario in self.matrix["scenarios"]])
        self.assertEqual(
            {"Java Controller", "Java AutoSwitchHA", "DLedger CommitLog"},
            set(self.matrix["excludedModes"]),
        )
        self.assertEqual("not-executed", self.matrix["remotePublication"])

    def test_matrix_rejects_duplicates_missing_capabilities_and_invalid_timeout(self) -> None:
        duplicate = json.loads(json.dumps(self.matrix))
        duplicate["scenarios"][1]["id"] = "U01-LF"
        with self.assertRaisesRegex(ValueError, "unique"):
            self.runner.validate_matrix(duplicate)

        incomplete = json.loads(json.dumps(self.matrix))
        incomplete["scenarios"][7]["capabilities"].remove("lease-expiry-self-fence")
        with self.assertRaisesRegex(ValueError, "required capabilities"):
            self.runner.validate_matrix(incomplete)

        invalid_timeout = json.loads(json.dumps(self.matrix))
        invalid_timeout["scenarios"][0]["timeoutSeconds"] = 601
        with self.assertRaisesRegex(ValueError, "timeoutSeconds"):
            self.runner.validate_matrix(invalid_timeout)

    def test_selector_requires_exactly_one_scenario_or_all(self) -> None:
        selected = self.runner.select_scenarios(self.matrix, "U01-CMP", False)
        self.assertEqual(["U01-CMP"], [scenario["id"] for scenario in selected])
        selected = self.runner.select_scenarios(self.matrix, None, True)
        self.assertEqual(RESULT_IDS, [scenario["id"] for scenario in selected])
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.runner.select_scenarios(self.matrix, None, False)

    def test_single_scenario_runs_child_without_generating_aggregate(self) -> None:
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
                    "U01-MP",
                    "--case-driver",
                    str(driver),
                    "--output",
                    str(output),
                    "--timeout-seconds",
                    "2",
                ]
            )
            self.assertEqual(0, exit_code)
            self.assertFalse((output / "aggregate.json").exists())
            run = json.loads((output / "run.json").read_text(encoding="utf-8"))
            self.assertEqual("passed", run["status"])
            self.assertEqual(["U01-MP"], run["scenarioIds"])

    def test_missing_skipped_failed_timeout_and_escaping_evidence_fail_closed(self) -> None:
        for mode, message in [
            ("missing", "missing result"),
            ("skipped", "status must be passed"),
            ("exit", "exited with 7"),
            ("timeout", "timed out"),
            ("escape", "evidence escapes"),
        ]:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                candidate = self._write_candidate(root)
                driver = self._write_driver(root)
                output = root / "output"
                with patch.dict(os.environ, {"FAKE_STORAGE_MODE": mode}):
                    exit_code = self.runner.main(
                        [
                            "--candidate-manifest",
                            str(candidate),
                            "--matrix",
                            str(MATRIX_PATH),
                            "--scenario",
                            "U01-UPG",
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

    def test_all_generates_u01_only_from_nine_independent_results(self) -> None:
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
                    "3",
                ]
            )
            self.assertEqual(0, exit_code)
            aggregate = json.loads((output / "aggregate.json").read_text(encoding="utf-8"))
            self.assertEqual("U01", aggregate["resultId"])
            self.assertEqual("passed", aggregate["status"])
            self.assertEqual(RESULT_IDS, aggregate["sourceResultIds"])
            self.assertEqual("not-executed", aggregate["remotePublication"])

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
        driver = root / "fake_storage_driver.py"
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
                mode = os.environ.get("FAKE_STORAGE_MODE", "passed")
                if mode == "timeout":
                    time.sleep(5)
                if mode == "exit":
                    raise SystemExit(7)
                if mode == "missing":
                    raise SystemExit(0)
                contract = json.loads(args.contract.read_text(encoding="utf-8"))
                candidate = json.loads(args.candidate_manifest.read_text(encoding="utf-8"))
                evidence_root = args.work_dir / "evidence"
                evidence_root.mkdir(parents=True, exist_ok=True)
                for name in contract["requiredEvidence"]:
                    (evidence_root / f"{name}.txt").write_text(name, encoding="utf-8")
                evidence = {
                    name: f"work/evidence/{name}.txt" for name in contract["requiredEvidence"]
                }
                if mode == "escape":
                    outside = args.work_dir.parent.parent / "outside.txt"
                    outside.write_text("escape", encoding="utf-8")
                    evidence[contract["requiredEvidence"][0]] = "../../outside.txt"
                result = {
                    "schemaVersion": 1,
                    "resultId": contract["id"],
                    "candidateId": candidate["candidate_id"],
                    "version": candidate["version"],
                    "runId": candidate["run_id"],
                    "attempt": candidate["attempt"],
                    "status": "skipped" if mode == "skipped" else "passed",
                    "assertions": {name: True for name in contract["requiredAssertions"]},
                    "evidence": evidence,
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

# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.tests.release_archive_test_support import create_candidate
from scripts.tests.release_test_support import load_module, read_json, write_json


class ReleaseEvidenceGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.guard = load_module("release_evidence_guard", "scripts/release_evidence_guard.py")

    def _result(self, candidate: Path, root: Path, result_id: str, *, status: str = "passed") -> None:
        value = read_json(candidate)
        write_json(
            root / f"{result_id}.json",
            {
                "schema_version": 1,
                "candidate_id": value["candidate_id"],
                "version": value["version"],
                "run_id": value["run_id"],
                "attempt": value["attempt"],
                "phase": 5,
                "gate_stage": "release-preparation",
                "result_id": result_id,
                "result_kind": "test",
                "status": status,
                "command": ["python", "fixture.py"],
                "exit_code": 0 if status == "passed" else 1,
                "matched_test_count": 1,
                "executed_test_count": 1,
                "passed_test_count": 1 if status == "passed" else 0,
                "failed_test_count": 0 if status == "passed" else 1,
                "ignored_test_count": 0,
                "capability_ids": [],
                "result_path": f"results/{result_id}.json",
            },
        )

    def test_closed_denominator_produces_semantic_readiness_record(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            results = root / "results"
            self._result(candidate, results, "R01")
            self._result(candidate, results, "R02")
            output = root / "EVIDENCE_INDEX.json"

            self.guard.build_evidence(
                candidate,
                results,
                phase=5,
                gate_stage="release-preparation",
                required_result_ids=["R01", "R02"],
                output=output,
            )

            value = read_json(output)
            self.assertTrue(value["all_required_passed"])
            self.assertEqual(value["release_result_ids"], {"R01": "passed", "R02": "passed"})
            self.assertNotIn("sha256", str(value).lower())

    def test_missing_duplicate_unknown_and_zero_executed_results_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            results = root / "results"
            self._result(candidate, results, "R01")
            with self.assertRaises(self.guard.EvidenceError):
                self.guard.build_evidence(
                    candidate,
                    results,
                    phase=5,
                    gate_stage="release-preparation",
                    required_result_ids=["R01", "R02"],
                    output=root / "missing.json",
                )

            self._result(candidate, results, "R02")
            duplicate = read_json(results / "R02.json")
            write_json(results / "duplicate.json", duplicate)
            with self.assertRaises(self.guard.EvidenceError):
                self.guard.build_evidence(
                    candidate,
                    results,
                    phase=5,
                    gate_stage="release-preparation",
                    required_result_ids=["R01", "R02"],
                    output=root / "duplicate-output.json",
                )

            (results / "duplicate.json").unlink()
            value = read_json(results / "R02.json")
            value["executed_test_count"] = 0
            value["passed_test_count"] = 0
            write_json(results / "R02.json", value)
            with self.assertRaises(self.guard.EvidenceError):
                self.guard.build_evidence(
                    candidate,
                    results,
                    phase=5,
                    gate_stage="release-preparation",
                    required_result_ids=["R01", "R02"],
                    output=root / "zero.json",
                )


if __name__ == "__main__":
    unittest.main()

# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.tests.release_archive_test_support import create_candidate
from scripts.tests.release_test_support import load_module, read_json, write_json

FINAL_HANDOFF_RESULTS = (
    "H01-LINUX",
    "H01-WINDOWS",
    "H01-MACOS",
    "H02-DRAFT-SEMANTIC",
    "H03-DRAFT-NO-REMOTE",
    "H04-FINAL-SEMANTIC",
    "H05-FINAL-NO-REMOTE",
)


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

    def test_final_handoff_native_results_are_normalized(self) -> None:
        """Handoff-native result shapes must enter the existing semantic evidence index."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            results = root / "handoff-results"
            required = list(FINAL_HANDOFF_RESULTS)
            for result_id in required:
                self._handoff_result(candidate, results, result_id)
            events = root / "events"
            contexts = root / "contexts"
            for result_id in required:
                self._handoff_event(candidate, events, contexts, result_id)

            try:
                evidence = self.guard.build_evidence(
                    candidate,
                    results,
                    phase=6,
                    gate_stage="final-handoff",
                    required_result_ids=required,
                    output=root / "FINAL_HANDOFF_EVIDENCE.json",
                    event_root=events,
                    context_root=contexts,
                )
            except Exception as error:  # noqa: BLE001 - failure is rendered as an assertion.
                self.fail(f"final handoff evidence was rejected: {error}")

            self.assertEqual(required, evidence["required_result_ids"])
            self.assertEqual({result_id: "passed" for result_id in required}, evidence["release_result_ids"])
            self.assertEqual("not-executed", evidence["remote_publication"]["status"])
            self.assertTrue(all(result["result_kind"] in {"smoke", "check"} for result in evidence["results"]))

    def test_final_handoff_results_without_event_context_closure_are_rejected(self) -> None:
        """Passed JSON alone must not substitute for attributable command execution."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            results = root / "handoff-results"
            required = list(FINAL_HANDOFF_RESULTS)
            for result_id in required:
                self._handoff_result(candidate, results, result_id)

            with self.assertRaisesRegex(self.guard.EvidenceError, "event/context roots"):
                self.guard.build_evidence(
                    candidate,
                    results,
                    phase=6,
                    gate_stage="final-handoff",
                    required_result_ids=required,
                    output=root / "FINAL_HANDOFF_EVIDENCE.json",
                )

    def test_handoff_result_worker_must_match_its_event_context(self) -> None:
        """A platform result cannot borrow another worker's successful command event."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            results = root / "handoff-results"
            required = list(FINAL_HANDOFF_RESULTS)
            for result_id in required:
                self._handoff_result(candidate, results, result_id)
            value = read_json(results / "H01-LINUX.json")
            value["worker_id"] = "forged-worker"
            write_json(results / "H01-LINUX.json", value)
            events = root / "events"
            contexts = root / "contexts"
            for result_id in required:
                self._handoff_event(candidate, events, contexts, result_id)

            with self.assertRaisesRegex(self.guard.EvidenceError, "result worker"):
                self.guard.build_evidence(
                    candidate,
                    results,
                    phase=6,
                    gate_stage="final-handoff",
                    required_result_ids=required,
                    output=root / "FINAL_HANDOFF_EVIDENCE.json",
                    event_root=events,
                    context_root=contexts,
                )

    def test_final_handoff_denominator_cannot_be_narrowed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            results = root / "handoff-results"
            result_id = "H01-LINUX"
            self._handoff_result(candidate, results, result_id)
            events = root / "events"
            contexts = root / "contexts"
            self._handoff_event(candidate, events, contexts, result_id)

            with self.assertRaisesRegex(self.guard.EvidenceError, "final-handoff result denominator"):
                self.guard.build_evidence(
                    candidate,
                    results,
                    phase=6,
                    gate_stage="final-handoff",
                    required_result_ids=[result_id],
                    output=root / "FINAL_HANDOFF_EVIDENCE.json",
                    event_root=events,
                    context_root=contexts,
                )

    def test_failed_handoff_command_event_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            results = root / "handoff-results"
            required = list(FINAL_HANDOFF_RESULTS)
            events = root / "events"
            contexts = root / "contexts"
            for result_id in required:
                self._handoff_result(candidate, results, result_id)
                self._handoff_event(candidate, events, contexts, result_id)
            completed_path = events / "H04-FINAL-SEMANTIC.completed.json"
            completed = read_json(completed_path)
            completed.update({"status": "failed", "exit_code": 17})
            write_json(completed_path, completed)

            with self.assertRaisesRegex(self.guard.EvidenceError, "command event is incomplete"):
                self.guard.build_evidence(
                    candidate,
                    results,
                    phase=6,
                    gate_stage="final-handoff",
                    required_result_ids=required,
                    output=root / "FINAL_HANDOFF_EVIDENCE.json",
                    event_root=events,
                    context_root=contexts,
                )

    def test_mixed_candidate_handoff_event_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            results = root / "handoff-results"
            required = list(FINAL_HANDOFF_RESULTS)
            events = root / "events"
            contexts = root / "contexts"
            for result_id in required:
                self._handoff_result(candidate, results, result_id)
                self._handoff_event(candidate, events, contexts, result_id)
            completed_path = events / "H02-DRAFT-SEMANTIC.completed.json"
            completed = read_json(completed_path)
            completed["candidate_id"] = "another-candidate"
            write_json(completed_path, completed)

            with self.assertRaisesRegex(self.guard.EvidenceError, "command event is incomplete"):
                self.guard.build_evidence(
                    candidate,
                    results,
                    phase=6,
                    gate_stage="final-handoff",
                    required_result_ids=required,
                    output=root / "FINAL_HANDOFF_EVIDENCE.json",
                    event_root=events,
                    context_root=contexts,
                )

    def test_missing_handoff_execution_context_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            results = root / "handoff-results"
            required = list(FINAL_HANDOFF_RESULTS)
            events = root / "events"
            contexts = root / "contexts"
            for result_id in required:
                self._handoff_result(candidate, results, result_id)
                self._handoff_event(candidate, events, contexts, result_id)
            (contexts / "worker-H05-FINAL-NO-REMOTE.json").unlink()

            with self.assertRaisesRegex(self.guard.ReleaseStateError, "execution context"):
                self.guard.build_evidence(
                    candidate,
                    results,
                    phase=6,
                    gate_stage="final-handoff",
                    required_result_ids=required,
                    output=root / "FINAL_HANDOFF_EVIDENCE.json",
                    event_root=events,
                    context_root=contexts,
                )

    @staticmethod
    def _handoff_result(candidate: Path, root: Path, result_id: str) -> None:
        identity = read_json(candidate)
        value = {
            "schema_version": 1,
            "candidate_id": identity["candidate_id"],
            "version": identity["version"],
            "run_id": identity["run_id"],
            "attempt": identity["attempt"],
            "phase": 6,
            "gate_stage": "final-handoff",
            "result_id": result_id,
        }
        if result_id in {"H03-DRAFT-NO-REMOTE", "H05-FINAL-NO-REMOTE"}:
            value.update(
                {
                    "remote_publication": {"status": "not-executed"},
                    "remote_publication_workflow_dispatches": [],
                    "publishing_credentials_provided": False,
                    "publishing_credential_names": [],
                    "violations": [],
                    "indeterminate_reasons": [],
                }
            )
        else:
            value.update(
                {
                    "mode": (
                        "draft-pre-ready"
                        if result_id.startswith(("H01", "H02"))
                        else "final-pre-ready"
                    ),
                    "status": "passed",
                    "skipped": False,
                    "remote_publication": {"status": "not-executed"},
                    "secret_scan": {"status": "passed", "findings": []},
                }
            )
            if result_id.startswith("H01"):
                value.update(
                    {
                        "worker_id": f"worker-{result_id}",
                        "archive_id": f"archive-{result_id}",
                        "archive_smoke_results": [
                            {"component": f"component-{index}", "exit_code": 0, "stdout": "ok"}
                            for index in range(6)
                        ],
                    }
                )
            elif result_id == "H04-FINAL-SEMANTIC":
                value["read_only_verified"] = True
        write_json(root / f"{result_id}.json", value)

    @staticmethod
    def _handoff_event(candidate: Path, event_root: Path, context_root: Path, result_id: str) -> None:
        identity = read_json(candidate)
        worker_id = f"worker-{result_id}"
        context_path = context_root / f"{worker_id}.json"
        common = {
            "schema_version": 1,
            "candidate_id": identity["candidate_id"],
            "version": identity["version"],
            "run_id": identity["run_id"],
            "attempt": identity["attempt"],
            "route_id": result_id,
            "worker_id": worker_id,
            "context_path": str(context_path.resolve()),
        }
        write_json(
            event_root / f"{result_id}.started.json",
            {**common, "status": "started", "command": ["python", "verify.py", result_id]},
        )
        write_json(
            event_root / f"{result_id}.completed.json",
            {**common, "status": "passed", "exit_code": 0},
        )
        write_json(
            context_path,
            {
                "schema_version": 1,
                "candidate_id": identity["candidate_id"],
                "version": identity["version"],
                "run_id": identity["run_id"],
                "attempt": identity["attempt"],
                "worker_id": worker_id,
                "publish_input": False,
                "publishing_credentials_provided": False,
            },
        )


if __name__ == "__main__":
    unittest.main()

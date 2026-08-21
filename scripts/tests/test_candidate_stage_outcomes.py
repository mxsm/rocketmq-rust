# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from scripts.tests.release_archive_test_support import create_candidate
from scripts.tests.release_test_support import read_json, write_json


ROOT = Path(__file__).resolve().parents[2]
COLLECTOR = ROOT / "scripts" / "collect_candidate_stage_outcomes.py"


class CandidateStageOutcomeTests(unittest.TestCase):
    def test_successful_worker_bundles_are_closed_and_atomically_merged(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            policy = self._policy(root)
            workflow = self._workflow_results(root, candidate, {"build-linux": "success", "aggregate": "success"})
            bundles = root / "bundles"
            self._bundle(bundles / "build-linux", candidate, "build-linux", "linux-worker", "x86_64-unknown-linux-gnu")
            self._bundle(bundles / "aggregate", candidate, "aggregate", "aggregate-worker", None)
            output = root / "canonical"

            completed = subprocess.run(
                [
                    sys.executable,
                    str(COLLECTOR),
                    "--candidate-manifest",
                    str(candidate),
                    "--bundles-root",
                    str(bundles),
                    "--workflow-results",
                    str(workflow),
                    "--policy",
                    str(policy),
                    "--profile",
                    "fixture",
                    "--output-root",
                    str(output),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )

            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            index = read_json(output / "CANDIDATE_STAGE_OUTCOMES.json")
            self.assertTrue(index["all_required_passed"])
            self.assertEqual([], index["failed_job_ids"])
            self.assertEqual(["build-linux", "aggregate"], [item["job_id"] for item in index["jobs"]])
            self.assertEqual(["build-linux"], index["jobs"][0]["expected_route_ids"])
            self.assertEqual(["build-linux"], index["jobs"][0]["route_ids"])
            self.assertEqual([], index["jobs"][0]["missing_route_ids"])
            self.assertEqual(2, len(list((output / "results").rglob("*.json"))))
            self.assertEqual(4, len(list((output / "events").rglob("*.json"))))
            self.assertEqual(2, len(list((output / "contexts").rglob("*.json"))))

    def test_missing_bundle_is_synthesized_as_a_rejection_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            policy = self._policy(root)
            workflow = self._workflow_results(
                root,
                candidate,
                {"build-linux": "success", "aggregate": "failure"},
            )
            bundles = root / "bundles"
            self._bundle(
                bundles / "build-linux",
                candidate,
                "build-linux",
                "linux-worker",
                "x86_64-unknown-linux-gnu",
            )
            output = root / "canonical"

            completed = subprocess.run(
                [
                    sys.executable,
                    str(COLLECTOR),
                    "--candidate-manifest",
                    str(candidate),
                    "--bundles-root",
                    str(bundles),
                    "--workflow-results",
                    str(workflow),
                    "--policy",
                    str(policy),
                    "--profile",
                    "fixture",
                    "--output-root",
                    str(output),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )

            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            index = read_json(output / "CANDIDATE_STAGE_OUTCOMES.json")
            self.assertFalse(index["all_required_passed"])
            self.assertEqual(["aggregate"], index["failed_job_ids"])
            aggregate = next(item for item in index["jobs"] if item["job_id"] == "aggregate")
            self.assertEqual("missing-worker", aggregate["status"])
            self.assertEqual("failure", aggregate["workflow_result"])

    def test_mixed_candidate_payload_fails_without_publishing_partial_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            policy = self._policy(root)
            workflow = self._workflow_results(
                root,
                candidate,
                {"build-linux": "success", "aggregate": "success"},
            )
            bundles = root / "bundles"
            self._bundle(
                bundles / "build-linux",
                candidate,
                "build-linux",
                "linux-worker",
                "x86_64-unknown-linux-gnu",
            )
            self._bundle(bundles / "aggregate", candidate, "aggregate", "aggregate-worker", None)
            result_path = bundles / "build-linux/results/build-linux.json"
            result = read_json(result_path)
            result["candidate_id"] = "another-candidate"
            write_json(result_path, result)
            output = root / "canonical"

            completed = subprocess.run(
                [
                    sys.executable,
                    str(COLLECTOR),
                    "--candidate-manifest",
                    str(candidate),
                    "--bundles-root",
                    str(bundles),
                    "--workflow-results",
                    str(workflow),
                    "--policy",
                    str(policy),
                    "--profile",
                    "fixture",
                    "--output-root",
                    str(output),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )

            self.assertEqual(1, completed.returncode)
            self.assertIn("belongs to another candidate", completed.stderr)
            self.assertFalse(output.exists())
            self.assertFalse(output.with_name(f".{output.name}.staging").exists())

    def test_undeclared_bundle_file_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            policy = self._policy(root)
            workflow = self._workflow_results(
                root,
                candidate,
                {"build-linux": "success", "aggregate": "success"},
            )
            bundles = root / "bundles"
            self._bundle(
                bundles / "build-linux",
                candidate,
                "build-linux",
                "linux-worker",
                "x86_64-unknown-linux-gnu",
            )
            self._bundle(bundles / "aggregate", candidate, "aggregate", "aggregate-worker", None)
            (bundles / "aggregate/undeclared.txt").write_text("not indexed\n", encoding="utf-8")

            completed = subprocess.run(
                [
                    sys.executable,
                    str(COLLECTOR),
                    "--candidate-manifest",
                    str(candidate),
                    "--bundles-root",
                    str(bundles),
                    "--workflow-results",
                    str(workflow),
                    "--policy",
                    str(policy),
                    "--profile",
                    "fixture",
                    "--output-root",
                    str(root / "canonical"),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )

            self.assertEqual(1, completed.returncode)
            self.assertIn("undeclared or missing files", completed.stderr)

    def test_failed_worker_bundle_is_preserved_as_rejection_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            policy = self._policy(root)
            workflow = self._workflow_results(
                root,
                candidate,
                {"build-linux": "failure", "aggregate": "success"},
            )
            bundles = root / "bundles"
            self._bundle(
                bundles / "build-linux",
                candidate,
                "build-linux",
                "linux-worker",
                "x86_64-unknown-linux-gnu",
                status="failed",
                workflow_result="failure",
            )
            self._bundle(bundles / "aggregate", candidate, "aggregate", "aggregate-worker", None)
            output = root / "canonical"

            completed = subprocess.run(
                [
                    sys.executable,
                    str(COLLECTOR),
                    "--candidate-manifest",
                    str(candidate),
                    "--bundles-root",
                    str(bundles),
                    "--workflow-results",
                    str(workflow),
                    "--policy",
                    str(policy),
                    "--profile",
                    "fixture",
                    "--output-root",
                    str(output),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )

            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            index = read_json(output / "CANDIDATE_STAGE_OUTCOMES.json")
            self.assertEqual(["build-linux"], index["failed_job_ids"])
            failed = next(item for item in index["jobs"] if item["job_id"] == "build-linux")
            self.assertEqual("failed", failed["status"])
            self.assertTrue(failed["result_files"])
            self.assertTrue(failed["event_files"])
            self.assertTrue(failed["context_files"])

    def test_unknown_job_bundle_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            policy = self._policy(root)
            workflow = self._workflow_results(
                root,
                candidate,
                {"build-linux": "success", "aggregate": "success"},
            )
            bundles = root / "bundles"
            self._bundle(
                bundles / "build-linux",
                candidate,
                "build-linux",
                "linux-worker",
                "x86_64-unknown-linux-gnu",
            )
            self._bundle(bundles / "aggregate", candidate, "aggregate", "aggregate-worker", None)
            self._bundle(bundles / "unknown", candidate, "unknown", "unknown-worker", None)

            completed = subprocess.run(
                [
                    sys.executable,
                    str(COLLECTOR),
                    "--candidate-manifest",
                    str(candidate),
                    "--bundles-root",
                    str(bundles),
                    "--workflow-results",
                    str(workflow),
                    "--policy",
                    str(policy),
                    "--profile",
                    "fixture",
                    "--output-root",
                    str(root / "canonical"),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )

            self.assertEqual(1, completed.returncode)
            self.assertIn("unknown job bundle", completed.stderr)

    def test_workflow_result_must_be_a_known_string(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            policy = self._policy(root)
            workflow = self._workflow_results(
                root,
                candidate,
                {"build-linux": {"result": "success"}, "aggregate": "success"},
            )
            bundles = root / "bundles"
            bundles.mkdir()

            completed = self._collect(root, candidate, policy, workflow, bundles)

            self.assertEqual(1, completed.returncode)
            self.assertIn("invalid status", completed.stderr)
            self.assertNotIn("Traceback", completed.stderr)

    def test_worker_status_must_exactly_match_the_workflow_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            policy = self._policy(root)
            workflow = self._workflow_results(
                root,
                candidate,
                {"build-linux": "cancelled", "aggregate": "success"},
            )
            bundles = root / "bundles"
            self._bundle(
                bundles / "build-linux",
                candidate,
                "build-linux",
                "linux-worker",
                "x86_64-unknown-linux-gnu",
                status="failed",
                workflow_result="cancelled",
            )
            self._bundle(bundles / "aggregate", candidate, "aggregate", "aggregate-worker", None)

            completed = self._collect(root, candidate, policy, workflow, bundles)

            self.assertEqual(1, completed.returncode)
            self.assertIn("disagrees with workflow result", completed.stderr)

    def test_stale_staging_directory_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            policy = self._policy(root)
            workflow = self._workflow_results(
                root,
                candidate,
                {"build-linux": "success", "aggregate": "success"},
            )
            bundles = root / "bundles"
            self._bundle(
                bundles / "build-linux",
                candidate,
                "build-linux",
                "linux-worker",
                "x86_64-unknown-linux-gnu",
            )
            self._bundle(bundles / "aggregate", candidate, "aggregate", "aggregate-worker", None)
            stale = root / ".canonical.staging"
            stale.mkdir()
            marker = stale / "interrupted.txt"
            marker.write_text("preserve for audit\n", encoding="utf-8")

            completed = self._collect(root, candidate, policy, workflow, bundles)

            self.assertEqual(1, completed.returncode)
            self.assertIn("stale candidate outcome staging", completed.stderr)
            self.assertTrue(marker.is_file())
            self.assertFalse((root / "canonical").exists())

    def test_payload_path_cannot_escape_the_worker_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            policy = self._policy(root)
            workflow = self._workflow_results(
                root,
                candidate,
                {"build-linux": "success", "aggregate": "success"},
            )
            bundles = root / "bundles"
            self._bundle(
                bundles / "build-linux",
                candidate,
                "build-linux",
                "linux-worker",
                "x86_64-unknown-linux-gnu",
            )
            self._bundle(bundles / "aggregate", candidate, "aggregate", "aggregate-worker", None)
            outcome_path = bundles / "build-linux/CANDIDATE_STAGE_OUTCOME.json"
            outcome = read_json(outcome_path)
            outcome["result_files"] = ["../outside.json"]
            write_json(outcome_path, outcome)

            completed = self._collect(root, candidate, policy, workflow, bundles)

            self.assertEqual(1, completed.returncode)
            self.assertIn("safe POSIX relative path", completed.stderr)

    def test_successful_job_must_cover_its_result_denominator(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            policy = self._policy(root)
            policy_value = read_json(policy)
            policy_value["profiles"]["fixture"][1]["result_ids"].append("another-result")
            write_json(policy, policy_value)
            workflow = self._workflow_results(
                root,
                candidate,
                {"build-linux": "success", "aggregate": "success"},
            )
            bundles = root / "bundles"
            self._bundle(
                bundles / "build-linux",
                candidate,
                "build-linux",
                "linux-worker",
                "x86_64-unknown-linux-gnu",
            )
            self._bundle(bundles / "aggregate", candidate, "aggregate", "aggregate-worker", None)

            completed = self._collect(root, candidate, policy, workflow, bundles)

            self.assertEqual(1, completed.returncode)
            self.assertIn("missing required results", completed.stderr)

    @staticmethod
    def _policy(root: Path) -> Path:
        path = root / "policy.json"
        write_json(
            path,
            {
                "schema_version": 1,
                "profiles": {
                    "fixture": [
                        {
                            "job_id": "build-linux",
                            "target": "x86_64-unknown-linux-gnu",
                            "result_ids": ["build-linux"],
                            "route_ids": ["build-linux"],
                        },
                        {
                            "job_id": "aggregate",
                            "target": None,
                            "result_ids": ["aggregate"],
                            "route_ids": ["aggregate"],
                        },
                    ]
                },
            },
        )
        return path

    @staticmethod
    def _workflow_results(root: Path, candidate: Path, jobs: dict[str, object]) -> Path:
        identity = read_json(candidate)
        path = root / "workflow-results.json"
        write_json(
            path,
            {
                "schema_version": 1,
                "candidate_id": identity["candidate_id"],
                "version": identity["version"],
                "run_id": identity["run_id"],
                "attempt": identity["attempt"],
                "jobs": jobs,
            },
        )
        return path

    @staticmethod
    def _collect(
        root: Path,
        candidate: Path,
        policy: Path,
        workflow: Path,
        bundles: Path,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(COLLECTOR),
                "--candidate-manifest",
                str(candidate),
                "--bundles-root",
                str(bundles),
                "--workflow-results",
                str(workflow),
                "--policy",
                str(policy),
                "--profile",
                "fixture",
                "--output-root",
                str(root / "canonical"),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )

    @staticmethod
    def _bundle(
        root: Path,
        candidate: Path,
        job_id: str,
        worker_id: str,
        target: str | None,
        *,
        status: str = "success",
        workflow_result: str = "success",
    ) -> None:
        identity = read_json(candidate)
        common = {
            "candidate_id": identity["candidate_id"],
            "version": identity["version"],
            "run_id": identity["run_id"],
            "attempt": identity["attempt"],
        }
        result_path = f"results/{job_id}.json"
        started_path = f"events/{job_id}.started.json"
        completed_path = f"events/{job_id}.completed.json"
        context_path = f"contexts/{worker_id}.json"
        write_json(
            root / result_path,
            {
                "schema_version": 1,
                **common,
                "result_id": job_id,
                "status": "passed" if status == "success" else "failed",
            },
        )
        event = {
            "schema_version": 1,
            **common,
            "route_id": job_id,
            "worker_id": worker_id,
            "context_path": context_path,
        }
        write_json(root / started_path, {**event, "status": "started", "command": ["python", "worker.py"]})
        write_json(
            root / completed_path,
            {
                **event,
                "status": "passed" if status == "success" else "failed",
                "exit_code": 0 if status == "success" else 1,
            },
        )
        write_json(
            root / context_path,
            {
                "schema_version": 1,
                **common,
                "worker_id": worker_id,
                "publish_input": False,
                "publishing_credentials_provided": False,
            },
        )
        write_json(
            root / "CANDIDATE_STAGE_OUTCOME.json",
            {
                "schema_version": 1,
                **common,
                "job_id": job_id,
                "worker_id": worker_id,
                "target": target,
                "status": status,
                "workflow_result": workflow_result,
                "sealed": True,
                "result_files": [result_path],
                "event_pairs": [
                    {"route_id": job_id, "started": started_path, "completed": completed_path}
                ],
                "context_files": [context_path],
            },
        )


if __name__ == "__main__":
    unittest.main()

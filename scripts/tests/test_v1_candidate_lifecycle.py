# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.tests.release_test_support import (
    ROOT,
    create_source_bundle,
    load_module,
    read_json,
    write_json,
)


RUNNER = ROOT / "scripts" / "v1_candidate_lifecycle.py"


class V1CandidateLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.series = load_module("release_series_for_v1_runner", "distribution/release_series.py")
        cls.candidate = load_module("candidate_run_for_v1_runner", "distribution/candidate_run.py")
        cls.collector = load_module(
            "candidate_outcome_collector_for_v1_runner",
            "scripts/collect_candidate_stage_outcomes.py",
        )

    def test_stage_rc_uses_the_atomic_lifecycle_route(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            series = self.series.create_series(root / "series", "1.0", "candidate-runner")
            candidate = self.candidate.create_candidate(
                root / "candidates", "1.0.0-rc.1", "rc1", 1, series
            )

            completed = self._run("StageRc", candidate)

            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            value = read_json(candidate)
            self.assertEqual("staged-rc", value["state"])
            self.assertFalse(value["sealed"])
            event = read_json(candidate.parent / "lifecycle/events/candidate-stage-rc.completed.json")
            self.assertTrue(event["lifecycle_atomic_completion"])
            self.assertTrue(
                (candidate.parent / f"transfer/CANDIDATE_CONTROL_BUNDLE.g{value['generation']}.tar").is_file()
            )
            series_value = read_json(series)
            self.assertTrue(
                (
                    series.parent
                    / f"RELEASE_SERIES_CONTROL_BUNDLE.g{series_value['generation']}.tar"
                ).is_file()
            )

    def test_cross_platform_entrypoints_are_fail_fast_thin_wrappers(self) -> None:
        powershell = (ROOT / "scripts/run-v1-candidate-lifecycle.ps1").read_text(encoding="utf-8")
        shell = (ROOT / "scripts/run-v1-candidate-lifecycle.sh").read_text(encoding="utf-8")

        self.assertIn('$PSNativeCommandUseErrorActionPreference = $true', powershell)
        self.assertIn('"scripts/v1_candidate_lifecycle.py"', powershell)
        self.assertIn("set -euo pipefail", shell)
        self.assertIn("scripts/v1_candidate_lifecycle.py", shell)
        for mode in ("StageRc", "FinalizeRc", "FinalizeFinalFunctional", "RejectFinalHandoff"):
            self.assertIn(mode, powershell)
            self.assertIn(mode, shell)

    def test_finalize_rc_closes_all_gates_and_seals_the_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            series = self.series.create_series(root / "series", "1.0", "candidate-runner")
            candidate = self.candidate.create_candidate(
                root / "candidates", "1.0.0-rc.1", "rc1", 1, series
            )
            source = create_source_bundle(
                candidate.parent / "CORE_SOURCE_TRANSFER.tar",
                version="1.0.0-rc.1",
                run_id="rc1",
                attempt=1,
            )
            self.candidate.record_build_source_bundle(candidate, source)
            self.assertEqual(0, self._run("StageRc", candidate).returncode)
            outcome_index = self._successful_outcomes(root, candidate)

            completed = self._run(
                "FinalizeRc",
                candidate,
                "--stage-outcomes-index",
                str(outcome_index),
            )

            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            value = read_json(candidate)
            self.assertEqual("rc-candidate-ready", value["state"])
            self.assertTrue(value["sealed"])
            self.assertIsInstance(value["source_snapshot"], str)
            self.assertTrue(Path(value["source_snapshot"]).is_file())
            self.assertTrue((candidate.parent / "evidence/FULL_MATRIX_EVIDENCE.json").is_file())
            self.assertTrue((candidate.parent / "evidence/NO_REMOTE_PUBLICATION.json").is_file())
            event = read_json(
                outcome_index.parent / "events/candidate-finalize-ready.completed.json"
            )
            self.assertTrue(event["lifecycle_atomic_completion"])
            self.assertTrue(
                (candidate.parent / f"transfer/CANDIDATE_CONTROL_BUNDLE.g{value['generation']}.tar").is_file()
            )

    def test_missing_worker_is_sealed_as_a_rejected_rc_before_failure_returns(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            series = self.series.create_series(root / "series", "1.0", "candidate-runner")
            candidate = self.candidate.create_candidate(
                root / "candidates", "1.0.0-rc.1", "rc1", 1, series
            )
            self.assertEqual(0, self._run("StageRc", candidate).returncode)
            outcome_index = self._successful_outcomes(root, candidate, missing_job="full-matrix")

            completed = self._run(
                "FinalizeRc",
                candidate,
                "--stage-outcomes-index",
                str(outcome_index),
            )

            self.assertEqual(1, completed.returncode)
            value = read_json(candidate)
            self.assertEqual("rejected", value["state"])
            self.assertTrue(value["sealed"])
            self.assertEqual("FinalizeRc gate failure", value["rejection_reason"])
            event = read_json(
                candidate.parent / "lifecycle/events/candidate-finalize-reject.completed.json"
            )
            self.assertTrue(event["lifecycle_atomic_completion"])
            self.assertTrue(
                (candidate.parent / f"transfer/CANDIDATE_CONTROL_BUNDLE.g{value['generation']}.tar").is_file()
            )

    def test_mixed_candidate_outcome_index_rejects_the_open_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            series = self.series.create_series(root / "series", "1.0", "candidate-runner")
            candidate = self.candidate.create_candidate(
                root / "candidates", "1.0.0-rc.1", "rc1", 1, series
            )
            self.assertEqual(0, self._run("StageRc", candidate).returncode)
            outcome_index = self._successful_outcomes(root, candidate)
            index = read_json(outcome_index)
            index["candidate_id"] = "another-candidate"
            write_json(outcome_index, index)

            completed = self._run(
                "FinalizeRc",
                candidate,
                "--stage-outcomes-index",
                str(outcome_index),
            )

            self.assertEqual(1, completed.returncode)
            value = read_json(candidate)
            self.assertEqual("rejected", value["state"])
            self.assertTrue(value["sealed"])
            self.assertFalse((candidate.parent / "evidence/FULL_MATRIX_EVIDENCE.json").exists())

    def test_publication_credentials_force_a_sealed_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            series = self.series.create_series(root / "series", "1.0", "candidate-runner")
            candidate = self.candidate.create_candidate(
                root / "candidates", "1.0.0-rc.1", "rc1", 1, series
            )
            self.assertEqual(0, self._run("StageRc", candidate).returncode)
            outcome_index = self._successful_outcomes(root, candidate)
            environment = os.environ.copy()
            environment["CRATES_IO_TOKEN"] = "fixture-secret"

            completed = self._run(
                "FinalizeRc",
                candidate,
                "--stage-outcomes-index",
                str(outcome_index),
                environment=environment,
            )

            self.assertEqual(1, completed.returncode)
            self.assertEqual("rejected", read_json(candidate)["state"])
            no_remote = read_json(candidate.parent / "evidence/NO_REMOTE_PUBLICATION.json")
            self.assertEqual("violation-detected", no_remote["remote_publication"]["status"])
            self.assertEqual(["CRATES_IO_TOKEN"], no_remote["publishing_credential_names"])
            self.assertNotIn("fixture-secret", str(no_remote))

    def test_finalize_final_requires_delta_and_enters_open_ga_candidate_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            series = self.series.create_series(root / "series", "1.0", "candidate-runner")
            parent: Path | None = None
            for suffix in (1, 2):
                candidate = self.candidate.create_candidate(
                    root / "candidates",
                    f"1.0.0-rc.{suffix}",
                    f"rc{suffix}",
                    1,
                    series,
                )
                source = create_source_bundle(
                    candidate.parent / "CORE_SOURCE_TRANSFER.tar",
                    version=f"1.0.0-rc.{suffix}",
                    run_id=f"rc{suffix}",
                    attempt=1,
                )
                self.candidate.record_build_source_bundle(candidate, source)
                self.assertEqual(0, self._run("StageRc", candidate).returncode)
                outcomes = self._successful_outcomes(root / f"rc{suffix}", candidate)
                finalized = self._run(
                    "FinalizeRc",
                    candidate,
                    "--stage-outcomes-index",
                    str(outcomes),
                )
                self.assertEqual(0, finalized.returncode, finalized.stdout + finalized.stderr)
                parent = candidate
            self.assertIsNotNone(parent)
            final = self.candidate.create_candidate(
                root / "candidates", "1.0.0", "final", 1, series
            )
            outcomes = self._successful_outcomes(root / "final", final)
            parent_value = read_json(parent)
            parent_source = Path(parent_value["source_snapshot"]).parent / "source"

            completed = self._run(
                "FinalizeFinalFunctional",
                final,
                "--stage-outcomes-index",
                str(outcomes),
                "--parent-manifest",
                str(parent),
                "--source-root",
                str(parent_source),
            )

            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            value = read_json(final)
            self.assertEqual("ga-candidate-ready", value["state"])
            self.assertFalse(value["sealed"])
            delta = read_json(final.parent / "evidence/FINAL_CANDIDATE_DELTA.json")
            self.assertEqual("passed", delta["status"])
            self.assertEqual(parent_value["candidate_id"], delta["parentCandidateId"])

            rejected = self._run(
                "RejectFinalHandoff",
                final,
                "--rejection-reason",
                "final handoff verification failed",
            )

            self.assertEqual(0, rejected.returncode, rejected.stdout + rejected.stderr)
            rejected_value = read_json(final)
            self.assertEqual("rejected", rejected_value["state"])
            self.assertTrue(rejected_value["sealed"])
            self.assertEqual(
                "final handoff verification failed", rejected_value["rejection_reason"]
            )

    def _successful_outcomes(
        self,
        root: Path,
        candidate: Path,
        *,
        missing_job: str | None = None,
    ) -> Path:
        identity = read_json(candidate)
        policy = ROOT / "distribution/candidate-stage-outcome-policy.json"
        entries = self.collector._load_policy(policy, "release-candidate")
        bundles = root / "bundles"
        workflow_jobs: dict[str, str] = {}
        required_capabilities = [
            *(f"F-{index:02d}" for index in range(1, 19)),
            *(f"G-{index:02d}" for index in range(1, 6)),
        ]
        common = {
            "candidate_id": identity["candidate_id"],
            "version": identity["version"],
            "run_id": identity["run_id"],
            "attempt": identity["attempt"],
        }
        for entry in entries:
            job_id = entry["job_id"]
            if job_id == missing_job:
                workflow_jobs[job_id] = "failure"
                continue
            worker_id = f"worker-{job_id}"
            job_root = bundles / job_id
            result_files: list[str] = []
            for result_id in entry["result_ids"]:
                relative = f"results/{result_id}.json"
                result_files.append(relative)
                is_test = result_id[0] in {"P", "I", "M", "L", "A", "U", "S"}
                write_json(
                    job_root / relative,
                    {
                        "schema_version": 1,
                        **common,
                        "phase": 6,
                        "gate_stage": "full-matrix",
                        "result_id": result_id,
                        "result_kind": "test" if is_test else "check",
                        "status": "passed",
                        "command": ["python", "fixture.py", result_id],
                        "exit_code": 0,
                        "matched_test_count": 1 if is_test else 0,
                        "executed_test_count": 1 if is_test else 0,
                        "passed_test_count": 1 if is_test else 0,
                        "failed_test_count": 0,
                        "ignored_test_count": 0,
                        "capability_ids": required_capabilities if result_id == "P01" else [],
                        "result_path": f"results/{job_id}/{result_id}.json",
                    },
                )
            event_pairs: list[dict[str, str]] = []
            for route_id in entry["route_ids"]:
                started = f"events/{route_id}.started.json"
                completed = f"events/{route_id}.completed.json"
                event_pairs.append({"route_id": route_id, "started": started, "completed": completed})
                event = {
                    "schema_version": 1,
                    **common,
                    "route_id": route_id,
                    "worker_id": worker_id,
                    "context_path": f"contexts/{worker_id}.json",
                }
                write_json(job_root / started, {**event, "status": "started", "command": ["python", "worker.py"]})
                write_json(job_root / completed, {**event, "status": "passed", "exit_code": 0})
            context = f"contexts/{worker_id}.json"
            write_json(
                job_root / context,
                {
                    "schema_version": 1,
                    **common,
                    "worker_id": worker_id,
                    "publish_input": False,
                    "publishing_credentials_provided": False,
                },
            )
            write_json(
                job_root / "CANDIDATE_STAGE_OUTCOME.json",
                {
                    "schema_version": 1,
                    **common,
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "target": entry["target"],
                    "status": "success",
                    "workflow_result": "success",
                    "sealed": True,
                    "result_files": result_files,
                    "event_pairs": event_pairs,
                    "context_files": [context],
                },
            )
            workflow_jobs[job_id] = "success"
        workflow = root / "workflow-results.json"
        write_json(workflow, {"schema_version": 1, **common, "jobs": workflow_jobs})
        return self.collector.collect_outcomes(
            candidate,
            bundles,
            workflow,
            policy,
            "release-candidate",
            root / "candidate-outcomes",
        )

    @staticmethod
    def _run(
        mode: str,
        candidate: Path,
        *arguments: str,
        environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(RUNNER),
                "--mode",
                mode,
                "--candidate-manifest",
                str(candidate),
                *arguments,
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
            env=environment,
        )


if __name__ == "__main__":
    unittest.main()

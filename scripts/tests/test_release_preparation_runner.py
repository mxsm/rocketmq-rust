# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.tests.release_archive_test_support import create_candidate
from scripts.tests.release_test_support import read_json, write_json


ROOT = Path(__file__).resolve().parents[2]


class ReleasePreparationRunnerTests(unittest.TestCase):
    def test_both_runners_are_fail_fast_and_never_publish(self) -> None:
        powershell = (ROOT / "scripts" / "run-release-preparation.ps1").read_text(encoding="utf-8")
        bash = (ROOT / "scripts" / "run-release-preparation.sh").read_text(encoding="utf-8")
        self.assertIn("$ErrorActionPreference = \"Stop\"", powershell)
        self.assertIn("$LASTEXITCODE", powershell)
        self.assertIn("set -euo pipefail", bash)
        for source in (powershell, bash):
            lowered = source.lower()
            self.assertNotIn("cargo publish", lowered)
            self.assertNotIn("docker push", lowered)
            self.assertNotIn("helm push", lowered)
            self.assertNotIn("gh release", lowered)
            self.assertIn("release_candidate_command.py", source)
            self.assertIn("no_remote_publication_guard.py", source)
            self.assertIn("release_evidence_guard.py", source)

    @unittest.skipUnless(os.name == "nt", "PowerShell runner integration is Windows-specific")
    def test_aggregate_selects_the_frozen_no_remote_route_denominator(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            candidate_value = read_json(candidate)
            candidate_value["route_denominator"] = json.loads(
                (ROOT / "distribution" / "candidate-route-denominator.json").read_text(
                    encoding="utf-8"
                )
            )
            candidate.write_text(json.dumps(candidate_value), encoding="utf-8")
            result_root = root / "results"
            for result_id in (
                "R01-RELEASE-VERSION",
                "R01-CANDIDATE-LIFECYCLE",
                "R01-CORE-IMAGE-WORKFLOW",
            ):
                write_json(
                    result_root / f"{result_id}.json",
                    {
                        "schema_version": 1,
                        "candidate_id": candidate_value["candidate_id"],
                        "version": candidate_value["version"],
                        "run_id": candidate_value["run_id"],
                        "attempt": candidate_value["attempt"],
                        "phase": 5,
                        "gate_stage": "release-preparation",
                        "result_id": result_id,
                        "result_kind": "check",
                        "status": "passed",
                        "command": ["python", "fixture.py"],
                        "exit_code": 0,
                        "matched_test_count": 0,
                        "executed_test_count": 0,
                        "passed_test_count": 0,
                        "failed_test_count": 0,
                        "ignored_test_count": 0,
                        "capability_ids": [],
                        "result_path": f"results/{result_id}.json",
                    },
                )
            output_root = root / "evidence"

            completed = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-File",
                    str(ROOT / "scripts" / "run-release-preparation.ps1"),
                    "-Mode",
                    "Aggregate",
                    "-CandidateManifest",
                    str(candidate),
                    "-ResultRoot",
                    str(result_root),
                    "-OutputRoot",
                    str(output_root),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )

            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            no_remote = read_json(output_root / "NO_REMOTE_PUBLICATION.json")
            self.assertEqual("release-preparation-aggregate", no_remote["audit_point"])
            self.assertEqual(["R11-aggregate-validate"], no_remote["required_route_ids"])


if __name__ == "__main__":
    unittest.main()

# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import json
import re
import shutil
import subprocess
import tempfile
import unittest

from scripts.tests.release_test_support import create_source_bundle, load_module, read_json, write_gate_evidence
from scripts.tests.test_publication_handoff import PublicationHandoffTests


ROOT = Path(__file__).resolve().parents[2]


class PublicationHandoffRunnerTests(unittest.TestCase):
    def test_cross_platform_runners_are_fail_fast_and_local_only(self) -> None:
        powershell = (ROOT / "scripts" / "run-publication-handoff.ps1").read_text(encoding="utf-8")
        bash = (ROOT / "scripts" / "run-publication-handoff.sh").read_text(encoding="utf-8")
        self.assertIn('$ErrorActionPreference = "Stop"', powershell)
        self.assertIn("$PSNativeCommandUseErrorActionPreference = $true", powershell)
        self.assertIn("set -euo pipefail", bash)
        for mode in ("PrepareDraft", "Platform", "Finalize"):
            self.assertIn(mode, powershell)
            self.assertIn(mode, bash)
        for argument in ("CandidateSourceBundle", "CandidateControlBundle", "DraftBundle"):
            self.assertIn(argument, powershell)
        for script in (powershell, bash):
            self.assertIn("release_candidate_command.py", script)
            self.assertNotRegex(script, re.compile(r"cargo\s+publish|docker\s+(?:login|push)|helm\s+push|git\s+(?:push|tag)", re.I))

    def test_workflow_has_closed_prepare_platform_aggregate_dag_without_publication_permissions(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "v1-functional-acceptance.yml").read_text(encoding="utf-8")
        for job in ("prepare-draft:", "platform-linux:", "platform-windows:", "platform-macos:", "aggregate-handoff:"):
            self.assertIn(job, workflow)
        self.assertIn("if: always()", workflow)
        self.assertIn("contents: read", workflow)
        self.assertNotRegex(workflow, re.compile(r"(?:contents|packages|id-token):\s*write", re.I))
        self.assertNotIn("secrets.", workflow)
        self.assertNotRegex(workflow, re.compile(r"cargo\s+publish|docker\s+(?:login|push)|helm\s+push|git\s+(?:push|tag)", re.I))

    def test_powershell_runner_completes_local_prepare_platform_finalize_flow(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            series_module = load_module("handoff_e2e_series", "distribution/release_series.py")
            candidate_module = load_module("handoff_e2e_candidate", "distribution/candidate_run.py")
            snapshot_module = load_module("handoff_e2e_snapshot", "distribution/create_candidate_source_snapshot.py")
            lifecycle_module = load_module("handoff_e2e_lifecycle", "scripts/release_lifecycle_guard.py")
            transfer_module = load_module("handoff_e2e_transfer", "distribution/transfer_candidate.py")
            series = series_module.create_series(root / "series", "1.0", "community-v1")

            for suffix in (1, 2):
                rc = candidate_module.create_candidate(
                    root / "candidates", f"1.0.0-rc.{suffix}", f"rc{suffix}", 1, series
                )
                rc_value = read_json(rc)
                bundle = create_source_bundle(
                    rc.parent / "CORE_SOURCE_TRANSFER.tar",
                    version=rc_value["version"],
                    run_id=rc_value["run_id"],
                    attempt=rc_value["attempt"],
                )
                candidate_module.record_build_source_bundle(rc, bundle)
                snapshot_module.create_snapshot(rc)
                evidence = write_gate_evidence(rc.parent / "gate-evidence.json", rc_value["candidate_id"])
                lifecycle_module.transition_candidate(rc, "staged-rc", phase=5)
                lifecycle_module.transition_candidate(rc, "rc-candidate-ready", phase=6, gate_evidence=evidence)

            final = candidate_module.create_candidate(root / "candidates", "1.0.0", "final", 1, series)
            gate = write_gate_evidence(final.parent / "gate-evidence.json", read_json(final)["candidate_id"])
            lifecycle_module.transition_candidate(final, "ga-candidate-ready", phase=6, gate_evidence=gate)
            (root / "payload").mkdir()
            payload = PublicationHandoffTests._fixture(root / "payload")
            for entry in payload["candidate_root"].iterdir():
                if entry.name == "CANDIDATE_RUN.json":
                    continue
                destination = final.parent / entry.name
                if entry.is_dir():
                    shutil.copytree(entry, destination)
                else:
                    shutil.copyfile(entry, destination)
            identity = read_json(final)
            for name in ("ARTIFACT_INDEX.json", "EVIDENCE_INDEX.json", "NO_REMOTE_PUBLICATION.json"):
                path = final.parent / name
                value = json.loads(path.read_text(encoding="utf-8"))
                for field in ("candidate_id", "version", "run_id", "attempt"):
                    value[field] = identity[field]
                path.write_text(json.dumps(value), encoding="utf-8")
            (final.parent / "evidence").mkdir(exist_ok=True)
            for name in ("EVIDENCE_INDEX.json", "NO_REMOTE_PUBLICATION.json"):
                (final.parent / name).replace(final.parent / "evidence" / name)

            transfer = final.parent / "transfer"
            source_bundle = transfer / "CANDIDATE_SOURCE_BUNDLE.tar"
            control_bundle = transfer / "CANDIDATE_CONTROL_BUNDLE.tar"
            self.assertEqual(
                0,
                transfer_module.main(
                    [
                        "export-artifacts",
                        "--candidate-manifest",
                        str(final),
                        "--output",
                        str(source_bundle),
                        "--repository-source-root",
                        str(payload["source_root"]),
                    ]
                ),
            )
            self.assertEqual(
                0,
                transfer_module.main(
                    ["export-build-control", "--candidate-manifest", str(final), "--output", str(control_bundle)]
                ),
            )
            output = root / "handoff"
            draft_bundle = root / "HANDOFF_DRAFT_TRANSFER.tar"
            platform_root = root / "platforms"
            script = ROOT / "scripts" / "run-publication-handoff.ps1"

            self._run_powershell(
                script,
                "PrepareDraft",
                source_bundle,
                control_bundle,
                ["-OutputRoot", str(output), "-DraftBundleOutput", str(draft_bundle)],
            )
            for platform, result_id in (
                ("linux", "H01-LINUX"),
                ("windows", "H01-WINDOWS"),
                ("macos", "H01-MACOS"),
            ):
                self._run_powershell(
                    script,
                    "Platform",
                    source_bundle,
                    control_bundle,
                    [
                        "-DraftBundle",
                        str(draft_bundle),
                        "-Platform",
                        platform,
                        "-ResultId",
                        result_id,
                        "-PlatformBundleOutput",
                        str(platform_root / result_id),
                    ],
                )
            self._run_powershell(
                script,
                "Finalize",
                source_bundle,
                control_bundle,
                [
                    "-OutputRoot",
                    str(output),
                    "-DraftBundle",
                    str(draft_bundle),
                    "-PlatformBundlesRoot",
                    str(platform_root),
                ],
            )
            handoff = output / "1.0.0" / identity["run_id"] / "attempt-1"
            self.assertTrue((handoff / "PUBLICATION_READY.json").is_file())
            self.assertEqual("publication-ready", read_json(final)["state"])
            self.assertEqual(
                "not-executed",
                read_json(handoff / "PUBLICATION_READY.json")["remote_publication"]["status"],
            )
            final_import = next(output.glob(".handoff-finalize-*/candidate-source"))
            verifier = load_module("handoff_e2e_ready_verifier", "distribution/verify_publication_handoff.py")
            ready = verifier.verify_handoff(
                handoff,
                final,
                final_import,
                final_import / "repository-source",
                mode="ready",
                result_id="H06-PUBLICATION-READY",
            )
            self.assertEqual("passed", ready["status"])

    @staticmethod
    def _run_powershell(
        script: Path,
        mode: str,
        source_bundle: Path,
        control_bundle: Path,
        extra: list[str],
    ) -> None:
        completed = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-File",
                str(script),
                "-Mode",
                mode,
                "-CandidateSourceBundle",
                str(source_bundle),
                "-CandidateControlBundle",
                str(control_bundle),
                *extra,
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        if completed.returncode != 0:
            raise AssertionError(
                f"handoff runner {mode} failed ({completed.returncode})\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
            )


if __name__ == "__main__":
    unittest.main()

# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import json
import os
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

from scripts.tests.release_archive_test_support import create_candidate
from scripts.tests.release_test_support import load_module, read_json


ROOT = Path(__file__).resolve().parents[2]


class ReleasePreparationRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.runner = load_module("release_preparation_test", "scripts/release_preparation.py")
        cls.series = load_module("release_series_preparation_test", "distribution/release_series.py")
        cls.candidate = load_module("candidate_run_preparation_test", "distribution/candidate_run.py")

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

    def test_prepare_common_plan_produces_closed_source_and_control_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            candidate_root = candidate.parent
            request = self.runner.PreparationRequest(
                mode="PrepareCommon",
                candidate_manifest=candidate,
                common_inputs_bundle_output=candidate_root / "common" / "COMMON_RELEASE_INPUTS.tar",
                build_source_bundle_output=candidate_root / "source" / "CORE_SOURCE_TRANSFER.tar",
                build_control_bundle_output=candidate_root / "control" / "CANDIDATE_BUILD_CONTROL_BUNDLE.tar",
            )

            routes = [step.route_id for step in self.runner.build_steps(request)]

            self.assertEqual(
                [
                    "R11-prepare-common-validate",
                    "R05-render-release-notes",
                    "R05-build-common-inputs",
                    "R05-export-common-inputs",
                    "R05-export-build-source",
                    "R05-record-build-source",
                    "R05-export-build-control",
                ],
                routes,
            )

    def test_prepare_common_executes_and_seals_portable_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            series = self.series.create_series(base / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(
                base / "candidates", "1.0.0-rc.1", "local", 1, series
            )
            root = candidate.parent
            common = root / "common" / "COMMON_RELEASE_INPUTS.tar"
            source = root / "source" / "CORE_SOURCE_TRANSFER.tar"
            control = root / "source" / "CANDIDATE_BUILD_CONTROL_BUNDLE.tar"

            self.runner.run_preparation(
                self.runner.PreparationRequest(
                    mode="PrepareCommon",
                    candidate_manifest=candidate,
                    common_inputs_bundle_output=common,
                    build_source_bundle_output=source,
                    build_control_bundle_output=control,
                )
            )

            self.assertTrue(common.is_file())
            self.assertTrue(source.is_file())
            self.assertTrue(control.is_file())
            candidate_value = read_json(candidate)
            self.assertEqual(str(source.resolve()), candidate_value["build_source_bundle"])
            self.assertGreater(candidate_value["generation"], 0)

    def test_target_plan_consumes_sealed_inputs_and_exports_one_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            candidate_root = candidate.parent
            common = candidate_root / "COMMON_RELEASE_INPUTS.tar"
            source = candidate_root / "CORE_SOURCE_TRANSFER.tar"
            control = candidate_root / "CANDIDATE_BUILD_CONTROL_BUNDLE.tar"
            for path in (common, source, control):
                path.write_bytes(b"fixture")
            request = self.runner.PreparationRequest(
                mode="Target",
                candidate_manifest=candidate,
                common_inputs_bundle=common,
                build_source_bundle=source,
                build_control_bundle=control,
                target="x86_64-unknown-linux-gnu",
                target_bundle_output=candidate_root / "targets" / "linux.tar",
            )

            routes = [step.route_id for step in self.runner.build_steps(request)]

            self.assertEqual("R05-import-build-source-x86_64-unknown-linux-gnu", routes[1])
            self.assertIn("R05-build-archive-x86_64-unknown-linux-gnu", routes)
            self.assertIn("R05-build-oci-x86_64-unknown-linux-gnu", routes)
            self.assertEqual("R05-export-target-x86_64-unknown-linux-gnu", routes[-1])

    def test_aggregate_plan_requires_the_complete_target_denominator(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            bundles = root / "target-bundles"
            bundles.mkdir()
            for target in self.runner.TARGETS[:-1]:
                (bundles / f"{target}.tar").write_bytes(b"fixture")
            request = self.runner.PreparationRequest(
                mode="Aggregate",
                candidate_manifest=candidate,
                target_bundles_root=bundles,
                candidate_source_bundle_output=candidate.parent / "transfer" / "CANDIDATE_SOURCE_BUNDLE.tar",
            )

            with self.assertRaisesRegex(self.runner.PreparationError, "missing target bundle"):
                self.runner.build_steps(request)

    def test_execution_stops_at_the_first_failed_route(self) -> None:
        steps = [
            self.runner.PreparationStep("first", ("python", "first.py")),
            self.runner.PreparationStep("second", ("python", "second.py")),
        ]
        executed: list[str] = []

        def execute(step):
            executed.append(step.route_id)
            return 17

        with self.assertRaisesRegex(self.runner.PreparationError, "first"):
            self.runner.execute_steps(steps, execute)
        self.assertEqual(["first"], executed)

    def test_target_rejects_a_cross_candidate_input_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = create_candidate(root)
            value = read_json(candidate)
            bundles = []
            for name, kind in (
                ("common.tar", "common-inputs"),
                ("source.tar", "build-source"),
                ("control.tar", "build-control"),
            ):
                bundle = root / name
                manifest = root / f"{name}.json"
                manifest.write_text(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "bundle_kind": kind,
                            "candidate_id": "another-candidate",
                            "version": value["version"],
                            "run_id": value["run_id"],
                            "attempt": value["attempt"],
                            "files": [],
                        }
                    ),
                    encoding="utf-8",
                )
                with tarfile.open(bundle, "w") as archive:
                    archive.add(manifest, arcname="CANDIDATE_TRANSFER.json")
                bundles.append(bundle)
            request = self.runner.PreparationRequest(
                mode="Target",
                candidate_manifest=candidate,
                common_inputs_bundle=bundles[0],
                build_source_bundle=bundles[1],
                build_control_bundle=bundles[2],
                target="x86_64-unknown-linux-gnu",
                target_bundle_output=candidate.parent / "target.tar",
            )

            with self.assertRaisesRegex(self.runner.PreparationError, "candidate identity"):
                self.runner.validate_request_bundles(request)

    @unittest.skipUnless(os.name == "nt", "PowerShell runner integration is Windows-specific")
    def test_aggregate_without_platform_inputs_fails_closed(self) -> None:
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
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )

            self.assertNotEqual(0, completed.returncode, completed.stdout + completed.stderr)
            self.assertIn("bundle is required", completed.stdout + completed.stderr)
            self.assertFalse((output_root / "NO_REMOTE_PUBLICATION.json").exists())


if __name__ == "__main__":
    unittest.main()

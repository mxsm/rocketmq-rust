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
from unittest import mock

from scripts.tests.release_test_support import (
    create_source_bundle,
    load_module,
    read_json,
    write_gate_evidence,
    write_json,
)
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

    def test_powershell_runner_prepares_real_release_archive_layout(self) -> None:
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
            for path in (final.parent / "archives").glob("*.manifest.json"):
                value = json.loads(path.read_text(encoding="utf-8"))
                for field in ("candidate_id", "version", "run_id", "attempt"):
                    value[field] = identity[field]
                value["artifact_id"] = f"{identity['candidate_id']}.{value['target']}.archive"
                for binary in value["binaries"]:
                    binary["artifact_id"] = (
                        f"{identity['candidate_id']}.{value['target']}.{binary['component']}"
                    )
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
            script = ROOT / "scripts" / "run-publication-handoff.ps1"

            self._run_powershell(
                script,
                "PrepareDraft",
                source_bundle,
                control_bundle,
                ["-OutputRoot", str(output), "-DraftBundleOutput", str(draft_bundle)],
            )
            transfer = load_module("handoff_e2e_draft_transfer", "distribution/transfer_handoff_draft.py")
            draft_manifest = transfer.read_transfer_manifest(draft_bundle)
            archive_paths = {
                entry["path"] for entry in draft_manifest["files"] if entry["path"].startswith("archives/")
            }
            self.assertEqual(3, len([path for path in archive_paths if path.endswith(".manifest.json")]))
            self.assertEqual(3, len([path for path in archive_paths if path.endswith((".zip", ".tar.gz"))]))
            self.assertFalse(any(path.startswith("archives/linux/") for path in archive_paths))

            prepare_import = next(output.glob(".handoff-preparedraft-*/candidate-source"))
            prepare_control = next(output.glob(".handoff-preparedraft-*/candidate-control/CANDIDATE_RUN.json"))
            draft = root / "platform-draft"
            transfer.import_draft(draft_bundle, draft, read_json(prepare_control))
            verifier = load_module("handoff_e2e_verifier", "distribution/verify_publication_handoff.py")
            layout = read_json(ROOT / "distribution/release-layout.json")
            binary_by_name = {
                entry.get("archive_binary", entry["binary"]): entry for entry in layout["binaries"]
            }
            platform_bundles = root / "platform-bundles"
            for platform, result_id, target in (
                ("linux", "H01-LINUX", "x86_64-unknown-linux-gnu"),
                ("windows", "H01-WINDOWS", "x86_64-pc-windows-msvc"),
                ("macos", "H01-MACOS", "x86_64-apple-darwin"),
            ):
                bundle = platform_bundles / result_id
                worker = f"handoff-{platform}"
                context_reference = f"contexts/{worker}.json"
                candidate_identity = read_json(prepare_control)

                def version_result(command, **_kwargs):
                    name = Path(command[0]).name.removesuffix(".exe")
                    binary = binary_by_name[name]
                    return mock.Mock(
                        returncode=0,
                        stdout=(
                            f"component={binary['id']}\n"
                            f"version={candidate_identity['version']}\n"
                            f"artifact_id={candidate_identity['candidate_id']}.{target}.{binary['id']}\n"
                            f"requested_features={','.join(binary['requested_features'])}\n"
                            f"effective_features={','.join(binary['effective_features'])}\n"
                        ),
                        stderr="",
                    )

                with mock.patch("subprocess.run", side_effect=version_result):
                    report = verifier.verify_handoff(
                        draft,
                        prepare_control,
                        prepare_import,
                        prepare_import / "repository-source",
                        mode="draft-pre-ready",
                        result_id=result_id,
                        platform=platform,
                        worker_id=worker,
                    )
                write_json(bundle / f"{result_id}.json", report)
                event_identity = {
                    "schema_version": 1,
                    "candidate_id": candidate_identity["candidate_id"],
                    "version": candidate_identity["version"],
                    "run_id": candidate_identity["run_id"],
                    "attempt": candidate_identity["attempt"],
                    "route_id": result_id,
                    "worker_id": worker,
                    "context_path": context_reference,
                }
                write_json(
                    bundle / "events" / f"{result_id}.started.json",
                    {**event_identity, "status": "started", "command": ["python", "verify.py", result_id]},
                )
                write_json(
                    bundle / "events" / f"{result_id}.completed.json",
                    {**event_identity, "status": "passed", "exit_code": 0},
                )
                write_json(
                    bundle / context_reference,
                    {
                        "schema_version": 1,
                        "candidate_id": candidate_identity["candidate_id"],
                        "version": candidate_identity["version"],
                        "run_id": candidate_identity["run_id"],
                        "attempt": candidate_identity["attempt"],
                        "worker_id": worker,
                        "publish_input": False,
                        "publishing_credentials_provided": False,
                    },
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
                    str(platform_bundles),
                ],
            )
            final_handoff = output / "1.0.0" / identity["run_id"] / "attempt-1"
            self.assertTrue((final_handoff / "PUBLICATION_READY.json").is_file())
            final_import = next(output.glob(".handoff-finalize-*/candidate-source"))
            final_evidence = read_json(final_import / "evidence/FINAL_HANDOFF_EVIDENCE.json")
            self.assertTrue(final_evidence["all_required_passed"])
            self.assertEqual("final-handoff", final_evidence["gate_stage"])
            self.assertEqual("publication-ready", read_json(final)["state"])

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

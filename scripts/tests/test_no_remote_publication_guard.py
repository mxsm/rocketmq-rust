# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import os
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.tests.release_archive_test_support import create_candidate
from scripts.tests.release_test_support import load_module, read_json


class NoRemotePublicationGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.context = load_module(
            "capture_context_for_no_remote", "scripts/capture_candidate_execution_context.py"
        )
        cls.command = load_module(
            "candidate_command_for_no_remote", "scripts/release_candidate_command.py"
        )
        cls.guard = load_module(
            "no_remote_publication_guard", "scripts/no_remote_publication_guard.py"
        )

    def _completed_route(self, candidate: Path, root: Path) -> tuple[Path, Path]:
        contexts = root / "contexts"
        events = root / "events"
        context = self.context.capture_context(candidate, "worker-1", contexts)
        result = self.command.run_command(
            candidate,
            route_id="R01-local-check",
            worker_id="worker-1",
            context_path=context,
            event_root=events,
            command=[os.sys.executable, "-c", "pass"],
        )
        self.assertEqual(result, 0)
        return contexts, events

    def test_local_read_only_workflows_and_paired_events_are_not_executed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            contexts, events = self._completed_route(candidate, root)
            output = root / "NO_REMOTE_PUBLICATION.json"

            self.guard.audit_no_remote_publication(
                candidate,
                phase=5,
                context_root=contexts,
                event_root=events,
                workflow_root=Path(__file__).resolve().parents[2] / ".github" / "workflows",
                output=output,
            )

            value = read_json(output)
            self.assertEqual(value["remote_publication"]["status"], "not-executed")
            self.assertEqual(value["remote_publication_workflow_dispatches"], [])
            self.assertFalse(value["publishing_credentials_provided"])

    def test_credentials_are_a_violation_and_missing_context_is_indeterminate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            contexts = root / "contexts"
            with mock.patch.dict(os.environ, {"CARGO_REGISTRY_TOKEN": "secret"}, clear=False):
                self.context.capture_context(candidate, "worker-1", contexts)
            with self.assertRaises(self.guard.NoRemotePublicationError) as violation:
                self.guard.audit_no_remote_publication(
                    candidate,
                    phase=5,
                    context_root=contexts,
                    event_root=root / "events",
                    workflow_root=Path(__file__).resolve().parents[2] / ".github" / "workflows",
                    output=root / "violation.json",
                )
            self.assertEqual(violation.exception.status, "violation-detected")

            with self.assertRaises(self.guard.NoRemotePublicationError) as indeterminate:
                self.guard.audit_no_remote_publication(
                    candidate,
                    phase=5,
                    context_root=root / "missing-contexts",
                    event_root=root / "events",
                    workflow_root=Path(__file__).resolve().parents[2] / ".github" / "workflows",
                    output=root / "indeterminate.json",
                )
            self.assertEqual(indeterminate.exception.status, "indeterminate")

    def test_guard_can_run_inside_its_current_route_without_self_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = create_candidate(root)
            contexts, events = self._completed_route(candidate, root)
            context = contexts / "worker-1.json"
            output = root / "NO_REMOTE_PUBLICATION.json"
            script = Path(__file__).resolve().parents[1] / "no_remote_publication_guard.py"

            exit_code = self.command.run_command(
                candidate,
                route_id="R11-no-remote",
                worker_id="worker-1",
                context_path=context,
                event_root=events,
                command=[
                    os.sys.executable,
                    str(script),
                    "--candidate-manifest",
                    str(candidate),
                    "--phase",
                    "5",
                    "--context-root",
                    str(contexts),
                    "--event-root",
                    str(events),
                    "--output",
                    str(output),
                ],
            )

            self.assertEqual(exit_code, 0)
            self.assertEqual(read_json(output)["remote_publication"]["status"], "not-executed")

    def test_static_guard_rejects_write_permissions_and_publication_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workflow_root = Path(temp_dir)
            (workflow_root / "release-candidate.yml").write_text(
                "on: [push]\npermissions:\n  packages: write\njobs:\n  bad:\n    steps:\n      - run: docker push example.invalid/image\n",
                encoding="utf-8",
            )
            findings = self.guard.audit_workflow_files(
                workflow_root, ["release-candidate.yml"]
            )
            self.assertTrue(any("write permission" in finding for finding in findings))
            self.assertTrue(any("remote publication command" in finding for finding in findings))

    def test_handoff_contract_is_scanned_instead_of_trusting_a_boolean(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            handoff = Path(temp_dir)
            (handoff / "PUBLICATION_HANDOFF.json").write_text(
                json.dumps(
                    {
                        "remote_publication": {"status": "not-executed"},
                        "future_publication": {"executed": False},
                    }
                ),
                encoding="utf-8",
            )
            (handoff / "safe.ps1").write_text("Write-Output 'local-only'\n", encoding="utf-8")
            self.assertEqual([], self.guard.audit_handoff(handoff))

            (handoff / "unsafe.ps1").write_text("docker login ghcr.io\n", encoding="utf-8")
            findings = self.guard.audit_handoff(handoff)
            self.assertTrue(any("remote publication command" in finding for finding in findings))


if __name__ == "__main__":
    unittest.main()

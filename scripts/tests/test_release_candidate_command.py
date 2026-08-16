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

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.tests.release_test_support import load_module, read_json


class ReleaseCandidateCommandTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.series = load_module("release_series_for_command", "distribution/release_series.py")
        cls.candidate = load_module("candidate_run_for_command", "distribution/candidate_run.py")
        cls.context = load_module(
            "capture_candidate_execution_context",
            "scripts/capture_candidate_execution_context.py",
        )
        cls.command = load_module("release_candidate_command", "scripts/release_candidate_command.py")

    def test_wrapper_records_worker_context_and_exactly_once_started_completed_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)
            context = self.context.capture_context(candidate, "windows-1", root / "context")
            events = root / "events"

            result = self.command.run_command(
                candidate,
                route_id="R01-version-check",
                worker_id="windows-1",
                context_path=context,
                event_root=events,
                command=[sys.executable, "-c", "print('ok')"],
            )
            self.assertEqual(result, 0)
            self.assertEqual(read_json(events / "R01-version-check.started.json")["status"], "started")
            self.assertEqual(read_json(events / "R01-version-check.completed.json")["exit_code"], 0)
            with self.assertRaises(self.command.CommandError):
                self.command.run_command(
                    candidate,
                    route_id="R01-version-check",
                    worker_id="windows-1",
                    context_path=context,
                    event_root=events,
                    command=[sys.executable, "-c", "pass"],
                )

    def test_failed_child_is_recorded_without_becoming_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)
            context = self.context.capture_context(candidate, "linux-1", root / "context")
            events = root / "events"
            exit_code = self.command.run_command(
                candidate,
                route_id="R01-failure",
                worker_id="linux-1",
                context_path=context,
                event_root=events,
                command=[sys.executable, "-c", "raise SystemExit(7)"],
            )
            self.assertEqual(exit_code, 7)
            self.assertEqual(read_json(events / "R01-failure.completed.json")["status"], "failed")

    def test_sealing_lifecycle_route_writes_completion_before_the_candidate_freezes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)
            context = self.context.capture_context(candidate, "lifecycle-1", root / "context")
            events = root / "events"
            lifecycle = Path(__file__).resolve().parents[1] / "release_lifecycle_guard.py"

            exit_code = self.command.run_command(
                candidate,
                route_id="R01-reject-rc",
                worker_id="lifecycle-1",
                context_path=context,
                event_root=events,
                command=[
                    sys.executable,
                    str(lifecycle),
                    "--candidate-manifest",
                    str(candidate),
                    "--transition",
                    "rejected",
                    "--phase",
                    "5",
                    "--rejection-reason",
                    "fixture",
                    "--current-route-id",
                    "R01-reject-rc",
                ],
            )
            self.assertEqual(exit_code, 0)
            completed = read_json(events / "R01-reject-rc.completed.json")
            self.assertTrue(completed["lifecycle_atomic_completion"])
            self.assertTrue(read_json(candidate)["sealed"])

    def test_child_failure_cannot_be_hidden_by_a_premature_success_fragment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)
            context = self.context.capture_context(candidate, "fault-1", root / "context")
            events = root / "events"
            child = (
                "import json,os; "
                "json.dump({'schema_version':1,'candidate_id':os.environ['RELEASE_CANDIDATE_MANIFEST'] and "
                "json.load(open(os.environ['RELEASE_CANDIDATE_MANIFEST']))['candidate_id'],"
                "'route_id':'R01-premature','status':'passed','exit_code':0},"
                "open(os.environ['RELEASE_CANDIDATE_COMPLETED_EVENT'],'w')); "
                "raise SystemExit(7)"
            )
            with self.assertRaises(self.command.CommandError):
                self.command.run_command(
                    candidate,
                    route_id="R01-premature",
                    worker_id="fault-1",
                    context_path=context,
                    event_root=events,
                    command=[sys.executable, "-c", child],
                )

    def test_secret_like_command_options_are_redacted_from_events(self) -> None:
        rendered = self.command._redact_command(
            [
                "publisher",
                "--api-token",
                "token-value",
                "--registry-password=password-value",
                "--private-key",
                "key-value",
                "--version",
                "1.0.0-rc.1",
            ]
        )
        self.assertEqual(
            rendered,
            [
                "publisher",
                "--api-token",
                "<redacted>",
                "--registry-password=<redacted>",
                "--private-key",
                "<redacted>",
                "--version",
                "1.0.0-rc.1",
            ],
        )

    def test_remote_publication_commands_are_rejected_before_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)
            context = self.context.capture_context(candidate, "publisher-1", root / "context")
            events = root / "events"

            with self.assertRaises(self.command.CommandError):
                self.command.run_command(
                    candidate,
                    route_id="R01-forbidden-publish",
                    worker_id="publisher-1",
                    context_path=context,
                    event_root=events,
                    command=["cargo", "publish"],
                )
            self.assertFalse(events.exists())

    def test_publishing_credentials_are_removed_from_child_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)
            context = self.context.capture_context(candidate, "sanitized-1", root / "context")
            with mock.patch.dict(
                os.environ,
                {"GITHUB_TOKEN": "secret", "CARGO_REGISTRIES_PRIVATE_TOKEN": "secret"},
                clear=False,
            ):
                exit_code = self.command.run_command(
                    candidate,
                    route_id="R01-sanitized",
                    worker_id="sanitized-1",
                    context_path=context,
                    event_root=root / "events",
                    command=[
                        sys.executable,
                        "-c",
                        "import os; raise SystemExit(any(name in os.environ for name in "
                        "('GITHUB_TOKEN','CARGO_REGISTRIES_PRIVATE_TOKEN')))",
                    ],
                )
            self.assertEqual(exit_code, 0)

    def test_sealed_candidate_cannot_capture_a_new_execution_context(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(
                root / "candidates", "1.0.0-rc.1", "rc1", 1, series
            )
            lifecycle = load_module(
                "release_lifecycle_for_sealed_context", "scripts/release_lifecycle_guard.py"
            )
            lifecycle.transition_candidate(
                candidate, "rejected", phase=5, rejection_reason="fixture"
            )

            with self.assertRaises(self.context.ContextError):
                self.context.capture_context(candidate, "late-worker", root / "context")


if __name__ == "__main__":
    unittest.main()

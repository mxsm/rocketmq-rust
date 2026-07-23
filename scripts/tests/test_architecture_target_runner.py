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

import contextlib
import io
import json
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import architecture_target_runner as runner  # noqa: E402


class ArchitectureTargetRunnerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = json.loads(
            (SCRIPTS / "architecture-performance-profiles.json").read_text(encoding="utf-8")
        )

    def test_inventory_exactly_matches_the_frozen_policy(self) -> None:
        expected_checks = set(self.policy["required_correctness_checks"])
        expected_variants = {
            (profile["id"], variant["id"])
            for profile in self.policy["profiles"]
            for variant in profile["variants"]
        }
        self.assertEqual(expected_checks, set(runner.CORRECTNESS_COMMANDS))
        self.assertEqual(expected_variants, set(runner.MEASUREMENT_COMMANDS))
        self.assertEqual(4, len(runner.CORRECTNESS_COMMANDS))
        self.assertEqual(11, len(runner.MEASUREMENT_COMMANDS))

    def test_every_command_is_an_explicit_argument_vector(self) -> None:
        for commands in runner.CORRECTNESS_COMMANDS.values():
            self.assertGreaterEqual(len(commands), 1)
            for command in commands:
                self.assertEqual("cargo", command[0])
                self.assertNotIn("shell", " ".join(command).lower())
        for (profile, variant), command in runner.MEASUREMENT_COMMANDS.items():
            self.assertEqual("cargo", command[0])
            self.assertIn("--release", command)
            self.assertIn("--quiet", command)
            self.assertEqual([profile, variant], list(command[-2:]))

    def test_resolves_only_complete_known_invocations(self) -> None:
        self.assertEqual(
            runner.CORRECTNESS_COMMANDS["sync_flush_crash_recovery"],
            runner.resolve_commands(["correctness", "sync_flush_crash_recovery"]),
        )
        self.assertEqual(
            (runner.MEASUREMENT_COMMANDS[("overload", "bounded-rejection")],),
            runner.resolve_commands(["measurement", "overload", "bounded-rejection"]),
        )
        for invalid in [
            [],
            ["correctness"],
            ["correctness", "unknown"],
            ["measurement", "overload"],
            ["measurement", "overload", "unknown"],
            ["other", "bounded_overload"],
        ]:
            with self.assertRaises(ValueError):
                runner.resolve_commands(invalid)

    @mock.patch.object(runner.subprocess, "run")
    def test_execution_stops_on_the_first_failure(self, run: mock.Mock) -> None:
        run.side_effect = [
            mock.Mock(returncode=0),
            mock.Mock(returncode=7),
            mock.Mock(returncode=0),
        ]
        result = runner.run_commands([["first"], ["second"], ["third"]])
        self.assertEqual(7, result)
        self.assertEqual(2, run.call_count)
        for call in run.call_args_list:
            self.assertEqual(runner.REPOSITORY_ROOT, call.kwargs["cwd"])
            self.assertFalse(call.kwargs["check"])

    def test_cli_unknown_inventory_fails_closed_without_execution(self) -> None:
        stderr = io.StringIO()
        with (
            mock.patch.object(runner, "run_commands") as run,
            contextlib.redirect_stderr(stderr),
        ):
            self.assertEqual(2, runner.main(["correctness", "unknown"]))
        run.assert_not_called()
        self.assertIn("ARCHITECTURE_TARGET_RUNNER_FAILED", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()

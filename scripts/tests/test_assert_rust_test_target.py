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
import importlib.util
import io
import subprocess
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "assert-rust-test-target.py"
SPEC = importlib.util.spec_from_file_location("assert_rust_test_target", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
target_guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(target_guard)


def completed(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


class AssertRustTestTargetTest(unittest.TestCase):
    @mock.patch.object(target_guard.subprocess, "run")
    def test_runs_list_and_target_with_explicit_features_and_cargo_args(self, run: mock.Mock) -> None:
        run.side_effect = [
            completed(0, "lifecycle::closes_once: test\n"),
            completed(0, "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n"),
        ]

        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            result = target_guard.main(
                [
                    "--package",
                    "rocketmq-store-local",
                    "--target",
                    "mapped_file_kernel",
                    "--features",
                    "safe-load,fast-load",
                    "--cargo-arg=--locked",
                ]
            )

        self.assertEqual(0, result)
        self.assertIn("RUST_TEST_TARGET_OK", stdout.getvalue())
        self.assertEqual(2, run.call_count)
        list_command = run.call_args_list[0].args[0]
        execute_command = run.call_args_list[1].args[0]
        expected_prefix = [
            "cargo",
            "test",
            "-p",
            "rocketmq-store-local",
            "--test",
            "mapped_file_kernel",
            "--features",
            "safe-load,fast-load",
            "--locked",
        ]
        self.assertEqual(expected_prefix + ["--", "--list", "--format", "terse"], list_command)
        self.assertEqual(expected_prefix, execute_command)
        for call in run.call_args_list:
            self.assertEqual(ROOT, call.kwargs["cwd"])
            self.assertTrue(call.kwargs["text"])
            self.assertTrue(call.kwargs["capture_output"])
            self.assertFalse(call.kwargs["check"])

    @mock.patch.object(target_guard.subprocess, "run")
    def test_empty_target_fails_before_execution(self, run: mock.Mock) -> None:
        run.return_value = completed(0, "0 tests, 0 benchmarks\n")

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as stderr:
            result = target_guard.main(["--package", "pkg", "--target", "empty"])

        self.assertEqual(1, result)
        self.assertEqual(1, run.call_count)
        self.assertIn("no tests were listed", stderr.getvalue())

    @mock.patch.object(target_guard.subprocess, "run")
    def test_all_ignored_target_fails(self, run: mock.Mock) -> None:
        run.side_effect = [
            completed(0, "only_test: test\n"),
            completed(0, "test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out\n"),
        ]

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as stderr:
            result = target_guard.main(["--package", "pkg", "--target", "ignored"])

        self.assertEqual(1, result)
        self.assertIn("no tests passed", stderr.getvalue())

    @mock.patch.object(target_guard.subprocess, "run")
    def test_list_or_compile_failure_preserves_cargo_exit_code(self, run: mock.Mock) -> None:
        run.return_value = completed(101, stderr="could not compile")

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = target_guard.main(["--package", "pkg", "--target", "broken"])
        self.assertEqual(101, result)
        self.assertEqual(1, run.call_count)

    @mock.patch.object(target_guard.subprocess, "run")
    def test_test_failure_preserves_cargo_exit_code(self, run: mock.Mock) -> None:
        run.side_effect = [
            completed(0, "fails: test\n"),
            completed(
                101,
                "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out\n",
            ),
        ]

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = target_guard.main(["--package", "pkg", "--target", "fails"])
        self.assertEqual(101, result)

    @mock.patch.object(target_guard.subprocess, "run")
    def test_missing_test_summary_fails_closed(self, run: mock.Mock) -> None:
        run.side_effect = [completed(0, "listed: test\n"), completed(0, "unexpected harness output\n")]

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as stderr:
            result = target_guard.main(["--package", "pkg", "--target", "unexpected"])

        self.assertEqual(1, result)
        self.assertIn("test result summary was not found", stderr.getvalue())

    @mock.patch.object(target_guard.subprocess, "run")
    def test_additional_harness_selection_is_rejected(self, run: mock.Mock) -> None:
        for selector in ("--lib", "--all", "-pother"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                target_guard.main(
                    [
                        "--package",
                        "pkg",
                        "--target",
                        "target",
                        f"--cargo-arg={selector}",
                    ]
                )
            self.assertEqual(2, raised.exception.code)
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()

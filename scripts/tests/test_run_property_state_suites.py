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

from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

from scripts import run_property_state_suites as runner


class PropertyRunnerTests(unittest.TestCase):
    def test_summary_distinguishes_executed_ignored_and_failed_tests(self):
        for output, expected in [
            ("test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out", 0),
            ("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out", 1),
            ("test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out", 0),
        ]:
            with self.subTest(output=output):
                self.assertEqual(expected, runner.successful_test_count(output))

    def test_execution_propagates_failures_and_rejects_empty_success(self):
        suite = {"id": "sample", "command": ["cargo", "test", "sample", "--", "--exact"], "expected_tests": 1}
        for status, output in [(7, ""), (0, "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out")]:
            with self.subTest(status=status), patch.object(
                runner.subprocess, "run", return_value=subprocess.CompletedProcess([], status, output)
            ):
                with self.assertRaises(RuntimeError):
                    runner.execute_suite(Path.cwd(), suite)

    def test_execution_accepts_the_requested_test(self):
        suite = {"id": "sample", "command": ["cargo", "test", "sample", "--", "--exact"], "expected_tests": 1}
        output = "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out"
        with patch.object(runner.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, output)) as run:
            runner.execute_suite(Path.cwd(), suite)
        self.assertEqual(suite["command"], run.call_args.args[0])
        self.assertNotIn("shell", run.call_args.kwargs)


if __name__ == "__main__":
    unittest.main()

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

import datetime
import io
from pathlib import Path
import sys
import textwrap
import unittest
from unittest.mock import Mock, patch


RATE_LIMIT = (
    "\x1b[1m\x1b[91merror\x1b[0m: failed to publish rocketmq-proxy-core v1.0.0\n"
    "\nCaused by:\n"
    "  the remote server responded with an error (status 429 Too Many Requests): "
    "You have published too many new crates in a short period of time. "
    "Please try again after Thu, 01 Oct 2026 01:11:12 GMT and see "
    "https://crates.io/docs/rate-limits for more details.\n"
    "CRATE_RELEASE_FAILED: Command returned non-zero exit status 101.\n"
)
NOW = datetime.datetime(2026, 10, 1, 1, 10, 27, tzinfo=datetime.timezone.utc).timestamp()


class CratePublicationRetryTests(unittest.TestCase):
    def setUp(self):
        workflow = (Path(__file__).resolve().parents[2] / ".github/workflows/release.yml").read_text()
        step = workflow.index("- name: Publish missing versions in dependency order")
        start = workflow.index("          import collections\n", step)
        end = workflow.index("\n          PY", start)
        self.wrapper = {"__name__": "workflow_test"}
        exec(compile(textwrap.dedent(workflow[start:end]), "release.yml", "exec"), self.wrapper)
        self.wrapper["time"] = Mock(time=Mock(return_value=NOW))
        self.command = ["python", "scripts/publish_core_crates.py", "--tag", "v1.0.0",
                        "--source-commit", "a" * 40, "--mode", "publish",
                        "--output", "target/release/crates-published.json"]

    def test_partial_publication_resumes_after_advertised_cooldown(self):
        run = Mock(side_effect=[(1, RATE_LIMIT), (0, "CRATE_RELEASE_OK\n")])
        self.wrapper["run_command"] = run
        with patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(0, self.wrapper["publish_with_retries"](self.command))
        self.assertEqual([self.command, self.command], [call.args[0] for call in run.call_args_list])
        self.wrapper["time"].sleep.assert_called_once_with(47)

    def test_publication_rate_limit_without_a_date_uses_bounded_backoff(self):
        no_date = RATE_LIMIT.replace(
            "Please try again after Thu, 01 Oct 2026 01:11:12 GMT and see ", "Please try again later. "
        )
        self.assertEqual(60, self.wrapper["retry_delay"](no_date, NOW))
        self.assertEqual(2, self.wrapper["retry_delay"](RATE_LIMIT, NOW + 100))

    def test_non_rate_limit_errors_fail_immediately(self):
        for error in (
            "error: failed to publish crate\nCaused by:\n  HTTP 403 Forbidden\n",
            "CRATE_RELEASE_FAILED: existing archive has a different source\n",
            "warning: HTTP 429 Too Many Requests\nerror: compilation failed\n",
            RATE_LIMIT + "error: authentication failed\n",
        ):
            with self.subTest(error=error):
                run = Mock(return_value=(101, error))
                self.wrapper["run_command"] = run
                self.assertEqual(101, self.wrapper["publish_with_retries"](self.command))
                run.assert_called_once()
        self.wrapper["time"].sleep.assert_not_called()

    def test_repeated_rate_limits_preserve_failure_after_six_attempts(self):
        run = Mock(return_value=(1, RATE_LIMIT))
        self.wrapper["run_command"] = run
        with patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(1, self.wrapper["publish_with_retries"](self.command))
        self.assertEqual(6, run.call_count)
        self.assertEqual(5, self.wrapper["time"].sleep.call_count)

    def test_long_cooldown_requires_a_later_rerun(self):
        run = Mock(return_value=(1, RATE_LIMIT.replace("01:11:12 GMT", "02:11:12 GMT")))
        self.wrapper["run_command"] = run
        with patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(1, self.wrapper["publish_with_retries"](self.command))
        run.assert_called_once()
        self.wrapper["time"].sleep.assert_not_called()

    def test_command_streams_both_outputs_and_preserves_failure_status(self):
        command = [sys.executable, "-c",
                   "import sys; print('upload started'); print('error: HTTP 403', file=sys.stderr); sys.exit(101)"]
        with patch("sys.stdout", new_callable=io.StringIO) as streamed:
            status, tail = self.wrapper["run_command"](command)
        self.assertEqual(101, status)
        self.assertIn("upload started", streamed.getvalue())
        self.assertIn("error: HTTP 403", streamed.getvalue())
        self.assertEqual(streamed.getvalue(), tail)


if __name__ == "__main__":
    unittest.main()

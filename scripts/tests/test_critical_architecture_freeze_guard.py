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

import json
from pathlib import Path
import tempfile
import unittest

from scripts import critical_architecture_freeze_guard as guard


class CriticalArchitectureFreezeGuardTests(unittest.TestCase):
    def test_growth_reports_rule_and_repository_relative_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "crate/src/processor.rs"
            source.parent.mkdir(parents=True)
            source.write_text(
                "fn decode(command: &Command) {\n"
                "    command.decode_command_custom_header::<Header>().unwrap();\n"
                "}\n",
                encoding="utf-8",
            )
            policy = policy_for("crate", maximum=0)

            findings = guard.evaluate(root, policy)

            self.assertEqual(1, len(findings))
            self.assertEqual("remote_header_decode_unwrap", findings[0].rule)
            self.assertEqual("crate/src/processor.rs", findings[0].path)
            self.assertIn("maximum=0", findings[0].render())

    def test_declining_counts_and_exact_ceiling_pass(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "crate/src/runtime.rs"
            source.parent.mkdir(parents=True)
            source.write_text(
                "fn spawn(context: &Context) {\n"
                "    context.task_group().child(task_name);\n"
                "}\n",
                encoding="utf-8",
            )

            self.assertEqual([], guard.evaluate(root, policy_for("crate/src/runtime.rs", maximum=1)))
            self.assertEqual([], guard.evaluate(root, policy_for("crate/src/runtime.rs", maximum=2)))

    def test_tests_benches_and_cfg_test_tail_are_excluded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            production = root / "crate/src/runtime.rs"
            production.parent.mkdir(parents=True)
            production.write_text(
                "fn production() {}\n"
                "#[cfg(test)]\n"
                "mod tests {\n"
                "    fn probe(context: &Context) { context.task_group().child(task_name); }\n"
                "}\n",
                encoding="utf-8",
            )
            test_source = root / "crate/tests/runtime.rs"
            test_source.parent.mkdir(parents=True)
            test_source.write_text(
                "fn probe(context: &Context) { context.task_group().child(task_name); }\n",
                encoding="utf-8",
            )
            bench_source = root / "crate/benches/runtime.rs"
            bench_source.parent.mkdir(parents=True)
            bench_source.write_text(
                "fn probe(context: &Context) { context.task_group().child(task_name); }\n",
                encoding="utf-8",
            )
            policy = {
                "schema_version": 1,
                "rules": {
                    "hot_path_task_group_child": {
                        "maximum": 0,
                        "matchers": [
                            {
                                "path": "crate",
                                "pattern": r"task_group\(\)\s*\.\s*child\s*\(",
                            }
                        ],
                    }
                },
            }

            self.assertEqual([], guard.evaluate(root, policy))

    def test_live_repository_policy_passes(self) -> None:
        policy = json.loads(guard.POLICY_PATH.read_text(encoding="utf-8"))
        self.assertEqual([], guard.evaluate(guard.ROOT, policy))


def policy_for(path: str, maximum: int) -> dict[str, object]:
    return {
        "schema_version": 1,
        "rules": {
            "remote_header_decode_unwrap": {
                "maximum": maximum,
                "matchers": [
                    {
                        "path": path,
                        "pattern": (
                            r"decode_command_custom_header(?:_fast)?"
                            r"\s*::\s*<[^>]+>\s*\(\s*\)\s*\.unwrap\s*\(\s*\)"
                        ),
                    }
                ],
            }
        },
    }


if __name__ == "__main__":
    unittest.main()

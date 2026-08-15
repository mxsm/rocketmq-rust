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

import sys
import tempfile
import unittest
from pathlib import Path

from scripts import core_release_static_guard as guard


class CoreReleaseStaticGuardTests(unittest.TestCase):
    def test_required_routes_use_core_structural_and_semantic_modes(self) -> None:
        routes = guard.required_routes()
        commands = {route.route_id: route.argv for route in routes}

        self.assertEqual(
            {
                "public-api-intent",
                "telemetry-semantic",
                "rust-hygiene",
                "rust-lint-debt",
                "architecture-dependency",
                "architecture-documentation",
                "architecture-debt",
                "module-maintainability",
                "stable-surface",
                "architecture-release",
            },
            set(commands),
        )
        for command in commands.values():
            self.assertIn("--scope", command)
            self.assertIn("core-release", command)
        self.assertEqual("structural", commands["architecture-dependency"][commands["architecture-dependency"].index("--mode") + 1])
        self.assertEqual("semantic", commands["architecture-documentation"][commands["architecture-documentation"].index("--mode") + 1])
        self.assertEqual("structural", commands["architecture-release"][commands["architecture-release"].index("--mode") + 1])
        serialized = " ".join(argument for command in commands.values() for argument in command).lower()
        for forbidden in ("sha256", "fingerprint", "--mode baseline", "--mode transition", "--mode target"):
            self.assertNotIn(forbidden, serialized)

    def test_run_routes_records_every_result_and_returns_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            routes = (
                guard.StaticRoute("pass-one", (sys.executable, "-c", "print('first')")),
                guard.StaticRoute("fail", (sys.executable, "-c", "raise SystemExit(7)")),
                guard.StaticRoute("pass-two", (sys.executable, "-c", "print('last')")),
            )

            status, results = guard.run_routes(Path(directory), routes)

        self.assertEqual(1, status)
        self.assertEqual(["pass-one", "fail", "pass-two"], [result.route_id for result in results])
        self.assertEqual([0, 7, 0], [result.exit_code for result in results])


if __name__ == "__main__":
    unittest.main()

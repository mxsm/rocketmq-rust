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
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
GUARD = ROOT / "scripts" / "admin_operation_guard.py"
MATRIX = ROOT / "scripts" / "admin-operation-matrix.json"
JAVA_INVENTORY = ROOT / "scripts" / "fixtures" / "java-5.5-core-inventory.json"
OPERATION_MAP = ROOT / "rocketmq-doc" / "en" / "admin" / "java-55-operation-map.md"
CAPABILITY_MANIFEST = ROOT / "scripts" / "v1-capability-manifest.json"
FUNCTIONAL_MATRIX = ROOT / "scripts" / "v1-functional-test-matrix.json"
GOLDENS = ROOT / "scripts" / "fixtures" / "admin-java-55" / "operation-goldens.json"


def run_guard(matrix: Path = MATRIX, *extra_args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(GUARD),
            "--matrix",
            str(matrix),
            "--java-inventory",
            str(JAVA_INVENTORY),
            "--goldens",
            str(GOLDENS),
            *extra_args,
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


class AdminOperationGuardTest(unittest.TestCase):
    def write_matrix(self, matrix: dict[str, object], directory: str) -> Path:
        path = Path(directory) / "admin-operation-matrix.json"
        path.write_text(json.dumps(matrix), encoding="utf-8")
        return path

    def write_goldens(self, goldens: dict[str, object], directory: str) -> Path:
        path = Path(directory) / "operation-goldens.json"
        path.write_text(json.dumps(goldens), encoding="utf-8")
        return path

    def test_repository_matrix_closes_the_raw_excluded_active_denominator(self) -> None:
        result = run_guard()

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("ADMIN_OPERATION_GUARD_OK raw=96 excluded=2 active=94", result.stdout)

        matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
        self.assertEqual(matrix["counts"], {"raw": 96, "excluded": 2, "active": 94})
        self.assertEqual(len(matrix["operations"]), 96)
        self.assertEqual(
            {
                operation["java_symbol"]
                for operation in matrix["operations"]
                if operation["classification"] == "excluded"
            },
            {"AddBrokerSubCommand", "RemoveBrokerSubCommand"},
        )

        self.assertTrue(OPERATION_MAP.is_file(), "Java 5.5 Admin operation map is missing")
        operation_map = OPERATION_MAP.read_text(encoding="utf-8")
        self.assertIn("Raw operations: **96**", operation_map)
        self.assertIn("Core active operations: **94**", operation_map)
        self.assertIn("Known placeholders: **0**", operation_map)
        self.assertIn("## Known incomplete operations\n\n_None._", operation_map)
        self.assertIn("`AddBrokerSubCommand`", operation_map)
        self.assertIn("`RemoveBrokerSubCommand`", operation_map)

    def test_duplicate_operation_id_is_rejected(self) -> None:
        matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
        matrix["operations"][1]["operation_id"] = matrix["operations"][0]["operation_id"]
        with tempfile.TemporaryDirectory() as directory:
            result = run_guard(self.write_matrix(matrix, directory))

        self.assertEqual(result.returncode, 1)
        self.assertIn("code=operation-id-duplicate", result.stdout)

    def test_missing_handler_owner_is_rejected(self) -> None:
        matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
        active = next(operation for operation in matrix["operations"] if operation["classification"] == "active")
        active["handler_owners"] = []
        with tempfile.TemporaryDirectory() as directory:
            result = run_guard(self.write_matrix(matrix, directory))

        self.assertEqual(result.returncode, 1)
        self.assertIn("code=active-field-missing", result.stdout)

    def test_strict_mode_accepts_the_completed_active_denominator(self) -> None:
        result = run_guard(MATRIX, "--require-complete")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("code=active-operation-incomplete", result.stdout)

    def test_repository_goldens_cover_every_active_operation_with_success_and_error(self) -> None:
        result = run_guard(MATRIX, "--require-complete")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("goldens=94 scenarios=278", result.stdout)

        fixture = json.loads(GOLDENS.read_text(encoding="utf-8"))
        self.assertEqual(fixture["counts"], {"operations": 94, "scenarios": 278})
        self.assertEqual(len(fixture["operations"]), 94)
        for operation in fixture["operations"]:
            expected_cases = {"success", "error"}
            if operation["side_effect_class"] == "read-only-query":
                expected_cases.update({"empty", "partial-failure"})
            self.assertEqual({scenario["case"] for scenario in operation["scenarios"]}, expected_cases)

    def test_missing_operation_golden_is_rejected(self) -> None:
        goldens = json.loads(GOLDENS.read_text(encoding="utf-8"))
        goldens["operations"].pop()
        with tempfile.TemporaryDirectory() as directory:
            result = run_guard(MATRIX, "--goldens", str(self.write_goldens(goldens, directory)))

        self.assertEqual(result.returncode, 1)
        self.assertIn("code=golden-operation-denominator-drift", result.stdout)

    def test_wrong_golden_exit_code_is_rejected(self) -> None:
        goldens = json.loads(GOLDENS.read_text(encoding="utf-8"))
        error = next(
            scenario
            for operation in goldens["operations"]
            for scenario in operation["scenarios"]
            if scenario["outcome"] == "error"
        )
        error["expected_exit_code"] = 0
        with tempfile.TemporaryDirectory() as directory:
            result = run_guard(MATRIX, "--goldens", str(self.write_goldens(goldens, directory)))

        self.assertEqual(result.returncode, 1)
        self.assertIn("code=golden-scenario-drift", result.stdout)

    def test_duplicate_golden_scenario_id_is_rejected(self) -> None:
        goldens = json.loads(GOLDENS.read_text(encoding="utf-8"))
        goldens["operations"][1]["scenarios"][0]["scenario_id"] = goldens["operations"][0]["scenarios"][0][
            "scenario_id"
        ]
        with tempfile.TemporaryDirectory() as directory:
            result = run_guard(MATRIX, "--goldens", str(self.write_goldens(goldens, directory)))

        self.assertEqual(result.returncode, 1)
        self.assertIn("code=golden-scenario-id-duplicate", result.stdout)

    def test_g05_capability_route_runs_the_structural_guard(self) -> None:
        manifest = json.loads(CAPABILITY_MANIFEST.read_text(encoding="utf-8"))
        capability = next(item for item in manifest["capabilities"] if item["capability_id"] == "G-05")
        self.assertEqual(
            capability["test_ids"],
            [
                "G05-ADMIN-OPERATION-GOLDEN-CORE",
                "G05-ADMIN-OPERATION-EXIT-CODES",
                "G05-ADMIN-OPERATION-GUARD",
            ],
        )
        self.assertEqual(
            capability["commands"],
            [
                "cargo test -p rocketmq-admin-core --test java_operation_golden",
                "cargo test -p rocketmq-admin-cli --test operation_exit_codes",
                "python scripts/admin_operation_guard.py --require-complete",
            ],
        )
        self.assertIn("scripts/admin-operation-matrix.json", capability["rust_surfaces"])
        self.assertIn("scripts/fixtures/admin-java-55/operation-goldens.json", capability["rust_surfaces"])
        self.assertIn("rocketmq-doc/en/admin/java-55-operation-map.md", capability["rust_surfaces"])
        self.assertEqual(capability["artifacts"], [])

        routes = json.loads(FUNCTIONAL_MATRIX.read_text(encoding="utf-8"))
        g05_routes = [item for item in routes["capability_routes"] if item["capability_id"] == "G-05"]
        self.assertEqual(
            [(route["test_id"], route["argv"]) for route in g05_routes],
            [
                (
                    "G05-ADMIN-OPERATION-GOLDEN-CORE",
                    ["cargo", "test", "-p", "rocketmq-admin-core", "--test", "java_operation_golden"],
                ),
                (
                    "G05-ADMIN-OPERATION-EXIT-CODES",
                    ["cargo", "test", "-p", "rocketmq-admin-cli", "--test", "operation_exit_codes"],
                ),
                (
                    "G05-ADMIN-OPERATION-GUARD",
                    ["python", "scripts/admin_operation_guard.py", "--require-complete"],
                ),
            ],
        )

if __name__ == "__main__":
    unittest.main()

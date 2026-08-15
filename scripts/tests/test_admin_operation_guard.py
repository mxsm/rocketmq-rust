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


def run_guard(matrix: Path = MATRIX, *extra_args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(GUARD),
            "--matrix",
            str(matrix),
            "--java-inventory",
            str(JAVA_INVENTORY),
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
        self.assertIn("Known placeholders: **5**", operation_map)
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

    def test_strict_mode_reports_known_placeholders(self) -> None:
        result = run_guard(MATRIX, "--require-complete")

        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout.count("code=active-operation-incomplete"), 5)

    def test_g05_capability_route_runs_the_structural_guard(self) -> None:
        manifest = json.loads(CAPABILITY_MANIFEST.read_text(encoding="utf-8"))
        capability = next(item for item in manifest["capabilities"] if item["capability_id"] == "G-05")
        self.assertEqual(capability["commands"], ["python scripts/admin_operation_guard.py"])
        self.assertIn("scripts/admin-operation-matrix.json", capability["rust_surfaces"])
        self.assertIn("rocketmq-doc/en/admin/java-55-operation-map.md", capability["rust_surfaces"])
        self.assertEqual(capability["artifacts"], [])

        routes = json.loads(FUNCTIONAL_MATRIX.read_text(encoding="utf-8"))
        route = next(item for item in routes["capability_routes"] if item["capability_id"] == "G-05")
        self.assertEqual(route["argv"], ["python", "scripts/admin_operation_guard.py"])

if __name__ == "__main__":
    unittest.main()

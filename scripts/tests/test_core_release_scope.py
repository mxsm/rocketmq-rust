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

import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
SCOPE_PATH = SCRIPTS / "core-release-scope.json"


class CoreReleaseScopeTests(unittest.TestCase):
    def load_live_scope(self) -> dict[str, object]:
        self.assertTrue(SCOPE_PATH.is_file(), "core release scope definition is missing")
        return json.loads(SCOPE_PATH.read_text(encoding="utf-8"))

    def metadata_for(self, scope: dict[str, object], *, extra_packages: tuple[str, ...] = ()) -> dict[str, object]:
        packages: list[dict[str, str]] = []
        members: list[str] = []
        entries = list(scope["core_packages"]) + list(scope["workspace_exclusions"])
        for entry in entries:
            name = entry["name"]
            package_id = f"{name} 1.0.0 (path+file://fixture/{name})"
            packages.append(
                {
                    "id": package_id,
                    "name": name,
                    "manifest_path": str(ROOT / entry["path"] / "Cargo.toml"),
                }
            )
            members.append(package_id)
        for name in extra_packages:
            package_id = f"{name} 1.0.0 (path+file://fixture/{name})"
            packages.append(
                {
                    "id": package_id,
                    "name": name,
                    "manifest_path": str(ROOT / name / "Cargo.toml"),
                }
            )
            members.append(package_id)
        return {"packages": packages, "workspace_members": members, "workspace_root": str(ROOT)}

    def validate_fixture(
        self,
        scope: dict[str, object],
        metadata: dict[str, object],
    ) -> list[dict[str, str]]:
        with tempfile.TemporaryDirectory() as temp_dir:
            scope_path = Path(temp_dir) / "scope.json"
            metadata_path = Path(temp_dir) / "metadata.json"
            scope_path.write_text(json.dumps(scope), encoding="utf-8")
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            code = """
import json
from pathlib import Path
import core_release_scope as scope_module

scope = scope_module.load_scope(Path(sys.argv[1]))
metadata = json.loads(Path(sys.argv[2]).read_text(encoding='utf-8'))
findings = scope_module.validate_metadata(scope, metadata, root=Path(sys.argv[3]))
print(json.dumps([finding.as_dict() for finding in findings]))
"""
            completed = subprocess.run(
                [sys.executable, "-c", "import sys\n" + code, str(scope_path), str(metadata_path), str(ROOT)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                env={**os.environ, "PYTHONPATH": str(SCRIPTS)},
            )
        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        return json.loads(completed.stdout)

    def test_live_guard_matches_the_27_package_workspace_scope(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(SCRIPTS / "core_release_guard.py")],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        self.assertIn("CORE_RELEASE_SCOPE_OK packages=27 workspace_exclusions=1", completed.stdout)

    def test_public_helpers_expose_core_and_excluded_projects(self) -> None:
        code = """
import core_release_scope as scope
loaded = scope.load_scope()
assert len(scope.core_packages(loaded)) == 27
assert {item['name'] for item in scope.excluded_projects(loaded)} >= {
    'rocketmq-dashboard-common', 'rocketmq-mcp', 'rocketmq-sre'
}
"""
        completed = subprocess.run(
            [sys.executable, "-c", code],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, "PYTHONPATH": str(SCRIPTS)},
        )

        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)

    def test_unclassified_workspace_package_is_a_core_finding(self) -> None:
        scope = self.load_live_scope()
        findings = self.validate_fixture(scope, self.metadata_for(scope, extra_packages=("rocketmq-new",)))

        self.assertIn("workspace-package-unclassified", {item["code"] for item in findings})

    def test_duplicate_missing_path_and_invalid_classification_are_findings(self) -> None:
        scope = self.load_live_scope()
        duplicate = copy.deepcopy(scope["core_packages"][0])
        scope["core_packages"].append(duplicate)
        scope["core_packages"][1]["path"] = "rocketmq-does-not-exist"
        scope["core_packages"][2]["classification"] = "implicit"

        findings = self.validate_fixture(scope, self.metadata_for(self.load_live_scope()))
        codes = {item["code"] for item in findings}
        self.assertTrue(
            {"scope-package-duplicate", "scope-path-missing", "scope-classification-invalid"}.issubset(codes),
            findings,
        )

    def test_malformed_classification_inventory_is_a_structured_finding(self) -> None:
        scope = self.load_live_scope()
        scope["allowed_classifications"] = [{}, "internal-only", "binary-only", "non-publish"]

        findings = self.validate_fixture(scope, self.metadata_for(self.load_live_scope()))
        self.assertIn("scope-classifications-invalid", {item["code"] for item in findings})

    def test_dashboard_mcp_and_sre_cannot_be_core_packages(self) -> None:
        for forbidden_name, forbidden_path in (
            ("rocketmq-dashboard-common", "rocketmq-dashboard/rocketmq-dashboard-common"),
            ("rocketmq-mcp", "rocketmq-tools/rocketmq-mcp"),
            ("rocketmq-sre", "rocketmq-sre"),
        ):
            with self.subTest(package=forbidden_name):
                scope = self.load_live_scope()
                scope["core_packages"][0] = {
                    "name": forbidden_name,
                    "path": forbidden_path,
                    "classification": "registry-publish",
                }
                findings = self.validate_fixture(scope, self.metadata_for(self.load_live_scope()))
                self.assertIn("excluded-project-in-core", {item["code"] for item in findings})

    def test_repository_global_finding_does_not_change_core_result(self) -> None:
        scope = self.load_live_scope()
        scope["repository_exclusions"][0]["path"] = "missing-standalone-project"

        findings = self.validate_fixture(scope, self.metadata_for(self.load_live_scope()))
        self.assertEqual([], [item for item in findings if item["scope"] == "core"])
        self.assertIn("scope-path-missing", {item["code"] for item in findings if item["scope"] == "repo-global"})

    def test_architecture_guard_has_an_independent_core_mode(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(SCRIPTS / "architecture_release_guard.py"), "--scope", "core-release"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        self.assertIn("ARCHITECTURE_RELEASE_CORE_OK packages=27", completed.stdout)
        self.assertNotIn("ARCHITECTURE_RELEASE_REPO_GLOBAL_FAILED", completed.stdout)


if __name__ == "__main__":
    unittest.main()

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

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str):
    path = ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def create_fixture(root: Path) -> None:
    scope = {
        "schema_version": 1,
        "core_packages": [
            {"name": "rocketmq-admin-core", "path": "admin-core", "classification": "registry-publish"},
            {"name": "rocketmq-admin-cli", "path": "admin-cli", "classification": "binary-only"},
            {"name": "rocketmq-protocol", "path": "protocol", "classification": "registry-publish"},
        ],
        "workspace_exclusions": [
            {"name": "rocketmq-dashboard-common", "path": "dashboard", "classification": "excluded-dashboard"}
        ],
    }
    write(root / "scripts/core-release-scope.json", json.dumps(scope))
    write(
        root / "Cargo.toml",
        '[workspace]\nmembers = ["admin-core", "admin-cli", "protocol", "dashboard"]\n\n'
        '[workspace.package]\nversion = "1.0.0-dev"\n\n'
        '[workspace.dependencies]\nrocketmq-admin-core = { version = "1.0.0-dev", path = "admin-core" }\n'
        'rocketmq-protocol = { version = "1.0.0-dev", path = "protocol" }\n'
        'rocketmq-dashboard-common = { version = "1.0.0-dev", path = "dashboard" }\n',
    )
    write(root / "admin-core/Cargo.toml", '[package]\nname = "rocketmq-admin-core"\nversion.workspace = true\n')
    write(
        root / "admin-cli/Cargo.toml",
        '[package]\nname = "rocketmq-admin-cli"\nversion.workspace = true\n\n[dependencies]\n'
        'rocketmq-admin-core = { version = "1.0.0-dev", path = "../admin-core" }\n',
    )
    write(root / "protocol/Cargo.toml", '[package]\nname = "rocketmq-protocol"\nversion.workspace = true\n')
    write(root / "dashboard/Cargo.toml", '[package]\nname = "rocketmq-dashboard-common"\nversion.workspace = true\n')
    write(
        root / "rocketmq-example/Cargo.toml",
        '[package]\nname = "example"\nversion = "0.1.0"\n\n[dependencies]\n'
        'rocketmq-protocol = { version = "1.0.0-dev", path = "../protocol" }\n',
    )
    package_blocks = (
        '[[package]]\nname = "rocketmq-admin-core"\nversion = "1.0.0-dev"\n\n'
        '[[package]]\nname = "rocketmq-protocol"\nversion = "1.0.0-dev"\n\n'
        '[[package]]\nname = "rocketmq-dashboard-common"\nversion = "1.0.0-dev"\n'
    )
    for relative in (
        "Cargo.lock",
        "rocketmq-example/Cargo.lock",
        "fuzz/Cargo.lock",
        "rocketmq-macros/tests/fixtures/renamed-consumer/Cargo.lock",
    ):
        write(root / relative, "version = 4\n\n" + package_blocks)


class ReleaseVersionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.setter = load_module("set_workspace_version")
        cls.checker = load_module("check_release_version")

    def test_three_release_transitions_update_core_manifests_and_locks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            create_fixture(root)
            for version in ("1.0.0-rc.1", "1.0.0-rc.2", "1.0.0"):
                result = self.setter.apply_version(root, version)
                self.assertGreaterEqual(result.changed_files, 6)
                self.assertEqual([], self.checker.check_version(root, version))

            for relative in (
                "Cargo.toml",
                "admin-cli/Cargo.toml",
                "rocketmq-example/Cargo.toml",
                "Cargo.lock",
                "rocketmq-example/Cargo.lock",
                "fuzz/Cargo.lock",
                "rocketmq-macros/tests/fixtures/renamed-consumer/Cargo.lock",
            ):
                self.assertIn('1.0.0', (root / relative).read_text(encoding="utf-8"))
                self.assertNotIn('1.0.0-rc.2', (root / relative).read_text(encoding="utf-8"))

    def test_excluded_manifest_is_untouched_while_inherited_root_lock_version_stays_valid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            create_fixture(root)
            dashboard_before = (root / "dashboard/Cargo.toml").read_bytes()
            self.setter.apply_version(root, "1.0.0-rc.1")

            self.assertEqual(dashboard_before, (root / "dashboard/Cargo.toml").read_bytes())
            root_lock = (root / "Cargo.lock").read_text(encoding="utf-8")
            dashboard = root_lock.split('name = "rocketmq-dashboard-common"', 1)[1]
            self.assertIn('version = "1.0.0-rc.1"', dashboard)

    def test_invalid_version_and_rc_jump_are_rejected_before_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            create_fixture(root)
            before = (root / "Cargo.toml").read_bytes()

            with self.assertRaises(self.setter.VersionError):
                self.setter.apply_version(root, "v1.0.0")
            with self.assertRaises(self.setter.VersionError):
                self.setter.apply_version(root, "1.0.0-rc.2")
            self.assertEqual(before, (root / "Cargo.toml").read_bytes())

    def test_malformed_lock_aborts_the_entire_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            create_fixture(root)
            write(root / "fuzz/Cargo.lock", "not a cargo lock")
            before = {path: path.read_bytes() for path in root.rglob("Cargo.toml")}

            with self.assertRaises(self.setter.VersionError):
                self.setter.apply_version(root, "1.0.0-rc.1")
            self.assertEqual(before, {path: path.read_bytes() for path in root.rglob("Cargo.toml")})

    def test_checker_reports_manifest_and_lock_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            create_fixture(root)
            self.setter.apply_version(root, "1.0.0-rc.1")
            write(root / "admin-cli/Cargo.toml", (root / "admin-cli/Cargo.toml").read_text().replace("1.0.0-rc.1", "1.0.0"))

            findings = self.checker.check_version(root, "1.0.0-rc.1")
            self.assertTrue(any(finding.code == "manifest-version-drift" for finding in findings), findings)

    def test_checker_reports_chart_and_oci_version_drift_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            create_fixture(root)
            self.setter.apply_version(root, "1.0.0-rc.1")
            write(root / "distribution/helm/rocketmq-rust-core/Chart.yaml", 'version: 1.0.0\nappVersion: "1.0.0"\n')
            write(root / "docker/core-container-policy.json", '{"release_version":"1.0.0"}\n')

            findings = self.checker.check_version(root, "1.0.0-rc.1")
            codes = {finding.code for finding in findings}
            self.assertIn("chart-version-drift", codes)
            self.assertIn("oci-version-drift", codes)

    def test_fixture_validation_does_not_modify_the_repository(self) -> None:
        before = (ROOT / "Cargo.toml").read_bytes()
        self.assertEqual(0, self.checker.run_fixture_validation())
        self.assertEqual(before, (ROOT / "Cargo.toml").read_bytes())


if __name__ == "__main__":
    unittest.main()

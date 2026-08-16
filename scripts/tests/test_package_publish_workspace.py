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
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import textwrap
import unittest
import tempfile

from scripts.tests.release_test_support import ROOT, load_module, read_json, write_json


PLANNER = ROOT / "distribution" / "package_publish_workspace.py"
STAGER = ROOT / "distribution" / "stage_publishable_crate.py"
LEGAL_POLICY = ROOT / "distribution" / "legal-policy.json"
PACKAGE_POLICY = ROOT / "distribution" / "release-package-policy.json"
SHELL_WRAPPER = ROOT / "distribution" / "package_publish_workspace.sh"
WINDOWS_WRAPPER = ROOT / "distribution" / "package_publish_workspace.bat"


class PackagePublishWorkspaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.planner = load_module(
            "package_publish_workspace", "distribution/package_publish_workspace.py"
        )
        cls.stager = load_module(
            "stage_publishable_crate", "distribution/stage_publishable_crate.py"
        )

    def candidate(self, root: Path) -> Path:
        candidate_root = root / "1.0.0" / "local" / "attempt-1"
        manifest = candidate_root / "CANDIDATE_RUN.json"
        write_json(
            manifest,
            {
                "schema_version": 1,
                "candidate_id": "1.0.0-runlocal-attempt1-ordinal1",
                "candidate_kind": "final",
                "version": "1.0.0",
                "run_id": "local",
                "attempt": 1,
                "ordinal": 1,
                "candidate_root": str(candidate_root.resolve()),
                "series_manifest": str((root / "RELEASE_SERIES.json").resolve()),
                "series_id": "community-v1",
                "series_generation": 1,
                "parent_manifest": None,
                "state": "development",
                "sealed": False,
                "outcome": None,
                "rejection_reason": None,
                "known_issues": [],
                "generation": 0,
                "build_source_bundle": None,
                "source_snapshot": None,
                "artifact_index": None,
                "evidence_index": None,
                "event_index": None,
                "execution_context_index": None,
                "creation_operation_id": "fixture",
                "created_at": "2026-08-16T00:00:00+00:00",
                "updated_at": "2026-08-16T00:00:00+00:00",
            },
        )
        return manifest

    def tiny_workspace(self, root: Path) -> tuple[dict[str, object], list[dict[str, object]]]:
        (root / "LICENSE-APACHE").write_text("Apache License fixture\n", encoding="utf-8")
        (root / "NOTICE").write_text("Fixture notice\n", encoding="utf-8")
        (root / "Cargo.toml").write_text(
            textwrap.dedent(
                """
                [workspace]
                members = ["tiny-dep", "tiny-top"]
                resolver = "2"
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        for name in ("tiny-dep", "tiny-top"):
            package = root / name
            (package / "src").mkdir(parents=True)
            dependencies = (
                '\n[dependencies]\ntiny-dep = { version = "1.0.0", path = "../tiny-dep" }\n'
                if name == "tiny-top"
                else ""
            )
            (package / "Cargo.toml").write_text(
                textwrap.dedent(
                    f"""
                    [package]
                    name = "{name}"
                    version = "1.0.0"
                    edition = "2021"
                    license = "Apache-2.0"
                    description = "local registry fixture"
                    repository = "https://example.invalid/community"
                    homepage = "https://example.invalid/community"
                    {dependencies}
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            (package / "src" / "lib.rs").write_text(
                "pub fn ready() -> bool { true }\n", encoding="utf-8"
            )
        generated = subprocess.run(
            ["cargo", "generate-lockfile", "--offline"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, generated.returncode, generated.stdout + generated.stderr)
        completed = subprocess.run(
            ["cargo", "metadata", "--locked", "--offline", "--format-version", "1", "--no-deps"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        metadata = json.loads(completed.stdout)
        by_name = {package["name"]: package for package in metadata["packages"]}
        legal_policy = {
            "license_source": "LICENSE-APACHE",
            "notice_source": "NOTICE",
            "license_archive_name": "LICENSE-APACHE",
            "notice_archive_name": "NOTICE",
            "allowed_licenses": ["Apache-2.0"],
            "required_metadata_fields": ["description", "homepage", "repository"],
        }
        candidate_root = root / "candidate"
        staged = self.stager.stage_workspace_crates(
            root,
            candidate_root,
            packages=[
                {
                    "name": name,
                    "version": "1.0.0",
                    "manifest": str(Path(by_name[name]["manifest_path"]).relative_to(root)),
                }
                for name in ("tiny-dep", "tiny-top")
            ],
            legal_policy=legal_policy,
        )
        return metadata, staged

    def test_planner_exposes_local_only_modes(self) -> None:
        self.assertTrue(PLANNER.is_file(), f"missing package planner: {PLANNER}")
        completed = subprocess.run(
            [sys.executable, str(PLANNER), "--help"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        self.assertIn("--plan-only", completed.stdout)
        self.assertIn("--package-only", completed.stdout)
        self.assertNotIn("cargo publish", completed.stdout.lower())

    def test_publishable_crate_stager_is_available(self) -> None:
        self.assertTrue(STAGER.is_file(), f"missing crate stager: {STAGER}")

    def test_packaging_policies_freeze_metadata_driven_local_only_contract(self) -> None:
        legal = read_json(LEGAL_POLICY)
        package = read_json(PACKAGE_POLICY)

        self.assertEqual(["Apache-2.0"], legal["allowed_licenses"])
        self.assertEqual(24, package["expected_registry_publish_count"])
        self.assertEqual(3, package["expected_binary_only_count"])
        self.assertEqual(
            "cargo metadata --locked --format-version 1 --no-deps",
            package["inventory_source"],
        )
        self.assertEqual("not-executed", package["remote_publication"])

    def test_platform_wrappers_delegate_to_the_same_local_only_planner(self) -> None:
        shell_source = SHELL_WRAPPER.read_text(encoding="utf-8")
        windows_source = WINDOWS_WRAPPER.read_text(encoding="utf-8")
        for source in (shell_source, windows_source):
            self.assertIn("package_publish_workspace.py", source)
            self.assertNotIn("cargo publish", source.lower())
            self.assertNotIn("PROJECT_1_", source)

        expected = subprocess.run(
            [sys.executable, str(PLANNER), "--help"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, expected.returncode, expected.stdout + expected.stderr)
        windows = subprocess.run(
            ["cmd", "/c", str(WINDOWS_WRAPPER), "--help"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, windows.returncode, windows.stdout + windows.stderr)
        self.assertEqual(
            expected.stdout.replace("\r\n", "\n"),
            windows.stdout.replace("\r\n", "\n"),
        )
        bash = shutil.which("bash")
        if bash is not None and sys.platform != "win32":
            shell = subprocess.run(
                [bash, str(SHELL_WRAPPER), "--help"],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, shell.returncode, shell.stdout + shell.stderr)
            self.assertEqual(
                expected.stdout.replace("\r\n", "\n"),
                shell.stdout.replace("\r\n", "\n"),
            )

    def test_staged_crate_contains_legal_files_without_dirtying_source_package(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _metadata, staged = self.tiny_workspace(root)
            crate = Path(staged[0]["crate_path"])

            with tarfile.open(crate, "r:gz") as archive:
                names = set(archive.getnames())
            prefix = "tiny-dep-1.0.0"
            self.assertIn(f"{prefix}/LICENSE-APACHE", names)
            self.assertIn(f"{prefix}/NOTICE", names)
            self.assertFalse((root / "tiny-dep" / "LICENSE-APACHE").exists())
            self.assertFalse((root / "tiny-dep" / "NOTICE").exists())

    def test_staged_crates_resolve_and_compile_through_temporary_local_registry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            metadata, staged = self.tiny_workspace(root)
            registry = root / "local-registry"

            self.stager.create_local_registry(registry, staged, metadata)
            result = self.stager.verify_local_registry(
                registry,
                package_name="tiny-top",
                version="1.0.0",
                work_root=root / "consumer-check",
            )

        self.assertEqual("passed", result["status"])
        self.assertEqual("tiny-top", result["package"])

    def test_real_core_graph_has_24_publishable_crates_and_three_explicit_skips(self) -> None:
        metadata = self.planner.collect_metadata(ROOT)
        scope = read_json(ROOT / "scripts" / "core-release-scope.json")

        plan = self.planner.build_plan(metadata, scope, selector=None)

        self.assertEqual(24, len(plan["packages"]))
        self.assertEqual(
            {"rocketmq-admin-cli", "rocketmq-admin-tui", "rocketmq-store-inspect"},
            {entry["name"] for entry in plan["skipped_packages"]},
        )
        positions = {entry["name"]: entry["order"] for entry in plan["packages"]}
        for entry in plan["packages"]:
            for dependency in entry["internal_dependencies"]:
                self.assertLess(positions[dependency], positions[entry["name"]])

        drifted_policy = read_json(PACKAGE_POLICY)
        drifted_policy["expected_registry_publish_count"] = 25
        with self.assertRaisesRegex(self.planner.PlannerError, "count drifted"):
            self.planner._validate_package_policy(drifted_policy, plan, all_core=True)

    def test_dependency_cycle_is_rejected(self) -> None:
        metadata = {
            "workspace_members": ["a 1", "b 1"],
            "packages": [
                {
                    "id": "a 1",
                    "name": "a",
                    "version": "1.0.0",
                    "manifest_path": str((ROOT / "Cargo.toml").resolve()),
                    "dependencies": [{"name": "b"}],
                },
                {
                    "id": "b 1",
                    "name": "b",
                    "version": "1.0.0",
                    "manifest_path": str((ROOT / "Cargo.toml").resolve()),
                    "dependencies": [{"name": "a"}],
                },
            ],
        }
        scope = {
            "core_packages": [
                {"name": "a", "path": "a", "classification": "registry-publish"},
                {"name": "b", "path": "b", "classification": "registry-publish"},
            ]
        }

        with self.assertRaisesRegex(self.planner.PlannerError, "cycle"):
            self.planner.build_plan(metadata, scope, selector=None)

    def test_unclassified_workspace_package_is_rejected(self) -> None:
        metadata = {
            "workspace_members": ["a 1", "new 1"],
            "packages": [
                {
                    "id": "a 1",
                    "name": "a",
                    "version": "1.0.0",
                    "manifest_path": str((ROOT / "Cargo.toml").resolve()),
                    "dependencies": [],
                },
                {
                    "id": "new 1",
                    "name": "new",
                    "version": "1.0.0",
                    "manifest_path": str((ROOT / "Cargo.toml").resolve()),
                    "dependencies": [],
                },
            ],
        }
        scope = {
            "core_packages": [
                {"name": "a", "path": "a", "classification": "registry-publish"}
            ],
            "workspace_exclusions": [],
        }

        with self.assertRaisesRegex(self.planner.PlannerError, "not classified"):
            self.planner.build_plan(metadata, scope, selector=None)

    def test_binary_only_and_unknown_project_selectors_are_rejected(self) -> None:
        metadata = self.planner.collect_metadata(ROOT)
        scope = read_json(ROOT / "scripts" / "core-release-scope.json")

        with self.assertRaisesRegex(self.planner.PlannerError, "binary-only"):
            self.planner.build_plan(metadata, scope, selector="rocketmq-admin-cli")
        with self.assertRaisesRegex(self.planner.PlannerError, "not a core package"):
            self.planner.build_plan(metadata, scope, selector="rocketmq-dashboard-common")

    def test_plan_only_writes_candidate_scoped_report_and_events_without_crates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = self.candidate(Path(temporary))
            candidate_root = candidate.parent
            report = candidate_root / "PACKAGE_PLAN.json"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(PLANNER),
                    "--all-core",
                    "--plan-only",
                    "--candidate-manifest",
                    str(candidate),
                    "--output-report",
                    str(report),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            value = read_json(report)
            self.assertEqual("not-executed", value["remote_publication"]["status"])
            self.assertEqual(24, value["registry_publish_count"])
            self.assertFalse((candidate_root / "crate-packages").exists())
            route = "R04-plan-all-core"
            self.assertEqual(
                0,
                read_json(candidate_root / "events" / f"{route}.completed.json")[
                    "exit_code"
                ],
            )
            rendered = json.dumps(value).lower()
            self.assertNotIn("cargo publish", rendered)
            self.assertNotIn("digest", rendered)
            self.assertNotIn("sha256", rendered)

    def test_package_only_requires_local_temp_registry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = self.candidate(Path(temporary))
            completed = subprocess.run(
                [
                    sys.executable,
                    str(PLANNER),
                    "--all-core",
                    "--package-only",
                    "--candidate-manifest",
                    str(candidate),
                    "--output-report",
                    str(candidate.parent / "PACKAGE_REPORT.json"),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertNotEqual(0, completed.returncode)
        self.assertIn("--staging-registry local-temp", completed.stdout + completed.stderr)

    def test_package_only_stages_selected_crate_and_verifies_local_registry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = self.candidate(Path(temporary))
            report = candidate.parent / "PACKAGE_REPORT.json"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(PLANNER),
                    "--project",
                    "rocketmq-error",
                    "--package-only",
                    "--candidate-manifest",
                    str(candidate),
                    "--output-report",
                    str(report),
                    "--staging-registry",
                    "local-temp",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            value = read_json(report)
            self.assertEqual("package-only", value["mode"])
            self.assertEqual("passed", value["local_registry_validation"]["status"])
            self.assertEqual("not-executed", value["remote_publication"]["status"])
            self.assertEqual(
                ["rocketmq-error"],
                [entry["name"] for entry in value["staged_packages"]],
            )
            crate = candidate.parent / value["staged_packages"][0]["crate_path"]
            self.assertTrue(crate.is_file())

    def test_report_cannot_escape_candidate_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidate = self.candidate(root)
            completed = subprocess.run(
                [
                    sys.executable,
                    str(PLANNER),
                    "--all-core",
                    "--plan-only",
                    "--candidate-manifest",
                    str(candidate),
                    "--output-report",
                    str(root / "outside.json"),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertNotEqual(0, completed.returncode)
        self.assertIn("candidate root", (completed.stdout + completed.stderr).lower())


if __name__ == "__main__":
    unittest.main()

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

import subprocess
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import ci_scope


class CiScopeTests(unittest.TestCase):
    members = (
        "rocketmq-client", "rocketmq-protocol", "rocketmq-runtime", "rocketmq-error",
        "rocketmq-observability", "rocketmq-broker", "rocketmq-store",
        "rocketmq-dashboard/rocketmq-dashboard-common",
    )

    def scope(self, *paths: str) -> dict[str, bool]:
        return ci_scope.classify(list(paths), self.members)

    def test_documentation_does_not_compile_rust(self) -> None:
        self.assertFalse(any(self.scope("README.md", "rocketmq-doc/en/design.md").values()))

    def test_markdown_inside_a_crate_can_be_a_compile_time_asset(self) -> None:
        self.assertTrue(self.scope("rocketmq-client/prompts/diagnose.md")["rust"])

    def test_local_instructions_only_run_routing(self) -> None:
        enabled = {name for name, value in self.scope("rocketmq-client/AGENTS.md").items() if value}
        self.assertEqual({"routing"}, enabled)

    def test_approval_changes_run_decision_tests_without_compiling_rust(self) -> None:
        scope = self.scope(".github/workflows/auto_approve_pull_requests.yml")
        self.assertTrue(scope["automation"] and scope["routing"])
        self.assertFalse(scope["rust"])

    def test_standalone_frontend_and_rust_do_not_compile_root_workspace(self) -> None:
        for path in (
            "rocketmq-ai/rocketmq-mcp/src/main.rs",
            "rocketmq-ai/rocketmq-sre/crates/core/src/lib.rs",
            "rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/App.tsx",
            "rocketmq-dashboard/rocketmq-dashboard-web/backend/src/lib.rs",
        ):
            with self.subTest(path=path):
                self.assertFalse(self.scope(path)["rust"])

    def test_shared_dashboard_crate_still_compiles_root_workspace(self) -> None:
        self.assertTrue(self.scope("rocketmq-dashboard/rocketmq-dashboard-common/src/lib.rs")["rust"])

    def test_internal_client_change_does_not_trigger_specialist_matrices(self) -> None:
        enabled = {name for name, value in self.scope("rocketmq-client/src/consumer.rs").items() if value}
        self.assertEqual({"rust"}, enabled)

    def test_affected_feature_and_contract_checks_are_selected(self) -> None:
        for path, expected in (
            ("rocketmq-broker/src/observability/service.rs", {"rust", "observability", "rocksdb"}),
            ("rocketmq-protocol/src/header.rs", {"rust", "header"}),
            ("scripts/request-header-codec/compare_header_schema.py", {"header"}),
            ("scripts/error_architecture_guard.py", {"errors"}),
        ):
            with self.subTest(path=path):
                enabled = {name for name, value in self.scope(path).items() if value}
                self.assertEqual(expected, enabled)

    def test_root_dependency_and_member_feature_changes_cover_matrices(self) -> None:
        for path in ("Cargo.lock", "Cargo.toml", "rocketmq-client/Cargo.toml"):
            with self.subTest(path=path):
                scope = self.scope(path)
                self.assertTrue(scope["rust"] and scope["observability"] and scope["rocksdb"])
                self.assertTrue(scope["full_features"])

    def test_ordinary_source_changes_keep_default_feature_checks(self) -> None:
        self.assertFalse(self.scope("rocketmq-client/src/consumer.rs")["full_features"])

    def test_scheduled_and_manual_runs_cover_all_checks(self) -> None:
        self.assertTrue(all(ci_scope.classify([], self.members, full=True).values()))

    def test_failed_change_detection_is_not_treated_as_an_empty_diff(self) -> None:
        with patch("scripts.ci_scope.subprocess.run", side_effect=subprocess.CalledProcessError(1, "git")):
            with self.assertRaises(subprocess.CalledProcessError):
                ci_scope.changed_paths(Path("."), "base")

    def test_git_diff_preserves_spaces_deletions_and_both_sides_of_moves(self) -> None:
        result = subprocess.CompletedProcess([], 0, stdout=b"rocketmq-client/old name.rs\0docs/new name.rs\0")
        with patch("scripts.ci_scope.subprocess.run", return_value=result) as run:
            self.assertEqual(
                ["rocketmq-client/old name.rs", "docs/new name.rs"],
                ci_scope.changed_paths(Path("."), "base"),
            )
        self.assertIn("--no-renames", run.call_args.args[0])
        self.assertIn("-z", run.call_args.args[0])

    def test_workspace_member_patterns_also_cover_deleted_packages(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ci-scope-") as directory:
            root = Path(directory)
            (root / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/*"]\n', encoding="utf-8")
            (root / "crates/member").mkdir(parents=True)
            (root / "crates/member/Cargo.toml").write_text('[package]\nname = "member"\n', encoding="utf-8")
            (root / "standalone").mkdir()
            (root / "standalone/Cargo.toml").write_text("[workspace]\n", encoding="utf-8")
            members = ci_scope.workspace_members(root)
            self.assertTrue(ci_scope.classify(["crates/member/src/lib.rs"], members)["rust"])
            self.assertTrue(ci_scope.classify(["crates/deleted/Cargo.toml"], members)["rust"])
            self.assertFalse(ci_scope.classify(["standalone/src/lib.rs"], members)["rust"])


class CiScopeEventTests(unittest.TestCase):
    """Exercise the CLI against real Git history and GitHub event/output files."""

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory(prefix="ci-scope-event-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "scripts").mkdir()
        (self.root / "scripts/ci_scope.py").write_bytes(Path(ci_scope.__file__).read_bytes())
        (self.root / "Cargo.toml").write_text('[workspace]\nmembers = ["member"]\n', encoding="utf-8")
        (self.root / "member").mkdir()
        (self.root / "member/Cargo.toml").write_text('[package]\nname = "member"\n', encoding="utf-8")
        (self.root / "member/old name.rs").write_text("pub fn original() {}\n", encoding="utf-8")
        self.git("init", "--quiet")
        self.git("add", ".")
        self.commit()
        self.base = self.git("rev-parse", "HEAD").strip()

    def git(self, *args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=self.root, check=True, capture_output=True, text=True,
        ).stdout

    def commit(self) -> None:
        self.git("-c", "user.name=CI Test", "-c", "user.email=ci@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "--quiet", "--no-verify", "-m", "test input")

    def run_event(self, event_name: str, payload: dict) -> tuple[subprocess.CompletedProcess, dict[str, str]]:
        event = self.root / "event.json"
        output = self.root / "outputs.txt"
        event.write_text(json.dumps(payload), encoding="utf-8")
        result = subprocess.run(
            [sys.executable, str(self.root / "scripts/ci_scope.py")], cwd=self.root,
            env={**os.environ, "GITHUB_EVENT_NAME": event_name, "GITHUB_EVENT_PATH": str(event),
                 "GITHUB_OUTPUT": str(output)}, capture_output=True, text=True,
        )
        values = dict(line.split("=", 1) for line in output.read_text().splitlines()) if output.exists() else {}
        return result, values

    def test_pr_move_out_of_workspace_still_validates_original_consumer(self) -> None:
        (self.root / "docs").mkdir()
        self.git("mv", "member/old name.rs", "docs/new name.rs")
        self.commit()
        result, values = self.run_event("pull_request", {"pull_request": {"base": {"sha": self.base}}})
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("true", values["rust"])
        self.assertEqual(2, json.loads(result.stdout)["changed_files"])

    def test_push_deletion_is_not_an_empty_change(self) -> None:
        self.git("rm", "member/old name.rs")
        self.commit()
        result, values = self.run_event("push", {"before": self.base})
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("true", values["rust"])

    def test_deleting_a_member_manifest_cannot_skip_rust_validation(self) -> None:
        self.git("rm", "member/Cargo.toml")
        self.commit()
        result, values = self.run_event("push", {"before": self.base})
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("true", values["rust"])
        self.assertEqual("true", values["full_features"])

    def test_empty_pr_diff_emits_false_outputs(self) -> None:
        result, values = self.run_event("pull_request", {"pull_request": {"base": {"sha": self.base}}})
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue(values and all(value == "false" for value in values.values()))

    def test_initial_push_selects_full_validation(self) -> None:
        result, values = self.run_event("push", {"before": "0" * 40})
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue(values and all(value == "true" for value in values.values()))

    def test_manual_and_scheduled_events_need_no_diff_payload(self) -> None:
        for event in ("schedule", "workflow_dispatch"):
            with self.subTest(event=event):
                result, values = self.run_event(event, {})
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertTrue(values and all(value == "true" for value in values.values()))

    def test_missing_git_revision_fails_without_success_outputs(self) -> None:
        result, values = self.run_event("push", {"before": "missing-revision"})
        self.assertNotEqual(0, result.returncode)
        self.assertEqual({}, values)


if __name__ == "__main__":
    unittest.main()

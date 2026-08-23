# Copyright 2023 The RocketMQ Rust Authors
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
import os
import shutil
import subprocess
import tempfile
import unittest
from datetime import datetime
from datetime import timezone
from pathlib import Path
from unittest import mock

from scripts import module_maintainability_guard as guard


def metric(
    path: str,
    lines: int,
    *,
    public_items: int = 0,
    reexports: int = 0,
    lock_sites: int = 1,
    state_owners: int = 1,
    fan_out: int = 2,
    score: float | None = None,
) -> guard.FileMetrics:
    return guard.FileMetrics(
        path=path,
        crate=path.split("/", 1)[0],
        production_lines=lines,
        public_items=public_items,
        reexports=reexports,
        lock_sites=lock_sites,
        state_owners=state_owners,
        fan_out=fan_out,
        test_functions=2,
        churn_commits=3,
        contributors=2,
        defect_commits=1,
        score=float(score if score is not None else lines),
    )


def baseline_metrics() -> list[guard.FileMetrics]:
    return [metric(f"crate-{index}/src/hotspot_{index}.rs", 900 - index, score=1000 - index) for index in range(20)]


def decision_section(path: str, *, outcome: str = "retained") -> str:
    return f"""
### `{path}`

- Decision: `{outcome}`.
- Owner: crate maintainers.
- State owner: the parent module remains the only mutable state owner.
- Evidence: focused behavior tests cover the retained boundary.
- Revisit when: public surface, fan-out, locks, or production lines grow.
"""


class ModuleMaintainabilityGuardTests(unittest.TestCase):
    def test_history_window_uses_utc_calendar_year_and_clamps_leap_day(self) -> None:
        anchor = datetime(2024, 2, 29, 23, 59, 58, tzinfo=timezone.utc)
        completed = subprocess.CompletedProcess([], 0, f"{int(anchor.timestamp())}\n", "")

        with mock.patch.object(guard.subprocess, "run", return_value=completed) as run:
            cutoff_epoch, anchor_epoch = guard.history_window(Path("fixture"))

        self.assertEqual(int(anchor.timestamp()), anchor_epoch)
        self.assertEqual(
            int(datetime(2023, 2, 28, 23, 59, 58, tzinfo=timezone.utc).timestamp()),
            cutoff_epoch,
        )
        self.assertEqual(
            ["git", "show", "-s", "--format=%ct", "HEAD"],
            run.call_args.args[0],
        )

    def test_same_head_uses_fixed_history_arguments_independent_of_wall_clock(self) -> None:
        anchor_epoch = int(datetime(2025, 3, 1, 0, 0, tzinfo=timezone.utc).timestamp())
        history = "@@commit\tauthor@example.test\tfix fixture\nfixture.rs\n"

        def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            if command[1] == "show":
                return subprocess.CompletedProcess(command, 0, f"{anchor_epoch}\n", "")
            return subprocess.CompletedProcess(command, 0, history, "")

        with mock.patch.object(guard.subprocess, "run", side_effect=run) as mocked:
            first = guard.git_history(Path("fixture"))
            second = guard.git_history(Path("fixture"))

        self.assertEqual(first, second)
        log_commands = [call.args[0] for call in mocked.call_args_list if call.args[0][1] == "log"]
        self.assertEqual(2, len(log_commands))
        self.assertEqual(log_commands[0], log_commands[1])

    def test_history_log_is_bounded_to_the_fixed_head_window(self) -> None:
        anchor_epoch = int(datetime(2025, 3, 1, 0, 0, tzinfo=timezone.utc).timestamp())

        def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            if command[1] == "show":
                return subprocess.CompletedProcess(command, 0, f"{anchor_epoch}\n", "")
            return subprocess.CompletedProcess(command, 0, "", "")

        with mock.patch.object(guard.subprocess, "run", side_effect=run) as mocked:
            guard.git_history(Path("fixture"))

        log_command = next(call.args[0] for call in mocked.call_args_list if call.args[0][1] == "log")
        self.assertIn("--since=@1709251200", log_command)
        self.assertIn("--until=@1740787200", log_command)
        self.assertNotIn("--since=12 months ago", log_command)

    def test_invalid_head_timestamp_fails_closed_without_running_git_log(self) -> None:
        completed = subprocess.CompletedProcess([], 0, "not-a-timestamp\n", "")

        with mock.patch.object(guard.subprocess, "run", return_value=completed) as run:
            self.assertEqual({}, guard.git_history(Path("fixture")))

        self.assertEqual(1, run.call_count)
        self.assertEqual(["git", "show", "-s", "--format=%ct", "HEAD"], run.call_args.args[0])

    def test_nonrepresentable_head_timestamp_fails_closed_without_running_git_log(self) -> None:
        completed = subprocess.CompletedProcess([], 0, "9223372036854775807\n", "")

        with (
            mock.patch.object(guard.subprocess, "run", return_value=completed) as run,
            mock.patch.object(guard, "datetime") as date_time,
        ):
            date_time.fromtimestamp.side_effect = OverflowError("timestamp out of range")
            self.assertEqual({}, guard.git_history(Path("fixture")))

        self.assertEqual(1, run.call_count)
        self.assertEqual(["git", "show", "-s", "--format=%ct", "HEAD"], run.call_args.args[0])

    def test_ranking_uses_history_and_ownership_not_only_lines(self) -> None:
        large_static = guard.score_metrics(1_000, 0, 0, 0, 1, 0, guard.History())
        smaller_high_churn = guard.score_metrics(
            700,
            8,
            12,
            3,
            8,
            25,
            guard.History(commits=40, contributors=8, defects=6),
        )

        self.assertGreater(smaller_high_churn, large_static)

    def test_projection_excludes_cfg_test_modules_and_test_only_files(self) -> None:
        source = """
pub struct RuntimeState;

#[cfg(test)]
mod tests {
    pub struct NotProduction;
    #[test]
    fn fixture() {}
}
"""
        projected = guard.production_projection(source)

        self.assertIn("RuntimeState", projected)
        self.assertNotIn("NotProduction", projected)
        self.assertTrue(guard.is_test_only_file(Path("crate/src/behavior_tests.rs")))
        self.assertFalse(guard.is_test_only_file(Path("crate/src/runtime.rs")))

    def test_scan_file_does_not_count_comment_only_lines_as_production_loc(
        self,
    ) -> None:
        with self.subTest("documentation and comments"):
            source = """
//! Crate documentation.
/// Item documentation.
// Implementation note.
pub struct RuntimeState;
"""
            self.assertEqual(1, guard.production_code_lines(source))

    def test_new_oversized_module_fails_closed(self) -> None:
        baseline = guard.baseline_payload(baseline_metrics())
        current = baseline_metrics() + [metric("crate-new/src/oversized.rs", 801)]

        findings = guard.compare(current, baseline)

        self.assertIn("new-oversized-module", {finding.code for finding in findings})

    def test_existing_hotspot_growth_and_public_expansion_fail(self) -> None:
        metrics = baseline_metrics()
        baseline = guard.baseline_payload(metrics)
        changed = list(metrics)
        changed[0] = metric(metrics[0].path, metrics[0].production_lines + 1, public_items=1)

        findings = guard.compare(changed, baseline)
        codes = {finding.code for finding in findings}

        self.assertIn("hotspot-growth", codes)
        self.assertIn("public-surface-growth", codes)
        self.assertIn("crate-public-surface-growth", codes)

    def test_private_move_can_reduce_hotspot_without_expanding_surface(self) -> None:
        metrics = baseline_metrics()
        baseline = guard.baseline_payload(metrics)
        reduced = list(metrics)
        reduced[0] = metric(metrics[0].path, 500)

        self.assertEqual([], guard.compare(reduced, baseline))

    def test_state_owner_and_fan_out_growth_fail(self) -> None:
        metrics = baseline_metrics()
        baseline = guard.baseline_payload(metrics)
        changed = list(metrics)
        changed[0] = metric(
            metrics[0].path,
            metrics[0].production_lines,
            state_owners=metrics[0].state_owners + 1,
            fan_out=metrics[0].fan_out + 1,
        )

        findings = guard.compare(changed, baseline)
        codes = {finding.code for finding in findings}

        self.assertIn("state-owner-growth", codes)
        self.assertIn("fan-out-growth", codes)

    def test_mechanical_fragment_names_fail(self) -> None:
        metrics = baseline_metrics()
        baseline = guard.baseline_payload(metrics)
        current = list(metrics) + [metric("crate/src/impl_2.rs", 20)]

        findings = guard.compare(current, baseline)

        self.assertIn("mechanical-module-fragment", {finding.code for finding in findings})

    def test_decision_ledger_requires_every_ranked_hotspot(self) -> None:
        metrics = baseline_metrics()
        document = "\n".join(decision_section(item.path) for item in metrics[:-1])

        findings = guard.validate_decision_ledger(metrics, document)

        self.assertEqual(1, len(findings))
        self.assertEqual("missing-hotspot-decision", findings[0].code)
        self.assertEqual(metrics[-1].path, findings[0].path)

    def test_decision_ledger_requires_complete_evidence(self) -> None:
        hotspot = metric("crate/src/hotspot.rs", 900)
        document = """
### `crate/src/hotspot.rs`

- Decision: `retained`.
- Owner: crate maintainers.
"""

        findings = guard.validate_decision_ledger([hotspot], document)

        self.assertEqual({"incomplete-hotspot-decision"}, {finding.code for finding in findings})


class GitHistoryWindowTests(unittest.TestCase):
    def init_repo(self) -> Path:
        directory = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        for command in (
            ["git", "init", "--quiet"],
            ["git", "config", "user.name", "Fixture Author"],
            ["git", "config", "user.email", "fixture@example.test"],
        ):
            subprocess.run(command, cwd=directory, check=True)
        return directory

    def commit(
        self,
        root: Path,
        timestamp: str,
        body: str = "pub struct Fixture;\n",
        *,
        path: str = "fixture.rs",
    ) -> None:
        (root / path).write_text(body, encoding="utf-8")
        environment = os.environ | {
            "GIT_AUTHOR_DATE": timestamp,
            "GIT_COMMITTER_DATE": timestamp,
        }
        subprocess.run(["git", "add", path], cwd=root, check=True)
        subprocess.run(
            ["git", "commit", "--quiet", "-m", "fixture history"],
            cwd=root,
            check=True,
            env=environment,
        )

    def test_history_includes_fixed_window_boundaries(self) -> None:
        root = self.init_repo()
        self.commit(root, "2020-01-01T00:00:00+0000", path="initial.rs")
        self.commit(
            root,
            "2024-02-29T23:59:59+0000",
            "pub struct BeforeCutoff;\n",
            path="before_cutoff.rs",
        )
        self.commit(
            root,
            "2024-03-01T00:00:00+0000",
            "pub struct Cutoff;\n",
            path="cutoff.rs",
        )
        self.commit(
            root,
            "2025-03-01T00:00:00+0000",
            "pub struct Anchor;\n",
            path="anchor.rs",
        )

        history = guard.git_history(root)

        self.assertEqual(guard.History(commits=1, contributors=1, defects=0), history["cutoff.rs"])
        self.assertEqual(guard.History(commits=1, contributors=1, defects=0), history["anchor.rs"])
        self.assertNotIn("before_cutoff.rs", history)

    def test_history_excludes_a_future_dated_ancestor(self) -> None:
        root = self.init_repo()
        self.commit(root, "2020-01-01T00:00:00+0000", path="initial.rs")
        self.commit(root, "2024-03-01T00:00:00+0000", path="cutoff.rs")
        self.commit(root, "2025-03-01T00:00:01+0000", path="future.rs")
        self.commit(root, "2025-03-01T00:00:00+0000", path="anchor.rs")

        history = guard.git_history(root)

        self.assertIn("cutoff.rs", history)
        self.assertIn("anchor.rs", history)
        self.assertNotIn("future.rs", history)

    def test_new_head_advances_window_and_counts_its_commit(self) -> None:
        root = self.init_repo()
        self.commit(root, "2024-06-01T00:00:00+0000")
        self.commit(root, "2025-01-01T00:00:00+0000", "pub struct FirstHead;\n")

        before = guard.git_history(root)
        self.commit(root, "2025-02-01T00:00:00+0000", "pub struct SecondHead;\n")
        after = guard.git_history(root)

        self.assertEqual(2, before["fixture.rs"].commits)
        self.assertEqual(3, after["fixture.rs"].commits)


class RepositoryModuleMaintainabilityContracts(unittest.TestCase):
    def test_core_scan_excludes_projects_outside_the_release_allowlist(self) -> None:
        root = Path(__file__).resolve().parents[2]
        metrics = guard.scan_tree(root, scope="core-release")

        self.assertTrue(metrics)
        self.assertFalse(any(item.path.startswith("rocketmq-sre/") for item in metrics))
        self.assertFalse(any("rocketmq-mcp" in item.path for item in metrics))
        self.assertFalse(any(item.path.startswith("rocketmq-dashboard/") for item in metrics))

    def test_ci_scans_maintainability_with_complete_git_history(self) -> None:
        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github/workflows/rocketmq-rust-ci.yaml").read_text(encoding="utf-8")
        architecture_guards = workflow.split("  architecture-guards:", 1)[1].split(
            "  architecture-contracts:", 1
        )[0]

        self.assertIn("fetch-depth: 0", architecture_guards)

    def test_repository_baseline_and_report_are_current(self) -> None:
        root = Path(__file__).resolve().parents[2]
        baseline = json.loads((root / "scripts/module-maintainability-baseline.json").read_text(encoding="utf-8"))
        metrics = guard.scan_tree(root, scope="core-release")

        self.assertEqual([], guard.compare(metrics, baseline))
        self.assertEqual(
            guard.render_report(baseline),
            (root / "rocketmq-doc/en/module-maintainability-board.md").read_text(encoding="utf-8"),
        )
        decision_path = root / "rocketmq-doc/en/hotspot-module-decisions.md"
        decision_document = decision_path.read_text(encoding="utf-8") if decision_path.is_file() else ""
        self.assertEqual(
            [],
            guard.validate_decision_ledger(metrics[: guard.RANKED_HOTSPOTS], decision_document),
        )


if __name__ == "__main__":
    unittest.main()

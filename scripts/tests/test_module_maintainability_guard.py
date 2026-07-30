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
import unittest
from pathlib import Path

from scripts import module_maintainability_guard as guard


def metric(
    path: str,
    lines: int,
    *,
    public_items: int = 0,
    reexports: int = 0,
    score: float | None = None,
) -> guard.FileMetrics:
    return guard.FileMetrics(
        path=path,
        crate=path.split("/", 1)[0],
        production_lines=lines,
        public_items=public_items,
        reexports=reexports,
        lock_sites=1,
        state_owners=1,
        fan_out=2,
        test_functions=2,
        churn_commits=3,
        contributors=2,
        defect_commits=1,
        score=float(score if score is not None else lines),
    )


def baseline_metrics() -> list[guard.FileMetrics]:
    return [metric(f"crate-{index}/src/hotspot_{index}.rs", 900 - index, score=1000 - index) for index in range(20)]


class ModuleMaintainabilityGuardTests(unittest.TestCase):
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


class RepositoryModuleMaintainabilityContracts(unittest.TestCase):
    def test_repository_baseline_and_report_are_current(self) -> None:
        root = Path(__file__).resolve().parents[2]
        baseline = json.loads((root / "scripts/module-maintainability-baseline.json").read_text(encoding="utf-8"))
        metrics = guard.scan_tree(root)

        self.assertEqual([], guard.compare(metrics, baseline))
        self.assertEqual(
            guard.render_report(baseline),
            (root / "rocketmq-doc/en/module-maintainability-board.md").read_text(encoding="utf-8"),
        )


if __name__ == "__main__":
    unittest.main()

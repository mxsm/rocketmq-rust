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
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import rust_lint_debt_guard as guard  # noqa: E402


class RustLintDebtGuardTests(unittest.TestCase):
    def test_inventory_classifies_crate_module_and_item_scopes(self) -> None:
        entries = guard.inventory_source(
            "crate/src/lib.rs",
            """
#![allow(dead_code)]
#[allow(unused_variables)]
mod legacy {}
#[allow(clippy::too_many_arguments, reason = "wire adapter")]
fn call() {}
""",
        )
        self.assertEqual(["crate", "module"], [entry["scope"] for entry in entries])
        self.assertTrue(all(entry["owner"] == "crate" for entry in entries))

    def test_item_allow_with_inline_reason_is_self_governing(self) -> None:
        entries = guard.inventory_source(
            "crate/src/lib.rs",
            """
#[allow(clippy::too_many_arguments, reason = "immutable wire fields preserve protocol ordering")]
fn call() {}
""",
        )

        self.assertEqual([], entries)

    def test_unreasoned_item_allow_remains_central_debt(self) -> None:
        entries = guard.inventory_source(
            "crate/src/lib.rs",
            """
#[allow(clippy::too_many_arguments)]
fn call() {}
""",
        )

        self.assertEqual(1, len(entries))
        self.assertEqual("item", entries[0]["scope"])

    def test_unregistered_allow_and_threshold_drift_fail(self) -> None:
        current = guard.current_inventory(ROOT)
        registry = guard.render_registry(current)
        addition = copy.deepcopy(current[0])
        addition["identity"] += ":new"
        findings = guard.compare(registry, [*current, addition], "too-many-arguments-threshold = 20")
        self.assertTrue(any("unregistered" in finding for finding in findings))
        self.assertTrue(any("threshold" in finding for finding in findings))

    def test_repository_registry_covers_every_active_targeted_allow(self) -> None:
        registry = guard.validate_registry(
            json.loads((ROOT / "scripts/rust-lint-debt-registry.json").read_text(encoding="utf-8"))
        )
        self.assertEqual(
            [],
            guard.compare(
                registry,
                guard.current_inventory(ROOT),
                (ROOT / ".clippy.toml").read_text(encoding="utf-8"),
            ),
        )

    def test_registry_requires_owner_reason_and_removal_issue(self) -> None:
        registry = guard.render_registry(guard.current_inventory(ROOT))
        broken = copy.deepcopy(registry)
        broken["entries"][0]["removal_issue"] = ""
        with self.assertRaises(ValueError):
            guard.validate_registry(broken)


if __name__ == "__main__":
    unittest.main()

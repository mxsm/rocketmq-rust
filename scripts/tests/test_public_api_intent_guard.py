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

import public_api_intent_guard as guard  # noqa: E402


class PublicApiIntentGuardTests(unittest.TestCase):
    def test_inventory_ignores_nested_items_and_classifies_compat(self) -> None:
        entries = guard.inventory_source(
            "crate/src/lib.rs",
            """
pub mod stable;
pub mod legacy_compat {
    pub struct Nested;
}
pub use stable::Api;
""",
            "crate",
        )
        self.assertEqual(3, len(entries))
        self.assertEqual("compat", entries[1]["category"])
        self.assertFalse(any("Nested" in entry["identity"] for entry in entries))

    def test_inventory_excludes_restricted_visibility(self) -> None:
        entries = guard.inventory_source(
            "crate/src/lib.rs",
            """
pub(crate) mod crate_only;
pub(super) use parent::ParentOnly;
pub(in crate::sealed) struct Sealed;
pub mod public;
""",
            "crate",
        )

        self.assertEqual(["pub mod public"], [entry["declaration"] for entry in entries])

    def test_manifest_regeneration_preserves_manual_classification(self) -> None:
        inventory = {
            "crate": [
                {
                    "identity": "crate/src/lib.rs:pub mod test_support",
                    "path": "crate/src/lib.rs",
                    "declaration": "pub mod test_support",
                    "category": "experimental",
                    "owner": "crate",
                    "rationale": "generated rationale",
                    "removal_condition": "generated removal condition",
                }
            ]
        }
        previous = guard.render_manifest(inventory)
        previous_entry = previous["crates"]["crate"]["entries"][0]
        previous_entry["rationale"] = "manually reviewed rationale"
        previous_entry["removal_condition"] = "manually reviewed removal condition"

        regenerated = guard.render_manifest(inventory, previous)
        regenerated_entry = regenerated["crates"]["crate"]["entries"][0]

        self.assertEqual("manually reviewed rationale", regenerated_entry["rationale"])
        self.assertEqual("manually reviewed removal condition", regenerated_entry["removal_condition"])

    def test_new_export_and_growth_fail_closed(self) -> None:
        inventory = guard.current_inventory(ROOT)
        manifest = guard.render_manifest(inventory)
        crate = next(iter(inventory))
        addition = dict(inventory[crate][0])
        addition["identity"] += ":new"
        inventory[crate] = [*inventory[crate], addition]

        findings = guard.compare(manifest, inventory)

        self.assertTrue(any("unclassified export" in finding for finding in findings))
        self.assertTrue(any("export count grew" in finding for finding in findings))

    def test_repository_manifest_is_complete_and_categorized(self) -> None:
        manifest = guard.validate_manifest(
            json.loads((ROOT / "scripts/public-api-intent.json").read_text(encoding="utf-8"))
        )
        findings = guard.compare(manifest, guard.current_inventory(ROOT))
        self.assertEqual([], findings, "\n".join(findings))
        counts = guard.summary(manifest)
        for crate in guard.CRATES:
            self.assertGreater(counts[crate]["total"], 0)
            self.assertGreater(counts[crate]["experimental"] + counts[crate]["compat"], 0)
        self.assertEqual(0, counts["rocketmq-store-local"]["stable"])

    def test_metadata_is_required_for_every_entry(self) -> None:
        manifest = guard.render_manifest(guard.current_inventory(ROOT))
        broken = copy.deepcopy(manifest)
        crate = next(iter(broken["crates"]))
        broken["crates"][crate]["entries"][0]["owner"] = ""
        with self.assertRaises(ValueError):
            guard.validate_manifest(broken)

    def test_structural_metadata_drift_fails_closed(self) -> None:
        inventory = guard.current_inventory(ROOT)
        manifest = guard.render_manifest(inventory)
        crate = next(iter(manifest["crates"]))
        manifest["crates"][crate]["entries"][0]["owner"] = "wrong-owner"

        findings = guard.compare(manifest, inventory)

        self.assertTrue(any("owner metadata drift" in finding for finding in findings))


if __name__ == "__main__":
    unittest.main()

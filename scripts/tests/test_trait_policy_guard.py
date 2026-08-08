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
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import trait_policy_guard as guard  # noqa: E402


class TraitPolicyGuardTests(unittest.TestCase):
    def test_inventory_classifies_macros_native_async_and_marker(self) -> None:
        entries = guard.inventory_source(
            "rocketmq-fixture/src/lib.rs",
            """
#[async_trait]
pub trait Legacy { async fn run(&self); }
#[trait_variant::make(Service: Send)]
pub trait LocalService { async fn serve(&self); }
pub trait Native { async fn load(&self); }
pub trait Empty: Send {}
#[cfg(test)]
mod tests {
    #[async_trait]
    impl Legacy for Fixture { async fn run(&self) {} }
}
""",
        )
        kinds = [entry["kind"] for entry in entries]
        self.assertEqual(1, kinds.count("async_trait"))
        self.assertEqual(1, kinds.count("trait_variant"))
        self.assertEqual(0, kinds.count("native_async"))
        self.assertEqual(1, kinds.count("empty_marker"))

    def test_native_async_is_compliant_and_not_inventory_debt(self) -> None:
        entries = guard.inventory_source(
            "rocketmq-client/src/capability.rs",
            """
#[allow(async_fn_in_trait)]
pub trait Capability: Send + Sync {
    async fn execute(&self);
}
""",
        )

        self.assertEqual([], entries)

    def test_empty_markers_inside_private_modules_are_not_public_debt(self) -> None:
        entries = guard.inventory_source(
            "rocketmq-protocol/src/sealed.rs",
            """
pub trait TopLevelMarker {}
mod private {
    pub trait Sealed {}
    pub mod nested_public {
        pub trait NestedSealed {}
    }
}
pub mod public {
    pub trait PublicMarker {}
}
pub(crate) mod crate_visible {
    pub trait CrateMarker {}
}
""",
        )

        self.assertEqual(
            ["trait TopLevelMarker", "trait PublicMarker", "trait CrateMarker"],
            [entry["item"] for entry in entries],
        )

    def test_mq_admin_marker_has_p2_4_owner_decision(self) -> None:
        entries = guard.inventory_source(
            "rocketmq-client/src/admin.rs",
            "pub trait MQAdminExtInner: Send + Sync + 'static {}\n",
        )
        self.assertEqual("remove-in-P2.4", entries[0]["decision"])
        self.assertEqual("client", entries[0]["owner"])

    def test_identity_is_stable_across_line_moves(self) -> None:
        entry = {
            "kind": "async_trait",
            "path": "crate/src/lib.rs",
            "line": 10,
            "item": "trait Service",
            "owner": "crate",
            "decision": "migrate-on-touch",
        }
        moved = dict(entry, line=200)
        self.assertEqual(guard.identity(entry), guard.identity(moved))

    def test_duplicate_identity_growth_is_rejected(self) -> None:
        entry = {
            "kind": "async_trait",
            "path": "crate/src/lib.rs",
            "line": 10,
            "item": "trait Service",
            "owner": "crate",
            "decision": "migrate-on-touch",
        }
        duplicate = dict(entry, line=200)
        additions, removed = guard.compare_entries([entry], [entry, duplicate])
        self.assertEqual([duplicate], additions)
        self.assertEqual(0, removed)

    def test_live_inventory_does_not_grow(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPTS / "trait_policy_guard.py")],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("TRAIT_POLICY_GUARD_OK", result.stdout)


if __name__ == "__main__":
    unittest.main()

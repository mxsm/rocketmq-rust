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
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
GUARD = REPO_ROOT / "scripts" / "stable_surface_guard.py"
sys.path.insert(0, str(REPO_ROOT / "scripts"))
import stable_surface_guard as stable_guard


class StableSurfaceGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="stable-surface-")
        self.root = Path(self.temporary.name)
        (self.root / "crate" / "src").mkdir(parents=True)
        self.source = self.root / "crate" / "src" / "lib.rs"
        self.source.write_text("#![feature(example_feature)]\npub fn value() {}\n", encoding="utf-8")
        self.policy = self.root / "policy.json"
        self.write_policy([self.allowed("crate/src/lib.rs", "example_feature")])

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def allowed(path: str, feature: str) -> dict[str, str]:
        return {
            "path": path,
            "feature": feature,
            "owner": "test",
            "remove_by": "R22",
            "reason": "Test fixture debt.",
        }

    def write_policy(self, allowed_features: list[dict[str, str]]) -> None:
        self.policy.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "target": "stable-default",
                    "allowed_features": allowed_features,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def run_guard(self, mode: str = "baseline") -> subprocess.CompletedProcess[str]:
        return self.run_guard_with_api(mode=mode)

    def run_guard_with_api(
        self,
        *,
        mode: str = "baseline",
        scope: str = "all",
        api_baseline: Path | None = None,
        api_reexport_inventory: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = [
            sys.executable,
            str(GUARD),
            "--root",
            str(self.root),
            "--policy",
            str(self.policy),
            "--mode",
            mode,
            "--scope",
            scope,
        ]
        if api_baseline is not None:
            command.extend(("--api-baseline", str(api_baseline)))
        if api_reexport_inventory is not None:
            command.extend(("--api-reexport-inventory", str(api_reexport_inventory)))
        return subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

    def test_exact_baseline_passes(self) -> None:
        result = self.run_guard()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("mode=baseline features=1", result.stdout)

    def test_unregistered_feature_fails_closed(self) -> None:
        self.source.write_text(
            "#![feature(example_feature, unregistered_feature)]\npub fn value() {}\n",
            encoding="utf-8",
        )
        result = self.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unregistered nightly features", result.stderr)

    def test_stale_policy_entry_is_rejected(self) -> None:
        self.source.write_text("pub fn value() {}\n", encoding="utf-8")
        result = self.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("stale allowed nightly features", result.stderr)

    def test_target_mode_rejects_registered_debt(self) -> None:
        result = self.run_guard("target")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("stable target still has nightly features", result.stderr)

    def test_clean_target_passes(self) -> None:
        self.source.write_text("pub fn value() {}\n", encoding="utf-8")
        self.write_policy([])
        result = self.run_guard("target")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("mode=target features=0", result.stdout)

    def test_target_mode_rejects_an_unapproved_api_break_decision(self) -> None:
        self.source.write_text("pub fn value() {}\n", encoding="utf-8")
        self.write_policy([])
        baseline = self.root / "api-baseline.json"
        value = self.api_freeze_fixture()
        value["compatibility_decisions"].append(
            {
                "id": "API-001",
                "classification": "approved-break",
                "applies_to": "post-freeze",
                "profile_id": "rocketmq-store:default",
                "package": "rocketmq-store",
                "item_path": "rocketmq_store::MappedFileBuilder",
                "change": "signature",
                "replacement": "rocketmq_store::MappedFileBuilder",
                "reason": "Test fixture.",
                "approved_by": "",
                "approved_on": "2026-08-16",
            }
        )
        baseline.write_text(json.dumps(value), encoding="utf-8")

        result = self.run_guard_with_api(mode="target", api_baseline=baseline)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("approved_by must be non-empty", result.stderr)

    def test_target_mode_accepts_a_complete_api_freeze_contract(self) -> None:
        self.source.write_text("pub fn value() {}\n", encoding="utf-8")
        self.write_policy([])
        baseline = self.root / "api-baseline.json"
        baseline.write_text(json.dumps(self.api_freeze_fixture()), encoding="utf-8")

        result = self.run_guard_with_api(mode="target", api_baseline=baseline)

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("api_freeze=verified", result.stdout)

    def test_explicit_default_baseline_resolves_the_default_reexport_inventory(self) -> None:
        baseline, inventory = stable_guard.resolve_api_contract_inputs(
            stable_guard.DEFAULT_API_BASELINE,
            None,
            mode="baseline",
            scope="all",
        )

        self.assertEqual(stable_guard.DEFAULT_API_BASELINE.resolve(), baseline)
        self.assertEqual(stable_guard.DEFAULT_API_REEXPORT_INVENTORY.resolve(), inventory)

    def test_inventory_without_a_baseline_is_rejected(self) -> None:
        with self.assertRaisesRegex(stable_guard.InputError, "requires --api-baseline"):
            stable_guard.resolve_api_contract_inputs(
                None,
                self.policy,
                mode="baseline",
                scope="all",
            )
        with self.assertRaisesRegex(stable_guard.InputError, "requires api_baseline"):
            stable_guard.validate(
                self.root,
                self.policy,
                "baseline",
                api_reexport_inventory=self.policy,
            )

        empty_inventory = self.root / "empty-api-reexport-inventory.json"
        empty_inventory.write_text("{}", encoding="utf-8")
        for inventory in (empty_inventory, stable_guard.DEFAULT_API_REEXPORT_INVENTORY):
            with self.subTest(inventory=inventory):
                result = self.run_guard_with_api(
                    mode="target",
                    scope="core-release",
                    api_reexport_inventory=inventory,
                )

                self.assertNotEqual(0, result.returncode)
                self.assertIn("requires --api-baseline", result.stderr)

    def test_target_mode_accepts_an_explicit_reexport_inventory_for_a_custom_baseline(self) -> None:
        self.source.write_text("pub fn value() {}\n", encoding="utf-8")
        self.write_policy([])
        baseline = self.root / "api-baseline.json"
        baseline.write_text(json.dumps(self.api_freeze_fixture()), encoding="utf-8")
        inventory = self.root / "api-reexport-inventory.json"
        inventory.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "profiles": {
                        "rocketmq-store:default": {
                            "package": "rocketmq-store",
                            "item_paths": ["rocketmq_store::MappedFileBuilder"],
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        result = self.run_guard_with_api(
            mode="target",
            api_baseline=baseline,
            api_reexport_inventory=inventory,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("api_freeze=verified", result.stdout)

    def test_target_mode_rejects_missing_or_unknown_explicit_reexport_inventory_selection(self) -> None:
        self.source.write_text("pub fn value() {}\n", encoding="utf-8")
        self.write_policy([])
        baseline = self.root / "api-baseline.json"
        baseline.write_text(json.dumps(self.api_freeze_fixture()), encoding="utf-8")
        invalid_inventories = (
            (
                "missing",
                {
                    "schema_version": 1,
                    "profiles": {
                        "rocketmq-store:default": {
                            "package": "rocketmq-store",
                            "item_paths": ["rocketmq_store::Missing"],
                        }
                    },
                },
                "paths are absent",
            ),
            (
                "unknown",
                {
                    "schema_version": 1,
                    "profiles": {
                        "rocketmq-unknown:default": {
                            "package": "rocketmq-store",
                            "item_paths": ["rocketmq_store::MappedFileBuilder"],
                        }
                    },
                },
                "unknown profile",
            ),
        )
        for name, value, expected in invalid_inventories:
            with self.subTest(name=name):
                inventory = self.root / f"{name}-inventory.json"
                inventory.write_text(json.dumps(value), encoding="utf-8")

                result = self.run_guard_with_api(
                    mode="target",
                    api_baseline=baseline,
                    api_reexport_inventory=inventory,
                )

                self.assertNotEqual(0, result.returncode)
                self.assertIn(expected, result.stderr)

    @staticmethod
    def api_freeze_fixture() -> dict[str, object]:
        profiles = {}
        packages = {}
        contracts = []
        anchors = {
            "F-13": ("rocketmq-store", "rocketmq_store::MappedFileBuilder"),
            "F-18": ("rocketmq-client-rust", "rocketmq_client_rust::DefaultMQPullConsumer"),
            "F-15": ("rocketmq-proxy-core", "rocketmq_proxy_core::SettingsPolicyValues"),
        }
        for capability_id, (package, item_path) in anchors.items():
            profile_id = f"{package}:default"
            profiles[profile_id] = {
                "package": package,
                "target": package.replace("-", "_"),
                "default_features": True,
                "all_features": False,
                "features": [],
                "declared_default_features": [],
                "source": "workspace-default",
                "matrix_ids": [],
                "crate_version": "1.0.0",
                "public_api": [
                    {
                        "package": package,
                        "module": item_path.rsplit("::", 1)[0],
                        "item_path": item_path,
                        "kind": "struct",
                        "visibility": "public",
                        "signature": "{}",
                        "feature": "default",
                    }
                ],
            }
            packages[package] = {"target": package.replace("-", "_"), "profile_ids": [profile_id]}
            contracts.append(
                {
                    "capability_id": capability_id,
                    "profile_id": profile_id,
                    "package": package,
                    "item_paths": [item_path],
                    "behavior": "Frozen test behavior.",
                    "evidence": ["fixture-test"],
                }
            )
        return {
            "schema_version": 3,
            "identity": "structural",
            "scope": "core-release",
            "freeze": {
                "version": "1.0.0-rc.1",
                "breaking_change_policy": "approval-required-after-freeze",
            },
            "toolchain": {"rustc": "x", "rustdoc": "x", "cargo": "x"},
            "packages": packages,
            "profiles": profiles,
            "public_api_intent": {},
            "compatibility_decisions": [],
            "frozen_contracts": contracts,
        }


class RepositoryStableSurfaceContracts(unittest.TestCase):
    def test_core_release_target_uses_the_phase_zero_scope(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(GUARD),
                "--root",
                str(REPO_ROOT),
                "--mode",
                "target",
                "--scope",
                "core-release",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        self.assertIn("scope=core-release", completed.stdout)

    def test_runtime_scheduler_uses_owned_stable_futures(self) -> None:
        crate_root = (REPO_ROOT / "rocketmq-runtime" / "src" / "lib.rs").read_text(encoding="utf-8")
        scheduler = (REPO_ROOT / "rocketmq-runtime" / "src" / "schedule.rs").read_text(encoding="utf-8")
        self.assertNotIn("#![feature(async_fn_traits)]", crate_root)
        self.assertNotIn("#![feature(unboxed_closures)]", crate_root)
        self.assertNotIn("AsyncFnMut", scheduler)
        self.assertEqual(scheduler.count("Fut: Future<Output = Result<()>> + Send + 'static"), 8)
        self.assertIn("let mut task_fn = task_fn.lock().await;", scheduler)
        self.assertIn("(task_fn)(token).await", scheduler)

    def test_arc_mut_compatibility_no_longer_requires_nightly(self) -> None:
        retired_benchmark = REPO_ROOT / "rocketmq-broker" / "benches" / "syncunsafecell_mut.rs"

        self.assertFalse((REPO_ROOT / "rocketmq").exists())
        self.assertFalse(retired_benchmark.exists())
        for crate in REPO_ROOT.glob("rocketmq-*"):
            for source_path in crate.rglob("*.rs"):
                source = source_path.read_text(encoding="utf-8")
                self.assertNotIn("#![feature(sync_unsafe_cell)]", source, source_path)
                self.assertNotIn("std::cell::SyncUnsafeCell", source, source_path)


if __name__ == "__main__":
    unittest.main()

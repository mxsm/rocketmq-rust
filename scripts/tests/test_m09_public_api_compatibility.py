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
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BASELINE = ROOT / "scripts" / "public-api-snapshot-baseline.json"
def load_module(name: str, relative: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {relative}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PUBLIC_API = load_module("public_api_snapshot", "scripts/public_api_snapshot.py")
MATRIX = load_module("m09_compatibility_matrix", "scripts/m09_compatibility_matrix.py")


def manifest(relative: str) -> dict:
    return tomllib.loads((ROOT / relative / "Cargo.toml").read_text(encoding="utf-8"))


class PublicApiCompatibilityTests(unittest.TestCase):
    def test_baseline_covers_every_workspace_library_target(self) -> None:
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
        targets = PUBLIC_API.workspace_library_targets()

        self.assertEqual(3, baseline["schema_version"])
        self.assertEqual("structural", baseline["identity"])
        self.assertEqual("core-release", baseline["scope"])
        self.assertGreater(len(targets), 0)
        self.assertEqual({package for package, _ in targets}, set(baseline["packages"]))
        def keys(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    yield key.lower()
                    yield from keys(item)
            elif isinstance(value, list):
                for item in value:
                    yield from keys(item)

        baseline_keys = set(keys(baseline))
        self.assertFalse(any("sha" in key for key in baseline_keys))
        self.assertFalse(any("digest" in key for key in baseline_keys))
        self.assertFalse(any("fingerprint" in key for key in baseline_keys))
        for package_name, package in baseline["packages"].items():
            self.assertIn(f"{package_name}:default", package["profile_ids"])
            for profile_id in package["profile_ids"]:
                profile = baseline["profiles"][profile_id]
                self.assertEqual(package_name, profile["package"])
                self.assertGreater(len(profile["public_api"]), 0)
                for item in profile["public_api"]:
                    self.assertEqual(
                        {"package", "module", "item_path", "kind", "visibility", "signature", "feature"},
                        set(item),
                    )

        expected_matrix_profiles = {entry.id for entry in MATRIX.MATRIX if entry.group == "feature"}
        actual_matrix_profiles = {
            matrix_id
            for profile in baseline["profiles"].values()
            for matrix_id in profile["matrix_ids"]
        }
        self.assertEqual(expected_matrix_profiles, actual_matrix_profiles)

    def test_snapshot_diff_requires_classification_and_marks_removal_breaking(self) -> None:
        item = {
            "package": "demo",
            "module": "demo",
            "item_path": "demo::Api",
            "kind": "struct",
            "visibility": "public",
            "signature": "{}",
            "feature": "default",
        }
        profile = {
            "package": "demo",
            "target": "demo",
            "default_features": True,
            "all_features": False,
            "features": [],
            "declared_default_features": [],
            "source": "workspace-default",
            "matrix_ids": [],
            "crate_version": "1.0.0",
            "public_api": [item],
        }
        baseline = {
            "schema_version": 3,
            "identity": "structural",
            "scope": "core-release",
            "freeze": {
                "version": "1.0.0-rc.1",
                "breaking_change_policy": "approval-required-after-freeze",
            },
            "toolchain": {"rustc": "same"},
            "packages": {"demo": {"target": "demo", "profile_ids": ["demo:default"]}},
            "profiles": {"demo:default": profile},
            "compatibility_decisions": [],
            "frozen_contracts": [],
        }

        self.assertEqual([], PUBLIC_API.compare_snapshots(baseline, baseline))
        changed = json.loads(json.dumps(baseline))
        changed["profiles"]["demo:default"]["public_api"][0]["signature"] = "changed"
        self.assertEqual("breaking", PUBLIC_API.compare_snapshots(baseline, changed)[0]["classification"])
        removed = {**baseline, "packages": {}, "profiles": {}}
        self.assertEqual("breaking", PUBLIC_API.compare_snapshots(baseline, removed)[0]["classification"])

    def test_current_feature_boundaries_are_exact(self) -> None:
        protocol = manifest("rocketmq-protocol")["features"]
        transport = manifest("rocketmq-transport")["features"]
        admin = manifest("rocketmq-tools/rocketmq-admin/rocketmq-admin-core")["features"]
        proxy = manifest("rocketmq-proxy")["features"]

        self.assertEqual([], protocol["default"])
        self.assertEqual(["tls", "socks"], transport["default"])
        self.assertEqual([], admin["default"])
        self.assertEqual(["cluster-mode", "local-mode"], proxy["default"])
        self.assertEqual(
            {
                "default",
                "cluster-mode",
                "local-mode",
                "observability",
                "otel-traces",
                "otel-logs",
                "otlp-metrics",
                "otlp-traces",
                "otlp-logs",
                "tieredstore",
                "tls",
            },
            set(proxy),
        )

    def test_frozen_matrix_covers_all_required_profiles_and_goldens(self) -> None:
        entries = {entry.id: entry for entry in MATRIX.MATRIX}
        groups = {entry.group for entry in MATRIX.MATRIX}
        store_features = {entry.id for entry in MATRIX.MATRIX if entry.id.startswith("store-") and entry.group == "feature"}

        self.assertEqual({"feature", "wire", "storage"}, groups)
        self.assertEqual(
            {
                "store-no-default",
                "store-default",
                "store-local-file",
                "store-fast-load",
                "store-safe-load",
                "store-fast-safe-load",
                "store-io-uring",
                "store-rocksdb",
                "store-tiered",
                "store-observability",
            },
            store_features,
        )
        for required in (
            "protocol-simd",
            "transport-default-tls",
            "transport-observability",
            "admin-no-default",
            "admin-client-adapter",
            "admin-default",
            "proxy-no-default",
            "proxy-default-modes",
            "proxy-observability",
            "proxy-tiered",
            "protocol-message-codec",
            "protocol-remoting-wire-golden",
            "transport-protocol-compatibility",
            "local-cq-20-byte",
            "local-index-codec",
            "proxy-grpc-ingress",
            "store-capability-conformance",
            "store-local-components",
            "store-public-api-contract",
            "rocksdb-foundation",
            "rocksdb-semantics",
        ):
            self.assertIn(required, entries)

    def test_capability_routes_cover_every_active_v1_capability(self) -> None:
        self.assertTrue(hasattr(MATRIX, "capability_routes"), "capability route provider is missing")
        routes = MATRIX.capability_routes()
        expected = {f"F-{number:02d}" for number in range(1, 19)} | {
            f"G-{number:02d}" for number in range(1, 7)
        }

        self.assertEqual(expected, {route.capability_id for route in routes})
        self.assertEqual(len(routes), len({route.test_id for route in routes}))
        self.assertTrue(all(route.command for route in routes))

    def test_java_inventory_route_is_listed_without_a_machine_specific_path(self) -> None:
        self.assertTrue(hasattr(MATRIX, "java_inventory_route"), "Java inventory route is missing")
        route = MATRIX.java_inventory_route()

        self.assertEqual("java-55-core-inventory", route.id)
        self.assertIn("<java-root>", route.command)
        self.assertNotIn("D:\\", route.command)

    def test_freeze_contracts_and_migration_decisions_are_machine_readable(self) -> None:
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))

        self.assertEqual(
            {"F-13", "F-15", "F-18"},
            {contract["capability_id"] for contract in baseline["frozen_contracts"]},
        )
        self.assertEqual(
            {"compatible-addition", "approved-break", "renamed-wrapper", "removed-placeholder"},
            {decision["classification"] for decision in baseline["compatibility_decisions"]},
        )
        for decision in baseline["compatibility_decisions"]:
            if decision["classification"] != "compatible-addition":
                self.assertTrue(decision["approved_by"])
                self.assertTrue(decision["approved_on"])


if __name__ == "__main__":
    unittest.main()

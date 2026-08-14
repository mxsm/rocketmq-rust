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
EVIDENCE = (
    ROOT
    / "rocketmq-doc"
    / "en"
    / "architecture-public-api-compatibility.md"
)


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

        self.assertEqual(1, baseline["schema_version"])
        self.assertEqual("default", baseline["feature_profile"])
        self.assertGreater(len(targets), 0)
        self.assertEqual({package for package, _ in targets}, set(baseline["packages"]))
        self.assertEqual(40, len(baseline["source_commit"]))
        for package in baseline["packages"].values():
            self.assertGreater(package["public_path_count"], 0)
            self.assertEqual(64, len(package["public_path_sha256"]))
            self.assertEqual(64, len(package["rustdoc_json_sha256"]))

    def test_snapshot_diff_requires_classification_and_marks_removal_breaking(self) -> None:
        package = {
            "target": "demo",
            "crate_version": "1.0.0",
            "public_path_count": 1,
            "public_path_sha256": "a",
            "rustdoc_json_sha256": "b",
        }
        baseline = {
            "schema_version": 1,
            "feature_profile": "default",
            "toolchain": {"rustc": "same"},
            "packages": {"demo": package},
        }

        self.assertEqual([], PUBLIC_API.compare_snapshots(baseline, baseline))
        changed = {**baseline, "packages": {"demo": {**package, "public_path_count": 2}}}
        self.assertEqual("unclassified", PUBLIC_API.compare_snapshots(baseline, changed)[0]["classification"])
        removed = {**baseline, "packages": {}}
        self.assertEqual("breaking", PUBLIC_API.compare_snapshots(baseline, removed)[0]["classification"])

    def test_current_feature_boundaries_are_exact(self) -> None:
        protocol = manifest("rocketmq-protocol")["features"]
        transport = manifest("rocketmq-transport")["features"]
        admin = manifest("rocketmq-tools/rocketmq-admin/rocketmq-admin-core")["features"]
        proxy = manifest("rocketmq-proxy")["features"]

        self.assertEqual([], protocol["default"])
        self.assertEqual(["tls"], transport["default"])
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

    def test_evidence_records_current_snapshot_and_approved_breaking_cleanup(self) -> None:
        evidence = EVIDENCE.read_text(encoding="utf-8")

        targets = PUBLIC_API.workspace_library_targets()

        self.assertIn(f"library_targets={len(targets)}", evidence)
        self.assertIn("differences=0", evidence)
        self.assertIn("40/40", evidence)
        self.assertIn("feature=24/24", evidence)
        self.assertIn("wire=6/6", evidence)
        self.assertIn("storage=10/10", evidence)
        self.assertIn("additive groups: 4", evidence)
        self.assertIn("deprecated: 0", evidence)
        self.assertIn("breaking groups: 4", evidence)
        self.assertIn("scoped Client capabilities", evidence)
        self.assertIn("versioned Runtime diagnostics", evidence)
        self.assertIn("authorized Transport dispatch capabilities", evidence)
        self.assertIn("non-default `test-support`", evidence)
        self.assertIn("Client compatibility facade", evidence)
        self.assertIn("Store `bench_support`", evidence)
        self.assertIn("ClientRuntime::new", evidence)
        self.assertIn("ClusterConfig", evidence)
        self.assertIn("Dashboard admin operations", evidence)
        self.assertIn("MappedBuffer::read_zero_copy", evidence)
        self.assertIn("MappedBuffer::read_copy", evidence)
        self.assertIn("repository owner explicitly approved", evidence)


if __name__ == "__main__":
    unittest.main()

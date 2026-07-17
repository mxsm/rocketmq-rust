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
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TAURI = ROOT / "rocketmq-dashboard" / "rocketmq-dashboard-tauri" / "src-tauri"
WEB = ROOT / "rocketmq-dashboard" / "rocketmq-dashboard-web" / "backend"

DASHBOARDS = {
    "rocketmq-dashboard-tauri-backend": TAURI,
    "rocketmq-dashboard-web-backend": WEB,
}

FORBIDDEN_DIRECT_PACKAGES = {
    "rocketmq-client-rust",
    "rocketmq-common",
    "rocketmq-remoting",
}

FORBIDDEN_SOURCE_CRATES = {
    "rocketmq_client",
    "rocketmq_client_rust",
    "rocketmq_common",
    "rocketmq_remoting",
}

FORBIDDEN_SOURCE_SYMBOLS = {
    "ClientConfig",
    "DefaultMQAdminExt",
    "DefaultMQProducer",
    "MQAdminExt",
    "RuntimeOwner",
    "TraceDataEncoder",
    "TransactionMQProducer",
}

DASHBOARD_SOURCE_ROOTS = {
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend/",
}


def load_toml(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def dependency_tables(manifest: dict) -> list[dict]:
    tables = [
        manifest.get("dependencies", {}),
        manifest.get("dev-dependencies", {}),
        manifest.get("build-dependencies", {}),
    ]
    for target in manifest.get("target", {}).values():
        tables.extend(
            [
                target.get("dependencies", {}),
                target.get("dev-dependencies", {}),
                target.get("build-dependencies", {}),
            ]
        )
    return tables


def dependency_names(manifest: dict) -> set[str]:
    return {
        name
        for table in dependency_tables(manifest)
        for name in table
    }


def rust_sources(project: Path) -> list[Path]:
    roots = [project / "src", project / "tests", project / "examples", project / "benches"]
    return sorted(
        path
        for source_root in roots
        if source_root.is_dir()
        for path in source_root.rglob("*.rs")
    )


class DashboardClientEdgeContractTest(unittest.TestCase):
    maxDiff = None

    def test_admin_core_feature_is_the_only_client_runtime_route(self) -> None:
        for name, project in DASHBOARDS.items():
            with self.subTest(dashboard=name):
                dependencies = load_toml(project / "Cargo.toml")["dependencies"]
                self.assertIn("rocketmq-admin-core", dependencies)
                admin_core = dependencies["rocketmq-admin-core"]
                self.assertIsInstance(admin_core, dict)
                failures = []
                if admin_core.get("default-features") is not False:
                    failures.append("rocketmq-admin-core must set default-features = false")
                features = set(admin_core.get("features", []))
                if "client-adapter" not in features:
                    failures.append("rocketmq-admin-core must enable client-adapter")
                if "legacy-common-compat" in features:
                    failures.append("rocketmq-admin-core must not enable legacy-common-compat")
                self.assertEqual([], failures)

    def test_manifests_have_no_direct_client_common_or_remoting_edge(self) -> None:
        for name, project in DASHBOARDS.items():
            with self.subTest(dashboard=name):
                dependencies = dependency_names(load_toml(project / "Cargo.toml"))
                self.assertEqual(set(), dependencies & FORBIDDEN_DIRECT_PACKAGES)

    def test_tauri_no_longer_owns_a_rocketmq_runtime(self) -> None:
        dependencies = dependency_names(load_toml(TAURI / "Cargo.toml"))

        self.assertNotIn("rocketmq-runtime", dependencies)

    def test_sources_have_no_client_common_or_remoting_import(self) -> None:
        for name, project in DASHBOARDS.items():
            with self.subTest(dashboard=name):
                violations = []
                for path in rust_sources(project):
                    source = read(path)
                    for crate_name in sorted(FORBIDDEN_SOURCE_CRATES):
                        if f"{crate_name}::" in source:
                            violations.append(
                                f"{path.relative_to(ROOT).as_posix()}: {crate_name}::"
                            )
                self.assertEqual([], violations)

    def test_sources_use_no_legacy_admin_or_dashboard_owned_client_runtime(self) -> None:
        for name, project in DASHBOARDS.items():
            with self.subTest(dashboard=name):
                violations = []
                for path in rust_sources(project):
                    source = read(path)
                    for symbol in sorted(FORBIDDEN_SOURCE_SYMBOLS):
                        if symbol in source:
                            violations.append(
                                f"{path.relative_to(ROOT).as_posix()}: {symbol}"
                            )
                self.assertEqual([], violations)

    def test_tauri_page_cache_uses_dashboard_queue_key_and_admin_queue_ref(self) -> None:
        source = read(TAURI / "src" / "message" / "page_cache.rs")
        failures = []
        if "struct QueueKey" not in source:
            failures.append("page cache must define dashboard-owned QueueKey")
        if "rocketmq_admin_core::core::queue::QueueRef" not in source:
            failures.append("page cache must import admin-core QueueRef")
        if "QueueRef" not in source:
            failures.append("page cache must store QueueRef")
        if "MessageQueue" in source:
            failures.append("page cache must not store MessageQueue")
        if "rocketmq_model::" in source:
            failures.append("page cache must not import rocketmq-model")
        if "rocketmq_protocol::" in source:
            failures.append("page cache must not import rocketmq-protocol")
        self.assertEqual([], failures)

    def test_architecture_guard_keeps_dashboards_scanned_and_not_allowlisted(self) -> None:
        policy = json.loads(read(ROOT / "scripts" / "architecture-dependency-policy.json"))
        client_policy = policy["client_policy"]

        manifest_callers = {entry["caller"] for entry in client_policy["target_manifest_allowlist"]}
        source_entries = client_policy["target_source_allowlist"]
        source_callers = {entry["caller"] for entry in source_entries}
        source_prefixes = {entry["path_prefix"] for entry in source_entries}

        self.assertTrue(
            set(DASHBOARDS).isdisjoint(manifest_callers)
        )
        self.assertTrue(
            set(DASHBOARDS).isdisjoint(source_callers)
        )
        self.assertTrue(
            DASHBOARD_SOURCE_ROOTS.isdisjoint(source_prefixes)
        )

    def test_architecture_baseline_cannot_restore_dashboard_bypass_edges(self) -> None:
        baseline = json.loads(read(ROOT / "scripts" / "architecture-dependency-baseline.json"))
        manifest_exceptions = (
            baseline["manifest_exceptions"]
            + baseline["compatibility_manifest_exceptions"]
        )
        failures = [
            f"manifest exception: {item.get('caller')} -> {item.get('target')}"
            for item in manifest_exceptions
            if item.get("caller") in DASHBOARDS
        ]
        failures.extend(
            f"source exception: {item.get('path')}"
            for item in baseline["source_exceptions"]
            if any(item.get("path", "").startswith(root) for root in DASHBOARD_SOURCE_ROOTS)
        )
        self.assertEqual([], failures)


if __name__ == "__main__":
    unittest.main()

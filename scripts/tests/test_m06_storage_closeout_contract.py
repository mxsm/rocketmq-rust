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
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / "scripts" / "architecture-dependency-policy.json"
STANDALONE_MANIFESTS = (
    "rocketmq-example/Cargo.toml",
    "rocketmq-dashboard/rocketmq-dashboard-gpui/Cargo.toml",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/Cargo.toml",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend/Cargo.toml",
)
STORAGE_PACKAGES = {
    "rocketmq-store-api",
    "rocketmq-store-local",
    "rocketmq-store-rocksdb",
    "rocketmq-tieredstore",
    "rocketmq-store",
}


def load_manifest(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def normal_dependency_names(manifest: dict) -> set[str]:
    tables = [manifest.get("dependencies", {})]
    tables.extend(
        target.get("dependencies", {})
        for target in manifest.get("target", {}).values()
    )
    names: set[str] = set()
    for table in tables:
        for alias, specification in table.items():
            package = (
                specification.get("package")
                if isinstance(specification, dict)
                else None
            )
            names.add(package or alias)
    return names


def all_dependency_names(manifest: dict) -> set[str]:
    names = normal_dependency_names(manifest)
    for table_name in ("dev-dependencies", "build-dependencies"):
        table = manifest.get(table_name, {})
        for alias, specification in table.items():
            package = (
                specification.get("package")
                if isinstance(specification, dict)
                else None
            )
            names.add(package or alias)
    for target in manifest.get("target", {}).values():
        for table_name in ("dev-dependencies", "build-dependencies"):
            table = target.get(table_name, {})
            for alias, specification in table.items():
                package = (
                    specification.get("package")
                    if isinstance(specification, dict)
                    else None
                )
                names.add(package or alias)
    return names


def workspace_manifests() -> dict[str, tuple[Path, dict]]:
    root = load_manifest(ROOT / "Cargo.toml")
    manifests: dict[str, tuple[Path, dict]] = {}
    for member in root["workspace"]["members"]:
        path = ROOT / member / "Cargo.toml"
        manifest = load_manifest(path)
        manifests[manifest["package"]["name"]] = (path, manifest)
    return manifests


class StorageCloseoutContractTests(unittest.TestCase):
    def test_workspace_registration_and_package_count_are_exact(self) -> None:
        root = load_manifest(ROOT / "Cargo.toml")
        members = root["workspace"]["members"]
        policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))

        self.assertEqual(29, len(members))
        self.assertTrue(STORAGE_PACKAGES.issubset(set(members)))
        self.assertEqual(32, policy["package_counts"]["target"])
        self.assertEqual(
            {
                "rocketmq-proxy-core",
                "rocketmq-proxy-cluster",
                "rocketmq-proxy-local",
            },
            set(policy["planned_packages"]) - set(members),
        )

        dependencies = root["workspace"]["dependencies"]
        for package in (
            "rocketmq-store-api",
            "rocketmq-store-local",
            "rocketmq-store-rocksdb",
        ):
            self.assertFalse(dependencies[package]["default-features"], package)
        self.assertEqual(
            {
                "version": "1.0.0",
                "path": "./rocketmq-tieredstore",
            },
            dependencies["rocketmq-tieredstore"],
        )

    def test_storage_subgraph_matches_the_target_dag(self) -> None:
        manifests = workspace_manifests()
        actual = {
            package: normal_dependency_names(manifests[package][1])
            & STORAGE_PACKAGES
            for package in STORAGE_PACKAGES
        }
        self.assertEqual(
            {
                "rocketmq-store-api": set(),
                "rocketmq-store-local": {"rocketmq-store-api"},
                "rocketmq-store-rocksdb": {
                    "rocketmq-store-api",
                    "rocketmq-store-local",
                },
                "rocketmq-tieredstore": {"rocketmq-store-api"},
                "rocketmq-store": {
                    "rocketmq-store-api",
                    "rocketmq-store-local",
                    "rocketmq-store-rocksdb",
                    "rocketmq-tieredstore",
                },
            },
            actual,
        )

        store = manifests["rocketmq-store"][1]["dependencies"]
        self.assertEqual({"workspace": True}, store["rocketmq-store-api"])
        self.assertEqual({"workspace": True}, store["rocketmq-store-local"])
        self.assertEqual(
            {"workspace": True, "optional": True},
            store["rocketmq-store-rocksdb"],
        )
        self.assertEqual(
            {"workspace": True, "optional": True},
            store["rocketmq-tieredstore"],
        )

    def test_dependency_policy_freezes_backend_and_facade_direction(self) -> None:
        policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
        rules = {rule["id"]: rule for rule in policy["package_rules"]}

        self.assertEqual(
            ["rocketmq-store"],
            rules["store-facade-no-high-level-services"]["callers"],
        )
        self.assertEqual(
            {
                "rocketmq-client-rust",
                "rocketmq-broker",
                "rocketmq-namesrv",
                "rocketmq-controller",
                "rocketmq-proxy",
            },
            set(rules["store-facade-no-high-level-services"]["forbidden_targets"]),
        )
        self.assertEqual(
            {"rocketmq-store-api"},
            set(policy["target_dag"]["rocketmq-tieredstore"])
            & STORAGE_PACKAGES,
        )
        self.assertEqual(
            STORAGE_PACKAGES - {"rocketmq-store"},
            set(policy["target_dag"]["rocketmq-store"]),
        )

    def test_workspace_and_standalone_consumer_inventory_is_exact(self) -> None:
        manifests = workspace_manifests()
        dependencies = {
            package: normal_dependency_names(manifest)
            for package, (_, manifest) in manifests.items()
        }

        consumers = {
            target: {
                package
                for package, direct_dependencies in dependencies.items()
                if target in direct_dependencies
            }
            for target in STORAGE_PACKAGES
        }
        self.assertEqual(
            {
                "rocketmq-broker",
                "rocketmq-store",
                "rocketmq-store-local",
                "rocketmq-store-rocksdb",
                "rocketmq-tieredstore",
            },
            consumers["rocketmq-store-api"],
        )
        self.assertEqual(
            {"rocketmq-broker", "rocketmq-proxy", "rocketmq-store-inspect"},
            consumers["rocketmq-store"],
        )
        self.assertEqual(
            {"rocketmq-store", "rocketmq-store-rocksdb"},
            consumers["rocketmq-store-local"],
        )
        self.assertEqual(
            {"rocketmq-store"},
            consumers["rocketmq-store-rocksdb"],
        )
        self.assertEqual({"rocketmq-store"}, consumers["rocketmq-tieredstore"])

        for relative_path in STANDALONE_MANIFESTS:
            manifest = load_manifest(ROOT / relative_path)
            self.assertTrue(
                STORAGE_PACKAGES.isdisjoint(all_dependency_names(manifest)),
                relative_path,
            )

    def test_broker_send_processor_is_the_direct_capability_consumer(self) -> None:
        broker_manifest = load_manifest(ROOT / "rocketmq-broker/Cargo.toml")
        self.assertEqual(
            {"workspace": True},
            broker_manifest["dependencies"]["rocketmq-store-api"],
        )

        processor = (
            ROOT / "rocketmq-broker/src/processor/send_message_processor.rs"
        ).read_text(encoding="utf-8")
        production = processor.split("#[cfg(test)]", 1)[0]
        self.assertIn("use rocketmq_store_api::MessageAppender;", production)
        self.assertIn("use rocketmq_store_api::StoreHealth;", production)
        self.assertIn("S: MessageAppender<M>", production)
        self.assertIn("S: StoreHealth<Snapshot = StoreHealthSnapshot>", production)

        direct_broker_files = {
            path.relative_to(ROOT).as_posix()
            for path in (ROOT / "rocketmq-broker/src").rglob("*.rs")
            if "rocketmq_store_api" in path.read_text(encoding="utf-8")
        }
        self.assertEqual(
            {"rocketmq-broker/src/processor/send_message_processor.rs"},
            direct_broker_files,
        )


if __name__ == "__main__":
    unittest.main()

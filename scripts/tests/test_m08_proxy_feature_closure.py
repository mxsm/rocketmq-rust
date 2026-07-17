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
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FACADE = ROOT / "rocketmq-proxy"
CORE = ROOT / "rocketmq-proxy-core"
CLUSTER = ROOT / "rocketmq-proxy-cluster"
LOCAL = ROOT / "rocketmq-proxy-local"
POLICY = ROOT / "scripts" / "architecture-dependency-policy.json"
NEXT_MAJOR_FIXTURE = ROOT / "scripts" / "fixtures" / "proxy-next-major-features.toml"
EVIDENCE = (
    ROOT
    / "docs"
    / "plans"
    / "architecture-refactor-migration"
    / "phase-2-core-boundaries"
    / "08-proxy-feature-closure-evidence.md"
)


def load_toml(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def cargo_tree(package: str, *args: str, edges: str = "normal") -> set[str]:
    completed = subprocess.run(
        ["cargo", "tree", "--locked", "-p", package, "-e", edges, "--prefix", "none", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return {line.split()[0] for line in completed.stdout.splitlines() if line.strip()}


class ProxyFeatureClosureContractTests(unittest.TestCase):
    def test_r0_manifests_keep_both_adapters_non_optional_with_empty_defaults(self) -> None:
        manifests = {path.name: load_toml(path / "Cargo.toml") for path in (CORE, CLUSTER, LOCAL, FACADE)}
        for name, manifest in manifests.items():
            self.assertEqual([], manifest["features"]["default"], name)

        facade = manifests[FACADE.name]
        for adapter in ("rocketmq-proxy-cluster", "rocketmq-proxy-local"):
            self.assertIn(adapter, facade["dependencies"])
            self.assertFalse(facade["dependencies"][adapter].get("optional", False))
        for next_major_feature in ("cluster-mode", "local-mode", "compat-all-modes"):
            self.assertNotIn(next_major_feature, facade["features"])

        self.assertEqual(["rocketmq-proxy-local/tieredstore"], facade["features"]["tieredstore"])
        self.assertEqual(["rocketmq-broker/tieredstore"], manifests[LOCAL.name]["features"]["tieredstore"])

    def test_r0_normal_closures_are_separated_at_adapter_boundaries(self) -> None:
        core = cargo_tree("rocketmq-proxy-core")
        cluster = cargo_tree("rocketmq-proxy-cluster")
        local = cargo_tree("rocketmq-proxy-local")
        facade = cargo_tree("rocketmq-proxy")

        self.assertEqual(core, cargo_tree("rocketmq-proxy-core", "--no-default-features"))
        self.assertEqual(cluster, cargo_tree("rocketmq-proxy-cluster", "--no-default-features"))
        self.assertEqual(local, cargo_tree("rocketmq-proxy-local", "--no-default-features"))
        self.assertEqual(facade, cargo_tree("rocketmq-proxy", "--no-default-features"))

        self.assertEqual(
            set(),
            core
            & {
                "rocketmq-auth",
                "rocketmq-broker",
                "rocketmq-client-rust",
                "rocketmq-proxy",
                "rocketmq-proxy-cluster",
                "rocketmq-proxy-local",
                "rocketmq-store",
            },
        )
        self.assertTrue({"rocketmq-proxy-core", "rocketmq-client-rust"} <= cluster)
        self.assertEqual(
            set(),
            cluster & {"rocketmq-broker", "rocketmq-proxy", "rocketmq-proxy-local", "rocketmq-store"},
        )
        self.assertTrue({"rocketmq-proxy-core", "rocketmq-proxy-local", "rocketmq-broker"} <= local)
        self.assertEqual(set(), local & {"rocketmq-client-rust", "rocketmq-proxy", "rocketmq-proxy-cluster"})
        self.assertTrue(
            {
                "rocketmq-proxy",
                "rocketmq-proxy-core",
                "rocketmq-proxy-cluster",
                "rocketmq-proxy-local",
                "rocketmq-client-rust",
                "rocketmq-broker",
            }
            <= facade
        )

    def test_r0_tiered_and_observability_closures_are_explicit(self) -> None:
        local_tiered = cargo_tree("rocketmq-proxy-local", "--features", "tieredstore")
        facade_observability = cargo_tree("rocketmq-proxy", "--features", "observability")

        self.assertIn("rocketmq-tieredstore", local_tiered)
        self.assertEqual(set(), local_tiered & {"rocketmq-client-rust", "rocketmq-proxy-cluster"})
        self.assertTrue(
            {
                "rocketmq-observability",
                "rocketmq-proxy-core",
                "rocketmq-proxy-cluster",
                "rocketmq-proxy-local",
            }
            <= facade_observability
        )

    def test_test_and_dev_edges_are_reported_separately(self) -> None:
        for package in ("rocketmq-proxy-core", "rocketmq-proxy-cluster", "rocketmq-proxy-local", "rocketmq-proxy"):
            dev_tree = cargo_tree(package, edges="dev")
            internal = {dependency for dependency in dev_tree if dependency.startswith("rocketmq-")}
            self.assertEqual({package}, internal, package)

    def test_client_allowlist_has_no_proxy_facade_temporary_entry(self) -> None:
        policy = json.loads(POLICY.read_text(encoding="utf-8"))["client_policy"]
        manifest_callers = {entry["caller"] for entry in policy["target_manifest_allowlist"]}
        source_callers = {entry["caller"] for entry in policy["target_source_allowlist"]}
        expected = {"rocketmq-admin-core", "rocketmq-proxy-cluster", "rocketmq-example"}

        self.assertEqual(expected, manifest_callers)
        self.assertEqual(expected, source_callers)
        self.assertNotIn("rocketmq-proxy", manifest_callers)
        self.assertNotIn("rocketmq-proxy", source_callers)

    def test_next_major_feature_fixture_is_frozen_but_not_active(self) -> None:
        fixture = load_toml(NEXT_MAJOR_FIXTURE)
        self.assertEqual(1, fixture["schema_version"])
        self.assertEqual("next-major", fixture["activation_window"])
        self.assertEqual(
            {
                "default": ["compat-all-modes"],
                "cluster-mode": ["dep:rocketmq-proxy-cluster"],
                "local-mode": ["dep:rocketmq-proxy-local"],
                "compat-all-modes": ["cluster-mode", "local-mode"],
                "tieredstore": ["local-mode", "rocketmq-proxy-local/tieredstore"],
            },
            fixture["features"],
        )
        self.assertTrue(fixture["dependencies"]["rocketmq-proxy-cluster"]["optional"])
        self.assertTrue(fixture["dependencies"]["rocketmq-proxy-local"]["optional"])
        self.assertEqual(
            {"rocketmq-proxy-cluster", "rocketmq-proxy-local", "rocketmq-client-rust", "rocketmq-broker"},
            set(fixture["closure"]["no-default"]["forbidden"]),
        )

        current = load_toml(FACADE / "Cargo.toml")
        for feature in fixture["features"]:
            if feature not in {"default", "tieredstore"}:
                self.assertNotIn(feature, current["features"])
        self.assertEqual([], current["features"]["default"])
        self.assertFalse(current["dependencies"]["rocketmq-proxy-cluster"].get("optional", False))
        self.assertFalse(current["dependencies"]["rocketmq-proxy-local"].get("optional", False))

    def test_markdown_evidence_links_fixture_and_records_all_matrix_rows(self) -> None:
        evidence = EVIDENCE.read_text(encoding="utf-8")
        for row in (
            "| Core default/no-default |",
            "| Cluster default/no-default |",
            "| Local default/no-default |",
            "| Local + `tieredstore` |",
            "| Facade default/no-default |",
            "| Facade + `observability` |",
            "| Facade + `tieredstore` |",
        ):
            self.assertIn(row, evidence)
        self.assertIn("scripts/fixtures/proxy-next-major-features.toml", evidence)
        self.assertIn("workspace 2 + standalone 1", evidence)
        self.assertIn("53/82", evidence)


if __name__ == "__main__":
    unittest.main()

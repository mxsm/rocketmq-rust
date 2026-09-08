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
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GUARD = ROOT / "scripts" / "architecture_dependency_guard.py"
POLICY = ROOT / "scripts" / "architecture-dependency-policy.json"


def package(
    name: str,
    dependencies: list[dict[str, object]] | None = None,
    *,
    manifest_path: str | None = None,
) -> dict[str, object]:
    return {
        "name": name,
        "id": f"path+file:///fixture/{name}#1.0.0",
        "manifest_path": manifest_path or f"/fixture/{name}/Cargo.toml",
        "dependencies": dependencies or [],
    }


def dependency(name: str, *, kind: str | None = None, rename: str | None = None) -> dict[str, object]:
    value: dict[str, object] = {"name": name, "kind": kind, "target": None}
    if rename is not None:
        value["rename"] = rename
    return value


def metadata(packages: list[dict[str, object]]) -> dict[str, object]:
    return {
        "packages": packages,
        "workspace_members": [item["id"] for item in packages],
        "workspace_root": "/fixture",
        "version": 1,
    }


class ArchitectureDependencyGuardTests(unittest.TestCase):
    maxDiff = None

    def run_guard(
        self,
        fixture: dict[str, object],
        *,
        source_files: dict[str, str] | None = None,
        policy_override: dict[str, object] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata_file = root / "metadata.json"
            metadata_file.write_text(json.dumps(fixture), encoding="utf-8")
            for relative, content in (source_files or {}).items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            policy_path = POLICY
            if policy_override is not None:
                policy_path = root / "policy.json"
                policy_path.write_text(json.dumps(policy_override), encoding="utf-8")
            return subprocess.run(
                [sys.executable, str(GUARD), "--scope", "all", "--policy", str(policy_path),
                 "--metadata-file", str(metadata_file), "--source-root", str(root)],
                cwd=ROOT, capture_output=True, text=True, encoding="utf-8", check=False,
            )

    def assert_rule(self, result: subprocess.CompletedProcess[str], rule: str) -> None:
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(f"rule={rule}", result.stdout)
        self.assertIn("caller=", result.stdout)
        self.assertIn("target=", result.stdout)
        self.assertIn("path=", result.stdout)
        self.assertIn("kind=", result.stdout)

    def test_cycle_is_rejected(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-model", [dependency("rocketmq-protocol")]),
                package("rocketmq-protocol", [dependency("rocketmq-model")]),
            ]
        )
        self.assert_rule(self.run_guard(fixture), "dependency-cycle")

    def test_protocol_must_not_depend_on_transport(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-protocol", [dependency("rocketmq-transport")]),
                package("rocketmq-transport"),
            ]
        )
        self.assert_rule(self.run_guard(fixture), "protocol-no-transport")

    def test_store_api_must_not_depend_on_backend(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-store-api", [dependency("rocketmq-store-local", kind="build")]),
                package("rocketmq-store-local"),
            ]
        )
        self.assert_rule(self.run_guard(fixture), "store-api-no-backend")

    def test_store_api_must_not_depend_on_runtime_or_observability(self) -> None:
        fixture = metadata(
            [
                package(
                    "rocketmq-store-api",
                    [dependency("rocketmq-runtime"), dependency("rocketmq-observability", kind="dev")],
                ),
                package("rocketmq-runtime"),
                package("rocketmq-observability"),
            ]
        )
        result = self.run_guard(fixture)
        self.assertEqual(1, result.returncode)
        self.assertIn("rule=store-api-runtime-neutral", result.stdout)
        self.assertIn("target=rocketmq-runtime", result.stdout)
        self.assertIn("target=rocketmq-observability", result.stdout)

    def test_proxy_local_must_not_depend_on_client(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-proxy-local", [dependency("rocketmq-client-rust", kind="dev")]),
                package("rocketmq-client-rust"),
            ]
        )
        self.assert_rule(self.run_guard(fixture), "proxy-local-no-client")

    def test_foundation_must_not_depend_on_facade(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-model", [dependency("rocketmq-store")]),
                package("rocketmq-store"),
            ]
        )
        self.assert_rule(self.run_guard(fixture), "foundation-no-facade")

    def test_security_api_must_not_depend_on_protocol(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-security-api", [dependency("rocketmq-protocol")]),
                package("rocketmq-protocol"),
            ]
        )
        self.assert_rule(self.run_guard(fixture), "security-api-contract-only")

    def test_foundation_crates_reject_runtime_framework_dependencies(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-protocol", [dependency("tokio")]),
                package("rocketmq-model"),
            ]
        )
        self.assert_rule(self.run_guard(fixture), "foundation-no-runtime-framework")

    def test_runtime_and_error_must_not_depend_on_business_crates(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-runtime", [dependency("rocketmq-model")]),
                package("rocketmq-error", [dependency("rocketmq-protocol", kind="dev")]),
                package("rocketmq-model"),
                package("rocketmq-protocol"),
            ]
        )
        result = self.run_guard(fixture)
        self.assertEqual(1, result.returncode)
        self.assertIn("rule=foundation-runtime-error-leaf", result.stdout)

    def test_store_backend_direction_and_facade_rule(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-store-local", [dependency("rocketmq-store")]),
                package("rocketmq-store-rocksdb", [dependency("rocketmq-tieredstore")]),
                package("rocketmq-tieredstore", [dependency("rocketmq-store")]),
                package("rocketmq-store"),
            ]
        )
        result = self.run_guard(fixture)
        self.assertEqual(1, result.returncode)
        self.assertIn("rule=facade-reverse-dependency", result.stdout)
        self.assertIn("rule=store-backend-direction", result.stdout)

    def test_transport_cannot_reach_facade_transitively(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-transport", [dependency("rocketmq-observability")]),
                package("rocketmq-observability", [dependency("rocketmq-store")]),
                package("rocketmq-store"),
            ]
        )
        self.assert_rule(self.run_guard(fixture), "transitive-forbidden-reachability")

    def test_dev_edges_do_not_contaminate_normal_closure(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-transport", [dependency("rocketmq-observability")]),
                package("rocketmq-observability", [dependency("rocketmq-store", kind="dev")]),
                package("rocketmq-store"),
            ]
        )
        result = self.run_guard(fixture)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_transport_must_not_depend_on_high_level_service(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-transport", [dependency("rocketmq-broker", kind="build")]),
                package("rocketmq-broker"),
            ]
        )
        self.assert_rule(self.run_guard(fixture), "transport-no-high-level")

    def test_forbidden_consumers_cannot_reexport_client_through_a_transitive_edge(self) -> None:
        forbidden_callers = [
            "rocketmq-broker",
            "rocketmq-namesrv",
            "rocketmq-proxy-core",
            "rocketmq-proxy-local",
        ]
        fixture = metadata(
            [package(caller, [dependency("rocketmq-admin-core")]) for caller in forbidden_callers]
            + [
                package("rocketmq-admin-core", [dependency("rocketmq-client-rust")]),
                package("rocketmq-client-rust"),
            ]
        )
        result = self.run_guard(fixture)
        findings = [
            line
            for line in result.stdout.splitlines()
            if "rule=transitive-forbidden-reachability" in line
            and "target=rocketmq-client-rust" in line
        ]

        self.assertEqual(len(forbidden_callers), len(findings), result.stdout)
        for caller in forbidden_callers:
            self.assertTrue(any(f"caller={caller}" in line for line in findings), result.stdout)

    def test_standalone_workspace_inherited_rename_resolves_for_all_kinds(self) -> None:
        policy = json.loads(POLICY.read_text(encoding="utf-8"))
        policy["package_rules"].append({
            "id": "fixture-no-store", "callers": ["rocketmq-example"],
            "forbidden_targets": ["rocketmq-store"],
        })
        result = self.run_guard(
            metadata([package("rocketmq-store")]),
            policy_override=policy,
            source_files={
                "Cargo.toml": """
[workspace]
members = []
[workspace.dependencies]
legacy-normal = { package = "rocketmq-store", path = "rocketmq-store" }
legacy-build = { package = "rocketmq-store", path = "rocketmq-store" }
legacy-dev = { package = "rocketmq-store", path = "rocketmq-store" }
""",
                "rocketmq-example/Cargo.toml": """
[package]
name = "rocketmq-example"
version = "0.1.0"
[dependencies]
legacy-normal = { workspace = true }
[build-dependencies]
legacy-build = { workspace = true }
[dev-dependencies]
legacy-dev = { workspace = true }
""",
            },
        )
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        findings = [
            line
            for line in result.stdout.splitlines()
            if "rule=fixture-no-store" in line
        ]
        self.assertEqual(3, len(findings), result.stdout)
        for kind in ("normal", "build", "dev"):
            self.assertTrue(
                any(f"kind={kind}" in line for line in findings),
                result.stdout,
            )
        self.assertTrue(all("target=rocketmq-store" in line for line in findings), result.stdout)

    def test_missing_workspace_inherited_dependency_is_input_error(self) -> None:
        result = self.run_guard(
            metadata([package("rocketmq-store")]),
            source_files={
                "Cargo.toml": "[workspace]\nmembers = []\n[workspace.dependencies]\n",
                "rocketmq-example/Cargo.toml": """
[package]
name = "rocketmq-example"
version = "0.1.0"
[dependencies]
missing-alias = { workspace = true }
""",
            },
        )
        self.assertEqual(2, result.returncode)
        self.assertIn("workspace dependency missing-alias", result.stderr)

    def test_distinct_directed_cycles_are_not_collapsed_by_node_set(self) -> None:
        fixture = metadata(
            [
                package(
                    "rocketmq-model",
                    [dependency("rocketmq-protocol"), dependency("rocketmq-security-api")],
                ),
                package(
                    "rocketmq-protocol",
                    [dependency("rocketmq-model"), dependency("rocketmq-security-api")],
                ),
                package(
                    "rocketmq-security-api",
                    [dependency("rocketmq-model"), dependency("rocketmq-protocol")],
                ),
            ]
        )
        result = self.run_guard(fixture)
        cycles = [line for line in result.stdout.splitlines() if "rule=dependency-cycle" in line]
        self.assertGreaterEqual(len(cycles), 2)

    def test_allowed_dependency_changes_do_not_need_a_snapshot(self) -> None:
        for dependencies in ([], [dependency("serde")], [dependency("serde", rename="serialization")]):
            with self.subTest(dependencies=dependencies):
                result = self.run_guard(metadata([package("new-tool", dependencies)]))
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_dev_cycle_is_a_valid_test_seam(self) -> None:
        fixture = metadata([package("a", [dependency("b")]), package("b", [dependency("a", kind="dev")])])
        result = self.run_guard(fixture)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()

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

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GUARD = ROOT / "scripts" / "architecture_dependency_guard.py"
POLICY = ROOT / "scripts" / "architecture-dependency-policy.json"
BASELINE = ROOT / "scripts" / "architecture-dependency-baseline.json"


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
        mode: str = "target",
        source_files: dict[str, str] | None = None,
        allow_missing: bool = True,
        policy_override: dict[str, object] | None = None,
        baseline_override: dict[str, object] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            metadata_file = temp / "metadata.json"
            metadata_file.write_text(json.dumps(fixture), encoding="utf-8")
            source_root = temp / "source"
            source_root.mkdir()
            for relative, content in (source_files or {}).items():
                path = source_root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")

            policy_path = POLICY
            if policy_override is not None:
                policy_path = temp / "policy.json"
                policy_path.write_text(json.dumps(policy_override), encoding="utf-8")
            baseline_path = BASELINE
            if baseline_override is not None:
                baseline_path = temp / "baseline.json"
                baseline_path.write_text(json.dumps(baseline_override), encoding="utf-8")

            command = [
                sys.executable,
                str(GUARD),
                "--mode",
                mode,
                "--policy",
                str(policy_path),
                "--baseline",
                str(baseline_path),
                "--metadata-file",
                str(metadata_file),
                "--source-root",
                str(source_root),
            ]
            if allow_missing:
                command.append("--allow-missing-planned-crates")
            return subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)

    def assert_rule(self, result: subprocess.CompletedProcess[str], rule: str) -> None:
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(f"rule={rule}", result.stdout)
        self.assertIn("caller=", result.stdout)
        self.assertIn("target=", result.stdout)
        self.assertIn("path=", result.stdout)
        self.assertIn("kind=", result.stdout)

    def baseline_packages(self, extra: list[dict[str, object]] | None = None) -> list[dict[str, object]]:
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
        values = [package(name) for name in baseline["workspace_packages"]]
        values.extend(extra or [])
        return values

    def test_clean_target_fixture_reports_incomplete_and_fails(self) -> None:
        result = self.run_guard(metadata([package("rocketmq-model"), package("rocketmq-protocol")]))
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn("TARGET_INCOMPLETE", result.stdout)
        self.assertNotIn("ARCHITECTURE_DEPENDENCY_GUARD_OK", result.stdout)

    def test_target_incomplete_json_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            metadata_file = temp / "metadata.json"
            output_file = temp / "result.json"
            source_root = temp / "source"
            source_root.mkdir()
            metadata_file.write_text(json.dumps(metadata([package("rocketmq-model")])), encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    str(GUARD),
                    "--mode",
                    "target",
                    "--policy",
                    str(POLICY),
                    "--baseline",
                    str(BASELINE),
                    "--metadata-file",
                    str(metadata_file),
                    "--source-root",
                    str(source_root),
                    "--output",
                    str(output_file),
                    "--allow-missing-planned-crates",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(1, result.returncode)
            self.assertEqual("incomplete", json.loads(output_file.read_text(encoding="utf-8"))["status"])

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
                package("rocketmq-model", [dependency("rocketmq-remoting")]),
                package("rocketmq-remoting"),
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
                package("rocketmq-observability", [dependency("rocketmq-common")]),
                package("rocketmq-common"),
            ]
        )
        self.assert_rule(self.run_guard(fixture), "transitive-forbidden-reachability")

    def test_dev_edges_do_not_contaminate_normal_closure(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-transport", [dependency("rocketmq-observability")]),
                package("rocketmq-observability", [dependency("rocketmq-common", kind="dev")]),
                package("rocketmq-common"),
            ]
        )
        result = self.run_guard(fixture)
        self.assertEqual(1, result.returncode)
        self.assertNotIn("rule=transitive-forbidden-reachability", result.stdout)
        self.assertIn("kind=dev", result.stdout)

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
            "rocketmq-common",
            "rocketmq-remoting",
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

    def test_client_manifest_allowlist_matches_the_full_edge_identity_once(self) -> None:
        manifest = "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/Cargo.toml"
        valid = metadata(
            [
                package(
                    "rocketmq-admin-core",
                    [dependency("rocketmq-client-rust")],
                    manifest_path=manifest,
                ),
                package("rocketmq-client-rust"),
            ]
        )
        self.assertNotIn("rule=client-manifest-allowlist", self.run_guard(valid).stdout)

        invalid_cases = {
            "wrong kind": package(
                "rocketmq-admin-core",
                [dependency("rocketmq-client-rust", kind="dev")],
                manifest_path=manifest,
            ),
            "renamed alias": package(
                "rocketmq-admin-core",
                [dependency("rocketmq-client-rust", rename="renamed_client")],
                manifest_path=manifest,
            ),
            "wrong path": package(
                "rocketmq-admin-core",
                [dependency("rocketmq-client-rust")],
                manifest_path="other/Cargo.toml",
            ),
            "duplicate edge": package(
                "rocketmq-admin-core",
                [dependency("rocketmq-client-rust"), dependency("rocketmq-client-rust")],
                manifest_path=manifest,
            ),
        }
        for label, caller in invalid_cases.items():
            with self.subTest(label=label):
                result = self.run_guard(metadata([caller, package("rocketmq-client-rust")]))
                self.assert_rule(result, "client-manifest-allowlist")

    def test_client_source_allowlist_matches_caller_prefix_and_alias(self) -> None:
        manifest = "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/Cargo.toml"
        fixture = metadata(
            [
                package(
                    "rocketmq-admin-core",
                    [dependency("rocketmq-client-rust")],
                    manifest_path=manifest,
                ),
                package("rocketmq-client-rust"),
            ]
        )
        allowed = self.run_guard(
            fixture,
            source_files={
                "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/client_adapter/ok.rs": (
                    "use rocketmq_client_rust::ClientConfig;\n"
                )
            },
        )
        self.assertNotIn("rule=client-source-allowlist", allowed.stdout)

        outside = self.run_guard(
            fixture,
            source_files={
                "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/outside.rs": (
                    "use rocketmq_client_rust::ClientConfig;\n"
                )
            },
        )
        self.assert_rule(outside, "client-source-allowlist")

        renamed_fixture = metadata(
            [
                package(
                    "rocketmq-admin-core",
                    [dependency("rocketmq-client-rust", rename="renamed_client")],
                    manifest_path=manifest,
                ),
                package("rocketmq-client-rust"),
            ]
        )
        renamed = self.run_guard(
            renamed_fixture,
            source_files={
                "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/client_adapter/renamed.rs": (
                    "use renamed_client::ClientConfig;\n"
                )
            },
        )
        self.assert_rule(renamed, "client-source-allowlist")

    def test_client_policy_schema_rejects_broadened_entries(self) -> None:
        fixture = metadata([package("rocketmq-model")])
        policy = json.loads(POLICY.read_text(encoding="utf-8"))
        invalid_policies = []

        extra_key = copy.deepcopy(policy)
        extra_key["client_policy"]["unexpected"] = True
        invalid_policies.append(("extra key", extra_key))

        unknown_alias = copy.deepcopy(policy)
        unknown_alias["client_policy"]["target_source_allowlist"][0]["aliases"] = [
            "renamed_client"
        ]
        invalid_policies.append(("unknown alias", unknown_alias))

        duplicate_identity = copy.deepcopy(policy)
        duplicate_identity["client_policy"]["target_manifest_allowlist"].append(
            copy.deepcopy(duplicate_identity["client_policy"]["target_manifest_allowlist"][0])
        )
        invalid_policies.append(("duplicate identity", duplicate_identity))

        for label, invalid_policy in invalid_policies:
            with self.subTest(label=label):
                result = self.run_guard(fixture, policy_override=invalid_policy)
                self.assertEqual(2, result.returncode, result.stdout + result.stderr)
                self.assertIn("policy client", result.stderr)

    def test_client_ledger_schema_rejects_broadened_proxy_debt(self) -> None:
        fixture = metadata([package("rocketmq-model")])
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
        invalid_baselines = []

        wrong_owner = copy.deepcopy(baseline)
        wrong_owner["source_exceptions"][0]["owner"] = "other"
        invalid_baselines.append(("wrong owner", wrong_owner))

        broadened_path = copy.deepcopy(baseline)
        broadened_path["source_exceptions"][0]["path"] = "rocketmq-proxy/src/other.rs"
        invalid_baselines.append(("broadened path", broadened_path))

        broadened_count = copy.deepcopy(baseline)
        broadened_count["source_exceptions"][0]["count"] = 13
        invalid_baselines.append(("broadened count", broadened_count))

        wrong_milestone = copy.deepcopy(baseline)
        wrong_milestone["manifest_exceptions"][-1]["remove_by"] = "M09"
        invalid_baselines.append(("wrong milestone", wrong_milestone))

        for label, invalid_baseline in invalid_baselines:
            with self.subTest(label=label):
                result = self.run_guard(fixture, baseline_override=invalid_baseline)
                self.assertEqual(2, result.returncode, result.stdout + result.stderr)
                self.assertIn("temporary Client", result.stderr)

    def test_client_source_alias_is_detected(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-broker", [dependency("rocketmq-client-rust", rename="mq_client")]),
                package("rocketmq-client-rust"),
            ]
        )
        result = self.run_guard(
            fixture,
            source_files={"rocketmq-broker/src/lib.rs": "use mq_client::producer::DefaultMQProducer;\n"},
        )
        self.assert_rule(result, "client-source-allowlist")
        self.assertIn("mq_client", result.stdout)

    def test_client_alias_is_scoped_to_its_manifest_caller(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-broker", [dependency("rocketmq-client-rust", rename="mq_client")]),
                package("rocketmq-model"),
                package("rocketmq-client-rust"),
            ]
        )
        result = self.run_guard(
            fixture,
            source_files={
                "rocketmq-model/src/lib.rs": "mod mq_client;\nuse mq_client::LocalValue;\n",
                "rocketmq-broker/src/lib.rs": (
                    "// use mq_client::producer::Producer;\n"
                    "/* use mq_client::consumer::Consumer; */\n"
                ),
            },
        )
        self.assertEqual(1, result.returncode)
        self.assertNotIn("rule=client-source-allowlist", result.stdout)

    def test_rust_lexer_ignores_literals_and_keeps_real_import(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-broker", [dependency("rocketmq-client-rust")]),
                package("rocketmq-client-rust"),
            ]
        )
        source = r'''
const A: &str = "rocketmq_client_rust::fake /*";
const B: &[u8] = b"rocketmq_client_rust::fake";
const C: &str = r#"rocketmq_client_rust::fake"#;
const D: &[u8] = br##"rocketmq_client_rust::fake"##;
const CH: char = '/';
fn lifetime<'a>(value: &'a str) -> &'a str { value }
/* outer /* nested rocketmq_client_rust::fake */ still comment */
use rocketmq_client_rust::producer::Producer;
'''
        result = self.run_guard(
            fixture,
            source_files={"rocketmq-broker/src/lib.rs": source},
        )
        source_findings = [
            line for line in result.stdout.splitlines() if "rule=client-source-allowlist" in line
        ]
        self.assertEqual(1, len(source_findings), result.stdout)

    def test_unclosed_rust_lexical_construct_fails_closed(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-broker", [dependency("rocketmq-client-rust")]),
                package("rocketmq-client-rust"),
            ]
        )
        result = self.run_guard(
            fixture,
            source_files={
                "rocketmq-broker/src/lib.rs": (
                    'const BAD: &str = "rocketmq_client_rust::unterminated;\n'
                )
            },
        )
        self.assertEqual(2, result.returncode)
        self.assertIn("unterminated Rust", result.stderr)

    def test_baseline_rejects_new_import_in_existing_consumer_directory(self) -> None:
        fixture_packages = [item for item in self.baseline_packages() if item["name"] != "rocketmq-broker"]
        fixture_packages.append(
            package(
                "rocketmq-broker",
                [dependency("rocketmq-client-rust")],
                manifest_path="rocketmq-broker/Cargo.toml",
            )
        )
        result = self.run_guard(
            metadata(fixture_packages),
            mode="baseline",
            source_files={"rocketmq-broker/src/new_dependency.rs": "use rocketmq_client_rust::ClientConfig;\n"},
        )
        self.assert_rule(result, "client-source-baseline-growth")

    def test_baseline_rejects_source_count_growth_move_and_alias_change(self) -> None:
        values = [item for item in self.baseline_packages() if item["name"] != "rocketmq-broker"]
        values.append(
            package(
                "rocketmq-broker",
                [dependency("rocketmq-client-rust")],
                manifest_path="rocketmq-broker/Cargo.toml",
            )
        )
        growth = self.run_guard(
            metadata(values),
            mode="baseline",
            source_files={
                "rocketmq-broker/src/broker_runtime.rs": (
                    "use rocketmq_client_rust::One;\nuse rocketmq_client_rust::Two;\n"
                )
            },
        )
        self.assert_rule(growth, "client-source-baseline-growth")

        renamed_values = [item for item in self.baseline_packages() if item["name"] != "rocketmq-broker"]
        renamed_values.append(
            package(
                "rocketmq-broker",
                [dependency("rocketmq-client-rust", rename="renamed_client")],
                manifest_path="rocketmq-broker/Cargo.toml",
            )
        )
        renamed = self.run_guard(
            metadata(renamed_values),
            mode="baseline",
            source_files={"rocketmq-broker/src/moved.rs": "use renamed_client::Client;\n"},
        )
        self.assertEqual(1, renamed.returncode)
        self.assertIn("rule=client-manifest-baseline-growth", renamed.stdout)
        self.assertIn("rule=client-source-baseline-growth", renamed.stdout)

    def test_baseline_rejects_second_or_renamed_manifest_edge(self) -> None:
        values = self.baseline_packages()
        values = [item for item in values if item["name"] != "rocketmq-broker"]
        values.append(
            package(
                "rocketmq-broker",
                [
                    dependency("rocketmq-client-rust"),
                    dependency("rocketmq-client-rust", kind="dev", rename="extra_client"),
                ],
                manifest_path="rocketmq-broker/Cargo.toml",
            )
        )
        self.assert_rule(self.run_guard(metadata(values), mode="baseline"), "client-manifest-baseline-growth")

    def test_compatibility_manifest_growth_fails_and_removal_passes(self) -> None:
        reduced = self.run_guard(metadata(self.baseline_packages()), mode="baseline")
        self.assertEqual(0, reduced.returncode, reduced.stdout + reduced.stderr)

        values = [item for item in self.baseline_packages() if item["name"] != "rocketmq-broker"]
        values.append(
            package(
                "rocketmq-broker",
                [
                    dependency("rocketmq-common"),
                    dependency("rocketmq-common", kind="dev", rename="common_test"),
                ],
                manifest_path="rocketmq-broker/Cargo.toml",
            )
        )
        self.assert_rule(
            self.run_guard(metadata(values), mode="baseline"),
            "compatibility-manifest-baseline-growth",
        )

    def test_standalone_compatibility_growth_rename_and_kind_fail(self) -> None:
        result = self.run_guard(
            metadata(self.baseline_packages()),
            mode="baseline",
            source_files={
                "rocketmq-example/Cargo.toml": """
[package]
name = "rocketmq-example"
version = "0.1.0"
[dev-dependencies]
rocketmq-common = { path = "../rocketmq-common" }
[build-dependencies]
renamed-common = { package = "rocketmq-common", path = "../rocketmq-common" }
""",
            },
        )
        self.assert_rule(result, "compatibility-manifest-baseline-growth")
        self.assertIn("kind=build", result.stdout)
        self.assertIn("alias=renamed_common", result.stdout)

    def test_standalone_workspace_inherited_rename_resolves_for_all_kinds(self) -> None:
        result = self.run_guard(
            metadata(self.baseline_packages()),
            mode="baseline",
            source_files={
                "Cargo.toml": """
[workspace]
members = []
[workspace.dependencies]
legacy-normal = { package = "rocketmq-common", path = "rocketmq-common" }
legacy-build = { package = "rocketmq-common", path = "rocketmq-common" }
legacy-dev = { package = "rocketmq-common", path = "rocketmq-common" }
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
            if "rule=compatibility-manifest-baseline-growth" in line
        ]
        self.assertEqual(3, len(findings), result.stdout)
        for kind, alias in (
            ("normal", "legacy_normal"),
            ("build", "legacy_build"),
            ("dev", "legacy_dev"),
        ):
            self.assertTrue(
                any(f"kind={kind}" in line and f"alias={alias}" in line for line in findings),
                result.stdout,
            )
        self.assertTrue(all("target=rocketmq-common" in line for line in findings), result.stdout)

    def test_missing_workspace_inherited_dependency_is_input_error(self) -> None:
        result = self.run_guard(
            metadata(self.baseline_packages()),
            mode="baseline",
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

    def test_baseline_allows_planned_package_but_rejects_unknown_package(self) -> None:
        planned = self.run_guard(
            metadata(self.baseline_packages([package("rocketmq-protocol")])),
            mode="baseline",
        )
        self.assertEqual(0, planned.returncode, planned.stdout + planned.stderr)
        unknown = self.run_guard(
            metadata(self.baseline_packages([package("rocketmq-unplanned")])),
            mode="baseline",
        )
        self.assertEqual(2, unknown.returncode)
        self.assertIn("unplanned workspace packages", unknown.stderr)

    def test_baseline_enforces_target_rules_on_added_planned_package(self) -> None:
        values = self.baseline_packages(
            [package("rocketmq-protocol", [dependency("rocketmq-common")])]
        )
        self.assert_rule(self.run_guard(metadata(values), mode="baseline"), "foundation-no-facade")

    def test_all_standalone_cargo_roots_are_scanned(self) -> None:
        fixture = metadata([package("rocketmq-model"), package("rocketmq-client-rust")])
        result = self.run_guard(
            fixture,
            source_files={
                "rocketmq-dashboard/rocketmq-dashboard-gpui/Cargo.toml": """
[package]
name = "fixture-gpui"
version = "0.1.0"
[dependencies]
rocketmq-client-rust = { path = "../../rocketmq-client" }
""",
            },
        )
        self.assert_rule(result, "client-manifest-allowlist")

    def test_distinct_directed_cycles_are_not_collapsed_by_node_set(self) -> None:
        fixture = metadata(
            [
                package("a", [dependency("b"), dependency("c")]),
                package("b", [dependency("a"), dependency("c")]),
                package("c", [dependency("a"), dependency("b")]),
            ]
        )
        result = self.run_guard(fixture)
        cycles = [line for line in result.stdout.splitlines() if "rule=dependency-cycle" in line]
        self.assertGreaterEqual(len(cycles), 2)

    def test_baseline_reproducibility_schema_is_complete(self) -> None:
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
        for key in (
            "cargo_metadata_command",
            "metadata_sha256",
            "rustc_version",
            "cargo_version",
            "generated_output_path",
        ):
            self.assertIn(key, baseline)

    def test_policy_encodes_all_32_target_packages(self) -> None:
        policy = json.loads(POLICY.read_text(encoding="utf-8"))
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
        expected = set(baseline["workspace_packages"]) | set(policy["planned_packages"])
        self.assertEqual(expected, set(policy["target_dag"]))
        self.assertEqual(32, len(policy["target_dag"]))

    def test_target_dag_rejects_edge_without_explicit_package_rule(self) -> None:
        fixture = metadata(
            [
                package("rocketmq-proxy-cluster", [dependency("rocketmq-common")]),
                package("rocketmq-common"),
            ]
        )
        self.assert_rule(self.run_guard(fixture), "target-dag-direct-dependency")

    def test_missing_planned_crates_without_override_is_input_error(self) -> None:
        result = self.run_guard(metadata([package("rocketmq-model")]), allow_missing=False)
        self.assertEqual(2, result.returncode, result.stdout + result.stderr)
        self.assertIn("missing planned packages", result.stderr)

    def test_unknown_mode_is_input_error(self) -> None:
        result = subprocess.run(
            [sys.executable, str(GUARD), "--mode", "unknown"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(2, result.returncode)


if __name__ == "__main__":
    unittest.main()

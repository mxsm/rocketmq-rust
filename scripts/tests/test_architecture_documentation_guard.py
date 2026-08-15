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

import tempfile
import json
import subprocess
import sys
import unittest
from pathlib import Path

from scripts import architecture_documentation_guard as guard


class ArchitectureDocumentationGuardTest(unittest.TestCase):
    def test_live_semantic_core_mode_records_paths_sections_links_and_commands(self) -> None:
        root = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "documentation.json"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(root / "scripts/architecture_documentation_guard.py"),
                    "--mode",
                    "semantic",
                    "--scope",
                    "core-release",
                    "--output",
                    str(output),
                ],
                cwd=root,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            payload = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(("core-release", "semantic", "compliant"), (payload["scope"], payload["mode"], payload["status"]))
        self.assertTrue(payload["documents"])
        self.assertNotIn("sha256", json.dumps(payload).lower())
        for record in payload["documents"]:
            self.assertEqual({"path", "sections", "link_targets", "commands"}, set(record))

    def test_documentation_workflow_provisions_property_suite_native_dependencies(self) -> None:
        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github/workflows/architecture-documentation.yml").read_text(encoding="utf-8")

        self.assertIn("Install native build dependencies", workflow)
        self.assertIn("clang llvm libclang-dev", workflow)
        self.assertIn("protobuf-compiler", workflow)

    def test_observability_cache_key_is_normalized_for_feature_lists(self) -> None:
        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github/workflows/rocketmq-rust-ci.yaml").read_text(encoding="utf-8")

        self.assertIn("id: observability-cache-key", workflow)
        self.assertIn('echo "value=${FEATURES//,/-}"', workflow)
        self.assertIn("steps.observability-cache-key.outputs.value", workflow)

    def test_standalone_workspace_expands_members_and_inherited_local_edges(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = {
                "Cargo.toml": """
[workspace]
members = ["shared"]
[workspace.package]
rust-version = "1.95.0"
""",
                "shared/Cargo.toml": """
[package]
name = "shared"
version = "0.1.0"
""",
                "standalone/Cargo.toml": """
[workspace]
members = ["crates/app"]
[workspace.package]
rust-version = "1.95.0"
[workspace.dependencies]
shared = { path = "../shared" }
tokio = { version = "1", features = ["macros"] }
""",
                "standalone/crates/app/Cargo.toml": """
[package]
name = "standalone-app"
version = "0.1.0"
[dependencies]
shared = { workspace = true }
tokio = { workspace = true, features = ["sync"] }
""",
            }
            for relative, source in files.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(source, encoding="utf-8")
            policy = {"standalone": [{"manifest": "standalone/Cargo.toml"}]}
            root_manifest = guard.load_toml(root / "Cargo.toml")

            paths = guard.manifest_paths(root, root_manifest, policy)
            edges = guard.collect_local_edges(root, policy)
            tokio = guard.collect_tokio(root, paths, root_manifest)

        self.assertIn(root / "standalone/crates/app/Cargo.toml", paths)
        self.assertEqual(
            (guard.LocalEdge("standalone-app", "shared", "shared"),),
            edges,
        )
        member_tokio = next(item for item in tokio if item.manifest == "standalone/crates/app/Cargo.toml")
        self.assertEqual(("macros", "sync"), member_tokio.features)
        self.assertTrue(member_tokio.inherited)

    def test_tokio_full_is_rejected_outside_application_allowlist(self) -> None:
        facts = guard.Facts(
            formal_toolchain="1.95.0",
            root_packages=(),
            standalone=(),
            node_projects=(),
            tokio=(
                guard.TokioDeclaration(
                    manifest="library/Cargo.toml",
                    dependency="tokio",
                    features=("full",),
                    inherited=False,
                ),
            ),
            local_edges=(),
            evidence_artifacts=(),
        )

        findings = guard.validate_tokio({"tokio_full_allowed": []}, facts)

        self.assertEqual(["tokio-full"], [finding.code for finding in findings])

    def test_retired_symbols_and_runtime_escapes_are_detected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = {
                "rocketmq-client/src/producer/default_mq_producer.rs": "borrowed_generations",
                "rocketmq-client/src/admin/legacy.rs": "pub trait MQAdminExtInner {}",
                "rocketmq-store/src/base.rs": "pub trait MessageStoreInner {}",
                "rocketmq-runtime/src/lib.rs": "pub use handle::RuntimeHandle;",
                "rocketmq-runtime/src/task_group.rs": "pub fn root() {}\nfn spawn_detached() {}",
            }
            for relative, source in files.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(source, encoding="utf-8")

            findings = guard.validate_compatibility(root)

        codes = {finding.code for finding in findings}
        self.assertIn("stable-config-history", codes)
        self.assertIn("retired-symbol-present", codes)
        self.assertIn("runtime-handle-public", codes)
        self.assertIn("runtime-task-escape", codes)

    def test_generated_document_is_deterministic(self) -> None:
        policy = {
            "root": {"commands": []},
            "implementation_baseline": {
                "id": "architecture-implementation-2026-07-29-v1",
                "generator": "scripts/architecture_implementation_baseline.py",
                "output": "target/architecture-optimization/baseline/current/baseline.json",
                "historical_review_commit": "8e01ee9ac0bfbd14528939160cd7c2b2fb6d01e4",
                "planning_snapshot_commit": "071fb7dfc835f828a79eabdfca1225c14123a093",
                "historical_difference": "Historical facts are separate.",
                "commands": ["cargo metadata --format-version 1 --no-deps"],
                "required_evidence": ["target/baseline.json"],
            },
            "candidate_record": {
                "commit": "d88a973131ce4f57d01a65def8ecb7944a45ba21",
                "markdown": "rocketmq-doc/en/architecture-candidates/2026-08-01-d88a97313.md",
                "json": "rocketmq-doc/en/architecture-candidates/2026-08-01-d88a97313.json",
                "code_system_score": "93.5 / 100",
                "production_certified": False,
            },
            "python_tests": {
                "ci": {"guards": "guards", "contracts": "contracts"},
                "entries": [],
            },
            "coverage": {"patch_target": "70%"},
            "evidence_governance": {
                "risk_matrix": "risk.json",
                "property_registry": "property.json",
                "fuzz_registry": "fuzz.json",
                "guard": "guard.py",
            },
            "documentation_contracts": {
                "core_capabilities": "core.md",
                "acknowledgement_adr": "ack.md",
                "regional_dr_adr": "dr.md",
                "acknowledgement_evidence_schema": "ack.json",
                "missing_docs_crates": [],
            },
        }
        facts = guard.Facts(
            formal_toolchain="1.95.0",
            root_packages=(guard.Package("rocketmq-runtime", "rocketmq-runtime"),),
            standalone=(),
            node_projects=(),
            tokio=(),
            local_edges=(),
            evidence_artifacts=(),
        )

        first = guard.render_document(policy, facts)
        second = guard.render_document(policy, facts)

        self.assertEqual(first, second)
        self.assertIn("rocketmq-runtime", first)
        self.assertIn("architecture-implementation-2026-07-29-v1", first)
        self.assertIn("d88a973131ce4f57d01a65def8ecb7944a45ba21", first)
        self.assertIn("Production certified: no", first)

    def test_implementation_baseline_rejects_unversioned_id_and_unsafe_paths(self) -> None:
        policy = {
            "implementation_baseline": {
                "id": "current",
                "generator": "../generator.py",
                "output": "C:/outside/baseline.json",
                "historical_review_commit": "invalid",
                "planning_snapshot_commit": "invalid",
                "historical_difference": "Historical facts are separate.",
                "commands": [],
                "required_evidence": ["../outside.json"],
            }
        }

        findings = guard.validate_implementation_baseline(Path("."), policy)

        codes = {finding.code for finding in findings}
        self.assertIn("implementation-baseline-id", codes)
        self.assertIn("implementation-baseline-commit", codes)
        self.assertIn("implementation-baseline-path", codes)
        self.assertIn("implementation-baseline-commands", codes)
        self.assertIn("implementation-baseline-evidence", codes)

    def test_python_test_inventory_fails_when_a_discovered_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tests = root / "scripts/tests"
            tests.mkdir(parents=True)
            (tests / "test_unowned.py").write_text("", encoding="utf-8")
            workflow = root / ".github/workflows/ci.yml"
            workflow.parent.mkdir(parents=True)
            workflow.write_text(
                "python scripts/run_architecture_tests.py --tier pr_static\n"
                "python scripts/run_architecture_tests.py --tier milestone_contract "
                "--tier phase_contract --tier dynamic_fixture\n",
                encoding="utf-8",
            )
            policy = {
                "root": {"workflow": ".github/workflows/ci.yml"},
                "python_tests": {
                    "expected_count": 53,
                    "ci": {
                        "guards": "python scripts/run_architecture_tests.py --tier pr_static",
                        "contracts": "python scripts/run_architecture_tests.py --tier milestone_contract "
                        "--tier phase_contract --tier dynamic_fixture",
                    },
                    "entries": [],
                },
            }

            findings = guard.validate_python_tests(root, policy)

        self.assertIn("test-inventory-count", {finding.code for finding in findings})


if __name__ == "__main__":
    unittest.main()

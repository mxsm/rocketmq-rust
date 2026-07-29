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
import unittest
from pathlib import Path

from scripts import architecture_documentation_guard as guard


class ArchitectureDocumentationGuardTest(unittest.TestCase):
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
            "python_tests": {
                "ci": {"guards": "guards", "contracts": "contracts"},
                "entries": [],
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
                    "expected_count": 50,
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

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


if __name__ == "__main__":
    unittest.main()

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

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import standalone_workspace_trigger_guard as guard  # noqa: E402


class StandaloneWorkspaceTriggerGuardTests(unittest.TestCase):
    def metadata(self, repository: Path) -> dict[str, object]:
        workspace = repository / "standalone"
        return {
            "workspace_root": str(workspace),
            "packages": [
                {
                    "id": "standalone",
                    "name": "standalone",
                    "manifest_path": str(workspace / "Cargo.toml"),
                    "source": None,
                },
                {
                    "id": "standalone-helper",
                    "name": "standalone-helper",
                    "manifest_path": str(workspace / "crates/helper/Cargo.toml"),
                    "source": None,
                },
                {
                    "id": "client",
                    "name": "rocketmq-client-rust",
                    "manifest_path": str(repository / "rocketmq-client/Cargo.toml"),
                    "source": None,
                },
                {
                    "id": "auth",
                    "name": "rocketmq-auth",
                    "manifest_path": str(repository / "rocketmq-auth/Cargo.toml"),
                    "source": None,
                },
                {
                    "id": "registry",
                    "name": "serde",
                    "manifest_path": str(repository / "registry/serde/Cargo.toml"),
                    "source": "registry+https://github.com/rust-lang/crates.io-index",
                },
                {
                    "id": "outside",
                    "name": "outside-repository",
                    "manifest_path": str(repository.parent / "outside/Cargo.toml"),
                    "source": None,
                },
            ],
        }

    def test_repository_path_dependencies_exclude_workspace_registry_and_outside_packages(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository = Path(temporary_directory) / "repository"

            roots = guard.repository_path_dependency_roots(
                self.metadata(repository),
                repository,
            )

        self.assertEqual({"rocketmq-auth", "rocketmq-client"}, roots)

    def test_missing_triggers_are_reported_for_each_required_event(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository = Path(temporary_directory) / "repository"
            workflow = """
on:
  push:
    paths:
      - "standalone/**"
      - "rocketmq-client/**"
      - "rocketmq-auth/**"
  pull_request:
    paths:
      - "standalone/**"
      - "rocketmq-client/**"
"""

            missing = guard.missing_workflow_triggers(
                self.metadata(repository),
                workflow,
                repository,
            )

        self.assertEqual(set(), missing["push"])
        self.assertEqual({"rocketmq-auth"}, missing["pull_request"])

    def test_parent_directory_trigger_covers_nested_path_dependency(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository = Path(temporary_directory) / "repository"
            metadata = self.metadata(repository)
            metadata["packages"].append(
                {
                    "id": "admin",
                    "name": "rocketmq-admin-core",
                    "manifest_path": str(
                        repository
                        / "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/Cargo.toml"
                    ),
                    "source": None,
                }
            )
            workflow = """
on:
  push:
    paths:
      - "rocketmq-auth/**"
      - "rocketmq-client/**"
      - "rocketmq-tools/rocketmq-admin/**"
  pull_request:
    paths:
      - "rocketmq-auth/**"
      - "rocketmq-client/**"
      - "rocketmq-tools/rocketmq-admin/**"
"""

            missing = guard.missing_workflow_triggers(
                metadata,
                workflow,
                repository,
            )

        self.assertEqual({"push": set(), "pull_request": set()}, missing)

    def test_missing_event_path_section_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository = Path(temporary_directory) / "repository"
            workflow = """
on:
  push:
    paths:
      - "rocketmq-auth/**"
      - "rocketmq-client/**"
  pull_request:
    branches: ["main"]
"""

            missing = guard.missing_workflow_triggers(
                self.metadata(repository),
                workflow,
                repository,
            )

        self.assertEqual(set(), missing["push"])
        self.assertEqual(
            {"rocketmq-auth", "rocketmq-client"},
            missing["pull_request"],
        )


if __name__ == "__main__":
    unittest.main()

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
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
GUARD = REPO_ROOT / "scripts" / "message_store_capability_guard.py"


class MessageStoreCapabilityGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="message-store-guard-")
        self.root = Path(self.temporary.name)
        self.trait = self.root / "rocketmq-store" / "src" / "base" / "message_store.rs"
        self.broker = self.root / "rocketmq-broker" / "src" / "consumer.rs"
        self.trait.parent.mkdir(parents=True)
        self.broker.parent.mkdir(parents=True)
        self.trait.write_text(
            """
            pub trait MessageStore: Send + Sync {
                fn read(&self, key: &str) -> Result<Vec<u8>, Error>;
                async fn start(&mut self);
                // fn commented_out(&self);
                const NOTE: &str = "fn literal(&self)";
            }
            """,
            encoding="utf-8",
        )
        self.broker.write_text(
            """
            use rocketmq_store::MessageStore;
            pub fn read<S: MessageStore>(store: &S) {
                let _ = store.read("key");
            }
            """,
            encoding="utf-8",
        )
        self.run_guard("--write-baseline", expected=0)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def run_guard(self, *arguments: str, expected: int | None = None) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, str(GUARD), "--root", str(self.root), *arguments],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if expected is not None:
            self.assertEqual(result.returncode, expected, result.stderr)
        return result

    def test_exact_baseline_passes_and_ignores_comments_and_literals(self) -> None:
        result = self.run_guard(expected=0)
        self.assertIn("methods=2", result.stdout)
        self.assertIn("broker_used_methods=1", result.stdout)

    def test_new_facade_method_fails(self) -> None:
        self.trait.write_text(
            self.trait.read_text(encoding="utf-8").replace(
                "async fn start(&mut self);",
                "async fn start(&mut self);\nfn new_admin_escape_hatch(&self);",
            ),
            encoding="utf-8",
        )

        result = self.run_guard()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("new MessageStore methods are forbidden", result.stderr)

    def test_new_broker_wide_dependency_fails(self) -> None:
        path = self.root / "rocketmq-broker" / "src" / "new_processor.rs"
        path.write_text("fn process<S: MessageStore>(store: &S) {}", encoding="utf-8")

        result = self.run_guard()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("new Broker MessageStore dependencies are forbidden", result.stderr)

    def test_every_allowlist_entry_requires_removal_metadata(self) -> None:
        baseline_path = self.root / "scripts" / "message-store-capability-baseline.json"
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
        baseline["broker_allowlist"][0]["owner"] = ""
        baseline_path.write_text(json.dumps(baseline), encoding="utf-8")

        result = self.run_guard()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("lack owner/reason/removal condition", result.stderr)

    def test_facade_reduction_is_allowed_after_report_refresh(self) -> None:
        self.trait.write_text(
            self.trait.read_text(encoding="utf-8").replace("async fn start(&mut self);", ""),
            encoding="utf-8",
        )
        self.run_guard("--write-report", expected=0)

        result = self.run_guard(expected=0)

        self.assertIn("methods=1", result.stdout)


class RepositoryMessageStoreCapabilityContracts(unittest.TestCase):
    def test_repository_baseline_and_generated_board_are_current(self) -> None:
        result = subprocess.run(
            [sys.executable, str(GUARD), "--root", str(REPO_ROOT)],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("methods=126", result.stdout)
        self.assertIn("broker_used_methods=62", result.stdout)


if __name__ == "__main__":
    unittest.main()

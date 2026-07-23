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
GUARD = REPO_ROOT / "scripts" / "stable_surface_guard.py"


class StableSurfaceGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="stable-surface-")
        self.root = Path(self.temporary.name)
        (self.root / "crate" / "src").mkdir(parents=True)
        self.source = self.root / "crate" / "src" / "lib.rs"
        self.source.write_text("#![feature(example_feature)]\npub fn value() {}\n", encoding="utf-8")
        self.policy = self.root / "policy.json"
        self.write_policy([self.allowed("crate/src/lib.rs", "example_feature")])

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def allowed(path: str, feature: str) -> dict[str, str]:
        return {
            "path": path,
            "feature": feature,
            "owner": "test",
            "remove_by": "R22",
            "reason": "Test fixture debt.",
        }

    def write_policy(self, allowed_features: list[dict[str, str]]) -> None:
        self.policy.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "target": "stable-default",
                    "allowed_features": allowed_features,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def run_guard(self, mode: str = "baseline") -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(GUARD),
                "--root",
                str(self.root),
                "--policy",
                str(self.policy),
                "--mode",
                mode,
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

    def test_exact_baseline_passes(self) -> None:
        result = self.run_guard()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("mode=baseline features=1", result.stdout)

    def test_unregistered_feature_fails_closed(self) -> None:
        self.source.write_text(
            "#![feature(example_feature, unregistered_feature)]\npub fn value() {}\n",
            encoding="utf-8",
        )
        result = self.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unregistered nightly features", result.stderr)

    def test_stale_policy_entry_is_rejected(self) -> None:
        self.source.write_text("pub fn value() {}\n", encoding="utf-8")
        result = self.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("stale allowed nightly features", result.stderr)

    def test_target_mode_rejects_registered_debt(self) -> None:
        result = self.run_guard("target")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("stable target still has nightly features", result.stderr)

    def test_clean_target_passes(self) -> None:
        self.source.write_text("pub fn value() {}\n", encoding="utf-8")
        self.write_policy([])
        result = self.run_guard("target")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("mode=target features=0", result.stdout)


class RepositoryStableSurfaceContracts(unittest.TestCase):
    def test_remoting_and_controller_no_longer_enable_retired_features(self) -> None:
        sources = {
            "remoting crate": REPO_ROOT / "rocketmq-remoting" / "src" / "lib.rs",
            "remoting integration test": REPO_ROOT / "rocketmq-remoting" / "tests" / "processor_v2_tests.rs",
            "remoting example": REPO_ROOT / "rocketmq-remoting" / "examples" / "processor_v2_complete_example.rs",
            "controller crate": REPO_ROOT / "rocketmq-controller" / "src" / "lib.rs",
        }
        for label, path in sources.items():
            with self.subTest(label=label):
                source = path.read_text(encoding="utf-8")
                self.assertNotIn("#![feature(impl_trait_in_assoc_type)]", source)
                self.assertNotIn("#![feature(arbitrary_self_types)]", source)

    def test_remoting_builtin_processors_keep_concrete_zero_allocation_futures(self) -> None:
        source = (REPO_ROOT / "rocketmq-remoting" / "src" / "runtime" / "processor_v2.rs").read_text(
            encoding="utf-8"
        )
        self.assertEqual(source.count("= Ready<RocketMQResult<Option<RemotingCommand>>>"), 3)
        self.assertNotIn("= impl Future<Output = RocketMQResult<Option<RemotingCommand>>>", source)

    def test_runtime_scheduler_uses_owned_stable_futures(self) -> None:
        crate_root = (REPO_ROOT / "rocketmq-runtime" / "src" / "lib.rs").read_text(encoding="utf-8")
        scheduler = (REPO_ROOT / "rocketmq-runtime" / "src" / "schedule.rs").read_text(encoding="utf-8")
        self.assertNotIn("#![feature(async_fn_traits)]", crate_root)
        self.assertNotIn("#![feature(unboxed_closures)]", crate_root)
        self.assertNotIn("AsyncFnMut", scheduler)
        self.assertEqual(scheduler.count("Fut: Future<Output = Result<()>> + Send + 'static"), 8)
        self.assertIn("let mut task_fn = task_fn.lock().await;", scheduler)
        self.assertIn("(task_fn)(token).await", scheduler)


if __name__ == "__main__":
    unittest.main()

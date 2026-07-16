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
import re
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
POLICY = ROOT / "scripts" / "architecture-dependency-policy.json"
LIFECYCLE_PATTERN = re.compile(
    r"rocketmq_rust::(?:schedule|task|Shutdown|wait_for_signal|ExecutorConfig|"
    r"ExecutorPool|TaskExecutor|SchedulerConfig|TaskScheduler|TaskContext|"
    r"TaskStatus|CronTrigger|DelayTrigger|DelayedIntervalTrigger|IntervalTrigger|Trigger)"
)
NEW_BOUNDARIES = {
    "rocketmq-model",
    "rocketmq-protocol",
    "rocketmq-transport",
    "rocketmq-security-api",
    "rocketmq-store-api",
    "rocketmq-store-local",
    "rocketmq-store-rocksdb",
    "rocketmq-proxy-core",
    "rocketmq-proxy-cluster",
    "rocketmq-proxy-local",
}


def load_toml(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def workspace_member_roots() -> list[Path]:
    manifest = load_toml(ROOT / "Cargo.toml")
    return [ROOT / member for member in manifest["workspace"]["members"]]


def rust_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.rs") if "target" not in path.parts)


class LegacyRuntimeDrainContractTest(unittest.TestCase):
    def test_runtime_owns_lifecycle_modules_and_legacy_is_reexport_only(self) -> None:
        owner_files = {
            "schedule.rs",
            "schedule/executor.rs",
            "schedule/scheduler.rs",
            "schedule/task.rs",
            "schedule/trigger.rs",
            "shutdown.rs",
            "signal.rs",
            "task.rs",
            "task/service_task.rs",
        }
        for relative in owner_files:
            self.assertTrue((ROOT / "rocketmq-runtime" / "src" / relative).is_file(), relative)

        legacy_shims = {
            "schedule.rs": "pub use rocketmq_runtime::schedule::*;",
            "shutdown.rs": "pub use rocketmq_runtime::shutdown::*;",
            "task.rs": "pub use rocketmq_runtime::task::*;",
        }
        for relative, expected in legacy_shims.items():
            source = (ROOT / "rocketmq" / "src" / relative).read_text(encoding="utf-8")
            self.assertIn(expected, source)
            self.assertNotRegex(source, r"pub (?:struct|enum|trait) ")

        self.assertFalse((ROOT / "rocketmq" / "src" / "task" / "service_task.rs").exists())
        for relative in ("executor.rs", "scheduler.rs", "task.rs", "trigger.rs"):
            self.assertFalse((ROOT / "rocketmq" / "src" / "schedule" / relative).exists())

    def test_runtime_remains_an_internal_dependency_leaf(self) -> None:
        runtime = load_toml(ROOT / "rocketmq-runtime" / "Cargo.toml")
        internal = {name for name in runtime["dependencies"] if name.startswith("rocketmq-")}
        self.assertEqual(set(), internal)

        legacy = load_toml(ROOT / "rocketmq" / "Cargo.toml")
        self.assertEqual(
            {"rocketmq-runtime", "serde", "tokio"},
            set(legacy["dependencies"]),
        )

        policy = json.loads(POLICY.read_text(encoding="utf-8"))
        self.assertEqual([], policy["target_dag"]["rocketmq-runtime"])

    def test_new_boundary_policy_rejects_legacy_runtime_dependency(self) -> None:
        policy = json.loads(POLICY.read_text(encoding="utf-8"))
        rules = {rule["id"]: rule for rule in policy["package_rules"]}
        rule = rules["new-boundaries-no-legacy-runtime"]
        self.assertEqual(NEW_BOUNDARIES, set(rule["callers"]))
        self.assertEqual(["rocketmq-rust"], rule["forbidden_targets"])

    def test_workspace_consumers_use_canonical_lifecycle_paths(self) -> None:
        findings: list[str] = []
        for member in workspace_member_roots():
            if member.name == "rocketmq":
                continue
            for path in rust_files(member):
                if LIFECYCLE_PATTERN.search(path.read_text(encoding="utf-8")):
                    findings.append(path.relative_to(ROOT).as_posix())
        self.assertEqual([], findings)

    def test_standalone_example_freezes_legacy_signal_compatibility(self) -> None:
        example = ROOT / "rocketmq-example"
        consumers = [
            path.relative_to(ROOT).as_posix()
            for path in rust_files(example)
            if "use rocketmq_rust::wait_for_signal;" in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(8, len(consumers))
        manifest = load_toml(example / "Cargo.toml")
        self.assertNotIn("rocketmq-runtime", manifest["dependencies"])

    def test_migrated_runtime_has_no_legacy_memory_or_detached_work(self) -> None:
        migrated = [
            ROOT / "rocketmq-runtime" / "src" / "schedule.rs",
            ROOT / "rocketmq-runtime" / "src" / "shutdown.rs",
            ROOT / "rocketmq-runtime" / "src" / "signal.rs",
            ROOT / "rocketmq-runtime" / "src" / "task" / "service_task.rs",
        ]
        source = "\n".join(path.read_text(encoding="utf-8") for path in migrated)
        for forbidden in (
            "ArcMut",
            "WeakArcMut",
            "SyncUnsafeCellWrapper",
            "tokio::spawn",
            "spawn_blocking",
            "std::thread",
        ):
            self.assertNotIn(forbidden, source)

        signal = (ROOT / "rocketmq-runtime" / "src" / "signal.rs").read_text(encoding="utf-8")
        self.assertNotIn(".expect(", signal)
        owner_definitions = sum(
            path.read_text(encoding="utf-8").count("pub struct RuntimeOwner")
            for path in rust_files(ROOT / "rocketmq-runtime")
        )
        self.assertEqual(1, owner_definitions)


if __name__ == "__main__":
    unittest.main()

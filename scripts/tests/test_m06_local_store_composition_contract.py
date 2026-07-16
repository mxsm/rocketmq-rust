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

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOCAL_ROOT = ROOT / "rocketmq-store-local"
STORE_FACADE = (
    ROOT / "rocketmq-store/src/message_store/local_file_message_store.rs"
)


def source(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


class M06LocalStoreCompositionContractTests(unittest.TestCase):
    def test_local_crate_owns_composition_and_normalized_config(self) -> None:
        local_lib = source("rocketmq-store-local/src/lib.rs")
        message_store_root = source("rocketmq-store-local/src/message_store.rs")
        canonical_root = source(
            "rocketmq-store-local/src/message_store/local_file_message_store.rs"
        )
        config_root = source("rocketmq-store-local/src/config.rs")
        backend_config = source("rocketmq-store-local/src/config/backend.rs")

        self.assertIn("pub mod message_store;", local_lib)
        for module in ("cleanup", "lifecycle", "local_file_message_store", "query", "reput"):
            self.assertIn(f"pub mod {module};", message_store_root)
        self.assertIn("pub struct LocalStoreComposition", canonical_root)
        self.assertIn("pub mod backend;", config_root)
        self.assertIn("pub struct LocalBackendConfig", backend_config)
        self.assertNotIn("Deserialize", backend_config)

    def test_store_keeps_legacy_facade_and_envelope_but_delegates_decisions(self) -> None:
        facade = STORE_FACADE.read_text(encoding="utf-8") + "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted(STORE_FACADE.with_suffix("").glob("*.rs"))
            if path.name != "tests.rs"
        )
        legacy_config = source("rocketmq-store/src/config/message_store_config.rs")

        self.assertIn("pub struct LocalFileMessageStore {", facade)
        self.assertIn("composition: LocalStoreComposition", facade)
        self.assertIn("self.composition.lifecycle()", facade)
        self.assertIn(".query()", facade)
        self.assertIn("self.composition.reput()", facade)
        self.assertIn("cleanup_policy.classify", facade)
        self.assertNotIn("enum StoreLifecycleState", facade)
        self.assertNotIn("struct DiskCleanDecision", facade)
        self.assertNotIn("manual_delete_requests: AtomicI32", facade)

        self.assertIn("pub struct MessageStoreConfig", legacy_config)
        self.assertIn("normalized_local_backend_config", legacy_config)
        self.assertIn("LocalBackendConfig", legacy_config)

    def test_local_boundary_excludes_facade_and_external_effect_types(self) -> None:
        manifest = source("rocketmq-store-local/Cargo.toml")
        local_sources = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((LOCAL_ROOT / "src").rglob("*.rs"))
        )

        for dependency in ("rocketmq-common", "rocketmq-remoting", "rocketmq-store ="):
            self.assertNotIn(dependency, manifest)
        for forbidden in (
            "rocketmq_common",
            "rocketmq_remoting",
            "pub struct LocalFileMessageStore {",
            "MessageExt",
            "ArcMut",
            "tokio::spawn",
            "ScheduledTaskGroup",
        ):
            self.assertNotIn(forbidden, local_sources)

    def test_new_local_modules_remain_below_the_review_upper_bound(self) -> None:
        paths = [
            LOCAL_ROOT / "src/config/backend.rs",
            LOCAL_ROOT / "src/message_store.rs",
            *sorted((LOCAL_ROOT / "src/message_store").glob("*.rs")),
        ]
        for path in paths:
            line_count = len(path.read_text(encoding="utf-8").splitlines())
            self.assertLessEqual(line_count, 800, f"{path} has {line_count} lines")

    def test_public_path_compile_fixture_is_present(self) -> None:
        fixture = source("rocketmq-store/src/lib.rs")
        self.assertIn(
            "message_store::local_file_message_store::LocalFileMessageStore",
            fixture,
        )
        self.assertIn(
            "message_store::local_file_message_store::LocalStoreComposition",
            fixture,
        )


if __name__ == "__main__":
    unittest.main()

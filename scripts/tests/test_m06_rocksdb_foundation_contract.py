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

import re
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ROCKS_CRATE = ROOT / "rocketmq-store-rocksdb"
STORE_CRATE = ROOT / "rocketmq-store"
LOCAL_CRATE = ROOT / "rocketmq-store-local"

FOUNDATION_MODULES = {
    "batch",
    "checkpoint",
    "codec",
    "column_family",
    "config",
    "consume_queue",
    "error",
    "index",
    "iterator",
    "key",
    "maintenance",
    "message",
    "options",
    "runtime",
    "snapshot",
    "store",
    "timer",
    "transaction",
    "message_store",
    "value",
}

FORBIDDEN_DEPENDENCIES = {
    "rocketmq-store",
    "rocketmq-tieredstore",
    "rocketmq-broker",
    "rocketmq-remoting",
    "rocketmq-common",
    "rocketmq-rust",
}

FORBIDDEN_SOURCE_TOKENS = {
    "rocketmq_store",
    "rocketmq_tieredstore",
    "rocketmq_broker",
    "rocketmq_remoting",
    "rocketmq_common",
    "rocketmq_rust",
    "DispatchRequest",
    "MessageStoreConfig",
    "CommitLogDispatcher",
}


def read(path: str | Path) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def rust_code(source: str) -> str:
    source = re.sub(r"/\*.*?\*/", " ", source, flags=re.DOTALL)
    source = re.sub(r"//[^\r\n]*", " ", source)
    return re.sub(r'(?s)(?:br|r|b)?(?:#+)?".*?"(?:#+)?', " ", source)


class RocksDbFoundationContractTests(unittest.TestCase):
    def test_workspace_and_feature_ownership_are_exact(self) -> None:
        root_manifest = tomllib.loads(read("Cargo.toml"))
        self.assertIn("rocketmq-store-rocksdb", root_manifest["workspace"]["members"])
        self.assertEqual(
            {
                "version": "1.0.0",
                "path": "./rocketmq-store-rocksdb",
                "default-features": False,
            },
            root_manifest["workspace"]["dependencies"]["rocketmq-store-rocksdb"],
        )

        rocks_manifest = tomllib.loads(read("rocketmq-store-rocksdb/Cargo.toml"))
        self.assertEqual({"default": []}, rocks_manifest["features"])
        dependencies = rocks_manifest["dependencies"]
        self.assertEqual({"workspace": True}, dependencies["rocksdb"])
        self.assertTrue(FORBIDDEN_DEPENDENCIES.isdisjoint(dependencies), dependencies)

        store_manifest = tomllib.loads(read("rocketmq-store/Cargo.toml"))
        self.assertEqual(
            ["dep:rocketmq-store-rocksdb"],
            store_manifest["features"]["rocksdb_store"],
        )
        self.assertEqual(
            {"workspace": True, "optional": True},
            store_manifest["dependencies"]["rocketmq-store-rocksdb"],
        )
        self.assertNotIn("rocksdb", store_manifest["dependencies"])
        self.assertNotIn("rocksdb_store", store_manifest["features"]["default"])

        local_manifest = tomllib.loads(read("rocketmq-store-local/Cargo.toml"))
        self.assertNotIn("rocksdb", local_manifest["dependencies"])
        self.assertNotIn("rocketmq-store-rocksdb", local_manifest["dependencies"])

    def test_foundation_owns_native_modules_without_facade_types(self) -> None:
        lib_source = read("rocketmq-store-rocksdb/src/lib.rs")
        for module in FOUNDATION_MODULES:
            self.assertRegex(lib_source, rf"pub mod {module};")

        findings: list[str] = []
        for path in sorted((ROCKS_CRATE / "src").rglob("*.rs")):
            source = rust_code(path.read_text(encoding="utf-8"))
            for token in FORBIDDEN_SOURCE_TOKENS:
                if re.search(rf"\b{re.escape(token)}\b", source):
                    findings.append(f"{path.relative_to(ROOT)}: {token}")
            self.assertLessEqual(
                len(path.read_text(encoding="utf-8").splitlines()),
                800,
                f"new RocksDB owner module exceeds the 800-line review limit: {path.relative_to(ROOT)}",
            )
        self.assertEqual([], findings)
        self.assertNotRegex(lib_source, r"\bCommitLog\b")

    def test_store_keeps_only_projection_dispatch_and_legacy_paths(self) -> None:
        config = read("rocketmq-store/src/rocksdb/config.rs")
        consume_queue = read("rocketmq-store/src/rocksdb/consume_queue.rs")
        index = read("rocketmq-store/src/rocksdb/index.rs")
        message_store = read("rocketmq-store/src/message_store/rocksdb_message_store.rs")

        self.assertIn("pub use rocketmq_store_rocksdb::config::RocksDbConfig;", config)
        self.assertIn("impl RocksDbConfigSource for MessageStoreConfig", config)
        self.assertIn(
            "pub use rocketmq_store_rocksdb::consume_queue::RocksDbConsumeQueueStore;",
            consume_queue,
        )
        self.assertIn("impl RocksDbConsumeQueueDispatch for DispatchRequest", consume_queue)
        self.assertIn("pub struct CommitLogDispatcherBuildRocksDbConsumeQueue", consume_queue)
        self.assertIn(
            "pub use rocketmq_store_rocksdb::index::RocksDbIndexBuildService;",
            index,
        )
        self.assertIn("impl RocksDbIndexDispatch for DispatchRequest", index)
        self.assertIn("pub struct CommitLogDispatcherBuildRocksDbIndex", index)
        self.assertIn("pub struct RocksDBMessageStore", message_store)

        for module in FOUNDATION_MODULES - {
            "config",
            "consume_queue",
            "index",
            "runtime",
            "timer",
            "transaction",
            "message_store",
        }:
            facade = read(f"rocketmq-store/src/rocksdb/{module}.rs")
            self.assertIn(f"rocketmq_store_rocksdb::{module}::", facade)

    def test_timer_transaction_and_message_store_kernel_move_to_owner(self) -> None:
        self.assertTrue((STORE_CRATE / "src/rocksdb/timer.rs").is_file())
        self.assertTrue((STORE_CRATE / "src/rocksdb/transaction.rs").is_file())
        self.assertTrue((STORE_CRATE / "src/message_store/rocksdb_message_store.rs").is_file())
        self.assertTrue((ROCKS_CRATE / "src/timer.rs").is_file())
        self.assertTrue((ROCKS_CRATE / "src/transaction.rs").is_file())
        self.assertTrue((ROCKS_CRATE / "src/message_store.rs").is_file())


if __name__ == "__main__":
    unittest.main()

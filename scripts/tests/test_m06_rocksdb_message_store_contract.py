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


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def rust_code(source: str) -> str:
    source = re.sub(r"/\*.*?\*/", " ", source, flags=re.DOTALL)
    source = re.sub(r"//[^\r\n]*", " ", source)
    return re.sub(r'(?s)(?:br|r|b)?(?:#+)?".*?"(?:#+)?', " ", source)


class RocksDbMessageStoreContractTests(unittest.TestCase):
    def test_dependency_direction_is_rocks_to_local_to_api(self) -> None:
        rocks = tomllib.loads(read("rocketmq-store-rocksdb/Cargo.toml"))["dependencies"]
        local = tomllib.loads(read("rocketmq-store-local/Cargo.toml"))["dependencies"]

        self.assertEqual({"workspace": True}, rocks["rocketmq-store-api"])
        self.assertEqual({"workspace": True}, rocks["rocketmq-store-local"])
        self.assertEqual({"workspace": True}, local["rocketmq-store-api"])
        self.assertNotIn("rocketmq-store-rocksdb", local)
        for forbidden in (
            "rocketmq-store",
            "rocketmq-tieredstore",
            "rocketmq-broker",
            "rocketmq-common",
            "rocketmq-remoting",
            "rocketmq-rust",
        ):
            self.assertNotIn(forbidden, rocks)

    def test_owner_composes_one_local_wal_and_derived_backend(self) -> None:
        local_port = rust_code(read("rocketmq-store-local/src/commit_log/read.rs"))
        owner = rust_code(read("rocketmq-store-rocksdb/src/message_store.rs"))

        self.assertIn("trait LocalWalPort", local_port)
        self.assertIn("pub struct RocksDbDerivedStore", owner)
        self.assertIn("pub struct RocksDbMessageStoreRoot", owner)
        self.assertIn("L: LocalWalPort", owner)
        self.assertIn("local: &L", owner)
        self.assertIn("derived: RocksDbDerivedStore", owner)
        for forbidden in (
            "CommitLog::new",
            "CommitLogRoot",
            "MappedFileQueue",
            "LocalFileMessageStore",
            "DispatchRequest",
            "MessageStoreConfig",
        ):
            self.assertNotIn(forbidden, owner)

    def test_store_is_a_legacy_projection_and_default_is_not_double_write(self) -> None:
        store = rust_code(read("rocketmq-store/src/message_store/rocksdb_message_store.rs"))
        local = rust_code(read("rocketmq-store/src/message_store/local_file_message_store.rs"))
        timer = rust_code(read("rocketmq-store/src/rocksdb/timer.rs"))
        transaction = rust_code(read("rocketmq-store/src/rocksdb/transaction.rs"))

        struct_body = re.search(
            r"pub\s+struct\s+RocksDBMessageStore\s*\{(?P<body>.*?)\n\}",
            store,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(struct_body)
        body = struct_body.group("body")
        self.assertIn("root: RocksDbMessageStoreRoot", body)
        for former_field in (
            "rocksdb_store:",
            "consume_queue_store:",
            "message_rocksdb_storage:",
            "rocksdb_index_service:",
            "rocksdb_timer_service:",
            "rocksdb_trans_service:",
        ):
            self.assertNotIn(former_field, body)

        self.assertIn("!message_store_config.is_enable_rocksdb_store()", local)
        self.assertIn("rocksdb_cq_double_write_enable", local)
        self.assertNotIn("rocksdb_cq_double_write_enable", store)
        self.assertIn("impl RocksDbTimerDispatch for DispatchRequest", timer)
        self.assertIn("impl RocksDbTransactionDispatch for DispatchRequest", transaction)

    def test_owner_source_has_no_client_broker_or_store_facade_leaks(self) -> None:
        findings: list[str] = []
        for path in sorted((ROOT / "rocketmq-store-rocksdb/src").rglob("*.rs")):
            source = rust_code(path.read_text(encoding="utf-8"))
            for token in (
                "rocketmq_broker",
                "rocketmq_common",
                "rocketmq_remoting",
                "rocketmq_rust",
                "rocketmq_client",
            ):
                if re.search(rf"\b{token}\b", source):
                    findings.append(f"{path.relative_to(ROOT)}: {token}")
        self.assertEqual([], findings)


if __name__ == "__main__":
    unittest.main()

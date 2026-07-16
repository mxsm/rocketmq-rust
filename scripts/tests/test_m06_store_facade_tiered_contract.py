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

import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def manifest(path: str) -> dict:
    return tomllib.loads(read(path))


def production_source(path: str) -> str:
    source = read(path)
    return source.split("#[cfg(test)]\nmod tests", 1)[0]


class StoreFacadeTieredContractTests(unittest.TestCase):
    def test_feature_owners_and_compatibility_aliases_are_exact(self) -> None:
        store = manifest("rocketmq-store/Cargo.toml")
        local = manifest("rocketmq-store-local/Cargo.toml")
        rocks = manifest("rocketmq-store-rocksdb/Cargo.toml")

        self.assertEqual(
            {
                "default": [],
                "fast-load": [],
                "safe-load": [],
                "io_uring": ["dep:tokio-uring"],
                "observability": [
                    "dep:rocketmq-observability",
                    "rocketmq-observability/otel-metrics",
                ],
            },
            local["features"],
        )
        self.assertEqual([], rocks["features"]["default"])

        features = store["features"]
        self.assertEqual(["local_file_store", "fast-load"], features["default"])
        self.assertEqual([], features["local_file_store"])
        self.assertEqual(["local_file_store"], features["data_store"])
        self.assertEqual(["dep:rocketmq-store-rocksdb"], features["rocksdb_store"])
        self.assertEqual(["rocksdb_store"], features["rocksdb-store"])
        self.assertEqual(["rocketmq-store-local/fast-load"], features["fast-load"])
        self.assertEqual(["rocketmq-store-local/safe-load"], features["safe-load"])
        self.assertEqual(["rocketmq-store-local/io_uring"], features["io_uring"])
        self.assertEqual(["dep:rocketmq-tieredstore"], features["tieredstore"])
        self.assertEqual(
            [
                "rocketmq-observability/otel-metrics",
                "rocketmq-store-local/observability",
                "rocketmq-tieredstore?/otel-metrics",
            ],
            features["observability"],
        )

    def test_no_default_features_preserves_the_local_compatibility_facade(self) -> None:
        source = read("rocketmq-store/src/message_store.rs")
        self.assertNotIn('cfg(feature = "local_file_store")', source)
        self.assertRegex(
            source,
            r"pub enum GenericMessageStore\s*\{\s*LocalFileStore\(",
        )
        self.assertIn("pub fn local_file(", source)

    def test_local_owner_keeps_fast_safe_priority(self) -> None:
        owner = read("rocketmq-store-local/src/commit_log/load_orchestration.rs")
        facade = read("rocketmq-store/src/log_file/commit_log.rs")

        self.assertIn("fast_load || !safe_load", owner)
        self.assertIn("parallel_commit_log_load_enabled()", facade)
        self.assertNotIn('cfg!(feature = "fast-load")', facade)
        self.assertNotIn('cfg!(feature = "safe-load")', facade)

    def test_tiered_owner_depends_only_on_the_neutral_store_contract(self) -> None:
        tiered = manifest("rocketmq-tieredstore/Cargo.toml")
        dependencies = tiered["dependencies"]

        self.assertEqual({"workspace": True}, dependencies["rocketmq-store-api"])
        self.assertNotIn("rocketmq-store", dependencies)
        self.assertNotIn("rocketmq-store-local", dependencies)
        self.assertNotIn("rocketmq-store-rocksdb", dependencies)

        source = read("rocketmq-tieredstore/src/store.rs")
        self.assertIn(
            "impl<P> rocketmq_store_api::StoreLifecycle for TieredStore<P>",
            source,
        )
        self.assertIn("TieredLifecycle::load(self).await?", source)
        self.assertIn("TieredLifecycle::start(self).await", source)
        self.assertIn("TieredLifecycle::shutdown(self).await", source)

    def test_store_decorator_owns_tiered_mapping_and_local_only_composes_it(self) -> None:
        local_facade = production_source(
            "rocketmq-store/src/message_store/local_file_message_store.rs"
        )
        decorator = read("rocketmq-store/src/tieredstore.rs")

        self.assertNotIn("rocketmq_tieredstore", local_facade)
        self.assertIn("TieredStoreDecorator", local_facade)
        self.assertNotIn("map_tiered_get_status", local_facade)
        self.assertNotIn("select_result_from_tiered_message", local_facade)
        self.assertNotIn("to_store_get_message_result", local_facade)

        self.assertIn("pub struct TieredStoreDecorator", decorator)
        self.assertIn("fn map_tiered_get_status", decorator)
        self.assertIn("fn select_result_from_tiered_message", decorator)
        self.assertIn("fn to_store_get_message_result", decorator)
        self.assertRegex(
            decorator,
            r'#\[cfg\(test\)\]\s+pub\(crate\) const fn inner',
        )


if __name__ == "__main__":
    unittest.main()

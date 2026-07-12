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
CRATE = ROOT / "rocketmq-store-api"
CAPABILITIES = {
    "StoreLifecycle",
    "MessageAppender",
    "MessageReader",
    "OffsetIndex",
    "StoreHealth",
    "ReplicationControl",
    "DerivedRecordSink",
    "AdminStore",
}
ALLOWED_NORMAL_DEPENDENCIES = {"rocketmq-model", "rocketmq-error", "bytes"}
FORBIDDEN_SOURCE_TOKENS = {
    "tokio",
    "rocketmq_runtime",
    "rocketmq_remoting",
    "rocketmq_observability",
    "MappedFile",
    "RocksDB",
    "TieredStore",
    "CommitLog",
    "BrokerRuntime",
}


class StoreApiContractTests(unittest.TestCase):
    def test_workspace_contains_runtime_neutral_store_api_crate(self) -> None:
        root_manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertIn("rocketmq-store-api", root_manifest["workspace"]["members"])
        self.assertIn("rocketmq-store-api", root_manifest["workspace"]["dependencies"])

        manifest = tomllib.loads((CRATE / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertEqual([], manifest["features"]["default"])
        self.assertEqual(ALLOWED_NORMAL_DEPENDENCIES, set(manifest.get("dependencies", {})))

    def test_public_contracts_are_narrow_and_backend_neutral(self) -> None:
        source = (CRATE / "src" / "lib.rs").read_text(encoding="utf-8")
        for capability in CAPABILITIES:
            self.assertIn(f"pub trait {capability}", source)
        for token in FORBIDDEN_SOURCE_TOKENS:
            self.assertNotIn(token, source)

    def test_broker_send_path_consumes_the_capability_boundary(self) -> None:
        manifest = tomllib.loads((ROOT / "rocketmq-broker" / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertIn("rocketmq-store-api", manifest["dependencies"])

        source = (ROOT / "rocketmq-broker" / "src" / "processor" / "send_message_processor.rs").read_text(
            encoding="utf-8"
        )
        self.assertIn("S: MessageAppender<M>", source)
        self.assertIn("S: StoreHealth", source)
        self.assertIn("append_message_with_store(&mut store", source)
        self.assertIn("store_health_reject_remark_from", source)


if __name__ == "__main__":
    unittest.main()

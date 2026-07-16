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


def source(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


class M06FlushLocalContractTests(unittest.TestCase):
    def test_group_commit_owner_and_store_facades_are_unique(self) -> None:
        local = source("rocketmq-store-local/src/flush/group_commit.rs")
        local_root = source("rocketmq-store-local/src/flush.rs")
        store_request = source(
            "rocketmq-store/src/log_file/flush_manager_impl/group_commit_request.rs"
        )
        store_manager = source(
            "rocketmq-store/src/log_file/flush_manager_impl/default_flush_manager.rs"
        )
        store_trait = source("rocketmq-store/src/base/flush_manager.rs")

        for owner in (
            "pub struct GroupCommitRequest<E>",
            "pub enum GroupCommitStatus",
            "pub struct SyncFlushStats",
            "pub struct SyncFlushRuntimeInfo",
            "pub fn complete_group_commit_batch<E>",
            "pub fn complete_group_commit_batch_error<E>",
        ):
            self.assertIn(owner, local)

        self.assertIn("pub mod group_commit;", local_root)
        self.assertIn("pub use group_commit::SyncFlushRuntimeInfo;", local_root)
        self.assertIn(
            "rocketmq_store_local::flush::group_commit::GroupCommitRequest<StoreError>",
            store_request,
        )
        self.assertNotIn("struct GroupCommitRequest", store_request)
        self.assertIn(
            "pub use rocketmq_store_local::flush::SyncFlushRuntimeInfo;",
            store_trait,
        )
        self.assertNotIn("struct SyncFlushRuntimeInfo", store_trait)

        self.assertIn(
            "use rocketmq_store_local::flush::group_commit::complete_group_commit_batch;",
            store_manager,
        )
        self.assertIn(
            "use rocketmq_store_local::flush::group_commit::complete_group_commit_batch_error;",
            store_manager,
        )
        self.assertNotIn("fn complete_group_commit_batch(", store_manager)
        self.assertNotIn("struct SyncFlushStats", store_manager)


if __name__ == "__main__":
    unittest.main()

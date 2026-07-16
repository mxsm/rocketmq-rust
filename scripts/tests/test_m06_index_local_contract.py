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


class M06IndexLocalContractTests(unittest.TestCase):
    def test_index_codec_and_layout_have_one_local_owner(self) -> None:
        local_root = source("rocketmq-store-local/src/lib.rs")
        local_module = source("rocketmq-store-local/src/index/mod.rs")
        local_codec = source("rocketmq-store-local/src/index/codec.rs")
        local_tests = source("rocketmq-store-local/tests/index_codec.rs")
        local_file = source("rocketmq-store-local/src/index/file.rs")
        local_file_tests = source("rocketmq-store-local/tests/index_file_driver.rs")
        local_service = source("rocketmq-store-local/src/index/service.rs")
        local_dispatch = source("rocketmq-store-local/src/index/dispatch.rs")
        local_service_tests = source("rocketmq-store-local/tests/index_service_root.rs")
        store_header = source("rocketmq-store/src/index/index_header.rs")
        store_file = source("rocketmq-store/src/index/index_file.rs")
        store_file_production = store_file.split("#[cfg(test)]", 1)[0]
        store_service = source("rocketmq-store/src/index/index_service.rs")
        store_service_production = store_service
        store_dispatch = source("rocketmq-store/src/index/index_dispatch.rs")
        store_dispatch_production = store_dispatch.split("#[cfg(test)]", 1)[0]
        store_query_result = source("rocketmq-store/src/index/query_offset_result.rs")

        self.assertIn("pub mod index;", local_root)
        self.assertIn("pub mod codec;", local_module)
        for owner in (
            "pub const INDEX_HEADER_SIZE: usize = 40;",
            "pub const INDEX_HASH_SLOT_SIZE: usize = 4;",
            "pub const INDEX_ENTRY_SIZE: usize = 20;",
            "pub struct IndexHeaderRecord",
            "pub struct IndexSlot(pub i32);",
            "pub struct IndexEntry",
            "pub enum IndexLayoutError",
            "pub fn index_file_total_size(",
            "pub fn hash_slot_position(",
            "pub fn index_entry_position(",
        ):
            self.assertIn(owner, local_codec)

        self.assertIn(
            "pub use rocketmq_store_local::index::codec::INDEX_HEADER_SIZE;",
            store_header,
        )
        self.assertNotIn("pub const INDEX_HEADER_SIZE", store_header)
        self.assertIn("IndexHeaderRecord::decode(&buffer)", store_header)
        self.assertIn(".encode();", store_header)

        self.assertIn("local_index_file_total_size(", store_file_production)
        for codec_call in (
            "hash_slot_position(slot_index)",
            "index_entry_position(",
            "IndexSlot::decode(",
            "IndexEntry::new(",
            "IndexEntry::decode(",
        ):
            self.assertIn(codec_call, local_file)
        for duplicate in (
            "const HASH_SLOT_SIZE",
            "const INDEX_SIZE",
            "const INVALID_INDEX",
            ".get_i32()",
            ".get_i64()",
            "i32::from_be_bytes",
            "i64::from_be_bytes",
            "abs_index_pos + 4 + 8",
        ):
            self.assertNotIn(duplicate, store_header + store_file_production)

        for test_name in (
            "header_encoding_matches_the_java_40_byte_golden",
            "header_decode_preserves_the_legacy_minimum_index_count",
            "slot_and_entry_codecs_preserve_signed_values_and_bounds",
            "checked_layout_matches_the_persisted_sections",
            "checked_layout_rejects_zero_dimensions_and_overflow",
        ):
            self.assertIn(f"fn {test_name}()", local_tests)

        self.assertIn("pub mod file;", local_module)
        for owner in (
            "pub struct IndexFileSnapshot",
            "pub enum IndexHeaderUpdate",
            "pub enum IndexPutOutcome",
            "pub enum IndexQueryOutcome",
            "pub const fn normalize_index_key_hash(",
            "pub const fn is_index_time_matched(",
            "pub fn drive_index_put<",
            "pub fn query_index_offsets<",
        ):
            self.assertIn(owner, local_file)
        for adapter_call in (
            "drive_index_put(",
            "query_index_offsets(",
            "normalize_index_key_hash(",
            "is_index_time_matched(",
            "self.mapped_file.get_slice(position, N)",
            "self.apply_header_update(update)",
        ):
            self.assertIn(adapter_call, store_file_production)
        for duplicate_driver in (
            "while phy_offsets.len() < max_num",
            "let mut next_index_to_read",
            "let mut time_diff",
            "if hash_code == i32::MIN",
            "slot_value <= INVALID_INDEX",
            "abs_slot_pos + INDEX_HASH_SLOT_SIZE",
            "abs_index_pos + INDEX_ENTRY_SIZE",
        ):
            self.assertNotIn(duplicate_driver, store_file_production)
        for test_name in (
            "put_driver_writes_entry_and_slot_before_the_legacy_header_sequence",
            "put_driver_normalizes_chain_head_and_clamps_time_diff",
            "put_driver_stops_before_io_when_full_or_invalid",
            "query_driver_walks_collision_chain_and_honors_time_and_result_limits",
            "query_driver_reports_unavailable_storage_without_losing_prior_results",
            "hash_and_time_helpers_preserve_legacy_edge_semantics",
        ):
            self.assertIn(f"fn {test_name}()", local_file_tests)
        self.assertIn(
            "fn query_reads_entries_from_the_second_half_of_the_index_file()",
            store_file,
        )

        for module in ("pub mod service;", "pub mod dispatch;"):
            self.assertIn(module, local_module)
        for owner in (
            "pub struct IndexServiceRoot<A>",
            "pub trait IndexServiceFile",
            "pub struct QueryOffsetResult",
            "pub fn query_index_files<",
            "pub fn restore_index_safe_offset<",
            "pub fn expired_index_file_count<",
            "pub fn plan_last_index_file<",
            "pub fn retry_index_file_create<",
            "pub fn drive_index_service_put<",
            "pub fn plan_index_build(",
            "pub fn drive_index_build_keys<",
            "pub fn build_index_key_into<'a>(",
        ):
            self.assertIn(owner, local_service)
        self.assertIn("pub struct IndexDispatchRoot<A>", local_dispatch)
        self.assertIn(
            "root: IndexServiceRoot<IndexServiceAdapter>", store_service_production
        )
        self.assertIn("impl Deref for IndexService", store_service_production)
        for adapter_call in (
            "shutdown_index_files(",
            "restore_index_safe_offset(",
            "total_index_file_size(",
            "max_index_dispatch_offset(",
            "expired_index_file_count(",
            "query_index_files(",
            "plan_index_build(",
            "drive_index_build_keys(",
            "drive_index_service_put(",
            "retry_index_file_create(",
            "plan_last_index_file(",
            "index_flush_checkpoint_timestamp(",
        ):
            self.assertIn(adapter_call, store_service_production)
        for duplicate_driver in (
            "for i in (1..=index_file_list.len()).rev()",
            "for attempt in 1..=MAX_TRY",
            "while phy_offsets.len()",
            "fn build_key_into<'a>",
            "fn index_build_key_capacity(",
            ".take_while(|f| (f.get_end_phy_offset()",
        ):
            self.assertNotIn(duplicate_driver, store_service_production)
        self.assertIn(
            "pub use rocketmq_store_local::index::service::QueryOffsetResult;",
            store_query_result,
        )
        self.assertNotIn("pub struct QueryOffsetResult", store_query_result)
        self.assertIn(
            "root: IndexDispatchRoot<IndexDispatchAdapter>",
            store_dispatch_production,
        )
        self.assertIn("self.root.dispatch(", store_dispatch_production)
        self.assertIn(".progress(enabled,", store_dispatch_production)
        self.assertNotIn(".message_index_enable.then(", store_dispatch_production)
        for test_name in (
            "service_root_preserves_adapter_identity_and_mutability",
            "query_driver_walks_newest_first_and_reports_newest_metadata",
            "load_and_expiration_plans_keep_safe_offset_and_newest_file_rules",
            "creation_retry_rollover_and_flush_plans_preserve_legacy_order",
            "build_preflight_and_key_driver_preserve_skip_and_key_order",
            "key_builders_reuse_exact_capacity_and_format",
            "dispatch_root_gates_build_and_progress_with_one_adapter",
        ):
            self.assertIn(f"fn {test_name}()", local_service_tests)
        for test_name in (
            "disabled_index_dispatch_skips_build_and_progress",
            "enabled_index_dispatch_builds_and_projects_progress",
        ):
            self.assertIn(f"fn {test_name}()", store_dispatch)


if __name__ == "__main__":
    unittest.main()

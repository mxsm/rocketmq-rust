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


class M06ConsumeQueueLocalContractTests(unittest.TestCase):
    def test_canonical_20_byte_record_owner_and_store_adapter_are_unique(self) -> None:
        local_root = source("rocketmq-store-local/src/lib.rs")
        local_module = source("rocketmq-store-local/src/consume_queue/mod.rs")
        local_record = source("rocketmq-store-local/src/consume_queue/record.rs")
        local_tests = source("rocketmq-store-local/tests/consume_queue_record.rs")
        local_single = source("rocketmq-store-local/src/consume_queue/single.rs")
        local_single_tests = source(
            "rocketmq-store-local/tests/single_consume_queue_search.rs"
        )
        local_batch = source("rocketmq-store-local/src/consume_queue/batch.rs")
        local_batch_tests = source(
            "rocketmq-store-local/tests/batch_consume_queue_kernel.rs"
        )
        local_extension = source(
            "rocketmq-store-local/src/consume_queue/extension.rs"
        )
        local_extension_tests = source(
            "rocketmq-store-local/tests/consume_queue_extension_kernel.rs"
        )
        local_cq_root = source("rocketmq-store-local/src/consume_queue/root.rs")
        local_root_tests = source(
            "rocketmq-store-local/tests/consume_queue_root.rs"
        )
        store_queue = source("rocketmq-store/src/queue/single_consume_queue.rs")
        store_batch = source("rocketmq-store/src/queue/batch_consume_queue.rs")
        store_extension = source("rocketmq-store/src/queue/consume_queue_ext.rs")
        store_extension_unit = source(
            "rocketmq-store/src/consume_queue/cq_ext_unit.rs"
        )
        store_root = source(
            "rocketmq-store/src/queue/local_file_consume_queue_store.rs"
        )
        store_dispatcher = source(
            "rocketmq-store/src/queue/build_consume_queue.rs"
        )

        self.assertIn("pub mod consume_queue;", local_root)
        self.assertIn("pub mod record;", local_module)
        for owner in (
            "pub const CQ_STORE_UNIT_SIZE: i32 = 20;",
            "pub const MSG_TAG_OFFSET_INDEX: i32 = 12;",
            "pub struct ConsumeQueueRecord",
            "pub fn encode(self) -> [u8; CQ_STORE_UNIT_SIZE as usize]",
            "pub fn decode(bytes: &[u8]) -> Option<Self>",
            "pub fn decode_at(bytes: &[u8], relative_offset: usize) -> Option<Self>",
        ):
            self.assertIn(owner, local_record)

        self.assertIn(
            "pub use rocketmq_store_local::consume_queue::record::CQ_STORE_UNIT_SIZE;",
            store_queue,
        )
        self.assertIn(
            "pub use rocketmq_store_local::consume_queue::record::MSG_TAG_OFFSET_INDEX;",
            store_queue,
        )
        self.assertNotIn("pub const CQ_STORE_UNIT_SIZE", store_queue)
        self.assertNotIn("pub const MSG_TAG_OFFSET_INDEX", store_queue)
        for adapter_call in (
            "ConsumeQueueRecord::new(offset, size, tags_code).encode()",
            "ConsumeQueueRecord::pre_blank().encode()",
            "ConsumeQueueRecord::decode(",
        ):
            self.assertIn(adapter_call, store_queue)
        for duplicate_codec in (
            ".put_i64(",
            ".put_i32(",
            ".get_i64(",
            ".get_i32(",
            "i64::from_be_bytes",
            "i32::from_be_bytes",
        ):
            self.assertNotIn(duplicate_codec, store_queue)

        for test_name in (
            "record_encoding_matches_the_java_20_byte_golden",
            "signed_record_round_trip_and_written_semantics_are_stable",
            "decode_at_is_bounded_and_does_not_require_an_exact_outer_slice",
            "pre_blank_record_keeps_the_existing_storage_sentinel",
        ):
            self.assertIn(f"fn {test_name}()", local_tests)

        for owner in (
            "pub enum ConsumeQueueTimeBoundary",
            "pub struct ConsumeQueueRecoveryScan",
            "pub struct ConsumeQueueMinOffsetMatch",
            "pub enum ConsumeQueueTruncatePlan",
            "pub fn find_min_offset_record<",
            "pub fn plan_truncate_records<",
            "pub fn scan_recovery_records<",
            "pub fn search_queue_offset_by_time<",
        ):
            self.assertIn(owner, local_single)
        self.assertIn("pub mod single;", local_module)
        self.assertIn("search_queue_offset_by_time(", store_queue)
        self.assertIn("scan_recovery_records(", store_queue)
        self.assertIn("find_min_offset_record(", store_queue)
        self.assertIn("plan_truncate_records(", store_queue)
        for duplicate_driver in (
            "fn middle_queue_offset(",
            "let mut timestamp_cache",
            "for index in 0..(mapped_file_size_logics / CQ_STORE_UNIT_SIZE)",
            "for index in 0..(mapped_file_size / CQ_STORE_UNIT_SIZE)",
            "while pos + CQ_STORE_UNIT_SIZE <= read_position",
        ):
            self.assertNotIn(duplicate_driver, store_queue)

        for test_name in (
            "exact_duplicate_timestamp_honors_lower_and_upper_boundaries",
            "missing_timestamp_returns_the_enclosing_queue_boundaries",
            "timestamps_outside_the_file_keep_legacy_edge_offsets",
            "failed_midpoint_lookup_reports_the_same_physical_offset",
            "recovery_scan_returns_the_complete_written_prefix_and_last_progress",
            "recovery_scan_stops_before_invalid_or_missing_records",
            "min_offset_scan_selects_the_first_written_record_at_the_boundary",
            "truncate_plan_deletes_from_the_first_boundary_or_retains_the_prefix",
            "truncate_plan_preserves_the_first_record_special_case",
        ):
            self.assertIn(f"fn {test_name}()", local_single_tests)

        for module in ("pub mod batch;", "pub mod extension;"):
            self.assertIn(module, local_module)
        for owner in (
            "pub const BATCH_CQ_STORE_UNIT_SIZE: i32 = 46;",
            "pub struct BatchConsumeQueueRecord",
            "pub fn find_batch_record_in_files<",
            "pub fn scan_batch_recovery<",
            "pub fn plan_batch_truncate<",
            "pub fn correct_batch_min_offsets<",
            "pub fn search_batch_offset_by_time<",
        ):
            self.assertIn(owner, local_batch)
        for adapter_call in (
            "find_batch_record_in_files(",
            "scan_batch_recovery(",
            "plan_batch_truncate(",
            "correct_batch_min_offsets(",
            "search_batch_offset_by_time(",
        ):
            self.assertIn(adapter_call, store_batch)
        batch_production = store_batch.split("#[cfg(test)]", 1)[0]
        for duplicate in (
            "fn encode_unit(",
            "struct BatchQueueEntry",
            "const MSG_STORE_TIME_OFFSET_INDEX",
            "while mapped_file_offset + CQ_STORE_UNIT_SIZE <= read_position",
        ):
            self.assertNotIn(duplicate, batch_production)
        for test_name in (
            "batch_record_round_trip_keeps_the_exact_46_byte_layout",
            "batch_record_validation_and_end_offsets_match_the_persisted_fields",
            "batch_record_search_returns_the_last_base_not_greater_than_the_target",
            "batch_recovery_and_truncate_select_the_same_valid_prefix",
            "batch_minimum_offset_correction_advances_over_expired_batches",
            "batch_timestamp_search_preserves_lower_and_upper_boundaries",
        ):
            self.assertIn(f"fn {test_name}()", local_batch_tests)

        for owner in (
            "pub struct CqExtUnit",
            "pub fn is_cq_ext_address(",
            "pub fn decorate_cq_ext_offset(",
            "pub fn undecorate_cq_ext_address(",
            "pub fn plan_cq_ext_append(",
            "pub fn scan_cq_ext_recovery(",
        ):
            self.assertIn(owner, local_extension)
        for reexport in (
            "pub use rocketmq_store_local::consume_queue::extension::CqExtUnit;",
            "pub use rocketmq_store_local::consume_queue::extension::MAX_EXT_UNIT_SIZE;",
            "pub use rocketmq_store_local::consume_queue::extension::MIN_EXT_UNIT_SIZE;",
        ):
            self.assertIn(reexport, store_extension_unit)
        self.assertNotIn("pub struct CqExtUnit", store_extension_unit)
        for adapter_call in (
            "decorate_cq_ext_offset(",
            "undecorate_cq_ext_address(",
            "plan_cq_ext_append(",
            "scan_cq_ext_recovery(",
            "cq_ext_file_before_min(",
        ):
            self.assertIn(adapter_call, store_extension)
        for duplicate in (
            "const MAX_ADDR:",
            "const MAX_REAL_OFFSET:",
            "ext_unit.read_by_skip(",
        ):
            self.assertNotIn(duplicate, store_extension)
        for test_name in (
            "extension_unit_round_trip_preserves_bitmap_and_exact_size",
            "extension_unit_rejects_missing_invalid_and_truncated_records",
            "extension_unit_mutators_write_to_and_skip_remain_compatible",
            "extension_address_decoration_round_trips_the_real_offset",
            "extension_recovery_scan_stops_before_dirty_or_partial_tail",
            "extension_append_and_file_retention_plans_keep_legacy_boundaries",
        ):
            self.assertIn(f"fn {test_name}()", local_extension_tests)

        self.assertIn("pub mod root;", local_module)
        for owner in (
            "pub struct ConsumeQueueRoot<A>",
            "pub type ConsumeQueueStoreRoot<A>",
            "pub struct ConsumeQueueDispatchRoot<A>",
            "pub enum ConsumeQueueDispatchMode",
            "pub enum ConsumeQueueDispatchOutcome",
            "pub fn drive_consume_queue_dispatch<",
            "pub fn find_or_create_consume_queue<",
            "pub fn clamp_consume_queue_offset(",
        ):
            self.assertIn(owner, local_cq_root)
        self.assertIn(
            "inner: ConsumeQueueStoreRoot<ArcMut<Inner>>",
            store_root,
        )
        self.assertIn("drive_find_or_create_consume_queue(", store_root)
        self.assertIn("clamp_consume_queue_offset(", store_root)
        self.assertIn(
            "root: ConsumeQueueDispatchRoot<ConsumeQueueStore>",
            store_dispatcher,
        )
        self.assertIn("self.root.dispatch(", store_dispatcher)
        self.assertIn(".progress_offset(", store_dispatcher)
        self.assertIn(
            "fn prepared_and_rollback_transactions_skip_consume_queue_dispatch()",
            store_dispatcher,
        )
        for adapter in (store_queue, store_batch):
            self.assertIn("drive_consume_queue_dispatch(", adapter)
        for duplicate in (
            "while i < max_retries && can_write",
            "for _ in 0..30",
            "// Fast path: check if queue already exists",
            "//Insert into table with triple-check pattern",
        ):
            self.assertNotIn(duplicate, store_queue + store_batch + store_root)
        for test_name in (
            "consume_queue_root_preserves_adapter_identity_and_mutability",
            "dispatcher_root_gates_transactions_and_projects_progress",
            "single_dispatch_retries_until_append_succeeds",
            "batch_dispatch_rejects_invalid_metadata_without_calling_adapter",
            "dispatch_distinguishes_not_writeable_and_exhausted",
            "find_or_create_skips_construction_on_hit_and_publishes_on_miss",
            "queue_offset_clamp_keeps_results_inside_current_bounds",
        ):
            self.assertIn(f"fn {test_name}()", local_root_tests)
        self.assertIn(
            "fn batch_consume_queue_rejects_invalid_dispatch_before_creating_a_file()",
            store_batch,
        )


if __name__ == "__main__":
    unittest.main()

// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::Bytes;
use rocketmq_store::transfer::batch::TransferPlan;
use rocketmq_store::transfer::planner::TransferPlanInput;
use rocketmq_store::transfer::planner::TransferPlanner;
use rocketmq_store::transfer::planner::DEFAULT_TRANSFER_BATCH_SIZE;
use rocketmq_store::transfer::segment::SegmentLease;
use rocketmq_store::transfer::segment::TransferCacheState;

#[test]
fn zero_configured_batch_size_uses_default_without_zero_data_frame() {
    let input = TransferPlanInput {
        requested_offset: 0,
        next_transfer_offset: 0,
        max_commit_log_offset: (DEFAULT_TRANSFER_BATCH_SIZE * 2) as i64,
        configured_max_batch_bytes: 0,
        flow_control_available_bytes: usize::MAX,
        mapped_file_size: 1024 * 1024 * 1024,
        allow_cross_file_batch: false,
        heartbeat_due: false,
    };

    let plan = TransferPlanner::plan(input, |offset, max_bytes, allow_cross_file| {
        assert_eq!(offset, 0);
        assert_eq!(max_bytes, DEFAULT_TRANSFER_BATCH_SIZE);
        assert!(!allow_cross_file);
        Ok(vec![SegmentLease::from_bytes(
            0,
            0,
            Bytes::from(vec![1; DEFAULT_TRANSFER_BATCH_SIZE]),
            TransferCacheState::Hot,
        )])
    })
    .expect("planner should build data plan");

    let TransferPlan::Data(batch) = plan else {
        panic!("expected data batch");
    };
    assert_eq!(batch.total_body_len, DEFAULT_TRANSFER_BATCH_SIZE);
    assert_eq!(batch.next_offset, DEFAULT_TRANSFER_BATCH_SIZE as i64);
    assert!(!batch.segments.is_empty());
}

#[test]
fn flow_control_zero_returns_no_data_without_selecting_segments() {
    let input = TransferPlanInput {
        requested_offset: 0,
        next_transfer_offset: 0,
        max_commit_log_offset: 4096,
        configured_max_batch_bytes: 4096,
        flow_control_available_bytes: 0,
        mapped_file_size: 1024 * 1024 * 1024,
        allow_cross_file_batch: false,
        heartbeat_due: false,
    };
    let mut selector_called = false;

    let plan = TransferPlanner::plan(input, |_, _, _| {
        selector_called = true;
        Ok(Vec::new())
    })
    .expect("flow control should not be an error");

    assert!(matches!(plan, TransferPlan::NoData));
    assert!(!selector_called);
}

#[test]
fn single_file_plan_caps_batch_at_mapped_file_boundary() {
    let input = TransferPlanInput {
        requested_offset: 900,
        next_transfer_offset: 900,
        max_commit_log_offset: 4096,
        configured_max_batch_bytes: 512,
        flow_control_available_bytes: 512,
        mapped_file_size: 1024,
        allow_cross_file_batch: false,
        heartbeat_due: false,
    };

    let plan = TransferPlanner::plan(input, |offset, max_bytes, allow_cross_file| {
        assert_eq!(offset, 900);
        assert_eq!(max_bytes, 124);
        assert!(!allow_cross_file);
        Ok(vec![SegmentLease::from_bytes(
            900,
            900,
            Bytes::from(vec![7; 124]),
            TransferCacheState::Cold,
        )])
    })
    .expect("planner should cap at file boundary");

    let TransferPlan::Data(batch) = plan else {
        panic!("expected data batch");
    };
    assert_eq!(batch.total_body_len, 124);
    assert_eq!(batch.next_offset, 1024);
}

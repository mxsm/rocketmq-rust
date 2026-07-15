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

use rocketmq_store_local::commit_log::append_frame::AppendFrameCrcPlan;
use rocketmq_store_local::commit_log::append_frame::AppendFrameKernel;
use rocketmq_store_local::commit_log::append_frame::HostWidth;
use rocketmq_store_local::commit_log::append_frame::SegmentAppendDecision;
use rocketmq_store_local::commit_log::record::BLANK_MAGIC_CODE;

#[test]
fn finalizes_ipv4_runtime_fields_at_legacy_offsets() {
    let mut frame = vec![0xA5; 96];
    let plan = AppendFrameKernel::finalize_frame(
        &mut frame,
        0x0102_0304_0506_0708,
        0x1112_1314_1516_1718,
        0x2122_2324_2526_2728,
        HostWidth::Ipv4,
        4,
    );

    assert_eq!(&frame[20..28], &0x0102_0304_0506_0708_i64.to_be_bytes());
    assert_eq!(&frame[28..36], &0x1112_1314_1516_1718_i64.to_be_bytes());
    assert_eq!(&frame[56..64], &0x2122_2324_2526_2728_i64.to_be_bytes());
    assert_eq!(
        plan,
        AppendFrameCrcPlan::Trailer {
            covered_end: 92,
            trailer_start: 92,
            trailer_end: 96,
        }
    );
}

#[test]
fn finalizes_ipv6_store_timestamp_at_legacy_offset() {
    let mut frame = vec![0x5A; 104];
    let plan = AppendFrameKernel::finalize_frame(&mut frame, 7, 11, 13, HostWidth::Ipv6, 0);

    assert_eq!(&frame[20..28], &7_i64.to_be_bytes());
    assert_eq!(&frame[28..36], &11_i64.to_be_bytes());
    assert_eq!(&frame[68..76], &13_i64.to_be_bytes());
    assert_eq!(plan, AppendFrameCrcPlan::Disabled);
}

#[test]
fn standard_and_zero_copy_share_the_same_crc_range_plan() {
    let mut standard = vec![0; 128];
    let mut zero_copy = vec![0; 128];

    let standard_plan = AppendFrameKernel::finalize_frame(&mut standard, 1, 2, 3, HostWidth::Ipv4, 4);
    let zero_copy_plan = AppendFrameKernel::finalize_frame(&mut zero_copy, 1, 2, 3, HostWidth::Ipv4, 4);

    assert_eq!(standard_plan, zero_copy_plan);
    assert_eq!(standard, zero_copy);
}

#[test]
fn batch_finalization_patches_fields_but_keeps_crc_as_no_op() {
    let mut frame = vec![0; 104];
    let plan = AppendFrameKernel::finalize_batch_frame(&mut frame, 17, 19, 23, HostWidth::Ipv6);

    assert_eq!(&frame[20..28], &17_i64.to_be_bytes());
    assert_eq!(&frame[28..36], &19_i64.to_be_bytes());
    assert_eq!(&frame[68..76], &23_i64.to_be_bytes());
    assert_eq!(plan, AppendFrameCrcPlan::Disabled);
}

#[test]
fn transaction_queue_offset_is_written_as_store_selected_zero() {
    let mut frame = vec![0; 96];
    AppendFrameKernel::finalize_frame(&mut frame, 0, 29, 31, HostWidth::Ipv4, 0);

    assert_eq!(&frame[20..28], &0_i64.to_be_bytes());
}

#[test]
fn rolls_only_when_encoded_length_plus_eight_exceeds_max_blank() {
    assert_eq!(
        AppendFrameKernel::segment_append_decision(92, 100),
        SegmentAppendDecision::Append
    );

    let SegmentAppendDecision::Roll = AppendFrameKernel::segment_append_decision(93, 100) else {
        panic!("93 + 8 must roll a 100-byte segment");
    };
    let marker = AppendFrameKernel::blank_marker(100);
    assert_eq!(marker.bytes().len(), 8);
    assert_eq!(marker.declared_wrote_bytes(), 100);
    assert_eq!(&marker.bytes()[0..4], &100_i32.to_be_bytes());
    assert_eq!(&marker.bytes()[4..8], &BLANK_MAGIC_CODE.to_be_bytes());
}

#[test]
fn blank_marker_declares_the_whole_remainder_but_contains_only_eight_bytes() {
    let marker = AppendFrameKernel::blank_marker(4096);

    assert_eq!(marker.bytes().len(), 8);
    assert_eq!(marker.declared_wrote_bytes(), 4096);
    assert_eq!(marker.bytes(), &[0, 0, 0x10, 0, 0xCB, 0xD4, 0x31, 0x94]);
}

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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_runtime::resource_budget::BudgetedItem;
use rocketmq_store_local::commit_log::append::finalized_append::FinalizedAppend;
use rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy;
use rocketmq_store_local::commit_log::append::prepared_payload::PreparedPayload;
use rocketmq_store_local::commit_log::append::sequencer::AppendSequencer;
use rocketmq_store_local::commit_log::append::sequencer::AppendSequencerConfig;
use rocketmq_store_local::commit_log::append_frame::AppendFrameCrcPlan;
use rocketmq_store_local::commit_log::append_frame::AppendFrameKernel;
use rocketmq_store_local::commit_log::append_frame::SegmentAppendDecision;
use tokio_util::sync::CancellationToken;

const QUEUE_OFFSET_POSITION: usize = 20;
const PHYSICAL_OFFSET_POSITION: usize = 28;
const STORE_TIMESTAMP_POSITION: usize = 56;

fn encoded_frame(len: usize, crc_trailer_bytes: usize) -> Bytes {
    assert!(len >= 80 + crc_trailer_bytes);
    let mut frame = BytesMut::zeroed(len);
    frame[..4].copy_from_slice(&(len as i32).to_be_bytes());
    frame.freeze()
}

fn encoded_batch(frame_lengths: &[usize], crc_trailer_bytes: usize) -> Bytes {
    let mut payload = BytesMut::new();
    for len in frame_lengths {
        payload.put(encoded_frame(*len, crc_trailer_bytes));
    }
    payload.freeze()
}

fn read_i64(bytes: &[u8], start: usize) -> i64 {
    i64::from_be_bytes(bytes[start..start + 8].try_into().expect("i64 bytes"))
}

fn sequencer_config(policy: MicroBatchPolicy) -> AppendSequencerConfig {
    AppendSequencerConfig {
        queue_capacity: 16,
        queue_bytes: 4096,
        micro_batch: policy,
    }
}

#[tokio::test]
async fn enabled_micro_batch_preserves_fifo_and_both_drain_limits() {
    let policy = MicroBatchPolicy::try_new(3, 240, Duration::ZERO).expect("policy");
    let (sender, mut receiver) = AppendSequencer::bounded(sequencer_config(policy)).expect("sequencer");
    for id in 0..5 {
        sender.try_submit(id, 80).expect("admit request");
    }
    let cancellation = CancellationToken::new();

    let first = receiver.next_batch(&cancellation).await.expect("first batch");
    let second = receiver.next_batch(&cancellation).await.expect("second batch");
    let first_ids = first
        .into_budgeted_items()
        .into_iter()
        .map(BudgetedItem::into_item)
        .collect::<Vec<_>>();
    let second_ids = second
        .into_budgeted_items()
        .into_iter()
        .map(BudgetedItem::into_item)
        .collect::<Vec<_>>();

    assert_eq!(first_ids, vec![0, 1, 2]);
    assert_eq!(second_ids, vec![3, 4]);
}

#[tokio::test]
async fn disabled_micro_batch_keeps_single_writer_and_one_result_per_drain() {
    let policy = MicroBatchPolicy::disabled(4096).expect("policy");
    let (sender, mut receiver) = AppendSequencer::bounded(sequencer_config(policy)).expect("sequencer");
    sender.try_submit("first", 80).expect("first");
    sender.try_submit("second", 80).expect("second");
    let cancellation = CancellationToken::new();

    let first = receiver.next_batch(&cancellation).await.expect("first drain");
    let second = receiver.next_batch(&cancellation).await.expect("second drain");

    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(
        first.into_budgeted_items().pop().map(BudgetedItem::into_item),
        Some("first")
    );
    assert_eq!(
        second.into_budgeted_items().pop().map(BudgetedItem::into_item),
        Some("second")
    );
}

#[test]
fn finalized_batch_assigns_contiguous_offsets_and_finalizes_each_checksum() {
    const CRC_TRAILER_BYTES: usize = 4;
    let prepared =
        PreparedPayload::try_batch(encoded_batch(&[84, 92], CRC_TRAILER_BYTES), CRC_TRAILER_BYTES).expect("prepare");
    let finalized = FinalizedAppend::try_new(&prepared, 70, 4096, 123_456).expect("finalize");
    let mut destination = vec![0; finalized.required_bytes()];
    let mut checksum_calls = 0;

    finalized
        .write_into(&mut destination, |frame, plan| {
            let AppendFrameCrcPlan::Trailer {
                covered_end,
                trailer_start,
                trailer_end,
            } = plan
            else {
                panic!("checksum trailer expected");
            };
            checksum_calls += 1;
            let checksum = frame[..covered_end]
                .iter()
                .fold(0_u32, |sum, byte| sum.wrapping_add(u32::from(*byte)));
            frame[trailer_start..trailer_end].copy_from_slice(&checksum.to_be_bytes());
        })
        .expect("write finalized payload");

    assert_eq!(checksum_calls, 2);
    assert_eq!(read_i64(&destination, QUEUE_OFFSET_POSITION), 70);
    assert_eq!(read_i64(&destination, PHYSICAL_OFFSET_POSITION), 4096);
    assert_eq!(read_i64(&destination, STORE_TIMESTAMP_POSITION), 123_456);
    assert_eq!(read_i64(&destination[84..], QUEUE_OFFSET_POSITION), 71);
    assert_eq!(read_i64(&destination[84..], PHYSICAL_OFFSET_POSITION), 4180);
}

#[test]
fn malformed_encoded_batch_fails_before_any_reservation() {
    let reservation_count = AtomicUsize::new(0);
    let mut malformed = encoded_frame(80, 0).to_vec();
    malformed[..4].copy_from_slice(&81_i32.to_be_bytes());

    let prepared = PreparedPayload::try_batch(Bytes::from(malformed), 0);
    if prepared.is_ok() {
        reservation_count.fetch_add(1, Ordering::Relaxed);
    }

    assert!(prepared.is_err());
    assert_eq!(reservation_count.load(Ordering::Relaxed), 0);
}

#[test]
fn rollover_is_decided_before_runtime_patch_or_copy() {
    let prepared = PreparedPayload::try_single(encoded_frame(96, 0), 0).expect("prepare");

    assert_eq!(
        AppendFrameKernel::segment_append_decision(prepared.retained_bytes() as i32, 103),
        SegmentAppendDecision::Roll
    );
    assert_eq!(
        AppendFrameKernel::segment_append_decision(prepared.retained_bytes() as i32, 104),
        SegmentAppendDecision::Append
    );
}

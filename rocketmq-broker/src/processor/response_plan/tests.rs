// Copyright 2026 The RocketMQ Rust Authors
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

use cheetah_string::CheetahString;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_store::DefaultMappedFile;
use rocketmq_store::MappedFile;
use rocketmq_transport::api::v2::ResponseBodyKind;

use super::*;

fn response_head() -> RemotingCommand {
    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
}

struct DropBytes {
    bytes: &'static [u8],
    drops: Arc<AtomicUsize>,
}

struct CountingFileLease {
    file: File,
    drops: Arc<AtomicUsize>,
}

impl FileRegionLease for CountingFileLease {
    fn file(&self) -> &File {
        &self.file
    }
}

impl Drop for CountingFileLease {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct CountingStoreLease {
    inner: StoreFileRegionLease,
    drops: Arc<AtomicUsize>,
}

impl FileRegionLease for CountingStoreLease {
    fn file(&self) -> &File {
        self.inner.file()
    }
}

impl Drop for CountingStoreLease {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl AsRef<[u8]> for DropBytes {
    fn as_ref(&self) -> &[u8] {
        self.bytes
    }
}

impl Drop for DropBytes {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn response_parts_reject_every_invalid_head_before_storing_an_owner() {
    let with_body = response_head().set_body(Bytes::from_static(b"body"));
    assert!(matches!(
        BrokerResponseParts::command(with_body),
        Err(BrokerResponseBuildError::ResponsePlan(ResponsePlanError::HeadHasBody))
    ));

    assert!(matches!(
        BrokerResponseParts::command(RemotingCommand::create_remoting_command(11)),
        Err(BrokerResponseBuildError::ResponsePlan(ResponsePlanError::RequestHead))
    ));

    assert!(matches!(
        BrokerResponseParts::command(response_head().mark_oneway_rpc()),
        Err(BrokerResponseBuildError::ResponsePlan(ResponsePlanError::OneWayHead))
    ));
}

#[test]
fn bytes_owner_moves_into_the_plan_without_a_builder_copy() {
    let drops = Arc::new(AtomicUsize::new(0));
    let body = Bytes::from_owner(DropBytes {
        bytes: b"owner-backed-body",
        drops: Arc::clone(&drops),
    });
    let pointer = body.as_ptr();

    let parts = BrokerResponseParts::bytes(response_head(), body).expect("valid byte response parts");
    let BrokerResponseBodyOwner::Bytes(stored) = parts.body() else {
        panic!("non-empty bytes must remain the response owner");
    };
    assert_eq!(stored.as_ptr(), pointer);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    let plan = parts.into_response_plan().expect("valid byte response plan");
    assert_eq!(plan.body_kind(), ResponseBodyKind::Bytes);
    assert_eq!(plan.body_len(), 17);
    assert_eq!(plan.body_part_count(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(plan);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn segments_are_body_only_ordered_and_move_their_backing_allocations() {
    let first = Bytes::from_static(b"first");
    let second = Bytes::from_static(b"second");
    let first_pointer = first.as_ptr();
    let second_pointer = second.as_ptr();

    let parts = BrokerResponseParts::segments(response_head(), vec![first, Bytes::new(), second])
        .expect("valid segmented response parts");
    let BrokerResponseBodyOwner::Segments(stored) = parts.body() else {
        panic!("non-empty segments must remain segmented");
    };
    assert_eq!(stored.len(), 3);
    assert_eq!(stored[0].as_ptr(), first_pointer);
    assert!(stored[1].is_empty());
    assert_eq!(stored[2].as_ptr(), second_pointer);
    assert_eq!(stored[0], b"first"[..]);
    assert_eq!(stored[2], b"second"[..]);

    let plan = parts.into_response_plan().expect("valid segmented response plan");
    assert_eq!(plan.body_kind(), ResponseBodyKind::Segments);
    assert_eq!(plan.body_len(), 11);
    assert_eq!(plan.body_part_count(), 2);
}

#[test]
fn empty_bytes_and_segments_normalize_to_an_empty_command_plan() {
    let byte_plan = BrokerResponseParts::bytes(response_head(), Bytes::new())
        .expect("valid empty bytes")
        .into_response_plan()
        .expect("valid empty byte plan");
    let segment_plan = BrokerResponseParts::segments(response_head(), vec![Bytes::new()])
        .expect("valid empty segments")
        .into_response_plan()
        .expect("valid empty segment plan");

    assert_eq!(byte_plan.body_kind(), ResponseBodyKind::Empty);
    assert_eq!(segment_plan.body_kind(), ResponseBodyKind::Empty);
}

#[test]
fn handler_outcome_seam_exposes_reply_metadata() {
    let outcome = BrokerResponseParts::bytes(response_head(), Bytes::from_static(b"reply"))
        .expect("valid reply parts")
        .into_handler_outcome()
        .expect("valid reply outcome");

    let HandlerOutcome::Reply(plan) = outcome else {
        panic!("immediate Broker responses must become Reply outcomes");
    };
    assert_eq!(plan.response_code(), ResponseCode::Success as i32);
    assert_eq!(plan.body_kind(), ResponseBodyKind::Bytes);
    assert_eq!(plan.body_len(), 5);
}

#[test]
fn all_store_file_ranges_become_one_affine_file_region_sequence() {
    let directory = tempfile::tempdir().expect("temporary response-plan range directory");
    let path = directory.path().join("00000000000000000000");
    let mapped_file =
        DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 64).expect("mapped file");
    assert!(mapped_file.append_message_bytes(b"ordered-regions"));

    let first = mapped_file
        .try_file_range_selection(0, 8)
        .expect("first selection")
        .expect("published first range");
    let second = mapped_file
        .try_file_range_selection(8, 7)
        .expect("second selection")
        .expect("published second range");
    assert!(!first.has_byte_snapshot());
    assert!(!second.has_byte_snapshot());

    let parts = store_response_parts(response_head(), vec![first, second]).expect("file-region response parts");
    assert!(matches!(
        parts.body(),
        BrokerResponseBodyOwner::FileRegions(regions) if regions.len() == 15
    ));

    let plan = parts.into_response_plan().expect("file-region response plan");
    assert_eq!(plan.body_kind(), ResponseBodyKind::FileRegions);
    assert_eq!(plan.body_len(), 15);
    assert_eq!(plan.body_part_count(), 2);
}

#[test]
fn file_region_lease_moves_from_builder_to_plan_and_drops_once() {
    let file = tempfile::tempfile().expect("temporary response-plan file");
    file.set_len(16).expect("size temporary response-plan file");
    let drops = Arc::new(AtomicUsize::new(0));
    let region = FileRegion::try_new(
        Arc::new(CountingFileLease {
            file,
            drops: Arc::clone(&drops),
        }),
        0,
        16,
    )
    .expect("validated counting region");
    let regions = FileRegionSequence::try_new(vec![region]).expect("validated counting sequence");

    let parts = BrokerResponseParts::file_regions(response_head(), regions).expect("file-region response parts");
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let plan = parts.into_response_plan().expect("file-region response plan");
    assert_eq!(plan.body_kind(), ResponseBodyKind::FileRegions);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    drop(plan);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn mixed_store_sources_fall_back_to_all_segments_without_reordering() {
    let directory = tempfile::tempdir().expect("temporary mixed response-plan directory");
    let path = directory.path().join("00000000000000000000");
    let mapped_file =
        DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 64).expect("mapped file");
    assert!(mapped_file.append_message_bytes(b"file-first"));

    let file_selection = mapped_file
        .try_file_range_selection(0, 10)
        .expect("file selection")
        .expect("published file range");
    let bytes = Bytes::from_static(b"bytes-second");
    let bytes_pointer = bytes.as_ptr();
    let byte_selection = SelectMappedBufferResult::from_bytes(10, bytes).expect("byte selection");

    let parts = store_response_parts(response_head(), vec![file_selection, byte_selection])
        .expect("mixed-source response parts");
    let BrokerResponseBodyOwner::Segments(segments) = parts.body() else {
        panic!("mixed selections must use the all-segments fallback");
    };
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0], b"file-first"[..]);
    assert_eq!(segments[1], b"bytes-second"[..]);
    assert_eq!(segments[1].as_ptr(), bytes_pointer);
}

fn assert_injected_file_region_failure_falls_back(
    failure_index: usize,
    failure_stage: StoreFileRegionStage,
    expected_attempt_drops: usize,
) {
    let directory = tempfile::tempdir().expect("temporary injected-fallback directory");
    let path = directory.path().join("00000000000000000000");
    let mapped_file =
        DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 64).expect("mapped file");
    assert!(mapped_file.append_message_bytes(b"firstsecond"));

    let first = mapped_file
        .try_file_range_selection(0, 5)
        .expect("first selection")
        .expect("first range");
    let second = mapped_file
        .try_file_range_selection(5, 6)
        .expect("second selection")
        .expect("second range");
    assert!(!first.has_byte_snapshot());
    assert!(!second.has_byte_snapshot());

    let attempt_drops = Arc::new(AtomicUsize::new(0));
    let attempt_drops_for_lease = Arc::clone(&attempt_drops);
    let parts = store_response_parts_with(
        response_head(),
        vec![first, second],
        |index, stage| !(index == failure_index && stage == failure_stage),
        move |handle| -> Arc<dyn FileRegionLease> {
            Arc::new(CountingStoreLease {
                inner: StoreFileRegionLease::new(handle),
                drops: Arc::clone(&attempt_drops_for_lease),
            })
        },
    )
    .expect("optimization failure must use the segment fallback");

    assert_eq!(attempt_drops.load(Ordering::SeqCst), expected_attempt_drops);
    let BrokerResponseBodyOwner::Segments(segments) = parts.body() else {
        panic!("failed file-region optimization must preserve all selections as segments");
    };
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0], b"first"[..]);
    assert_eq!(segments[1], b"second"[..]);

    let plan = parts.into_response_plan().expect("fallback response plan");
    assert_eq!(plan.body_kind(), ResponseBodyKind::Segments);
    assert_eq!(plan.body_len(), 11);
    assert_eq!(plan.body_part_count(), 2);
    drop(plan);
    assert_eq!(attempt_drops.load(Ordering::SeqCst), expected_attempt_drops);
}

#[test]
fn transfer_handle_failure_after_one_region_falls_back_without_losing_owners() {
    assert_injected_file_region_failure_falls_back(1, StoreFileRegionStage::TransferHandle, 1);
}

#[test]
fn region_failure_after_one_region_falls_back_without_losing_owners() {
    assert_injected_file_region_failure_falls_back(1, StoreFileRegionStage::Region, 2);
}

#[test]
fn sequence_failure_releases_all_attempted_regions_before_segment_fallback() {
    assert_injected_file_region_failure_falls_back(2, StoreFileRegionStage::Sequence, 2);
}

#[tokio::test]
async fn legacy_heap_delivery_returns_an_explicit_command() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let stream = std::net::TcpStream::connect(address).expect("connect test stream");
    stream.set_nonblocking(true).expect("nonblocking test stream");
    let _accepted = listener.accept().expect("accept test stream");
    let connection = rocketmq_transport::test_support::Connection::new(
        tokio::net::TcpStream::from_std(stream).expect("Tokio test stream"),
    );
    let channel = rocketmq_transport::test_support::TestChannelBuilder::new(
        connection,
        crate::test_task_group("broker-response-plan-command"),
    )
    .addresses(address, address)
    .build()
    .expect("test channel");

    let delivery = BrokerResponseParts::bytes(response_head(), Bytes::from_static(b"legacy"))
        .expect("valid legacy parts")
        .deliver_legacy(&channel)
        .await
        .expect("legacy command delivery");
    let LegacyResponseDelivery::Command(command) = delivery else {
        panic!("heap compatibility delivery must return a command");
    };
    assert_eq!(command.body(), Some(&Bytes::from_static(b"legacy")));
}

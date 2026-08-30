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

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_model::common::key_builder::KeyBuilder;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_store::CqExtUnit;
use rocketmq_store::MessageFilter;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::DeferredAdmission;
use rocketmq_transport::api::DeferredExpiryMargins;
use rocketmq_transport::api::DeferredWaitLimits;

use super::*;

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test value is non-zero")
}

fn request(born_time: u64, poll_time: u64) -> PopRequestData {
    request_with_host(born_time, poll_time, CheetahString::from_static_str("127.0.0.1:10911"))
}

fn request_with_host(born_time: u64, poll_time: u64, caller_host: CheetahString) -> PopRequestData {
    PopRequestData::from_test_header(
        PopMessageRequestHeader {
            consumer_group: CheetahString::from_static_str("group"),
            topic: CheetahString::from_static_str("topic"),
            queue_id: 3,
            max_msg_nums: 8,
            invisible_time: 30_000,
            poll_time,
            born_time,
            init_mode: 0,
            exp_type: None,
            exp: None,
            order: Some(false),
            attempt_id: None,
            topic_request_header: None,
        },
        caller_host,
    )
}

fn service(max_entries: usize, per_key: usize) -> PopDeferredService {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission =
        DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(max_entries, 16 * 1024 * 1024))
            .expect("deferred admission");
    PopDeferredService::new(
        admission,
        PopCriteriaLimits::new(nonzero(max_entries), nonzero(per_key)),
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        nonzero(8),
    )
}

#[tokio::test]
async fn closed_task_group_releases_pending_offset_for_the_next_service_tick() {
    let service = service(2, 2);
    let key = criteria_key(3);
    service
        .latch_queue_offset_for_test(key.topic(), key.queue_id(), 10)
        .expect("retain offset arrival");
    assert_eq!(service.resource_snapshot().pending_arrivals, 1);

    let pending = service
        .pending_offset_reservations()
        .pop()
        .expect("first producer tick reserves replay");
    let stopped = crate::test_task_group("pop-offset-replay-spawn-rejection");
    let report = stopped.shutdown(Duration::ZERO).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert!(stopped
        .spawn(
            "rejected-pop-offset-replay",
            rocketmq_runtime::TaskKind::Worker,
            async move {
                drop(pending);
            }
        )
        .is_err());
    assert_eq!(service.resource_snapshot().pending_arrivals, 1);
    assert_eq!(service.resource_snapshot().active_continuations, 0);

    let retry = service
        .pending_offset_reservations()
        .pop()
        .expect("next producer tick retries the exact range");
    assert_eq!(retry.range().first, 10);
    assert_eq!(retry.range().last, 10);
    drop(retry);
    service.seal();
    assert_eq!(service.resource_snapshot().pending_arrivals, 0);
    assert_eq!(service.resource_snapshot().pending_arrival_bytes, 0);
}

fn criteria_key(queue_id: i32) -> PopCriteriaKey {
    PopCriteriaKey::new(
        CheetahString::from_static_str("topic"),
        CheetahString::from_static_str("group"),
        queue_id,
    )
}

fn deadline_after(base: tokio::time::Instant, millis: u64) -> LongPollingDeadline {
    LongPollingDeadline::checked(0, millis + 49, 0, base).expect("test deadline")
}

fn match_all_criteria() -> Arc<PopMatchCriteria> {
    Arc::new(PopMatchCriteria::new(None, None))
}

struct ToggleFilter {
    matches: Arc<AtomicBool>,
}

impl MessageFilter for ToggleFilter {
    fn is_matched_by_consume_queue(&self, _tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        self.matches.load(Ordering::SeqCst)
    }

    fn is_matched_by_commit_log(
        &self,
        _msg_buffer: Option<&[u8]>,
        _properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        self.matches.load(Ordering::SeqCst)
    }
}

struct CountingFilter {
    drops: Arc<AtomicUsize>,
}

impl Drop for CountingFilter {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl MessageFilter for CountingFilter {
    fn is_matched_by_consume_queue(&self, _tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        true
    }

    fn is_matched_by_commit_log(
        &self,
        _msg_buffer: Option<&[u8]>,
        _properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        true
    }
}

#[test]
fn deadline_preserves_legacy_strict_fifty_millisecond_formula() {
    let monotonic_now = tokio::time::Instant::now();
    let deadline = LongPollingDeadline::checked(1_000, 100, 1_049, monotonic_now).expect("live deadline");
    assert_eq!(deadline.protocol_millis(), 1_051);
    assert_eq!(deadline.protocol_at(), monotonic_now + Duration::from_millis(2));

    let boundary = LongPollingDeadline::checked(1_000, 100, 1_050, monotonic_now).expect("strict boundary is live");
    assert_eq!(boundary.protocol_at(), monotonic_now + Duration::from_millis(1));

    let expired = LongPollingDeadline::checked(1_000, 100, 1_051, monotonic_now).expect_err("strictly later expires");
    assert_eq!(expired.kind(), LongPollingDeadlineErrorKind::AlreadyExpired);
}

#[test]
fn deadline_rejects_zero_and_checked_protocol_overflow() {
    assert_eq!(
        LongPollingDeadline::checked(1, 0, 0, tokio::time::Instant::now())
            .expect_err("zero polling is not deferred")
            .kind(),
        LongPollingDeadlineErrorKind::ZeroPollTime
    );
    assert_eq!(
        LongPollingDeadline::checked(u64::MAX, 1, 0, tokio::time::Instant::now())
            .expect_err("protocol addition is checked")
            .kind(),
        LongPollingDeadlineErrorKind::ProtocolOverflow
    );
}

#[test]
fn index_reservations_enforce_both_limits_and_drop_to_zero() {
    let index = PopCriteriaIndex::<u64>::new(PopCriteriaLimits::new(nonzero(2), nonzero(1)));
    let first_key = PopCriteriaKey::new(
        CheetahString::from_static_str("topic"),
        CheetahString::from_static_str("group"),
        0,
    );
    let second_key = PopCriteriaKey::new(
        CheetahString::from_static_str("topic"),
        CheetahString::from_static_str("group"),
        1,
    );
    let first = index.reserve(first_key.clone()).expect("first reservation");
    let per_key = match index.reserve(first_key) {
        Ok(_) => panic!("per-key capacity must reject"),
        Err(error) => error,
    };
    assert_eq!(per_key.kind(), PopIndexErrorKind::BucketCapacity);
    let second = index.reserve(second_key.clone()).expect("second reservation");
    let global = match index.reserve(second_key) {
        Ok(_) => panic!("global capacity must reject"),
        Err(error) => error,
    };
    assert_eq!(global.kind(), PopIndexErrorKind::GlobalCapacity);
    assert_eq!(index.snapshot().reserved(), 2);
    drop((first, second));
    assert_eq!(index.snapshot(), PopIndexSnapshot::default());
}

#[test]
fn index_merges_exact_and_wildcard_by_deadline_then_sequence_in_both_orders() {
    let index = PopCriteriaIndex::<u64>::new(PopCriteriaLimits::new(nonzero(8), nonzero(8)));
    let base = tokio::time::Instant::now();
    let exact_first = index.reserve(criteria_key(3)).expect("exact reservation").publish(
        10,
        deadline_after(base, 30),
        match_all_criteria(),
    );
    let wildcard = index.reserve(criteria_key(-1)).expect("wildcard reservation").publish(
        20,
        deadline_after(base, 10),
        match_all_criteria(),
    );
    let exact_second = index
        .reserve(criteria_key(3))
        .expect("second exact reservation")
        .publish(30, deadline_after(base, 30), match_all_criteria());
    let arrival = PopArrival::new(
        CheetahString::from_static_str("topic"),
        CheetahString::from_static_str("group"),
        3,
    );

    assert_eq!(
        index.matching_ids(&arrival, PopSelectionOrder::Oldest, nonzero(8)),
        vec![20, 10, 30]
    );
    assert_eq!(
        index.matching_ids(&arrival, PopSelectionOrder::Newest, nonzero(8)),
        vec![30, 10, 20]
    );
    drop((exact_first, wildcard, exact_second));
    assert_eq!(index.snapshot(), PopIndexSnapshot::default());
}

#[test]
fn filter_miss_keeps_membership_for_a_later_matching_arrival() {
    let index = PopCriteriaIndex::<u64>::new(PopCriteriaLimits::new(nonzero(2), nonzero(2)));
    let matches = Arc::new(AtomicBool::new(false));
    let criteria = Arc::new(PopMatchCriteria::new(
        Some(SubscriptionData::default()),
        Some(Arc::new(ToggleFilter {
            matches: Arc::clone(&matches),
        })),
    ));
    let lease = index.reserve(criteria_key(3)).expect("reservation").publish(
        7,
        deadline_after(tokio::time::Instant::now(), 10),
        criteria,
    );
    let arrival = PopArrival::new(
        CheetahString::from_static_str("topic"),
        CheetahString::from_static_str("group"),
        3,
    )
    .with_filter_metadata(Some(42), 1, None, None);

    assert!(index
        .matching_ids(&arrival, PopSelectionOrder::Oldest, nonzero(1))
        .is_empty());
    assert!(index.contains(7));
    assert_eq!(index.snapshot().live(), 1);
    matches.store(true, Ordering::SeqCst);
    assert_eq!(
        index.matching_ids(&arrival, PopSelectionOrder::Oldest, nonzero(1)),
        vec![7]
    );
    drop(lease);
    assert_eq!(index.snapshot(), PopIndexSnapshot::default());
}

#[test]
fn retry_topic_versions_normalize_to_the_original_topic_key() {
    let index = PopCriteriaIndex::<u64>::new(PopCriteriaLimits::new(nonzero(2), nonzero(2)));
    let lease = index.reserve(criteria_key(3)).expect("reservation").publish(
        11,
        deadline_after(tokio::time::Instant::now(), 10),
        match_all_criteria(),
    );
    for retry in [
        KeyBuilder::build_pop_retry_topic_v1("topic", "group"),
        KeyBuilder::build_pop_retry_topic_v2("topic", "group"),
    ] {
        let arrival = PopArrival::from_retry_topic(
            CheetahString::from_string(retry),
            CheetahString::from_static_str("group"),
            3,
        );
        assert_eq!(
            index.matching_ids(&arrival, PopSelectionOrder::Oldest, nonzero(1)),
            vec![11]
        );
    }
    drop(lease);
    assert_eq!(index.snapshot(), PopIndexSnapshot::default());
}

#[test]
fn affine_index_lease_releases_record_and_owned_filter_exactly_once() {
    let index = PopCriteriaIndex::<u64>::new(PopCriteriaLimits::new(nonzero(1), nonzero(1)));
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = index.reserve(criteria_key(3)).expect("reservation").publish(
        99,
        deadline_after(tokio::time::Instant::now(), 10),
        Arc::new(PopMatchCriteria::new(
            Some(SubscriptionData::default()),
            Some(Arc::new(CountingFilter {
                drops: Arc::clone(&drops),
            })),
        )),
    );
    assert_eq!(lease.deferred_id(), 99);
    assert_eq!(index.snapshot().live(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    drop(lease);

    assert_eq!(index.snapshot(), PopIndexSnapshot::default());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(index);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn prepared_registration_owns_index_and_wait_capacity_until_drop() {
    let service = service(2, 2);
    let monotonic_now = tokio::time::Instant::now();
    let prepared = service
        .prepare_at(
            request(10_000, 1_000),
            None,
            None,
            PopRetainedEstimate::default(),
            10_100,
            monotonic_now,
        )
        .expect("prepare before responder transfer");
    assert!(prepared.retained_bytes() > 0);
    assert_eq!(
        prepared.deadline().protocol_at(),
        monotonic_now + Duration::from_millis(851)
    );
    assert_eq!(service.index_snapshot().reserved(), 1);
    assert_eq!(service.admission_snapshot().waiting_count(), 1);
    drop(prepared);
    assert_eq!(service.index_snapshot(), PopIndexSnapshot::default());
    assert_eq!(service.admission_snapshot().waiting_count(), 0);
    assert_eq!(service.admission_snapshot().retained_bytes(), 0);
}

#[test]
fn shutdown_rejects_prepare_before_reserving_business_or_wait_capacity() {
    let service = service(2, 2);
    let _ = service.shutdown();
    let error = match service.prepare_at(
        request(10_000, 1_000),
        None,
        None,
        PopRetainedEstimate::default(),
        10_100,
        tokio::time::Instant::now(),
    ) {
        Ok(_) => panic!("closed service must reject preparation"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), PopDeferredPrepareErrorKind::ServiceClosed);
    assert_eq!(service.index_snapshot(), PopIndexSnapshot::default());
    assert_eq!(service.admission_snapshot().waiting_count(), 0);
    assert_eq!(service.admission_snapshot().retained_bytes(), 0);
}

#[test]
fn missing_effective_peer_fails_before_reserving_capacity() {
    let service = service(2, 2);
    let error = match service.prepare_at(
        request_with_host(10_000, 1_000, CheetahString::new()),
        None,
        None,
        PopRetainedEstimate::default(),
        10_100,
        tokio::time::Instant::now(),
    ) {
        Ok(_) => panic!("missing trusted peer must reject preparation"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), PopDeferredPrepareErrorKind::MissingCallerHost);
    assert_eq!(service.index_snapshot(), PopIndexSnapshot::default());
    assert_eq!(service.admission_snapshot().waiting_count(), 0);
}

#[test]
fn typed_request_data_retains_no_transport_context() {
    let request = request(7, 9);
    assert_eq!(request.topic().as_str(), "topic");
    assert_eq!(request.consumer_group().as_str(), "group");
    assert_eq!(request.queue_id(), 3);
    assert_eq!(request.caller_host().as_str(), "127.0.0.1:10911");
    assert!(request.estimated_dynamic_bytes().expect("retained estimate is checked") >= "127.0.0.1:10911".len());
    assert_eq!(request.header().born_time, 7);
    assert_eq!(request.into_header().poll_time, 9);
}

#[test]
fn retained_accounting_includes_nested_rpc_strings_and_rejects_overflow_before_take() {
    let mut header = request(10_000, 1_000).into_header();
    header.topic_request_header = Some(TopicRequestHeader {
        lo: Some(true),
        rpc: Some(RpcRequestHeader::new(
            Some(CheetahString::from_static_str("tenant-a")),
            Some(true),
            Some(CheetahString::from_static_str("broker-a")),
            Some(false),
        )),
    });
    let data = PopRequestData::from_test_header(header, CheetahString::from_static_str("127.0.0.1:10911"));
    let bytes = data
        .estimated_dynamic_bytes()
        .expect("nested retained accounting is checked");
    assert!(bytes >= "tenant-a".len() + "broker-a".len() + "127.0.0.1:10911".len());

    let service = service(2, 2);
    let error = match service.prepare_at(
        data,
        None,
        None,
        PopRetainedEstimate::new(usize::MAX, 0, 0),
        10_100,
        tokio::time::Instant::now(),
    ) {
        Ok(_) => panic!("retained-size overflow must fail before responder transfer"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), PopDeferredPrepareErrorKind::RetainedSizeOverflow);
    assert_eq!(service.index_snapshot(), PopIndexSnapshot::default());
    assert_eq!(service.admission_snapshot().waiting_count(), 0);
}

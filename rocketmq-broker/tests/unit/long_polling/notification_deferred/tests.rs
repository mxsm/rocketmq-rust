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
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_store::CqExtUnit;
use rocketmq_store::MessageFilter;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::DeferredAdmission;
use rocketmq_transport::api::DeferredExpiryMargins;
use rocketmq_transport::api::DeferredWaitLimits;

use super::deadline::NotificationWaitDeadline;
use super::deadline::NotificationWaitDeadlineErrorKind;
use super::index::NotificationArrivalView;
use super::index::NotificationCandidateSelection;
use super::index::NotificationCriteriaIndex;
use super::index::NotificationCriteriaKey;
use super::index::NotificationCriteriaLimits;
use super::index::NotificationIndexErrorKind;
use super::index::NotificationIndexSnapshot;
use super::index::NotificationMatchCriteria;
use super::index::NotificationScanCursor;
use super::service::NotificationContinuationError;
use super::service::NotificationDeferredPrepareErrorKind;
use super::service::NotificationDeferredService;
use super::service::NotificationRequestData;
use super::service::NotificationRetainedEstimate;

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test limit is non-zero")
}

fn key(group: &str, queue_id: i32) -> NotificationCriteriaKey {
    NotificationCriteriaKey::new(
        CheetahString::from_static_str("topic"),
        CheetahString::from_string(group.to_owned()),
        queue_id,
    )
}

fn request(born_time: i64, poll_time: i64, group: &str, queue_id: i32) -> NotificationRequestData {
    NotificationRequestData::new(
        NotificationRequestHeader {
            consumer_group: CheetahString::from_string(group.to_owned()),
            topic: CheetahString::from_static_str("topic"),
            queue_id,
            poll_time,
            born_time,
            order: false,
            attempt_id: None,
            exp_type: None,
            exp: None,
            is_lite_consumer: false,
            client_id: None,
            topic_request_header: None,
        },
        "127.0.0.1:10911".parse::<SocketAddr>().expect("test socket address"),
    )
}

fn service(
    scan_limit: usize,
    conflicts: usize,
    continuation_count: usize,
    continuation_bytes: usize,
) -> NotificationDeferredService {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(16, 16 * 1024 * 1024))
        .expect("Notification wait admission");
    NotificationDeferredService::new(
        admission,
        NotificationCriteriaLimits::new(nonzero(16), 7, 1),
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        nonzero(scan_limit),
        nonzero(conflicts),
        nonzero(continuation_count),
        nonzero(continuation_bytes),
    )
}

struct ToggleFilter(Arc<AtomicBool>);

impl MessageFilter for ToggleFilter {
    fn is_matched_by_consume_queue(&self, _tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    fn is_matched_by_commit_log(
        &self,
        _msg_buffer: Option<&[u8]>,
        _properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

#[test]
fn notification_deferred_deadline_preserves_signed_strict_fifty_millisecond_boundary() {
    let monotonic = tokio::time::Instant::now();
    let live = NotificationWaitDeadline::checked(1_000, 100, 1_050, monotonic).expect("cutoff equality is live");
    assert_eq!(live.protocol_millis(), 1_051);
    assert_eq!(live.protocol_at(), monotonic + Duration::from_millis(1));
    assert_eq!(
        NotificationWaitDeadline::checked(1_000, 100, 1_051, monotonic)
            .expect_err("the next integer millisecond expires")
            .kind(),
        NotificationWaitDeadlineErrorKind::AlreadyExpired
    );
    assert_eq!(
        NotificationWaitDeadline::checked(-1, 100, 0, monotonic)
            .expect_err("negative born time is rejected")
            .kind(),
        NotificationWaitDeadlineErrorKind::NegativeBornTime
    );
    assert_eq!(
        NotificationWaitDeadline::checked(i64::MAX, 1, 0, monotonic)
            .expect_err("signed end overflow is rejected")
            .kind(),
        NotificationWaitDeadlineErrorKind::ProtocolOverflow
    );
    assert_eq!(
        NotificationWaitDeadline::checked(1_000, 0, 0, monotonic)
            .expect_err("non-positive poll time is immediate")
            .kind(),
        NotificationWaitDeadlineErrorKind::NonPositivePollTime
    );
}

#[test]
fn notification_deferred_index_uses_legacy_plus_one_per_key_and_hint_is_not_a_key_limit() {
    let index = NotificationCriteriaIndex::<u64>::new(NotificationCriteriaLimits::new(nonzero(4), 1, 1));
    let criteria = Arc::new(NotificationMatchCriteria::new(None, None));
    let deadline = NotificationWaitDeadline::checked(0, 1_000, 0, tokio::time::Instant::now()).expect("test deadline");
    let first =
        index
            .reserve(key("group-a", 0))
            .expect("legacy length zero")
            .publish(1, deadline, Arc::clone(&criteria));
    let second =
        index
            .reserve(key("group-a", 0))
            .expect("legacy length one")
            .publish(2, deadline, Arc::clone(&criteria));
    let full = match index.reserve(key("group-a", 0)) {
        Ok(_) => panic!("third entry exceeds pop_polling_size + 1"),
        Err(error) => error,
    };
    assert_eq!(full.kind(), NotificationIndexErrorKind::PerKeyCapacity);
    let other = index
        .reserve(key("group-b", 0))
        .expect("allocation hint is not a key cap")
        .publish(3, deadline, Arc::clone(&criteria));
    let fourth = index
        .reserve(key("group-c", 0))
        .expect("fourth global entry")
        .publish(4, deadline, criteria);
    let global = match index.reserve(key("group-d", 0)) {
        Ok(_) => panic!("fifth entry exceeds the global live+reserved cap"),
        Err(error) => error,
    };
    assert_eq!(global.kind(), NotificationIndexErrorKind::GlobalCapacity);
    assert_eq!(index.snapshot().keys(), 3);
    drop((first, second, other, fourth));
    assert_eq!(index.snapshot(), NotificationIndexSnapshot::default());
}

#[test]
fn notification_deferred_cursor_is_wildcard_first_and_newest_first_without_revisiting_miss() {
    let index = NotificationCriteriaIndex::<u64>::new(NotificationCriteriaLimits::direct(nonzero(8), nonzero(8)));
    let deadline = NotificationWaitDeadline::checked(0, 1_000, 0, tokio::time::Instant::now()).expect("test deadline");
    let toggle = Arc::new(AtomicBool::new(false));
    let filtered = Arc::new(NotificationMatchCriteria::new(
        Some(SubscriptionData::default()),
        Some(Arc::new(ToggleFilter(Arc::clone(&toggle)))),
    ));
    let wildcard = index.reserve(key("group", -1)).expect("wildcard").publish(
        1,
        deadline,
        Arc::new(NotificationMatchCriteria::new(None, None)),
    );
    let exact_old = index.reserve(key("group", 3)).expect("exact old").publish(
        2,
        deadline,
        Arc::new(NotificationMatchCriteria::new(None, None)),
    );
    let exact_new = index
        .reserve(key("group", 3))
        .expect("exact new")
        .publish(3, deadline, filtered);
    let topic = CheetahString::from_static_str("topic");
    let arrival = NotificationArrivalView::new(&topic, 3).with_filter_metadata(Some(7), 10, None, None);
    let mut cursor = index.scan_cursor(&arrival);

    let NotificationCandidateSelection::Candidate(first) = index.reserve_next(&mut cursor) else {
        panic!("wildcard candidate expected");
    };
    assert_eq!(first.id(), 1);
    cursor.advance_key();
    drop(first);

    let NotificationCandidateSelection::Candidate(miss) = index.reserve_next(&mut cursor) else {
        panic!("newest exact candidate expected");
    };
    assert_eq!(miss.id(), 3);
    assert!(!miss.criteria().matches(&arrival, &arrival.cq_ext_unit()));
    drop(miss);
    let NotificationCandidateSelection::Candidate(next) = index.reserve_next(&mut cursor) else {
        panic!("older exact candidate expected after miss");
    };
    assert_eq!(next.id(), 2);
    drop(next);

    toggle.store(true, Ordering::SeqCst);
    let mut fresh = index.scan_cursor(&arrival);
    let NotificationCandidateSelection::Candidate(retry_wildcard) = index.reserve_next(&mut fresh) else {
        panic!("fresh arrival starts at wildcard");
    };
    fresh.advance_key();
    drop(retry_wildcard);
    let NotificationCandidateSelection::Candidate(retry_exact) = index.reserve_next(&mut fresh) else {
        panic!("fresh arrival sees newest exact again");
    };
    assert_eq!(retry_exact.id(), 3);
    drop(retry_exact);
    drop((wildcard, exact_old, exact_new));
    assert_eq!(index.snapshot(), NotificationIndexSnapshot::default());
}

#[test]
fn notification_deferred_retry_arrival_normalizes_before_fanout_snapshot() {
    let index = NotificationCriteriaIndex::<u64>::new(NotificationCriteriaLimits::direct(nonzero(2), nonzero(2)));
    let deadline = NotificationWaitDeadline::checked(0, 1_000, 0, tokio::time::Instant::now()).expect("test deadline");
    let lease = index.reserve(key("group", 3)).expect("normal topic key").publish(
        7,
        deadline,
        Arc::new(NotificationMatchCriteria::new(None, None)),
    );
    let retry = CheetahString::from_string(
        rocketmq_model::common::key_builder::KeyBuilder::build_pop_retry_topic_v2("topic", "group"),
    );
    let arrival = NotificationArrivalView::new(&retry, 3);
    let mut cursor = index.scan_cursor(&arrival);
    let NotificationCandidateSelection::Candidate(candidate) = index.reserve_next(&mut cursor) else {
        panic!("retry-arrival resolves the normal topic key");
    };
    assert_eq!(candidate.id(), 7);
    drop(candidate);
    drop(lease);
    assert_eq!(index.snapshot(), NotificationIndexSnapshot::default());
}

#[test]
fn notification_deferred_prepare_failures_release_index_and_wait_capacity() {
    let service = service(2, 2, 1, 1024);
    let monotonic = tokio::time::Instant::now();
    let invalid = match service.prepare_at(
        request(-1, 100, "group", 0),
        None,
        None,
        NotificationRetainedEstimate::default(),
        0,
        monotonic,
    ) {
        Ok(_) => panic!("invalid signed deadline stays pre-take"),
        Err(error) => error,
    };
    assert_eq!(invalid.kind(), NotificationDeferredPrepareErrorKind::Deadline);
    assert_eq!(service.snapshot().index(), NotificationIndexSnapshot::default());
    assert_eq!(service.snapshot().admission().waiting_count(), 0);

    let prepared = service
        .prepare_at(
            request(1_000, 1_000, "group", 0),
            None,
            None,
            NotificationRetainedEstimate::default(),
            1_100,
            monotonic,
        )
        .expect("valid prepared registration");
    assert!(prepared.retained_bytes() > 0);
    assert_eq!(service.snapshot().prepared(), 1);
    assert_eq!(service.snapshot().index().reserved(), 1);
    drop(prepared);
    assert_eq!(service.snapshot().prepared(), 0);
    assert_eq!(service.snapshot().index(), NotificationIndexSnapshot::default());
    assert_eq!(service.snapshot().admission().waiting_count(), 0);
}

#[test]
fn notification_deferred_continuation_has_independent_count_and_bytes_admission() {
    let deferred_service = service(1, 1, 1, 4096);
    let topic = CheetahString::from_static_str("topic");
    let arrival = NotificationArrivalView::new(&topic, 0);
    let continuation = deferred_service
        .admit_continuation(arrival, NotificationScanCursor::for_test(1))
        .expect("first continuation is admitted");
    assert_eq!(deferred_service.snapshot().active_continuations(), 1);
    let second = match deferred_service.admit_continuation(arrival, NotificationScanCursor::for_test(1)) {
        Ok(_) => panic!("second continuation exceeds the count cap"),
        Err(error) => error,
    };
    assert_eq!(second, NotificationContinuationError::CountFull);
    drop(continuation);
    assert_eq!(deferred_service.snapshot().active_continuations(), 0);
    assert_eq!(deferred_service.snapshot().continuation_bytes(), 0);
    assert_eq!(deferred_service.snapshot().continuation_rejected(), 1);

    let bytes_limited = service(1, 1, 2, 4);
    let large_topic = CheetahString::from_static_str("topic-larger-than-four-bytes");
    let bytes_error = match bytes_limited.admit_continuation(
        NotificationArrivalView::new(&large_topic, 0),
        NotificationScanCursor::for_test(1),
    ) {
        Ok(_) => panic!("arrival payload exceeds continuation byte capacity"),
        Err(error) => error,
    };
    assert_eq!(bytes_error, NotificationContinuationError::BytesFull);
    assert_eq!(bytes_limited.snapshot().active_continuations(), 0);
    assert_eq!(bytes_limited.snapshot().continuation_bytes(), 0);
}

#[test]
fn notification_deferred_continuation_charges_fixed_cursor_and_property_storage_before_copy() {
    let topic = CheetahString::from_string("topic".repeat(32));
    let properties = (0..32)
        .map(|index| {
            (
                CheetahString::from_string(format!("k{index}")),
                CheetahString::from_string(format!("v{index}")),
            )
        })
        .collect::<HashMap<_, _>>();
    let arrival = NotificationArrivalView::new(&topic, 0).with_filter_metadata(None, 0, None, Some(&properties));
    let constrained = service(1, 1, 1, 4096);
    let error = match constrained.admit_continuation(arrival, NotificationScanCursor::for_test(64)) {
        Ok(_) => panic!("fixed owners, cursor backing, buckets, and short properties exceed the byte cap"),
        Err(error) => error,
    };
    assert_eq!(error, NotificationContinuationError::BytesFull);
    assert_eq!(constrained.snapshot().active_continuations(), 0);
    assert_eq!(constrained.snapshot().continuation_bytes(), 0);

    let admitted = service(1, 1, 1, 1024 * 1024);
    let continuation = admitted
        .admit_continuation(arrival, NotificationScanCursor::for_test(64))
        .expect("the conservative retained oracle fits the generous cap");
    assert!(
        admitted.snapshot().continuation_bytes() > topic.len() + properties.len(),
        "the charge includes fixed owners and collection backing, not only string payload bytes"
    );
    drop(continuation);
    assert_eq!(admitted.snapshot().active_continuations(), 0);
    assert_eq!(admitted.snapshot().continuation_bytes(), 0);
}

#[tokio::test]
async fn notification_deferred_continuation_spends_one_conflict_budget_across_batches() {
    let service = Arc::new(service(1, 2, 1, 1024 * 1024));
    service.index.force_conflicts(8);
    let topic = CheetahString::from_static_str("topic");
    let arrival = NotificationArrivalView::new(&topic, 0);
    let initial = service.prepare_arrival_batch(arrival, Some(NotificationScanCursor::for_test(1)));
    assert_eq!(initial.conflicts(), 1);
    let initial = service.claim_prepared_arrival(initial).await;
    let (initial_claims, cursor) = initial.into_parts();
    assert!(initial_claims.is_empty());
    let continuation = service
        .admit_continuation(arrival, cursor)
        .expect("conflict continuation admission");
    assert_eq!(service.snapshot().active_continuations(), 1);
    let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("notification-conflict-budget"))
        .expect("test runtime configuration is valid")
        .build()
        .expect("conflict-budget runtime owner");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    service
        .spawn_continuation(
            runtime.root_context().component("notification.conflict").task_group(),
            continuation,
            Arc::new(
                move |claims: Vec<rocketmq_transport::api::ClaimedDeferred<super::service::ResumeNotification>>| {
                    let observed = Arc::clone(&observed);
                    async move {
                        assert!(claims.is_empty());
                        observed.fetch_add(1, Ordering::AcqRel);
                    }
                },
            ),
        )
        .expect("spawn conflict continuation");
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::Acquire) != 1 || service.snapshot().active_continuations() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("continuation terminates when its shared conflict budget is exhausted");
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(service.index.forced_conflicts_remaining(), 6);
    assert_eq!(service.snapshot().continuation_bytes(), 0);
    let report = runtime.shutdown_tasks().await;
    assert!(report.is_healthy(), "{}", report.to_json());
    let final_report = runtime.shutdown_background();
    assert!(final_report.is_healthy(), "{}", final_report.to_json());
}

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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store::CqExtUnit;
use rocketmq_store::MessageFilter;

use super::deadline::PullWaitDeadline;
use super::deadline::PullWaitDeadlineErrorKind;
use super::index::PullArrivalView;
use super::index::PullCriteriaIndex;
use super::index::PullCriteriaKey;
use super::index::PullCriteriaLimits;
use super::index::PullIndexErrorKind;
use super::index::PullIndexSnapshot;
use super::index::PullScanCursor;
use super::service::PullSuspendTiming;
use super::PullMatchCriteria;

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test value is non-zero")
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

struct SplitFilter {
    consume: bool,
    commit: bool,
}

impl MessageFilter for SplitFilter {
    fn is_matched_by_consume_queue(&self, _tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        self.consume
    }

    fn is_matched_by_commit_log(
        &self,
        _msg_buffer: Option<&[u8]>,
        _properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        self.commit
    }
}

fn criteria(offset: i64, matches: Arc<AtomicBool>) -> Arc<PullMatchCriteria> {
    Arc::new(PullMatchCriteria::new(
        CheetahString::from_static_str("TopicA"),
        3,
        offset,
        SubscriptionData::default(),
        Arc::new(ToggleFilter(matches)),
    ))
}

fn key() -> PullCriteriaKey {
    PullCriteriaKey::new(CheetahString::from_static_str("TopicA"), 3)
}

#[test]
fn pull_deadline_preserves_inclusive_legacy_boundary() {
    let suspend_monotonic = tokio::time::Instant::now();
    let before = PullWaitDeadline::checked(
        1_000,
        suspend_monotonic,
        100,
        1_099,
        suspend_monotonic + Duration::from_millis(99),
    )
    .expect("one millisecond remains");
    assert_eq!(before.protocol_end_millis(), 1_100);
    assert_eq!(before.protocol_at(), suspend_monotonic + Duration::from_millis(100));

    let equal = PullWaitDeadline::checked(
        1_000,
        suspend_monotonic,
        100,
        1_100,
        suspend_monotonic + Duration::from_millis(99),
    )
    .expect_err("equal wall boundary expires");
    assert_eq!(equal.kind(), PullWaitDeadlineErrorKind::AlreadyExpired);
    let monotonic_equal = PullWaitDeadline::checked(
        1_000,
        suspend_monotonic,
        100,
        1_099,
        suspend_monotonic + Duration::from_millis(100),
    )
    .expect_err("equal monotonic boundary expires");
    assert_eq!(monotonic_equal.kind(), PullWaitDeadlineErrorKind::AlreadyExpired);
    let zero = PullWaitDeadline::checked(1_000, suspend_monotonic, 0, 1_000, suspend_monotonic)
        .expect_err("zero timeout is immediate");
    assert_eq!(zero.kind(), PullWaitDeadlineErrorKind::AlreadyExpired);
}

#[test]
fn pull_deadline_checks_wall_and_monotonic_addition() {
    let monotonic = tokio::time::Instant::now();
    let overflow =
        PullWaitDeadline::checked(u64::MAX, monotonic, 1, 0, monotonic).expect_err("wall addition overflows");
    assert_eq!(overflow.kind(), PullWaitDeadlineErrorKind::ProtocolOverflow);

    let mut representable_seconds = 0u64;
    let mut upper = u64::MAX;
    while representable_seconds < upper {
        let midpoint = representable_seconds + (upper - representable_seconds) / 2 + 1;
        if monotonic.checked_add(Duration::from_secs(midpoint)).is_some() {
            representable_seconds = midpoint;
        } else {
            upper = midpoint - 1;
        }
    }
    let overflow_base = monotonic
        .checked_add(Duration::from_secs(representable_seconds))
        .expect("binary search retains a representable monotonic instant");
    let monotonic_overflow = PullWaitDeadline::checked(0, overflow_base, u64::MAX, 0, monotonic)
        .expect_err("monotonic addition overflows before registration");
    assert_eq!(monotonic_overflow.kind(), PullWaitDeadlineErrorKind::MonotonicOverflow);
}

#[test]
fn pull_suspend_timing_selects_long_or_short_policy_at_suspend_start() {
    let monotonic = tokio::time::Instant::now();
    assert_eq!(
        PullSuspendTiming::from_policy(10, monotonic, true, 30_000, 1_000),
        PullSuspendTiming::new(10, monotonic, 30_000)
    );
    assert_eq!(
        PullSuspendTiming::from_policy(10, monotonic, false, 30_000, 1_000),
        PullSuspendTiming::new(10, monotonic, 1_000)
    );
    let timing = PullSuspendTiming::from_policy(10, monotonic, false, 30_000, 1_000);
    assert_eq!(timing.suspend_wall_millis(), 10);
    assert_eq!(timing.suspend_monotonic(), monotonic);
    assert_eq!(timing.effective_timeout_millis(), 1_000);
}

#[test]
fn index_reservations_enforce_global_and_per_key_capacity() {
    let index = PullCriteriaIndex::<u64>::new(PullCriteriaLimits::new(nonzero(2), nonzero(1)));
    let first = index.reserve(key()).expect("first reservation");
    let per_key = match index.reserve(key()) {
        Ok(_) => panic!("per-key capacity must reject"),
        Err(error) => error,
    };
    assert_eq!(per_key.kind(), PullIndexErrorKind::BucketCapacity);
    let second_key = PullCriteriaKey::new(CheetahString::from_static_str("TopicB"), 3);
    let second = index.reserve(second_key.clone()).expect("second reservation");
    let global = match index.reserve(second_key) {
        Ok(_) => panic!("global capacity must reject"),
        Err(error) => error,
    };
    assert_eq!(global.kind(), PullIndexErrorKind::GlobalCapacity);
    assert_eq!(index.snapshot().reserved(), 2);
    drop((first, second));
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
}

#[test]
fn stale_offset_and_filter_miss_remain_visible_for_later_arrival() {
    let index = PullCriteriaIndex::<u64>::new(PullCriteriaLimits::new(nonzero(2), nonzero(2)));
    let matches = Arc::new(AtomicBool::new(false));
    let lease = index
        .reserve(key())
        .expect("reservation")
        .publish(7, criteria(10, Arc::clone(&matches)));
    let topic = CheetahString::from_static_str("TopicA");

    let mut stale_cursor = PullScanCursor::new();
    assert!(index
        .reserve_matching(
            &PullArrivalView::new(&topic, 3, 10),
            &mut stale_cursor,
            nonzero(2),
            nonzero(2),
        )
        .is_empty());
    let mut miss_cursor = PullScanCursor::new();
    assert!(index
        .reserve_matching(
            &PullArrivalView::new(&topic, 3, 11),
            &mut miss_cursor,
            nonzero(2),
            nonzero(2),
        )
        .is_empty());
    assert!(index.contains(7));

    matches.store(true, Ordering::SeqCst);
    let mut match_cursor = PullScanCursor::new();
    let candidates = index.reserve_matching(
        &PullArrivalView::new(&topic, 3, 11),
        &mut match_cursor,
        nonzero(2),
        nonzero(2),
    );
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].id(), 7);
    drop(candidates);
    assert!(index.contains(7), "losing candidate restores the waiter");
    drop(lease);
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
}

#[test]
fn bounded_continuation_advances_every_current_match_in_sequence_order() {
    let index = PullCriteriaIndex::<u64>::new(PullCriteriaLimits::new(nonzero(4), nonzero(4)));
    let matches = Arc::new(AtomicBool::new(true));
    let leases = (1..=3)
        .map(|id| {
            index
                .reserve(key())
                .expect("reservation")
                .publish(id, criteria(0, Arc::clone(&matches)))
        })
        .collect::<Vec<_>>();
    let topic = CheetahString::from_static_str("TopicA");
    let arrival = PullArrivalView::new(&topic, 3, 1);
    let mut cursor = PullScanCursor::new();
    let mut candidates = Vec::new();
    loop {
        let batch = index.reserve_matching(&arrival, &mut cursor, nonzero(1), nonzero(1));
        if batch.is_empty() {
            break;
        }
        candidates.extend(batch);
    }
    assert_eq!(
        candidates.iter().map(|candidate| candidate.id()).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(index.snapshot().candidates(), 3);
    drop(candidates);
    assert_eq!(index.snapshot().live(), 3);
    drop(leases);
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
}

#[test]
fn bounded_scan_continues_past_an_empty_filtered_prefix() {
    let index = PullCriteriaIndex::<u64>::new(PullCriteriaLimits::new(nonzero(4), nonzero(4)));
    let miss = Arc::new(AtomicBool::new(false));
    let hit = Arc::new(AtomicBool::new(true));
    let leases = [
        index
            .reserve(key())
            .expect("miss reservation")
            .publish(1, criteria(0, miss)),
        index
            .reserve(key())
            .expect("hit reservation")
            .publish(2, criteria(0, hit)),
    ];
    let topic = CheetahString::from_static_str("TopicA");
    let arrival = PullArrivalView::new(&topic, 3, 1);
    let mut cursor = PullScanCursor::new();
    let first = index.reserve_matching_batch(&arrival, &mut cursor, nonzero(1), nonzero(1));
    assert_eq!(first.inspected(), 1);
    assert!(!first.exhausted());
    assert!(first.into_candidates().is_empty());
    let second = index.reserve_matching_batch(&arrival, &mut cursor, nonzero(1), nonzero(1));
    assert_eq!(second.into_candidates()[0].id(), 2);
    drop(leases);
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
}

#[test]
fn stale_offset_requires_one_current_max_offset_refresh() {
    let index = PullCriteriaIndex::<u64>::new(PullCriteriaLimits::new(nonzero(2), nonzero(2)));
    let lease = index
        .reserve(key())
        .expect("reservation")
        .publish(3, criteria(10, Arc::new(AtomicBool::new(true))));
    let topic = CheetahString::from_static_str("TopicA");
    let stale = PullArrivalView::new(&topic, 3, 10);
    assert!(index.needs_offset_refresh(&stale));
    let current = stale.with_max_offset(11);
    assert!(!index.needs_offset_refresh(&current));
    let mut cursor = PullScanCursor::new();
    assert_eq!(
        index.reserve_matching(&current, &mut cursor, nonzero(1), nonzero(1))[0].id(),
        3
    );
    drop(lease);
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
}

#[test]
fn absent_properties_preserve_legacy_consume_queue_match() {
    let index = PullCriteriaIndex::<u64>::new(PullCriteriaLimits::new(nonzero(1), nonzero(1)));
    let criteria = Arc::new(PullMatchCriteria::new(
        CheetahString::from_static_str("TopicA"),
        3,
        0,
        SubscriptionData::default(),
        Arc::new(SplitFilter {
            consume: true,
            commit: false,
        }),
    ));
    let lease = index.reserve(key()).expect("reservation").publish(4, criteria);
    let topic = CheetahString::from_static_str("TopicA");
    let mut no_properties_cursor = PullScanCursor::new();
    assert_eq!(
        index.reserve_matching(
            &PullArrivalView::new(&topic, 3, 1),
            &mut no_properties_cursor,
            nonzero(1),
            nonzero(1),
        )[0]
        .id(),
        4
    );
    let properties = HashMap::new();
    let mut properties_cursor = PullScanCursor::new();
    assert!(index
        .reserve_matching(
            &PullArrivalView::new(&topic, 3, 1).with_filter_metadata(None, 0, None, Some(&properties)),
            &mut properties_cursor,
            nonzero(1),
            nonzero(1),
        )
        .is_empty());
    drop(lease);
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
}

#[test]
fn forced_refresh_bypasses_offset_and_filter_without_retaining_arrival_data() {
    let index = PullCriteriaIndex::<u64>::new(PullCriteriaLimits::new(nonzero(1), nonzero(1)));
    let matches = Arc::new(AtomicBool::new(false));
    let lease = index
        .reserve(key())
        .expect("reservation")
        .publish(9, criteria(100, matches));
    let topic = CheetahString::from_static_str("TopicA");
    let bitmap = [1, 2, 3];
    let properties = HashMap::new();
    let arrival = PullArrivalView::new(&topic, 3, 0)
        .with_filter_metadata(Some(7), 8, Some(&bitmap), Some(&properties))
        .forced();
    let mut cursor = PullScanCursor::new();
    let candidates = index.reserve_matching(&arrival, &mut cursor, nonzero(1), nonzero(1));
    assert_eq!(candidates[0].id(), 9);
    drop(candidates);
    drop(lease);
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
}

#[test]
fn forced_cursor_advances_all_keys_in_bounded_sequence_order() {
    let index = PullCriteriaIndex::<u64>::new(PullCriteriaLimits::new(nonzero(3), nonzero(2)));
    let matches = Arc::new(AtomicBool::new(false));
    let other_key = PullCriteriaKey::new(CheetahString::from_static_str("TopicB"), 7);
    let leases = [
        index
            .reserve(key())
            .expect("first reservation")
            .publish(1, criteria(100, Arc::clone(&matches))),
        index
            .reserve(other_key)
            .expect("second reservation")
            .publish(2, criteria(100, matches)),
    ];
    let mut cursor = PullScanCursor::new();
    let first = index.reserve_forced_batch(&mut cursor, nonzero(1), nonzero(1));
    assert_eq!(first.into_candidates()[0].id(), 1);
    let second = index.reserve_forced_batch(&mut cursor, nonzero(1), nonzero(1));
    assert_eq!(second.into_candidates()[0].id(), 2);
    let exhausted = index.reserve_forced_batch(&mut cursor, nonzero(1), nonzero(1));
    assert!(exhausted.exhausted());
    drop(leases);
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
}

#[test]
fn successful_candidate_commit_detaches_record_before_lease_drop() {
    let index = PullCriteriaIndex::<u64>::new(PullCriteriaLimits::new(nonzero(1), nonzero(1)));
    let lease = index
        .reserve(key())
        .expect("reservation")
        .publish(12, criteria(0, Arc::new(AtomicBool::new(true))));
    let topic = CheetahString::from_static_str("TopicA");
    let mut cursor = PullScanCursor::new();
    let candidate = index
        .reserve_matching(&PullArrivalView::new(&topic, 3, 1), &mut cursor, nonzero(1), nonzero(1))
        .pop()
        .expect("candidate");
    candidate.commit();
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
    drop(lease);
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
}

#[test]
fn dropping_lease_while_candidate_is_reserved_releases_exactly_once() {
    let index = PullCriteriaIndex::<u64>::new(PullCriteriaLimits::new(nonzero(1), nonzero(1)));
    let lease = index
        .reserve(key())
        .expect("reservation")
        .publish(13, criteria(0, Arc::new(AtomicBool::new(true))));
    let topic = CheetahString::from_static_str("TopicA");
    let mut cursor = PullScanCursor::new();
    let candidates = index.reserve_matching(&PullArrivalView::new(&topic, 3, 1), &mut cursor, nonzero(1), nonzero(1));
    assert_eq!(index.snapshot().candidates(), 1);
    drop(lease);
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
    drop(candidates);
    assert_eq!(index.snapshot(), PullIndexSnapshot::default());
}

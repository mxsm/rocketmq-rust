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

use std::alloc::Layout;
use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store::CqExtUnit;
use rocketmq_store::MessageFilter;

use super::*;
use crate::long_polling::pop_deferred::deadline::LongPollingDeadlineOutcome;

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test value is non-zero")
}

fn key(group: &str, queue_id: i32) -> PopCriteriaKey {
    PopCriteriaKey::new(
        CheetahString::from_static_str("topic"),
        CheetahString::from_string(group.to_owned()),
        queue_id,
    )
}

fn arrival(group: &str, queue_id: i32) -> PopArrival {
    PopArrival::new(
        CheetahString::from_static_str("topic"),
        CheetahString::from_string(group.to_owned()),
        queue_id,
    )
}

fn deadline(base: tokio::time::Instant, millis: u64) -> LongPollingDeadline {
    match LongPollingDeadline::checked(0, millis + 49, 0, base).expect("test deadline") {
        LongPollingDeadlineOutcome::Pending(deadline) => deadline,
        LongPollingDeadlineOutcome::Immediate => panic!("test deadline must remain pending"),
    }
}

fn match_all() -> Arc<PopMatchCriteria> {
    Arc::new(PopMatchCriteria::new(None, None))
}

struct TwoArrivalBarrierFilter {
    barrier: Arc<Barrier>,
    calls: AtomicUsize,
}

impl MessageFilter for TwoArrivalBarrierFilter {
    fn is_matched_by_consume_queue(&self, _tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        if self.calls.fetch_add(1, Ordering::SeqCst) < 2 {
            self.barrier.wait();
        }
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
fn concurrent_arrival_loser_advances_to_second_waiter_with_one_candidate_budget() {
    let index = PopCriteriaIndex::<u64>::new(PopCriteriaLimits::new(nonzero(2), nonzero(2)));
    let filter = Arc::new(TwoArrivalBarrierFilter {
        barrier: Arc::new(Barrier::new(2)),
        calls: AtomicUsize::new(0),
    });
    let criteria = Arc::new(PopMatchCriteria::new(Some(SubscriptionData::default()), Some(filter)));
    let base = tokio::time::Instant::now();
    let first_lease = index.reserve(key("group", 3)).expect("first reservation").publish(
        1,
        deadline(base, 10),
        Arc::clone(&criteria),
    );
    let second_lease =
        index
            .reserve(key("group", 3))
            .expect("second reservation")
            .publish(2, deadline(base, 20), criteria);
    let start = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let worker_index = index.clone();
        let worker_start = Arc::clone(&start);
        workers.push(thread::spawn(move || {
            worker_start.wait();
            worker_index.reserve_matching(&arrival("group", 3), PopSelectionOrder::Oldest, nonzero(1))
        }));
    }
    start.wait();
    let mut reservations = workers
        .into_iter()
        .flat_map(|worker| worker.join().expect("arrival worker"))
        .collect::<Vec<_>>();
    let mut ids = reservations.iter().map(PopCandidateReservation::id).collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2]);
    assert!(
        index
            .reserve_matching(&arrival("group", 3), PopSelectionOrder::Oldest, nonzero(1))
            .is_empty(),
        "both affine candidates remain unavailable while their arrival owns them"
    );

    reservations.clear();
    assert_eq!(
        index.matching_ids(&arrival("group", 3), PopSelectionOrder::Oldest, nonzero(2)),
        vec![1, 2]
    );
    drop((first_lease, second_lease));
    assert_eq!(index.snapshot(), PopIndexSnapshot::default());
}

#[test]
fn candidate_drop_reopens_but_lease_drop_retires_the_affine_membership() {
    let index = PopCriteriaIndex::<u64>::new(PopCriteriaLimits::new(nonzero(1), nonzero(1)));
    let lease = index.reserve(key("group", 3)).expect("reservation").publish(
        7,
        deadline(tokio::time::Instant::now(), 10),
        match_all(),
    );
    let mut candidates = index.reserve_matching(&arrival("group", 3), PopSelectionOrder::Oldest, nonzero(1));
    let candidate = candidates.pop().expect("candidate");
    assert!(index
        .reserve_matching(&arrival("group", 3), PopSelectionOrder::Oldest, nonzero(1))
        .is_empty());
    drop(candidate);
    assert_eq!(
        index.matching_ids(&arrival("group", 3), PopSelectionOrder::Oldest, nonzero(1)),
        vec![7]
    );

    let candidate = index
        .reserve_matching(&arrival("group", 3), PopSelectionOrder::Oldest, nonzero(1))
        .pop()
        .expect("candidate after release");
    drop(lease);
    drop(candidate);
    assert_eq!(index.snapshot(), PopIndexSnapshot::default());
    assert!(index.consumer_groups(&"topic".into(), 3, nonzero(1)).is_empty());
}

#[test]
fn preflight_reserves_storage_for_every_candidate_and_pending_publish() {
    let index = PopCriteriaIndex::<u64>::new(PopCriteriaLimits::new(nonzero(1_024), nonzero(1_024)));
    let criteria_key = key("group", 3);
    let base = tokio::time::Instant::now();
    let mut leases = Vec::new();
    loop {
        let (len, capacity) = index
            .inner
            .state
            .lock()
            .buckets
            .get(&criteria_key)
            .map_or((0, 0), |bucket| (bucket.entries.len(), bucket.entries.capacity()));
        if len > 0 && len == capacity {
            break;
        }
        let id = leases.len() as u64;
        leases.push(
            index
                .reserve(criteria_key.clone())
                .expect("fill the current bucket allocation")
                .publish(id, deadline(base, id + 1), match_all()),
        );
    }

    let filled_capacity = index
        .inner
        .state
        .lock()
        .buckets
        .get(&criteria_key)
        .expect("filled bucket")
        .entries
        .capacity();
    let candidates = index.reserve_matching(
        &arrival("group", 3),
        PopSelectionOrder::Oldest,
        nonzero(filled_capacity),
    );
    assert_eq!(candidates.len(), filled_capacity);

    let pending = index
        .reserve(criteria_key.clone())
        .expect("preflight accounts for candidate reinsertion");
    let reserved_capacity = index
        .inner
        .state
        .lock()
        .buckets
        .get(&criteria_key)
        .expect("candidate-held bucket")
        .entries
        .capacity();
    assert!(reserved_capacity > filled_capacity);
    let last_id = filled_capacity as u64;
    leases.push(pending.publish(last_id, deadline(base, last_id + 1), match_all()));
    drop(candidates);

    let state = index.inner.state.lock();
    let bucket = state.buckets.get(&criteria_key).expect("restored bucket");
    assert_eq!(bucket.entries.len(), filled_capacity + 1);
    assert_eq!(bucket.entries.capacity(), reserved_capacity);
    drop(state);
    drop(leases);
    assert_eq!(index.snapshot(), PopIndexSnapshot::default());
}

#[test]
fn retained_floor_counts_worst_case_bucket_fanout_and_arc_membership() {
    let (arc_header, _) = Layout::new::<AtomicUsize>()
        .extend(Layout::new::<AtomicUsize>())
        .expect("Arc header layout");
    let (membership, _) = arc_header
        .extend(Layout::new::<PopIndexMembership>())
        .expect("Arc membership layout");
    let membership = membership.pad_to_align().size();
    let expected = [
        std::mem::size_of::<PopIndexRecord<u64>>(),
        std::mem::size_of::<PopCriteriaKey>(),
        std::mem::size_of::<PopIndexBucket<u64>>(),
        std::mem::size_of::<PopTopicQueueKey>(),
        std::mem::size_of::<PopFanoutBucket>(),
        std::mem::size_of::<PopFanoutGroup>(),
        membership,
    ]
    .into_iter()
    .try_fold(0usize, |total, part| total.checked_add(part))
    .expect("test retained floor");
    let previous_incomplete_floor = std::mem::size_of::<PopIndexRecord<u64>>()
        + std::mem::size_of::<PopCriteriaKey>()
        + std::mem::size_of::<PopFanoutGroup>()
        + std::mem::size_of::<PopIndexMembership>();

    assert_eq!(PopCriteriaIndex::<u64>::try_retained_bytes_per_entry(), Some(expected));
    assert!(expected > previous_incomplete_floor);
    assert_eq!(checked_retained_layout_sum([usize::MAX, 1]), None);
}

#[test]
fn topic_queue_fanout_is_bounded_round_robin_and_cleans_up_exact_and_wildcard_groups() {
    let index = PopCriteriaIndex::<u64>::new(PopCriteriaLimits::new(nonzero(4), nonzero(4)));
    let base = tokio::time::Instant::now();
    let mut leases = Vec::new();
    for (id, group) in [(1, "a"), (2, "b"), (3, "c")] {
        leases.push(index.reserve(key(group, 3)).expect("exact fanout reservation").publish(
            id,
            deadline(base, id * 10),
            match_all(),
        ));
    }
    leases.push(
        index
            .reserve(key("wildcard", -1))
            .expect("wildcard fanout reservation")
            .publish(4, deadline(base, 40), match_all()),
    );

    let mut observed = HashSet::new();
    for _ in 0..3 {
        let groups = index.consumer_groups(&"topic".into(), 3, nonzero(1));
        assert!(groups.len() <= 2, "exact and wildcard scopes are independently bounded");
        observed.extend(groups.into_iter().map(|group| group.to_string()));
    }
    assert_eq!(
        observed,
        HashSet::from(["a".to_owned(), "b".to_owned(), "c".to_owned(), "wildcard".to_owned()])
    );

    drop(leases);
    assert!(index.consumer_groups(&"topic".into(), 3, nonzero(1)).is_empty());
    assert_eq!(index.snapshot(), PopIndexSnapshot::default());
}

#[test]
fn arrival_fanout_cursor_visits_every_group_beyond_one_batch() {
    let index = PopCriteriaIndex::<u64>::new(PopCriteriaLimits::new(nonzero(4), nonzero(4)));
    let base = tokio::time::Instant::now();
    let mut leases = Vec::new();
    for (id, group, queue_id) in [(1, "a", 3), (2, "b", 3), (3, "c", 3), (4, "wildcard", -1)] {
        leases.push(
            index
                .reserve(key(group, queue_id))
                .expect("fanout reservation")
                .publish(id, deadline(base, id * 10), match_all()),
        );
    }

    let topic = CheetahString::from_static_str("topic");
    let mut cursor = PopFanoutCursor::new();
    let mut observed = Vec::new();
    loop {
        let batch = index.consumer_group_batch(&topic, 3, &mut cursor, nonzero(1));
        let exhausted = batch.exhausted();
        observed.extend(batch.into_consumer_groups().into_iter().map(|group| group.to_string()));
        if exhausted {
            break;
        }
    }
    assert_eq!(observed, ["a", "b", "c", "wildcard"]);

    drop(leases);
}

struct CountingMatchFilter(Arc<AtomicUsize>);

impl MessageFilter for CountingMatchFilter {
    fn is_matched_by_consume_queue(&self, _tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        self.0.fetch_add(1, Ordering::SeqCst);
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
fn ordered_selection_filters_and_reserves_only_the_bounded_merge_prefix() {
    let index = PopCriteriaIndex::<u64>::new(PopCriteriaLimits::new(nonzero(16), nonzero(16)));
    let calls = Arc::new(AtomicUsize::new(0));
    let criteria = Arc::new(PopMatchCriteria::new(
        Some(SubscriptionData::default()),
        Some(Arc::new(CountingMatchFilter(Arc::clone(&calls)))),
    ));
    let base = tokio::time::Instant::now();
    let mut leases = Vec::new();
    for id in 0..16 {
        leases.push(
            index
                .reserve(key("group", if id % 2 == 0 { 3 } else { -1 }))
                .expect("ordered reservation")
                .publish(id, deadline(base, (16 - id) * 10), Arc::clone(&criteria)),
        );
    }

    let oldest = index.reserve_matching(&arrival("group", 3), PopSelectionOrder::Oldest, nonzero(2));
    assert_eq!(
        oldest.iter().map(PopCandidateReservation::id).collect::<Vec<_>>(),
        vec![15, 14]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    drop(oldest);
    let newest = index.reserve_matching(&arrival("group", 3), PopSelectionOrder::Newest, nonzero(2));
    assert_eq!(
        newest.iter().map(PopCandidateReservation::id).collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    drop(newest);
    drop(leases);
    assert_eq!(index.snapshot(), PopIndexSnapshot::default());
}

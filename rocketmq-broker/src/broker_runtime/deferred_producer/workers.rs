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

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;
use rocketmq_runtime::TaskKind;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_store::GetMessageResult;
use rocketmq_transport::api::v2::DeferredResumeRetainedSize;
use rocketmq_transport::api::v2::DeferredWakeReason;
use tracing::warn;

use super::BrokerDeferredProducer;
use crate::deferred_generation_handoff::DeferredGeneration;
use crate::long_polling::notification_deferred::service::NotificationDeferredService;
use crate::long_polling::notification_deferred::service::NotificationPendingOffsetReservation;
use crate::long_polling::pending_arrival_latch::PendingOffsetRange;
use crate::long_polling::pop_deferred::index::PopArrivalView;
use crate::long_polling::pop_deferred::index::PopFanoutCursor;
use crate::long_polling::pop_deferred::index::PopSelectionOrder;
use crate::long_polling::pop_deferred::service::PopDeferredService;
use crate::long_polling::pop_deferred::service::PopPendingOffsetReservation;
use crate::long_polling::pop_lite_deferred::service::PopLiteDeferredService;
use crate::long_polling::pull_deferred::PullDeferredService;
use crate::long_polling::pull_deferred::PullPendingOffsetReservation;
use crate::processor::notification_processor::NotificationProcessor;
use crate::processor::pop_lite_message_processor::PopLiteMessageProcessor;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pull_message_processor::PullMessageProcessor;

impl<MS> BrokerDeferredProducer<MS>
where
    MS: BrokerReadWriteStore + Send + Sync + 'static,
{
    pub(super) fn spawn_pull_pending_arrival(
        self: &Arc<Self>,
        reservation: crate::long_polling::pull_deferred::PullPendingArrivalReservation,
    ) {
        let producer = Arc::clone(self);
        if let Err(error) =
            self.task_group
                .spawn("broker.deferred.pull-pending-arrival", TaskKind::Worker, async move {
                    let Some(mut pending) = reservation.claim() else {
                        return;
                    };
                    loop {
                        let reason = pending.value_mut().reason();
                        let batch = producer.pull.reserve_pending_arrival_batch(pending.value_mut());
                        let exhausted = batch.exhausted();
                        producer.resume_pull_candidates(batch.into_candidates(), reason).await;
                        if exhausted && pending.finish_if_clean() {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
        {
            warn!(%error, "failed to submit deferred Pull pending arrival");
        }
    }

    async fn resume_pull_candidates(
        &self,
        candidates: Vec<crate::long_polling::pull_deferred::PullCandidateReservation>,
        reason: DeferredWakeReason,
    ) {
        for candidate in candidates {
            let key = candidate.key();
            let Ok(route) = self.handoff.acquire_pull_candidate(key.topic().clone(), key.queue_id()) else {
                continue;
            };
            if route.generation() != DeferredGeneration::New {
                continue;
            }
            let Ok(claimed) = self.pull.claim_candidate(candidate, reason).await else {
                continue;
            };
            let _route = route;
            submit_pull(Arc::clone(&self.pull), self.pull_processor.clone(), claimed);
        }
    }

    pub(super) fn spawn_pull_pending_offset(self: &Arc<Self>, reservation: PullPendingOffsetReservation) {
        let producer = Arc::clone(self);
        if let Err(error) = self
            .task_group
            .spawn("broker.deferred.pull-pending-offset", TaskKind::Worker, async move {
                producer.replay_pull_offset(reservation).await;
            })
        {
            warn!(%error, "failed to submit deferred Pull offset replay");
        }
    }

    async fn replay_pull_offset(&self, mut reservation: PullPendingOffsetReservation) {
        let mut covered = reservation.range();
        let mut ranges = VecDeque::from([covered]);
        loop {
            while let Some(range) = ranges.pop_front() {
                let Some(store) = self.replay_message_store() else {
                    return;
                };
                let key = reservation.key().clone();
                let mut cursor = self.pull.scan_cursor();
                loop {
                    let batch = self.pull.reserve_offset_replay_batch(&key, &mut cursor);
                    let exhausted = batch.exhausted();
                    for candidate in batch.into_candidates() {
                        let candidate_key = candidate.key();
                        let Ok(route) = self
                            .handoff
                            .acquire_pull_candidate(candidate_key.topic().clone(), candidate_key.queue_id())
                        else {
                            continue;
                        };
                        if route.generation() != DeferredGeneration::New {
                            continue;
                        }
                        let criteria = Arc::clone(candidate.criteria());
                        match store_range_matches(
                            store.as_ref(),
                            &pull_replay_group(),
                            criteria.physical_topic(),
                            criteria.physical_queue_id(),
                            range,
                            criteria.pull_from_offset(),
                            self.pull.replay_read_limit(),
                            Some(criteria.filter().clone()),
                        )
                        .await
                        {
                            StoreReplayMatch::Retry => return,
                            StoreReplayMatch::Miss => continue,
                            StoreReplayMatch::Match => {}
                        }
                        let Ok(claimed) = self
                            .pull
                            .claim_candidate(candidate, DeferredWakeReason::MessageArrived)
                            .await
                        else {
                            continue;
                        };
                        let _route = route;
                        submit_pull(Arc::clone(&self.pull), self.pull_processor.clone(), claimed);
                    }
                    if exhausted {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
            let Some(updated) = reservation.finish_or_updated() else {
                break;
            };
            enqueue_range_extensions(&mut ranges, covered, updated);
            covered = updated;
            tokio::task::yield_now().await;
        }
    }

    pub(super) fn spawn_pop_pending_arrival(
        self: &Arc<Self>,
        reservation: crate::long_polling::pop_deferred::service::PopPendingArrivalReservation,
    ) {
        let producer = Arc::clone(self);
        if let Err(error) = self
            .task_group
            .spawn("broker.deferred.pop-pending-arrival", TaskKind::Worker, async move {
                let Some(mut pending) = reservation.claim() else {
                    return;
                };
                loop {
                    let batch = producer.pop.pending_consumer_group_batch(pending.value_mut());
                    let exhausted = batch.exhausted();
                    for consumer_group in batch.into_consumer_groups() {
                        let arrival = pending.value_mut().view(&consumer_group);
                        let Some(candidate) = producer
                            .pop
                            .reserve_arrival_candidate(arrival, PopSelectionOrder::Oldest)
                        else {
                            continue;
                        };
                        let key = candidate.key();
                        let Ok(route) = producer.handoff.acquire_pop_candidate(
                            key.topic().clone(),
                            key.consumer_group().clone(),
                            key.queue_id(),
                        ) else {
                            continue;
                        };
                        if route.generation() != DeferredGeneration::New {
                            continue;
                        }
                        let Ok(claimed) = producer
                            .pop
                            .claim_candidate(candidate, DeferredWakeReason::MessageArrived)
                            .await
                        else {
                            continue;
                        };
                        let _route = route;
                        submit_pop(Arc::clone(&producer.pop), producer.pop_processor.clone(), claimed);
                    }
                    if exhausted && pending.finish_if_clean() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
        {
            warn!(%error, "failed to submit deferred POP pending arrival");
        }
    }

    pub(super) fn spawn_pop_pending_offset(self: &Arc<Self>, reservation: PopPendingOffsetReservation) {
        let producer = Arc::clone(self);
        if let Err(error) = self
            .task_group
            .spawn("broker.deferred.pop-pending-offset", TaskKind::Worker, async move {
                producer.replay_pop_offset(reservation).await;
            })
        {
            warn!(%error, "failed to submit deferred POP offset replay");
        }
    }

    async fn replay_pop_offset(&self, mut reservation: PopPendingOffsetReservation) {
        let mut covered = reservation.range();
        let mut ranges = VecDeque::from([covered]);
        loop {
            while let Some(range) = ranges.pop_front() {
                let Some(store) = self.replay_message_store() else {
                    return;
                };
                let target = reservation.key().clone();
                for offset in range.first..=range.last {
                    let mut cursor = PopFanoutCursor::new();
                    loop {
                        let batch = self.pop.pending_offset_consumer_group_batch(&target, &mut cursor);
                        let exhausted = batch.exhausted();
                        for consumer_group in batch.into_consumer_groups() {
                            let mut skipped = Vec::new();
                            for _ in 0..self.pop.replay_read_limit() {
                                let arrival =
                                    PopArrivalView::new(target.topic(), &consumer_group, target.queue_id()).forced();
                                let Some(candidate) =
                                    self.pop.reserve_arrival_candidate(arrival, PopSelectionOrder::Oldest)
                                else {
                                    break;
                                };
                                let key = candidate.key();
                                let Ok(route) = self.handoff.acquire_pop_candidate(
                                    key.topic().clone(),
                                    key.consumer_group().clone(),
                                    key.queue_id(),
                                ) else {
                                    skipped.push(candidate);
                                    continue;
                                };
                                if route.generation() != DeferredGeneration::New {
                                    skipped.push(candidate);
                                    continue;
                                }
                                let filter = candidate.criteria().filter().cloned();
                                match store_exact_matches(
                                    store.as_ref(),
                                    &consumer_group,
                                    target.topic(),
                                    target.queue_id(),
                                    offset,
                                    filter,
                                )
                                .await
                                {
                                    StoreReplayMatch::Retry => return,
                                    StoreReplayMatch::Miss => {
                                        skipped.push(candidate);
                                        continue;
                                    }
                                    StoreReplayMatch::Match => {}
                                }
                                if let Ok(claimed) = self
                                    .pop
                                    .claim_candidate(candidate, DeferredWakeReason::MessageArrived)
                                    .await
                                {
                                    let _route = route;
                                    submit_pop(Arc::clone(&self.pop), self.pop_processor.clone(), claimed);
                                }
                                break;
                            }
                            drop(skipped);
                        }
                        if exhausted {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    tokio::task::yield_now().await;
                }
            }
            let Some(updated) = reservation.finish_or_updated() else {
                break;
            };
            enqueue_range_extensions(&mut ranges, covered, updated);
            covered = updated;
        }
    }

    pub(super) fn spawn_notification_pending_arrival(
        self: &Arc<Self>,
        reservation: crate::long_polling::notification_deferred::service::NotificationPendingArrivalReservation,
    ) {
        let producer = Arc::clone(self);
        if let Err(error) = self.task_group.spawn(
            "broker.deferred.notification-pending-arrival",
            TaskKind::Worker,
            async move {
                let Some(mut pending) = reservation.claim() else {
                    return;
                };
                loop {
                    let batch = producer.notification.reserve_pending_arrival_batch(pending.value_mut());
                    let exhausted = batch.exhausted();
                    producer.resume_notification_candidates(batch.into_candidates()).await;
                    if exhausted && pending.finish_if_clean() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            },
        ) {
            warn!(%error, "failed to submit deferred Notification pending arrival");
        }
    }

    pub(super) fn spawn_notification_pending_offset(
        self: &Arc<Self>,
        reservation: NotificationPendingOffsetReservation,
    ) {
        let producer = Arc::clone(self);
        if let Err(error) = self.task_group.spawn(
            "broker.deferred.notification-pending-offset",
            TaskKind::Worker,
            async move {
                producer.replay_notification_offset(reservation).await;
            },
        ) {
            warn!(%error, "failed to submit deferred Notification offset replay");
        }
    }

    async fn replay_notification_offset(&self, mut reservation: NotificationPendingOffsetReservation) {
        let mut covered = reservation.range();
        let mut ranges = VecDeque::from([covered]);
        loop {
            while let Some(range) = ranges.pop_front() {
                let Some(store) = self.replay_message_store() else {
                    return;
                };
                let target = reservation.key().clone();
                let mut cursor = None;
                loop {
                    let prepared = self.notification.reserve_offset_replay_batch(&target, cursor);
                    let (candidates, next_cursor) = prepared.into_parts();
                    let exhausted = next_cursor.is_complete();
                    for candidate in candidates {
                        let key = candidate.key();
                        let Ok(route) = self.handoff.acquire_notification_candidate(
                            key.topic().clone(),
                            key.consumer_group().clone(),
                            key.queue_id(),
                        ) else {
                            continue;
                        };
                        if route.generation() != DeferredGeneration::New {
                            continue;
                        }
                        let filter = candidate.criteria().filter().cloned();
                        match store_range_matches(
                            store.as_ref(),
                            key.consumer_group(),
                            target.topic(),
                            target.queue_id(),
                            range,
                            range.first,
                            self.notification.replay_read_limit(),
                            filter,
                        )
                        .await
                        {
                            StoreReplayMatch::Retry => return,
                            StoreReplayMatch::Miss => continue,
                            StoreReplayMatch::Match => {}
                        }
                        let Ok(claimed) = self.notification.claim_arrival_candidate(candidate).await else {
                            continue;
                        };
                        let _route = route;
                        submit_notification(
                            Arc::clone(&self.notification),
                            self.notification_processor.clone(),
                            claimed,
                        );
                    }
                    if exhausted {
                        break;
                    }
                    cursor = Some(next_cursor);
                    tokio::task::yield_now().await;
                }
            }
            let Some(updated) = reservation.finish_or_updated() else {
                break;
            };
            enqueue_range_extensions(&mut ranges, covered, updated);
            covered = updated;
            tokio::task::yield_now().await;
        }
    }

    pub(super) async fn resume_notification_candidates(
        &self,
        candidates: Vec<crate::long_polling::notification_deferred::index::NotificationCandidateReservation>,
    ) {
        for candidate in candidates {
            let key = candidate.key();
            let Ok(route) = self.handoff.acquire_notification_candidate(
                key.topic().clone(),
                key.consumer_group().clone(),
                key.queue_id(),
            ) else {
                continue;
            };
            if route.generation() != DeferredGeneration::New {
                continue;
            }
            let Ok(claimed) = self.notification.claim_arrival_candidate(candidate).await else {
                continue;
            };
            let _route = route;
            submit_notification(
                Arc::clone(&self.notification),
                self.notification_processor.clone(),
                claimed,
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoreReplayMatch {
    Match,
    Miss,
    Retry,
}

trait DeferredReplayRead: Send + Sync {
    fn read_message<'a>(
        &'a self,
        group: &'a CheetahString,
        topic: &'a CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
        filter: Option<ArcMessageFilter>,
    ) -> impl Future<Output = Option<GetMessageResult>> + Send + 'a;
}

impl<MS> DeferredReplayRead for MS
where
    MS: BrokerReadWriteStore,
{
    fn read_message<'a>(
        &'a self,
        group: &'a CheetahString,
        topic: &'a CheetahString,
        queue_id: i32,
        offset: i64,
        max_messages: i32,
        filter: Option<ArcMessageFilter>,
    ) -> impl Future<Output = Option<GetMessageResult>> + Send + 'a {
        self.get_message(group, topic, queue_id, offset, max_messages, filter)
    }
}

fn enqueue_range_extensions(
    ranges: &mut VecDeque<PendingOffsetRange>,
    covered: PendingOffsetRange,
    updated: PendingOffsetRange,
) {
    if updated.first < covered.first {
        if let Some(last) = covered.first.checked_sub(1) {
            ranges.push_back(PendingOffsetRange {
                first: updated.first,
                last,
            });
        }
    }
    if updated.last > covered.last {
        if let Some(first) = covered.last.checked_add(1) {
            ranges.push_back(PendingOffsetRange {
                first,
                last: updated.last,
            });
        }
    }
}

fn pull_replay_group() -> CheetahString {
    CheetahString::from_static_str("__BROKER_DEFERRED_REPLAY")
}

#[allow(clippy::too_many_arguments, reason = "mirrors the Store range read contract")]
async fn store_range_matches<MS: DeferredReplayRead>(
    store: &MS,
    group: &CheetahString,
    topic: &CheetahString,
    queue_id: i32,
    range: PendingOffsetRange,
    minimum_offset: i64,
    read_limit: i32,
    filter: Option<ArcMessageFilter>,
) -> StoreReplayMatch {
    let mut offset = range.first.max(minimum_offset);
    while offset <= range.last {
        let remaining = range.last.saturating_sub(offset).saturating_add(1);
        let batch = i32::try_from(remaining).unwrap_or(i32::MAX).min(read_limit.max(1));
        let Some(result) = store
            .read_message(group, topic, queue_id, offset, batch, filter.clone())
            .await
        else {
            return StoreReplayMatch::Retry;
        };
        if result.message_count() > 0 && result.next_begin_offset() <= range.last.saturating_add(1) {
            return StoreReplayMatch::Match;
        }
        let next = result.next_begin_offset();
        if next <= offset || next > range.last {
            return StoreReplayMatch::Miss;
        }
        offset = next;
        tokio::task::yield_now().await;
    }
    StoreReplayMatch::Miss
}

async fn store_exact_matches<MS: DeferredReplayRead>(
    store: &MS,
    group: &CheetahString,
    topic: &CheetahString,
    queue_id: i32,
    offset: i64,
    filter: Option<ArcMessageFilter>,
) -> StoreReplayMatch {
    let Ok(expected_offset) = u64::try_from(offset) else {
        return StoreReplayMatch::Miss;
    };
    let Some(result) = store.read_message(group, topic, queue_id, offset, 1, filter).await else {
        return StoreReplayMatch::Retry;
    };
    let message_count = result.message_count();
    if message_count <= 0 {
        return StoreReplayMatch::Miss;
    }
    let Some(expected_next_offset) = offset.checked_add(i64::from(message_count)) else {
        return StoreReplayMatch::Miss;
    };
    if result.message_queue_offset().as_slice() == [expected_offset]
        && result.next_begin_offset() == expected_next_offset
    {
        StoreReplayMatch::Match
    } else {
        StoreReplayMatch::Miss
    }
}

#[cfg(test)]
#[allow(
    clippy::items_after_test_module,
    reason = "replay tests stay adjacent to the private Store matching seam they exercise"
)]
mod replay_tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use parking_lot::Mutex;
    use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
    use rocketmq_store::MessageFilter;

    use super::*;
    use crate::long_polling::pop_deferred::index::PopMatchCriteria;

    struct ScriptedReplayStore {
        responses: Mutex<VecDeque<Option<GetMessageResult>>>,
        filtered_reads: AtomicUsize,
    }

    impl ScriptedReplayStore {
        fn new(responses: Vec<Option<GetMessageResult>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                filtered_reads: AtomicUsize::new(0),
            }
        }
    }

    impl DeferredReplayRead for ScriptedReplayStore {
        fn read_message<'a>(
            &'a self,
            _group: &'a CheetahString,
            _topic: &'a CheetahString,
            _queue_id: i32,
            _offset: i64,
            _max_messages: i32,
            filter: Option<ArcMessageFilter>,
        ) -> impl Future<Output = Option<GetMessageResult>> + Send + 'a {
            if filter.is_some() {
                self.filtered_reads.fetch_add(1, Ordering::AcqRel);
            }
            let response = self.responses.lock().pop_front().flatten();
            async move { response }
        }
    }

    struct MarkerFilter;

    impl MessageFilter for MarkerFilter {
        fn is_matched_by_consume_queue(
            &self,
            _tags_code: Option<i64>,
            _cq_ext_unit: Option<&rocketmq_store::CqExtUnit>,
        ) -> bool {
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

    struct TagFilter(i64);

    impl MessageFilter for TagFilter {
        fn is_matched_by_consume_queue(
            &self,
            tags_code: Option<i64>,
            _cq_ext_unit: Option<&rocketmq_store::CqExtUnit>,
        ) -> bool {
            tags_code == Some(self.0)
        }

        fn is_matched_by_commit_log(
            &self,
            _msg_buffer: Option<&[u8]>,
            _properties: Option<&HashMap<CheetahString, CheetahString>>,
        ) -> bool {
            true
        }
    }

    struct TaggedReplayStore {
        tags_by_offset: HashMap<i64, i64>,
    }

    impl DeferredReplayRead for TaggedReplayStore {
        fn read_message<'a>(
            &'a self,
            _group: &'a CheetahString,
            _topic: &'a CheetahString,
            _queue_id: i32,
            offset: i64,
            _max_messages: i32,
            filter: Option<ArcMessageFilter>,
        ) -> impl Future<Output = Option<GetMessageResult>> + Send + 'a {
            let matched = self.tags_by_offset.get(&offset).is_some_and(|tag| {
                filter.as_ref().is_none_or(|filter| {
                    let cq = rocketmq_store::CqExtUnit::new(*tag, 0, None);
                    filter.is_matched_by_consume_queue(Some(*tag), Some(&cq))
                })
            });
            async move { Some(result(i32::from(matched), offset.saturating_add(1))) }
        }
    }

    fn result(message_count: i32, next_offset: i64) -> GetMessageResult {
        let mut result = GetMessageResult::new();
        result.set_message_count(message_count);
        result.set_next_begin_offset(next_offset);
        if message_count > 0 {
            if let Ok(queue_offset) = u64::try_from(next_offset.saturating_sub(1)) {
                result.set_message_queue_offset(vec![queue_offset]);
            }
        }
        result
    }

    fn result_with_queue_offset(message_count: i32, next_offset: i64, queue_offset: u64) -> GetMessageResult {
        let mut result = result(message_count, next_offset);
        result.set_message_queue_offset(vec![queue_offset]);
        result
    }

    #[tokio::test]
    async fn unavailable_store_probe_retries_without_converting_to_forced_match() {
        let store = ScriptedReplayStore::new(vec![None, Some(result(1, 11))]);
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("topic-a");
        let filter: ArcMessageFilter = Arc::new(MarkerFilter);
        let range = PendingOffsetRange { first: 10, last: 10 };

        assert_eq!(
            store_range_matches(&store, &group, &topic, 0, range, 10, 1, Some(filter.clone())).await,
            StoreReplayMatch::Retry
        );
        assert_eq!(
            store_range_matches(&store, &group, &topic, 0, range, 10, 1, Some(filter)).await,
            StoreReplayMatch::Match
        );
        assert_eq!(store.filtered_reads.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn exact_pop_replay_requires_requested_offset_and_exact_batch_span() {
        let store = ScriptedReplayStore::new(vec![
            Some(result_with_queue_offset(3, 13, 10)),
            Some(result_with_queue_offset(3, 13, 11)),
            Some(result_with_queue_offset(3, 12, 10)),
            Some(result_with_queue_offset(1, i64::MAX, i64::MAX as u64)),
        ]);
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("topic-a");
        let filter: ArcMessageFilter = Arc::new(MarkerFilter);

        assert_eq!(
            store_exact_matches(&store, &group, &topic, 0, 10, Some(filter.clone())).await,
            StoreReplayMatch::Match,
            "one batch CQ unit may advance by its positive message count"
        );
        assert_eq!(
            store_exact_matches(&store, &group, &topic, 0, 10, Some(filter.clone())).await,
            StoreReplayMatch::Miss,
            "a filtered hit at offset 11 cannot satisfy the offset-10 arrival"
        );
        assert_eq!(
            store_exact_matches(&store, &group, &topic, 0, 10, Some(filter.clone())).await,
            StoreReplayMatch::Miss,
            "the reported span must equal the positive message count"
        );
        assert_eq!(
            store_exact_matches(&store, &group, &topic, 0, i64::MAX, Some(filter)).await,
            StoreReplayMatch::Miss,
            "an overflowing exact span must fail closed"
        );
    }

    #[tokio::test]
    async fn consecutive_tag_events_use_each_waiters_frozen_filter() {
        let store = TaggedReplayStore {
            tags_by_offset: HashMap::from([(10, 1), (11, 2)]),
        };
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("topic-a");
        let tag_one = PopMatchCriteria::new(Some(SubscriptionData::default()), Some(Arc::new(TagFilter(1))));
        let tag_two = PopMatchCriteria::new(Some(SubscriptionData::default()), Some(Arc::new(TagFilter(2))));

        assert_eq!(
            store_exact_matches(&store, &group, &topic, 0, 10, tag_one.filter().cloned()).await,
            StoreReplayMatch::Match
        );
        assert_eq!(
            store_exact_matches(&store, &group, &topic, 0, 10, tag_two.filter().cloned()).await,
            StoreReplayMatch::Miss
        );
        assert_eq!(
            store_exact_matches(&store, &group, &topic, 0, 11, tag_two.filter().cloned()).await,
            StoreReplayMatch::Match
        );
        assert_eq!(
            store_exact_matches(&store, &group, &topic, 0, 11, tag_one.filter().cloned()).await,
            StoreReplayMatch::Miss
        );
    }

    #[test]
    fn concurrent_range_growth_enqueues_only_unprocessed_offsets() {
        let mut ranges = VecDeque::new();
        enqueue_range_extensions(
            &mut ranges,
            PendingOffsetRange { first: 10, last: 12 },
            PendingOffsetRange { first: 8, last: 15 },
        );
        assert_eq!(
            ranges,
            VecDeque::from([
                PendingOffsetRange { first: 8, last: 9 },
                PendingOffsetRange { first: 13, last: 15 },
            ])
        );
    }
}

pub(super) fn submit_pop<MS>(
    service: Arc<PopDeferredService>,
    processor: Weak<PopMessageProcessor<MS>>,
    claim: rocketmq_transport::api::v2::ClaimedDeferred<crate::long_polling::pop_deferred::service::ResumePop>,
) where
    MS: BrokerReadWriteStore + Send + Sync + 'static,
{
    let Some(processor) = processor.upgrade() else {
        return;
    };
    let retained = DeferredResumeRetainedSize::new(std::mem::size_of::<Arc<PopMessageProcessor<MS>>>());
    let _ = service.submit_claimed(claim, retained, move |resume, reason| async move {
        processor.resume_pop(resume, reason).await
    });
}

pub(super) fn submit_pull<MS>(
    service: Arc<PullDeferredService>,
    processor: Weak<PullMessageProcessor<MS>>,
    claim: rocketmq_transport::api::v2::ClaimedDeferred<crate::long_polling::pull_deferred::ResumePull>,
) where
    MS: BrokerReadWriteStore + Send + Sync + 'static,
{
    let Some(processor) = processor.upgrade() else {
        return;
    };
    let retained = DeferredResumeRetainedSize::new(std::mem::size_of::<Arc<PullMessageProcessor<MS>>>());
    let _ = service.submit_claimed(claim, retained, move |resume, reason| async move {
        processor.resume_pull(resume, reason).await
    });
}

pub(super) fn submit_notification<MS>(
    service: Arc<NotificationDeferredService>,
    processor: Weak<NotificationProcessor<MS>>,
    claim: rocketmq_transport::api::v2::ClaimedDeferred<
        crate::long_polling::notification_deferred::service::ResumeNotification,
    >,
) where
    MS: BrokerReadWriteStore + Send + Sync + 'static,
{
    let Some(processor) = processor.upgrade() else {
        return;
    };
    let retained = DeferredResumeRetainedSize::new(std::mem::size_of::<Arc<NotificationProcessor<MS>>>());
    let _ = service.submit_claimed(claim, retained, move |resume, reason| async move {
        processor.resume_notification(resume, reason).await
    });
}

pub(super) fn submit_pop_lite_event<MS>(
    service: Arc<PopLiteDeferredService>,
    processor: Weak<PopLiteMessageProcessor<MS>>,
    claim: crate::long_polling::pop_lite_deferred::service::PopLiteEventClaim,
) where
    MS: BrokerReadWriteStore + Send + Sync + 'static,
{
    let Some(processor) = processor.upgrade() else {
        return;
    };
    let retained = DeferredResumeRetainedSize::new(std::mem::size_of::<Arc<PopLiteMessageProcessor<MS>>>());
    let _ = service.submit_event_claim(claim, retained, move |resume, reason, events| async move {
        processor.resume_pop_lite(resume, reason, events).await
    });
}

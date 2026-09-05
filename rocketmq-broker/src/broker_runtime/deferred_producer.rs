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
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_runtime::TaskKind;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_transport::api::DeferredClaimOutcome;
use rocketmq_transport::api::DeferredResumeRetainedSize;
use rocketmq_transport::api::DeferredWakeReason;
use tracing::warn;

use crate::deferred_generation_handoff::DeferredGenerationHandoff;
use crate::deferred_generation_handoff::RoutePermit;
use crate::lite::lite_event_dispatcher::DeferredEventObserver;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;
use crate::long_polling::notification_deferred::index::NotificationArrivalView;
use crate::long_polling::notification_deferred::service::NotificationDeferredService;
use crate::long_polling::pop_deferred::index::PopArrivalView;
use crate::long_polling::pop_deferred::index::PopSelectionOrder;
use crate::long_polling::pop_deferred::service::PopDeferredService;
use crate::long_polling::pop_deferred::service::PopDeferredWakeupObserver;
use crate::long_polling::pop_deferred::service::PopWakeupCompletion;
use crate::long_polling::pop_lite_deferred::service::PopLiteDeferredService;
use crate::long_polling::pop_lite_deferred::service::PopLiteReplayObservation;
use crate::long_polling::pull_deferred::PullDeferredService;
use crate::processor::notification_processor::NotificationProcessor;
use crate::processor::pop_lite_message_processor::PopLiteMessageProcessor;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pull_message_processor::PullMessageProcessor;

#[cfg(test)]
#[path = "../../tests/unit/broker_runtime/deferred_producer/e2e.rs"]
mod e2e_tests;
mod workers;

use workers::submit_notification;
use workers::submit_pop;
use workers::submit_pop_lite_event;
use workers::submit_pull;

const PRODUCER_TICK: Duration = Duration::from_millis(20);

type PopLagRefreshCallback =
    dyn Fn(&CheetahString, &CheetahString) -> Option<PopWakeupCompletion> + Send + Sync + 'static;

/// Routes Broker-owned deferred wake producers without owning a second runtime.
pub(crate) struct BrokerDeferredProducer<MS: BrokerReadWriteStore> {
    handoff: Arc<DeferredGenerationHandoff>,
    pop: Arc<PopDeferredService>,
    pull: Arc<PullDeferredService>,
    notification: Arc<NotificationDeferredService>,
    pop_lite: Arc<PopLiteDeferredService>,
    lite_dispatcher: LiteEventDispatcher,
    lite_observer: Arc<DeferredEventObserver>,
    message_store: RwLock<Option<Weak<MS>>>,
    pop_lag_refresh_callback: Arc<PopLagRefreshCallback>,
    pull_processor: Weak<PullMessageProcessor<MS>>,
    pop_processor: Weak<PopMessageProcessor<MS>>,
    notification_processor: Weak<NotificationProcessor<MS>>,
    pop_lite_processor: Weak<PopLiteMessageProcessor<MS>>,
    task_group: TaskGroup,
    short_poll_interval: Duration,
}

impl<MS> BrokerDeferredProducer<MS>
where
    MS: BrokerReadWriteStore + Send + Sync + 'static,
{
    #[allow(
        clippy::too_many_arguments,
        reason = "the producer owns one explicit capability for each deferred protocol"
    )]
    pub(crate) fn new(
        handoff: Arc<DeferredGenerationHandoff>,
        pop: Arc<PopDeferredService>,
        pull: Arc<PullDeferredService>,
        notification: Arc<NotificationDeferredService>,
        pop_lite: Arc<PopLiteDeferredService>,
        lite_dispatcher: LiteEventDispatcher,
        pull_processor: &Arc<PullMessageProcessor<MS>>,
        pop_processor: &Arc<PopMessageProcessor<MS>>,
        notification_processor: &Arc<NotificationProcessor<MS>>,
        pop_lite_processor: &Arc<PopLiteMessageProcessor<MS>>,
        task_group: TaskGroup,
        short_poll_interval: Duration,
    ) -> Result<Arc<Self>, BrokerDeferredProducerInstallError> {
        Self::new_with_store(
            handoff,
            pop,
            pull,
            notification,
            pop_lite,
            lite_dispatcher,
            Weak::new(),
            pull_processor,
            pop_processor,
            notification_processor,
            pop_lite_processor,
            task_group,
            short_poll_interval,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the producer owns one explicit capability for each deferred protocol"
    )]
    pub(crate) fn new_with_store(
        handoff: Arc<DeferredGenerationHandoff>,
        pop: Arc<PopDeferredService>,
        pull: Arc<PullDeferredService>,
        notification: Arc<NotificationDeferredService>,
        pop_lite: Arc<PopLiteDeferredService>,
        lite_dispatcher: LiteEventDispatcher,
        message_store: Weak<MS>,
        pull_processor: &Arc<PullMessageProcessor<MS>>,
        pop_processor: &Arc<PopMessageProcessor<MS>>,
        notification_processor: &Arc<NotificationProcessor<MS>>,
        pop_lite_processor: &Arc<PopLiteMessageProcessor<MS>>,
        task_group: TaskGroup,
        short_poll_interval: Duration,
    ) -> Result<Arc<Self>, BrokerDeferredProducerInstallError> {
        let producer = Arc::new_cyclic(|weak_producer: &Weak<Self>| {
            let weak_lite = weak_producer.clone();
            let lite_observer: Arc<DeferredEventObserver> = Arc::new(move |client_id| {
                weak_lite
                    .upgrade()
                    .is_some_and(|producer| producer.notify_pop_lite_event(client_id))
            });
            let weak_pop = weak_producer.clone();
            let pop_lag_refresh_callback: Arc<PopLagRefreshCallback> = Arc::new(move |topic, consumer_group| {
                let producer = weak_pop.upgrade()?;
                producer.notify_pop_lag_refresh(topic, consumer_group)
            });
            Self {
                handoff,
                pop,
                pull,
                notification,
                pop_lite,
                lite_dispatcher: lite_dispatcher.clone(),
                lite_observer,
                message_store: RwLock::new(None),
                pop_lag_refresh_callback,
                pull_processor: Arc::downgrade(pull_processor),
                pop_processor: Arc::downgrade(pop_processor),
                notification_processor: Arc::downgrade(notification_processor),
                pop_lite_processor: Arc::downgrade(pop_lite_processor),
                task_group,
                short_poll_interval: short_poll_interval.max(Duration::from_millis(1)),
            }
        });
        if message_store.strong_count() != 0 && producer.bind_message_store(message_store).is_err() {
            return Err(BrokerDeferredProducerInstallError::ReplayStore);
        }
        if lite_dispatcher
            .install_deferred_event_observer(Arc::clone(&producer.lite_observer))
            .is_err()
        {
            return Err(BrokerDeferredProducerInstallError::LiteEventObserver);
        }
        if pop_processor
            .install_lag_refresh_producer(Arc::clone(&producer.pop_lag_refresh_callback))
            .is_err()
        {
            lite_dispatcher.uninstall_deferred_event_observer(&producer.lite_observer);
            return Err(BrokerDeferredProducerInstallError::PopLagRefresh);
        }
        Ok(producer)
    }

    /// Binds the Store replay capability after exclusive Store mutation has
    /// completed. Holding this `Weak` before `Arc::get_mut` would defeat the
    /// Broker's exclusive listener installation boundary.
    pub(crate) fn bind_message_store(&self, message_store: Weak<MS>) -> Result<(), Weak<MS>> {
        if message_store.upgrade().is_none() {
            return Err(message_store);
        }
        let mut slot = self.message_store.write();
        if slot.is_some() {
            return Err(message_store);
        }
        *slot = Some(message_store);
        Ok(())
    }

    /// Drops the Store replay capability after every producer task has joined.
    ///
    /// This must run before the Broker requests exclusive access to the Store:
    /// `Arc::get_mut` rejects even a live `Weak` owner.
    pub(crate) fn unbind_message_store(&self) -> bool {
        self.message_store.write().take().is_some()
    }

    fn replay_message_store(&self) -> Option<Arc<MS>> {
        self.message_store.read().as_ref().and_then(Weak::upgrade)
    }

    #[cfg(test)]
    fn retry_pending_for_test(self: &Arc<Self>) {
        self.produce_pending_arrivals();
    }

    pub(crate) fn start(self: &Arc<Self>) -> RuntimeResult<Vec<TaskId>> {
        let mut tasks = Vec::with_capacity(2);
        let producer = Arc::clone(self);
        let cancellation = self.task_group.cancellation_token();
        tasks.push(
            self.task_group
                .spawn_service("broker.deferred.expiry-and-events", async move {
                    loop {
                        if tokio::time::timeout(PRODUCER_TICK, cancellation.cancelled())
                            .await
                            .is_ok()
                        {
                            break;
                        }
                        producer.produce_pending_arrivals();
                        producer.produce_expired();
                        producer.produce_pending_dispatcher_events();
                        producer.produce_pending_pop_lite_events();
                    }
                })?,
        );

        let producer = Arc::clone(self);
        let cancellation = self.task_group.cancellation_token();
        let interval = self.short_poll_interval;
        tasks.push(
            self.task_group
                .spawn_service("broker.deferred.pull-short-poll", async move {
                    loop {
                        if tokio::time::timeout(interval, cancellation.cancelled()).await.is_ok() {
                            break;
                        }
                        producer.produce_pull_short_poll();
                    }
                })?,
        );
        Ok(tasks)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the arrival boundary intentionally retains the Store callback shape"
    )]
    pub(crate) fn route_pull_arrival(
        self: &Arc<Self>,
        topic: &CheetahString,
        queue_id: i32,
        logic_offset: i64,
        _tags_code: Option<i64>,
        _message_store_time: i64,
        _filter_bitmap: Option<&[u8]>,
        _properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        let Ok(route) = self.handoff.acquire_pull_candidate(topic.clone(), queue_id) else {
            return;
        };
        match self.pull.latch_offset(topic, queue_id, logic_offset) {
            Ok(()) => self.produce_pending_pull_offsets(),
            Err(error) => warn!(?error, "failed to retain deferred Pull arrival replay"),
        }
        drop(route);
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the arrival boundary intentionally retains the Store callback shape"
    )]
    pub(crate) fn route_pop_arrival(
        self: &Arc<Self>,
        topic: &CheetahString,
        queue_id: i32,
        tags_code: Option<i64>,
        message_store_time: i64,
        filter_bitmap: Option<&[u8]>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        match self.pop.latch_arrival(
            topic,
            queue_id,
            tags_code,
            message_store_time,
            filter_bitmap,
            properties,
            self.pop.fanout_cursor(),
        ) {
            Ok(()) => self.produce_pending_pop_arrivals(),
            Err(error) => warn!(?error, "failed to retain deferred POP arrival replay"),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the arrival boundary preserves the Store listener callback shape"
    )]
    pub(crate) fn route_pop_arrival_at(
        self: &Arc<Self>,
        topic: &CheetahString,
        queue_id: i32,
        logic_offset: i64,
        _tags_code: Option<i64>,
        _message_store_time: i64,
        _filter_bitmap: Option<&[u8]>,
        _properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        match self.pop.latch_offset(topic, queue_id, logic_offset) {
            Ok(()) => self.produce_pending_pop_offsets(),
            Err(error) => warn!(?error, "failed to retain deferred POP offset replay"),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the arrival boundary intentionally retains the Store callback shape"
    )]
    pub(crate) fn route_notification_arrival(
        self: &Arc<Self>,
        topic: &CheetahString,
        queue_id: i32,
        tags_code: Option<i64>,
        message_store_time: i64,
        filter_bitmap: Option<&[u8]>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        let arrival = NotificationArrivalView::new(topic, queue_id).with_filter_metadata(
            tags_code,
            message_store_time,
            filter_bitmap,
            properties,
        );
        match self
            .notification
            .latch_arrival(arrival, self.notification.arrival_cursor(arrival))
        {
            Ok(()) => self.produce_pending_notification_arrivals(),
            Err(error) => warn!(?error, "failed to retain deferred Notification arrival replay"),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the arrival boundary preserves the Store listener callback shape"
    )]
    pub(crate) fn route_notification_arrival_at(
        self: &Arc<Self>,
        topic: &CheetahString,
        queue_id: i32,
        logic_offset: i64,
        _tags_code: Option<i64>,
        _message_store_time: i64,
        _filter_bitmap: Option<&[u8]>,
        _properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        match self.notification.latch_offset(topic, queue_id, logic_offset) {
            Ok(()) => self.produce_pending_notification_offsets(),
            Err(error) => warn!(?error, "failed to retain deferred Notification offset replay"),
        }
    }

    pub(crate) fn notify_pop_lag_refresh(
        &self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
    ) -> Option<PopWakeupCompletion> {
        for key in self.pop.forced_target_batch(topic, consumer_group) {
            let Ok(route) =
                self.handoff
                    .acquire_pop_candidate(key.topic().clone(), key.consumer_group().clone(), key.queue_id())
            else {
                continue;
            };
            let arrival = PopArrivalView::new(topic, consumer_group, key.queue_id()).forced();
            let Some(candidate) = self
                .pop
                .reserve_target_arrival_candidate(&key, arrival, PopSelectionOrder::Oldest)
            else {
                continue;
            };
            let (observer, completion) = PopDeferredWakeupObserver::new();
            self.spawn_observed_pop_candidate(candidate, observer, route);
            return Some(completion);
        }
        None
    }

    fn notify_pop_lite_event(&self, client_id: &CheetahString) -> bool {
        let Ok(route) = self.handoff.acquire_pop_lite_candidate(client_id.clone()) else {
            return true;
        };
        match self.pop_lite.observe_pending_replay(client_id) {
            PopLiteReplayObservation::NoEvent | PopLiteReplayObservation::AlreadyObserved => true,
            PopLiteReplayObservation::NewlyObserved => {
                self.spawn_pop_lite_client(client_id.clone(), route);
                true
            }
        }
    }

    fn produce_pending_pop_lite_events(&self) {
        for client_id in self.pop_lite.take_pending_replays(NonZeroUsize::MIN) {
            let _ = self.notify_pop_lite_event(&client_id);
        }
    }

    fn produce_pending_dispatcher_events(&self) {
        for client_id in self.lite_dispatcher.pending_client_ids() {
            self.lite_dispatcher.replay_pending_event(&client_id);
        }
    }

    fn produce_pull_short_poll(self: &Arc<Self>) {
        let Some(processor) = self.pull_processor.upgrade() else {
            return;
        };
        for key in self.pull.target_batch() {
            let Ok(_route) = self.handoff.acquire_pull_candidate(key.topic().clone(), key.queue_id()) else {
                continue;
            };
            let Some(max_offset) = processor.deferred_current_max_offset(key.topic(), key.queue_id()) else {
                continue;
            };
            match self
                .pull
                .latch_max_offset_range(key.topic(), key.queue_id(), max_offset)
            {
                Ok(()) => self.produce_pending_pull_offsets(),
                Err(error) => warn!(?error, "failed to retain deferred Pull short-poll replay"),
            }
        }
    }

    fn produce_pending_arrivals(self: &Arc<Self>) {
        self.produce_pending_pull_arrivals();
        self.produce_pending_pull_offsets();
        self.produce_pending_pop_arrivals();
        self.produce_pending_pop_offsets();
        self.produce_pending_notification_arrivals();
        self.produce_pending_notification_offsets();
    }

    fn produce_pending_pull_arrivals(self: &Arc<Self>) {
        for reservation in self.pull.pending_arrival_reservations() {
            self.spawn_pull_pending_arrival(reservation);
        }
    }

    fn produce_pending_pop_arrivals(self: &Arc<Self>) {
        for reservation in self.pop.pending_arrival_reservations() {
            self.spawn_pop_pending_arrival(reservation);
        }
    }

    fn produce_pending_notification_arrivals(self: &Arc<Self>) {
        for reservation in self.notification.pending_arrival_reservations() {
            self.spawn_notification_pending_arrival(reservation);
        }
    }

    fn produce_pending_pull_offsets(self: &Arc<Self>) {
        for reservation in self.pull.pending_offset_reservations() {
            self.spawn_pull_pending_offset(reservation);
        }
    }

    fn produce_pending_pop_offsets(self: &Arc<Self>) {
        for reservation in self.pop.pending_offset_reservations() {
            self.spawn_pop_pending_offset(reservation);
        }
    }

    fn produce_pending_notification_offsets(self: &Arc<Self>) {
        for reservation in self.notification.pending_offset_reservations() {
            self.spawn_notification_pending_offset(reservation);
        }
    }

    fn produce_expired(&self) {
        for claim in self.pop.sweep_expired().into_claims() {
            self.spawn_pop_claim(claim);
        }
        for claim in self.pull.sweep_expired().into_claims() {
            self.spawn_pull_claim(claim);
        }
        for claim in self.notification.sweep_expired().into_claims() {
            self.spawn_notification_claim(claim);
        }
        for claim in self.pop_lite.sweep_expired().into_claims() {
            self.spawn_pop_lite_timeout_claim(claim);
        }
    }

    fn spawn_pull_candidates(
        &self,
        candidates: Vec<crate::long_polling::pull_deferred::PullCandidateReservation>,
        reason: DeferredWakeReason,
    ) {
        for candidate in candidates {
            let key = candidate.key();
            let Ok(route) = self.handoff.acquire_pull_candidate(key.topic().clone(), key.queue_id()) else {
                continue;
            };
            self.spawn_pull_candidate(candidate, reason, route);
        }
    }

    fn spawn_pull_candidate(
        &self,
        candidate: crate::long_polling::pull_deferred::PullCandidateReservation,
        reason: DeferredWakeReason,
        route: RoutePermit,
    ) {
        let service = Arc::clone(&self.pull);
        let processor = self.pull_processor.clone();
        let spawn = self
            .task_group
            .spawn("broker.deferred.pull-arrival", TaskKind::Worker, async move {
                let _route = route;
                if let Ok(DeferredClaimOutcome::Claimed(claimed)) = service.claim_candidate(candidate, reason).await {
                    submit_pull(service, processor, claimed);
                }
            });
        if let Err(error) = spawn {
            warn!(%error, "failed to submit deferred Pull arrival");
        }
    }

    fn spawn_pop_candidate(
        &self,
        candidate: crate::long_polling::pop_deferred::index::PopCandidateReservation,
        reason: DeferredWakeReason,
        route: RoutePermit,
    ) {
        let service = Arc::clone(&self.pop);
        let processor = self.pop_processor.clone();
        let spawn = self
            .task_group
            .spawn("broker.deferred.pop-arrival", TaskKind::Worker, async move {
                let _route = route;
                if let Ok(DeferredClaimOutcome::Claimed(claimed)) = service.claim_candidate(candidate, reason).await {
                    submit_pop(service, processor, claimed);
                }
            });
        if let Err(error) = spawn {
            warn!(%error, "failed to submit deferred POP arrival");
        }
    }

    fn spawn_observed_pop_candidate(
        &self,
        candidate: crate::long_polling::pop_deferred::index::PopCandidateReservation,
        observer: PopDeferredWakeupObserver,
        route: RoutePermit,
    ) {
        let service = Arc::clone(&self.pop);
        let processor = self.pop_processor.clone();
        let spawn = self
            .task_group
            .spawn("broker.deferred.pop-lag", TaskKind::Worker, async move {
                let _route = route;
                match service.claim_forced_candidate(candidate).await {
                    Ok(DeferredClaimOutcome::Claimed(claimed)) => {
                        let Some(processor) = processor.upgrade() else {
                            drop(observer);
                            return;
                        };
                        let retained =
                            DeferredResumeRetainedSize::new(std::mem::size_of::<Arc<PopMessageProcessor<MS>>>());
                        let _ = service.submit_claimed_observed(
                            claimed,
                            retained,
                            observer,
                            move |resume, reason| async move { processor.resume_pop(resume, reason).await },
                        );
                    }
                    result => observer.complete_claim_result(&result),
                }
            });
        if let Err(error) = spawn {
            warn!(%error, "failed to submit deferred POP lag refresh");
        }
    }

    fn spawn_notification_candidate(
        &self,
        candidate: crate::long_polling::notification_deferred::index::NotificationCandidateReservation,
        route: RoutePermit,
    ) {
        let service = Arc::clone(&self.notification);
        let processor = self.notification_processor.clone();
        let spawn = self
            .task_group
            .spawn("broker.deferred.notification-arrival", TaskKind::Worker, async move {
                let _route = route;
                if let Ok(DeferredClaimOutcome::Claimed(claimed)) = service.claim_arrival_candidate(candidate).await {
                    submit_notification(service, processor, claimed);
                }
            });
        if let Err(error) = spawn {
            warn!(%error, "failed to submit deferred Notification arrival");
        }
    }

    fn spawn_pop_lite_client(&self, client_id: CheetahString, route: RoutePermit) {
        let service = Arc::clone(&self.pop_lite);
        let rejected_client = client_id.clone();
        let processor = self.pop_lite_processor.clone();
        let spawn = self
            .task_group
            .spawn("broker.deferred.pop-lite-event", TaskKind::Worker, async move {
                let _route = route;
                match service.claim_event(&client_id).await {
                    Ok(Some(claim)) => submit_pop_lite_event(Arc::clone(&service), processor, claim),
                    Ok(None) | Err(_) => {
                        service.observe_pending_event(&client_id);
                    }
                }
                service.finish_event_producer(&client_id);
            });
        if let Err(error) = spawn {
            self.pop_lite.finish_event_producer(&rejected_client);
            warn!(%error, "failed to submit deferred PopLite event");
        }
    }

    fn spawn_pop_claim(
        &self,
        claim: rocketmq_transport::api::ClaimedDeferred<crate::long_polling::pop_deferred::service::ResumePop>,
    ) {
        let service = Arc::clone(&self.pop);
        let processor = self.pop_processor.clone();
        if let Err(error) = self
            .task_group
            .spawn("broker.deferred.pop-timeout", TaskKind::Worker, async move {
                submit_pop(service, processor, claim);
            })
        {
            warn!(%error, "failed to submit deferred POP timeout");
        }
    }

    fn spawn_pull_claim(
        &self,
        claim: rocketmq_transport::api::ClaimedDeferred<crate::long_polling::pull_deferred::ResumePull>,
    ) {
        let service = Arc::clone(&self.pull);
        let processor = self.pull_processor.clone();
        if let Err(error) = self
            .task_group
            .spawn("broker.deferred.pull-timeout", TaskKind::Worker, async move {
                submit_pull(service, processor, claim);
            })
        {
            warn!(%error, "failed to submit deferred Pull timeout");
        }
    }

    fn spawn_notification_claim(
        &self,
        claim: rocketmq_transport::api::ClaimedDeferred<
            crate::long_polling::notification_deferred::service::ResumeNotification,
        >,
    ) {
        let service = Arc::clone(&self.notification);
        let processor = self.notification_processor.clone();
        if let Err(error) =
            self.task_group
                .spawn("broker.deferred.notification-timeout", TaskKind::Worker, async move {
                    submit_notification(service, processor, claim);
                })
        {
            warn!(%error, "failed to submit deferred Notification timeout");
        }
    }

    fn spawn_pop_lite_timeout_claim(
        &self,
        claim: rocketmq_transport::api::ClaimedDeferred<crate::long_polling::pop_lite_deferred::data::ResumePopLite>,
    ) {
        let service = Arc::clone(&self.pop_lite);
        let processor = self.pop_lite_processor.clone();
        if let Err(error) = self
            .task_group
            .spawn("broker.deferred.pop-lite-timeout", TaskKind::Worker, async move {
                let Some(processor) = processor.upgrade() else {
                    return;
                };
                let retained = DeferredResumeRetainedSize::new(std::mem::size_of::<Arc<PopLiteMessageProcessor<MS>>>());
                let _ = service.submit_claimed(claim, retained, move |resume, reason| async move {
                    processor.resume_pop_lite_timeout(resume, reason)
                });
            })
        {
            warn!(%error, "failed to submit deferred PopLite timeout");
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BrokerDeferredProducerInstallError {
    ReplayStore,
    LiteEventObserver,
    PopLagRefresh,
}

impl fmt::Display for BrokerDeferredProducerInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ReplayStore => "deferred replay Store capability was already bound or unavailable",
            Self::LiteEventObserver => "PopLite event observer was already installed",
            Self::PopLagRefresh => "POP lag-refresh producer was already installed",
        })
    }
}

impl Error for BrokerDeferredProducerInstallError {}

impl<MS: BrokerReadWriteStore> Drop for BrokerDeferredProducer<MS> {
    fn drop(&mut self) {
        self.lite_dispatcher
            .uninstall_deferred_event_observer(&self.lite_observer);
        if let Some(pop_processor) = self.pop_processor.upgrade() {
            pop_processor.uninstall_lag_refresh_producer(&self.pop_lag_refresh_callback);
        }
    }
}

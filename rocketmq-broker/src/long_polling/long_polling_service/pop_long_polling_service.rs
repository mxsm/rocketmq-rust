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

#![allow(unused_variables)]

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;

use crate::config::broker_config::BrokerConfig;
use cheetah_string::CheetahString;
use crossbeam_skiplist::SkipSet;
use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_model::common::key_builder::KeyBuilder;
use rocketmq_model::common::pop_ack_constants::PopAckConstants;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
#[cfg(test)]
use rocketmq_runtime::TaskKind;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::CqExtUnit;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::LegacySessionCleanupInstallError;
use rocketmq_transport::api::v1::LegacySessionExecutionEnrollment;
use tokio::select;
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::broker_task_group_or_current;
use crate::deferred_generation_handoff::DeferredGeneration;
use crate::deferred_generation_handoff::DeferredGenerationHandoff;
use crate::deferred_generation_handoff::DeferredGenerationLegacyEnrollmentError;
use crate::deferred_generation_handoff::DeferredGenerationTarget;
use crate::deferred_generation_handoff::LegacyWakeLease;
use crate::deferred_generation_handoff::RoutePermit;
use crate::long_polling::long_polling_service::LegacyExecutionTracker;
use crate::long_polling::long_polling_service::LegacyServiceFinalization;
use crate::long_polling::long_polling_service::LegacyServiceResourceSnapshot;
use crate::long_polling::long_polling_service::LegacyServiceShutdownReport;
use crate::long_polling::polling_header::PollingHeader;
use crate::long_polling::polling_result::PollingResult;
use crate::long_polling::pop_request::PopRequest;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub(crate) trait PollingCountProvider: Send + Sync {
    fn polling_count(&self, key: &str) -> i32;
}

fn remove_session_pop_waiter(
    polling_map: &DashMap<CheetahString, SkipSet<Arc<PopRequest>>>,
    total_polling_num: &AtomicU64,
    key: &CheetahString,
    request: &Weak<PopRequest>,
) {
    let Some(request) = request.upgrade() else {
        return;
    };
    request.mark_legacy_session_closed();
    let removed = polling_map
        .get(key)
        .is_some_and(|queue| queue.remove(&request).is_some());
    if removed {
        release_published_polling_count(total_polling_num);
        request.release_resource_permit();
        request.release_legacy_wait();
    }
}

fn reserve_published_polling_count(total_polling_num: &AtomicU64) -> bool {
    total_polling_num
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_add(1))
        .is_ok()
}

fn release_published_polling_count(total_polling_num: &AtomicU64) {
    let released = total_polling_num
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_sub(1))
        .is_ok();
    assert!(released, "a published POP waiter owns one total-polling count");
}

fn restore_published_polling_count(total_polling_num: &AtomicU64) {
    assert!(
        reserve_published_polling_count(total_polling_num),
        "a requeued POP waiter must restore its released count"
    );
}

#[cfg(test)]
mod notification_v1_acceptance_tests;
#[cfg(test)]
mod permit_release_tests;

#[derive(Clone)]
pub(crate) struct PopLongPollingPolicy {
    pop_polling_map_size: usize,
    max_pop_polling_size: u64,
    pop_polling_size: usize,
}

impl PopLongPollingPolicy {
    pub(crate) fn from_config(broker_config: &BrokerConfig) -> Self {
        Self {
            pop_polling_map_size: broker_config.pop_polling_map_size,
            max_pop_polling_size: broker_config.max_pop_polling_size,
            pop_polling_size: broker_config.pop_polling_size,
        }
    }
}

#[derive(Clone)]
pub(crate) struct PopLongPollingServiceContext {
    policy: PopLongPollingPolicy,
    topic_config_manager: Arc<TopicConfigManager>,
    subscription_group_lookup: SubscriptionGroupConfigLookup,
    service_context: Option<ChildServiceContext>,
}

impl PopLongPollingServiceContext {
    pub(crate) fn new(
        policy: PopLongPollingPolicy,
        topic_config_manager: Arc<TopicConfigManager>,
        subscription_group_lookup: SubscriptionGroupConfigLookup,
        service_context: Option<ChildServiceContext>,
    ) -> Self {
        Self {
            policy,
            topic_config_manager,
            subscription_group_lookup,
            service_context,
        }
    }
}

pub(crate) struct PopLongPollingService<RP> {
    context: PopLongPollingServiceContext,
    topic_cid_map: DashMap<CheetahString, DashMap<CheetahString, u8>>,
    polling_map: Arc<DashMap<CheetahString, SkipSet<Arc<PopRequest>>>>,
    last_clean_time: AtomicU64,
    total_polling_num: Arc<AtomicU64>,
    notify_last: bool,
    processor: Weak<RP>,
    running: AtomicBool,
    lifecycle: AsyncMutex<()>,
    polling_admission: Mutex<()>,
    handoff: OnceLock<Arc<DeferredGenerationHandoff>>,
    producer_task_group: Mutex<Option<TaskGroup>>,
    task_group: Mutex<Option<TaskGroup>>,
    execution_tracker: Arc<LegacyExecutionTracker>,
    shutdown_wake_failures: AtomicU64,
}

struct PopWakeupClaim {
    request: Arc<PopRequest>,
    wake: Option<LegacyWakeLease>,
    execution: Option<LegacySessionExecutionEnrollment>,
}

#[trait_variant::make(PopLongPollingRequestProcessor: Send)]
pub(crate) trait LocalPopLongPollingRequestProcessor {
    async fn process_request_when_wakeup(
        &self,
        channel: rocketmq_transport::api::v1::Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PopWakeupOutcome {
    ProcessingCompleted,
    ProcessingFailed,
    InactiveChannel,
    AlreadyCompleted,
    ProcessorUnavailable,
    ServiceNotRunning,
    ServiceCancelled,
}

pub(crate) type PopWakeupCompletion = oneshot::Receiver<PopWakeupOutcome>;

struct PopWakeupObserver {
    sender: Option<oneshot::Sender<PopWakeupOutcome>>,
}

impl PopWakeupObserver {
    fn new(sender: oneshot::Sender<PopWakeupOutcome>) -> Self {
        Self { sender: Some(sender) }
    }

    fn complete(mut self, outcome: PopWakeupOutcome) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(outcome);
        }
    }
}

impl Drop for PopWakeupObserver {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(PopWakeupOutcome::ServiceCancelled);
        }
    }
}

impl<RP: PopLongPollingRequestProcessor + Sync + 'static> PopLongPollingService<RP> {
    pub fn new(context: PopLongPollingServiceContext, notify_last: bool, processor: Weak<RP>) -> Self {
        Self {
            // 100000 topic default,  100000 lru topic + cid + qid
            topic_cid_map: DashMap::with_capacity(context.policy.pop_polling_map_size),
            polling_map: Arc::new(DashMap::with_capacity(context.policy.pop_polling_map_size)),
            last_clean_time: AtomicU64::new(0),
            total_polling_num: Arc::new(AtomicU64::new(0)),
            notify_last,
            context,
            processor,
            running: AtomicBool::new(false),
            lifecycle: AsyncMutex::new(()),
            polling_admission: Mutex::new(()),
            handoff: OnceLock::new(),
            producer_task_group: Mutex::new(None),
            task_group: Mutex::new(None),
            execution_tracker: Arc::new(LegacyExecutionTracker::default()),
            shutdown_wake_failures: AtomicU64::new(0),
        }
    }

    /// Installs the single Broker-owned generation coordinator before this
    /// service accepts any legacy request. Reinstalling the same identity is
    /// idempotent; replacing it or attaching after occupancy exists fails.
    pub(crate) fn install_handoff(
        &self,
        handoff: Arc<DeferredGenerationHandoff>,
    ) -> Result<(), Arc<DeferredGenerationHandoff>> {
        let _admission = self.polling_admission.lock();
        if let Some(installed) = self.handoff.get() {
            return if Arc::ptr_eq(installed, &handoff) {
                Ok(())
            } else {
                Err(handoff)
            };
        }
        if self.total_polling_num.load(Ordering::Acquire) != 0 {
            return Err(handoff);
        }
        self.handoff.set(handoff)
    }

    pub async fn start(this: &Arc<Self>) {
        let _lifecycle = this.lifecycle.lock().await;
        if this
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let Some(producer_task_group) = broker_task_group_or_current(
            this.context.service_context.as_ref(),
            "rocketmq-broker.long-polling.pop.producer",
            "failed to start PopLongPollingService outside Tokio runtime",
        ) else {
            this.running.store(false, Ordering::Release);
            return;
        };
        let Some(execution_task_group) = broker_task_group_or_current(
            this.context.service_context.as_ref(),
            "rocketmq-broker.long-polling.pop.executions",
            "failed to start PopLongPollingService execution owner outside Tokio runtime",
        ) else {
            this.running.store(false, Ordering::Release);
            return;
        };
        let cancellation_token = producer_task_group.cancellation_token();
        let service = Arc::downgrade(this);
        *this.producer_task_group.lock() = Some(producer_task_group.clone());
        *this.task_group.lock() = Some(execution_task_group);
        this.shutdown_wake_failures.store(0, Ordering::Release);

        let spawn_result = producer_task_group.spawn_service("broker.long-polling.pop.scan", async move {
            loop {
                select! {
                    _ = cancellation_token.cancelled() => {break;}
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(20)) => {}
                }

                let Some(service) = service.upgrade() else {
                    break;
                };

                if service.polling_map.is_empty() {
                    continue;
                }
                let polling_keys = service
                    .polling_map
                    .iter()
                    .map(|entry| entry.key().clone())
                    .collect::<Vec<_>>();
                for key in polling_keys {
                    service.wake_up_expired_requests(&key);
                }

                let last_clean_time = service.last_clean_time.load(Ordering::Acquire);
                if last_clean_time == 0 || current_millis().saturating_sub(last_clean_time) > 5 * 60 * 1000 {
                    service.clean_unused_resource();
                }
            }

            if let Some(service) = service.upgrade() {
                service.running.store(false, Ordering::Release);
            }
        });

        if let Err(error) = spawn_result {
            this.producer_task_group.lock().take();
            this.task_group.lock().take();
            this.running.store(false, Ordering::Release);
            warn!(?error, "failed to spawn PopLongPollingService scan task");
        }
    }

    pub(crate) async fn stop_producer_until(&self, deadline: ShutdownDeadline) -> Option<ShutdownReport> {
        let _lifecycle = self.lifecycle.lock().await;
        {
            let _admission = self.polling_admission.lock();
            self.running.store(false, Ordering::Release);
        }
        let task_group = self.producer_task_group.lock().take();
        match task_group {
            Some(task_group) => Some(task_group.shutdown_until(deadline).await),
            None => None,
        }
    }

    pub(crate) async fn drain_executions_until(&self, deadline: ShutdownDeadline) -> Option<ShutdownReport> {
        let _lifecycle = self.lifecycle.lock().await;
        let task_group = self.task_group.lock().take();
        match task_group {
            Some(task_group) => Some(task_group.shutdown_until(deadline).await),
            None => None,
        }
    }

    pub(crate) async fn finalize_shutdown(&self) -> LegacyServiceFinalization {
        let _lifecycle = self.lifecycle.lock().await;
        let observed_after_session_drain = self.legacy_resource_snapshot();
        let keys = self
            .polling_map
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        let retired = keys
            .into_iter()
            .filter_map(|key| self.polling_map.remove(&key).map(|(_, queue)| queue))
            .collect::<Vec<_>>();
        // Drop request cleanup enrollments only after every DashMap shard
        // guard has been released. Registration publishes cleanup -> table,
        // so fallback retirement must not hold table -> cleanup.
        for queue in retired {
            self.drain_polling_queue(&queue);
        }
        self.running.store(false, Ordering::Release);
        LegacyServiceFinalization {
            observed_after_session_drain,
            terminal: self.legacy_resource_snapshot(),
        }
    }

    pub async fn shutdown(&self) -> LegacyServiceShutdownReport {
        let deadline = ShutdownDeadline::after(Duration::from_secs(5));
        let producer = self.stop_producer_until(deadline).await;
        let executions = self.drain_executions_until(deadline).await;
        let finalization = self.finalize_shutdown().await;
        LegacyServiceShutdownReport {
            name: "pop_long_polling",
            producer,
            executions,
            observed_after_session_drain: finalization.observed_after_session_drain,
            resources: finalization.terminal,
        }
    }

    pub(crate) fn legacy_resource_snapshot(&self) -> LegacyServiceResourceSnapshot {
        LegacyServiceResourceSnapshot {
            table_entries: self.polling_map.iter().map(|queue| queue.value().len()).sum(),
            tracked_waiters: self.total_polling_num.load(Ordering::Acquire),
            active_executions: self.execution_tracker.active(),
            task_count: self
                .producer_task_group
                .lock()
                .as_ref()
                .map_or(0, TaskGroup::task_count),
            wake_task_count: self.task_group.lock().as_ref().map_or(0, TaskGroup::task_count),
            shutdown_wake_failures: self.shutdown_wake_failures.load(Ordering::Acquire),
            ..Default::default()
        }
    }

    fn clean_unused_resource(&self) {
        // Clean up topicCidMap
        {
            let mut topic_keys_to_remove = Vec::new();

            for topic_entry in self.topic_cid_map.iter() {
                let topic = topic_entry.key();

                if self.context.topic_config_manager.select_topic_config(topic).is_none() {
                    info!(target: "pop_logger", "remove non-existent topic {} in topicCidMap!", topic);
                    topic_keys_to_remove.push(topic.clone());
                    continue;
                }

                let cid_map = topic_entry.value();
                let mut cid_keys_to_remove = Vec::new();

                for cid_entry in cid_map.iter() {
                    let cid = cid_entry.key();

                    if !self.context.subscription_group_lookup.contains_subscription_group(cid) {
                        info!(target: "pop_logger", "remove non-existent sub {} of topic {} in topicCidMap!", cid, topic);
                        cid_keys_to_remove.push(cid.clone());
                    }
                }

                // Remove CIDs outside the iteration
                for cid in cid_keys_to_remove {
                    cid_map.remove(&cid);
                }
            }

            // Remove topics outside the iteration
            for topic in topic_keys_to_remove {
                self.topic_cid_map.remove(&topic);
            }
        }

        {
            // Clean up pollingMap
            let mut polling_keys_to_remove = Vec::new();

            for polling_entry in self.polling_map.iter() {
                let key = polling_entry.key();

                if key.is_empty() {
                    continue;
                }

                let key_array: Vec<&str> = key.split_str(PopAckConstants::SPLIT).collect();
                if key_array.len() != 3 {
                    continue;
                }

                let topic = CheetahString::from_slice(key_array[0]);
                let cid = CheetahString::from_slice(key_array[1]);

                if self.context.topic_config_manager.select_topic_config(&topic).is_none() {
                    info!(target: "pop_logger", "remove non-existent topic {} in pollingMap!", topic);
                    polling_keys_to_remove.push(key.clone());
                    continue;
                }
                if !self.context.subscription_group_lookup.contains_subscription_group(&cid) {
                    info!(target: "pop_logger", "remove non-existent sub {} of topic {} in pollingMap!", cid, topic);
                    polling_keys_to_remove.push(key.clone());
                }
            }

            // Remove polling entries outside the iteration
            for key in polling_keys_to_remove {
                if let Some((_, queue)) = self.polling_map.remove(&key) {
                    self.discard_polling_queue(&queue);
                }
            }
        }
        self.last_clean_time.store(current_millis(), Ordering::Release);
    }

    pub fn notify_message_arriving(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        cid: &CheetahString,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        self.take_matching_request(
            topic,
            queue_id,
            cid,
            false,
            tags_code,
            msg_store_time,
            filter_bit_map,
            properties,
        )
        .is_some_and(|claim| self.wake_up_claim(claim, None))
    }

    pub(crate) fn notify_message_arriving_before_lag(
        &self,
        topic: &CheetahString,
        cid: &CheetahString,
    ) -> Option<PopWakeupCompletion> {
        self.take_matching_request(topic, -1, cid, true, None, 0, None, None)
            .map(|claim| self.wake_up_claim_with_completion(claim))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the long-polling match boundary mirrors the message-arrival metadata"
    )]
    fn take_matching_request(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        cid: &CheetahString,
        force: bool,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> Option<PopWakeupClaim> {
        let _admission = self.polling_admission.lock();
        if !self.running.load(Ordering::Acquire) {
            return None;
        }
        let key = CheetahString::from_string(KeyBuilder::build_polling_key(topic, cid, queue_id));
        if let Some((pop_request, route)) = self.poll_remoting_commands(&key) {
            let (message_filter, subscription_data) =
                (pop_request.get_message_filter(), pop_request.get_subscription_data());

            if !force {
                if let (Some(message_filter), Some(_subscription_data)) = (message_filter, subscription_data) {
                    let mut match_result = message_filter.is_matched_by_consume_queue(
                        tags_code,
                        Some(&CqExtUnit::new(
                            tags_code.unwrap_or_default(),
                            msg_store_time,
                            filter_bit_map,
                        )),
                    );
                    if match_result {
                        if let Some(props) = properties {
                            match_result = message_filter.is_matched_by_commit_log(None, Some(props));
                        }
                    }
                    if !match_result {
                        self.requeue_claimed_request(key, pop_request);
                        return None;
                    }
                }
            }

            if let Some(claim) = self.begin_wake(Arc::clone(&pop_request), route) {
                return Some(claim);
            }
            self.requeue_claimed_request(key, pop_request);
        }
        None
    }

    fn requeue_claimed_request(&self, key: CheetahString, request: Arc<PopRequest>) {
        // Restore accounting before publishing the node, matching fresh
        // registration. A concurrent cleanup can then remove the node without
        // observing an uncounted waiter.
        restore_published_polling_count(&self.total_polling_num);
        self.polling_map
            .entry(key.clone())
            .or_default()
            .insert(Arc::clone(&request));
        let _ = self.retract_terminal_requeue(&key, &request);
    }

    fn retract_terminal_requeue(&self, key: &CheetahString, request: &Arc<PopRequest>) -> bool {
        if !request.legacy_session_closed() {
            return false;
        }
        let removed = self
            .polling_map
            .get(key)
            .is_some_and(|queue| queue.remove(request).is_some());
        // Cleanup and terminal reread race to remove the exact Arc. Only the
        // winner owns count, retained-budget, and handoff release.
        if removed {
            release_published_polling_count(&self.total_polling_num);
            request.release_resource_permit();
            request.release_legacy_wait();
        }
        removed
    }

    /// Notifies that a message has arrived on a retry topic.
    ///
    /// # Parameters
    ///
    /// * `topic` - The topic name
    /// * `queue_id` - The queue ID
    pub fn notify_message_arriving_with_retry_topic(&self, topic: &CheetahString, queue_id: i32) {
        self.notify_message_arriving_with_retry_topic_full(topic.clone(), queue_id, None, 0, None, None);
    }

    /// Notifies that a message has arrived on a retry topic with extended information.
    ///
    /// # Parameters
    ///
    /// * `topic` - The topic name
    /// * `queue_id` - The queue ID
    /// * `tags_code` - Optional tag code for filtering
    /// * `msg_store_time` - The timestamp when the message was stored
    /// * `filter_bit_map` - Optional filter bitmap for message matching
    /// * `properties` - Optional message properties
    pub fn notify_message_arriving_with_retry_topic_full(
        &self,
        topic: CheetahString,
        queue_id: i32,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        let notify_topic = if KeyBuilder::is_pop_retry_topic_v2(&topic) {
            KeyBuilder::parse_normal_topic_default(topic.as_str()).into()
        } else {
            topic
        };

        self.notify_message_arriving_(
            &notify_topic,
            queue_id,
            tags_code,
            msg_store_time,
            filter_bit_map,
            properties,
        );
    }

    /// Notifies that a message has arrived on a topic queue.
    ///
    /// This method looks up all consumer groups subscribed to the given topic
    /// and notifies them about the new message.
    ///
    /// # Parameters
    ///
    /// * `topic` - The topic name
    /// * `queue_id` - The queue ID
    /// * `tags_code` - Optional tag code for filtering
    /// * `msg_store_time` - The timestamp when the message was stored
    /// * `filter_bit_map` - Optional filter bitmap for message matching
    /// * `properties` - Optional message properties
    pub fn notify_message_arriving_(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        // Get the consumer IDs for this topic from the topic-consumer map
        // Return early if there are no consumers for this topic
        let cids = match self.topic_cid_map.get(topic) {
            Some(cids) => cids,
            None => return,
        };

        // For each consumer ID associated with this topic
        for entry in cids.iter() {
            let cid = entry.key();
            // If queue_id is valid (>= 0), also notify for queue_id = -1 (which indicates "all
            // queues") This allows consumers to be notified about both specific queues
            // and all queues
            if queue_id >= 0 {
                let filter_bit_map_ = filter_bit_map.clone();
                self.notify_message_arriving(topic, -1, cid, tags_code, msg_store_time, filter_bit_map_, properties);
            }
            let filter_bit_map_ = filter_bit_map.clone();
            // Always notify for the specific queue_id provided
            self.notify_message_arriving(
                topic,
                queue_id,
                cid,
                tags_code,
                msg_store_time,
                filter_bit_map_,
                properties,
            );
        }
    }

    pub fn polling_(
        &self,
        ctx: ConnectionHandlerContext,
        remoting_command: &mut RemotingCommand,
        request_header: PollingHeader,
    ) -> PollingResult {
        self.polling(ctx, remoting_command, request_header, None, None)
    }

    pub fn polling(
        &self,
        ctx: ConnectionHandlerContext,
        remoting_command: &mut RemotingCommand,
        request_header: PollingHeader,
        subscription_data: Option<SubscriptionData>,
        message_filter: Option<ArcMessageFilter>,
    ) -> PollingResult {
        //this method may be need to optimize
        if request_header.get_poll_time() <= 0 {
            return PollingResult::NotPolling;
        }

        let _admission = self.polling_admission.lock();
        if !self.running.load(Ordering::Acquire) {
            return PollingResult::PollingTimeout;
        }

        {
            let cids = self
                .topic_cid_map
                .entry(request_header.get_topic().clone())
                .or_default();
            cids.entry(request_header.get_consumer_group().clone())
                .or_insert(u8::MIN);
        }

        let expired = request_header.get_born_time() + request_header.get_poll_time();
        let request = Arc::new(PopRequest::new(
            remoting_command.clone(),
            ctx,
            expired as u64,
            subscription_data,
            message_filter,
        ));

        if self.total_polling_num.load(Ordering::SeqCst) >= self.context.policy.max_pop_polling_size {
            return PollingResult::PollingFull;
        }

        if request.is_timeout() {
            return PollingResult::PollingTimeout;
        }

        let key = CheetahString::from_string(KeyBuilder::build_polling_key(
            request_header.get_topic(),
            request_header.get_consumer_group(),
            request_header.get_queue_id(),
        ));
        if let Some(handoff) = self.handoff.get() {
            let target = match RequestCode::from(remoting_command.code()) {
                RequestCode::Notification => DeferredGenerationTarget::notification(
                    request_header.get_topic().clone(),
                    request_header.get_consumer_group().clone(),
                    request_header.get_queue_id(),
                ),
                _ => DeferredGenerationTarget::pop(
                    request_header.get_topic().clone(),
                    request_header.get_consumer_group().clone(),
                    request_header.get_queue_id(),
                ),
            };
            let rollback_map = Arc::clone(&self.polling_map);
            let rollback_total = Arc::clone(&self.total_polling_num);
            let rollback_key = key.clone();
            let rollback_request = Arc::downgrade(&request);
            let enrollment = handoff.arrival_adapter().install_legacy_wait(
                target.clone(),
                |lease| {
                    let queue = self.polling_map.entry(key.clone()).or_default();
                    if queue.len() > self.context.policy.pop_polling_size {
                        return Err((PollingResult::PollingFull, lease));
                    }
                    if self.total_polling_num.load(Ordering::SeqCst) >= self.context.policy.max_pop_polling_size {
                        return Err((PollingResult::PollingFull, lease));
                    }
                    request
                        .install_legacy_handoff(&target, lease)
                        .map_err(|lease| (PollingResult::PollingTimeout, lease))?;
                    let cleanup_map = Arc::clone(&self.polling_map);
                    let cleanup_total = Arc::clone(&self.total_polling_num);
                    let cleanup_key = key.clone();
                    let cleanup_request = Arc::downgrade(&request);
                    match request.get_ctx().install_legacy_session_execution(
                        move || {
                            remove_session_pop_waiter(&cleanup_map, &cleanup_total, &cleanup_key, &cleanup_request);
                        },
                        |cleanup| {
                            if !reserve_published_polling_count(&self.total_polling_num) {
                                return Err((PollingResult::PollingFull, cleanup));
                            }
                            if let Err(cleanup) = request.install_legacy_session_cleanup(cleanup) {
                                release_published_polling_count(&self.total_polling_num);
                                return Err((PollingResult::PollingTimeout, cleanup));
                            }
                            remoting_command.set_suspended_ref(true);
                            queue.insert(Arc::clone(&request));
                            Ok(())
                        },
                    ) {
                        Ok(()) => Ok(()),
                        Err(LegacySessionCleanupInstallError::Unavailable) => {
                            #[cfg(test)]
                            {
                                if !reserve_published_polling_count(&self.total_polling_num) {
                                    let lease = request
                                        .take_legacy_wait()
                                        .expect("unavailable cleanup retains the fresh wait lease");
                                    return Err((PollingResult::PollingFull, lease));
                                }
                                remoting_command.set_suspended_ref(true);
                                queue.insert(Arc::clone(&request));
                                Ok(())
                            }
                            #[cfg(not(test))]
                            {
                                let lease = request
                                    .take_legacy_wait()
                                    .expect("unavailable cleanup retains the fresh wait lease");
                                Err((PollingResult::PollingTimeout, lease))
                            }
                        }
                        Err(_) => {
                            let lease = request
                                .take_legacy_wait()
                                .expect("failed cleanup installation retains the fresh wait lease");
                            Err((PollingResult::PollingTimeout, lease))
                        }
                    }
                },
                move || {
                    remove_session_pop_waiter(&rollback_map, &rollback_total, &rollback_key, &rollback_request);
                },
            );
            match enrollment {
                Ok(()) => {}
                Err(DeferredGenerationLegacyEnrollmentError::Enrollment(result)) => return result,
                Err(_) => return PollingResult::PollingTimeout,
            }
        } else {
            let queue = self.polling_map.entry(key).or_default();
            if queue.len() > self.context.policy.pop_polling_size {
                return PollingResult::PollingFull;
            }
            if !reserve_published_polling_count(&self.total_polling_num) {
                return PollingResult::PollingFull;
            }
            remoting_command.set_suspended_ref(true);
            queue.insert(request);
        }
        PollingResult::PollingSuc
    }

    // wake up and try process request
    pub fn wake_up(&self, pop_request: Arc<PopRequest>) -> bool {
        let _admission = self.polling_admission.lock();
        if !self.running.load(Ordering::Acquire) {
            return false;
        }
        let Some(route) = self.acquire_route_for(&pop_request) else {
            return false;
        };
        let Some(claim) = self.begin_wake(pop_request, route) else {
            return false;
        };
        self.wake_up_claim(claim, None)
    }

    pub(crate) fn wake_up_with_completion(&self, pop_request: Arc<PopRequest>) -> PopWakeupCompletion {
        let (sender, receiver) = oneshot::channel();
        let completion = PopWakeupObserver::new(sender);
        let _admission = self.polling_admission.lock();
        if !self.running.load(Ordering::Acquire) {
            completion.complete(PopWakeupOutcome::ServiceNotRunning);
            return receiver;
        }
        let Some(route) = self.acquire_route_for(&pop_request) else {
            completion.complete(PopWakeupOutcome::ServiceNotRunning);
            return receiver;
        };
        let Some(claim) = self.begin_wake(pop_request, route) else {
            completion.complete(PopWakeupOutcome::AlreadyCompleted);
            return receiver;
        };
        self.wake_up_claim(claim, Some(completion));
        receiver
    }

    fn wake_up_claim_with_completion(&self, claim: PopWakeupClaim) -> PopWakeupCompletion {
        let (sender, receiver) = oneshot::channel();
        self.wake_up_claim(claim, Some(PopWakeupObserver::new(sender)));
        receiver
    }

    fn wake_up_claim(&self, claim: PopWakeupClaim, completion: Option<PopWakeupObserver>) -> bool {
        let PopWakeupClaim {
            request: pop_request,
            wake,
            execution,
        } = claim;
        pop_request.release_resource_permit();
        if !pop_request.complete() {
            if let Some(completion) = completion {
                completion.complete(PopWakeupOutcome::AlreadyCompleted);
            }
            return false;
        }
        #[cfg(test)]
        if execution.is_none() && !pop_request.get_channel().connection_ref().is_healthy() {
            if let Some(completion) = completion {
                completion.complete(PopWakeupOutcome::InactiveChannel);
            }
            return false;
        }
        match self.processor.upgrade() {
            None => {
                self.shutdown_wake_failures.fetch_add(1, Ordering::AcqRel);
                if let Some(completion) = completion {
                    completion.complete(PopWakeupOutcome::ProcessorUnavailable);
                }
                false
            }
            Some(processor) => {
                let continuation = wake.map(LegacyWakeLease::into_continuation);
                let execution_guard = self.execution_tracker.enter();
                let task = async move {
                    let _execution_guard = execution_guard;
                    let _continuation = continuation;
                    let channel = pop_request.get_channel().clone();
                    let ctx = pop_request.get_ctx().clone();
                    let opaque = pop_request.get_remoting_command().opaque();
                    let response = processor
                        .process_request_when_wakeup(channel, ctx, pop_request.get_remoting_command().clone())
                        .await;
                    match response {
                        Ok(None) => {
                            if let Some(completion) = completion {
                                completion.complete(PopWakeupOutcome::ProcessingCompleted);
                            }
                        }
                        Ok(Some(mut response)) => {
                            let channel = pop_request.get_channel();
                            response.set_opaque_mut(opaque);
                            let outcome = if channel.channel_inner().send_oneway(response, 1000).await.is_ok() {
                                PopWakeupOutcome::ProcessingCompleted
                            } else {
                                PopWakeupOutcome::ProcessingFailed
                            };
                            if let Some(completion) = completion {
                                completion.complete(outcome);
                            }
                        }
                        Err(e) => {
                            error!("ExecuteRequestWhenWakeup run {}", e);
                            if let Some(completion) = completion {
                                completion.complete(PopWakeupOutcome::ProcessingFailed);
                            }
                        }
                    }
                };

                if let Some(execution) = execution {
                    if let Err(error) = execution.try_execute(task) {
                        self.shutdown_wake_failures.fetch_add(1, Ordering::AcqRel);
                        warn!(?error, "canonical session rejected PopLongPollingService wake-up");
                        return false;
                    }
                    return true;
                }

                #[cfg(test)]
                {
                    let task_group = self.task_group.lock().as_ref().cloned();
                    let Some(task_group) = task_group else {
                        self.shutdown_wake_failures.fetch_add(1, Ordering::AcqRel);
                        warn!("PopLongPollingService test wake-up owner is not running");
                        return false;
                    };
                    if let Err(error) = task_group.spawn("broker.long-polling.pop.test-wake-up", TaskKind::Worker, task)
                    {
                        self.shutdown_wake_failures.fetch_add(1, Ordering::AcqRel);
                        warn!(?error, "failed to spawn PopLongPollingService test wake-up task");
                        return false;
                    }
                    true
                }
                #[cfg(not(test))]
                {
                    self.shutdown_wake_failures.fetch_add(1, Ordering::AcqRel);
                    warn!("PopLongPollingService wake-up has no canonical session owner");
                    false
                }
            }
        }
    }

    fn acquire_route_for(&self, request: &PopRequest) -> Option<Option<RoutePermit>> {
        let Some(handoff) = self.handoff.get() else {
            return Some(None);
        };
        let target = request.legacy_handoff_target()?;
        let route = handoff.acquire_route(target).ok()?;
        if route.generation() != DeferredGeneration::Legacy {
            return None;
        }
        Some(Some(route))
    }

    fn begin_wake(&self, request: Arc<PopRequest>, route: Option<RoutePermit>) -> Option<PopWakeupClaim> {
        let Some(route) = route else {
            return Some(PopWakeupClaim {
                request,
                wake: None,
                execution: None,
            });
        };
        let wait = request.take_legacy_wait()?;
        match wait.begin_wake(route) {
            Ok(wake) => {
                let execution = request.take_legacy_session_execution();
                #[cfg(not(test))]
                execution.as_ref()?;
                Some(PopWakeupClaim {
                    request,
                    wake: Some(wake),
                    execution,
                })
            }
            Err(error) => {
                let (wait, _route) = error.into_wait_and_route();
                if let Err(wait) = request.restore_legacy_wait(wait) {
                    drop(wait);
                }
                None
            }
        }
    }

    fn poll_remoting_commands(&self, key: &CheetahString) -> Option<(Arc<PopRequest>, Option<RoutePermit>)> {
        self.claim_remoting_command(key, None)
    }

    fn claim_remoting_command(
        &self,
        key: &CheetahString,
        expected: Option<&Arc<PopRequest>>,
    ) -> Option<(Arc<PopRequest>, Option<RoutePermit>)> {
        loop {
            let candidate = match expected {
                Some(request) => Arc::clone(request),
                None => {
                    let remoting_commands = self.polling_map.get(key)?;
                    if self.notify_last {
                        remoting_commands.back().map(|entry| entry.value().clone())
                    } else {
                        remoting_commands.front().map(|entry| entry.value().clone())
                    }?
                }
            };
            let (pop_request, route) = if let Some(handoff) = self.handoff.get() {
                let target = candidate.legacy_handoff_target()?;
                let mut claimed = handoff
                    .arrival_adapter()
                    .claim_legacy_table(
                        target,
                        |claimed| {
                            let Some(remoting_commands) = self.polling_map.get(key) else {
                                return;
                            };
                            if remoting_commands.remove(&candidate).is_some() {
                                claimed.push(Arc::clone(&candidate));
                            }
                        },
                        |requests| {
                            let queue = self.polling_map.entry(key.clone()).or_default();
                            for request in requests {
                                queue.insert(request);
                            }
                        },
                    )
                    .ok()?;
                let (request, route) = claimed.pop()?;
                (request, Some(route))
            } else {
                let remoting_commands = self.polling_map.get(key)?;
                let request = if expected.is_some() {
                    remoting_commands
                        .remove(&candidate)
                        .map(|entry| entry.value().clone())?
                } else {
                    if self.notify_last {
                        remoting_commands.pop_back().map(|entry| entry.value().clone())
                    } else {
                        remoting_commands.pop_front().map(|entry| entry.value().clone())
                    }?
                };
                (request, None)
            };

            release_published_polling_count(&self.total_polling_num);
            if !pop_request.get_channel().connection_ref().is_healthy() {
                pop_request.release_resource_permit();
                pop_request.release_legacy_wait();
                if expected.is_some() {
                    return None;
                }
                continue;
            }
            return Some((pop_request, route));
        }
    }

    fn wake_up_expired_requests(&self, key: &CheetahString) {
        let _admission = self.polling_admission.lock();
        if !self.running.load(Ordering::Acquire) {
            return;
        }
        loop {
            let Some(candidate) = self
                .polling_map
                .get(key)
                .and_then(|queue| queue.front().map(|entry| entry.value().clone()))
            else {
                break;
            };
            if !candidate.is_timeout() {
                break;
            }
            let Some((first, route)) = self.claim_remoting_command(key, Some(&candidate)) else {
                continue;
            };
            if let Some(claim) = self.begin_wake(Arc::clone(&first), route) {
                self.wake_up_claim(claim, None);
            } else {
                self.requeue_claimed_request(key.clone(), first);
            }
        }
    }

    fn drain_polling_queue(&self, queue: &SkipSet<Arc<PopRequest>>) {
        while let Some(first) = queue.pop_front() {
            release_published_polling_count(&self.total_polling_num);
            let request = first.value().clone();
            request.release_resource_permit();
            request.release_legacy_session_cleanup();
            request.release_legacy_wait();
        }
    }

    fn discard_polling_queue(&self, queue: &SkipSet<Arc<PopRequest>>) {
        while let Some(first) = queue.pop_front() {
            release_published_polling_count(&self.total_polling_num);
            first.value().release_resource_permit();
            first.value().release_legacy_session_cleanup();
            first.value().release_legacy_wait();
        }
    }

    /// Gets the number of polling requests for a given key
    ///
    /// # Arguments
    /// * `key` - The polling key (topic@consumerGroup@queueId)
    ///
    /// # Returns
    /// The number of polling requests, or 0 if no polling requests exist for the key
    #[inline]
    pub fn get_polling_num(&self, key: &str) -> i32 {
        self.polling_map.get(key).map(|queue| queue.len() as i32).unwrap_or(0)
    }

    pub(crate) fn legacy_target_occupied(&self, target: &DeferredGenerationTarget) -> bool {
        let (topic, consumer_group, queue_id) = match target {
            DeferredGenerationTarget::Pop {
                topic,
                consumer_group,
                queue_id,
            }
            | DeferredGenerationTarget::Notification {
                topic,
                consumer_group,
                queue_id,
            } => (topic, consumer_group, *queue_id),
            _ => return false,
        };
        let key = KeyBuilder::build_polling_key(topic, consumer_group, queue_id);
        let _admission = self.polling_admission.lock();
        self.polling_map
            .get(key.as_str())
            .is_some_and(|queue| !queue.is_empty())
    }

    #[cfg(test)]
    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn task_group_for_test(&self) -> Option<TaskGroup> {
        self.task_group.lock().as_ref().cloned()
    }
}

impl<RP> PollingCountProvider for PopLongPollingService<RP>
where
    RP: PopLongPollingRequestProcessor + Sync + 'static,
{
    fn polling_count(&self, key: &str) -> i32 {
        self.get_polling_num(key)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_model::common::key_builder::KeyBuilder;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_runtime::common::time_utils::current_millis;
    use rocketmq_runtime::ShutdownDeadline;
    use rocketmq_store::MessageFilter;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_transport::api::v1::Channel;
    use rocketmq_transport::api::v1::ConnectionHandlerContextWrapper;
    use rocketmq_transport::test_support::Connection;
    use rocketmq_transport::test_support::LegacySessionExecutionHarness;
    use rocketmq_transport::test_support::TestChannelBuilder;
    use tokio::sync::Notify;

    use super::PopLongPollingRequestProcessor;
    use super::PopLongPollingService;
    use super::PopLongPollingServiceContext;
    use super::PopWakeupOutcome;
    use crate::broker_runtime::BrokerRuntime;
    use crate::config::broker_config::BrokerConfig;
    use crate::deferred_generation_handoff::DeferredGenerationHandoff;
    use crate::deferred_generation_handoff::DeferredGenerationTarget;
    use crate::deferred_generation_handoff::DeferredGenerationV2Publisher;
    use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingPolicy;
    use crate::long_polling::polling_header::PollingHeader;
    use crate::long_polling::polling_result::PollingResult;
    use crate::long_polling::pop_request::PopRequest;

    struct RejectAllFilter;

    struct MatchTagFilter(i64);

    impl MessageFilter for RejectAllFilter {
        fn is_matched_by_consume_queue(
            &self,
            _tags_code: Option<i64>,
            _cq_ext_unit: Option<&rocketmq_store::CqExtUnit>,
        ) -> bool {
            false
        }

        fn is_matched_by_commit_log(
            &self,
            _msg_buffer: Option<&[u8]>,
            _properties: Option<&std::collections::HashMap<CheetahString, CheetahString>>,
        ) -> bool {
            false
        }
    }

    impl MessageFilter for MatchTagFilter {
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
            _properties: Option<&std::collections::HashMap<CheetahString, CheetahString>>,
        ) -> bool {
            true
        }
    }

    struct FailingProcessor {
        calls: AtomicUsize,
    }

    struct ControlledProcessor {
        calls: AtomicUsize,
        started: Notify,
        release: Notify,
    }

    struct ControlledResponseProcessor {
        started: Notify,
        release: Notify,
    }

    struct ImmediateResponseProcessor {
        calls: AtomicUsize,
    }

    impl PopLongPollingRequestProcessor for FailingProcessor {
        async fn process_request_when_wakeup(
            &self,
            _channel: Channel,
            _ctx: rocketmq_transport::api::v1::ConnectionHandlerContext,
            _request: RemotingCommand,
        ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Err(rocketmq_error::RocketMQError::illegal_argument(
                "deterministic POP processing failure",
            ))
        }
    }

    impl PopLongPollingRequestProcessor for ControlledProcessor {
        async fn process_request_when_wakeup(
            &self,
            _channel: Channel,
            _ctx: rocketmq_transport::api::v1::ConnectionHandlerContext,
            _request: RemotingCommand,
        ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.started.notify_one();
            self.release.notified().await;
            Ok(None)
        }
    }

    impl PopLongPollingRequestProcessor for ControlledResponseProcessor {
        async fn process_request_when_wakeup(
            &self,
            _channel: Channel,
            _ctx: rocketmq_transport::api::v1::ConnectionHandlerContext,
            _request: RemotingCommand,
        ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(Some(RemotingCommand::create_remoting_command(0).mark_response_type()))
        }
    }

    impl PopLongPollingRequestProcessor for ImmediateResponseProcessor {
        async fn process_request_when_wakeup(
            &self,
            _channel: Channel,
            _ctx: rocketmq_transport::api::v1::ConnectionHandlerContext,
            _request: RemotingCommand,
        ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Ok(Some(RemotingCommand::create_remoting_command(0).mark_response_type()))
        }
    }

    fn test_service<RP>(processor: &Arc<RP>) -> Arc<PopLongPollingService<RP>>
    where
        RP: PopLongPollingRequestProcessor + Sync + 'static,
    {
        let mut runtime = BrokerRuntime::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        );
        let state = runtime.runtime_state_mut();
        let context = PopLongPollingServiceContext::new(
            PopLongPollingPolicy::from_config(&state.broker_config()),
            state.topic_config_manager_handle(),
            state.subscription_group_manager().config_lookup(),
            state.broker_service_context(),
        );
        Arc::new(PopLongPollingService::new(context, false, Arc::downgrade(processor)))
    }

    async fn test_pop_request() -> Arc<PopRequest> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener addr");
        let stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        stream.set_nonblocking(true).expect("set nonblocking");
        let stream = tokio::net::TcpStream::from_std(stream).expect("convert tcp stream");
        let connection = Connection::new(stream);
        let channel = rocketmq_transport::test_support::TestChannelBuilder::new(
            connection,
            crate::test_task_group("pop-lag-refresh-channel"),
        )
        .addresses(local_addr, local_addr)
        .build()
        .expect("build test channel");
        let ctx = Arc::new(ConnectionHandlerContextWrapper::new(channel));
        Arc::new(PopRequest::new(
            RemotingCommand::create_remoting_command(0),
            ctx,
            current_millis() + 60_000,
            None,
            None,
        ))
    }

    async fn test_context() -> rocketmq_transport::api::v1::ConnectionHandlerContext {
        test_pop_request().await.get_ctx().clone()
    }

    async fn session_execution_test_context(
        owner_id: u64,
        request_code: RequestCode,
        writer_barrier: Option<(Arc<Notify>, Arc<Notify>)>,
    ) -> (
        LegacySessionExecutionHarness,
        rocketmq_runtime::TaskGroup,
        rocketmq_transport::api::v1::ConnectionHandlerContext,
        std::net::TcpStream,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind legacy session listener");
        let local_addr = listener.local_addr().expect("legacy session listener address");
        let stream = std::net::TcpStream::connect(local_addr).expect("connect legacy session peer");
        let (peer, _) = listener.accept().expect("accept legacy session peer");
        peer.set_nonblocking(true).expect("set legacy session peer nonblocking");
        stream
            .set_nonblocking(true)
            .expect("set legacy session stream nonblocking");
        let stream = tokio::net::TcpStream::from_std(stream).expect("convert legacy session stream");
        let connection = Connection::new(stream);
        let mut builder = TestChannelBuilder::new(
            connection,
            crate::test_task_group(format!("legacy-session-channel-{owner_id}")),
        )
        .addresses(local_addr, local_addr);
        if let Some((entered, release)) = writer_barrier {
            builder = builder.write_preflight_barrier(entered, release);
        }
        let channel = builder.build().expect("build legacy session channel");
        let session_group = crate::test_task_group(format!("legacy-session-execution-{owner_id}"));
        let harness = LegacySessionExecutionHarness::new(owner_id, &session_group);
        let context = harness.context(channel, 4 * 1024, request_code.to_i32());
        (harness, session_group, context, peer)
    }

    fn session_polling_header(topic: &CheetahString, group: &CheetahString) -> PollingHeader {
        let header = rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader {
            consumer_group: group.clone(),
            topic: topic.clone(),
            queue_id: 0,
            max_msg_nums: 1,
            invisible_time: 30_000,
            poll_time: 60_000,
            born_time: current_millis(),
            init_mode: 0,
            exp_type: None,
            exp: None,
            order: Some(false),
            attempt_id: None,
            topic_request_header: None,
        };
        PollingHeader::new_from_pop_message_request_header(&header)
    }

    fn register_session_waiter<RP>(
        service: &PopLongPollingService<RP>,
        context: rocketmq_transport::api::v1::ConnectionHandlerContext,
        request_code: RequestCode,
        topic: &CheetahString,
        group: &CheetahString,
    ) -> (CheetahString, Arc<PopRequest>)
    where
        RP: PopLongPollingRequestProcessor + Sync + 'static,
    {
        let mut command = RemotingCommand::create_remoting_command(request_code);
        assert_eq!(
            service.polling(context, &mut command, session_polling_header(topic, group), None, None,),
            PollingResult::PollingSuc
        );
        let key = CheetahString::from_string(KeyBuilder::build_polling_key(topic, group, 0));
        let request = service
            .polling_map
            .get(&key)
            .and_then(|queue| queue.front().map(|entry| Arc::clone(entry.value())))
            .expect("registered legacy session waiter");
        (key, request)
    }

    fn assert_peer_received_no_bytes(peer: &mut std::net::TcpStream) {
        let mut byte = [0_u8; 1];
        let error = peer
            .read(&mut byte)
            .expect_err("cancelled legacy session wrote no response bytes");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    }

    async fn assert_terminal_pop_requeue_has_one_release_owner(request_code: RequestCode, owner_id: u64) {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install terminal requeue coordinator");
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_string(format!("terminal-requeue-topic-{owner_id}"));
        let group = CheetahString::from_string(format!("terminal-requeue-group-{owner_id}"));

        let (missed_session, missed_group, missed_context, _missed_peer) =
            session_execution_test_context(owner_id, request_code, None).await;
        let (key, registered) = register_session_waiter(&service, missed_context, request_code, &topic, &group);
        let (request, route) = service
            .claim_remoting_command(&key, Some(&registered))
            .expect("claim waiter before terminal cleanup misses the table");
        missed_session.close();
        service.requeue_claimed_request(key.clone(), request);
        drop(route);
        assert_eq!(service.get_polling_num(&key), 0);
        assert!(handoff.zero_report().is_zero());
        let report = missed_group
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(report.is_healthy(), "{}", report.to_json());

        let (winning_session, winning_group, winning_context, _winning_peer) =
            session_execution_test_context(owner_id + 1, request_code, None).await;
        let (key, registered) = register_session_waiter(&service, winning_context, request_code, &topic, &group);
        let (request, route) = service
            .claim_remoting_command(&key, Some(&registered))
            .expect("claim waiter before terminal publication races the reread");
        super::restore_published_polling_count(&service.total_polling_num);
        service
            .polling_map
            .entry(key.clone())
            .or_default()
            .insert(Arc::clone(&request));
        winning_session.close();
        assert!(
            !service.retract_terminal_requeue(&key, &request),
            "session cleanup already owns the exact published waiter"
        );
        drop(route);
        assert_eq!(service.get_polling_num(&key), 0);
        assert!(handoff.zero_report().is_zero());
        let report = winning_group
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(report.is_healthy(), "{}", report.to_json());

        service.shutdown().await;
    }

    #[tokio::test]
    async fn pop_terminal_requeue_races_have_one_release_owner() {
        assert_terminal_pop_requeue_has_one_release_owner(RequestCode::PopMessage, 8_311).await;
    }

    #[tokio::test]
    async fn notification_terminal_requeue_races_have_one_release_owner() {
        assert_terminal_pop_requeue_has_one_release_owner(RequestCode::Notification, 8_313).await;
    }

    async fn assert_session_close_after_claim_is_fail_closed(request_code: RequestCode, owner_id: u64) {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install session claim coordinator");
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_string(format!("session-claim-topic-{owner_id}"));
        let group = CheetahString::from_string(format!("session-claim-group-{owner_id}"));
        let (session, session_group, context, mut peer) =
            session_execution_test_context(owner_id, request_code, None).await;
        let (key, registered) = register_session_waiter(&service, context, request_code, &topic, &group);
        let (request, route) = service
            .claim_remoting_command(&key, Some(&registered))
            .expect("claim exact session-owned waiter");
        assert_eq!(service.get_polling_num(&key), 0);

        session.close();
        let claim = service
            .begin_wake(request, route)
            .expect("closed session claim retains its affine execution enrollment");
        assert!(!service.wake_up_claim(claim, None));
        assert_eq!(processor.calls.load(Ordering::Acquire), 0);
        assert_peer_received_no_bytes(&mut peer);
        assert!(handoff.zero_report().is_zero());

        let session_report = session_group
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(session_report.is_healthy(), "{}", session_report.to_json());
        service.shutdown().await;
    }

    async fn assert_session_close_before_first_handler_poll_is_fail_closed(request_code: RequestCode, owner_id: u64) {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install first-poll coordinator");
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_string(format!("session-first-poll-topic-{owner_id}"));
        let group = CheetahString::from_string(format!("session-first-poll-group-{owner_id}"));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (session, session_group, context, mut peer) =
            session_execution_test_context(owner_id, request_code, None).await;
        session.set_first_poll_gate(Arc::clone(&entered), Arc::clone(&release));
        register_session_waiter(&service, context, request_code, &topic, &group);

        assert!(service.notify_message_arriving(&topic, 0, &group, None, 0, None, None));
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("session executor accepted the legacy wake before handler poll");
        session.close();
        release.notify_one();
        let session_report = session_group
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(session_report.is_healthy(), "{}", session_report.to_json());
        assert_eq!(processor.calls.load(Ordering::Acquire), 0);
        assert_peer_received_no_bytes(&mut peer);
        assert!(handoff.zero_report().is_zero());
        service.shutdown().await;
    }

    async fn assert_session_close_at_writer_preflight_writes_nothing(request_code: RequestCode, owner_id: u64) {
        let processor = Arc::new(ImmediateResponseProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install writer coordinator");
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_string(format!("session-writer-topic-{owner_id}"));
        let group = CheetahString::from_string(format!("session-writer-group-{owner_id}"));
        let writer_entered = Arc::new(Notify::new());
        let writer_release = Arc::new(Notify::new());
        let (session, session_group, context, mut peer) = session_execution_test_context(
            owner_id,
            request_code,
            Some((Arc::clone(&writer_entered), Arc::clone(&writer_release))),
        )
        .await;
        register_session_waiter(&service, context, request_code, &topic, &group);

        assert!(service.notify_message_arriving(&topic, 0, &group, None, 0, None, None));
        tokio::time::timeout(Duration::from_secs(1), writer_entered.notified())
            .await
            .expect("legacy response reached the canonical writer preflight");
        assert_eq!(processor.calls.load(Ordering::Acquire), 1);
        session.close();
        let session_report = session_group
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(session_report.is_healthy(), "{}", session_report.to_json());
        writer_release.notify_one();
        assert_peer_received_no_bytes(&mut peer);
        assert!(handoff.zero_report().is_zero());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn pop_session_close_after_claim_runs_no_handler_or_writer() {
        assert_session_close_after_claim_is_fail_closed(RequestCode::PopMessage, 8_301).await;
    }

    #[tokio::test]
    async fn notification_session_close_after_claim_runs_no_handler_or_writer() {
        assert_session_close_after_claim_is_fail_closed(RequestCode::Notification, 8_302).await;
    }

    #[tokio::test]
    async fn pop_session_close_before_first_handler_poll_runs_no_handler_or_writer() {
        assert_session_close_before_first_handler_poll_is_fail_closed(RequestCode::PopMessage, 8_303).await;
    }

    #[tokio::test]
    async fn notification_session_close_before_first_handler_poll_runs_no_handler_or_writer() {
        assert_session_close_before_first_handler_poll_is_fail_closed(RequestCode::Notification, 8_304).await;
    }

    #[tokio::test]
    async fn pop_session_close_at_writer_preflight_writes_no_bytes() {
        assert_session_close_at_writer_preflight_writes_nothing(RequestCode::PopMessage, 8_305).await;
    }

    #[tokio::test]
    async fn notification_session_close_at_writer_preflight_writes_no_bytes() {
        assert_session_close_at_writer_preflight_writes_nothing(RequestCode::Notification, 8_306).await;
    }

    async fn registration_and_transition_share_gate_before_pop_table(request_code: RequestCode) {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_static_str("gate-order-topic");
        let group = CheetahString::from_static_str("gate-order-group");
        let header = rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader {
            consumer_group: group.clone(),
            topic: topic.clone(),
            queue_id: 0,
            max_msg_nums: 1,
            invisible_time: 30_000,
            poll_time: 60_000,
            born_time: current_millis(),
            init_mode: 0,
            exp_type: None,
            exp: None,
            order: Some(false),
            attempt_id: None,
            topic_request_header: None,
        };
        let execution_group = crate::test_task_group("gate-order-pop-session");
        let session =
            LegacySessionExecutionHarness::new(8_150 + u64::from(request_code as i32 as u32), &execution_group);
        let base_context = test_context().await;
        let context = session.context(base_context.channel().clone(), 0, request_code as i32);
        let entered = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let checkpoint_entered = Arc::clone(&entered);
        let checkpoint_release = Arc::clone(&release);
        session.set_insert_checkpoint(move |state_locked| {
            assert!(state_locked);
            checkpoint_entered.wait();
            checkpoint_release.wait();
        });

        let registering_service = Arc::clone(&service);
        let registering = std::thread::spawn(move || {
            let mut command = RemotingCommand::create_remoting_command(request_code);
            registering_service.polling(
                context,
                &mut command,
                PollingHeader::new_from_pop_message_request_header(&header),
                None,
                None,
            )
        });
        entered.wait();

        let cutover_handoff = Arc::clone(&handoff);
        let (cutover_started_tx, cutover_started_rx) = std::sync::mpsc::channel();
        let cutover = std::thread::spawn(move || {
            cutover_started_tx.send(()).expect("signal cutover attempt");
            let mut transaction = cutover_handoff.cutover_transaction().expect("cutover transaction");
            transaction.seal_legacy_acceptance().expect("seal legacy acceptance");
            transaction
                .publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(|| Ok::<_, ()>(())))
                .expect("publish aggregate");
            transaction.publish_default_new().expect("publish New default");
        });
        cutover_started_rx.recv().expect("cutover started");
        assert!(!cutover.is_finished(), "cutover must wait behind registration's gate");
        release.wait();
        assert_eq!(
            registering.join().expect("registration thread"),
            PollingResult::PollingSuc
        );
        cutover.join().expect("cutover thread");

        let target = match request_code {
            RequestCode::Notification => DeferredGenerationTarget::notification(topic.clone(), group.clone(), 0),
            _ => DeferredGenerationTarget::pop(topic.clone(), group.clone(), 0),
        };
        let key = CheetahString::from_string(KeyBuilder::build_polling_key(&topic, &group, 0));
        assert!(service.legacy_target_occupied(&target));
        assert!(!service.legacy_target_occupied(&DeferredGenerationTarget::pop(
            CheetahString::from_static_str("unrelated-pop-topic"),
            group.clone(),
            0,
        )));
        assert!(matches!(
            handoff.try_transition_target_to_new(target.clone(), |_| {
                service.polling_map.get(&key).is_some_and(|queue| !queue.is_empty())
            }),
            Err(crate::deferred_generation_handoff::DeferredGenerationTargetTransitionError::Draining(_))
                | Err(crate::deferred_generation_handoff::DeferredGenerationTargetTransitionError::LegacyTableOccupied)
        ));
        session.close();
        assert!(!service.legacy_target_occupied(&target));
        let replay = handoff
            .try_transition_target_to_new(target, |_| {
                service.polling_map.get(&key).is_some_and(|queue| !queue.is_empty())
            })
            .expect("closed session leaves target drained");
        replay.complete_after_replay_accepted();
        assert!(handoff.zero_report().is_zero());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn pop_registration_and_transition_use_gate_then_table_order() {
        registration_and_transition_share_gate_before_pop_table(RequestCode::PopMessage).await;
    }

    #[tokio::test]
    async fn notification_registration_and_transition_use_gate_then_table_order() {
        registration_and_transition_share_gate_before_pop_table(RequestCode::Notification).await;
    }

    #[tokio::test]
    async fn session_close_removes_only_its_exact_pop_waiter() {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_static_str("cleanup-pop-topic");
        let group = CheetahString::from_static_str("cleanup-pop-group");
        let header = rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader {
            consumer_group: group.clone(),
            topic: topic.clone(),
            queue_id: 0,
            max_msg_nums: 1,
            invisible_time: 30_000,
            poll_time: 60_000,
            born_time: current_millis(),
            init_mode: 0,
            exp_type: None,
            exp: None,
            order: Some(false),
            attempt_id: None,
            topic_request_header: None,
        };
        let (first_session, _first_group, first_context, _first_peer) =
            session_execution_test_context(8_101, RequestCode::PopMessage, None).await;
        let (second_session, _second_group, second_context, _second_peer) =
            session_execution_test_context(8_102, RequestCode::PopMessage, None).await;
        for context in [first_context, second_context] {
            let mut command = RemotingCommand::create_remoting_command(RequestCode::PopMessage);
            assert_eq!(
                service.polling(
                    context,
                    &mut command,
                    PollingHeader::new_from_pop_message_request_header(&header),
                    None,
                    None,
                ),
                crate::long_polling::polling_result::PollingResult::PollingSuc
            );
        }
        let key = KeyBuilder::build_polling_key(&topic, &group, 0);
        assert_eq!(service.get_polling_num(&key), 2);
        assert_eq!(handoff.snapshot().occupancy, 2);

        first_session.close();
        assert_eq!(service.get_polling_num(&key), 1);
        assert_eq!(handoff.snapshot().occupancy, 1);

        second_session.close();
        assert_eq!(service.get_polling_num(&key), 0);
        assert!(handoff.zero_report().is_zero());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn session_close_cannot_observe_a_half_published_pop_waiter() {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_static_str("atomic-cleanup-pop-topic");
        let group = CheetahString::from_static_str("atomic-cleanup-pop-group");
        let header = rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader {
            consumer_group: group.clone(),
            topic: topic.clone(),
            queue_id: 0,
            max_msg_nums: 1,
            invisible_time: 30_000,
            poll_time: 60_000,
            born_time: current_millis(),
            init_mode: 0,
            exp_type: None,
            exp: None,
            order: Some(false),
            attempt_id: None,
            topic_request_header: None,
        };
        let (session, _session_group, context, _peer) =
            session_execution_test_context(8_103, RequestCode::PopMessage, None).await;
        let session = Arc::new(session);
        let checkpoint = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let insert_checkpoint = Arc::clone(&checkpoint);
        let insert_release = Arc::clone(&release);
        session.set_insert_checkpoint(move |state_locked| {
            assert!(state_locked, "cleanup enrollment must hold its publication gate");
            insert_checkpoint.wait();
            insert_release.wait();
        });

        let polling_service = Arc::clone(&service);
        let polling = std::thread::spawn(move || {
            let mut command = RemotingCommand::create_remoting_command(RequestCode::PopMessage);
            let result = polling_service.polling(
                context,
                &mut command,
                PollingHeader::new_from_pop_message_request_header(&header),
                None,
                None,
            );
            (result, command.suspended())
        });
        checkpoint.wait();

        let (close_started_tx, close_started_rx) = std::sync::mpsc::channel();
        let (close_done_tx, close_done_rx) = std::sync::mpsc::channel();
        let closing_session = Arc::clone(&session);
        let closing = std::thread::spawn(move || {
            close_started_tx.send(()).expect("signal POP close attempt");
            closing_session.close();
            close_done_tx.send(()).expect("signal POP close completion");
        });
        close_started_rx.recv().expect("POP close thread started");
        assert!(
            close_done_rx.try_recv().is_err(),
            "close cannot pass an in-progress POP publication"
        );
        release.wait();

        let (result, suspended) = polling.join().expect("POP polling thread");
        closing.join().expect("POP close thread");
        assert_eq!(result, crate::long_polling::polling_result::PollingResult::PollingSuc);
        assert!(suspended, "accepted POP waiter must publish suspended=true");
        let key = KeyBuilder::build_polling_key(&topic, &group, 0);
        assert_eq!(service.get_polling_num(&key), 0);
        assert_eq!(service.total_polling_num.load(Ordering::Acquire), 0);
        let resources = service.legacy_resource_snapshot();
        assert_eq!(resources.table_entries, 0);
        assert_eq!(resources.tracked_waiters, 0);
        assert!(handoff.zero_report().is_zero());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn observed_wakeup_reports_processing_failure_exactly_once() {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        PopLongPollingService::start(&service).await;

        let request = test_pop_request().await;
        let completion = service.wake_up_with_completion(request);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), completion)
                .await
                .expect("completion must be bounded")
                .expect("completion sender must stay owned"),
            PopWakeupOutcome::ProcessingFailed
        );
        assert_eq!(processor.calls.load(Ordering::Acquire), 1);

        service.shutdown().await;
    }

    #[tokio::test]
    async fn observed_wakeup_reports_failure_when_canonical_write_fails() {
        let processor = Arc::new(ControlledResponseProcessor {
            started: Notify::new(),
            release: Notify::new(),
        });
        let service = test_service(&processor);
        PopLongPollingService::start(&service).await;

        let request = test_pop_request().await;
        let connection = request.get_channel().connection_ref().clone();
        let completion = service.wake_up_with_completion(request);
        tokio::time::timeout(Duration::from_secs(1), processor.started.notified())
            .await
            .expect("handler must start");
        connection.close();
        processor.release.notify_one();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), completion)
                .await
                .expect("completion must be bounded")
                .expect("completion sender must stay owned"),
            PopWakeupOutcome::ProcessingFailed
        );

        service.shutdown().await;
    }

    #[tokio::test]
    async fn lag_refresh_force_wakes_a_filtered_suspended_request() {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_static_str("lag-topic");
        let group = CheetahString::from_static_str("lag-group");
        let header = rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader {
            consumer_group: group.clone(),
            topic: topic.clone(),
            queue_id: -1,
            max_msg_nums: 1,
            invisible_time: 30_000,
            poll_time: 60_000,
            born_time: current_millis(),
            init_mode: 0,
            exp_type: None,
            exp: None,
            order: Some(false),
            attempt_id: None,
            topic_request_header: None,
        };
        let mut command = RemotingCommand::create_remoting_command(0);
        assert_eq!(
            service.polling(
                test_context().await,
                &mut command,
                PollingHeader::new_from_pop_message_request_header(&header),
                Some(rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData::default()),
                Some(Arc::new(RejectAllFilter)),
            ),
            crate::long_polling::polling_result::PollingResult::PollingSuc
        );

        assert!(!service.notify_message_arriving(&topic, -1, &group, None, 0, None, None));
        let completion = service
            .notify_message_arriving_before_lag(&topic, &group)
            .expect("forced lag refresh should claim the suspended request");
        assert_eq!(
            completion.await.expect("completion sender must stay owned"),
            PopWakeupOutcome::ProcessingFailed
        );
        assert_eq!(processor.calls.load(Ordering::Acquire), 1);

        service.shutdown().await;
    }

    #[tokio::test]
    async fn installed_handoff_keeps_filter_miss_registered_until_terminal() {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator before acceptance");
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_static_str("handoff-filter-topic");
        let group = CheetahString::from_static_str("handoff-filter-group");
        let header = rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader {
            consumer_group: group.clone(),
            topic: topic.clone(),
            queue_id: 0,
            max_msg_nums: 1,
            invisible_time: 30_000,
            poll_time: 60_000,
            born_time: current_millis(),
            init_mode: 0,
            exp_type: None,
            exp: None,
            order: Some(false),
            attempt_id: None,
            topic_request_header: None,
        };
        let mut command = RemotingCommand::create_remoting_command(RequestCode::PopMessage);
        assert_eq!(
            service.polling(
                test_context().await,
                &mut command,
                PollingHeader::new_from_pop_message_request_header(&header),
                Some(rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData::default()),
                Some(Arc::new(RejectAllFilter)),
            ),
            crate::long_polling::polling_result::PollingResult::PollingSuc
        );
        assert_eq!(handoff.snapshot().occupancy, 1);

        assert!(!service.notify_message_arriving(&topic, 0, &group, Some(7), 0, None, None));
        let snapshot = handoff.snapshot();
        assert_eq!(snapshot.occupancy, 1);
        assert_eq!(snapshot.candidates, 0);
        assert_eq!(snapshot.active_wakes, 0);

        service.shutdown().await;
        assert!(handoff.zero_report().is_zero());
    }

    #[tokio::test]
    async fn wake_continuation_is_owned_until_handler_terminal() {
        let processor = Arc::new(ControlledProcessor {
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let service = test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        PopLongPollingService::start(&service).await;
        let target = DeferredGenerationTarget::pop(
            CheetahString::from_static_str("continuation-topic"),
            CheetahString::from_static_str("continuation-group"),
            0,
        );
        let request = test_pop_request().await;
        handoff
            .arrival_adapter()
            .install_legacy_wait(
                target.clone(),
                |lease| {
                    request
                        .install_legacy_handoff(&target, lease)
                        .map_err(|lease| ((), lease))
                },
                || request.release_legacy_wait(),
            )
            .expect("register exact request node");

        assert!(service.wake_up(Arc::clone(&request)));
        tokio::time::timeout(Duration::from_secs(1), processor.started.notified())
            .await
            .expect("handler must start");
        let active = handoff.snapshot();
        assert_eq!(active.occupancy, 0);
        assert_eq!(active.active_wakes, 1);
        assert_eq!(active.continuations, 1);

        let deadline = ShutdownDeadline::after(Duration::from_secs(1));
        let producer_report = service
            .stop_producer_until(deadline)
            .await
            .expect("started service owns its producer group");
        assert!(producer_report.is_healthy(), "{}", producer_report.to_json());
        let mut execution_drain = Box::pin(service.drain_executions_until(deadline));
        tokio::select! {
            biased;
            _ = &mut execution_drain => panic!("accepted POP handler completed before its barrier was released"),
            _ = tokio::task::yield_now() => {}
        }
        assert_eq!(service.legacy_resource_snapshot().active_executions, 1);

        processor.release.notify_one();
        let execution_report = execution_drain.await.expect("started service owns its execution group");
        assert!(execution_report.is_healthy(), "{}", execution_report.to_json());
        assert!(handoff.zero_report().is_zero());
        assert!(service.finalize_shutdown().await.terminal.is_zero());
    }

    #[tokio::test]
    async fn spawn_failure_releases_legacy_wake_fail_closed() {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        let stopped_group = crate::test_task_group("pop-legacy-spawn-failure");
        stopped_group.cancel();
        *service.task_group.lock() = Some(stopped_group);
        let target = DeferredGenerationTarget::pop(
            CheetahString::from_static_str("spawn-failure-topic"),
            CheetahString::from_static_str("spawn-failure-group"),
            0,
        );
        let request = test_pop_request().await;
        handoff
            .arrival_adapter()
            .install_legacy_wait(
                target.clone(),
                |lease| {
                    request
                        .install_legacy_handoff(&target, lease)
                        .map_err(|lease| ((), lease))
                },
                || request.release_legacy_wait(),
            )
            .expect("register exact request node");

        assert!(!service.wake_up(request));
        assert_eq!(processor.calls.load(Ordering::Acquire), 0);
        assert!(handoff.zero_report().is_zero());
    }

    #[tokio::test]
    async fn notification_v1_wake_compatibility_filter_requeues_a_miss_then_wakes_on_a_match() {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_static_str("notification-filter-topic");
        let group = CheetahString::from_static_str("notification-filter-group");
        let header = rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader {
            consumer_group: group.clone(),
            topic: topic.clone(),
            queue_id: 0,
            born_time: i64::try_from(current_millis()).expect("test clock fits i64"),
            order: false,
            attempt_id: None,
            exp_type: Some(CheetahString::from_static_str("TAG")),
            exp: Some(CheetahString::from_static_str("blue")),
            is_lite_consumer: false,
            client_id: None,
            poll_time: 60_000,
            topic_request_header: None,
        };
        let mut command = RemotingCommand::create_remoting_command(0);
        assert_eq!(
            service.polling(
                test_context().await,
                &mut command,
                PollingHeader::new_from_notification_request_header(&header),
                Some(rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData::default()),
                Some(Arc::new(MatchTagFilter(7))),
            ),
            crate::long_polling::polling_result::PollingResult::PollingSuc
        );

        assert!(!service.notify_message_arriving(&topic, 0, &group, Some(6), 0, None, None));
        assert_eq!(processor.calls.load(Ordering::Acquire), 0);
        assert!(service.notify_message_arriving(&topic, 0, &group, Some(7), 0, None, None));
        tokio::time::timeout(Duration::from_secs(1), async {
            while processor.calls.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("matching notification should wake without sleeping");

        service.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_drains_accepted_notification_execution_after_handler_terminal() {
        let processor = Arc::new(ControlledProcessor {
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let service = test_service(&processor);
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_static_str("notification-shutdown-barrier-topic");
        let group = CheetahString::from_static_str("notification-shutdown-barrier-group");
        let header = rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader {
            consumer_group: group.clone(),
            topic: topic.clone(),
            queue_id: 0,
            born_time: i64::try_from(current_millis()).expect("test clock fits i64"),
            order: false,
            attempt_id: None,
            exp_type: None,
            exp: None,
            is_lite_consumer: false,
            client_id: None,
            poll_time: 60_000,
            topic_request_header: None,
        };
        let mut command = RemotingCommand::create_remoting_command(RequestCode::Notification);
        assert_eq!(
            service.polling(
                test_context().await,
                &mut command,
                PollingHeader::new_from_notification_request_header(&header),
                None,
                None,
            ),
            crate::long_polling::polling_result::PollingResult::PollingSuc
        );
        assert!(service.notify_message_arriving(&topic, 0, &group, None, 0, None, None));
        tokio::time::timeout(Duration::from_secs(1), processor.started.notified())
            .await
            .expect("accepted Notification handler must start");

        let deadline = ShutdownDeadline::after(Duration::from_secs(1));
        let producer_report = service
            .stop_producer_until(deadline)
            .await
            .expect("started service owns its producer group");
        assert!(producer_report.is_healthy(), "{}", producer_report.to_json());
        let mut execution_drain = Box::pin(service.drain_executions_until(deadline));
        tokio::select! {
            biased;
            _ = &mut execution_drain => panic!("Notification execution drained before the accepted handler barrier"),
            _ = tokio::task::yield_now() => {}
        }
        assert_eq!(service.legacy_resource_snapshot().active_executions, 1);

        processor.release.notify_one();
        let execution_report = execution_drain.await.expect("started service owns its execution group");
        assert!(execution_report.is_healthy(), "{}", execution_report.to_json());
        assert!(service.finalize_shutdown().await.terminal.is_zero());
    }

    #[tokio::test]
    async fn shutdown_clears_suspended_notification_accounting() {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        PopLongPollingService::start(&service).await;
        let topic = CheetahString::from_static_str("notification-shutdown-topic");
        let group = CheetahString::from_static_str("notification-shutdown-group");
        let header = rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader {
            consumer_group: group.clone(),
            topic: topic.clone(),
            queue_id: 0,
            born_time: i64::try_from(current_millis()).expect("test clock fits i64"),
            order: false,
            attempt_id: None,
            exp_type: None,
            exp: None,
            is_lite_consumer: false,
            client_id: None,
            poll_time: 60_000,
            topic_request_header: None,
        };
        let mut command = RemotingCommand::create_remoting_command(0);
        assert_eq!(
            service.polling(
                test_context().await,
                &mut command,
                PollingHeader::new_from_notification_request_header(&header),
                None,
                None,
            ),
            crate::long_polling::polling_result::PollingResult::PollingSuc
        );
        assert_eq!(service.total_polling_num.load(Ordering::Acquire), 1);

        service.shutdown().await;

        assert_eq!(service.total_polling_num.load(Ordering::Acquire), 0);
        let key = KeyBuilder::build_polling_key(&topic, &group, 0);
        assert_eq!(service.get_polling_num(&key), 0);
    }

    #[tokio::test]
    async fn observed_wakeup_completes_after_processing_and_rejects_duplicate_claim() {
        let processor = Arc::new(ControlledProcessor {
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let service = test_service(&processor);
        PopLongPollingService::start(&service).await;
        let request = test_pop_request().await;

        let mut first = service.wake_up_with_completion(Arc::clone(&request));
        processor.started.notified().await;
        assert_eq!(first.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty));

        let duplicate = service.wake_up_with_completion(request);
        assert_eq!(
            duplicate.await.expect("duplicate completion must be reported"),
            PopWakeupOutcome::AlreadyCompleted
        );

        processor.release.notify_one();
        assert_eq!(
            first.await.expect("processing completion must be reported"),
            PopWakeupOutcome::ProcessingCompleted
        );
        assert_eq!(processor.calls.load(Ordering::Acquire), 1);
        service.shutdown().await;
    }

    #[tokio::test]
    async fn observed_wakeup_reports_inactive_channel_without_processing() {
        let processor = Arc::new(FailingProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = test_service(&processor);
        PopLongPollingService::start(&service).await;
        let request = test_pop_request().await;
        request.get_channel().connection_ref().close();

        let completion = service.wake_up_with_completion(request);

        assert_eq!(
            completion.await.expect("inactive channel must be reported"),
            PopWakeupOutcome::InactiveChannel
        );
        assert_eq!(processor.calls.load(Ordering::Acquire), 0);
        service.shutdown().await;
    }

    #[tokio::test]
    async fn observed_wakeup_reports_service_cancellation() {
        let processor = Arc::new(ControlledProcessor {
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let service = test_service(&processor);
        PopLongPollingService::start(&service).await;
        let request = test_pop_request().await;
        let completion = service.wake_up_with_completion(request);
        processor.started.notified().await;

        let report = service
            .task_group_for_test()
            .expect("service task group must exist")
            .shutdown_now();
        assert!(report.aborted > 0);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), completion)
                .await
                .expect("cancelled completion must be bounded")
                .expect("completion observer must send cancellation"),
            PopWakeupOutcome::ServiceCancelled
        );
        service.shutdown().await;
    }
}

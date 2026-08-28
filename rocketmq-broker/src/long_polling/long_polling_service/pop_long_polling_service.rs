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
use std::sync::Weak;
use std::time::Duration;

use crate::config::broker_config::BrokerConfig;
use cheetah_string::CheetahString;
use crossbeam_skiplist::SkipSet;
use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_model::common::key_builder::KeyBuilder;
use rocketmq_model::common::pop_ack_constants::PopAckConstants;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::CqExtUnit;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use tokio::select;
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::broker_task_group_or_current;
use crate::long_polling::polling_header::PollingHeader;
use crate::long_polling::polling_result::PollingResult;
use crate::long_polling::pop_request::PopRequest;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub(crate) trait PollingCountProvider: Send + Sync {
    fn polling_count(&self, key: &str) -> i32;
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
    polling_map: DashMap<CheetahString, SkipSet<Arc<PopRequest>>>,
    last_clean_time: AtomicU64,
    total_polling_num: AtomicU64,
    notify_last: bool,
    processor: Weak<RP>,
    running: AtomicBool,
    lifecycle: AsyncMutex<()>,
    task_group: Mutex<Option<TaskGroup>>,
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
            polling_map: DashMap::with_capacity(context.policy.pop_polling_map_size),
            last_clean_time: AtomicU64::new(0),
            total_polling_num: AtomicU64::new(0),
            notify_last,
            context,
            processor,
            running: AtomicBool::new(false),
            lifecycle: AsyncMutex::new(()),
            task_group: Mutex::new(None),
        }
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

        let Some(task_group) = broker_task_group_or_current(
            this.context.service_context.as_ref(),
            "rocketmq-broker.long-polling.pop",
            "failed to start PopLongPollingService outside Tokio runtime",
        ) else {
            this.running.store(false, Ordering::Release);
            return;
        };
        let cancellation_token = task_group.cancellation_token();
        let service = Arc::downgrade(this);
        *this.task_group.lock() = Some(task_group.clone());

        let spawn_result = task_group.spawn_service("broker.long-polling.pop.scan", async move {
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
                for entry in service.polling_map.iter() {
                    let value = entry.value();
                    if value.is_empty() {
                        continue;
                    }
                    service.wake_up_expired_requests(value);
                }

                let last_clean_time = service.last_clean_time.load(Ordering::Acquire);
                if last_clean_time == 0 || current_millis().saturating_sub(last_clean_time) > 5 * 60 * 1000 {
                    service.clean_unused_resource();
                }
            }

            if let Some(service) = service.upgrade() {
                // Clean all suspended requests before the owned scan task exits.
                for entry in service.polling_map.iter() {
                    service.drain_polling_queue(entry.value());
                }
                service.running.store(false, Ordering::Release);
            }
        });

        if let Err(error) = spawn_result {
            this.task_group.lock().take();
            this.running.store(false, Ordering::Release);
            warn!(?error, "failed to spawn PopLongPollingService scan task");
        }
    }

    pub async fn shutdown(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        let task_group = self.task_group.lock().take();
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(Duration::from_secs(5)).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "PopLongPollingService shutdown report is unhealthy"
                );
            }
        }
        self.running.store(false, Ordering::Release);
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
        .is_some_and(|pop_request| self.wake_up(pop_request))
    }

    pub(crate) fn notify_message_arriving_before_lag(
        &self,
        topic: &CheetahString,
        cid: &CheetahString,
    ) -> Option<PopWakeupCompletion> {
        self.take_matching_request(topic, -1, cid, true, None, 0, None, None)
            .map(|pop_request| self.wake_up_with_completion(pop_request))
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
    ) -> Option<Arc<PopRequest>> {
        let key = CheetahString::from_string(KeyBuilder::build_polling_key(topic, cid, queue_id));
        if let Some(remoting_commands) = self.polling_map.get(&key) {
            let value_ = remoting_commands.value();
            if value_.is_empty() {
                return None;
            }

            if let Some(pop_request) = self.poll_remoting_commands(value_) {
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
                            remoting_commands.value().insert(pop_request);
                            self.total_polling_num.fetch_add(1, Ordering::AcqRel);
                            return None;
                        }
                    }
                }

                return Some(pop_request);
            }
        }
        None
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

        let cids = self
            .topic_cid_map
            .entry(request_header.get_topic().clone())
            .or_default();
        cids.entry(request_header.get_consumer_group().clone())
            .or_insert(u8::MIN);

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
        let queue = self.polling_map.entry(key).or_default();
        if queue.len() > self.context.policy.pop_polling_size {
            return PollingResult::PollingFull;
        }

        queue.insert(request);

        remoting_command.set_suspended_ref(true);
        self.total_polling_num.fetch_add(1, Ordering::SeqCst);
        PollingResult::PollingSuc
    }

    // wake up and try process request
    pub fn wake_up(&self, pop_request: Arc<PopRequest>) -> bool {
        self.wake_up_inner(pop_request, None)
    }

    pub(crate) fn wake_up_with_completion(&self, pop_request: Arc<PopRequest>) -> PopWakeupCompletion {
        let (sender, receiver) = oneshot::channel();
        self.wake_up_inner(pop_request, Some(PopWakeupObserver::new(sender)));
        receiver
    }

    fn wake_up_inner(&self, pop_request: Arc<PopRequest>, completion: Option<PopWakeupObserver>) -> bool {
        pop_request.release_resource_permit();
        if !pop_request.complete() {
            if let Some(completion) = completion {
                completion.complete(PopWakeupOutcome::AlreadyCompleted);
            }
            return false;
        }
        if !pop_request.get_channel().connection_ref().is_healthy() {
            if let Some(completion) = completion {
                completion.complete(PopWakeupOutcome::InactiveChannel);
            }
            return false;
        }
        match self.processor.upgrade() {
            None => {
                if let Some(completion) = completion {
                    completion.complete(PopWakeupOutcome::ProcessorUnavailable);
                }
                false
            }
            Some(processor) => {
                let task_group = self.task_group.lock().as_ref().cloned();
                let Some(task_group) = task_group else {
                    warn!("PopLongPollingService wake-up skipped because task group is not running");
                    if let Some(completion) = completion {
                        completion.complete(PopWakeupOutcome::ServiceNotRunning);
                    }
                    return false;
                };

                let spawn_result = task_group.spawn("broker.long-polling.pop.wake-up", TaskKind::Worker, async move {
                    let channel = pop_request.get_channel().clone();
                    let ctx = pop_request.get_ctx().clone();
                    let opaque = pop_request.get_remoting_command().opaque();
                    let response = processor
                        .process_request_when_wakeup(channel, ctx, pop_request.get_remoting_command().clone())
                        .await;
                    match response {
                        Ok(result) => {
                            if let Some(completion) = completion {
                                completion.complete(PopWakeupOutcome::ProcessingCompleted);
                            }
                            if let Some(mut response) = result {
                                let channel = pop_request.get_channel();
                                response.set_opaque_mut(opaque);
                                let _ = channel.channel_inner().send_oneway(response, 1000).await;
                            }
                        }
                        Err(e) => {
                            error!("ExecuteRequestWhenWakeup run {}", e);
                            if let Some(completion) = completion {
                                completion.complete(PopWakeupOutcome::ProcessingFailed);
                            }
                        }
                    }
                });
                if let Err(error) = spawn_result {
                    warn!(?error, "failed to spawn PopLongPollingService wake-up task");
                    return false;
                }
                true
            }
        }
    }

    fn poll_remoting_commands(&self, remoting_commands: &SkipSet<Arc<PopRequest>>) -> Option<Arc<PopRequest>> {
        if remoting_commands.is_empty() {
            return None;
        }

        //maybe need to optimize
        loop {
            let pop_request = if self.notify_last {
                remoting_commands.pop_back().map(|entry| entry.value().clone())
            } else {
                remoting_commands.pop_front().map(|entry| entry.value().clone())
            }?;

            self.total_polling_num.fetch_sub(1, Ordering::AcqRel);
            if !pop_request.get_channel().connection_ref().is_healthy() {
                pop_request.release_resource_permit();
                continue;
            }
            return Some(pop_request);
        }
    }

    fn wake_up_expired_requests(&self, queue: &SkipSet<Arc<PopRequest>>) {
        loop {
            let Some(first) = queue.pop_front() else {
                break;
            };
            let first = first.value().clone();
            if !first.is_timeout() {
                queue.insert(first);
                break;
            }
            self.total_polling_num.fetch_sub(1, Ordering::AcqRel);
            self.wake_up(first);
        }
    }

    fn drain_polling_queue(&self, queue: &SkipSet<Arc<PopRequest>>) {
        while let Some(first) = queue.pop_front() {
            self.total_polling_num.fetch_sub(1, Ordering::AcqRel);
            self.wake_up(first.value().clone());
        }
    }

    fn discard_polling_queue(&self, queue: &SkipSet<Arc<PopRequest>>) {
        while let Some(first) = queue.pop_front() {
            self.total_polling_num.fetch_sub(1, Ordering::AcqRel);
            first.value().release_resource_permit();
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_model::common::key_builder::KeyBuilder;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_runtime::common::time_utils::current_millis;
    use rocketmq_store::MessageFilter;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_transport::api::v1::Channel;
    use rocketmq_transport::api::v1::ConnectionHandlerContextWrapper;
    use rocketmq_transport::test_support::Connection;
    use tokio::sync::Notify;

    use super::PopLongPollingRequestProcessor;
    use super::PopLongPollingService;
    use super::PopLongPollingServiceContext;
    use super::PopWakeupOutcome;
    use crate::broker_runtime::BrokerRuntime;
    use crate::config::broker_config::BrokerConfig;
    use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingPolicy;
    use crate::long_polling::polling_header::PollingHeader;
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

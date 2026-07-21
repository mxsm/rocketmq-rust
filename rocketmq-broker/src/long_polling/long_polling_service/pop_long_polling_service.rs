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

use cheetah_string::CheetahString;
use crossbeam_skiplist::SkipSet;
use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_store::consume_queue::cq_ext_unit::CqExtUnit;
use rocketmq_store::filter::ArcMessageFilter;
use tokio::select;
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
    parent_task_group: Option<TaskGroup>,
}

impl PopLongPollingServiceContext {
    pub(crate) fn new(
        policy: PopLongPollingPolicy,
        topic_config_manager: Arc<TopicConfigManager>,
        subscription_group_lookup: SubscriptionGroupConfigLookup,
        parent_task_group: Option<TaskGroup>,
    ) -> Self {
        Self {
            policy,
            topic_config_manager,
            subscription_group_lookup,
            parent_task_group,
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
        channel: rocketmq_remoting::net::channel::Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>>;
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
            this.context.parent_task_group.as_ref(),
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
                    let key = entry.key();
                    let value = entry.value();
                    if value.is_empty() {
                        continue;
                    }
                    loop {
                        let first = value.pop_front();
                        if first.is_none() {
                            break;
                        }
                        let first = first.unwrap().value().clone();
                        if !first.is_timeout() {
                            value.insert(first);
                            break;
                        }
                        service.total_polling_num.fetch_sub(1, Ordering::AcqRel);
                        service.wake_up(first);
                    }
                }

                let last_clean_time = service.last_clean_time.load(Ordering::Acquire);
                if last_clean_time == 0 || current_millis().saturating_sub(last_clean_time) > 5 * 60 * 1000 {
                    service.clean_unused_resource();
                }
            }

            if let Some(service) = service.upgrade() {
                // Clean all suspended requests before the owned scan task exits.
                for entry in service.polling_map.iter() {
                    let value = entry.value();
                    while let Some(first) = value.pop_front() {
                        service.wake_up(first.value().clone());
                    }
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

                let key_array: Vec<&str> = key.split(PopAckConstants::SPLIT).collect();
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
                self.polling_map.remove(&key);
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
        let key = CheetahString::from_string(KeyBuilder::build_polling_key(topic, cid, queue_id));
        if let Some(remoting_commands) = self.polling_map.get(&key) {
            let value_ = remoting_commands.value();
            if value_.is_empty() {
                return false;
            }

            if let Some(pop_request) = self.poll_remoting_commands(value_) {
                let (message_filter, subscription_data) =
                    (pop_request.get_message_filter(), pop_request.get_subscription_data());

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
                        return false;
                    }
                }

                return self.wake_up(pop_request);
            }
        }
        false
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
        if !pop_request.complete() {
            return false;
        }
        match self.processor.upgrade() {
            None => false,
            Some(processor) => {
                let task_group = self.task_group.lock().as_ref().cloned();
                let Some(task_group) = task_group else {
                    warn!("PopLongPollingService wake-up skipped because task group is not running");
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
                            if let Some(mut response) = result {
                                let channel = pop_request.get_channel();
                                response.set_opaque_mut(opaque);
                                let _ = channel.channel_inner().send_oneway(response, 1000).await;
                            }
                        }
                        Err(e) => {
                            error!("ExecuteRequestWhenWakeup run {}", e);
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

        let mut pop_request: Option<Arc<PopRequest>>;

        //maybe need to optimize
        loop {
            if self.notify_last {
                pop_request = remoting_commands.pop_back().map(|entry| entry.value().clone());
            } else {
                pop_request = remoting_commands.pop_front().map(|entry| entry.value().clone());
            }

            self.total_polling_num.fetch_sub(1, Ordering::AcqRel);
            if pop_request.is_some()
                && !pop_request
                    .as_ref()
                    .unwrap()
                    .get_channel()
                    .connection_ref()
                    .is_healthy()
            {
                continue;
            } else {
                break;
            }
        }
        pop_request
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

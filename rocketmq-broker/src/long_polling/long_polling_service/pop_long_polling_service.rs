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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use crossbeam_skiplist::SkipSet;
use dashmap::DashMap;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::consume_queue::cq_ext_unit::CqExtUnit;
use rocketmq_store::filter::MessageFilter;
use tokio::select;
use tokio::sync::Notify;
use tracing::error;
use tracing::info;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::long_polling::polling_header::PollingHeader;
use crate::long_polling::polling_result::PollingResult;
use crate::long_polling::pop_request::PopRequest;

pub(crate) struct PopLongPollingService<MS: MessageStore, RP> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    topic_cid_map: DashMap<CheetahString, DashMap<CheetahString, u8>>,
    polling_map: DashMap<CheetahString, SkipSet<Arc<PopRequest>>>,
    last_clean_time: u64,
    total_polling_num: AtomicU64,
    notify_last: bool,
    processor: Option<ArcMut<RP>>,
    notify: Notify,
}

impl<MS: MessageStore, RP: RequestProcessor + Sync + 'static> PopLongPollingService<MS, RP> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>, notify_last: bool) -> Self {
        Self {
            // 100000 topic default,  100000 lru topic + cid + qid
            topic_cid_map: DashMap::with_capacity(broker_runtime_inner.broker_config().pop_polling_map_size),
            polling_map: DashMap::with_capacity(broker_runtime_inner.broker_config().pop_polling_map_size),
            last_clean_time: 0,
            total_polling_num: AtomicU64::new(0),
            notify_last,
            broker_runtime_inner,
            processor: None,
            notify: Default::default(),
        }
    }

    pub fn start(this: ArcMut<Self>) {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = this.notify.notified() => {break;}
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(20)) => {}
                }

                if this.polling_map.is_empty() {
                    continue;
                }
                for entry in this.polling_map.iter() {
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
                        this.total_polling_num.fetch_sub(1, Ordering::AcqRel);
                        this.wake_up(first);
                    }
                }

                if this.last_clean_time == 0 || get_current_millis() - this.last_clean_time > 5 * 60 * 1000 {
                    this.mut_from_ref().clean_unused_resource();
                }
            }

            //clean all
            for entry in this.polling_map.iter() {
                let value = entry.value();
                while let Some(first) = value.pop_front() {
                    this.wake_up(first.value().clone());
                }
            }
        });
    }

    pub fn shutdown(&mut self) {
        self.notify.notify_waiters();
    }

    fn clean_unused_resource(&mut self) {
        // Clean up topicCidMap
        {
            let mut topic_keys_to_remove = Vec::new();

            for topic_entry in self.topic_cid_map.iter() {
                let topic = topic_entry.key();

                if self
                    .broker_runtime_inner
                    .topic_config_manager()
                    .select_topic_config(topic)
                    .is_none()
                {
                    info!(target: "pop_logger", "remove non-existent topic {} in topicCidMap!", topic);
                    topic_keys_to_remove.push(topic.clone());
                    continue;
                }

                let cid_map = topic_entry.value();
                let mut cid_keys_to_remove = Vec::new();

                for cid_entry in cid_map.iter() {
                    let cid = cid_entry.key();

                    let subscription_group_table = self
                        .broker_runtime_inner
                        .subscription_group_manager()
                        .subscription_group_table();
                    if !subscription_group_table.contains_key(cid) {
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

                if self
                    .broker_runtime_inner
                    .topic_config_manager()
                    .select_topic_config(&topic)
                    .is_none()
                {
                    info!(target: "pop_logger", "remove non-existent topic {} in pollingMap!", topic);
                    polling_keys_to_remove.push(key.clone());
                    continue;
                }
                let subscription_group_table = self
                    .broker_runtime_inner
                    .subscription_group_manager()
                    .subscription_group_table();
                if !subscription_group_table.contains_key(&cid) {
                    info!(target: "pop_logger", "remove non-existent sub {} of topic {} in pollingMap!", cid, topic);
                    polling_keys_to_remove.push(key.clone());
                }
            }

            // Remove polling entries outside the iteration
            for key in polling_keys_to_remove {
                self.polling_map.remove(&key);
            }
        }
        self.last_clean_time = get_current_millis();
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
        message_filter: Option<Arc<Box<dyn MessageFilter>>>,
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

        if self.total_polling_num.load(Ordering::SeqCst)
            >= self.broker_runtime_inner.broker_config().max_pop_polling_size
        {
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
        if queue.len() > self.broker_runtime_inner.broker_config().pop_polling_size {
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
        match self.processor.clone() {
            None => false,
            Some(mut processor) => {
                tokio::spawn(async move {
                    let channel = pop_request.get_channel().clone();
                    let ctx = pop_request.get_ctx().clone();
                    let opaque = pop_request.get_remoting_command().opaque();
                    let response = processor
                        .process_request(channel, ctx, &mut pop_request.get_remoting_command().clone()) // maybe can optimize
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

    pub fn set_processor(&mut self, processor: ArcMut<RP>) {
        self.processor = Some(processor);
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#![allow(unused_variables)]

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::SkipSet;
use dashmap::DashMap;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::consume_queue::consume_queue_ext::CqExtUnit;
use rocketmq_store::filter::MessageFilter;
use rocketmq_store::log_file::MessageStore;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::long_polling::polling_header::PollingHeader;
use crate::long_polling::polling_result::PollingResult;
use crate::long_polling::pop_request::PopRequest;

pub(crate) struct PopLongPollingService<MS> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    topic_cid_map: DashMap<CheetahString, DashMap<String, u8>>,
    polling_map: SkipMap<CheetahString, SkipSet<PopRequest>>,
    last_clean_time: u64,
    total_polling_num: AtomicU64,
    notify_last: bool,
}

impl<MS: MessageStore> PopLongPollingService<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>, notify_last: bool) -> Self {
        Self {
            // 100000 topic default,  100000 lru topic + cid + qid
            topic_cid_map: DashMap::with_capacity(
                broker_runtime_inner.broker_config().pop_polling_map_size,
            ),
            polling_map: SkipMap::new(),
            last_clean_time: 0,
            total_polling_num: AtomicU64::new(0),
            notify_last,
            broker_runtime_inner,
        }
    }

    pub fn start(&mut self) {
        warn!("PopLongPollingService::start is not implemented");
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
                let (message_filter, subscription_data) = (
                    pop_request.get_message_filter(),
                    pop_request.get_subscription_data(),
                );

                let mut match_result = message_filter.is_matched_by_consume_queue(
                    tags_code,
                    Some(&CqExtUnit::new(
                        tags_code.unwrap(),
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
                    return false;
                }

                return self.wake_up(pop_request);
            }
        }
        false
    }

    pub fn polling(
        &self,
        ctx: ConnectionHandlerContext,
        remoting_command: RemotingCommand,
        request_header: PollingHeader,
        subscription_data: SubscriptionData,
        message_filter: Option<Arc<Box<dyn MessageFilter>>>,
    ) -> PollingResult {
        unimplemented!("PopLongPollingService::polling")
    }

    pub fn wake_up(&self, pop_request: PopRequest) -> bool {
        unimplemented!("PopLongPollingService::wake_up")
    }

    fn poll_remoting_commands(
        &self,
        remoting_commands: &SkipSet<PopRequest>,
    ) -> Option<PopRequest> {
        if remoting_commands.is_empty() {
            return None;
        }

        let mut pop_request: Option<PopRequest>;

        //maybe need to optimize
        loop {
            if self.notify_last {
                pop_request = remoting_commands
                    .pop_back()
                    .map(|entry| entry.value().clone());
            } else {
                pop_request = remoting_commands
                    .pop_front()
                    .map(|entry| entry.value().clone());
            }

            if let Some(ref request) = pop_request {
                self.total_polling_num.fetch_sub(1, Ordering::AcqRel);
            } else {
                break;
            }
        }
        pop_request
    }
}

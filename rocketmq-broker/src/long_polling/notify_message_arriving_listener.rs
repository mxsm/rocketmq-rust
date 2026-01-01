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

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_arriving_listener::MessageArrivingListener;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;

pub struct NotifyMessageArrivingListener<MS: MessageStore> {
    //pull_request_hold_service: ArcMut<PullRequestHoldService<MS>>,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> NotifyMessageArrivingListener<MS>
where
    MS: MessageStore + Send + Sync,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
}

#[allow(unused_variables)]
impl<MS> MessageArrivingListener for NotifyMessageArrivingListener<MS>
where
    MS: MessageStore + Send + Sync,
{
    fn arriving(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        logic_offset: i64,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        self.broker_runtime_inner
            .pull_request_hold_service()
            .as_ref()
            .unwrap()
            .notify_message_arriving_ext(
                topic,
                queue_id,
                logic_offset,
                tags_code,
                msg_store_time,
                filter_bit_map.clone(),
                properties,
            );

        self.broker_runtime_inner
            .pop_message_processor_unchecked()
            .notify_message_arriving_full(
                topic.clone(),
                queue_id,
                tags_code,
                msg_store_time,
                filter_bit_map.clone(),
                properties,
            );

        self.broker_runtime_inner
            .notification_processor_unchecked()
            .notify_message_arriving(
                topic.clone(),
                queue_id,
                tags_code,
                msg_store_time,
                filter_bit_map,
                properties,
            );
    }
}

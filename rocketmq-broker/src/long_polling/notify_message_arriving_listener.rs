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
use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;
use rocketmq_store::base::message_arriving_listener::MessageArrivingListener;
use rocketmq_store::base::message_store::MessageStore;

use crate::long_polling::long_polling_service::pull_request_hold_service::PullRequestHoldService;
use crate::processor::notification_processor::NotificationProcessor;
use crate::processor::pop_message_processor::PopMessageProcessor;

pub struct NotifyMessageArrivingListener<MS: MessageStore> {
    pull_request_hold_service: Weak<PullRequestHoldService<MS>>,
    pop_message_processor: Weak<PopMessageProcessor<MS>>,
    notification_processor: Weak<NotificationProcessor<MS>>,
}

impl<MS> NotifyMessageArrivingListener<MS>
where
    MS: MessageStore + Send + Sync,
{
    pub fn new(
        pull_request_hold_service: &Arc<PullRequestHoldService<MS>>,
        pop_message_processor: &Arc<PopMessageProcessor<MS>>,
        notification_processor: &Arc<NotificationProcessor<MS>>,
    ) -> Self {
        Self {
            pull_request_hold_service: Arc::downgrade(pull_request_hold_service),
            pop_message_processor: Arc::downgrade(pop_message_processor),
            notification_processor: Arc::downgrade(notification_processor),
        }
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
        if let Some(pull_request_hold_service) = self.pull_request_hold_service.upgrade() {
            pull_request_hold_service.notify_message_arriving_ext(
                topic,
                queue_id,
                logic_offset,
                tags_code,
                msg_store_time,
                filter_bit_map.clone(),
                properties,
            );
        }

        if let Some(pop_message_processor) = self.pop_message_processor.upgrade() {
            pop_message_processor.notify_message_arriving_full(
                topic.clone(),
                queue_id,
                tags_code,
                msg_store_time,
                filter_bit_map.clone(),
                properties,
            );
        }

        if let Some(notification_processor) = self.notification_processor.upgrade() {
            notification_processor.notify_message_arriving(
                topic.clone(),
                queue_id,
                tags_code,
                msg_store_time,
                filter_bit_map,
                properties,
            );
        }
    }
}

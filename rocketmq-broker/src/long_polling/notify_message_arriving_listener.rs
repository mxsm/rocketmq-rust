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
use std::collections::HashMap;

use rocketmq_common::ArcRefCellWrapper;
use rocketmq_store::base::message_arriving_listener::MessageArrivingListener;
use rocketmq_store::log_file::MessageStore;

use crate::long_polling::long_polling_service::pull_request_hold_service::PullRequestHoldService;

pub struct NotifyMessageArrivingListener<MS> {
    pull_request_hold_service: ArcRefCellWrapper<PullRequestHoldService<MS>>,
}

impl<MS> NotifyMessageArrivingListener<MS>
where
    MS: MessageStore + Send + Sync,
{
    pub fn new(pull_request_hold_service: ArcRefCellWrapper<PullRequestHoldService<MS>>) -> Self {
        Self {
            pull_request_hold_service,
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
        topic: &str,
        queue_id: i32,
        logic_offset: i64,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<String, String>>,
    ) {
        self.pull_request_hold_service.notify_message_arriving_ext(
            topic,
            queue_id,
            logic_offset,
            tags_code,
            msg_store_time,
            filter_bit_map,
            properties,
        );
    }
}

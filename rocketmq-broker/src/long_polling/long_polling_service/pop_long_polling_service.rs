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
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::filter::MessageFilter;

use crate::long_polling::polling_header::PollingHeader;
use crate::long_polling::polling_result::PollingResult;

pub(crate) struct PopLongPollingService;

impl PopLongPollingService {
    pub fn notify_message_arriving(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        cid: &CheetahString,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> Self {
        unimplemented!("PopLongPollingService::notifyMessageArriving")
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
}

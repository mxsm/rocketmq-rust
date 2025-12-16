//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::base::mq_admin::MQAdmin;

#[allow(async_fn_in_trait)]
pub trait MQConsumer: MQAdmin {
    async fn send_message_back(
        &mut self,
        msg: MessageExt,
        delay_level: i32,
        broker_name: &str,
    ) -> rocketmq_error::RocketMQResult<()>;

    async fn fetch_subscribe_message_queues(
        &mut self,
        topic: &str,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>>;
}

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
use std::error::Error;
use std::sync::Arc;

use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;

use crate::implementation::communication_mode::CommunicationMode;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::send_result::SendResult;

#[derive(Clone)]
pub struct SendMessageContext {
    pub producer_group: String,
    pub message: Message,
    pub mq: MessageQueue,
    pub broker_addr: String,
    pub born_host: String,
    pub communication_mode: CommunicationMode,
    pub send_result: Option<SendResult>,
    pub exception: Option<Arc<Box<dyn Error>>>,
    pub mq_trace_context: Option<Arc<Box<dyn std::any::Any>>>,
    pub props: HashMap<String, String>,
    pub producer: Arc<DefaultMQProducerImpl>,
    pub msg_type: MessageType,
    pub namespace: String,
}

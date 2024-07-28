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
use std::collections::HashSet;
use std::sync::Arc;

use rocketmq_common::common::message::message_single::MessageExt;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;

use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::transaction_listener::TransactionListener;

#[trait_variant::make(MQProducerInner: Send)]
pub trait MQProducerInnerLocal: Send {
    fn get_publish_topic_list(&self) -> HashSet<String>;

    fn is_publish_topic_need_update(&self, topic: &str) -> bool;

    fn get_check_listener(&self) -> Arc<Box<dyn TransactionListener>>;

    fn check_transaction_state(
        &self,
        addr: &str,
        msg: &MessageExt,
        check_request_header: &CheckTransactionStateRequestHeader,
    );

    fn update_topic_publish_info(&self, topic: &str, info: &TopicPublishInfo);

    fn is_unit_mode(&self) -> bool;
}

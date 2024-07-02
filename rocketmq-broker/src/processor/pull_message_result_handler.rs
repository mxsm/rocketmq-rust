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

use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::filter::MessageFilter;

pub trait PullMessageResultHandler: Sync + Send + 'static {
    fn handle(
        &self,
        get_message_result: GetMessageResult,
        request: RemotingCommand,
        request_header: PullMessageRequestHeader,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        subscription_data: SubscriptionData,
        subscription_group_config: SubscriptionGroupConfig,
        broker_allow_suspend: bool,
        message_filter: Box<dyn MessageFilter>,
        response: RemotingCommand,
        mapping_context: TopicQueueMappingContext,
        begin_time_mills: u64,
    ) -> Option<RemotingCommand>;
}

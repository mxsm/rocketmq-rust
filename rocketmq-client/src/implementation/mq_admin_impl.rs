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
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;

use crate::base::client_config::ClientConfig;
use crate::error::MQClientError::MQClientException;
use crate::factory::mq_client_instance;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;
use crate::Result;

pub struct MQAdminImpl {
    timeout_millis: u64,
}

impl MQAdminImpl {
    pub fn new() -> Self {
        MQAdminImpl {
            timeout_millis: 60000,
        }
    }
}

impl MQAdminImpl {
    pub fn parse_publish_message_queues(
        &mut self,
        message_queue_array: &[MessageQueue],
        client_config: &mut ClientConfig,
    ) -> Vec<MessageQueue> {
        let mut message_queues = Vec::new();
        for message_queue in message_queue_array {
            let user_topic = NamespaceUtil::without_namespace_with_namespace(
                message_queue.get_topic(),
                client_config
                    .get_namespace()
                    .unwrap_or("".to_string())
                    .as_str(),
            );

            let message_queue = MessageQueue::from_parts(
                user_topic,
                message_queue.get_broker_name(),
                message_queue.get_queue_id(),
            );
            message_queues.push(message_queue);
        }
        message_queues
    }

    pub async fn fetch_publish_message_queues(
        &mut self,
        topic: &str,
        mq_client_api_impl: ArcRefCellWrapper<MQClientAPIImpl>,
        client_config: &mut ClientConfig,
    ) -> Result<Vec<MessageQueue>> {
        let topic_route_data = mq_client_api_impl
            .get_topic_route_info_from_name_server_detail(topic, self.timeout_millis, true)
            .await?;
        if let Some(mut topic_route_data) = topic_route_data {
            let topic_publish_info = mq_client_instance::topic_route_data2topic_publish_info(
                topic,
                &mut topic_route_data,
            );
            if topic_publish_info.ok() {
                return Ok(self.parse_publish_message_queues(
                    &topic_publish_info.message_queue_list,
                    client_config,
                ));
            }
        }
        Err(MQClientException(
            -1,
            format!(
                "Unknow why, Can not find Message Queue for this topic, {}",
                topic
            ),
        ))
    }
}

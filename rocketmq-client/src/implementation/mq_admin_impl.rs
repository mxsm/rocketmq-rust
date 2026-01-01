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

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_rust::ArcMut;

use crate::base::client_config::ClientConfig;
use crate::factory::mq_client_instance;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;

pub struct MQAdminImpl {
    timeout_millis: u64,
    client: Option<ArcMut<MQClientInstance>>,
}

impl MQAdminImpl {
    pub fn new() -> Self {
        MQAdminImpl {
            timeout_millis: 60000,
            client: None,
        }
    }

    pub fn set_client(&mut self, client: ArcMut<MQClientInstance>) {
        self.client = Some(client);
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
                client_config.get_namespace().unwrap_or_default().as_str(),
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
        mq_client_api_impl: ArcMut<MQClientAPIImpl>,
        client_config: &mut ClientConfig,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        let topic_route_data = mq_client_api_impl
            .get_topic_route_info_from_name_server_detail(topic, self.timeout_millis, true)
            .await?;
        if let Some(mut topic_route_data) = topic_route_data {
            let topic_publish_info =
                mq_client_instance::topic_route_data2topic_publish_info(topic, &mut topic_route_data);
            if topic_publish_info.ok() {
                return Ok(self.parse_publish_message_queues(&topic_publish_info.message_queue_list, client_config));
            }
        }
        Err(mq_client_err!(format!(
            "Unknow why, Can not find Message Queue for this topic, {}",
            topic
        )))
    }

    pub async fn max_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        let client = self.client.as_mut().expect("client is None");
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let mut broker_addr = client.find_broker_address_in_publish(broker_name.as_ref()).await;
        if broker_addr.is_none() {
            client
                .update_topic_route_info_from_name_server_topic(mq.get_topic_cs())
                .await;
            let broker_name = client.get_broker_name_from_message_queue(mq).await;
            broker_addr = client.find_broker_address_in_publish(broker_name.as_ref()).await;
        }
        if let Some(ref broker_addr) = broker_addr {
            let offset = client
                .mq_client_api_impl
                .as_mut()
                .expect("mq_client_api_impl is None")
                .get_max_offset(broker_addr, mq, self.timeout_millis)
                .await?;
            return Ok(offset);
        }

        unimplemented!("max_offset")
    }
    pub async fn search_offset(&mut self, mq: &MessageQueue, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        unimplemented!("max_offset")
    }
}

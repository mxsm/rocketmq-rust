//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::mix_all;
use rocketmq_remoting::local::LocalRequestHarness;
use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use tokio::time::timeout;

use crate::broker_runtime::BrokerRuntime;

const LOCAL_PROXY_RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);

pub struct ProxyBrokerFacade {
    runtime: BrokerRuntime,
}

impl ProxyBrokerFacade {
    pub fn new(mut broker_config: BrokerConfig, message_store_config: MessageStoreConfig) -> Self {
        broker_config.transfer_msg_by_heap = true;
        broker_config.broker_server_config.listen_port = broker_config.listen_port;
        Self {
            runtime: BrokerRuntime::new(Arc::new(broker_config), Arc::new(message_store_config)),
        }
    }

    pub async fn initialize(&mut self) -> bool {
        self.runtime.initialize().await
    }

    pub async fn start(&mut self) {
        self.runtime.start().await;
    }

    pub async fn shutdown(&mut self) {
        self.runtime.shutdown().await;
    }

    pub fn broker_config(&self) -> &BrokerConfig {
        self.runtime.broker_config()
    }

    pub fn query_route(&self, topic: &str) -> rocketmq_error::RocketMQResult<TopicRouteData> {
        let topic_name = CheetahString::from(topic);
        let topic_config =
            self.runtime
                .topic_config(&topic_name)
                .ok_or_else(|| rocketmq_error::RocketMQError::TopicNotExist {
                    topic: topic.to_owned(),
                })?;

        Ok(build_topic_route(self.runtime.broker_config(), topic_config.as_ref()))
    }

    pub fn query_topic_message_type(&self, topic: &str) -> rocketmq_error::RocketMQResult<TopicMessageType> {
        let topic_name = CheetahString::from(topic);
        let topic_config =
            self.runtime
                .topic_config(&topic_name)
                .ok_or_else(|| rocketmq_error::RocketMQError::TopicNotExist {
                    topic: topic.to_owned(),
                })?;
        Ok(topic_config.get_topic_message_type())
    }

    pub fn query_subscription_group(
        &self,
        group: &str,
    ) -> rocketmq_error::RocketMQResult<Option<Arc<SubscriptionGroupConfig>>> {
        Ok(self.runtime.subscription_group(&CheetahString::from(group)))
    }

    pub async fn process_request(
        &self,
        mut request: rocketmq_remoting::protocol::remoting_command::RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<rocketmq_remoting::protocol::remoting_command::RemotingCommand> {
        request.make_custom_header_to_net();
        let mut processor = self.runtime.proxy_request_processor().ok_or_else(|| {
            rocketmq_error::RocketMQError::Internal(
                "embedded broker request processor is not ready; call start() first".to_owned(),
            )
        })?;

        let opaque = request.opaque();
        let mut harness = LocalRequestHarness::new().await?;
        if let Some(mut response) = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await?
        {
            response.make_custom_header_to_net();
            return Ok(response.set_opaque(opaque).mark_response_type());
        }

        let response = timeout(LOCAL_PROXY_RESPONSE_TIMEOUT, harness.receive_response())
            .await
            .map_err(|_| rocketmq_error::RocketMQError::Timeout {
                operation: "embedded_broker_response",
                timeout_ms: LOCAL_PROXY_RESPONSE_TIMEOUT.as_millis() as u64,
            })??;

        response.ok_or_else(|| {
            rocketmq_error::RocketMQError::Internal(format!(
                "embedded broker produced no response for request code {}",
                request.code()
            ))
        })
    }
}

fn build_topic_route(broker_config: &BrokerConfig, topic_config: &TopicConfig) -> TopicRouteData {
    let broker_name = broker_config.broker_identity.broker_name.clone();
    let broker_addr = CheetahString::from(format!(
        "{}:{}",
        broker_config.broker_ip1, broker_config.broker_server_config.listen_port
    ));
    let mut broker_addrs = HashMap::new();
    broker_addrs.insert(mix_all::MASTER_ID, broker_addr);

    TopicRouteData {
        queue_datas: vec![QueueData::new(
            broker_name.clone(),
            topic_config.read_queue_nums,
            topic_config.write_queue_nums,
            topic_config.perm,
            topic_config.topic_sys_flag,
        )],
        broker_datas: vec![BrokerData::new(
            broker_config.broker_identity.broker_cluster_name.clone(),
            broker_name,
            broker_addrs,
            None,
        )],
        ..TopicRouteData::default()
    }
}

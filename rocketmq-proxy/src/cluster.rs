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

use std::future::Future;

use async_trait::async_trait;
use cheetah_string::CheetahString;
use rocketmq_client_rust::base::client_config::ClientConfig as RocketmqClientConfig;
use rocketmq_client_rust::factory::mq_client_instance::MQClientInstance;
use rocketmq_client_rust::implementation::mq_client_manager::MQClientManager;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_rust::ArcMut;

use crate::config::ClusterConfig;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::service::ProxyTopicMessageType;
use crate::service::ResourceIdentity;
use crate::service::SubscriptionGroupMetadata;

#[async_trait]
pub trait ClusterClient: Send + Sync {
    async fn query_route(&self, topic: &ResourceIdentity) -> ProxyResult<TopicRouteData>;

    async fn query_assignment(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
        client_id: &str,
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>>;

    async fn query_topic_message_type(&self, topic: &ResourceIdentity) -> ProxyResult<ProxyTopicMessageType>;

    async fn query_subscription_group(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>>;
}

pub struct RocketmqClusterClient {
    config: ClusterConfig,
}

impl RocketmqClusterClient {
    pub fn new(config: ClusterConfig) -> Self {
        Self { config }
    }
}

impl Default for RocketmqClusterClient {
    fn default() -> Self {
        Self::new(ClusterConfig::default())
    }
}

#[async_trait]
impl ClusterClient for RocketmqClusterClient {
    async fn query_route(&self, topic: &ResourceIdentity) -> ProxyResult<TopicRouteData> {
        let config = self.config.clone();
        let topic_name = topic.to_string();

        run_cluster_task(move || async move {
            let client = initialize_client_instance(config.clone()).await?;
            let route = client
                .get_mq_client_api_impl()
                .get_topic_route_info_from_name_server(topic_name.as_str(), config.mq_client_api_timeout_ms)
                .await?;

            route.ok_or_else(|| RocketMQError::route_not_found(topic_name).into())
        })
        .await
    }

    async fn query_assignment(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
        client_id: &str,
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
        let config = self.config.clone();
        let topic_name = topic.to_string();
        let group_name = group.to_string();
        let client_id = client_id.to_owned();

        run_cluster_task(move || async move {
            let client = initialize_client_instance(config.clone()).await?;
            let route = client
                .get_mq_client_api_impl()
                .get_topic_route_info_from_name_server(topic_name.as_str(), config.mq_client_api_timeout_ms)
                .await?
                .ok_or_else(|| RocketMQError::route_not_found(topic_name.clone()))?;
            let broker_addr = select_master_broker_addr(&route).ok_or_else(|| RocketMQError::BrokerNotFound {
                name: topic_name.clone(),
            })?;
            let mut api = client.get_mq_client_api_impl();
            let assignments = api
                .query_assignment(
                    &broker_addr,
                    CheetahString::from(topic_name),
                    CheetahString::from(group_name),
                    CheetahString::from(client_id),
                    CheetahString::from(config.query_assignment_strategy_name),
                    MessageModel::Clustering,
                    config.mq_client_api_timeout_ms,
                )
                .await?;

            Ok(assignments.map(|items| items.into_iter().collect()))
        })
        .await
    }

    async fn query_topic_message_type(&self, topic: &ResourceIdentity) -> ProxyResult<ProxyTopicMessageType> {
        let config = self.config.clone();
        let topic_name = topic.to_string();

        run_cluster_task(move || async move {
            let client = initialize_client_instance(config.clone()).await?;
            let route = client
                .get_mq_client_api_impl()
                .get_topic_route_info_from_name_server(topic_name.as_str(), config.mq_client_api_timeout_ms)
                .await?
                .ok_or_else(|| RocketMQError::route_not_found(topic_name.clone()))?;
            let broker_addr = select_master_broker_addr(&route).ok_or_else(|| RocketMQError::BrokerNotFound {
                name: topic_name.clone(),
            })?;
            let topic_config = client
                .get_topic_config(
                    &broker_addr,
                    CheetahString::from(topic_name),
                    config.mq_client_api_timeout_ms,
                )
                .await?;

            Ok(convert_topic_message_type(topic_config.get_topic_message_type()))
        })
        .await
    }

    async fn query_subscription_group(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        let config = self.config.clone();
        let topic_name = topic.to_string();
        let group_name = group.to_string();

        run_cluster_task(move || async move {
            let client = initialize_client_instance(config.clone()).await?;
            let route = client
                .get_mq_client_api_impl()
                .get_topic_route_info_from_name_server(topic_name.as_str(), config.mq_client_api_timeout_ms)
                .await?
                .ok_or_else(|| RocketMQError::route_not_found(topic_name.clone()))?;
            let Some(broker_addr) = select_master_broker_addr(&route) else {
                return Ok(None);
            };
            let group_config = client
                .get_subscription_group_config(
                    &broker_addr,
                    CheetahString::from(group_name),
                    config.mq_client_api_timeout_ms,
                )
                .await?;

            Ok(Some(convert_subscription_group(group_config)))
        })
        .await
    }
}

async fn run_cluster_task<T, F, Fut>(task: F) -> ProxyResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ProxyResult<T>> + 'static,
{
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| ProxyError::Transport {
                message: format!("failed to build proxy cluster runtime: {error}"),
            })?;
        runtime.block_on(task())
    })
    .await
    .map_err(|error| ProxyError::Transport {
        message: format!("proxy cluster task failed: {error}"),
    })?
}

async fn initialize_client_instance(config: ClusterConfig) -> ProxyResult<ArcMut<MQClientInstance>> {
    let mut client_config = RocketmqClientConfig::default();
    client_config.set_instance_name(CheetahString::from(config.instance_name));
    client_config.set_mq_client_api_timeout(config.mq_client_api_timeout_ms);
    if let Some(namesrv_addr) = config.namesrv_addr {
        client_config.set_namesrv_addr(CheetahString::from(namesrv_addr));
    }

    let mut instance = MQClientManager::get_instance().get_or_create_mq_client_instance(client_config, None);
    let this = instance.clone();
    instance.start(this).await?;
    Ok(instance)
}

fn select_master_broker_addr(route: &TopicRouteData) -> Option<CheetahString> {
    route.broker_datas.iter().find_map(|broker| {
        broker
            .broker_addrs()
            .get(&MASTER_ID)
            .cloned()
            .or_else(|| broker.select_broker_addr())
    })
}

fn convert_topic_message_type(message_type: TopicMessageType) -> ProxyTopicMessageType {
    match message_type {
        TopicMessageType::Unspecified => ProxyTopicMessageType::Unspecified,
        TopicMessageType::Normal => ProxyTopicMessageType::Normal,
        TopicMessageType::Fifo => ProxyTopicMessageType::Fifo,
        TopicMessageType::Delay => ProxyTopicMessageType::Delay,
        TopicMessageType::Transaction => ProxyTopicMessageType::Transaction,
        TopicMessageType::Mixed => ProxyTopicMessageType::Mixed,
    }
}

fn convert_subscription_group(group_config: SubscriptionGroupConfig) -> SubscriptionGroupMetadata {
    SubscriptionGroupMetadata {
        consume_message_orderly: group_config.consume_message_orderly(),
        lite_bind_topic: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

    use super::convert_subscription_group;
    use super::convert_topic_message_type;
    use super::select_master_broker_addr;
    use crate::service::ProxyTopicMessageType;

    #[test]
    fn cluster_client_prefers_master_broker_address() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(1_u64, CheetahString::from("127.0.0.2:10911"));
        broker_addrs.insert(0_u64, CheetahString::from("127.0.0.1:10911"));
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 1, 1, 6, 0)],
            broker_datas: vec![BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                None,
            )],
            ..Default::default()
        };

        assert_eq!(select_master_broker_addr(&route).unwrap().as_str(), "127.0.0.1:10911");
    }

    #[test]
    fn topic_message_type_conversion_matches_proxy_model() {
        assert_eq!(
            convert_topic_message_type(TopicMessageType::Fifo),
            ProxyTopicMessageType::Fifo
        );
    }

    #[test]
    fn subscription_group_conversion_preserves_order_flag() {
        let mut config = SubscriptionGroupConfig::default();
        config.set_consume_message_orderly(true);

        let converted = convert_subscription_group(config);
        assert!(converted.consume_message_orderly);
    }
}

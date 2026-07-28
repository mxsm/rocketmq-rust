// Copyright 2026 The RocketMQ Rust Authors
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

//! Narrow read-only administration surface.
//!
//! This trait is the capability boundary used by diagnostics and AI SRE
//! integrations. Mutation methods remain unavailable unless the separate
//! `admin-mutation` capability is enabled.

use cheetah_string::CheetahString;
use rand::seq::IndexedRandom;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_protocol::protocol::body::group_list::GroupList;
use rocketmq_protocol::protocol::body::kv_table::KVTable;
use rocketmq_protocol::protocol::body::producer_connection::ProducerConnection;
use rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainStateResponseBody;
use rocketmq_protocol::protocol::body::topic::topic_list::TopicList;
use rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_protocol::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::route_facade::BrokerDataExt;

use super::default_mq_admin_ext::DefaultMQAdminExt;

/// Sanitized allowlisted Broker fields required by supervised configuration
/// patching. Arbitrary Broker properties never cross this read boundary.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BrokerConfigAllowlisted {
    pub send_message_thread_pool_nums: Option<u32>,
    pub pull_message_thread_pool_nums: Option<u32>,
    pub flush_delay_offset_interval_ms: Option<u64>,
}

#[allow(async_fn_in_trait)]
pub trait MQAdminReadExt: Send {
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()>;

    async fn shutdown(&mut self);

    async fn fetch_all_topic_list(&self) -> rocketmq_error::RocketMQResult<TopicList>;

    async fn fetch_broker_runtime_stats(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<KVTable>;

    /// Reads only the three non-sensitive Broker fields supported by the SRE
    /// generation-CAS action.
    async fn get_broker_config_allowlisted(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<BrokerConfigAllowlisted>;

    /// Returns the authenticated, bounded drain state for one Proxy endpoint.
    async fn proxy_drain_state(
        &self,
        proxy_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProxyDrainStateResponseBody>;

    async fn examine_consume_stats(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
        broker_addr: Option<CheetahString>,
        timeout_millis: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<ConsumeStats>;

    async fn examine_broker_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo>;

    async fn examine_topic_route_info(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>>;

    async fn examine_consumer_connection_info(
        &self,
        consumer_group: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<ConsumerConnection>;

    async fn examine_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection>;

    async fn get_all_producer_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerTableInfo>;

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> rocketmq_error::RocketMQResult<GroupList>;
}

impl MQAdminReadExt for DefaultMQAdminExt {
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        self.inner_mut().start_admin().await
    }

    async fn shutdown(&mut self) {
        self.inner_mut().shutdown_admin().await;
    }

    async fn fetch_all_topic_list(&self) -> rocketmq_error::RocketMQResult<TopicList> {
        self.inner()
            .mq_client_api()?
            .get_all_topic_list_from_name_server(self.inner().remoting_timeout_millis()?)
            .await
    }

    async fn fetch_broker_runtime_stats(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<KVTable> {
        self.inner()
            .mq_client_api()?
            .get_broker_runtime_info(&broker_addr, self.inner().remoting_timeout_millis()?)
            .await
    }

    async fn get_broker_config_allowlisted(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<BrokerConfigAllowlisted> {
        let properties = self
            .inner()
            .mq_client_api()?
            .get_broker_config(&broker_addr, self.inner().remoting_timeout_millis()?)
            .await?;
        Ok(BrokerConfigAllowlisted {
            send_message_thread_pool_nums: parse_allowlisted_value(&properties, "sendMessageThreadPoolNums")?,
            pull_message_thread_pool_nums: parse_allowlisted_value(&properties, "pullMessageThreadPoolNums")?,
            flush_delay_offset_interval_ms: parse_allowlisted_value(&properties, "flushDelayOffsetInterval")?,
        })
    }

    async fn proxy_drain_state(
        &self,
        proxy_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProxyDrainStateResponseBody> {
        self.inner()
            .mq_client_api()?
            .get_proxy_drain_state(&proxy_addr, self.inner().remoting_timeout_millis()?)
            .await
    }

    async fn examine_consume_stats(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        _cluster_name: Option<CheetahString>,
        broker_addr: Option<CheetahString>,
        timeout_millis: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<ConsumeStats> {
        let timeout = timeout_millis.unwrap_or(self.inner().remoting_timeout_millis()?);
        let topic = topic.unwrap_or_default();
        if let Some(broker_addr) = broker_addr {
            return self
                .inner()
                .mq_client_api()?
                .get_consume_stats(
                    &broker_addr,
                    GetConsumeStatsRequestHeader {
                        consumer_group,
                        topic,
                        topic_request_header: None,
                    },
                    timeout,
                )
                .await;
        }

        let retry_topic: CheetahString = mix_all::get_retry_topic(&consumer_group).into();
        let route = self
            .inner()
            .mq_client_api()?
            .get_topic_route_info_from_name_server(&retry_topic, timeout)
            .await?;
        let mut result = ConsumeStats::new();
        if let Some(route) = route {
            for broker in &route.broker_datas {
                if let Some(master_addr) = broker.broker_addrs().get(&mix_all::MASTER_ID) {
                    let stats = self
                        .inner()
                        .mq_client_api()?
                        .get_consume_stats(
                            master_addr,
                            GetConsumeStatsRequestHeader {
                                consumer_group: consumer_group.clone(),
                                topic: topic.clone(),
                                topic_request_header: None,
                            },
                            timeout,
                        )
                        .await?;
                    result.get_offset_table_mut().extend(stats.offset_table);
                    result.set_consume_tps(result.get_consume_tps() + stats.consume_tps);
                }
            }
        }
        Ok(result)
    }

    async fn examine_broker_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo> {
        self.inner()
            .mq_client_api()?
            .get_broker_cluster_info(self.inner().remoting_timeout_millis()?)
            .await
    }

    async fn examine_topic_route_info(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.inner()
            .mq_client_api()?
            .get_topic_route_info_from_name_server(&topic, self.inner().remoting_timeout_millis()?)
            .await
    }

    async fn examine_consumer_connection_info(
        &self,
        consumer_group: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<ConsumerConnection> {
        let timeout = self.inner().remoting_timeout_millis()?;
        let selected_addr = match broker_addr {
            Some(broker_addr) => Some(broker_addr),
            None => {
                let retry_topic = CheetahString::from_string(mix_all::get_retry_topic(consumer_group.as_str()));
                self.inner()
                    .mq_client_api()?
                    .get_topic_route_info_from_name_server(&retry_topic, timeout)
                    .await?
                    .and_then(|route| {
                        route
                            .broker_datas
                            .choose(&mut rand::rng())
                            .and_then(BrokerDataExt::select_broker_addr)
                    })
            }
        };
        let mut result = ConsumerConnection::new();
        if let Some(broker_addr) = selected_addr {
            result = self
                .inner()
                .mq_client_api()?
                .get_consumer_connection_list(broker_addr.as_str(), consumer_group, timeout)
                .await?;
        }
        if result.get_connection_set().is_empty() {
            return Err(crate::mq_client_err!(
                rocketmq_protocol::code::response_code::ResponseCode::ConsumerNotOnline,
                "Not found the consumer group connection"
            ));
        }
        Ok(result)
    }

    async fn examine_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection> {
        let timeout = self.inner().remoting_timeout_millis()?;
        let route = self
            .inner()
            .mq_client_api()?
            .get_topic_route_info_from_name_server(&topic, timeout)
            .await?;
        let mut result = ProducerConnection::new();
        if let Some(broker_addr) = route.and_then(|route| {
            route
                .broker_datas
                .choose(&mut rand::rng())
                .and_then(BrokerDataExt::select_broker_addr)
        }) {
            result = self
                .inner()
                .mq_client_api()?
                .get_producer_connection_list(broker_addr.as_str(), producer_group, timeout)
                .await?;
        }
        if result.connection_set().is_empty() {
            return Err(crate::mq_client_err!("Not found the producer group connection"));
        }
        Ok(result)
    }

    async fn get_all_producer_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerTableInfo> {
        self.inner()
            .mq_client_api()?
            .get_all_producer_info(broker_addr.as_str(), self.inner().remoting_timeout_millis()?)
            .await
    }

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> rocketmq_error::RocketMQResult<GroupList> {
        let timeout = self.inner().remoting_timeout_millis()?;
        let route = self
            .inner()
            .mq_client_api()?
            .get_topic_route_info_from_name_server(&topic, timeout)
            .await?;
        if let Some(route) = route {
            for broker in &route.broker_datas {
                if let Some(master_addr) = broker.broker_addrs().get(&mix_all::MASTER_ID) {
                    return self
                        .inner()
                        .mq_client_api()?
                        .query_topic_consume_by_who(
                            master_addr,
                            QueryTopicConsumeByWhoRequestHeader {
                                topic: topic.clone(),
                                topic_request_header: None,
                            },
                            timeout,
                        )
                        .await;
                }
            }
        }
        Ok(GroupList::default())
    }
}

fn parse_allowlisted_value<T>(
    properties: &std::collections::HashMap<CheetahString, CheetahString>,
    key: &str,
) -> rocketmq_error::RocketMQResult<Option<T>>
where
    T: std::str::FromStr,
{
    properties
        .iter()
        .find_map(|(name, value)| (name.as_str() == key).then_some(value.as_str()))
        .map(|value| {
            value.parse().map_err(|_| {
                rocketmq_error::RocketMQError::IllegalArgument(format!(
                    "Broker allowlisted configuration `{key}` is malformed"
                ))
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::parse_allowlisted_value;

    #[test]
    fn parses_only_the_requested_allowlisted_value() {
        let properties = HashMap::from([
            (
                CheetahString::from_static_str("sendMessageThreadPoolNums"),
                CheetahString::from_static_str("32"),
            ),
            (
                CheetahString::from_static_str("accessKey"),
                CheetahString::from_static_str("must-not-cross-boundary"),
            ),
        ]);

        let value = parse_allowlisted_value::<u32>(&properties, "sendMessageThreadPoolNums");
        assert_eq!(value.unwrap(), Some(32));
        assert_eq!(
            parse_allowlisted_value::<u32>(&properties, "pullMessageThreadPoolNums").unwrap(),
            None
        );
    }

    #[test]
    fn rejects_malformed_allowlisted_values() {
        let properties = HashMap::from([(
            CheetahString::from_static_str("flushDelayOffsetInterval"),
            CheetahString::from_static_str("not-a-number"),
        )]);

        let error = parse_allowlisted_value::<u64>(&properties, "flushDelayOffsetInterval").unwrap_err();
        assert!(error.to_string().contains("flushDelayOffsetInterval"));
    }
}

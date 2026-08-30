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

use std::future::Future;
use std::net::IpAddr;
use std::net::SocketAddr;

use cheetah_string::CheetahString;
use rand::seq::IndexedRandom;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
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
use rocketmq_protocol::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::route_facade::BrokerDataExt;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

use super::default_mq_admin_ext::DefaultMQAdminExt;

/// Sanitized allowlisted Broker fields required by supervised configuration
/// patching. Arbitrary Broker properties never cross this read boundary.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BrokerConfigAllowlisted {
    pub generation: u64,
    pub send_message_thread_pool_nums: Option<u32>,
    pub pull_message_thread_pool_nums: Option<u32>,
    pub flush_delay_offset_interval_ms: Option<u64>,
    pub max_client_event_count: Option<i32>,
}

/// Topic configuration paired with the Broker's monotonic metadata version.
#[derive(Clone, Debug, PartialEq)]
pub struct TopicConfigVersioned {
    pub version: u64,
    pub config: TopicConfig,
}

/// Subscription Group configuration paired with the Broker's monotonic
/// metadata version.
#[derive(Clone, Debug)]
pub struct SubscriptionGroupConfigVersioned {
    pub version: u64,
    pub config: SubscriptionGroupConfig,
}

/// Stable failure classification for one exact Broker read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ReadFailureCode {
    SourceUnavailable,
    Timeout,
    PermissionDenied,
    NotFound,
    RateLimited,
    InvalidResponse,
}

/// Address-free evidence for one failed Broker read.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct BrokerReadFailure {
    broker_name: String,
    code: ReadFailureCode,
    retryable: bool,
}

impl BrokerReadFailure {
    /// Creates failure evidence while reducing the target to a bounded logical
    /// Broker identifier.
    pub fn new(broker_name: impl AsRef<str>, code: ReadFailureCode, retryable: bool) -> Self {
        Self {
            broker_name: sanitize_broker_logical_target(broker_name.as_ref()),
            code,
            retryable,
        }
    }

    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub const fn code(&self) -> ReadFailureCode {
        self.code
    }

    pub const fn retryable(&self) -> bool {
        self.retryable
    }
}

/// Multi-Broker consumer statistics together with completeness evidence.
#[derive(Debug)]
pub struct ConsumeStatsReadResult {
    pub stats: ConsumeStats,
    pub attempted_brokers: usize,
    pub successful_brokers: usize,
    pub failures: Vec<BrokerReadFailure>,
}

/// Fixed, body-free metadata returned by the read-only message lookup.
///
/// Network addresses, arbitrary properties, body bytes, and body digests are
/// intentionally absent. Callers must pseudonymize identifier fields before
/// exposing the value outside their trusted read boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageMetadataRead {
    pub topic: String,
    pub message_id: String,
    pub unique_message_id: Option<String>,
    pub born_timestamp: i64,
    pub store_timestamp: i64,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub store_size: i32,
    pub reconsume_times: i32,
    pub sys_flag: i32,
    pub flag: i32,
    pub prepared_transaction_offset: i64,
}

#[allow(async_fn_in_trait)]
pub trait MQAdminReadExt: Send {
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()>;

    async fn shutdown(&mut self);

    async fn fetch_all_topic_list(&self) -> rocketmq_error::RocketMQResult<TopicList>;

    async fn fetch_broker_runtime_stats(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<KVTable>;

    /// Reads only the fixed non-sensitive Broker fields evaluated by the SRE
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

    /// Reads consumer statistics without discarding an individual Broker
    /// failure. Broker addresses and backend error strings never enter the
    /// returned evidence.
    async fn examine_consume_stats_with_evidence(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        broker_addr: Option<CheetahString>,
        timeout_millis: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<ConsumeStatsReadResult> {
        let stats = self
            .examine_consume_stats(consumer_group, topic, None, broker_addr, timeout_millis)
            .await?;
        Ok(ConsumeStatsReadResult {
            stats,
            attempted_brokers: 1,
            successful_brokers: 1,
            failures: Vec::new(),
        })
    }

    async fn examine_broker_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo>;

    async fn examine_topic_route_info(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>>;

    /// Reads one Broker's Topic configuration and metadata version atomically.
    async fn topic_config_with_version(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfigVersioned>;

    /// Reads one Broker's Subscription Group configuration and metadata
    /// version atomically.
    async fn subscription_group_config_with_version(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupConfigVersioned>;

    async fn examine_consumer_connection_info(
        &self,
        consumer_group: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<ConsumerConnection>;

    /// Reads one exact target without converting an authoritative empty
    /// connection set into a synthetic error or offline inference.
    async fn observe_consumer_connection_at(
        &self,
        consumer_group: CheetahString,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumerConnection>;

    async fn examine_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection>;

    /// Reads producer connections without converting an authoritative empty
    /// set into a fabricated status or an unavailable observation.
    async fn observe_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection>;

    /// Reads producer connections from one exact broker address. An empty
    /// connection set is authoritative and is returned unchanged.
    async fn observe_producer_connection_at(
        &self,
        producer_group: CheetahString,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection>;

    async fn get_all_producer_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerTableInfo>;

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> rocketmq_error::RocketMQResult<GroupList>;
}

/// Additive read-only capability for a NameServer Topic inventory.
///
/// This capability deliberately exposes only the cluster/global inventory
/// requests. It does not grant route, consumer, or mutation access.
#[allow(async_fn_in_trait)]
pub trait MQAdminTopicInventoryReadExt: Send {
    /// Fetches one cluster inventory when `cluster` is present, or the global
    /// NameServer inventory otherwise.
    async fn fetch_topic_inventory(&self, cluster: Option<CheetahString>) -> rocketmq_error::RocketMQResult<TopicList>;
}

/// Additive read-only capability for fixed, body-free message metadata.
#[allow(async_fn_in_trait)]
pub trait MQAdminMessageReadExt: Send {
    /// Looks up one message while returning only a fixed body-free metadata
    /// projection.
    async fn query_message_metadata(
        &self,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MessageMetadataRead>;
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
        let snapshot = self
            .inner()
            .mq_client_api()?
            .get_broker_config_snapshot(&broker_addr, self.inner().remoting_timeout_millis()?)
            .await?;
        let generation = snapshot.generation.filter(|value| *value > 0).ok_or_else(|| {
            rocketmq_error::RocketMQError::ResponseProcessFailed {
                operation: "get_broker_config_allowlisted",
                reason: "Broker config response does not include a positive config generation".to_owned(),
            }
        })?;
        let properties = snapshot.properties;
        Ok(BrokerConfigAllowlisted {
            generation,
            send_message_thread_pool_nums: parse_allowlisted_value(&properties, "sendMessageThreadPoolNums")?,
            pull_message_thread_pool_nums: parse_allowlisted_value(&properties, "pullMessageThreadPoolNums")?,
            flush_delay_offset_interval_ms: parse_allowlisted_value(&properties, "flushDelayOffsetInterval")?,
            max_client_event_count: parse_allowlisted_value(&properties, "maxClientEventCount")?,
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
                        topic_list: None,
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
                                topic_list: None,
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

    async fn examine_consume_stats_with_evidence(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        broker_addr: Option<CheetahString>,
        timeout_millis: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<ConsumeStatsReadResult> {
        let timeout = timeout_millis.unwrap_or(self.inner().remoting_timeout_millis()?);
        let topic = topic.unwrap_or_default();
        let targets = match broker_addr {
            Some(broker_addr) => vec![ConsumeStatsReadTarget::Ready {
                broker_name: "selected-broker".to_string(),
                address: broker_addr,
            }],
            None => {
                let retry_topic: CheetahString = mix_all::get_retry_topic(&consumer_group).into();
                let route = self
                    .inner()
                    .mq_client_api()?
                    .get_topic_route_info_from_name_server(&retry_topic, timeout)
                    .await?;
                consume_stats_read_targets(route.map(|route| route.broker_datas).unwrap_or_default())
            }
        };
        let mut result = ConsumeStatsReadResult {
            stats: ConsumeStats::new(),
            attempted_brokers: targets.len(),
            successful_brokers: 0,
            failures: Vec::new(),
        };
        for target in targets {
            let (broker_name, address) = match target {
                ConsumeStatsReadTarget::Ready { broker_name, address } => (broker_name, address),
                ConsumeStatsReadTarget::Failed(failure) => {
                    result.failures.push(failure);
                    continue;
                }
            };
            match self
                .inner()
                .mq_client_api()?
                .get_consume_stats(
                    &address,
                    GetConsumeStatsRequestHeader {
                        consumer_group: consumer_group.clone(),
                        topic: topic.clone(),
                        topic_list: None,
                        topic_request_header: None,
                    },
                    timeout,
                )
                .await
            {
                Ok(stats) => {
                    result.successful_brokers += 1;
                    result.stats.get_offset_table_mut().extend(stats.offset_table);
                    result
                        .stats
                        .set_consume_tps(result.stats.get_consume_tps() + stats.consume_tps);
                }
                Err(error) => result.failures.push(broker_read_failure(broker_name, &error)),
            }
        }
        result.failures.sort();
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

    async fn topic_config_with_version(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfigVersioned> {
        self.inner()
            .mq_client_api()?
            .get_topic_config_with_version(&broker_addr, topic, self.inner().remoting_timeout_millis()?)
            .await
    }

    async fn subscription_group_config_with_version(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupConfigVersioned> {
        self.inner()
            .mq_client_api()?
            .get_subscription_group_config_with_version(&broker_addr, group, self.inner().remoting_timeout_millis()?)
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

    async fn observe_consumer_connection_at(
        &self,
        consumer_group: CheetahString,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumerConnection> {
        self.inner()
            .mq_client_api()?
            .get_consumer_connection_list(
                broker_addr.as_str(),
                consumer_group,
                self.inner().remoting_timeout_millis()?,
            )
            .await
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

    async fn observe_producer_connection_info(
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
        let Some(broker_addr) = route.and_then(|route| {
            route
                .broker_datas
                .choose(&mut rand::rng())
                .and_then(BrokerDataExt::select_broker_addr)
        }) else {
            return Ok(ProducerConnection::new());
        };
        self.inner()
            .mq_client_api()?
            .get_producer_connection_list(broker_addr.as_str(), producer_group, timeout)
            .await
    }

    async fn observe_producer_connection_at(
        &self,
        producer_group: CheetahString,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection> {
        self.inner()
            .mq_client_api()?
            .get_producer_connection_list(
                broker_addr.as_str(),
                producer_group,
                self.inner().remoting_timeout_millis()?,
            )
            .await
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

impl MQAdminTopicInventoryReadExt for DefaultMQAdminExt {
    async fn fetch_topic_inventory(&self, cluster: Option<CheetahString>) -> rocketmq_error::RocketMQResult<TopicList> {
        let client_api = self.inner().mq_client_api()?;
        let timeout_millis = self.inner().remoting_timeout_millis()?;
        fetch_topic_inventory_from(
            cluster,
            |cluster| client_api.get_topics_by_cluster(cluster, timeout_millis),
            || client_api.get_all_topic_list_from_name_server(timeout_millis),
        )
        .await
    }
}

impl MQAdminMessageReadExt for DefaultMQAdminExt {
    async fn query_message_metadata(
        &self,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MessageMetadataRead> {
        MessageDecoder::validate_message_id(message_id.as_str())
            .map_err(|error| rocketmq_error::RocketMQError::IllegalArgument(format!("Invalid message ID: {error}")))?;
        let decoded = MessageDecoder::decode_message_id(message_id.as_str()).map_err(|error| {
            rocketmq_error::RocketMQError::IllegalArgument(format!("Failed to decode message ID: {error}"))
        })?;
        let broker_addr = CheetahString::from_string(format!("{}:{}", decoded.address.ip(), decoded.address.port()));
        let message = self
            .inner()
            .mq_client_api()?
            .view_message(
                &broker_addr,
                ViewMessageRequestHeader {
                    topic: Some(topic.clone()),
                    offset: decoded.offset,
                },
                self.inner().remoting_timeout_millis()?,
            )
            .await?;
        Ok(MessageMetadataRead {
            topic: topic.to_string(),
            message_id: message.msg_id().to_string(),
            unique_message_id: message
                .properties()
                .get(rocketmq_model::common::message::MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .map(ToString::to_string)
                .filter(|value| !value.is_empty()),
            born_timestamp: message.born_timestamp(),
            store_timestamp: message.store_timestamp(),
            queue_id: message.queue_id(),
            queue_offset: message.queue_offset(),
            store_size: message.store_size(),
            reconsume_times: message.reconsume_times(),
            sys_flag: message.sys_flag(),
            flag: message.flag(),
            prepared_transaction_offset: message.prepared_transaction_offset(),
        })
    }
}

async fn fetch_topic_inventory_from<ClusterFetch, ClusterFuture, GlobalFetch, GlobalFuture>(
    cluster: Option<CheetahString>,
    fetch_cluster: ClusterFetch,
    fetch_global: GlobalFetch,
) -> rocketmq_error::RocketMQResult<TopicList>
where
    ClusterFetch: FnOnce(CheetahString) -> ClusterFuture,
    ClusterFuture: Future<Output = rocketmq_error::RocketMQResult<TopicList>>,
    GlobalFetch: FnOnce() -> GlobalFuture,
    GlobalFuture: Future<Output = rocketmq_error::RocketMQResult<TopicList>>,
{
    match cluster {
        Some(cluster) => fetch_cluster(cluster).await,
        None => fetch_global().await,
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

#[derive(Debug, Eq, PartialEq)]
enum ConsumeStatsReadTarget {
    Ready {
        broker_name: String,
        address: CheetahString,
    },
    Failed(BrokerReadFailure),
}

fn consume_stats_read_targets(brokers: Vec<BrokerData>) -> Vec<ConsumeStatsReadTarget> {
    brokers
        .into_iter()
        .map(|broker| {
            let broker_name = broker.broker_name().to_string();
            match broker.broker_addrs().get(&mix_all::MASTER_ID).cloned() {
                Some(address) if !address.as_str().trim().is_empty() => {
                    ConsumeStatsReadTarget::Ready { broker_name, address }
                }
                Some(_) | None => ConsumeStatsReadTarget::Failed(BrokerReadFailure::new(
                    broker_name,
                    ReadFailureCode::InvalidResponse,
                    false,
                )),
            }
        })
        .collect()
}

fn broker_read_failure(broker_name: String, error: &rocketmq_error::RocketMQError) -> BrokerReadFailure {
    let view = error.boundary_view();
    let status = view.http().status.as_u16();
    let code = match status {
        401 | 403 => ReadFailureCode::PermissionDenied,
        404 => ReadFailureCode::NotFound,
        408 | 504 => ReadFailureCode::Timeout,
        429 => ReadFailureCode::RateLimited,
        400 | 413 | 422 => ReadFailureCode::InvalidResponse,
        _ => ReadFailureCode::SourceUnavailable,
    };
    BrokerReadFailure::new(broker_name, code, view.is_retryable())
}

fn sanitize_broker_logical_target(target: &str) -> String {
    const UNKNOWN_TARGET: &str = "unknown";
    const MAX_TARGET_BYTES: usize = 128;
    const SENSITIVE_OR_ERROR_MARKERS: &[&str] = &[
        "access_key",
        "accesskey",
        "authorization",
        "bearer",
        "credential",
        "denied",
        "error",
        "exception",
        "failed",
        "failure",
        "password",
        "passwd",
        "refused",
        "secret",
        "signature",
        "source_unavailable",
        "timed_out",
        "timeout",
        "token",
    ];

    let raw_target = target;
    let target = raw_target.trim();
    let lowercase = target.to_ascii_lowercase();
    let invalid = target.is_empty()
        || raw_target.len() > MAX_TARGET_BYTES
        || !target.is_ascii()
        || target.parse::<IpAddr>().is_ok()
        || target.parse::<SocketAddr>().is_ok()
        || target.contains([':', '/', '\\', '@', '=', '&', '?', '#'])
        || raw_target.chars().any(char::is_control)
        || !target
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
        || lowercase.starts_with("akia")
        || lowercase.starts_with("sk-")
        || SENSITIVE_OR_ERROR_MARKERS
            .iter()
            .any(|marker| lowercase.contains(marker));
    if invalid {
        UNKNOWN_TARGET.to_string()
    } else {
        target.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_model::common::mix_all;
    use rocketmq_protocol::protocol::body::topic::topic_list::TopicList;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

    use super::consume_stats_read_targets;
    use super::fetch_topic_inventory_from;
    use super::parse_allowlisted_value;
    use super::BrokerReadFailure;
    use super::ConsumeStatsReadTarget;
    use super::ReadFailureCode;

    fn route_broker(name: &str, master: Option<&str>) -> BrokerData {
        let broker_addrs = master
            .map(|address| HashMap::from([(mix_all::MASTER_ID, CheetahString::from_string(address.to_string()))]))
            .unwrap_or_default();
        BrokerData::new(
            CheetahString::from_static_str("cluster-a"),
            CheetahString::from_string(name.to_string()),
            broker_addrs,
            None,
        )
    }

    #[tokio::test]
    async fn cluster_topic_inventory_selects_exactly_one_cluster_source_call() {
        let cluster_calls = Arc::new(AtomicUsize::new(0));
        let global_calls = Arc::new(AtomicUsize::new(0));
        let result = fetch_topic_inventory_from(
            Some(CheetahString::from("cluster-a")),
            {
                let cluster_calls = Arc::clone(&cluster_calls);
                move |cluster| async move {
                    cluster_calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(cluster, CheetahString::from("cluster-a"));
                    Ok(TopicList::default())
                }
            },
            {
                let global_calls = Arc::clone(&global_calls);
                move || async move {
                    global_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(TopicList::default())
                }
            },
        )
        .await
        .unwrap();

        assert!(result.topic_list.is_empty());
        assert_eq!(cluster_calls.load(Ordering::SeqCst), 1);
        assert_eq!(global_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn global_topic_inventory_selects_exactly_one_global_source_call() {
        let cluster_calls = Arc::new(AtomicUsize::new(0));
        let global_calls = Arc::new(AtomicUsize::new(0));
        let result = fetch_topic_inventory_from(
            None,
            {
                let cluster_calls = Arc::clone(&cluster_calls);
                move |_| async move {
                    cluster_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(TopicList::default())
                }
            },
            {
                let global_calls = Arc::clone(&global_calls);
                move || async move {
                    global_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(TopicList::default())
                }
            },
        )
        .await
        .unwrap();

        assert!(result.topic_list.is_empty());
        assert_eq!(cluster_calls.load(Ordering::SeqCst), 0);
        assert_eq!(global_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn parses_only_the_requested_allowlisted_value() {
        let properties = HashMap::from([
            (
                CheetahString::from_static_str("sendMessageThreadPoolNums"),
                CheetahString::from_static_str("32"),
            ),
            (
                CheetahString::from_static_str("maxClientEventCount"),
                CheetahString::from_static_str("100"),
            ),
            (
                CheetahString::from_static_str("accessKey"),
                CheetahString::from_static_str("must-not-cross-boundary"),
            ),
        ]);

        let value = parse_allowlisted_value::<u32>(&properties, "sendMessageThreadPoolNums");
        assert_eq!(value.unwrap(), Some(32));
        assert_eq!(
            parse_allowlisted_value::<i32>(&properties, "maxClientEventCount").unwrap(),
            Some(100)
        );
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

    #[test]
    fn consume_stats_targets_preserve_mixed_valid_and_unusable_route_entries() {
        let targets = consume_stats_read_targets(vec![
            route_broker("broker-a", Some("127.0.0.1:10911")),
            route_broker("broker-b", None),
            route_broker("broker-c", Some("  ")),
        ]);

        assert_eq!(targets.len(), 3);
        assert!(matches!(
            &targets[0],
            ConsumeStatsReadTarget::Ready { broker_name, .. } if broker_name == "broker-a"
        ));
        for target in &targets[1..] {
            let ConsumeStatsReadTarget::Failed(failure) = target else {
                panic!("unusable master must remain a failed routed attempt");
            };
            assert_eq!(failure.code(), ReadFailureCode::InvalidResponse);
            assert!(!failure.retryable());
        }
    }

    #[test]
    fn consume_stats_targets_preserve_all_missing_masters_as_failures() {
        let targets = consume_stats_read_targets(vec![route_broker("broker-a", None), route_broker("broker-b", None)]);

        assert_eq!(targets.len(), 2);
        assert!(targets.iter().all(|target| matches!(
            target,
            ConsumeStatsReadTarget::Failed(failure)
                if failure.code() == ReadFailureCode::InvalidResponse && !failure.retryable()
        )));
    }

    #[test]
    fn consume_stats_targets_keep_zero_broker_route_authoritatively_empty() {
        assert!(consume_stats_read_targets(Vec::new()).is_empty());
    }

    #[test]
    fn broker_read_failure_preserves_valid_logical_targets() {
        let failure = BrokerReadFailure::new(" broker-a_1.prod ", ReadFailureCode::Timeout, true);

        assert_eq!(failure.broker_name(), "broker-a_1.prod");
        assert_eq!(failure.code(), ReadFailureCode::Timeout);
        assert!(failure.retryable());
    }

    #[test]
    fn broker_read_failure_replaces_unsafe_targets_with_one_safe_token() {
        let overlong = "a".repeat(129);
        let unsafe_targets = [
            "127.0.0.1",
            "::1",
            "127.0.0.1:10911",
            "[::1]:10911",
            "https://broker.example.invalid/runtime",
            "broker\nname",
            "broker-a\r",
            overlong.as_str(),
            "password-secret-value",
            "AKIAIOSFODNN7EXAMPLE",
            "connection_error_from_backend",
        ];

        for target in unsafe_targets {
            let failure = BrokerReadFailure::new(target, ReadFailureCode::SourceUnavailable, true);
            assert_eq!(
                failure.broker_name(),
                "unknown",
                "target should be rejected: {target:?}"
            );
        }
    }
}

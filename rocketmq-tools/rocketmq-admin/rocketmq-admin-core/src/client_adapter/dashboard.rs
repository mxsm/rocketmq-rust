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

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin_adapter_compat::error::RocketMQError;
use rocketmq_client_rust::MQAdminExt;
use rocketmq_model::message::MessageQueue;
use rocketmq_model::result::PullStatus;
use rocketmq_model::topic::TopicConfig;
use rocketmq_model::topic::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::version::value2version;
use rocketmq_protocol::common::wire_constants::MASTER_ID;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_protocol::protocol::body::acl_info::AclInfo;
use rocketmq_protocol::protocol::body::acl_info::PolicyEntryInfo;
use rocketmq_protocol::protocol::body::acl_info::PolicyInfo;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::kv_table::KVTable;
use rocketmq_protocol::protocol::body::producer_connection::ProducerConnection;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::dashboard;
use crate::core::AdminError;
use crate::core::AdminFuture;

const DEFAULT_PAGE_SIZE: usize = 2_000;
const MAX_PAGE_SIZE: usize = 2_000;
const PULL_BATCH_SIZE: i64 = 32;
const PULL_TIMEOUT_MILLIS: u64 = 5_000;

#[derive(Debug, Clone)]
struct AclTarget {
    broker_name: String,
    broker_addr: String,
}

mod broker;
mod consumer;
mod message;
mod producer;
mod security;
mod topic;

use self::broker::*;
use self::consumer::*;
use self::producer::*;
use self::security::*;
use self::topic::*;

impl dashboard::DashboardAdmin for AdminSession {
    fn dashboard_list_topics(&mut self) -> AdminFuture<'_, dashboard::DashboardTopicList> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic_list = self
                .inner
                .fetch_all_topic_list()
                .await
                .map_err(|error| backend_error("fetch_all_topic_list", error))?;
            let mut items = Vec::with_capacity(topic_list.topic_list.len());
            for topic in topic_list.topic_list {
                let topic = topic.to_string();
                let route = self
                    .inner
                    .examine_topic_route_info(CheetahString::from(topic.as_str()))
                    .await
                    .ok()
                    .flatten()
                    .map(|route| map_topic_route(&topic, &route));
                items.push(topic_info_from_route(&topic, route.as_ref()));
            }
            items.sort_by(|left, right| left.topic.cmp(&right.topic));
            Ok(dashboard::DashboardTopicList { items })
        })
    }

    fn dashboard_topic_route<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, dashboard::DashboardTopicRoute> {
        Box::pin(async move {
            self.ensure_open()?;
            let route = self
                .inner
                .examine_topic_route_info(CheetahString::from(topic))
                .await
                .map_err(|error| backend_error("examine_topic_route_info", error))?
                .ok_or_else(|| AdminError::not_found("topic", topic))?;
            Ok(map_topic_route(topic, &route))
        })
    }

    fn dashboard_topic_stats<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, dashboard::DashboardTopicStats> {
        Box::pin(async move {
            self.ensure_open()?;
            let stats = self
                .inner
                .examine_topic_stats(CheetahString::from(topic), None)
                .await
                .map_err(|error| backend_error("examine_topic_stats", error))?;
            Ok(map_topic_stats(topic, &stats))
        })
    }

    fn dashboard_upsert_topic<'a>(
        &'a mut self,
        request: &'a dashboard::DashboardTopicMutationRequest,
    ) -> AdminFuture<'a, dashboard::AdminMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let mut target_addrs = HashSet::new();
            let mut target_broker_names = HashSet::new();
            for cluster_name in &request.cluster_name_list {
                for (broker_name, broker_addr) in master_targets_by_cluster_name(&cluster_info, cluster_name)? {
                    target_broker_names.insert(broker_name);
                    target_addrs.insert(broker_addr);
                }
            }
            for broker_name in &request.broker_name_list {
                let broker_addr = find_master_addr_by_broker_name(&cluster_info, broker_name).ok_or_else(|| {
                    AdminError::invalid_argument("brokerName", format!("broker `{broker_name}` was not found"))
                })?;
                target_broker_names.insert(broker_name.clone());
                target_addrs.insert(broker_addr);
            }
            if target_addrs.is_empty() {
                return Err(AdminError::invalid_argument(
                    "targets",
                    "no writable broker target could be resolved",
                ));
            }

            let mut attributes = HashMap::new();
            attributes.insert(
                CheetahString::from_static_str("+message.type"),
                CheetahString::from(normalize_message_type(request.message_type.as_deref())),
            );
            let topic_config = TopicConfig {
                topic_name: Some(CheetahString::from(request.topic.as_str())),
                read_queue_nums: request.read_queue_count.max(1),
                write_queue_nums: request.write_queue_count.max(1),
                perm: request.perm,
                order: request.order,
                attributes,
                ..TopicConfig::default()
            };
            for broker_addr in &target_addrs {
                self.inner
                    .create_and_update_topic_config(broker_addr.clone(), topic_config.clone())
                    .await
                    .map_err(|error| backend_error("create_and_update_topic_config", error))?;
            }
            if request.order {
                let order_conf = build_order_conf(&target_broker_names, topic_config.write_queue_nums);
                self.inner
                    .create_or_update_order_conf(request.topic.as_str().into(), order_conf.into(), true)
                    .await
                    .map_err(|error| backend_error("create_or_update_order_conf", error))?;
            }
            Ok(dashboard::AdminMutationResult {
                message: format!(
                    "Topic `{}` was saved on {} broker target(s)",
                    request.topic,
                    target_addrs.len()
                ),
                target_count: target_addrs.len(),
            })
        })
    }

    fn dashboard_delete_topic<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, dashboard::AdminMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let route = self
                .inner
                .examine_topic_route_info(CheetahString::from(topic))
                .await
                .map_err(|error| backend_error("examine_topic_route_info", error))?
                .ok_or_else(|| AdminError::not_found("topic", topic))?;
            let mut clusters = route
                .broker_datas
                .iter()
                .map(|broker| broker.cluster().to_string())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            clusters.sort();
            validate_delete_clusters(topic, &clusters)?;
            for cluster_name in &clusters {
                self.inner
                    .delete_topic(topic.into(), cluster_name.as_str().into())
                    .await
                    .map_err(|error| backend_error("delete_topic", error))?;
            }
            Ok(dashboard::AdminMutationResult {
                message: format!("Topic `{topic}` was deleted from {} cluster(s)", clusters.len()),
                target_count: clusters.len(),
            })
        })
    }

    fn dashboard_list_consumers(&mut self) -> AdminFuture<'_, dashboard::DashboardConsumerList> {
        Box::pin(async move {
            self.ensure_open()?;
            let broker_addrs = broker_addresses(&self.inner).await?;
            let mut groups = BTreeSet::new();
            for address in broker_addrs {
                if let Ok(wrapper) = self
                    .inner
                    .get_all_subscription_group(CheetahString::from(address.as_str()), 5_000)
                    .await
                {
                    for entry in wrapper.get_subscription_group_table().iter() {
                        groups.insert(entry.key().to_string());
                    }
                }
            }
            let mut items = Vec::new();
            for group in groups {
                let stats = self
                    .inner
                    .examine_consume_stats(CheetahString::from(group.as_str()), None, None, None, None)
                    .await
                    .ok();
                let connection = self
                    .inner
                    .examine_consumer_connection_info(CheetahString::from(group.as_str()), None)
                    .await
                    .ok();
                let diff_total = stats.as_ref().map(ConsumeStats::compute_total_diff).unwrap_or_default();
                let (consume_type, message_model, client_count) = connection
                    .as_ref()
                    .map(|connection| {
                        (
                            connection
                                .get_consume_type()
                                .map(|value| value.to_string())
                                .unwrap_or_default(),
                            connection
                                .get_message_model()
                                .map(|value| value.to_string())
                                .unwrap_or_default(),
                            connection.get_connection_set().len(),
                        )
                    })
                    .unwrap_or_default();
                items.push(dashboard::DashboardConsumerGroup {
                    group,
                    consume_type,
                    message_model,
                    client_count,
                    diff_total,
                });
            }
            items.sort_by(|left, right| left.group.cmp(&right.group));
            Ok(dashboard::DashboardConsumerList { items })
        })
    }

    fn dashboard_consumer_progress<'a>(
        &'a mut self,
        group: &'a str,
    ) -> AdminFuture<'a, dashboard::DashboardConsumerProgress> {
        Box::pin(async move {
            self.ensure_open()?;
            let stats = self
                .inner
                .examine_consume_stats(CheetahString::from(group), None, None, None, None)
                .await
                .map_err(|error| backend_error("examine_consume_stats", error))?;
            Ok(map_consumer_progress(group, &stats))
        })
    }

    fn dashboard_reset_consumer<'a>(
        &'a mut self,
        request: &'a dashboard::DashboardConsumerResetRequest,
    ) -> AdminFuture<'a, dashboard::AdminMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let offsets = self
                .inner
                .reset_offset_by_timestamp(
                    None,
                    CheetahString::from(request.topic.as_str()),
                    CheetahString::from(request.group.as_str()),
                    request.reset_timestamp,
                    request.force,
                )
                .await
                .map_err(|error| backend_error("reset_offset_by_timestamp", error))?;
            Ok(dashboard::AdminMutationResult {
                message: format!(
                    "Consumer group `{}` offset was reset for {} queue(s)",
                    request.group,
                    offsets.len()
                ),
                target_count: offsets.len(),
            })
        })
    }

    fn dashboard_list_producers(&mut self) -> AdminFuture<'_, Vec<dashboard::DashboardProducerInfo>> {
        Box::pin(async move {
            self.ensure_open()?;
            let mut producer_counts: HashMap<String, usize> = HashMap::new();
            for address in broker_addresses(&self.inner).await? {
                if let Ok(table) = self
                    .inner
                    .get_all_producer_info(CheetahString::from(address.as_str()))
                    .await
                {
                    for (group, producers) in table.data() {
                        *producer_counts.entry(group.clone()).or_default() += producers.len();
                    }
                }
            }
            let mut items = producer_counts
                .into_iter()
                .map(|(producer_group, connection_count)| dashboard::DashboardProducerInfo {
                    topic: String::new(),
                    producer_group,
                    connection_count,
                })
                .collect::<Vec<_>>();
            items.sort_by(|left, right| left.producer_group.cmp(&right.producer_group));
            Ok(items)
        })
    }

    fn dashboard_producer_connections<'a>(
        &'a mut self,
        topic: &'a str,
        producer_group: &'a str,
    ) -> AdminFuture<'a, dashboard::DashboardProducerConnections> {
        Box::pin(async move {
            self.ensure_open()?;
            let connection = self
                .inner
                .examine_producer_connection_info(CheetahString::from(producer_group), CheetahString::from(topic))
                .await
                .map_err(|error| backend_error("examine_producer_connection_info", error))?;
            Ok(map_producer_connection(topic, producer_group, &connection))
        })
    }

    fn dashboard_list_brokers(&mut self) -> AdminFuture<'_, dashboard::DashboardBrokerList> {
        Box::pin(async move {
            self.ensure_open()?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let mut items = Vec::new();
            let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() else {
                return Ok(dashboard::DashboardBrokerList {
                    clusters: Vec::new(),
                    items,
                });
            };
            let mut clusters = cluster_addr_table.keys().map(ToString::to_string).collect::<Vec<_>>();
            clusters.sort();
            let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() else {
                return Ok(dashboard::DashboardBrokerList { clusters, items });
            };
            let mut cluster_names = cluster_addr_table.keys().cloned().collect::<Vec<_>>();
            cluster_names.sort();
            for cluster_name in cluster_names {
                let Some(broker_names) = cluster_addr_table.get(cluster_name.as_str()) else {
                    continue;
                };
                let mut broker_names = broker_names.iter().cloned().collect::<Vec<_>>();
                broker_names.sort();
                for broker_name in broker_names {
                    let Some(broker_data) = broker_addr_table.get(broker_name.as_str()) else {
                        continue;
                    };
                    let mut broker_addrs = broker_data
                        .broker_addrs()
                        .iter()
                        .map(|(broker_id, address)| (*broker_id, address.clone()))
                        .collect::<Vec<_>>();
                    broker_addrs.sort_by_key(|(broker_id, _)| *broker_id);
                    for (broker_id, address) in broker_addrs {
                        let (entries, runtime_error) =
                            match self.inner.fetch_broker_runtime_stats(address.clone()).await {
                                Ok(runtime) => (kv_table_to_map(&runtime), None),
                                Err(error) => (BTreeMap::new(), Some(error.to_string())),
                            };
                        items.push(dashboard::DashboardBrokerInfo {
                            cluster_name: cluster_name.to_string(),
                            broker_name: broker_name.to_string(),
                            broker_id,
                            address: address.to_string(),
                            role: if broker_id == MASTER_ID { "MASTER" } else { "SLAVE" }.to_string(),
                            version: entries.get("brokerVersionDesc").cloned().unwrap_or_default(),
                            produce_tps: parse_rate_value(entries.get("putTps")),
                            consume_tps: parse_rate_value(select_consume_tps_value(&entries)),
                            runtime_entries: entries,
                            runtime_error,
                        });
                    }
                }
            }
            items.sort_by(|left, right| {
                left.cluster_name
                    .cmp(&right.cluster_name)
                    .then(left.broker_name.cmp(&right.broker_name))
                    .then(left.broker_id.cmp(&right.broker_id))
            });
            Ok(dashboard::DashboardBrokerList { clusters, items })
        })
    }

    fn dashboard_broker_runtime<'a>(
        &'a mut self,
        target: &'a dashboard::DashboardBrokerTarget,
    ) -> AdminFuture<'a, dashboard::DashboardBrokerRuntime> {
        Box::pin(async move {
            self.ensure_open()?;
            let address = match target.broker_addr.as_deref() {
                Some(address) => address.to_string(),
                None => resolve_broker_address(&self.inner, target.broker_name.as_str()).await?,
            };
            let table = self
                .inner
                .fetch_broker_runtime_stats(CheetahString::from(address.as_str()))
                .await
                .map_err(|error| backend_error("fetch_broker_runtime_stats", error))?;
            Ok(dashboard::DashboardBrokerRuntime {
                broker_name: target.broker_name.clone(),
                address,
                entries: kv_table_to_map(&table),
            })
        })
    }

    fn dashboard_broker_config<'a>(
        &'a mut self,
        target: &'a dashboard::DashboardBrokerTarget,
    ) -> AdminFuture<'a, dashboard::DashboardBrokerConfig> {
        Box::pin(async move {
            self.ensure_open()?;
            let address = match target.broker_addr.as_deref() {
                Some(address) => address.to_string(),
                None => resolve_broker_address(&self.inner, target.broker_name.as_str()).await?,
            };
            let entries = self
                .inner
                .get_broker_config(CheetahString::from(address.as_str()))
                .await
                .map_err(|error| backend_error("get_broker_config", error))?
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect();
            Ok(dashboard::DashboardBrokerConfig {
                broker_name: target.broker_name.clone(),
                address,
                entries,
            })
        })
    }

    fn dashboard_update_broker_config<'a>(
        &'a mut self,
        request: &'a dashboard::DashboardBrokerConfigUpdateRequest,
    ) -> AdminFuture<'a, dashboard::AdminMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let address = match request.broker_addr.as_deref() {
                Some(address) => address.to_string(),
                None => resolve_broker_address(&self.inner, request.broker_name.as_str()).await?,
            };
            let properties = request
                .entries
                .iter()
                .map(|(key, value)| (CheetahString::from(key.as_str()), CheetahString::from(value.as_str())))
                .collect();
            self.inner
                .update_broker_config(CheetahString::from(address.as_str()), properties)
                .await
                .map_err(|error| backend_error("update_broker_config", error))?;
            Ok(dashboard::AdminMutationResult {
                message: format!("Broker `{}` config was updated at `{address}`", request.broker_name),
                target_count: 1,
            })
        })
    }

    fn dashboard_list_acl_users<'a>(
        &'a mut self,
        query: &'a dashboard::DashboardAclQuery,
    ) -> AdminFuture<'a, Vec<dashboard::DashboardAclUser>> {
        Box::pin(async move {
            self.ensure_open()?;
            let targets = resolve_acl_targets(&self.inner, &query.selector).await?;
            let mut users = Vec::new();
            for target in targets {
                let user_infos = self
                    .inner
                    .list_users(
                        CheetahString::from(target.broker_addr.as_str()),
                        CheetahString::from(query.search.as_str()),
                    )
                    .await
                    .map_err(|error| backend_error("list_users", error))?;
                users.extend(user_infos.iter().map(|user| map_acl_user(&target, user)));
            }
            users.sort_by(|left, right| {
                left.username
                    .cmp(&right.username)
                    .then(left.broker_name.cmp(&right.broker_name))
                    .then(left.broker_addr.cmp(&right.broker_addr))
            });
            Ok(users)
        })
    }

    fn dashboard_create_acl_user<'a>(
        &'a mut self,
        request: &'a dashboard::DashboardAclUserMutationRequest,
    ) -> AdminFuture<'a, dashboard::AdminMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let targets = resolve_acl_targets(&self.inner, &request.selector).await?;
            let target_count = targets.len();
            for target in targets {
                self.inner
                    .create_user(
                        target.broker_addr.as_str().into(),
                        request.username.as_str().into(),
                        request.password.as_str().into(),
                        request.user_type.as_str().into(),
                    )
                    .await
                    .map_err(|error| backend_error("create_user", error))?;
            }
            Ok(dashboard::AdminMutationResult {
                message: format!(
                    "Created ACL user {} on {target_count} broker target(s)",
                    request.username
                ),
                target_count,
            })
        })
    }

    fn dashboard_update_acl_user<'a>(
        &'a mut self,
        request: &'a dashboard::DashboardAclUserMutationRequest,
    ) -> AdminFuture<'a, dashboard::AdminMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let targets = resolve_acl_targets(&self.inner, &request.selector).await?;
            let target_count = targets.len();
            let user_status = request.user_status.as_deref().unwrap_or_default();
            for target in targets {
                self.inner
                    .update_user(
                        target.broker_addr.as_str().into(),
                        request.username.as_str().into(),
                        request.password.as_str().into(),
                        request.user_type.as_str().into(),
                        user_status.into(),
                    )
                    .await
                    .map_err(|error| backend_error("update_user", error))?;
            }
            Ok(dashboard::AdminMutationResult {
                message: format!(
                    "Updated ACL user {} on {target_count} broker target(s)",
                    request.username
                ),
                target_count,
            })
        })
    }

    fn dashboard_delete_acl_user<'a>(
        &'a mut self,
        selector: &'a dashboard::TargetSelector,
        username: &'a str,
    ) -> AdminFuture<'a, dashboard::AdminMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let targets = resolve_acl_targets(&self.inner, selector).await?;
            let target_count = targets.len();
            for target in targets {
                self.inner
                    .delete_user(target.broker_addr.as_str().into(), username.into())
                    .await
                    .map_err(|error| backend_error("delete_user", error))?;
            }
            Ok(dashboard::AdminMutationResult {
                message: format!("Deleted ACL user {username} from {target_count} broker target(s)"),
                target_count,
            })
        })
    }

    fn dashboard_list_acl_policies<'a>(
        &'a mut self,
        query: &'a dashboard::DashboardAclQuery,
    ) -> AdminFuture<'a, Vec<dashboard::DashboardAclPolicy>> {
        Box::pin(async move {
            self.ensure_open()?;
            let targets = resolve_acl_targets(&self.inner, &query.selector).await?;
            let mut policies = Vec::new();
            for target in targets {
                let subject_matches = self
                    .inner
                    .list_acl(
                        target.broker_addr.as_str().into(),
                        query.search.as_str().into(),
                        CheetahString::default(),
                    )
                    .await
                    .map_err(|error| backend_error("list_acl", error))?;
                let resource_matches = if query.resource.is_empty() && query.search.is_empty() {
                    Vec::new()
                } else {
                    self.inner
                        .list_acl(
                            target.broker_addr.as_str().into(),
                            CheetahString::default(),
                            if query.resource.is_empty() {
                                query.search.as_str().into()
                            } else {
                                query.resource.as_str().into()
                            },
                        )
                        .await
                        .map_err(|error| backend_error("list_acl", error))?
                };
                let mut all = subject_matches;
                all.extend(resource_matches);
                policies.extend(all.iter().flat_map(|acl| map_acl_policy(&target, acl)));
            }
            policies.sort_by(|left, right| {
                left.subject
                    .cmp(&right.subject)
                    .then(left.policy_type.cmp(&right.policy_type))
                    .then(left.broker_name.cmp(&right.broker_name))
            });
            policies.dedup_by(|left, right| {
                left.broker_addr == right.broker_addr
                    && left.subject == right.subject
                    && left.policy_type == right.policy_type
                    && left.entries == right.entries
            });
            Ok(policies)
        })
    }

    fn dashboard_create_acl_policy<'a>(
        &'a mut self,
        request: &'a dashboard::DashboardAclPolicyMutationRequest,
    ) -> AdminFuture<'a, dashboard::AdminMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let acl_info = build_acl_info(request)?;
            let targets = resolve_acl_targets(&self.inner, &request.selector).await?;
            let target_count = targets.len();
            for target in targets {
                self.inner
                    .create_acl_with_acl_info(target.broker_addr.as_str().into(), acl_info.clone())
                    .await
                    .map_err(|error| backend_error("create_acl", error))?;
            }
            Ok(dashboard::AdminMutationResult {
                message: format!(
                    "Created ACL policy for {} on {target_count} broker target(s)",
                    request.subject
                ),
                target_count,
            })
        })
    }

    fn dashboard_update_acl_policy<'a>(
        &'a mut self,
        request: &'a dashboard::DashboardAclPolicyMutationRequest,
    ) -> AdminFuture<'a, dashboard::AdminMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let acl_info = build_acl_info(request)?;
            let targets = resolve_acl_targets(&self.inner, &request.selector).await?;
            let target_count = targets.len();
            for target in targets {
                self.inner
                    .update_acl_with_acl_info(target.broker_addr.as_str().into(), acl_info.clone())
                    .await
                    .map_err(|error| backend_error("update_acl", error))?;
            }
            Ok(dashboard::AdminMutationResult {
                message: format!(
                    "Updated ACL policy for {} on {target_count} broker target(s)",
                    request.subject
                ),
                target_count,
            })
        })
    }

    fn dashboard_delete_acl_policy<'a>(
        &'a mut self,
        selector: &'a dashboard::TargetSelector,
        subject: &'a str,
        resource: &'a str,
    ) -> AdminFuture<'a, dashboard::AdminMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let targets = resolve_acl_targets(&self.inner, selector).await?;
            let target_count = targets.len();
            for target in targets {
                self.inner
                    .delete_acl(target.broker_addr.as_str().into(), subject.into(), resource.into())
                    .await
                    .map_err(|error| backend_error("delete_acl", error))?;
            }
            Ok(dashboard::AdminMutationResult {
                message: format!("Deleted ACL subject {subject} from {target_count} broker target(s)"),
                target_count,
            })
        })
    }

    fn dashboard_query_messages<'a>(
        &'a mut self,
        request: &'a dashboard::DashboardMessageQuery,
    ) -> AdminFuture<'a, dashboard::DashboardMessageList> {
        message::query(self, request)
    }

    fn dashboard_query_dlq_messages<'a>(
        &'a mut self,
        request: &'a dashboard::DashboardDlqMessageQuery,
    ) -> AdminFuture<'a, dashboard::DashboardMessageList> {
        message::query_dlq(self, request)
    }

    fn dashboard_message_trace<'a>(
        &'a mut self,
        topic: &'a str,
        message_id: &'a str,
        trace_topic: &'a str,
    ) -> AdminFuture<'a, dashboard::DashboardMessageTrace> {
        message::trace(self, topic, message_id, trace_topic)
    }

    fn dashboard_consume_message_directly<'a>(
        &'a mut self,
        request: &'a dashboard::DashboardDirectConsumeRequest,
    ) -> AdminFuture<'a, dashboard::AdminMutationResult> {
        message::consume_directly(self, request)
    }
}

fn backend_error(operation: &'static str, error: RocketMQError) -> AdminError {
    let view = error.boundary_view();
    let context = (!view.context().is_empty()).then(|| view.context().to_string());
    AdminError::backend_view(
        operation,
        view.code().as_str(),
        view.message(),
        context,
        view.http().status.as_u16(),
        view.is_retryable(),
    )
}

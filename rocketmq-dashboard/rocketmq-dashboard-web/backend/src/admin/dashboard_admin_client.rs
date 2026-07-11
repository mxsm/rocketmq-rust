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
use crate::error::DashboardError;
use crate::model::AclMutationResult;
use crate::model::AclPolicyEntryView;
use crate::model::AclPolicyRequest;
use crate::model::AclPolicyView;
use crate::model::AclQuery;
use crate::model::AclUserUpsertRequest;
use crate::model::AclUserView;
use crate::model::BrokerConfigUpdateRequest;
use crate::model::BrokerConfigView;
use crate::model::BrokerInfo;
use crate::model::BrokerListView;
use crate::model::BrokerRuntimeStats;
use crate::model::ConsumerGroupInfo;
use crate::model::ConsumerListView;
use crate::model::ConsumerProgress;
use crate::model::ConsumerQueueProgress;
use crate::model::ConsumerResetOffsetRequest;
use crate::model::DashboardConfigView;
use crate::model::DashboardOverview;
use crate::model::DashboardTopicCurrent;
use crate::model::DlqBatchResendRequest;
use crate::model::DlqExportView;
use crate::model::DlqMessageQuery;
use crate::model::DlqMessageResendResult;
use crate::model::MessageListView;
use crate::model::MessageResendRequest;
use crate::model::MessageTraceNode;
use crate::model::MessageTraceView;
use crate::model::MessageView;
use crate::model::MutationResult;
use crate::model::ProducerConnectionInfo;
use crate::model::ProducerConnectionView;
use crate::model::ProducerInfo;
use crate::model::TopicCurrentMetric;
use crate::model::TopicInfo;
use crate::model::TopicListView;
use crate::model::TopicMutationRequest;
use crate::model::TopicRouteBroker;
use crate::model::TopicRouteInfo;
use crate::model::TopicRouteQueue;
use crate::model::TopicStatsInfo;
use cheetah_string::CheetahString;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_client_rust::consumer::pull_status::PullStatus;
use rocketmq_common::common::attribute::Attribute;
use rocketmq_common::common::attribute::topic_attributes::TopicAttributes;
use rocketmq_common::common::config::TopicConfig as RocketMqTopicConfig;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::protocol::body::acl_info::PolicyEntryInfo;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
use rocketmq_remoting::protocol::body::user_info::UserInfo;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

const DLQ_DEFAULT_PAGE_SIZE: usize = 32;
const DLQ_MAX_PAGE_SIZE: usize = 200;
const DLQ_PULL_BATCH_SIZE: i64 = 32;
const MESSAGE_DEFAULT_PAGE_SIZE: usize = 2_000;
const MESSAGE_MAX_PAGE_SIZE: usize = 2_000;
const MESSAGE_PULL_BATCH_SIZE: i64 = 32;
const PULL_TIMEOUT_MILLIS: u64 = 5_000;

#[derive(Clone)]
pub struct DashboardAdminClient {
    config: Arc<RwLock<DashboardConfigView>>,
    admin_session: Arc<Mutex<Option<ManagedAdminSession>>>,
}

struct ManagedAdminSession {
    admin: DefaultMQAdminExt,
    snapshot: AdminConfigSnapshot,
}

#[derive(Debug, Clone)]
struct AclTarget {
    broker_name: String,
    broker_addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AdminConfigSnapshot {
    namesrv_addr: String,
    use_vip_channel: bool,
    use_tls: bool,
}

impl DashboardAdminClient {
    pub fn new(config: Arc<RwLock<DashboardConfigView>>) -> Self {
        Self {
            config,
            admin_session: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn dashboard_overview(&self) -> Result<DashboardOverview, DashboardError> {
        let config = self.config.read().await.clone();
        if config.current_namesrv.is_none() {
            return Ok(DashboardOverview {
                current_namesrv: None,
                broker_count: 0,
                topic_count: 0,
                consumer_group_count: 0,
                producer_count: 0,
                message_backlog: 0,
                system_status: "UNCONFIGURED".to_string(),
            });
        }

        let counts_result: Result<_, DashboardError> = async {
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;
            let session = session_guard
                .as_mut()
                .expect("admin session should be initialized before use");

            let brokers = self.list_brokers_with_admin(&mut session.admin).await?;
            let topics = session.admin.fetch_all_topic_list().await.map_err(map_rocketmq_error)?;
            let consumers = self
                .list_consumer_groups_with_admin(&mut session.admin)
                .await
                .unwrap_or_else(|_| ConsumerListView {
                    items: Vec::new(),
                    total: 0,
                });
            let producers = self
                .list_producers_with_admin(&mut session.admin)
                .await
                .unwrap_or_default();
            let message_backlog = consumers.items.iter().map(|item| item.diff_total).sum();

            Ok((
                brokers.total,
                topics.topic_list.len(),
                consumers.total,
                producers.len(),
                message_backlog,
            ))
        }
        .await;
        let (broker_count, topic_count, consumer_group_count, producer_count, message_backlog, system_status) =
            match counts_result {
                Ok((broker_count, topic_count, consumer_group_count, producer_count, message_backlog)) => (
                    broker_count,
                    topic_count,
                    consumer_group_count,
                    producer_count,
                    message_backlog,
                    "UP",
                ),
                Err(DashboardError::RocketMq(error)) => {
                    tracing::warn!(error = %error, "RocketMQ cluster is not reachable while building dashboard overview");
                    (0, 0, 0, 0, 0, "DOWN")
                }
                Err(error) => return Err(error),
            };

        Ok(DashboardOverview {
            current_namesrv: config.current_namesrv,
            broker_count,
            topic_count,
            consumer_group_count,
            producer_count,
            message_backlog,
            system_status: system_status.to_string(),
        })
    }

    pub async fn topic_current(&self) -> Result<DashboardTopicCurrent, DashboardError> {
        let topics = match self.list_topics().await {
            Ok(topics) => topics,
            Err(DashboardError::RocketMq(error)) => {
                tracing::warn!(error = %error, "RocketMQ cluster is not reachable while building topic-current metrics");
                return Ok(DashboardTopicCurrent {
                    total_topics: 0,
                    top_topics: Vec::new(),
                });
            }
            Err(error) => return Err(error),
        };
        let mut top_topics = Vec::new();
        {
            let mut session_guard = self.admin_session.lock().await;
            if let Err(error) = self.ensure_admin_session(&mut session_guard).await {
                return match error {
                    DashboardError::RocketMq(error) => {
                        tracing::warn!(error = %error, "RocketMQ cluster is not reachable while loading topic stats");
                        Ok(DashboardTopicCurrent {
                            total_topics: topics.total,
                            top_topics: Vec::new(),
                        })
                    }
                    error => Err(error),
                };
            }
            let session = session_guard
                .as_mut()
                .expect("admin session should be initialized before use");

            for topic in topics.items.iter().take(20) {
                if let Ok(stats) = session
                    .admin
                    .examine_topic_stats(CheetahString::from(topic.topic.as_str()), None)
                    .await
                {
                    let total_msg = stats
                        .get_offset_table()
                        .values()
                        .map(|offset| offset.get_max_offset().saturating_sub(offset.get_min_offset()))
                        .sum();
                    top_topics.push(TopicCurrentMetric {
                        topic: topic.topic.clone(),
                        total_msg,
                        in_tps: 0.0,
                        out_tps: 0.0,
                    });
                }
            }
        }
        top_topics.sort_by_key(|topic| Reverse(topic.total_msg));

        Ok(DashboardTopicCurrent {
            total_topics: topics.total,
            top_topics,
        })
    }

    pub async fn list_topics(&self) -> Result<TopicListView, DashboardError> {
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");

        let topic_list = session.admin.fetch_all_topic_list().await.map_err(map_rocketmq_error)?;
        let mut topics = Vec::with_capacity(topic_list.topic_list.len());
        for topic in &topic_list.topic_list {
            let topic = topic.to_string();
            let route = match session
                .admin
                .examine_topic_route_info(CheetahString::from(topic.as_str()))
                .await
            {
                Ok(Some(route)) => Some(map_topic_route(&topic, &route)),
                Ok(None) => None,
                Err(error) => {
                    tracing::warn!(
                        topic,
                        error = %error,
                        "Failed to load topic route while building topic list; using name-only metadata"
                    );
                    None
                }
            };
            topics.push(topic_info_from_route(&topic, route.as_ref()));
        }
        topics.sort_by(|left, right| left.topic.cmp(&right.topic));

        Ok(TopicListView {
            total: topics.len(),
            items: topics,
        })
    }

    pub async fn get_topic(&self, topic: &str) -> Result<TopicInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let route = self.topic_route(topic).await?;
        Ok(topic_info_from_route(topic, Some(&route)))
    }

    pub async fn topic_route(&self, topic: &str) -> Result<TopicRouteInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");

        let route = session
            .admin
            .examine_topic_route_info(CheetahString::from(topic))
            .await
            .map_err(map_rocketmq_error)?
            .ok_or_else(|| DashboardError::NotFound(format!("Topic `{topic}` was not found")))?;

        Ok(map_topic_route(topic, &route))
    }

    pub async fn topic_stats(&self, topic: &str) -> Result<TopicStatsInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");

        let stats = session
            .admin
            .examine_topic_stats(CheetahString::from(topic), None)
            .await
            .map_err(map_rocketmq_error)?;

        Ok(map_topic_stats(topic, &stats))
    }

    pub async fn create_or_update_topic(
        &self,
        request: TopicMutationRequest,
    ) -> Result<MutationResult, DashboardError> {
        validate_name(&request.topic, "Topic")?;
        if request.cluster_name_list.is_empty() && request.broker_name_list.is_empty() {
            return Err(DashboardError::Validation(
                "Select at least one cluster or broker before saving the topic".to_string(),
            ));
        }

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");

        let cluster_info = session
            .admin
            .examine_broker_cluster_info()
            .await
            .map_err(map_rocketmq_error)?;
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
                DashboardError::Validation(format!(
                    "Broker `{broker_name}` was not found in the current cluster view"
                ))
            })?;
            target_broker_names.insert(broker_name.clone());
            target_addrs.insert(broker_addr);
        }

        if target_addrs.is_empty() {
            return Err(DashboardError::Validation(
                "No writable broker target could be resolved".to_string(),
            ));
        }

        let mut attributes = HashMap::new();
        attributes.insert(
            format!("+{}", TopicAttributes::topic_message_type_attribute().name()).into(),
            normalize_message_type(request.message_type.as_deref()).into(),
        );
        let topic_config = RocketMqTopicConfig {
            topic_name: Some(request.topic.clone().into()),
            read_queue_nums: request.read_queue_count.max(1),
            write_queue_nums: request.write_queue_count.max(1),
            perm: request.perm,
            order: request.order.unwrap_or(false),
            attributes,
            ..RocketMqTopicConfig::default()
        };

        for broker_addr in &target_addrs {
            session
                .admin
                .create_and_update_topic_config(broker_addr.clone(), topic_config.clone())
                .await
                .map_err(map_rocketmq_error)?;
        }

        if topic_config.order {
            let order_conf = build_order_conf(&target_broker_names, topic_config.write_queue_nums);
            session
                .admin
                .create_or_update_order_conf(request.topic.clone().into(), order_conf.into(), true)
                .await
                .map_err(map_rocketmq_error)?;
        }

        Ok(MutationResult {
            message: format!(
                "Topic `{}` was saved on {} broker target(s)",
                request.topic,
                target_addrs.len()
            ),
        })
    }

    pub async fn delete_topic(&self, topic: &str) -> Result<MutationResult, DashboardError> {
        validate_name(topic, "Topic")?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");

        let route = session
            .admin
            .examine_topic_route_info(CheetahString::from(topic))
            .await
            .map_err(map_rocketmq_error)?
            .ok_or_else(|| DashboardError::NotFound(format!("Topic `{topic}` was not found")))?;
        let mut clusters: Vec<String> = route
            .broker_datas
            .iter()
            .map(|broker| broker.cluster().to_string())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        clusters.sort();

        if clusters.is_empty() {
            return Err(DashboardError::Validation(format!(
                "Topic `{topic}` has no cluster mapping to delete"
            )));
        }

        for cluster_name in &clusters {
            session
                .admin
                .delete_topic(topic.into(), cluster_name.as_str().into())
                .await
                .map_err(map_rocketmq_error)?;
        }

        Ok(MutationResult {
            message: format!("Topic `{topic}` was deleted from {} cluster(s)", clusters.len()),
        })
    }

    pub async fn list_consumer_groups(&self) -> Result<ConsumerListView, DashboardError> {
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        self.list_consumer_groups_with_admin(&mut session.admin).await
    }

    pub async fn consumer_progress(&self, group: &str) -> Result<ConsumerProgress, DashboardError> {
        validate_name(group, "Consumer group")?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");

        let stats = session
            .admin
            .examine_consume_stats(CheetahString::from(group), None, None, None, None)
            .await
            .map_err(map_rocketmq_error)?;

        Ok(map_consumer_progress(group, &stats))
    }

    pub async fn reset_consumer_offset(
        &self,
        group: &str,
        request: ConsumerResetOffsetRequest,
    ) -> Result<MutationResult, DashboardError> {
        validate_name(group, "Consumer group")?;
        validate_name(&request.topic, "Topic")?;
        if request.reset_timestamp < 0 {
            return Err(DashboardError::Validation(
                "Reset timestamp must be a non-negative millisecond timestamp".to_string(),
            ));
        }

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let offsets = session
            .admin
            .reset_offset_by_timestamp(
                None,
                CheetahString::from(request.topic.as_str()),
                CheetahString::from(group),
                request.reset_timestamp as u64,
                request.force,
            )
            .await
            .map_err(map_rocketmq_error)?;

        Ok(MutationResult {
            message: format!(
                "Consumer group `{group}` offset was reset for {} queue(s)",
                offsets.len()
            ),
        })
    }

    pub async fn list_producers(&self) -> Result<Vec<ProducerInfo>, DashboardError> {
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        self.list_producers_with_admin(&mut session.admin).await
    }

    pub async fn producer_connections(
        &self,
        topic: &str,
        producer_group: &str,
    ) -> Result<ProducerConnectionView, DashboardError> {
        validate_name(topic, "Topic")?;
        validate_name(producer_group, "Producer group")?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");

        let connection = session
            .admin
            .examine_producer_connection_info(CheetahString::from(producer_group), CheetahString::from(topic))
            .await
            .map_err(map_rocketmq_error)?;

        Ok(map_producer_connection(topic, producer_group, &connection))
    }

    pub async fn list_brokers(&self) -> Result<BrokerListView, DashboardError> {
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        self.list_brokers_with_admin(&mut session.admin).await
    }

    pub async fn broker_runtime_stats(&self, broker_name: &str) -> Result<BrokerRuntimeStats, DashboardError> {
        validate_name(broker_name, "Broker")?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let address = resolve_broker_address(&mut session.admin, broker_name).await?;
        let entries = fetch_runtime_entries(&mut session.admin, &address).await?;

        Ok(BrokerRuntimeStats {
            broker_name: broker_name.to_string(),
            address,
            entries,
        })
    }

    pub async fn broker_config(&self, broker_name: &str) -> Result<BrokerConfigView, DashboardError> {
        validate_name(broker_name, "Broker")?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let address = resolve_broker_address(&mut session.admin, broker_name).await?;
        let entries = session
            .admin
            .get_broker_config(CheetahString::from(address.as_str()))
            .await
            .map_err(map_rocketmq_error)?
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();

        Ok(BrokerConfigView {
            broker_name: broker_name.to_string(),
            address,
            entries,
        })
    }

    pub async fn update_broker_config(
        &self,
        broker_name: &str,
        request: BrokerConfigUpdateRequest,
    ) -> Result<MutationResult, DashboardError> {
        validate_name(broker_name, "Broker")?;
        if request.entries.is_empty() {
            return Err(DashboardError::Validation(
                "Broker config update entries cannot be empty".to_string(),
            ));
        }

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let address = resolve_broker_address(&mut session.admin, broker_name).await?;
        let properties = request
            .entries
            .into_iter()
            .map(|(key, value)| (CheetahString::from(key), CheetahString::from(value)))
            .collect();

        session
            .admin
            .update_broker_config(CheetahString::from(address.as_str()), properties)
            .await
            .map_err(map_rocketmq_error)?;

        Ok(MutationResult {
            message: format!("Broker `{broker_name}` config was updated at `{address}`"),
        })
    }

    pub async fn list_acl_users(&self, query: AclQuery) -> Result<Vec<AclUserView>, DashboardError> {
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let targets = resolve_acl_targets(
            &mut session.admin,
            query.cluster_name.as_deref(),
            query.broker_name.as_deref(),
        )
        .await?;
        let filter = query.filter.or(query.search_param).unwrap_or_default();
        let mut users = Vec::new();

        for target in targets {
            let user_infos = session
                .admin
                .list_users(
                    CheetahString::from(target.broker_addr.as_str()),
                    CheetahString::from(filter.as_str()),
                )
                .await
                .map_err(map_rocketmq_error)?;
            users.extend(user_infos.iter().map(|user| map_acl_user(&target, user)));
        }

        users.sort_by(|left, right| {
            left.username
                .cmp(&right.username)
                .then(left.broker_name.cmp(&right.broker_name))
                .then(left.broker_addr.cmp(&right.broker_addr))
        });
        Ok(users)
    }

    pub async fn create_acl_user(&self, request: AclUserUpsertRequest) -> Result<AclMutationResult, DashboardError> {
        let username = required_request_field(request.username.as_deref(), "username")?;
        validate_name(username, "Username")?;
        validate_name(&request.password, "Password")?;
        validate_name(&request.user_type, "User type")?;

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let targets = resolve_acl_targets(
            &mut session.admin,
            request.cluster_name.as_deref(),
            request.broker_name.as_deref(),
        )
        .await?;
        let target_count = targets.len();

        for target in targets {
            session
                .admin
                .create_user(
                    CheetahString::from(target.broker_addr.as_str()),
                    CheetahString::from(username),
                    CheetahString::from(request.password.as_str()),
                    CheetahString::from(request.user_type.as_str()),
                )
                .await
                .map_err(map_rocketmq_error)?;
        }

        Ok(AclMutationResult {
            message: format!("Created ACL user {username} on {target_count} broker target(s)"),
            target_count,
        })
    }

    pub async fn update_acl_user(
        &self,
        username: &str,
        request: AclUserUpsertRequest,
    ) -> Result<AclMutationResult, DashboardError> {
        validate_name(username, "Username")?;
        validate_name(&request.password, "Password")?;
        validate_name(&request.user_type, "User type")?;
        let user_status = required_request_field(request.user_status.as_deref(), "userStatus")?;

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let targets = resolve_acl_targets(
            &mut session.admin,
            request.cluster_name.as_deref(),
            request.broker_name.as_deref(),
        )
        .await?;
        let target_count = targets.len();

        for target in targets {
            session
                .admin
                .update_user(
                    CheetahString::from(target.broker_addr.as_str()),
                    CheetahString::from(username),
                    CheetahString::from(request.password.as_str()),
                    CheetahString::from(request.user_type.as_str()),
                    CheetahString::from(user_status),
                )
                .await
                .map_err(map_rocketmq_error)?;
        }

        Ok(AclMutationResult {
            message: format!("Updated ACL user {username} on {target_count} broker target(s)"),
            target_count,
        })
    }

    pub async fn delete_acl_user(&self, username: &str, query: AclQuery) -> Result<AclMutationResult, DashboardError> {
        validate_name(username, "Username")?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let targets = resolve_acl_targets(
            &mut session.admin,
            query.cluster_name.as_deref(),
            query.broker_name.as_deref(),
        )
        .await?;
        let target_count = targets.len();

        for target in targets {
            session
                .admin
                .delete_user(
                    CheetahString::from(target.broker_addr.as_str()),
                    CheetahString::from(username),
                )
                .await
                .map_err(map_rocketmq_error)?;
        }

        Ok(AclMutationResult {
            message: format!("Deleted ACL user {username} from {target_count} broker target(s)"),
            target_count,
        })
    }

    pub async fn list_acl_policies(&self, query: AclQuery) -> Result<Vec<AclPolicyView>, DashboardError> {
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let targets = resolve_acl_targets(
            &mut session.admin,
            query.cluster_name.as_deref(),
            query.broker_name.as_deref(),
        )
        .await?;
        let search = query.search_param.or(query.filter).unwrap_or_default();
        let resource = query.resource.unwrap_or_default();
        let mut policies = Vec::new();

        for target in targets {
            let subject_matches = session
                .admin
                .list_acl(
                    CheetahString::from(target.broker_addr.as_str()),
                    CheetahString::from(search.as_str()),
                    CheetahString::default(),
                )
                .await
                .map_err(map_rocketmq_error)?;
            let resource_matches = if resource.is_empty() && search.is_empty() {
                Vec::new()
            } else {
                session
                    .admin
                    .list_acl(
                        CheetahString::from(target.broker_addr.as_str()),
                        CheetahString::default(),
                        CheetahString::from(if resource.is_empty() {
                            search.as_str()
                        } else {
                            resource.as_str()
                        }),
                    )
                    .await
                    .map_err(map_rocketmq_error)?
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
    }

    pub async fn create_acl_policy(&self, request: AclPolicyRequest) -> Result<AclMutationResult, DashboardError> {
        let acl_info = build_acl_info_from_request(&request)?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let targets = resolve_acl_targets(
            &mut session.admin,
            request.cluster_name.as_deref(),
            request.broker_name.as_deref(),
        )
        .await?;
        let target_count = targets.len();

        for target in targets {
            session
                .admin
                .create_acl_with_acl_info(CheetahString::from(target.broker_addr.as_str()), acl_info.clone())
                .await
                .map_err(map_rocketmq_error)?;
        }

        Ok(AclMutationResult {
            message: format!(
                "Created ACL policy for {} on {target_count} broker target(s)",
                request.subject
            ),
            target_count,
        })
    }

    pub async fn update_acl_policy(
        &self,
        subject: &str,
        mut request: AclPolicyRequest,
    ) -> Result<AclMutationResult, DashboardError> {
        validate_name(subject, "ACL subject")?;
        request.subject = subject.to_string();
        let acl_info = build_acl_info_from_request(&request)?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let targets = resolve_acl_targets(
            &mut session.admin,
            request.cluster_name.as_deref(),
            request.broker_name.as_deref(),
        )
        .await?;
        let target_count = targets.len();

        for target in targets {
            session
                .admin
                .update_acl_with_acl_info(CheetahString::from(target.broker_addr.as_str()), acl_info.clone())
                .await
                .map_err(map_rocketmq_error)?;
        }

        Ok(AclMutationResult {
            message: format!("Updated ACL policy for {subject} on {target_count} broker target(s)"),
            target_count,
        })
    }

    pub async fn delete_acl_policy(&self, subject: &str, query: AclQuery) -> Result<AclMutationResult, DashboardError> {
        validate_name(subject, "ACL subject")?;
        let resource = query.resource.unwrap_or_default();
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let targets = resolve_acl_targets(
            &mut session.admin,
            query.cluster_name.as_deref(),
            query.broker_name.as_deref(),
        )
        .await?;
        let target_count = targets.len();

        for target in targets {
            session
                .admin
                .delete_acl(
                    CheetahString::from(target.broker_addr.as_str()),
                    CheetahString::from(subject),
                    CheetahString::from(resource.as_str()),
                )
                .await
                .map_err(map_rocketmq_error)?;
        }

        Ok(AclMutationResult {
            message: format!("Deleted ACL subject {subject} from {target_count} broker target(s)"),
            target_count,
        })
    }

    pub async fn query_messages(
        &self,
        topic: Option<&str>,
        key: Option<&str>,
        message_id: Option<&str>,
        begin: Option<i64>,
        end: Option<i64>,
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<MessageListView, DashboardError> {
        match (topic, key, message_id) {
            (Some(topic), Some(key), _) => self.query_message_by_key_with_window(topic, key, begin, end).await,
            (Some(topic), None, Some(message_id)) => {
                self.query_message_by_id_with_topic(topic, message_id, begin, end).await
            }
            (Some(topic), None, None) => {
                self.scan_messages_by_topic(topic, begin, end, page_num, page_size)
                    .await
            }
            (None, _, Some(_)) => Err(DashboardError::Validation(
                "Message ID query requires topic. Use /api/messages?topic=...&messageId=...".to_string(),
            )),
            _ => Err(DashboardError::Validation(
                "Message query requires topic with either key or messageId".to_string(),
            )),
        }
    }

    pub async fn query_message_by_key(&self, topic: &str, key: &str) -> Result<MessageListView, DashboardError> {
        self.query_message_by_key_with_window(topic, key, None, None).await
    }

    pub async fn query_message_by_id(&self, message_id: &str) -> Result<MessageListView, DashboardError> {
        validate_name(message_id, "Message ID")?;
        Err(DashboardError::Validation(
            "Message ID query requires topic. Use /api/messages?topic=...&messageId=...".to_string(),
        ))
    }

    #[allow(deprecated)]
    pub async fn message_trace(
        &self,
        message_id: &str,
        topic: Option<&str>,
        trace_topic: &str,
    ) -> Result<MessageTraceView, DashboardError> {
        validate_name(message_id, "Message ID")?;
        validate_name(trace_topic, "Trace topic")?;
        let topic = topic.ok_or_else(|| {
            DashboardError::Validation(
                "Message trace requires topic. Use /api/messages/:id/trace?topic=...".to_string(),
            )
        })?;
        validate_name(topic, "Topic")?;

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let message = find_message_by_topic_and_id(&mut session.admin, topic, message_id).await?;
        let tracks = session
            .admin
            .message_track_detail(message)
            .await
            .map_err(map_rocketmq_error)?;
        let mut nodes: Vec<_> = tracks
            .into_iter()
            .map(|track| MessageTraceNode {
                node_type: "CONSUMER".to_string(),
                name: track.consumer_group,
                status: track
                    .track_type
                    .map(|track_type| track_type.to_string())
                    .unwrap_or_else(|| "UNKNOWN".to_string()),
                timestamp: 0,
            })
            .collect();
        nodes.sort_by(|left, right| left.name.cmp(&right.name));

        Ok(MessageTraceView {
            message_id: message_id.to_string(),
            trace_topic: trace_topic.to_string(),
            nodes,
        })
    }

    pub async fn resend_message(
        &self,
        message_id: &str,
        request: MessageResendRequest,
    ) -> Result<MutationResult, DashboardError> {
        validate_name(message_id, "Message ID")?;
        validate_name(&request.topic, "Topic")?;
        validate_name(&request.consumer_group, "Consumer group")?;

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let client_id = request.client_id.unwrap_or_default();
        let consume_result = session
            .admin
            .consume_message_directly(
                CheetahString::from(request.consumer_group.as_str()),
                CheetahString::from(client_id.as_str()),
                CheetahString::from(request.topic.as_str()),
                CheetahString::from(message_id),
            )
            .await
            .map_err(map_rocketmq_error)?;
        let consume_result_text = consume_result
            .consume_result()
            .map(ToString::to_string)
            .unwrap_or_else(|| "UNKNOWN".to_string());
        let mut message = format!(
            "Direct consume returned {consume_result_text} for `{message_id}` on `{}` in consumer group `{}`",
            request.topic, request.consumer_group
        );
        if let Some(remark) = consume_result.remark().map(ToString::to_string)
            && !remark.trim().is_empty()
        {
            message.push_str(&format!(". Remark: {remark}"));
        }

        Ok(MutationResult { message })
    }

    pub async fn query_dlq_messages(&self, query: DlqMessageQuery) -> Result<MessageListView, DashboardError> {
        validate_name(&query.consumer_group, "Consumer group")?;
        let topic = mix_all::get_dlq_topic(&query.consumer_group);

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        if session
            .admin
            .examine_topic_route_info(CheetahString::from(topic.as_str()))
            .await
            .map_err(map_rocketmq_error)?
            .is_none()
        {
            return Ok(MessageListView {
                items: Vec::new(),
                total: 0,
            });
        }
        drop(session_guard);

        match (query.key.as_deref(), query.message_id.as_deref()) {
            (Some(key), _) => {
                self.query_message_by_key_with_window(&topic, key, query.begin, query.end)
                    .await
            }
            (None, Some(message_id)) => {
                self.query_message_by_id_with_topic(&topic, message_id, query.begin, query.end)
                    .await
            }
            (None, None) => self.scan_dlq_messages_by_page(&topic, &query).await,
        }
    }

    async fn scan_dlq_messages_by_page(
        &self,
        topic: &str,
        query: &DlqMessageQuery,
    ) -> Result<MessageListView, DashboardError> {
        let page_size = normalize_dlq_page_size(query.page_size);
        let page_num = query.page_num.unwrap_or(1).max(1) as usize;
        let skip_count = page_num.saturating_sub(1).saturating_mul(page_size);
        let mut seen = 0usize;
        let mut items = Vec::with_capacity(page_size);

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let route = session
            .admin
            .examine_topic_route_info(CheetahString::from(topic))
            .await
            .map_err(map_rocketmq_error)?
            .ok_or_else(|| DashboardError::NotFound(format!("DLQ topic `{topic}` was not found")))?;
        let targets = topic_queue_targets(topic, &route);
        for (broker_addr, queue) in targets {
            if items.len() >= page_size {
                break;
            }

            let mut min_offset = session
                .admin
                .min_offset(
                    CheetahString::from(broker_addr.as_str()),
                    queue.clone(),
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(map_rocketmq_error)?;
            let mut max_offset = session
                .admin
                .max_offset(
                    CheetahString::from(broker_addr.as_str()),
                    queue.clone(),
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(map_rocketmq_error)?;

            if let Some(begin) = query.begin.filter(|timestamp| *timestamp > 0) {
                min_offset = session
                    .admin
                    .search_offset(
                        CheetahString::from(broker_addr.as_str()),
                        CheetahString::from(topic),
                        queue.queue_id(),
                        begin as u64,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await
                    .map_err(map_rocketmq_error)? as i64;
            }
            if let Some(end) = query.end.filter(|timestamp| *timestamp > 0) {
                max_offset = session
                    .admin
                    .search_offset(
                        CheetahString::from(broker_addr.as_str()),
                        CheetahString::from(topic),
                        queue.queue_id(),
                        end as u64,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await
                    .map_err(map_rocketmq_error)? as i64;
            }
            if max_offset <= min_offset {
                continue;
            }

            let mut offset = min_offset;
            while offset < max_offset && items.len() < page_size {
                let batch_size = (max_offset - offset).clamp(1, DLQ_PULL_BATCH_SIZE) as i32;
                let pull_result = session
                    .admin
                    .pull_message_from_queue(&broker_addr, &queue, "*", offset, batch_size, PULL_TIMEOUT_MILLIS)
                    .await
                    .map_err(map_rocketmq_error)?;
                let next_offset = pull_result.next_begin_offset() as i64;
                offset = if next_offset > offset {
                    next_offset
                } else {
                    offset + i64::from(batch_size)
                };

                match *pull_result.pull_status() {
                    PullStatus::Found => {
                        if let Some(messages) = pull_result.msg_found_list() {
                            for message in messages {
                                if !message_matches_window(message, query.begin, query.end) {
                                    continue;
                                }
                                if seen < skip_count {
                                    seen += 1;
                                    continue;
                                }
                                items.push(map_message(message));
                                seen += 1;
                                if items.len() >= page_size {
                                    break;
                                }
                            }
                        }
                    }
                    PullStatus::NoMatchedMsg => {}
                    PullStatus::NoNewMsg | PullStatus::OffsetIllegal => break,
                }
            }
        }

        Ok(MessageListView { total: seen, items })
    }

    async fn scan_messages_by_topic(
        &self,
        topic: &str,
        begin: Option<i64>,
        end: Option<i64>,
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<MessageListView, DashboardError> {
        validate_name(topic, "Topic")?;
        let begin = begin
            .filter(|timestamp| *timestamp > 0)
            .ok_or_else(|| DashboardError::Validation("Topic message query requires begin time".to_string()))?;
        let end = end
            .filter(|timestamp| *timestamp > 0)
            .ok_or_else(|| DashboardError::Validation("Topic message query requires end time".to_string()))?;
        if end < begin {
            return Err(DashboardError::Validation(
                "Topic message query end time must be later than begin time".to_string(),
            ));
        }

        let page_size = normalize_message_page_size(page_size);
        let page_num = page_num.unwrap_or(1).max(1) as usize;
        let skip_count = page_num.saturating_sub(1).saturating_mul(page_size);
        let mut seen = 0usize;
        let mut items = Vec::with_capacity(page_size);

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let route = session
            .admin
            .examine_topic_route_info(CheetahString::from(topic))
            .await
            .map_err(map_rocketmq_error)?
            .ok_or_else(|| DashboardError::NotFound(format!("Topic `{topic}` was not found")))?;
        let targets = topic_queue_targets(topic, &route);

        for (broker_addr, queue) in targets {
            if items.len() >= page_size {
                break;
            }

            let min_offset = session
                .admin
                .search_offset(
                    CheetahString::from(broker_addr.as_str()),
                    CheetahString::from(topic),
                    queue.queue_id(),
                    begin as u64,
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(map_rocketmq_error)? as i64;
            let max_offset = session
                .admin
                .search_offset(
                    CheetahString::from(broker_addr.as_str()),
                    CheetahString::from(topic),
                    queue.queue_id(),
                    end as u64,
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(map_rocketmq_error)? as i64;
            if max_offset < min_offset {
                continue;
            }

            let mut offset = min_offset;
            while offset <= max_offset && items.len() < page_size {
                let batch_size = (max_offset - offset + 1).clamp(1, MESSAGE_PULL_BATCH_SIZE) as i32;
                let pull_result = session
                    .admin
                    .pull_message_from_queue(&broker_addr, &queue, "*", offset, batch_size, PULL_TIMEOUT_MILLIS)
                    .await
                    .map_err(map_rocketmq_error)?;
                let next_offset = pull_result.next_begin_offset() as i64;
                offset = if next_offset > offset {
                    next_offset
                } else {
                    offset + i64::from(batch_size)
                };

                match *pull_result.pull_status() {
                    PullStatus::Found => {
                        if let Some(messages) = pull_result.msg_found_list() {
                            for message in messages {
                                if !message_matches_window(message, Some(begin), Some(end)) {
                                    continue;
                                }
                                if seen < skip_count {
                                    seen += 1;
                                    continue;
                                }
                                items.push(map_message(message));
                                seen += 1;
                                if items.len() >= page_size {
                                    break;
                                }
                            }
                        }
                    }
                    PullStatus::NoMatchedMsg => {}
                    PullStatus::NoNewMsg | PullStatus::OffsetIllegal => break,
                }
            }
        }

        items.sort_by_key(|message| Reverse(message.store_timestamp));
        Ok(MessageListView { total: seen, items })
    }

    pub async fn resend_dlq_messages(
        &self,
        request: DlqBatchResendRequest,
    ) -> Result<Vec<DlqMessageResendResult>, DashboardError> {
        if request.messages.is_empty() {
            return Err(DashboardError::Validation(
                "DLQ resend messages cannot be empty".to_string(),
            ));
        }

        let mut results = Vec::with_capacity(request.messages.len());
        for message in request.messages {
            validate_name(&message.consumer_group, "Consumer group")?;
            validate_name(&message.msg_id, "Message ID")?;
            let topic = message
                .topic_name
                .filter(|topic| !topic.trim().is_empty())
                .unwrap_or_else(|| mix_all::get_dlq_topic(&message.consumer_group));
            let result = self
                .resend_message(
                    &message.msg_id,
                    MessageResendRequest {
                        topic,
                        consumer_group: message.consumer_group,
                        client_id: message.client_id,
                    },
                )
                .await?;
            results.push(DlqMessageResendResult {
                msg_id: message.msg_id,
                consume_result: "REQUESTED".to_string(),
                remark: Some(result.message),
            });
        }
        Ok(results)
    }

    pub async fn export_dlq_messages(&self, query: DlqMessageQuery) -> Result<DlqExportView, DashboardError> {
        let consumer_group = query.consumer_group.clone();
        let messages = self.query_dlq_messages(query).await?;
        let csv = build_dlq_csv(&messages.items);
        Ok(DlqExportView {
            file_name: format!("dlq-{consumer_group}.csv"),
            rows: messages.items,
            csv,
        })
    }

    async fn query_message_by_key_with_window(
        &self,
        topic: &str,
        key: &str,
        begin: Option<i64>,
        end: Option<i64>,
    ) -> Result<MessageListView, DashboardError> {
        validate_name(topic, "Topic")?;
        validate_name(key, "Message key")?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let result = session
            .admin
            .query_message_by_key(
                None,
                CheetahString::from(topic),
                CheetahString::from(key),
                64,
                begin.unwrap_or_default(),
                end.unwrap_or(i64::MAX),
                CheetahString::from_static_str("K"),
                None,
            )
            .await
            .map_err(map_rocketmq_error)?;
        Ok(map_message_list(result.message_list()))
    }

    async fn query_message_by_id_with_topic(
        &self,
        topic: &str,
        message_id: &str,
        begin: Option<i64>,
        end: Option<i64>,
    ) -> Result<MessageListView, DashboardError> {
        validate_name(topic, "Topic")?;
        validate_name(message_id, "Message ID")?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let session = session_guard
            .as_mut()
            .expect("admin session should be initialized before use");
        let result = session
            .admin
            .query_message_by_unique_key(
                None,
                CheetahString::from(topic),
                CheetahString::from(message_id),
                32,
                begin.unwrap_or_default(),
                end.unwrap_or(i64::MAX),
            )
            .await
            .map_err(map_rocketmq_error)?;
        Ok(map_message_list_by_unique_key(result.message_list(), message_id))
    }

    async fn list_brokers_with_admin(&self, admin: &mut DefaultMQAdminExt) -> Result<BrokerListView, DashboardError> {
        let cluster_info = admin.examine_broker_cluster_info().await.map_err(map_rocketmq_error)?;
        let mut items = Vec::new();
        let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() else {
            return Ok(BrokerListView { items, total: 0 });
        };
        let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() else {
            return Ok(BrokerListView { items, total: 0 });
        };

        let mut cluster_names: Vec<_> = cluster_addr_table.keys().cloned().collect();
        cluster_names.sort();
        for cluster_name in cluster_names {
            let Some(broker_names) = cluster_addr_table.get(cluster_name.as_str()) else {
                continue;
            };
            let mut broker_names: Vec<_> = broker_names.iter().cloned().collect();
            broker_names.sort();
            for broker_name in broker_names {
                let Some(broker_data) = broker_addr_table.get(broker_name.as_str()) else {
                    continue;
                };
                let mut broker_addrs: Vec<_> = broker_data
                    .broker_addrs()
                    .iter()
                    .map(|(broker_id, address)| (*broker_id, address.clone()))
                    .collect();
                broker_addrs.sort_by_key(|(broker_id, _)| *broker_id);

                for (broker_id, address) in broker_addrs {
                    let runtime = admin.fetch_broker_runtime_stats(address.clone()).await.ok();
                    let entries = runtime.as_ref().map(kv_table_to_map).unwrap_or_default();
                    items.push(BrokerInfo {
                        cluster_name: cluster_name.to_string(),
                        broker_name: broker_name.to_string(),
                        broker_id,
                        address: address.to_string(),
                        role: if broker_id == 0 { "MASTER" } else { "SLAVE" }.to_string(),
                        version: entries.get("brokerVersionDesc").cloned().unwrap_or_default(),
                        produce_tps: parse_rate_value(entries.get("putTps")),
                        consume_tps: parse_rate_value(select_consume_tps_value(&entries)),
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

        Ok(BrokerListView {
            total: items.len(),
            items,
        })
    }

    async fn list_consumer_groups_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
    ) -> Result<ConsumerListView, DashboardError> {
        let broker_addrs = broker_addresses(admin).await?;
        let mut groups = BTreeSet::new();
        for address in broker_addrs {
            if let Ok(wrapper) = admin
                .get_all_subscription_group(CheetahString::from(address.as_str()), 5_000)
                .await
            {
                for group in wrapper.get_subscription_group_table().keys() {
                    groups.insert(group.to_string());
                }
            }
        }

        let mut items = Vec::new();
        for group in groups {
            let stats = admin
                .examine_consume_stats(CheetahString::from(group.as_str()), None, None, None, None)
                .await
                .ok();
            let connection = admin
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
                .unwrap_or_else(|| (String::new(), String::new(), 0));

            items.push(ConsumerGroupInfo {
                group,
                consume_type,
                message_model,
                client_count,
                diff_total,
            });
        }
        items.sort_by(|left, right| left.group.cmp(&right.group));

        Ok(ConsumerListView {
            total: items.len(),
            items,
        })
    }

    async fn list_producers_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
    ) -> Result<Vec<ProducerInfo>, DashboardError> {
        let mut producer_counts: HashMap<String, usize> = HashMap::new();
        for address in broker_addresses(admin).await? {
            if let Ok(table) = admin.get_all_producer_info(CheetahString::from(address.as_str())).await {
                for (group, producers) in table.data() {
                    *producer_counts.entry(group.clone()).or_default() += producers.len();
                }
            }
        }

        let mut items: Vec<_> = producer_counts
            .into_iter()
            .map(|(producer_group, connection_count)| ProducerInfo {
                topic: String::new(),
                producer_group,
                connection_count,
            })
            .collect();
        items.sort_by(|left, right| left.producer_group.cmp(&right.producer_group));
        Ok(items)
    }

    async fn ensure_admin_session(&self, session_slot: &mut Option<ManagedAdminSession>) -> Result<(), DashboardError> {
        let snapshot = self.admin_config_snapshot().await?;
        let needs_reconnect = session_slot.as_ref().is_none_or(|session| session.snapshot != snapshot);

        if needs_reconnect {
            if let Some(mut session) = session_slot.take() {
                session.admin.shutdown().await;
            }

            let mut admin =
                DefaultMQAdminExt::with_admin_ext_group_and_timeout(unique_admin_group(), Duration::from_millis(5_000));
            admin
                .client_config_mut()
                .set_namesrv_addr(CheetahString::from(snapshot.namesrv_addr.as_str()));
            admin
                .client_config_mut()
                .set_vip_channel_enabled(snapshot.use_vip_channel);
            admin.client_config_mut().set_use_tls(snapshot.use_tls);
            admin.start().await.map_err(map_rocketmq_error)?;
            tracing::info!(
                namesrv = %snapshot.namesrv_addr,
                use_vip_channel = snapshot.use_vip_channel,
                use_tls = snapshot.use_tls,
                "connected RocketMQ dashboard admin session"
            );
            *session_slot = Some(ManagedAdminSession { admin, snapshot });
        }

        Ok(())
    }

    async fn admin_config_snapshot(&self) -> Result<AdminConfigSnapshot, DashboardError> {
        let config = self.config.read().await;
        let namesrv_addr = config
            .current_namesrv
            .clone()
            .ok_or_else(|| DashboardError::Config("No active NameServer is configured".to_string()))?;

        Ok(AdminConfigSnapshot {
            namesrv_addr,
            use_vip_channel: config.use_vip_channel,
            use_tls: config.use_tls,
        })
    }
}

async fn broker_addresses(admin: &mut DefaultMQAdminExt) -> Result<Vec<String>, DashboardError> {
    let cluster_info = admin.examine_broker_cluster_info().await.map_err(map_rocketmq_error)?;
    Ok(broker_addresses_from_cluster(&cluster_info))
}

async fn resolve_acl_targets(
    admin: &mut DefaultMQAdminExt,
    cluster_name: Option<&str>,
    broker_name: Option<&str>,
) -> Result<Vec<AclTarget>, DashboardError> {
    if let Some(broker_name) = broker_name.map(str::trim).filter(|value| !value.is_empty()) {
        let broker_addr = resolve_broker_address(admin, broker_name).await?;
        return Ok(vec![AclTarget {
            broker_name: broker_name.to_string(),
            broker_addr,
        }]);
    }

    let cluster_info = admin.examine_broker_cluster_info().await.map_err(map_rocketmq_error)?;
    let broker_addr_table = cluster_info
        .broker_addr_table
        .as_ref()
        .ok_or_else(|| DashboardError::NotFound("No broker address table returned by cluster".to_string()))?;
    let allowed_brokers = cluster_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|cluster| {
            cluster_info
                .cluster_addr_table
                .as_ref()
                .and_then(|table| table.get(cluster))
                .cloned()
                .ok_or_else(|| DashboardError::NotFound(format!("Cluster `{cluster}` was not found")))
        })
        .transpose()?;

    let mut targets = Vec::new();
    for (name, broker_data) in broker_addr_table {
        if allowed_brokers
            .as_ref()
            .is_some_and(|brokers| !brokers.iter().any(|broker| broker.as_str() == name.as_str()))
        {
            continue;
        }
        for address in broker_data.broker_addrs().values() {
            targets.push(AclTarget {
                broker_name: name.to_string(),
                broker_addr: address.to_string(),
            });
        }
    }
    targets.sort_by(|left, right| {
        left.broker_name
            .cmp(&right.broker_name)
            .then(left.broker_addr.cmp(&right.broker_addr))
    });

    if targets.is_empty() {
        return Err(DashboardError::NotFound("No ACL broker target was found".to_string()));
    }
    Ok(targets)
}

fn broker_addresses_from_cluster(cluster_info: &ClusterInfo) -> Vec<String> {
    let mut addresses = BTreeSet::new();
    if let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() {
        for broker_data in broker_addr_table.values() {
            for address in broker_data.broker_addrs().values() {
                addresses.insert(address.to_string());
            }
        }
    }
    addresses.into_iter().collect()
}

fn find_master_addr_by_broker_name(cluster_info: &ClusterInfo, broker_name: &str) -> Option<CheetahString> {
    cluster_info
        .broker_addr_table
        .as_ref()
        .and_then(|table| table.get(broker_name))
        .and_then(|broker_data| broker_data.broker_addrs().get(&mix_all::MASTER_ID).cloned())
}

fn master_targets_by_cluster_name(
    cluster_info: &ClusterInfo,
    cluster_name: &str,
) -> Result<Vec<(String, CheetahString)>, DashboardError> {
    let cluster_addr_table = cluster_info.cluster_addr_table.as_ref().ok_or_else(|| {
        DashboardError::RocketMq(RocketMQError::response_process_failed(
            "examine_broker_cluster_info",
            "NameServer did not return cluster address data",
        ))
    })?;
    let broker_addr_table = cluster_info.broker_addr_table.as_ref().ok_or_else(|| {
        DashboardError::RocketMq(RocketMQError::response_process_failed(
            "examine_broker_cluster_info",
            "NameServer did not return broker address data",
        ))
    })?;
    let broker_names = cluster_addr_table.get(cluster_name).ok_or_else(|| {
        DashboardError::Validation(format!(
            "Cluster `{cluster_name}` was not found in the current NameServer view"
        ))
    })?;

    let mut targets = Vec::new();
    for broker_name in broker_names {
        if let Some(master_addr) = broker_addr_table
            .get(broker_name)
            .and_then(|broker_data| broker_data.broker_addrs().get(&mix_all::MASTER_ID))
        {
            targets.push((broker_name.to_string(), master_addr.clone()));
        }
    }
    targets.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(targets)
}

async fn resolve_broker_address(admin: &mut DefaultMQAdminExt, broker_name: &str) -> Result<String, DashboardError> {
    if broker_name.contains(':') {
        return Ok(broker_name.to_string());
    }

    let cluster_info = admin.examine_broker_cluster_info().await.map_err(map_rocketmq_error)?;
    let broker_addr_table = cluster_info
        .broker_addr_table
        .as_ref()
        .ok_or_else(|| DashboardError::NotFound("No broker address table returned by cluster".to_string()))?;
    let broker_data = broker_addr_table
        .get(broker_name)
        .ok_or_else(|| DashboardError::NotFound(format!("Broker `{broker_name}` was not found")))?;
    broker_data
        .select_broker_addr()
        .map(|address| address.to_string())
        .ok_or_else(|| DashboardError::NotFound(format!("Broker `{broker_name}` has no address")))
}

fn normalize_dlq_page_size(page_size: Option<u32>) -> usize {
    page_size
        .unwrap_or(DLQ_DEFAULT_PAGE_SIZE as u32)
        .clamp(1, DLQ_MAX_PAGE_SIZE as u32) as usize
}

fn normalize_message_page_size(page_size: Option<u32>) -> usize {
    page_size
        .unwrap_or(MESSAGE_DEFAULT_PAGE_SIZE as u32)
        .clamp(1, MESSAGE_MAX_PAGE_SIZE as u32) as usize
}

fn topic_queue_targets(topic: &str, route: &TopicRouteData) -> Vec<(String, MessageQueue)> {
    let mut broker_addrs = BTreeMap::new();
    for broker in &route.broker_datas {
        let selected_addr = broker
            .broker_addrs()
            .get(&mix_all::MASTER_ID)
            .or_else(|| broker.broker_addrs().values().next())
            .map(ToString::to_string);
        if let Some(address) = selected_addr {
            broker_addrs.insert(broker.broker_name().to_string(), address);
        }
    }

    let mut targets = Vec::new();
    for queue in &route.queue_datas {
        let Some(address) = broker_addrs.get(queue.broker_name().as_str()).cloned() else {
            continue;
        };
        for queue_id in 0..queue.read_queue_nums() {
            targets.push((
                address.clone(),
                MessageQueue::from_parts(topic, queue.broker_name().clone(), queue_id as i32),
            ));
        }
    }
    targets.sort_by(|left, right| {
        left.1
            .broker_name()
            .cmp(right.1.broker_name())
            .then(left.1.queue_id().cmp(&right.1.queue_id()))
    });
    targets
}

fn message_matches_window(message: &MessageExt, begin: Option<i64>, end: Option<i64>) -> bool {
    let store_timestamp = message.store_timestamp();
    begin.is_none_or(|begin| store_timestamp >= begin) && end.is_none_or(|end| store_timestamp <= end)
}

async fn fetch_runtime_entries(
    admin: &mut DefaultMQAdminExt,
    broker_addr: &str,
) -> Result<BTreeMap<String, String>, DashboardError> {
    let kv_table = admin
        .fetch_broker_runtime_stats(CheetahString::from(broker_addr))
        .await
        .map_err(map_rocketmq_error)?;
    Ok(kv_table_to_map(&kv_table))
}

fn map_acl_user(target: &AclTarget, user: &UserInfo) -> AclUserView {
    AclUserView {
        broker_name: target.broker_name.clone(),
        broker_addr: target.broker_addr.clone(),
        username: user.username.as_ref().map(ToString::to_string).unwrap_or_default(),
        password: None,
        user_type: user.user_type.as_ref().map(ToString::to_string),
        user_status: user.user_status.as_ref().map(ToString::to_string),
    }
}

fn map_acl_policy(target: &AclTarget, acl: &AclInfo) -> Vec<AclPolicyView> {
    let Some(policies) = acl.policies.as_ref() else {
        return Vec::new();
    };

    policies
        .iter()
        .map(|policy| AclPolicyView {
            broker_name: target.broker_name.clone(),
            broker_addr: target.broker_addr.clone(),
            subject: acl.subject.as_ref().map(ToString::to_string),
            policy_type: policy.policy_type.as_ref().map(ToString::to_string),
            entries: policy
                .entries
                .as_ref()
                .map(|entries| entries.iter().map(map_acl_policy_entry).collect())
                .unwrap_or_default(),
        })
        .collect()
}

fn map_acl_policy_entry(entry: &PolicyEntryInfo) -> AclPolicyEntryView {
    AclPolicyEntryView {
        resource: entry.resource.as_ref().map(ToString::to_string),
        actions: entry
            .actions
            .as_ref()
            .map(|actions| actions.iter().map(ToString::to_string).collect())
            .unwrap_or_default(),
        source_ips: entry
            .source_ips
            .as_ref()
            .map(|source_ips| source_ips.iter().map(ToString::to_string).collect())
            .unwrap_or_default(),
        decision: entry.decision.as_ref().map(ToString::to_string),
    }
}

fn build_acl_info_from_request(request: &AclPolicyRequest) -> Result<AclInfo, DashboardError> {
    validate_name(&request.subject, "ACL subject")?;
    if request.policies.is_empty() {
        return Err(DashboardError::Validation("ACL policies cannot be empty".to_string()));
    }

    let mut policies = Vec::with_capacity(request.policies.len());
    for policy in &request.policies {
        validate_name(&policy.policy_type, "ACL policy type")?;
        if policy.entries.is_empty() {
            return Err(DashboardError::Validation(
                "ACL policy entries cannot be empty".to_string(),
            ));
        }

        let mut entries = Vec::new();
        for entry in &policy.entries {
            if entry.resource.is_empty() {
                return Err(DashboardError::Validation(
                    "ACL policy resource cannot be empty".to_string(),
                ));
            }
            if entry.actions.is_empty() {
                return Err(DashboardError::Validation(
                    "ACL policy actions cannot be empty".to_string(),
                ));
            }
            validate_name(&entry.decision, "ACL policy decision")?;

            for resource in &entry.resource {
                validate_name(resource, "ACL policy resource")?;
                entries.push(PolicyEntryInfo {
                    resource: Some(CheetahString::from(resource.as_str())),
                    actions: Some(
                        entry
                            .actions
                            .iter()
                            .map(|action| CheetahString::from(action.as_str()))
                            .collect(),
                    ),
                    source_ips: Some(
                        entry
                            .source_ips
                            .iter()
                            .map(|source_ip| CheetahString::from(source_ip.as_str()))
                            .collect(),
                    ),
                    decision: Some(CheetahString::from(entry.decision.as_str())),
                });
            }
        }

        policies.push(rocketmq_remoting::protocol::body::acl_info::PolicyInfo {
            policy_type: Some(CheetahString::from(policy.policy_type.as_str())),
            entries: Some(entries),
        });
    }

    Ok(AclInfo {
        subject: Some(CheetahString::from(request.subject.as_str())),
        policies: Some(policies),
    })
}

fn map_topic_route(topic: &str, route: &TopicRouteData) -> TopicRouteInfo {
    let mut brokers: Vec<_> = route
        .broker_datas
        .iter()
        .map(|broker| {
            let mut broker_addrs: Vec<_> = broker.broker_addrs().values().map(ToString::to_string).collect();
            broker_addrs.sort();
            TopicRouteBroker {
                broker_name: broker.broker_name().to_string(),
                broker_addrs,
            }
        })
        .collect();
    brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));

    let mut queues: Vec<_> = route
        .queue_datas
        .iter()
        .map(|queue| TopicRouteQueue {
            broker_name: queue.broker_name().to_string(),
            read_queue_nums: queue.read_queue_nums(),
            write_queue_nums: queue.write_queue_nums(),
            perm: queue.perm(),
        })
        .collect();
    queues.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));

    TopicRouteInfo {
        topic: topic.to_string(),
        brokers,
        queues,
    }
}

fn topic_info_from_route(topic: &str, route: Option<&TopicRouteInfo>) -> TopicInfo {
    let read_queue_count = route
        .map(|route| route.queues.iter().map(|queue| queue.read_queue_nums).sum())
        .unwrap_or_default();
    let write_queue_count = route
        .map(|route| route.queues.iter().map(|queue| queue.write_queue_nums).sum())
        .unwrap_or_default();
    let perm = route
        .and_then(|route| route.queues.iter().map(|queue| queue.perm).max())
        .unwrap_or_default();
    let broker_name = route.and_then(|route| route.brokers.first().map(|broker| broker.broker_name.clone()));

    TopicInfo {
        topic: topic.to_string(),
        broker_name,
        read_queue_count,
        write_queue_count,
        perm,
        category: classify_topic(topic).to_string(),
    }
}

fn map_topic_stats(topic: &str, stats: &TopicStatsTable) -> TopicStatsInfo {
    let queue_count = stats.get_offset_table().len();
    let total_min_offset = stats
        .get_offset_table()
        .values()
        .map(|offset| offset.get_min_offset())
        .sum();
    let total_max_offset = stats
        .get_offset_table()
        .values()
        .map(|offset| offset.get_max_offset())
        .sum();

    TopicStatsInfo {
        topic: topic.to_string(),
        queue_count,
        total_min_offset,
        total_max_offset,
    }
}

fn map_consumer_progress(group: &str, stats: &ConsumeStats) -> ConsumerProgress {
    let mut queues: Vec<_> = stats
        .get_offset_table()
        .iter()
        .map(|(queue, offset)| {
            let broker_offset = offset.get_broker_offset();
            let consumer_offset = offset.get_consumer_offset();
            ConsumerQueueProgress {
                topic: queue.topic_str().to_string(),
                broker_name: queue.broker_name().to_string(),
                queue_id: queue.queue_id(),
                broker_offset,
                consumer_offset,
                diff: broker_offset.saturating_sub(consumer_offset),
            }
        })
        .collect();
    queues.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.broker_name.cmp(&right.broker_name))
            .then(left.queue_id.cmp(&right.queue_id))
    });

    let topic_count = queues
        .iter()
        .map(|queue| queue.topic.clone())
        .collect::<BTreeSet<_>>()
        .len();
    ConsumerProgress {
        group: group.to_string(),
        topic_count,
        diff_total: stats.compute_total_diff(),
        queues,
    }
}

fn map_producer_connection(
    topic: &str,
    producer_group: &str,
    connection: &ProducerConnection,
) -> ProducerConnectionView {
    let mut connections: Vec<_> = connection
        .connection_set()
        .iter()
        .map(|item| ProducerConnectionInfo {
            client_id: item.get_client_id().to_string(),
            client_addr: item.get_client_addr().to_string(),
            language: item.get_language().to_string(),
            version: item.get_version().to_string(),
        })
        .collect();
    connections.sort_by(|left, right| left.client_id.cmp(&right.client_id));

    ProducerConnectionView {
        topic: topic.to_string(),
        producer_group: producer_group.to_string(),
        connections,
    }
}

fn map_message_list(messages: &[MessageExt]) -> MessageListView {
    let mut items: Vec<_> = messages.iter().map(map_message).collect();
    items.sort_by_key(|message| Reverse(message.store_timestamp));

    MessageListView {
        total: items.len(),
        items,
    }
}

fn map_message_list_by_unique_key(messages: &[MessageExt], message_id: &str) -> MessageListView {
    let mut items: Vec<_> = messages
        .iter()
        .filter(|message| message_unique_key_matches(message, message_id))
        .map(map_message)
        .collect();
    items.sort_by_key(|message| Reverse(message.store_timestamp));

    MessageListView {
        total: items.len(),
        items,
    }
}

fn message_unique_key_matches(message: &MessageExt, message_id: &str) -> bool {
    message
        .properties()
        .get(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        .is_some_and(|value| value == message_id)
}

async fn find_message_by_topic_and_id(
    admin: &mut DefaultMQAdminExt,
    topic: &str,
    message_id: &str,
) -> Result<MessageExt, DashboardError> {
    match admin
        .query_message_by_unique_key(
            None,
            CheetahString::from(topic),
            CheetahString::from(message_id),
            32,
            0,
            i64::MAX,
        )
        .await
    {
        Ok(result) if !result.message_list().is_empty() => {
            if let Some(message) = result
                .message_list()
                .iter()
                .find(|message| message_unique_key_matches(message, message_id))
                .cloned()
            {
                return Ok(message);
            }
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(
                topic,
                message_id,
                error = %error,
                "query_message_by_unique_key failed; falling back to query_message"
            );
        }
    }

    admin
        .query_message(
            CheetahString::default(),
            CheetahString::from(topic),
            CheetahString::from(message_id),
        )
        .await
        .map_err(map_rocketmq_error)
}

fn map_message(message: &MessageExt) -> MessageView {
    let mut properties: BTreeMap<String, String> = message
        .properties()
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    let store_message_id = message.msg_id().to_string();
    let message_id = message
        .properties()
        .get(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        .map(ToString::to_string)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| store_message_id.clone());
    properties
        .entry("STORE_MESSAGE_ID".to_string())
        .or_insert(store_message_id);
    let body = message
        .body()
        .map(|body| String::from_utf8_lossy(&body).into_owned())
        .unwrap_or_default();

    MessageView {
        topic: message.topic().to_string(),
        message_id,
        keys: message
            .properties()
            .get(MessageConst::PROPERTY_KEYS)
            .map(ToString::to_string),
        tags: message.get_tags().map(|tags| tags.to_string()),
        born_timestamp: message.born_timestamp(),
        store_timestamp: message.store_timestamp(),
        born_host: message.born_host().to_string(),
        store_host: message.store_host().to_string(),
        queue_id: message.queue_id(),
        queue_offset: message.queue_offset(),
        store_size: message.store_size(),
        reconsume_times: message.reconsume_times(),
        body_crc: message.body_crc(),
        sys_flag: message.sys_flag(),
        flag: message.flag(),
        prepared_transaction_offset: message.prepared_transaction_offset(),
        body,
        properties,
    }
}

fn build_dlq_csv(messages: &[MessageView]) -> String {
    let mut csv = String::from("messageId,topic,keys,tags,storeTimestamp,queueId,queueOffset,body\n");
    for message in messages {
        csv.push_str(
            &[
                csv_escape(&message.message_id),
                csv_escape(&message.topic),
                csv_escape(message.keys.as_deref().unwrap_or_default()),
                csv_escape(message.tags.as_deref().unwrap_or_default()),
                message.store_timestamp.to_string(),
                message.queue_id.to_string(),
                message.queue_offset.to_string(),
                csv_escape(&message.body),
            ]
            .join(","),
        );
        csv.push('\n');
    }
    csv
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn kv_table_to_map(kv_table: &KVTable) -> BTreeMap<String, String> {
    kv_table
        .table
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn parse_rate_value(value: Option<&String>) -> f64 {
    value
        .and_then(|raw| raw.split_whitespace().next())
        .and_then(|token| token.parse::<f64>().ok())
        .unwrap_or_default()
}

fn select_consume_tps_value(raw_status: &BTreeMap<String, String>) -> Option<&String> {
    match raw_status.get("getTransferedTps") {
        Some(value) if !value.trim().is_empty() => Some(value),
        _ => raw_status.get("getTransferredTps"),
    }
}

fn classify_topic(topic: &str) -> &'static str {
    if topic.starts_with("%RETRY%") {
        "retry"
    } else if topic.starts_with("%DLQ%") {
        "dlq"
    } else if topic.starts_with("RMQ_SYS_")
        || topic.starts_with("SCHEDULE_TOPIC_")
        || topic == "TBW102"
        || topic == "OFFSET_MOVED_EVENT"
        || topic == "BenchmarkTest"
    {
        "system"
    } else {
        "normal"
    }
}

fn normalize_message_type(message_type: Option<&str>) -> String {
    match message_type.unwrap_or("NORMAL").trim().to_uppercase().as_str() {
        "FIFO" => "FIFO".to_string(),
        "DELAY" => "DELAY".to_string(),
        "TRANSACTION" => "TRANSACTION".to_string(),
        "UNSPECIFIED" => "UNSPECIFIED".to_string(),
        _ => "NORMAL".to_string(),
    }
}

fn build_order_conf(broker_names: &HashSet<String>, write_queue_nums: u32) -> String {
    let mut ordered_brokers: Vec<String> = broker_names.iter().cloned().collect();
    ordered_brokers.sort();
    ordered_brokers
        .into_iter()
        .map(|broker_name| format!("{broker_name}:{write_queue_nums}"))
        .collect::<Vec<_>>()
        .join(";")
}

fn unique_admin_group() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("dashboard-web-admin-{}-{millis}", std::process::id())
}

fn map_rocketmq_error(error: RocketMQError) -> DashboardError {
    DashboardError::RocketMq(error)
}

fn validate_name(value: &str, label: &str) -> Result<(), DashboardError> {
    if value.trim().is_empty() {
        return Err(DashboardError::Validation(format!("{label} cannot be empty")));
    }
    Ok(())
}

fn required_request_field<'a>(value: Option<&'a str>, label: &str) -> Result<&'a str, DashboardError> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| DashboardError::Validation(format!("{label} cannot be empty")))
}

#[cfg(test)]
mod tests {
    use super::AclTarget;
    use super::build_order_conf;
    use super::classify_topic;
    use super::map_acl_user;
    use super::map_message;
    use super::normalize_message_type;
    use super::parse_rate_value;
    use super::select_consume_tps_value;
    use super::topic_info_from_route;
    use crate::model::TopicRouteBroker;
    use crate::model::TopicRouteInfo;
    use crate::model::TopicRouteQueue;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_builder::MessageBuilder;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_remoting::protocol::body::user_info::UserInfo;
    use std::collections::BTreeMap;
    use std::collections::HashSet;

    #[test]
    fn classify_topic_matches_dashboard_categories() {
        assert_eq!(classify_topic("TopicTest"), "normal");
        assert_eq!(classify_topic("%RETRY%group-a"), "retry");
        assert_eq!(classify_topic("%DLQ%group-a"), "dlq");
        assert_eq!(classify_topic("RMQ_SYS_TRANS_HALF_TOPIC"), "system");
        assert_eq!(classify_topic("TBW102"), "system");
    }

    #[test]
    fn parse_rate_value_uses_first_token() {
        assert_eq!(parse_rate_value(Some(&"12.50 1min".to_string())), 12.50);
        assert_eq!(parse_rate_value(Some(&"not-number".to_string())), 0.0);
        assert_eq!(parse_rate_value(None), 0.0);
    }

    #[test]
    fn select_consume_tps_keeps_java_legacy_spelling_priority() {
        let raw_status = BTreeMap::from([
            ("getTransferedTps".to_string(), "7.50 1min".to_string()),
            ("getTransferredTps".to_string(), "8.25 1min".to_string()),
        ]);

        assert_eq!(
            select_consume_tps_value(&raw_status).map(String::as_str),
            Some("7.50 1min")
        );
    }

    #[test]
    fn normalize_message_type_matches_java_dashboard_values() {
        assert_eq!(normalize_message_type(None), "NORMAL");
        assert_eq!(normalize_message_type(Some("fifo")), "FIFO");
        assert_eq!(normalize_message_type(Some("delay")), "DELAY");
        assert_eq!(normalize_message_type(Some("transaction")), "TRANSACTION");
        assert_eq!(normalize_message_type(Some("unknown")), "NORMAL");
    }

    #[test]
    fn build_order_conf_is_sorted() {
        let brokers = HashSet::from(["broker-b".to_string(), "broker-a".to_string()]);

        assert_eq!(build_order_conf(&brokers, 8), "broker-a:8;broker-b:8");
    }

    #[test]
    fn map_acl_user_does_not_expose_password() {
        let target = AclTarget {
            broker_name: "broker-a".to_string(),
            broker_addr: "127.0.0.1:10911".to_string(),
        };
        let user = UserInfo {
            username: Some(CheetahString::from("alice")),
            password: Some(CheetahString::from("broker-password")),
            user_type: Some(CheetahString::from("Normal")),
            user_status: Some(CheetahString::from("enable")),
        };

        let view = map_acl_user(&target, &user);

        assert_eq!(view.username, "alice");
        assert_eq!(view.password, None);
    }

    #[test]
    fn map_message_populates_dashboard_message_info_fields() {
        let message = MessageBuilder::new()
            .topic("TopicTest")
            .body_slice(b"hello")
            .tags("TagA")
            .key("KeyA")
            .flag_bits(9)
            .build_unchecked();
        let mut message_ext = MessageExt::default();
        message_ext.set_message_inner(message);
        message_ext.set_msg_id(CheetahString::from_static_str("store-msg-id"));
        message_ext.set_born_host("172.20.48.1:61266".parse().expect("born host"));
        message_ext.set_store_host("172.20.48.1:10911".parse().expect("store host"));
        message_ext.set_store_size(128);
        message_ext.set_reconsume_times(2);
        message_ext.set_body_crc(613_185_359);
        message_ext.set_sys_flag(7);
        message_ext.set_prepared_transaction_offset(42);

        let view = map_message(&message_ext);

        assert_eq!(view.born_host, "172.20.48.1:61266");
        assert_eq!(view.store_host, "172.20.48.1:10911");
        assert_eq!(view.store_size, 128);
        assert_eq!(view.reconsume_times, 2);
        assert_eq!(view.body_crc, 613_185_359);
        assert_eq!(view.sys_flag, 7);
        assert_eq!(view.flag, 9);
        assert_eq!(view.prepared_transaction_offset, 42);
        assert_eq!(
            serde_json::to_value(&view).expect("message view should serialize")["bodyCRC"],
            613_185_359
        );
    }

    #[test]
    fn topic_info_from_route_populates_list_row_metadata() {
        let route = TopicRouteInfo {
            topic: "TopicTest".to_string(),
            brokers: vec![TopicRouteBroker {
                broker_name: "broker-a".to_string(),
                broker_addrs: vec!["127.0.0.1:10911".to_string()],
            }],
            queues: vec![
                TopicRouteQueue {
                    broker_name: "broker-a".to_string(),
                    read_queue_nums: 4,
                    write_queue_nums: 4,
                    perm: 6,
                },
                TopicRouteQueue {
                    broker_name: "broker-b".to_string(),
                    read_queue_nums: 1,
                    write_queue_nums: 1,
                    perm: 7,
                },
            ],
        };

        let topic = topic_info_from_route("TopicTest", Some(&route));

        assert_eq!(topic.broker_name.as_deref(), Some("broker-a"));
        assert_eq!(topic.read_queue_count, 5);
        assert_eq!(topic.write_queue_count, 5);
        assert_eq!(topic.perm, 7);
        assert_eq!(topic.category, "normal");
    }
}

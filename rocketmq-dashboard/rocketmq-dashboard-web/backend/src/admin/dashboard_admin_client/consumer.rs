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

use rocketmq_admin_core::core::AdminError;
use rocketmq_admin_core::core::consumer as core_consumer;
use rocketmq_admin_core::core::consumer::ConsumerDiagnosticAdmin;
use rocketmq_admin_core::core::consumer::ConsumerQueryAdmin;

use super::*;
use crate::model::*;

impl DashboardAdminClient {
    pub async fn consumer_group_list(&self, query: ConsumerQuery) -> Result<ConsumerGroupListView, DashboardError> {
        let config = self.config.read().await.clone();
        let scope = resolve_consumer_query_scope(&config, &query)?;
        let address = scope.address.clone();
        let skip_system = query.skip_system.unwrap_or(false);
        let list = run_consumer_admin_rpc!(self, |admin| async move {
            let request = core_consumer::DashboardConsumerGroupListRequest {
                skip_sys_group: skip_system,
                address,
            };
            admin.query_dashboard_consumer_groups(&request).await
        })?;

        Ok(ConsumerGroupListView {
            items: list.items.into_iter().map(map_consumer_group_list_item).collect(),
            total: 0,
            query_scope: scope,
            capabilities: ConsumerCapabilities {
                connections: true,
                progress: true,
                configuration: true,
                running_info: true,
                jstack: true,
            },
        })
    }

    pub async fn consumer_summary(
        &self,
        group: &str,
        query: ConsumerQuery,
    ) -> Result<ConsumerSummaryView, DashboardError> {
        let group = normalize_consumer_group(group)?;
        let config = self.config.read().await.clone();
        let scope = resolve_consumer_query_scope(&config, &query)?;
        let address = scope.address.clone();
        let list = run_consumer_admin_rpc!(self, |admin| async move {
            let request = core_consumer::DashboardConsumerGroupListRequest {
                skip_sys_group: false,
                address,
            };
            admin.query_dashboard_consumer_groups(&request).await
        })?;
        let item = list
            .items
            .into_iter()
            .find(|item| item.raw_group_name == group || item.display_group_name == group)
            .ok_or_else(|| DashboardError::NotFound(format!("Consumer group `{group}` was not found")))?;

        Ok(map_consumer_summary(item, scope))
    }

    pub async fn consumer_connections(
        &self,
        group: &str,
        query: ConsumerQuery,
    ) -> Result<ConsumerConnectionView, DashboardError> {
        let group = normalize_consumer_group(group)?;
        let config = self.config.read().await.clone();
        let scope = resolve_consumer_query_scope(&config, &query)?;
        let address = scope.address.clone();
        let connection = run_consumer_admin_rpc!(self, |admin| async move {
            let request = core_consumer::DashboardConsumerConnectionRequest {
                consumer_group: group.clone(),
                address,
            };
            admin.query_dashboard_consumer_connection(&request).await
        })?;

        Ok(map_consumer_connection(connection, scope))
    }

    pub async fn consumer_progress_view(
        &self,
        group: &str,
        query: ConsumerQuery,
    ) -> Result<ConsumerProgressView, DashboardError> {
        let group = normalize_consumer_group(group)?;
        let config = self.config.read().await.clone();
        let scope = resolve_consumer_query_scope(&config, &query)?;
        let address = scope.address.clone();
        let progress = run_consumer_admin_rpc!(self, |admin| async move {
            let request = core_consumer::DashboardConsumerProgressRequest {
                consumer_group: group.clone(),
                address,
            };
            admin.query_dashboard_consumer_progress(&request).await
        })?;

        Ok(map_consumer_progress(progress, scope))
    }

    pub async fn consumer_config_view(
        &self,
        group: &str,
        query: ConsumerQuery,
    ) -> Result<ConsumerConfigView, DashboardError> {
        let group = normalize_consumer_group(group)?;
        let config = self.config.read().await.clone();
        let scope = resolve_consumer_query_scope(&config, &query)?;
        let group_owned = group.clone();
        let fetches = run_consumer_admin_rpc!(self, |admin| async move {
            let list_request = core_consumer::DashboardConsumerGroupListRequest {
                skip_sys_group: false,
                address: None,
            };
            let list = admin.query_dashboard_consumer_groups(&list_request).await?;
            let item = list
                .items
                .into_iter()
                .find(|item| item.raw_group_name == group_owned || item.display_group_name == group_owned)
                .ok_or_else(|| AdminError::not_found("consumerGroup", group_owned.clone()))?;

            let mut fetches = Vec::with_capacity(item.broker_addresses.len());
            for broker_address in item.broker_addresses {
                let request = core_consumer::DashboardConsumerConfigRequest {
                    consumer_group: group_owned.clone(),
                    address: Some(broker_address.clone()),
                };
                let result = admin.query_dashboard_consumer_config(&request).await;
                fetches.push(ConsumerConfigTargetFetch { broker_address, result });
            }
            Ok::<_, AdminError>(fetches)
        })?;

        Ok(map_consumer_config(&group, fetches, scope))
    }

    pub async fn consumer_running_info(
        &self,
        group: &str,
        client_id: &str,
        query: ConsumerQuery,
        include_jstack: bool,
    ) -> Result<ConsumerRunningInfoView, DashboardError> {
        let group = normalize_consumer_group(group)?;
        self.resolve_and_query_running_info(&group, client_id, query, include_jstack)
            .await
    }

    pub async fn consumer_jstack(
        &self,
        group: &str,
        client_id: &str,
        query: ConsumerQuery,
    ) -> Result<ConsumerJStackView, DashboardError> {
        let group = normalize_consumer_group(group)?;
        let running_info = self
            .resolve_and_query_running_info(&group, client_id, query, true)
            .await?;
        Ok(ConsumerJStackView {
            consumer_group: running_info.consumer_group,
            client_id: running_info.client_id,
            jstack: running_info.jstack,
            truncated: running_info.truncated,
        })
    }

    async fn resolve_and_query_running_info(
        &self,
        group: &str,
        client_id: &str,
        query: ConsumerQuery,
        include_jstack: bool,
    ) -> Result<ConsumerRunningInfoView, DashboardError> {
        let config = self.config.read().await.clone();
        let _scope = resolve_consumer_query_scope(&config, &query)?;
        let group = group.to_string();
        let client_id = client_id.trim().to_string();
        let request =
            core_consumer::DashboardConsumerRunningInfoRequest::try_new(group, client_id, include_jstack, 1024 * 1024)?;
        let running_info =
            run_consumer_admin_rpc!(self, |admin| { admin.query_dashboard_consumer_running_info(&request) })?;
        Ok(map_consumer_running_info(running_info))
    }
}

pub(super) fn resolve_consumer_query_scope(
    config: &DashboardConfigView,
    query: &ConsumerQuery,
) -> Result<ConsumerQueryScope, DashboardError> {
    match query.mode {
        ConsumerQueryMode::NameServer => Ok(ConsumerQueryScope {
            mode: ConsumerQueryMode::NameServer,
            address: None,
        }),
        ConsumerQueryMode::Proxy => {
            let requested = query
                .proxy_address
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());
            let address = match requested {
                Some(address) => {
                    if !config
                        .proxy_addr_list
                        .iter()
                        .any(|candidate| candidate.trim() == address)
                    {
                        return Err(DashboardError::Validation(format!(
                            "Proxy address `{address}` is not configured"
                        )));
                    }
                    address.to_string()
                }
                None => config
                    .current_proxy_addr
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        DashboardError::Validation(
                            "Proxy Client mode requires a configured current Proxy endpoint".to_string(),
                        )
                    })?,
            };
            Ok(ConsumerQueryScope {
                mode: ConsumerQueryMode::Proxy,
                address: Some(address),
            })
        }
    }
}

pub(super) struct ConsumerConfigTargetFetch {
    broker_address: String,
    result: Result<core_consumer::DashboardConsumerConfig, AdminError>,
}

fn map_consumer_group_list_item(item: core_consumer::DashboardConsumerGroupItem) -> ConsumerGroupListItem {
    ConsumerGroupListItem {
        display_group_name: item.display_group_name,
        raw_group_name: item.raw_group_name,
        category: item.category,
        connection_count: item.connection_count,
        consume_tps: item.consume_tps,
        diff_total: item.diff_total,
        message_model: item.message_model,
        consume_type: item.consume_type,
        version: item.version,
        version_desc: item.version_desc,
        broker_names: item.broker_names,
        broker_addresses: item.broker_addresses,
        update_timestamp: item.update_timestamp,
    }
}

fn map_consumer_summary(
    item: core_consumer::DashboardConsumerGroupItem,
    query_scope: ConsumerQueryScope,
) -> ConsumerSummaryView {
    ConsumerSummaryView {
        group: item.raw_group_name.clone(),
        display_group_name: item.display_group_name,
        category: item.category,
        connection_count: item.connection_count,
        consume_tps: item.consume_tps,
        diff_total: item.diff_total,
        message_model: item.message_model,
        consume_type: item.consume_type,
        version: item.version,
        version_desc: item.version_desc,
        broker_names: item.broker_names,
        broker_addresses: item.broker_addresses,
        update_timestamp: item.update_timestamp,
        query_scope,
    }
}

fn map_consumer_connection(
    connection: core_consumer::DashboardConsumerConnection,
    query_scope: ConsumerQueryScope,
) -> ConsumerConnectionView {
    ConsumerConnectionView {
        group: connection.consumer_group,
        connection_count: connection.connection_count,
        consume_type: connection.consume_type,
        message_model: connection.message_model,
        consume_from_where: connection.consume_from_where,
        connections: connection
            .connections
            .into_iter()
            .map(|item| ConsumerConnectionItem {
                client_id: item.client_id,
                client_addr: item.client_addr,
                language: item.language,
                version: item.version,
                version_desc: item.version_desc,
                capabilities: ConsumerClientCapabilities {
                    running_info: true,
                    jstack: true,
                    running_info_reason: None,
                    jstack_reason: None,
                },
            })
            .collect(),
        subscriptions: connection
            .subscriptions
            .into_iter()
            .map(|item| ConsumerSubscriptionItem {
                topic: item.topic,
                sub_string: item.sub_string,
                expression_type: item.expression_type,
                tags_set: item.tags_set,
                code_set: item.code_set,
                sub_version: item.sub_version,
            })
            .collect(),
        query_scope,
    }
}

fn map_consumer_progress(
    progress: core_consumer::DashboardConsumerProgress,
    query_scope: ConsumerQueryScope,
) -> ConsumerProgressView {
    ConsumerProgressView {
        group: progress.consumer_group,
        topic_count: progress.topic_count,
        total_diff: progress.total_diff,
        topics: progress
            .topics
            .into_iter()
            .map(|topic| ConsumerProgressTopic {
                topic: topic.topic,
                diff_total: topic.diff_total,
                last_timestamp: topic.last_timestamp,
                queues: topic
                    .queues
                    .into_iter()
                    .map(|queue| ConsumerProgressTopicQueue {
                        broker_name: queue.broker_name,
                        queue_id: queue.queue_id,
                        broker_offset: queue.broker_offset,
                        consumer_offset: queue.consumer_offset,
                        diff_total: queue.diff_total,
                        client_info: queue.client_info,
                        last_timestamp: queue.last_timestamp,
                    })
                    .collect(),
            })
            .collect(),
        query_scope,
    }
}

fn map_consumer_config(
    group: &str,
    fetches: Vec<ConsumerConfigTargetFetch>,
    query_scope: ConsumerQueryScope,
) -> ConsumerConfigView {
    let mut targets = Vec::with_capacity(fetches.len());
    for fetch in fetches {
        match fetch.result {
            Ok(config) => targets.push(ConsumerConfigTarget {
                broker_name: config.broker_name.clone(),
                broker_address: config.broker_address.clone(),
                config: Some(map_consumer_config_value(&config)),
                subscription_topics: config.subscription_topics,
                attributes: config
                    .attributes
                    .into_iter()
                    .map(map_consumer_config_attribute)
                    .collect(),
                error: None,
            }),
            Err(error) => targets.push(ConsumerConfigTarget {
                broker_name: fetch.broker_address.clone(),
                broker_address: fetch.broker_address,
                config: None,
                subscription_topics: Vec::new(),
                attributes: Vec::new(),
                error: Some(error.to_string()),
            }),
        }
    }

    let successful = targets
        .iter()
        .filter_map(|target| target.config.as_ref())
        .collect::<Vec<_>>();
    let effective = successful.first().map(|first| {
        let mut effective = (*first).clone();
        if successful.iter().any(|value| *value != &effective) {
            effective = ConsumerConfigValue::default();
        }
        effective
    });
    let inconsistent_fields = collect_inconsistent_config_fields(successful.as_slice());

    ConsumerConfigView {
        group: group.to_string(),
        effective,
        inconsistent_fields,
        targets,
        query_scope,
    }
}

fn map_consumer_config_value(config: &core_consumer::DashboardConsumerConfig) -> ConsumerConfigValue {
    ConsumerConfigValue {
        consume_enable: config.consume_enable,
        consume_from_min_enable: config.consume_from_min_enable,
        consume_broadcast_enable: config.consume_broadcast_enable,
        consume_message_orderly: config.consume_message_orderly,
        retry_queue_nums: config.retry_queue_nums,
        retry_max_times: config.retry_max_times,
        broker_id: config.broker_id,
        which_broker_when_consume_slowly: config.which_broker_when_consume_slowly,
        notify_consumer_ids_changed_enable: config.notify_consumer_ids_changed_enable,
        group_sys_flag: config.group_sys_flag,
        consume_timeout_minute: config.consume_timeout_minute,
        group_retry_policy_json: config.group_retry_policy_json.clone(),
    }
}

fn map_consumer_config_attribute(
    attribute: core_consumer::DashboardConsumerConfigAttribute,
) -> ConsumerConfigAttribute {
    ConsumerConfigAttribute {
        key: attribute.key,
        value: attribute.value,
    }
}

fn collect_inconsistent_config_fields(values: &[&ConsumerConfigValue]) -> Vec<String> {
    let Some(first) = values.first() else {
        return Vec::new();
    };
    let mut fields = Vec::new();
    if values.iter().any(|value| value.consume_enable != first.consume_enable) {
        fields.push("consumeEnable".to_string());
    }
    if values
        .iter()
        .any(|value| value.consume_from_min_enable != first.consume_from_min_enable)
    {
        fields.push("consumeFromMinEnable".to_string());
    }
    if values
        .iter()
        .any(|value| value.consume_broadcast_enable != first.consume_broadcast_enable)
    {
        fields.push("consumeBroadcastEnable".to_string());
    }
    if values
        .iter()
        .any(|value| value.consume_message_orderly != first.consume_message_orderly)
    {
        fields.push("consumeMessageOrderly".to_string());
    }
    if values
        .iter()
        .any(|value| value.retry_queue_nums != first.retry_queue_nums)
    {
        fields.push("retryQueueNums".to_string());
    }
    if values
        .iter()
        .any(|value| value.retry_max_times != first.retry_max_times)
    {
        fields.push("retryMaxTimes".to_string());
    }
    if values.iter().any(|value| value.broker_id != first.broker_id) {
        fields.push("brokerId".to_string());
    }
    if values
        .iter()
        .any(|value| value.which_broker_when_consume_slowly != first.which_broker_when_consume_slowly)
    {
        fields.push("whichBrokerWhenConsumeSlowly".to_string());
    }
    if values
        .iter()
        .any(|value| value.notify_consumer_ids_changed_enable != first.notify_consumer_ids_changed_enable)
    {
        fields.push("notifyConsumerIdsChangedEnable".to_string());
    }
    if values.iter().any(|value| value.group_sys_flag != first.group_sys_flag) {
        fields.push("groupSysFlag".to_string());
    }
    if values
        .iter()
        .any(|value| value.consume_timeout_minute != first.consume_timeout_minute)
    {
        fields.push("consumeTimeoutMinute".to_string());
    }
    if values
        .iter()
        .any(|value| value.group_retry_policy_json != first.group_retry_policy_json)
    {
        fields.push("groupRetryPolicyJson".to_string());
    }
    fields
}

fn map_consumer_running_info(running_info: core_consumer::DashboardConsumerRunningInfo) -> ConsumerRunningInfoView {
    ConsumerRunningInfoView {
        consumer_group: running_info.consumer_group,
        client_id: running_info.client_id,
        properties: running_info
            .properties
            .into_iter()
            .map(map_consumer_config_attribute)
            .collect(),
        subscriptions: running_info
            .subscriptions
            .into_iter()
            .map(|item| ConsumerSubscriptionItem {
                topic: item.topic,
                sub_string: item.sub_string,
                expression_type: item.expression_type,
                tags_set: item.tags_set,
                code_set: item.code_set,
                sub_version: item.sub_version,
            })
            .collect(),
        process_queues: running_info
            .process_queues
            .into_iter()
            .map(|queue| ConsumerProcessQueue {
                topic: queue.topic,
                broker_name: queue.broker_name,
                queue_id: queue.queue_id,
                cached_message_count: queue.cached_message_count,
                cached_message_size_in_mib: queue.cached_message_size_in_mib,
                commit_offset: queue.commit_offset,
                dropped: queue.dropped,
                last_consume_timestamp: queue.last_consume_timestamp,
            })
            .collect(),
        jstack: running_info.jstack,
        truncated: running_info.truncated,
    }
}

fn normalize_consumer_group(group: &str) -> Result<String, DashboardError> {
    let group = group.trim();
    if group.is_empty() {
        return Err(DashboardError::Validation("Consumer group is required".to_string()));
    }
    Ok(group.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_proxies(proxies: &[&str], current: Option<&str>) -> DashboardConfigView {
        DashboardConfigView {
            proxy_addr_list: proxies.iter().map(|value| (*value).to_string()).collect(),
            current_proxy_addr: current.map(str::to_string),
            ..DashboardConfigView::default()
        }
    }

    #[test]
    fn proxy_scope_requires_the_configured_current_endpoint() {
        let config = config_with_proxies(&["proxy-a:8081"], Some("proxy-a:8081"));
        assert_eq!(
            resolve_consumer_query_scope(&config, &ConsumerQuery::proxy(None))
                .unwrap()
                .address(),
            Some("proxy-a:8081")
        );
        assert!(
            resolve_consumer_query_scope(&config, &ConsumerQuery::proxy(Some("proxy-b:8081".to_string()))).is_err()
        );
        assert!(
            resolve_consumer_query_scope(
                &config_with_proxies(&["proxy-a:8081"], None),
                &ConsumerQuery::proxy(None)
            )
            .is_err()
        );
    }

    impl ConsumerQuery {
        fn proxy(address: Option<String>) -> Self {
            Self {
                mode: ConsumerQueryMode::Proxy,
                proxy_address: address,
                skip_system: None,
            }
        }
    }
}

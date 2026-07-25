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

use crate::consumer::admin::ManagedConsumerAdmin;
use crate::consumer::types::ConsumerConfigView;
use crate::consumer::types::ConsumerConnectionView;
use crate::consumer::types::ConsumerError;
use crate::consumer::types::ConsumerGroupListItem;
use crate::consumer::types::ConsumerGroupListResponse;
use crate::consumer::types::ConsumerMutationResult;
use crate::consumer::types::ConsumerResult;
use crate::consumer::types::ConsumerTopicDetailView;
use crate::nameserver::NameServerRuntimeState;
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::core::AdminError;
use rocketmq_admin_core::core::consumer::ConsumerAdmin;
use rocketmq_admin_core::core::consumer::DashboardConsumerConfigRequest;
use rocketmq_admin_core::core::consumer::DashboardConsumerConnectionRequest;
use rocketmq_admin_core::core::consumer::DashboardConsumerDeleteRequest;
use rocketmq_admin_core::core::consumer::DashboardConsumerGroupListRequest;
use rocketmq_admin_core::core::consumer::DashboardConsumerProgressRequest;
use rocketmq_admin_core::core::consumer::DashboardConsumerUpsertRequest;
use rocketmq_dashboard_common::ConsumerConfigQueryRequest;
use rocketmq_dashboard_common::ConsumerConnectionQueryRequest;
use rocketmq_dashboard_common::ConsumerCreateOrUpdateRequest;
use rocketmq_dashboard_common::ConsumerDeleteRequest;
use rocketmq_dashboard_common::ConsumerGroupListRequest;
use rocketmq_dashboard_common::ConsumerGroupRefreshRequest;
use rocketmq_dashboard_common::ConsumerTopicDetailQueryRequest;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use std::sync::Arc;
use tokio::sync::Mutex;

mod mapping;

use self::mapping::build_summary;
use self::mapping::map_admin_error;
use self::mapping::map_consumer_config_view;
use self::mapping::map_consumer_connection_view;
use self::mapping::map_consumer_group_item;
use self::mapping::map_consumer_mutation_result;
use self::mapping::map_consumer_topic_detail_view;

#[derive(Clone)]
pub(crate) struct ConsumerManager {
    runtime: Arc<NameServerRuntimeState>,
    admin_session: Arc<Mutex<Option<ManagedConsumerAdmin>>>,
}

impl ConsumerManager {
    pub(crate) fn new(runtime: Arc<NameServerRuntimeState>) -> Self {
        Self {
            runtime,
            admin_session: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) async fn shutdown(&self) {
        let mut session = self.admin_session.lock().await;
        if let Some(mut admin) = session.take() {
            admin.shutdown().await;
        }
    }

    pub(crate) async fn query_consumer_groups(
        &self,
        request: ConsumerGroupListRequest,
    ) -> ConsumerResult<ConsumerGroupListResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let snapshot = session_guard
                .as_ref()
                .expect("consumer admin session should be initialized before use")
                .snapshot
                .clone();
            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("consumer admin session should be initialized before use");
                self.query_consumer_groups_with_admin(&mut session.admin, &snapshot, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_consumer_groups failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_consumer_groups` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn refresh_consumer_group(
        &self,
        request: ConsumerGroupRefreshRequest,
    ) -> ConsumerResult<ConsumerGroupListItem> {
        if request.consumer_group.trim().is_empty() {
            return Err(ConsumerError::Validation("Consumer group is required.".into()));
        }

        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("consumer admin session should be initialized before use");
                self.refresh_consumer_group_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "refresh_consumer_group failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `refresh_consumer_group` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn refresh_all_consumer_groups(
        &self,
        request: ConsumerGroupListRequest,
    ) -> ConsumerResult<ConsumerGroupListResponse> {
        self.query_consumer_groups(request).await
    }

    pub(crate) async fn query_consumer_connection(
        &self,
        request: ConsumerConnectionQueryRequest,
    ) -> ConsumerResult<ConsumerConnectionView> {
        let group_name = validate_consumer_group_name(&request.consumer_group)?;

        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("consumer admin session should be initialized before use");
                self.query_consumer_connection_with_admin(&mut session.admin, &group_name, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_consumer_connection failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_consumer_connection` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn query_consumer_topic_detail(
        &self,
        request: ConsumerTopicDetailQueryRequest,
    ) -> ConsumerResult<ConsumerTopicDetailView> {
        let group_name = validate_consumer_group_name(&request.consumer_group)?;

        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("consumer admin session should be initialized before use");
                self.query_consumer_topic_detail_with_admin(&mut session.admin, &group_name, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_consumer_topic_detail failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_consumer_topic_detail` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn query_consumer_config(
        &self,
        request: ConsumerConfigQueryRequest,
    ) -> ConsumerResult<ConsumerConfigView> {
        let group_name = validate_consumer_group_name(&request.consumer_group)?;

        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("consumer admin session should be initialized before use");
                self.query_consumer_config_with_admin(&mut session.admin, &group_name, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_consumer_config failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_consumer_config` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn create_or_update_consumer_group(
        &self,
        request: ConsumerCreateOrUpdateRequest,
    ) -> ConsumerResult<ConsumerMutationResult> {
        let group_name = validate_consumer_group_name(&request.consumer_group)?;
        validate_consumer_targets(&request.cluster_name_list, &request.broker_name_list)?;
        validate_consumer_limits(&request)?;

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;

        let result = {
            let session = session_guard
                .as_mut()
                .expect("consumer admin session should be initialized before use");
            self.create_or_update_consumer_group_with_admin(&mut session.admin, &group_name, request)
                .await
        };

        let should_reset = Self::should_reset_session(&result);
        if should_reset {
            self.reset_admin_session(&mut session_guard, "create_or_update_consumer_group failed")
                .await;
        }
        drop(session_guard);

        result
    }

    pub(crate) async fn delete_consumer_group(
        &self,
        request: ConsumerDeleteRequest,
    ) -> ConsumerResult<ConsumerMutationResult> {
        let group_name = validate_consumer_group_name(&request.consumer_group)?;
        if request.broker_name_list.is_empty() {
            return Err(ConsumerError::Validation(
                "Select at least one broker before deleting the consumer group.".into(),
            ));
        }

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;

        let result = {
            let session = session_guard
                .as_mut()
                .expect("consumer admin session should be initialized before use");
            self.delete_consumer_group_with_admin(&mut session.admin, &group_name, request)
                .await
        };

        let should_reset = Self::should_reset_session(&result);
        if should_reset {
            self.reset_admin_session(&mut session_guard, "delete_consumer_group failed")
                .await;
        }
        drop(session_guard);

        result
    }

    async fn ensure_admin_session(&self, session_slot: &mut Option<ManagedConsumerAdmin>) -> ConsumerResult<()> {
        let generation = self.runtime.generation();
        let needs_reconnect = session_slot
            .as_ref()
            .is_none_or(|session| !session.matches_generation(generation));

        if needs_reconnect {
            self.reset_admin_session(session_slot, "refreshing consumer admin session")
                .await;
            let session = ManagedConsumerAdmin::connect(&self.runtime).await?;
            log::info!(
                "Connected consumer admin session for namesrv `{}` at generation {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                session.generation
            );
            *session_slot = Some(session);
        }

        Ok(())
    }

    async fn reset_admin_session(&self, session_slot: &mut Option<ManagedConsumerAdmin>, reason: &str) {
        if let Some(mut session) = session_slot.take() {
            log::info!(
                "Shutting down consumer admin session for namesrv `{}`: {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                reason
            );
            session.shutdown().await;
        }
    }

    fn should_reset_session<T>(result: &ConsumerResult<T>) -> bool {
        match result {
            Err(ConsumerError::Admin(error)) => error.is_retryable() || matches!(error, AdminError::SessionClosed),
            _ => false,
        }
    }

    async fn query_consumer_groups_with_admin(
        &self,
        admin: &mut AdminSession,
        snapshot: &NameServerConfigSnapshot,
        request: ConsumerGroupListRequest,
    ) -> ConsumerResult<ConsumerGroupListResponse> {
        let result = admin
            .query_dashboard_consumer_groups(&DashboardConsumerGroupListRequest {
                skip_sys_group: request.skip_sys_group,
                address: request.address,
            })
            .await
            .map_err(map_admin_error)?;
        let items = result
            .items
            .into_iter()
            .map(map_consumer_group_item)
            .collect::<Vec<_>>();
        let summary = build_summary(&items);

        Ok(ConsumerGroupListResponse {
            items,
            summary,
            current_namesrv: snapshot.current_namesrv.clone().unwrap_or_default(),
            use_vip_channel: snapshot.use_vip_channel,
            use_tls: snapshot.use_tls,
        })
    }

    async fn refresh_consumer_group_with_admin(
        &self,
        admin: &mut AdminSession,
        request: ConsumerGroupRefreshRequest,
    ) -> ConsumerResult<ConsumerGroupListItem> {
        let raw_group_name = strip_system_prefix(&request.consumer_group);
        let result = admin
            .query_dashboard_consumer_groups(&DashboardConsumerGroupListRequest {
                skip_sys_group: false,
                address: request.address,
            })
            .await
            .map_err(map_admin_error)?;
        result
            .items
            .into_iter()
            .find(|item| item.raw_group_name == raw_group_name)
            .map(map_consumer_group_item)
            .ok_or_else(|| {
                ConsumerError::Validation(format!(
                    "Consumer group `{}` was not found in the current cluster view.",
                    request.consumer_group
                ))
            })
    }

    async fn query_consumer_connection_with_admin(
        &self,
        admin: &mut AdminSession,
        raw_group_name: &str,
        request: ConsumerConnectionQueryRequest,
    ) -> ConsumerResult<ConsumerConnectionView> {
        admin
            .query_dashboard_consumer_connection(&DashboardConsumerConnectionRequest {
                consumer_group: raw_group_name.to_string(),
                address: request.address,
            })
            .await
            .map(map_consumer_connection_view)
            .map_err(map_admin_error)
    }

    async fn query_consumer_topic_detail_with_admin(
        &self,
        admin: &mut AdminSession,
        raw_group_name: &str,
        request: ConsumerTopicDetailQueryRequest,
    ) -> ConsumerResult<ConsumerTopicDetailView> {
        admin
            .query_dashboard_consumer_progress(&DashboardConsumerProgressRequest {
                consumer_group: raw_group_name.to_string(),
                address: request.address,
            })
            .await
            .map(map_consumer_topic_detail_view)
            .map_err(map_admin_error)
    }

    async fn query_consumer_config_with_admin(
        &self,
        admin: &mut AdminSession,
        raw_group_name: &str,
        request: ConsumerConfigQueryRequest,
    ) -> ConsumerResult<ConsumerConfigView> {
        admin
            .query_dashboard_consumer_config(&DashboardConsumerConfigRequest {
                consumer_group: raw_group_name.to_string(),
                address: request.address,
            })
            .await
            .map(map_consumer_config_view)
            .map_err(map_admin_error)
    }

    async fn create_or_update_consumer_group_with_admin(
        &self,
        admin: &mut AdminSession,
        raw_group_name: &str,
        request: ConsumerCreateOrUpdateRequest,
    ) -> ConsumerResult<ConsumerMutationResult> {
        admin
            .upsert_dashboard_consumer_group(&DashboardConsumerUpsertRequest {
                cluster_name_list: request.cluster_name_list,
                broker_name_list: request.broker_name_list,
                consumer_group: raw_group_name.to_string(),
                consume_enable: request.consume_enable,
                consume_from_min_enable: request.consume_from_min_enable,
                consume_broadcast_enable: request.consume_broadcast_enable,
                consume_message_orderly: request.consume_message_orderly,
                retry_queue_nums: request.retry_queue_nums,
                retry_max_times: request.retry_max_times,
                broker_id: request.broker_id,
                which_broker_when_consume_slowly: request.which_broker_when_consume_slowly,
                notify_consumer_ids_changed_enable: request.notify_consumer_ids_changed_enable,
                group_sys_flag: request.group_sys_flag,
                consume_timeout_minute: request.consume_timeout_minute,
            })
            .await
            .map(map_consumer_mutation_result)
            .map_err(map_admin_error)
    }

    async fn delete_consumer_group_with_admin(
        &self,
        admin: &mut AdminSession,
        raw_group_name: &str,
        request: ConsumerDeleteRequest,
    ) -> ConsumerResult<ConsumerMutationResult> {
        admin
            .delete_dashboard_consumer_group(&DashboardConsumerDeleteRequest {
                consumer_group: raw_group_name.to_string(),
                broker_name_list: request.broker_name_list,
            })
            .await
            .map(map_consumer_mutation_result)
            .map_err(map_admin_error)
    }
}

fn strip_system_prefix(group_name: &str) -> String {
    group_name
        .strip_prefix("%SYS%")
        .unwrap_or(group_name)
        .trim()
        .to_string()
}

fn validate_consumer_group_name(group_name: &str) -> ConsumerResult<String> {
    let normalized = strip_system_prefix(group_name);
    if normalized.trim().is_empty() {
        Err(ConsumerError::Validation("Consumer group is required.".into()))
    } else {
        Ok(normalized)
    }
}

fn validate_consumer_targets(cluster_name_list: &[String], broker_name_list: &[String]) -> ConsumerResult<()> {
    if cluster_name_list.iter().all(|value| value.trim().is_empty())
        && broker_name_list.iter().all(|value| value.trim().is_empty())
    {
        return Err(ConsumerError::Validation(
            "Select at least one cluster or broker before saving the consumer group.".into(),
        ));
    }
    Ok(())
}

fn validate_consumer_limits(request: &ConsumerCreateOrUpdateRequest) -> ConsumerResult<()> {
    if request.retry_queue_nums < 0 {
        return Err(ConsumerError::Validation(
            "Retry queues must be zero or greater.".into(),
        ));
    }
    if request.retry_max_times < -1 {
        return Err(ConsumerError::Validation("Max retries must be -1 or greater.".into()));
    }
    if request.consume_timeout_minute <= 0 {
        return Err(ConsumerError::Validation(
            "Consume timeout must be greater than zero.".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::strip_system_prefix;

    #[test]
    fn strip_system_prefix_handles_prefixed_group_names() {
        assert_eq!(strip_system_prefix("%SYS%TOOLS_CONSUMER"), "TOOLS_CONSUMER");
        assert_eq!(strip_system_prefix("group-a"), "group-a");
    }
}

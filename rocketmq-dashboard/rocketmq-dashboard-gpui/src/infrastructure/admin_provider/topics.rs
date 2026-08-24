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

//! Topic-specific provider boundary. Every admin response is allowlisted here
//! before it can reach application services.

use std::{fmt, sync::Arc};

use rocketmq_admin_core::core::topic::{
    DeleteTopicsInBrokerRequest, DetailedTopicCatalog, DetailedTopicCatalogItem, DetailedTopicConsumers,
    DetailedTopicStats, PatchTopicConfigOutcome, PatchTopicConfigRequest, QueryTopicConfigCasRequest,
    TopicBatchDeleteRequest, TopicBatchMutationOutcome, TopicBatchUpsertRequest, TopicConfigCasPatch,
    TopicConfigCasState, TopicInspectionCompleteness, TopicInspectionFailure, TopicInspectionFailureCode,
    TopicInspectionStage, TopicMutationOutcome, TopicOffsetMutationFailureCode, TopicOffsetMutationOutcome,
    TopicOffsetMutationRequest, TopicRoute, TopicSendRequest,
};
use rocketmq_dashboard_common::{
    TopicCategory, TopicCompleteness, TopicConfigField, TopicConfigTargetView, TopicConfigView, TopicConsumerView,
    TopicConsumersView, TopicFailureCode, TopicFailureStage, TopicIdentity, TopicInventory, TopicInventoryItem,
    TopicMessageType, TopicMutationGuarantee, TopicMutationKind, TopicPartialOutcome, TopicPermission,
    TopicQueueOffsetView, TopicRouteBrokerView, TopicRouteQueueView, TopicRouteView, TopicStatsView,
    TopicTargetFailure, TopicTargetIdentity, TopicTargetOutcome,
};

use super::{
    GpuiAdminProvider, ProviderError, ProviderErrorCode, mutation_for_revision, query_for_revision, select_admin,
};

#[path = "topics/mapping.rs"]
mod mapping;

use mapping::*;

pub(crate) struct SafeTopicCreateRequest {
    pub topic: TopicIdentity,
    pub targets: Vec<TopicTargetIdentity>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub permission: TopicPermission,
    pub ordered: bool,
    pub message_type: TopicMessageType,
}

impl fmt::Debug for SafeTopicCreateRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeTopicCreateRequest")
            .field("target_count", &self.targets.len())
            .field("read_queue_count", &self.read_queue_count)
            .field("write_queue_count", &self.write_queue_count)
            .field("ordered", &self.ordered)
            .field("message_type", &self.message_type)
            .finish()
    }
}

pub(crate) struct SafeTopicQueuePatchRequest {
    pub topic: TopicIdentity,
    pub target: TopicTargetIdentity,
    pub expected_version: u64,
    pub read_queue_count: Option<u32>,
    pub write_queue_count: Option<u32>,
}

impl fmt::Debug for SafeTopicQueuePatchRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeTopicQueuePatchRequest")
            .field("expected_version", &self.expected_version)
            .field("read_queue_count", &self.read_queue_count)
            .field("write_queue_count", &self.write_queue_count)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SafeTopicPatchOutcome {
    Applied { previous_version: u64, version: u64 },
    VersionConflict { expected_version: u64, actual_version: u64 },
}

pub(crate) struct SafeTopicDeleteRequest {
    pub topic: TopicIdentity,
    pub cluster_names: Vec<String>,
}

impl fmt::Debug for SafeTopicDeleteRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeTopicDeleteRequest")
            .field("cluster_count", &self.cluster_names.len())
            .finish_non_exhaustive()
    }
}

pub(crate) struct SafeTopicDeleteBrokerRequest {
    pub topic: TopicIdentity,
    pub target: TopicTargetIdentity,
}

impl fmt::Debug for SafeTopicDeleteBrokerRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeTopicDeleteBrokerRequest")
            .finish_non_exhaustive()
    }
}

pub(crate) struct SafeTopicSendRequest {
    pub topic: TopicIdentity,
    pub key: String,
    pub tag: String,
    pub body: String,
    pub trace_enabled: bool,
}

impl fmt::Debug for SafeTopicSendRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeTopicSendRequest")
            .field("has_key", &!self.key.is_empty())
            .field("has_tag", &!self.tag.is_empty())
            .field("body_length", &self.body.len())
            .field("trace_enabled", &self.trace_enabled)
            .finish_non_exhaustive()
    }
}

impl Drop for SafeTopicSendRequest {
    fn drop(&mut self) {
        self.body.clear();
    }
}

impl SafeTopicSendRequest {
    fn into_parts(mut self) -> (TopicIdentity, String, String, String, bool) {
        (
            self.topic.clone(),
            std::mem::take(&mut self.key),
            std::mem::take(&mut self.tag),
            std::mem::take(&mut self.body),
            self.trace_enabled,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SafeTopicSendReceipt {
    pub delivered: bool,
}

pub(crate) struct SafeTopicOffsetRequest {
    pub topic: TopicIdentity,
    pub consumer_group: String,
    pub cluster_name: String,
    pub timestamp: Option<u64>,
    pub force: bool,
}

impl fmt::Debug for SafeTopicOffsetRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeTopicOffsetRequest")
            .field("has_cluster", &!self.cluster_name.is_empty())
            .field("has_timestamp", &self.timestamp.is_some())
            .field("force", &self.force)
            .finish_non_exhaustive()
    }
}

impl GpuiAdminProvider {
    pub async fn topic_inventory(self: &Arc<Self>, revision: u64) -> Result<TopicInventory, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-inventory", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let response = select_admin(cancellation, session.topic_catalog()).await?;
            sanitize_inventory(response)
        })
        .await
    }

    pub async fn topic_route(
        self: &Arc<Self>,
        revision: u64,
        topic: TopicIdentity,
    ) -> Result<TopicRouteView, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-route", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let route = select_admin(cancellation, session.topic_route(topic.as_str())).await?;
            route.map(|route| sanitize_route(topic, route)).ok_or_else(not_found)
        })
        .await
    }

    pub async fn detailed_topic_stats(
        self: &Arc<Self>,
        revision: u64,
        topic: TopicIdentity,
    ) -> Result<TopicStatsView, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-stats", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            select_admin(cancellation, session.detailed_topic_stats(topic.as_str()))
                .await
                .map(|response| sanitize_stats(topic, response))
        })
        .await
    }

    pub async fn detailed_topic_config(
        self: &Arc<Self>,
        revision: u64,
        topic: TopicIdentity,
    ) -> Result<TopicConfigView, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-config", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let response = select_admin(cancellation, session.topic_config(topic.as_str())).await?;
            sanitize_config(topic, response)
        })
        .await
    }

    pub async fn detailed_topic_consumers(
        self: &Arc<Self>,
        revision: u64,
        topic: TopicIdentity,
    ) -> Result<TopicConsumersView, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-consumers", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            select_admin(cancellation, session.topic_consumers(topic.as_str()))
                .await
                .map(|response| sanitize_consumers(topic, response))
        })
        .await
    }

    pub async fn topic_config_preflight(
        self: &Arc<Self>,
        revision: u64,
        topic: TopicIdentity,
        target: TopicTargetIdentity,
    ) -> Result<TopicConfigCasState, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-config-preflight", move |cancellation| async move {
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let request = QueryTopicConfigCasRequest::try_new(target.broker_address(), topic.as_str())
                .map_err(|_| invalid_request())?;
            select_admin(cancellation, session.topic_config_cas_state(&request)).await
        })
        .await
    }

    pub async fn create_topic(
        self: &Arc<Self>,
        revision: u64,
        request: SafeTopicCreateRequest,
    ) -> Result<TopicPartialOutcome, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-create", move |cancellation| async move {
            let preflight = {
                let guard = this.query_session.read().await;
                let session = query_for_revision(&guard, revision)?;
                select_admin(cancellation.clone(), session.topic_catalog()).await?
            };
            if !preflight.completeness.is_complete() {
                return Ok(rejected_outcome(
                    request.topic,
                    TopicMutationKind::Create,
                    &request.targets,
                    TopicFailureCode::Unavailable,
                ));
            }
            if preflight.items.iter().any(|item| item.topic == request.topic.as_str()) {
                return Ok(rejected_outcome(
                    request.topic,
                    TopicMutationKind::Create,
                    &request.targets,
                    TopicFailureCode::Conflict,
                ));
            }
            if !targets_still_match(&preflight, &request.targets) {
                return Ok(rejected_outcome(
                    request.topic,
                    TopicMutationKind::Create,
                    &request.targets,
                    TopicFailureCode::Conflict,
                ));
            }
            let batch = TopicBatchUpsertRequest::try_new(
                request.topic.as_str(),
                request
                    .targets
                    .iter()
                    .map(|target| target.broker_name().to_owned())
                    .collect(),
                request.write_queue_count,
                request.read_queue_count,
                u32::from(request.permission.bits()),
                request.ordered,
                mutation_message_type(request.message_type)?,
            )
            .map_err(|_| invalid_request())?;
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let response = select_admin(cancellation, session.upsert_topic_batch(&batch)).await?;
            Ok(sanitize_batch_outcome(
                request.topic,
                TopicMutationKind::Create,
                response,
            ))
        })
        .await
    }

    pub async fn patch_topic_queue_counts(
        self: &Arc<Self>,
        revision: u64,
        request: SafeTopicQueuePatchRequest,
    ) -> Result<SafeTopicPatchOutcome, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-edit", move |cancellation| async move {
            let catalog = {
                let guard = this.query_session.read().await;
                let session = query_for_revision(&guard, revision)?;
                select_admin(cancellation.clone(), session.topic_catalog()).await?
            };
            if !catalog_topic_is_mutation_safe(&catalog, &request.topic)
                || !catalog_has_exact_target(&catalog, &request.target)
            {
                return Err(invalid_request());
            }
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let preflight =
                QueryTopicConfigCasRequest::try_new(request.target.broker_address(), request.topic.as_str())
                    .map_err(|_| invalid_request())?;
            let state = select_admin(cancellation.clone(), session.topic_config_cas_state(&preflight)).await?;
            if state.version != request.expected_version {
                return Ok(SafeTopicPatchOutcome::VersionConflict {
                    expected_version: request.expected_version,
                    actual_version: state.version,
                });
            }
            let patch = PatchTopicConfigRequest::try_new(
                request.target.broker_address(),
                request.topic.as_str(),
                request.expected_version,
                TopicConfigCasPatch {
                    read_queue_nums: request.read_queue_count,
                    write_queue_nums: request.write_queue_count,
                    order: None,
                },
            )
            .map_err(|_| invalid_request())?;
            select_admin(cancellation, session.patch_topic_config(&patch))
                .await
                .map(|outcome| match outcome {
                    PatchTopicConfigOutcome::Applied {
                        previous_version,
                        version,
                    } => SafeTopicPatchOutcome::Applied {
                        previous_version,
                        version,
                    },
                    PatchTopicConfigOutcome::VersionConflict {
                        expected_version,
                        actual_version,
                    } => SafeTopicPatchOutcome::VersionConflict {
                        expected_version,
                        actual_version,
                    },
                })
        })
        .await
    }

    pub async fn delete_topic(
        self: &Arc<Self>,
        revision: u64,
        request: SafeTopicDeleteRequest,
    ) -> Result<TopicPartialOutcome, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-delete", move |cancellation| async move {
            let preflight = {
                let guard = this.query_session.read().await;
                let session = query_for_revision(&guard, revision)?;
                select_admin(cancellation.clone(), session.topic_catalog()).await?
            };
            let Some(item) = preflight.items.iter().find(|item| item.topic == request.topic.as_str()) else {
                return Ok(rejected_names(
                    request.topic,
                    TopicMutationKind::DeleteTopic,
                    &request.cluster_names,
                    TopicFailureCode::NotFound,
                ));
            };
            if !preflight.completeness.is_complete()
                || !catalog_item_is_mutation_safe(item)
                || !catalog_clusters_match(item, &request.cluster_names)
            {
                return Ok(rejected_names(
                    request.topic,
                    TopicMutationKind::DeleteTopic,
                    &request.cluster_names,
                    TopicFailureCode::Conflict,
                ));
            }
            let batch = TopicBatchDeleteRequest::try_new(request.topic.as_str(), request.cluster_names)
                .map_err(|_| invalid_request())?;
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let response = select_admin(cancellation, session.delete_topic_batch(&batch)).await?;
            Ok(sanitize_delete_outcome(request.topic, response))
        })
        .await
    }

    pub async fn delete_topic_from_broker(
        self: &Arc<Self>,
        revision: u64,
        request: SafeTopicDeleteBrokerRequest,
    ) -> Result<TopicPartialOutcome, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-delete-broker", move |cancellation| async move {
            let preflight = {
                let guard = this.query_session.read().await;
                let session = query_for_revision(&guard, revision)?;
                select_admin(cancellation.clone(), session.topic_catalog()).await?
            };
            let exact_target = catalog_has_exact_target(&preflight, &request.target);
            let item = preflight.items.iter().find(|item| item.topic == request.topic.as_str());
            if !preflight.completeness.is_complete()
                || !exact_target
                || item.is_none_or(|item| {
                    !catalog_item_is_mutation_safe(item)
                        || !item.brokers.contains(&request.target.broker_name().to_owned())
                })
            {
                return Ok(rejected_outcome(
                    request.topic,
                    TopicMutationKind::DeleteBroker,
                    std::slice::from_ref(&request.target),
                    TopicFailureCode::Conflict,
                ));
            }
            let mutation = DeleteTopicsInBrokerRequest::try_new(
                request.target.broker_address(),
                vec![request.topic.as_str().to_owned()],
            )
            .map_err(|_| invalid_request())?;
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let response = select_admin(cancellation, session.delete_topics_in_broker(&mutation)).await?;
            Ok(single_mutation_outcome(
                request.topic,
                TopicMutationKind::DeleteBroker,
                request.target.broker_name().to_owned(),
                response,
            ))
        })
        .await
    }

    pub async fn send_topic_message(
        self: &Arc<Self>,
        revision: u64,
        request: SafeTopicSendRequest,
    ) -> Result<SafeTopicSendReceipt, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-send", move |cancellation| async move {
            let (topic, key, tag, body, trace_enabled) = request.into_parts();
            {
                let guard = this.query_session.read().await;
                let session = query_for_revision(&guard, revision)?;
                let catalog = select_admin(cancellation.clone(), session.topic_catalog()).await?;
                if !catalog_topic_is_mutation_safe(&catalog, &topic) {
                    return Err(invalid_request());
                }
            }
            let mutation = TopicSendRequest {
                topic: topic.as_str().to_owned(),
                key,
                tag,
                message_body: body,
                trace_enabled,
            };
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let result = select_admin(cancellation, session.send_topic_message(&mutation)).await;
            drop(mutation);
            result.map(|_| SafeTopicSendReceipt { delivered: true })
        })
        .await
    }

    pub async fn reset_topic_offset(
        self: &Arc<Self>,
        revision: u64,
        request: SafeTopicOffsetRequest,
    ) -> Result<TopicPartialOutcome, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-reset-offset", move |cancellation| async move {
            let timestamp = request.timestamp.ok_or_else(invalid_request)?;
            let safe = {
                let guard = this.query_session.read().await;
                let session = query_for_revision(&guard, revision)?;
                let catalog = select_admin(cancellation.clone(), session.topic_catalog()).await?;
                catalog_topic_is_mutation_safe(&catalog, &request.topic)
            };
            if !safe {
                return Ok(rejected_names(
                    request.topic,
                    TopicMutationKind::ResetOffset,
                    std::slice::from_ref(&request.consumer_group),
                    TopicFailureCode::Conflict,
                ));
            }
            let mutation = TopicOffsetMutationRequest::try_new(
                request.topic.as_str(),
                request.consumer_group.clone(),
                request.cluster_name,
                Some(timestamp),
                request.force,
            )
            .map_err(|_| invalid_request())?;
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let outcome = select_admin(cancellation, session.reset_topic_offset_detailed(&mutation)).await?;
            Ok(sanitize_offset_outcome(
                request.topic,
                TopicMutationKind::ResetOffset,
                outcome,
            ))
        })
        .await
    }

    pub async fn skip_topic_accumulated(
        self: &Arc<Self>,
        revision: u64,
        request: SafeTopicOffsetRequest,
    ) -> Result<TopicPartialOutcome, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-skip-accumulated", move |cancellation| async move {
            if request.timestamp.is_some() {
                return Err(invalid_request());
            }
            let safe = {
                let guard = this.query_session.read().await;
                let session = query_for_revision(&guard, revision)?;
                let catalog = select_admin(cancellation.clone(), session.topic_catalog()).await?;
                catalog_topic_is_mutation_safe(&catalog, &request.topic)
            };
            if !safe {
                return Ok(rejected_names(
                    request.topic,
                    TopicMutationKind::SkipBacklog,
                    std::slice::from_ref(&request.consumer_group),
                    TopicFailureCode::Conflict,
                ));
            }
            let mutation = TopicOffsetMutationRequest::try_new(
                request.topic.as_str(),
                request.consumer_group.clone(),
                request.cluster_name,
                None,
                request.force,
            )
            .map_err(|_| invalid_request())?;
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let outcome = select_admin(cancellation, session.skip_topic_accumulated_detailed(&mutation)).await?;
            Ok(sanitize_offset_outcome(
                request.topic,
                TopicMutationKind::SkipBacklog,
                outcome,
            ))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_debug_and_drop_do_not_expose_message_body() {
        let request = SafeTopicSendRequest {
            topic: TopicIdentity::parse("orders").expect("topic"),
            key: "key".into(),
            tag: "tag".into(),
            body: "delivery-secret-body".into(),
            trace_enabled: false,
        };
        assert!(!format!("{request:?}").contains("delivery-secret-body"));
    }

    #[test]
    fn mutation_guarantee_never_claims_create_or_delete_cas() {
        let outcome = rejected_names(
            TopicIdentity::parse("orders").expect("topic"),
            TopicMutationKind::Create,
            &["broker-a".into()],
            TopicFailureCode::Conflict,
        );
        assert_eq!(outcome.guarantee, TopicMutationGuarantee::PreflightBestEffort);
    }

    #[test]
    fn delete_preflight_requires_the_current_cluster_set_to_equal_the_confirmed_set() {
        let item = DetailedTopicCatalogItem {
            topic: "orders".into(),
            category: "APPLICATION".into(),
            message_type: Some("NORMAL".into()),
            clusters: vec!["cluster-a".into(), "cluster-b".into()],
            brokers: vec!["broker-a".into(), "broker-b".into()],
            read_queue_count: Some(8),
            write_queue_count: Some(8),
            perm: Some(6),
            order: Some(false),
            system_topic: false,
        };
        assert!(catalog_clusters_match(&item, &["cluster-b".into(), "cluster-a".into()]));
        assert!(!catalog_clusters_match(&item, &["cluster-a".into()]));
        assert!(!catalog_clusters_match(
            &item,
            &["cluster-a".into(), "cluster-b".into(), "cluster-c".into()]
        ));
    }
}

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

//! Allowlists protocol-facing Topic values into application-safe DTOs.

use super::*;

pub(super) fn sanitize_inventory(response: DetailedTopicCatalog) -> Result<TopicInventory, ProviderError> {
    let items = response
        .items
        .into_iter()
        .map(|item| {
            Ok(TopicInventoryItem {
                identity: TopicIdentity::parse(item.topic).map_err(|_| invalid_data())?,
                category: TopicCategory::parse(&item.category),
                message_type: TopicMessageType::parse(item.message_type.as_deref()),
                clusters: item.clusters,
                brokers: item.brokers,
                read_queue_count: item.read_queue_count,
                write_queue_count: item.write_queue_count,
                permission: item.perm.and_then(|perm| TopicPermission::parse(perm).ok()),
                ordered: item.order,
            })
        })
        .collect::<Result<Vec<_>, ProviderError>>()?;
    let targets = response
        .broker_targets
        .into_iter()
        .map(|target| TopicTargetIdentity::parse(target.cluster_name, target.broker_name, target.broker_addr))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid_data())?;
    Ok(TopicInventory {
        items,
        targets,
        completeness: sanitize_completeness(response.completeness),
        failures: response.failures.into_iter().map(sanitize_failure).collect(),
    })
}

pub(super) fn sanitize_route(topic: TopicIdentity, response: TopicRoute) -> TopicRouteView {
    TopicRouteView {
        topic,
        brokers: response
            .brokers
            .into_iter()
            .map(|broker| TopicRouteBrokerView {
                cluster_name: broker.cluster,
                broker_name: broker.broker_name,
                address_count: broker.broker_addrs.len(),
                zone_name: broker.zone_name,
                acting_master: broker.enable_acting_master,
            })
            .collect(),
        queues: response
            .queues
            .into_iter()
            .map(|queue| TopicRouteQueueView {
                broker_name: queue.broker_name,
                read_queue_count: queue.read_queue_nums,
                write_queue_count: queue.write_queue_nums,
                permission: i32::try_from(queue.perm)
                    .ok()
                    .and_then(|perm| TopicPermission::parse(perm).ok()),
            })
            .collect(),
    }
}

pub(super) fn sanitize_stats(topic: TopicIdentity, response: DetailedTopicStats) -> TopicStatsView {
    TopicStatsView {
        topic,
        total_message_count: response.stats.total_message_count,
        offsets: response
            .stats
            .offsets
            .into_iter()
            .map(|offset| TopicQueueOffsetView {
                broker_name: offset.broker_name,
                queue_id: offset.queue_id,
                min_offset: offset.min_offset,
                max_offset: offset.max_offset,
                last_update_timestamp: offset.last_update_timestamp,
            })
            .collect(),
        completeness: sanitize_completeness(response.completeness),
        failures: response.failures.into_iter().map(sanitize_failure).collect(),
    }
}

pub(super) fn sanitize_config(
    topic: TopicIdentity,
    response: rocketmq_admin_core::core::topic::DetailedTopicConfig,
) -> Result<TopicConfigView, ProviderError> {
    let targets = response
        .targets
        .into_iter()
        .map(|target| {
            Ok(TopicConfigTargetView {
                target: TopicTargetIdentity::parse(target.cluster_name, target.broker_name, target.broker_addr)
                    .map_err(|_| invalid_data())?,
                version: target.version,
                read_queue_count: target.read_queue_nums,
                write_queue_count: target.write_queue_nums,
                permission: i32::try_from(target.perm)
                    .ok()
                    .and_then(|perm| TopicPermission::parse(perm).ok()),
                ordered: target.order,
                message_type: TopicMessageType::parse(Some(&target.message_type)),
            })
        })
        .collect::<Result<Vec<_>, ProviderError>>()?;
    Ok(TopicConfigView {
        topic,
        targets,
        inconsistent_fields: response
            .inconsistent_fields
            .into_iter()
            .filter_map(|field| match field.as_str() {
                "readQueueNums" => Some(TopicConfigField::ReadQueues),
                "writeQueueNums" => Some(TopicConfigField::WriteQueues),
                "perm" => Some(TopicConfigField::Permission),
                "order" => Some(TopicConfigField::Ordered),
                "messageType" => Some(TopicConfigField::MessageType),
                _ => None,
            })
            .collect(),
        completeness: sanitize_completeness(response.completeness),
        failures: response.failures.into_iter().map(sanitize_failure).collect(),
    })
}

pub(super) fn sanitize_consumers(topic: TopicIdentity, response: DetailedTopicConsumers) -> TopicConsumersView {
    TopicConsumersView {
        topic,
        items: response
            .items
            .into_iter()
            .map(|item| TopicConsumerView {
                consumer_group: item.consumer_group,
                total_diff: item.total_diff,
                inflight_diff: item.inflight_diff,
                consume_tps: item.consume_tps,
            })
            .collect(),
        completeness: sanitize_completeness(response.completeness),
        failures: response.failures.into_iter().map(sanitize_failure).collect(),
    }
}

pub(super) fn sanitize_completeness(value: TopicInspectionCompleteness) -> TopicCompleteness {
    match value {
        TopicInspectionCompleteness::Complete => TopicCompleteness::Complete,
        TopicInspectionCompleteness::Partial {
            successful_target_count,
            failed_target_count,
        } => TopicCompleteness::Partial {
            successful_target_count,
            failed_target_count,
        },
    }
}

pub(super) fn sanitize_failure(failure: TopicInspectionFailure) -> TopicTargetFailure {
    TopicTargetFailure {
        target: failure.target,
        stage: match failure.stage {
            TopicInspectionStage::CatalogConfig => TopicFailureStage::CatalogConfig,
            TopicInspectionStage::CatalogRoute => TopicFailureStage::CatalogRoute,
            TopicInspectionStage::Stats => TopicFailureStage::Stats,
            TopicInspectionStage::Configuration => TopicFailureStage::Configuration,
            TopicInspectionStage::Consumer => TopicFailureStage::Consumer,
        },
        code: match failure.code {
            TopicInspectionFailureCode::NotFound => TopicFailureCode::NotFound,
            TopicInspectionFailureCode::InvalidData => TopicFailureCode::InvalidData,
            TopicInspectionFailureCode::Unavailable => TopicFailureCode::Unavailable,
        },
        retryable: failure.retryable,
    }
}

pub(super) fn targets_still_match(catalog: &DetailedTopicCatalog, targets: &[TopicTargetIdentity]) -> bool {
    !targets.is_empty()
        && targets.iter().all(|expected| {
            catalog.broker_targets.iter().any(|actual| {
                actual.cluster_name == expected.cluster_name()
                    && actual.broker_name == expected.broker_name()
                    && actual.broker_addr == expected.broker_address()
            })
        })
}

pub(super) fn mutation_message_type(value: TopicMessageType) -> Result<Option<String>, ProviderError> {
    match value {
        TopicMessageType::Normal => Ok(Some("NORMAL".into())),
        TopicMessageType::Delay => Ok(Some("DELAY".into())),
        TopicMessageType::Fifo => Ok(Some("FIFO".into())),
        TopicMessageType::Transaction => Ok(Some("TRANSACTION".into())),
        TopicMessageType::Retry
        | TopicMessageType::Dlq
        | TopicMessageType::System
        | TopicMessageType::Unspecified
        | TopicMessageType::Unknown => Err(invalid_request()),
    }
}

pub(super) fn catalog_has_exact_target(catalog: &DetailedTopicCatalog, expected: &TopicTargetIdentity) -> bool {
    catalog.broker_targets.iter().any(|actual| {
        actual.cluster_name == expected.cluster_name()
            && actual.broker_name == expected.broker_name()
            && actual.broker_addr == expected.broker_address()
    })
}

pub(super) fn catalog_topic_is_mutation_safe(catalog: &DetailedTopicCatalog, topic: &TopicIdentity) -> bool {
    catalog.completeness.is_complete()
        && catalog
            .items
            .iter()
            .find(|item| item.topic == topic.as_str())
            .is_some_and(catalog_item_is_mutation_safe)
}

pub(super) fn catalog_item_is_mutation_safe(item: &DetailedTopicCatalogItem) -> bool {
    !item.system_topic
        && TopicCategory::parse(&item.category) != TopicCategory::Unknown
        && TopicMessageType::parse(item.message_type.as_deref()) != TopicMessageType::Unknown
        && item.read_queue_count.is_some()
        && item.write_queue_count.is_some()
        && item.perm.is_some()
        && item.order.is_some()
        && !item.clusters.is_empty()
        && !item.brokers.is_empty()
}

pub(super) fn catalog_clusters_match(item: &DetailedTopicCatalogItem, confirmed: &[String]) -> bool {
    let current = item
        .clusters
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let confirmed = confirmed
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    !current.is_empty() && current == confirmed
}

pub(super) fn sanitize_batch_outcome(
    topic: TopicIdentity,
    kind: TopicMutationKind,
    response: TopicBatchMutationOutcome,
) -> TopicPartialOutcome {
    let mut targets = response
        .targets
        .into_iter()
        .map(|target| TopicTargetOutcome {
            target: target.broker_name,
            stage: TopicFailureStage::Mutation,
            applied: target.success,
            failure: (!target.success).then_some(TopicFailureCode::Unavailable),
            retryable: !target.success,
        })
        .collect::<Vec<_>>();
    if let Some(order) = response.order_config
        && !order.success
    {
        targets.push(TopicTargetOutcome {
            target: "order-configuration".into(),
            stage: TopicFailureStage::Mutation,
            applied: false,
            failure: Some(TopicFailureCode::Unavailable),
            retryable: true,
        });
    }
    TopicPartialOutcome {
        topic,
        kind,
        guarantee: TopicMutationGuarantee::PreflightBestEffort,
        targets,
        reload_failed: false,
    }
}

pub(super) fn sanitize_delete_outcome(
    topic: TopicIdentity,
    response: rocketmq_admin_core::core::topic::TopicBatchDeleteOutcome,
) -> TopicPartialOutcome {
    sanitize_batch_outcome(
        topic,
        TopicMutationKind::DeleteTopic,
        TopicBatchMutationOutcome {
            targets: response.targets,
            order_config: response.order_config,
        },
    )
}

pub(super) fn rejected_outcome(
    topic: TopicIdentity,
    kind: TopicMutationKind,
    targets: &[TopicTargetIdentity],
    failure: TopicFailureCode,
) -> TopicPartialOutcome {
    rejected_names(
        topic,
        kind,
        &targets
            .iter()
            .map(|target| target.broker_name().to_owned())
            .collect::<Vec<_>>(),
        failure,
    )
}

pub(super) fn rejected_names(
    topic: TopicIdentity,
    kind: TopicMutationKind,
    targets: &[String],
    failure: TopicFailureCode,
) -> TopicPartialOutcome {
    TopicPartialOutcome {
        topic,
        kind,
        guarantee: TopicMutationGuarantee::PreflightBestEffort,
        targets: targets
            .iter()
            .map(|target| TopicTargetOutcome {
                target: target.clone(),
                stage: TopicFailureStage::Mutation,
                applied: false,
                failure: Some(failure),
                retryable: failure == TopicFailureCode::Unavailable,
            })
            .collect(),
        reload_failed: false,
    }
}

pub(super) fn single_mutation_outcome(
    topic: TopicIdentity,
    kind: TopicMutationKind,
    target: String,
    _response: TopicMutationOutcome,
) -> TopicPartialOutcome {
    TopicPartialOutcome {
        topic,
        kind,
        guarantee: TopicMutationGuarantee::PreflightBestEffort,
        targets: vec![TopicTargetOutcome {
            target,
            stage: TopicFailureStage::Mutation,
            applied: true,
            failure: None,
            retryable: false,
        }],
        reload_failed: false,
    }
}

pub(super) fn sanitize_offset_outcome(
    topic: TopicIdentity,
    kind: TopicMutationKind,
    response: TopicOffsetMutationOutcome,
) -> TopicPartialOutcome {
    TopicPartialOutcome {
        topic,
        kind,
        guarantee: TopicMutationGuarantee::PreflightBestEffort,
        targets: response
            .targets
            .into_iter()
            .map(|target| TopicTargetOutcome {
                target: target.queue_id.map_or(target.broker_name.clone(), |queue_id| {
                    format!("{} / queue {queue_id}", target.broker_name)
                }),
                stage: TopicFailureStage::Mutation,
                applied: target.applied,
                failure: target.failure.map(|failure| match failure {
                    TopicOffsetMutationFailureCode::InvalidData => TopicFailureCode::InvalidData,
                    TopicOffsetMutationFailureCode::Unavailable => TopicFailureCode::Unavailable,
                }),
                retryable: target.retryable,
            })
            .collect(),
        reload_failed: false,
    }
}

pub(super) fn invalid_request() -> ProviderError {
    ProviderError::new(
        ProviderErrorCode::Unavailable,
        "The Topic operation request is invalid.",
        false,
    )
}

pub(super) fn invalid_data() -> ProviderError {
    ProviderError::new(
        ProviderErrorCode::Unavailable,
        "The Topic response contains invalid identity data.",
        false,
    )
}

pub(super) fn not_found() -> ProviderError {
    ProviderError::new(ProviderErrorCode::Unavailable, "The Topic was not found.", false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_sanitization_retains_applied_and_failed_queue_targets() {
        let outcome = sanitize_offset_outcome(
            TopicIdentity::parse("orders").expect("topic"),
            TopicMutationKind::ResetOffset,
            TopicOffsetMutationOutcome {
                targets: vec![
                    rocketmq_admin_core::core::topic::TopicOffsetTargetOutcome {
                        broker_name: "broker-a".into(),
                        queue_id: Some(0),
                        applied: true,
                        failure: None,
                        retryable: false,
                    },
                    rocketmq_admin_core::core::topic::TopicOffsetTargetOutcome {
                        broker_name: "broker-a".into(),
                        queue_id: Some(1),
                        applied: false,
                        failure: Some(TopicOffsetMutationFailureCode::Unavailable),
                        retryable: true,
                    },
                ],
            },
        );

        assert_eq!(outcome.applied_count(), 1);
        assert_eq!(outcome.failed_count(), 1);
        assert_eq!(outcome.targets[0].target, "broker-a / queue 0");
        assert_eq!(outcome.targets[1].target, "broker-a / queue 1");
        assert_eq!(outcome.targets[1].failure, Some(TopicFailureCode::Unavailable));
        assert!(outcome.targets[1].retryable);
    }
}

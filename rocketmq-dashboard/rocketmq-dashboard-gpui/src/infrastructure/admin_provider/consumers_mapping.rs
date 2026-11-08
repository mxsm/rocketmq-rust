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

//! Safe Consumer and Producer projections from the narrow Admin workspace contracts.

use super::*;

pub(super) fn map_inventory(
    scope: ConnectionScope,
    response: ConsumerInventoryResult,
) -> Result<ConsumerInventory, ProviderFailure> {
    let mut groups = response
        .items
        .into_iter()
        .map(|item| {
            let client_count = map_observation(item.client_count, std::convert::identity);
            let connection_state = map_connection_state(&client_count);
            Ok(ConsumerGroupObservation {
                identity: ConsumerIdentity::parse(item.group).map_err(|_| invalid_data())?,
                category: map_category(&item.category),
                connection_state,
                client_count,
                lag: map_observation(item.diff_total, std::convert::identity),
                consume_type: map_observation(item.consume_type, std::convert::identity),
                message_model: map_observation(item.message_model, std::convert::identity),
                targets: item
                    .targets
                    .into_iter()
                    .map(map_target)
                    .collect::<Result<Vec<_>, ProviderFailure>>()?,
            })
        })
        .collect::<Result<Vec<_>, ProviderFailure>>()?;
    groups.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(ConsumerInventory {
        groups,
        targets: response
            .targets
            .into_iter()
            .map(map_target)
            .collect::<Result<Vec<_>, ProviderFailure>>()?,
        target_observation: map_state(response.target_observation),
        group_observation: map_state(response.group_observation),
        observation: map_state(response.observation),
        failures: response.failures.into_iter().map(map_failure).collect(),
        capabilities: ConsumerCapabilities::for_scope(scope),
    })
}

pub(super) fn map_connection_state(
    count: &ConsumerObservation<usize>,
) -> ConsumerObservation<rocketmq_dashboard_common::ConsumerConnectionState> {
    use rocketmq_dashboard_common::ConsumerConnectionState::{Connected, Disconnected};
    match count {
        ConsumerObservation::Complete(count) => {
            ConsumerObservation::Complete(if *count == 0 { Disconnected } else { Connected })
        }
        ConsumerObservation::Partial {
            value,
            successful_target_count,
            failures,
        } if *value > 0 => ConsumerObservation::Partial {
            value: Connected,
            successful_target_count: *successful_target_count,
            failures: failures.clone(),
        },
        ConsumerObservation::Partial { .. } | ConsumerObservation::Unknown { .. } => ConsumerObservation::Unknown {
            reason: ConsumerUnknownReason::Unavailable,
        },
    }
}

pub(super) fn map_clients(
    group: ConsumerIdentity,
    connection: rocketmq_admin_core::core::consumer::DashboardConsumerConnection,
) -> Result<ConsumerClients, ProviderFailure> {
    Ok(ConsumerClients {
        group,
        clients: connection
            .connections
            .into_iter()
            .map(map_client)
            .collect::<Result<Vec<_>, ProviderFailure>>()?,
        consume_type: text_observation(connection.consume_type),
        message_model: text_observation(connection.message_model),
        subscriptions: connection
            .subscriptions
            .into_iter()
            .map(|subscription| ConsumerSubscription {
                topic: subscription.topic,
                expression: subscription.sub_string,
                expression_type: subscription.expression_type,
            })
            .collect(),
    })
}

pub(super) fn map_client(
    client: rocketmq_admin_core::core::consumer::DashboardConsumerConnectionItem,
) -> Result<ConsumerClientObservation, ProviderFailure> {
    Ok(ConsumerClientObservation {
        identity: ConsumerClientIdentity::parse(client.client_id).map_err(|_| invalid_data())?,
        address: client.client_addr,
        language: client.language,
        version: client.version,
        version_description: client.version_desc,
    })
}

pub(super) fn map_producer_client(
    client: rocketmq_admin_core::core::dashboard::DashboardProducerConnection,
) -> Result<ConsumerClientObservation, ProviderFailure> {
    Ok(ConsumerClientObservation {
        identity: ConsumerClientIdentity::parse(client.client_id).map_err(|_| invalid_data())?,
        address: client.client_addr,
        language: client.language,
        version: client.version,
        version_description: client.version_desc,
    })
}

pub(super) fn map_progress(
    group: ConsumerIdentity,
    progress: rocketmq_admin_core::core::consumer::DashboardConsumerProgress,
) -> ConsumerProgress {
    let rows = progress
        .topics
        .into_iter()
        .flat_map(|topic| {
            let topic_name = topic.topic;
            topic.queues.into_iter().map(move |queue| ConsumerProgressRow {
                topic: topic_name.clone(),
                broker_name: queue.broker_name,
                queue_id: queue.queue_id,
                broker_offset: queue.broker_offset,
                consumer_offset: queue.consumer_offset,
                delta: queue.diff_total,
                last_timestamp: queue.last_timestamp,
            })
        })
        .collect();
    ConsumerProgress::from_rows(group, rows)
}

pub(super) fn map_configuration(
    group: ConsumerIdentity,
    targets: Vec<ConsumerConfigTarget>,
    observation: WorkspaceObservationState,
    failures: Vec<WorkspaceTargetFailure>,
) -> Result<ConsumerConfiguration, ProviderFailure> {
    let snapshots = targets
        .into_iter()
        .filter_map(|target| match target.observation {
            WorkspaceObservation::Complete { value } | WorkspaceObservation::Partial { value, .. } => {
                Some((target.target, value, target.details))
            }
            WorkspaceObservation::Unknown { .. } => None,
        })
        .map(|(target, state, details)| {
            Ok(ConsumerConfigSnapshot {
                identity: ConsumerConfigIdentity {
                    group: group.clone(),
                    target: map_target(target)?,
                },
                generation: state.version,
                entries: ConsumerConfigEntries {
                    retry_max_times: state.retry_max_times,
                    retry_queue_nums: state.retry_queue_nums,
                    consume_timeout_minutes: state.consume_timeout_minutes,
                    consume_enable: state.consume_enable,
                    consume_from_min_enable: state.consume_from_min_enable,
                    consume_broadcast_enable: state.consume_broadcast_enable,
                    consume_message_orderly: state.consume_message_orderly,
                    broker_id: state.broker_id,
                    which_broker_when_consume_slowly: state.which_broker_when_consume_slowly,
                    notify_consumer_ids_changed_enable: state.notify_consumer_ids_changed_enable,
                    group_sys_flag: state.group_sys_flag,
                },
                read_only: rocketmq_dashboard_common::ConsumerConfigReadOnly {
                    retry_policy: match details.retry_policy {
                        rocketmq_admin_core::core::consumer_configuration::ConsumerRetryPolicy::DefaultCustomized => {
                            rocketmq_dashboard_common::ConsumerRetryPolicy::DefaultCustomized
                        }
                        rocketmq_admin_core::core::consumer_configuration::ConsumerRetryPolicy::DefaultFallback => {
                            rocketmq_dashboard_common::ConsumerRetryPolicy::DefaultFallback
                        }
                        rocketmq_admin_core::core::consumer_configuration::ConsumerRetryPolicy::Customized {
                            delays_ms,
                        } => rocketmq_dashboard_common::ConsumerRetryPolicy::Customized { delays_ms },
                        rocketmq_admin_core::core::consumer_configuration::ConsumerRetryPolicy::Exponential {
                            initial_ms,
                            maximum_ms,
                            multiplier,
                        } => rocketmq_dashboard_common::ConsumerRetryPolicy::Exponential {
                            initial_ms,
                            maximum_ms,
                            multiplier,
                        },
                    },
                    subscribed_topics: details.subscribed_topics,
                    attributes: details.attributes,
                    truncated: details.truncated,
                },
            })
        })
        .collect::<Result<Vec<_>, ProviderFailure>>()?;
    Ok(ConsumerConfiguration {
        group,
        snapshots,
        observation: map_state(observation),
        failures: failures.into_iter().map(map_failure).collect(),
    })
}

pub(super) fn map_observation<T, U>(
    observation: WorkspaceObservation<T>,
    map: impl FnOnce(T) -> U,
) -> ConsumerObservation<U> {
    match observation {
        WorkspaceObservation::Complete { value } => ConsumerObservation::Complete(map(value)),
        WorkspaceObservation::Partial {
            value,
            successful_target_count,
            failures,
        } => ConsumerObservation::Partial {
            value: map(value),
            successful_target_count,
            failures: failures.into_iter().map(map_failure).collect(),
        },
        WorkspaceObservation::Unknown { reason } => ConsumerObservation::Unknown {
            reason: map_unknown(reason),
        },
    }
}

pub(super) fn map_fallible_observation<T, U>(
    observation: WorkspaceObservation<T>,
    map: impl FnOnce(T) -> Result<U, ProviderFailure>,
) -> Result<ConsumerObservation<U>, ProviderFailure> {
    match observation {
        WorkspaceObservation::Complete { value } => Ok(ConsumerObservation::Complete(map(value)?)),
        WorkspaceObservation::Partial {
            value,
            successful_target_count,
            failures,
        } => Ok(ConsumerObservation::Partial {
            value: map(value)?,
            successful_target_count,
            failures: failures.into_iter().map(map_failure).collect(),
        }),
        WorkspaceObservation::Unknown { reason } => Ok(ConsumerObservation::Unknown {
            reason: map_unknown(reason),
        }),
    }
}

pub(super) fn text_observation(value: String) -> ConsumerObservation<String> {
    if value.trim().is_empty() || value.eq_ignore_ascii_case("unknown") {
        ConsumerObservation::Unknown {
            reason: ConsumerUnknownReason::InvalidResponse,
        }
    } else {
        ConsumerObservation::Complete(value)
    }
}

pub(super) fn map_target(target: ConsumerWorkspaceTarget) -> Result<ConsumerTargetIdentity, ProviderFailure> {
    ConsumerTargetIdentity::parse(target.cluster_name, target.broker_name, target.broker_address)
        .map_err(|_| invalid_data())
}

pub(super) fn map_failure(failure: WorkspaceTargetFailure) -> ConsumerTargetFailure {
    ConsumerTargetFailure {
        target: failure.target,
        stage: match failure.stage {
            WorkspaceFailureStage::Inventory => ConsumerFailureStage::Inventory,
            WorkspaceFailureStage::Clients => ConsumerFailureStage::Clients,
            WorkspaceFailureStage::Progress => ConsumerFailureStage::Progress,
            WorkspaceFailureStage::Configuration => ConsumerFailureStage::Configuration,
            WorkspaceFailureStage::Diagnostics => ConsumerFailureStage::ConnectionObservation,
        },
        code: match failure.code {
            WorkspaceFailureCode::NotFound => ConsumerFailureCode::NotFound,
            WorkspaceFailureCode::Unavailable => ConsumerFailureCode::Unavailable,
            WorkspaceFailureCode::Unsupported => ConsumerFailureCode::Unsupported,
            WorkspaceFailureCode::InvalidData => ConsumerFailureCode::InvalidData,
        },
        retryable: failure.retryable,
    }
}

pub(super) const fn map_state(state: WorkspaceObservationState) -> ConsumerObservationState {
    match state {
        WorkspaceObservationState::Complete => ConsumerObservationState::Complete,
        WorkspaceObservationState::Partial => ConsumerObservationState::Partial,
        WorkspaceObservationState::Unknown => ConsumerObservationState::Unknown,
    }
}

pub(super) const fn map_unknown(reason: WorkspaceUnknownReason) -> ConsumerUnknownReason {
    match reason {
        WorkspaceUnknownReason::Unsupported => ConsumerUnknownReason::Unsupported,
        WorkspaceUnknownReason::Unavailable => ConsumerUnknownReason::Unavailable,
        WorkspaceUnknownReason::InvalidResponse => ConsumerUnknownReason::InvalidResponse,
    }
}

pub(super) fn map_category(category: &str) -> ConsumerCategory {
    match category.trim().to_ascii_uppercase().as_str() {
        "SYSTEM" => ConsumerCategory::System,
        "NORMAL" | "FIFO" => ConsumerCategory::Application,
        _ => ConsumerCategory::Unknown,
    }
}

pub(super) fn invalid_data() -> ProviderFailure {
    ProviderFailure::invalid_data("The Admin response did not contain a valid Consumer identity.")
}

pub(super) fn invalid_request() -> ProviderFailure {
    ProviderFailure::invalid_request("The Consumer operation did not pass its exact-target preflight.")
}

pub(super) fn not_configured() -> ProviderFailure {
    ProviderFailure::not_configured("A Proxy endpoint is required for Proxy scope.")
}

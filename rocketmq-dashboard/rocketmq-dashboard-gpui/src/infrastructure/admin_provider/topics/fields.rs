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

//! Allowlisted full-field edits; raw Broker configuration never reaches a view.

use rocketmq_admin_core::core::{
    supervised_mutation::{MutationFailureCode, TopicMessageType as AdminMessageType},
    topic::{TopicConfigFieldsPatch as AdminPatch, TopicConfigFieldsPatchRequest, TopicConfigFieldsTarget},
};
use rocketmq_dashboard_common::{TopicConfigEditOutcome, TopicConfigFieldsPatch};

use super::*;

pub(crate) struct SafeTopicFieldsPatchRequest {
    pub topic: TopicIdentity,
    pub targets: Vec<TopicConfigTargetView>,
    pub patch: TopicConfigFieldsPatch,
}

impl GpuiAdminProvider {
    pub async fn patch_topic_fields(
        self: &Arc<Self>,
        revision: u64,
        request: SafeTopicFieldsPatchRequest,
    ) -> Result<TopicConfigEditOutcome, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-topic-fields-edit", move |cancellation| async move {
            let catalog = {
                let guard = this.query_session.read().await;
                let session = query_for_revision(&guard, revision)?;
                select_admin(cancellation.clone(), session.topic_catalog()).await?
            };
            if !catalog_topic_is_mutation_safe(&catalog, &request.topic)
                || request
                    .targets
                    .iter()
                    .any(|target| !catalog_has_exact_target(&catalog, &target.target))
            {
                return Err(invalid_request());
            }
            let patch = AdminPatch {
                read_queue_nums: request.patch.read_queue_count,
                write_queue_nums: request.patch.write_queue_count,
                perm: request.patch.permission.map(|permission| u32::from(permission.bits())),
                order: request.patch.ordered,
                message_type: request.patch.message_type.map(admin_message_type).transpose()?,
            };
            let targets = request.targets.iter().map(|target| TopicConfigFieldsTarget {
                cluster_name: target.target.cluster_name().to_owned(),
                broker_name: target.target.broker_name().to_owned(),
                broker_address: target.target.broker_address().to_owned(),
                expected_version: target.version,
            });
            let command = TopicConfigFieldsPatchRequest::try_new(request.topic.as_str(), targets, patch)
                .map_err(|_| invalid_request())?;
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let report = select_admin(cancellation, session.patch_topic_fields(&command)).await?;
            let mut conflicts = Vec::new();
            let mut targets = Vec::new();
            for result in report.targets {
                if !result.applied
                    && !result.outcome_unknown
                    && result.failure == Some(MutationFailureCode::Conflict)
                    && let Some(latest) = result.latest
                    && let (Some(version), Some(config)) = (latest.version, latest.configuration)
                {
                    let conflict = TopicConfigTargetView {
                        target: TopicTargetIdentity::parse(
                            &result.target.cluster_name,
                            &result.target.broker_name,
                            &result.target.broker_address,
                        )
                        .map_err(|_| invalid_request())?,
                        version,
                        read_queue_count: config.read_queue_nums,
                        write_queue_count: config.write_queue_nums,
                        permission: i32::try_from(config.perm)
                            .ok()
                            .and_then(|bits| TopicPermission::parse(bits).ok()),
                        ordered: config.order,
                        message_type: message_type(config.message_type),
                    };
                    if conflict.editable_values().is_some() {
                        conflicts.push(conflict);
                    }
                }
                targets.push(TopicTargetOutcome {
                    target: result.target.broker_name,
                    stage: TopicFailureStage::Mutation,
                    applied: result.applied,
                    failure: if result.outcome_unknown {
                        Some(TopicFailureCode::Unconfirmed)
                    } else {
                        result.failure.map(|failure| match failure {
                            MutationFailureCode::Conflict => TopicFailureCode::Conflict,
                            MutationFailureCode::InvalidData => TopicFailureCode::InvalidData,
                            MutationFailureCode::Unavailable
                            | MutationFailureCode::PersistenceFailed
                            | MutationFailureCode::VerificationFailed
                            | MutationFailureCode::OrderReconciliationFailed => TopicFailureCode::Unavailable,
                        })
                    },
                    retryable: false,
                });
            }
            if let Some(reconciled) = report.order_reconciled {
                targets.push(TopicTargetOutcome {
                    target: "order_topic_config".into(),
                    stage: TopicFailureStage::Mutation,
                    applied: reconciled,
                    failure: (!reconciled).then_some(TopicFailureCode::Unconfirmed),
                    retryable: false,
                });
            }
            Ok(TopicConfigEditOutcome {
                outcome: TopicPartialOutcome {
                    topic: request.topic,
                    kind: TopicMutationKind::Edit,
                    guarantee: if report.order_reconciled.is_some() {
                        TopicMutationGuarantee::PreflightBestEffort
                    } else {
                        TopicMutationGuarantee::VersionCas
                    },
                    targets,
                    reload_failed: false,
                },
                conflicts,
            })
        })
        .await
    }
}

fn admin_message_type(value: TopicMessageType) -> Result<AdminMessageType, ProviderFailure> {
    Ok(match value {
        TopicMessageType::Normal => AdminMessageType::Normal,
        TopicMessageType::Fifo => AdminMessageType::Fifo,
        TopicMessageType::Delay => AdminMessageType::Delay,
        TopicMessageType::Transaction => AdminMessageType::Transaction,
        TopicMessageType::Unspecified => AdminMessageType::Unspecified,
        TopicMessageType::Retry | TopicMessageType::Dlq | TopicMessageType::System | TopicMessageType::Unknown => {
            return Err(invalid_request());
        }
    })
}

fn message_type(value: AdminMessageType) -> TopicMessageType {
    match value {
        AdminMessageType::Normal => TopicMessageType::Normal,
        AdminMessageType::Fifo => TopicMessageType::Fifo,
        AdminMessageType::Delay => TopicMessageType::Delay,
        AdminMessageType::Transaction => TopicMessageType::Transaction,
        AdminMessageType::Unspecified => TopicMessageType::Unspecified,
    }
}

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

//! Topic field-edit service and authoritative refresh after partial mutations.

use super::*;
use crate::infrastructure::admin_provider::SafeTopicFieldsPatchRequest;
use rocketmq_dashboard_common::{
    AuditTopicField, TopicConfigEditOutcome, TopicConfigFieldsPatch, TopicConfigTargetView,
};

#[derive(Clone)]
pub(crate) struct TopicFieldsPatchCommand {
    pub topic: TopicIdentity,
    pub targets: Vec<TopicConfigTargetView>,
    pub patch: TopicConfigFieldsPatch,
}

pub(crate) struct TopicFieldsPatchResult {
    pub edit: TopicConfigEditOutcome,
    pub configuration: Option<TopicConfigView>,
    pub inventory: Option<TopicInventory>,
    pub reload_error: Option<UiError>,
    pub invalidations: Vec<TopicCacheInvalidation>,
}

impl fmt::Debug for TopicFieldsPatchResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicFieldsPatchResult")
            .field("outcome", &self.edit.outcome)
            .field("conflict_count", &self.edit.conflicts.len())
            .field("reload_failed", &self.reload_error.is_some())
            .finish_non_exhaustive()
    }
}

impl RealTopicBackend {
    pub(super) fn edit_fields(
        &self,
        scope: TopicRequestScope,
        command: TopicFieldsPatchCommand,
    ) -> ServiceFuture<'_, Result<TopicConfigEditOutcome, UiError>> {
        Box::pin(async move {
            self.provider()?
                .patch_topic_fields(
                    scope.revision,
                    SafeTopicFieldsPatchRequest {
                        topic: command.topic,
                        targets: command.targets,
                        patch: command.patch,
                    },
                )
                .await
                .map_err(mutation_error)
        })
    }
}

impl AppServices {
    pub async fn patch_topic_fields(
        &self,
        scope: TopicRequestScope,
        command: TopicFieldsPatchCommand,
    ) -> Result<TopicFieldsPatchResult, UiError> {
        let backend = Arc::clone(&self.topics);
        let mut operation = self.accept_operation(
            scope.revision,
            AuditAction::TopicUpdated,
            OperationResource::Topic(command.topic.as_str().to_owned()),
        )?;
        if let Some(operation) = &mut operation {
            operation.topic_fields = [
                (
                    command.patch.read_queue_count.is_some(),
                    AuditTopicField::ReadQueueCount,
                ),
                (
                    command.patch.write_queue_count.is_some(),
                    AuditTopicField::WriteQueueCount,
                ),
                (command.patch.permission.is_some(), AuditTopicField::Permission),
                (command.patch.ordered.is_some(), AuditTopicField::Ordered),
                (command.patch.message_type.is_some(), AuditTopicField::MessageType),
            ]
            .into_iter()
            .filter_map(|(changed, field)| changed.then_some(field))
            .collect();
        }
        self.run_mutation("gpui-service-topic-fields-edit", operation, async move {
            let topic = command.topic.clone();
            let mut edit = backend.patch_fields(scope, command).await?;
            let should_reload = edit.outcome.applied_count() > 0
                || edit
                    .outcome
                    .targets
                    .iter()
                    .any(|target| target.failure == Some(rocketmq_dashboard_common::TopicFailureCode::Unconfirmed));
            let invalidations = if should_reload {
                mutation_invalidations(rocketmq_dashboard_common::TopicMutationKind::Edit, topic.clone())
            } else {
                Vec::new()
            };
            let (mut configuration, mut inventory, mut reload_error) = (None, None, None);
            if should_reload {
                match (backend.config(scope, topic).await, backend.inventory(scope).await) {
                    (Ok(config), Ok(catalog)) => {
                        configuration = Some(config);
                        inventory = Some(catalog);
                    }
                    (Err(error), _) | (_, Err(error)) => {
                        edit.outcome.reload_failed = true;
                        reload_error = Some(error);
                    }
                }
            }
            Ok(TopicFieldsPatchResult {
                edit,
                invalidations,
                configuration,
                inventory,
                reload_error,
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::topics::test_support::FakeTopicBackend;
    use rocketmq_dashboard_common::{
        TopicCompleteness, TopicFailureCode, TopicFailureStage, TopicMutationGuarantee, TopicMutationKind,
        TopicTargetOutcome,
    };

    #[tokio::test]
    async fn unknown_topic_write_refreshes_authority_and_keeps_reload_failure_separate() {
        let topic = TopicIdentity::parse("orders").unwrap();
        let target = TopicConfigTargetView {
            target: TopicTargetIdentity::parse("cluster-a", "broker-a", "broker-a:10911").unwrap(),
            version: 7,
            read_queue_count: 8,
            write_queue_count: 8,
            permission: TopicPermission::parse(6).ok(),
            ordered: false,
            message_type: TopicMessageType::Normal,
        };
        let fake = Arc::new(FakeTopicBackend::default());
        fake.queue_fields(Ok(TopicConfigEditOutcome {
            outcome: TopicPartialOutcome {
                topic: topic.clone(),
                kind: TopicMutationKind::Edit,
                guarantee: TopicMutationGuarantee::VersionCas,
                targets: vec![TopicTargetOutcome {
                    target: "broker-a".into(),
                    stage: TopicFailureStage::Mutation,
                    applied: false,
                    failure: Some(TopicFailureCode::Unconfirmed),
                    retryable: false,
                }],
                reload_failed: false,
            },
            conflicts: Vec::new(),
        }));
        fake.queue_config(Err(UiError::new(
            "Configuration refresh failed.",
            UiErrorCode::Connection,
            true,
        )));
        fake.queue_inventory(Ok(TopicInventory {
            items: Vec::new(),
            targets: Vec::new(),
            completeness: TopicCompleteness::Complete,
            failures: Vec::new(),
        }));
        let services = AppServices::default().with_topic_backend(fake.clone());
        let result = services
            .patch_topic_fields(
                TopicRequestScope { revision: 1, epoch: 1 },
                TopicFieldsPatchCommand {
                    topic: topic.clone(),
                    targets: vec![target],
                    patch: TopicConfigFieldsPatch {
                        permission: TopicPermission::parse(4).ok(),
                        ..Default::default()
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(result.edit.outcome.applied_count(), 0);
        assert_eq!(
            result.edit.outcome.targets[0].failure,
            Some(TopicFailureCode::Unconfirmed)
        );
        assert!(result.edit.outcome.reload_failed);
        assert!(result.reload_error.is_some());
        assert!(result.edit.conflicts.is_empty());
        assert!(
            result
                .invalidations
                .contains(&TopicCacheInvalidation::Configuration(topic))
        );
        let calls = fake.calls();
        assert_eq!(calls.fields.len(), 1);
        assert_eq!(calls.config.len(), 1);
        assert_eq!(calls.inventory.len(), 1);
    }
}

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

//! Rule CAS, state and terminal audit share the canonical writer's single replacement.

use rocketmq_dashboard_common::{
    MonitorMutationOutcome, MonitorRuleChange, MonitorRuleMutation, MonitorValidationError,
};

use super::*;
use crate::infrastructure::{environment_store::MonitorMigrationState, monitor_store::MonitorRule};

impl DesktopConfigStore {
    pub async fn commit_monitor_rule(
        &self,
        request: MonitorRuleMutation,
        context: OperationContext,
    ) -> Result<MonitorMutationOutcome, ConfigStoreFailure> {
        request.validate().map_err(ConfigStoreFailure::validation)?;
        if request.environment_id != context.environment_id
            || context.resource != OperationResource::Monitor(request.consumer_group.clone())
        {
            return Err(ConfigStoreFailure::validation(MonitorValidationError::Group));
        }
        let _guard = self.io_gate.lock().await;
        if self.protected_original.load(Ordering::Acquire) || self.document.read().is_none() {
            return Err(ConfigStoreFailure::writer_unavailable());
        }
        let path = self.path.clone();
        let (document, outcome) = self
            .context
            .storage_io()
            .spawn_io("gpui-monitor-cas", move || {
                let (mut document, _) = load_document(&path)?;
                if context.configuration_revision != document.settings.revision
                    || context.environment_id != document.active_environment_id
                {
                    return Err(ConfigStoreFailure::stale_revision());
                }
                let matches = document
                    .monitors
                    .rules
                    .iter()
                    .enumerate()
                    .filter(|(_, rule)| {
                        rule.environment_id.as_ref() == Some(&context.environment_id)
                            && rule.rule.consumer_group == request.consumer_group
                    })
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                if matches.len() > 1 {
                    return Err(ConfigStoreFailure::validation(MonitorValidationError::AmbiguousLegacy));
                }
                let scoped = matches.first().copied();
                let legacy = match &request.change {
                    MonitorRuleChange::Save {
                        adopt_legacy_id: Some(id),
                        ..
                    } => {
                        if let Some(index) = scoped {
                            let latest = Some(document.monitors.rules[index].view());
                            return Ok((document, MonitorMutationOutcome::Conflict { latest }));
                        }
                        document.monitors.rules.iter().position(|rule| {
                            rule.environment_id.is_none()
                                && rule.rule.id == *id
                                && rule.rule.consumer_group == request.consumer_group
                        })
                    }
                    _ => None,
                };
                let index = legacy.or(scoped);
                let expected_matches = match index {
                    Some(index) => {
                        request.expected_revision != 0
                            && request.expected_revision == document.monitors.rules[index].revision
                    }
                    None => {
                        request.expected_revision == 0
                            && matches!(
                                request.change,
                                MonitorRuleChange::Save {
                                    adopt_legacy_id: None,
                                    ..
                                }
                            )
                    }
                };
                if !expected_matches
                    || document
                        .local_operations
                        .iter()
                        .any(|record| record.context.request_id == context.request_id)
                {
                    let latest = index.map(|index| document.monitors.rules[index].view());
                    return Ok((document, MonitorMutationOutcome::Conflict { latest }));
                }
                let next_revision = document
                    .monitors
                    .revision_counter
                    .max(
                        document
                            .monitors
                            .rules
                            .iter()
                            .map(|rule| rule.revision)
                            .max()
                            .unwrap_or(0),
                    )
                    .checked_add(1)
                    .ok_or_else(|| ConfigStoreFailure::validation(MonitorValidationError::Revision))?;
                document.monitors.revision_counter = next_revision;
                let (kind, outcome) = match request.change {
                    MonitorRuleChange::Save {
                        thresholds, enabled, ..
                    } => {
                        let id = index
                            .map(|index| document.monitors.rules[index].rule.id.clone())
                            .unwrap_or_else(|| request.consumer_group.clone());
                        let timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_err(ConfigStoreFailure::validation)?;
                        let updated_at_epoch_ms =
                            u64::try_from(timestamp.as_millis()).map_err(ConfigStoreFailure::validation)?;
                        let saved = StoredMonitorRule {
                            environment_id: Some(context.environment_id.clone()),
                            rule: MonitorRule {
                                id,
                                consumer_group: request.consumer_group,
                                enabled,
                            },
                            migration_state: MonitorMigrationState::Ready,
                            thresholds: Some(thresholds),
                            revision: next_revision,
                            updated_at_epoch_ms: Some(updated_at_epoch_ms),
                        };
                        let view = saved.view();
                        if let Some(index) = index {
                            document.monitors.rules[index] = saved;
                        } else {
                            document.monitors.rules.push(saved);
                        }
                        (LocalOperationKind::MonitorSaved, MonitorMutationOutcome::Saved(view))
                    }
                    MonitorRuleChange::Delete => {
                        if let Some(index) = index {
                            document.monitors.rules.remove(index);
                        }
                        (LocalOperationKind::MonitorDeleted, MonitorMutationOutcome::Deleted)
                    }
                };
                let document = persist_local_document(&path, document, context, kind)?;
                Ok::<_, ConfigStoreFailure>((document, outcome))
            })
            .await
            .map_err(ConfigStoreFailure::runtime)??;
        *self.document.write() = Some(document);
        Ok(outcome)
    }
}

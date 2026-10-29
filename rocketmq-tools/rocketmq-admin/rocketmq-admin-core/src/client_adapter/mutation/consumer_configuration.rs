// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Versioned Dashboard patches merged with one authoritative Broker's editable state.

use super::MutationAdminSession;
use crate::core::{
    consumer_configuration::{
        ConsumerConfigPersistence, ConsumerWorkspaceConfigPatchResult, ConsumerWorkspaceMutationAdmin,
        PatchConsumerWorkspaceConfigRequest,
    },
    AdminError, AdminFuture, AdminResult,
};
use cheetah_string::CheetahString;
use rocketmq_client_rust::{
    MQAdminMutationExt, MetadataCasReport, MutationExpectedState, MutationPersistenceState,
    MutationSubscriptionGroupConfig, MutationSubscriptionGroupConfigState,
};

trait PatchExecutor {
    fn current<'a>(&'a self, address: &'a str, group: &'a str)
        -> AdminFuture<'a, MutationSubscriptionGroupConfigState>;
    fn commit<'a>(
        &'a self,
        request: &'a PatchConsumerWorkspaceConfigRequest,
        replacement: MutationSubscriptionGroupConfig,
    ) -> AdminFuture<'a, MetadataCasReport>;
}

impl PatchExecutor for MutationAdminSession {
    fn current<'a>(
        &'a self,
        address: &'a str,
        group: &'a str,
    ) -> AdminFuture<'a, MutationSubscriptionGroupConfigState> {
        Box::pin(async move {
            self.inner
                .inner
                .mutation_subscription_group_config_state(address.into(), group.into())
                .await
                .map_err(|source| super::promote_to_admin_error("consumer_workspace_config_preflight", source))
        })
    }
    fn commit<'a>(
        &'a self,
        request: &'a PatchConsumerWorkspaceConfigRequest,
        replacement: MutationSubscriptionGroupConfig,
    ) -> AdminFuture<'a, MetadataCasReport> {
        Box::pin(async move {
            self.inner
                .inner
                .replace_subscription_group_config_if_state(
                    CheetahString::from(request.broker_addr.as_str()),
                    CheetahString::from(request.group.as_str()),
                    MutationExpectedState::Present {
                        version: request.expected_version,
                    },
                    replacement,
                )
                .await
                .map_err(|source| super::promote_to_admin_error("consumer_workspace_config_cas", source))
        })
    }
}

impl ConsumerWorkspaceMutationAdmin for MutationAdminSession {
    fn patch_consumer_workspace_config<'a>(
        &'a mut self,
        request: &'a PatchConsumerWorkspaceConfigRequest,
    ) -> AdminFuture<'a, ConsumerWorkspaceConfigPatchResult> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            execute_patch(self, request).await
        })
    }
}

async fn execute_patch(
    executor: &impl PatchExecutor,
    request: &PatchConsumerWorkspaceConfigRequest,
) -> AdminResult<ConsumerWorkspaceConfigPatchResult> {
    let request = PatchConsumerWorkspaceConfigRequest::try_new(
        &request.broker_addr,
        &request.group,
        request.expected_version,
        request.patch,
    )?;
    let current = executor.current(&request.broker_addr, &request.group).await?;
    let MutationExpectedState::Present { version } = current.state else {
        return Err(AdminError::not_found(
            "consumer_group",
            "Subscription Group no longer exists",
        ));
    };
    if version != request.expected_version {
        return Ok(ConsumerWorkspaceConfigPatchResult::VersionConflict {
            expected_version: request.expected_version,
            actual_version: version,
        });
    }
    let mut replacement = current.config.ok_or_else(|| {
        AdminError::operation_failed(
            "consumer_workspace_config_preflight",
            "present state has no configuration",
        )
    })?;
    let patch = request.patch;
    if let Some(value) = patch.retry_max_times {
        replacement.retry_max_times = value;
    }
    if let Some(value) = patch.retry_queue_nums {
        replacement.retry_queue_nums = value as i32;
    }
    if let Some(value) = patch.consume_timeout_minutes {
        replacement.consume_timeout_minute = value as i32;
    }
    macro_rules! apply { ($($field:ident),+ $(,)?) => { $(if let Some(value) = patch.$field { replacement.$field = value; })+ }; }
    apply!(
        consume_enable,
        consume_from_min_enable,
        consume_broadcast_enable,
        consume_message_orderly,
        broker_id,
        which_broker_when_consume_slowly
    );
    let report = executor.commit(&request, replacement).await?;
    let MutationExpectedState::Present { version } = report.state else {
        return Err(AdminError::operation_failed(
            "consumer_workspace_config_cas",
            "Broker returned an absent state",
        ));
    };
    let persistence = match report.persistence {
        MutationPersistenceState::Failed => Some(ConsumerConfigPersistence::Failed),
        MutationPersistenceState::Unconfirmed => Some(ConsumerConfigPersistence::Unconfirmed),
        MutationPersistenceState::NotRequired | MutationPersistenceState::Persisted => None,
    };
    if let Some(persistence) = persistence {
        return Ok(ConsumerWorkspaceConfigPatchResult::PersistenceIncomplete {
            applied: report.applied,
            previous_version: request.expected_version,
            version,
            persistence,
        });
    }
    if !report.applied {
        return Ok(ConsumerWorkspaceConfigPatchResult::VersionConflict {
            expected_version: request.expected_version,
            actual_version: version,
        });
    }
    Ok(ConsumerWorkspaceConfigPatchResult::Applied {
        previous_version: request.expected_version,
        version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::consumer_configuration::ConsumerWorkspaceConfigPatch;
    use std::sync::Mutex;
    struct Fake {
        version: u64,
        applied: bool,
        persistence: MutationPersistenceState,
        writes: Mutex<Vec<(u64, MutationSubscriptionGroupConfig)>>,
    }
    fn current_value() -> MutationSubscriptionGroupConfig {
        MutationSubscriptionGroupConfig {
            consume_enable: true,
            consume_from_min_enable: false,
            consume_broadcast_enable: true,
            consume_message_orderly: false,
            retry_queue_nums: 4,
            retry_max_times: 16,
            broker_id: 7,
            which_broker_when_consume_slowly: 9,
            notify_consumer_ids_changed_enable: false,
            group_sys_flag: 23,
            consume_timeout_minute: 30,
        }
    }
    impl PatchExecutor for Fake {
        fn current<'a>(&'a self, _: &'a str, _: &'a str) -> AdminFuture<'a, MutationSubscriptionGroupConfigState> {
            Box::pin(async {
                Ok(MutationSubscriptionGroupConfigState {
                    state: MutationExpectedState::Present { version: self.version },
                    config: Some(current_value()),
                })
            })
        }
        fn commit<'a>(
            &'a self,
            request: &'a PatchConsumerWorkspaceConfigRequest,
            replacement: MutationSubscriptionGroupConfig,
        ) -> AdminFuture<'a, MetadataCasReport> {
            self.writes
                .lock()
                .unwrap()
                .push((request.expected_version, replacement));
            Box::pin(async {
                Ok(MetadataCasReport {
                    applied: self.applied,
                    changed: self.applied,
                    state: MutationExpectedState::Present {
                        version: self.version + u64::from(self.applied),
                    },
                    persistence: self.persistence,
                })
            })
        }
    }
    fn request() -> PatchConsumerWorkspaceConfigRequest {
        PatchConsumerWorkspaceConfigRequest::try_new(
            "broker:10911",
            "orders",
            7,
            ConsumerWorkspaceConfigPatch {
                retry_max_times: Some(3),
                consume_enable: Some(false),
                broker_id: Some(2),
                ..Default::default()
            },
        )
        .unwrap()
    }
    #[tokio::test]
    async fn dashboard_consumer_patch_decreases_retries_and_preserves_every_unselected_field() {
        let fake = Fake {
            version: 7,
            applied: true,
            persistence: MutationPersistenceState::Persisted,
            writes: Mutex::new(Vec::new()),
        };
        assert_eq!(
            execute_patch(&fake, &request()).await.unwrap(),
            ConsumerWorkspaceConfigPatchResult::Applied {
                previous_version: 7,
                version: 8
            }
        );
        let writes = fake.writes.lock().unwrap();
        let mut expected = current_value();
        expected.retry_max_times = 3;
        expected.consume_enable = false;
        expected.broker_id = 2;
        assert_eq!(writes.as_slice(), &[(7, expected)]);
    }
    #[tokio::test]
    async fn dashboard_consumer_patch_conflict_and_unconfirmed_persistence_do_not_replay() {
        let fake = Fake {
            version: 8,
            applied: true,
            persistence: MutationPersistenceState::Persisted,
            writes: Mutex::new(Vec::new()),
        };
        assert!(matches!(
            execute_patch(&fake, &request()).await.unwrap(),
            ConsumerWorkspaceConfigPatchResult::VersionConflict { actual_version: 8, .. }
        ));
        assert!(fake.writes.lock().unwrap().is_empty());
        let fake = Fake {
            version: 7,
            applied: true,
            persistence: MutationPersistenceState::Unconfirmed,
            writes: Mutex::new(Vec::new()),
        };
        assert!(matches!(
            execute_patch(&fake, &request()).await.unwrap(),
            ConsumerWorkspaceConfigPatchResult::PersistenceIncomplete {
                persistence: ConsumerConfigPersistence::Unconfirmed,
                ..
            }
        ));
        assert_eq!(fake.writes.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn dashboard_consumer_patch_persistence_dirty_and_applied_failures_preserve_distinct_evidence() {
        for (applied, persistence) in [
            (false, MutationPersistenceState::Failed),
            (true, MutationPersistenceState::Failed),
            (true, MutationPersistenceState::Unconfirmed),
        ] {
            let fake = Fake {
                version: 7,
                applied,
                persistence,
                writes: Mutex::new(Vec::new()),
            };
            let result = execute_patch(&fake, &request()).await.unwrap();
            assert_eq!(
                result,
                ConsumerWorkspaceConfigPatchResult::PersistenceIncomplete {
                    applied,
                    previous_version: 7,
                    version: 7 + u64::from(applied),
                    persistence: match persistence {
                        MutationPersistenceState::Failed => ConsumerConfigPersistence::Failed,
                        MutationPersistenceState::Unconfirmed => ConsumerConfigPersistence::Unconfirmed,
                        _ => unreachable!(),
                    },
                }
            );
            assert_eq!(fake.writes.lock().unwrap().len(), 1);
        }
    }
    #[test]
    fn dashboard_consumer_patch_broker_ranges_are_separate_from_supervised_limits() {
        assert!(ConsumerWorkspaceConfigPatch {
            retry_max_times: Some(-1),
            retry_queue_nums: Some(0),
            ..Default::default()
        }
        .validate()
        .is_ok());
        assert!(ConsumerWorkspaceConfigPatch {
            retry_max_times: Some(-2),
            ..Default::default()
        }
        .validate()
        .is_err());
    }
}

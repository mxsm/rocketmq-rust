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

//! Dashboard changed-field edits use the closed state replacement protocol.

use super::supervised::{map_client_persistence, map_client_topic_config, map_topic_replacement_to_client};
use super::*;
use crate::core::topic::{
    execute_fields_patch, TopicConfigFieldsMutationAdmin, TopicConfigFieldsPatchReport, TopicConfigFieldsPatchRequest,
    TopicConfigFieldsState, TopicConfigFieldsTarget, TopicFieldsBackend, TopicFieldsWriteResult,
};

impl TopicConfigFieldsMutationAdmin for MutationAdminSession {
    fn patch_topic_config_fields<'a>(
        &'a mut self,
        request: &'a TopicConfigFieldsPatchRequest,
    ) -> AdminFuture<'a, TopicConfigFieldsPatchReport> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            execute_fields_patch(&mut ClientTopicFields(&self.inner.inner), request).await
        })
    }
}

struct ClientTopicFields<'a>(&'a rocketmq_client_rust::DefaultMQAdminExt);

impl TopicFieldsBackend for ClientTopicFields<'_> {
    fn validate_targets<'a>(&'a mut self, targets: &'a [TopicConfigFieldsTarget]) -> AdminFuture<'a, ()> {
        Box::pin(async move {
            let cluster_info = self
                .0
                .mutation_cluster_info()
                .await
                .map_err(|source| promote_to_admin_error("mutation_cluster_info", source))?;
            for target in targets {
                let exact = master_targets_by_cluster_name(&cluster_info, &target.cluster_name)?;
                if !exact
                    .iter()
                    .any(|(name, address)| name == &target.broker_name && address.as_str() == target.broker_address)
                {
                    return Err(AdminError::invalid_argument(
                        "targets",
                        "the confirmed Broker identity changed",
                    ));
                }
            }
            Ok(())
        })
    }

    fn state<'a>(
        &'a mut self,
        topic: &'a str,
        target: &'a TopicConfigFieldsTarget,
    ) -> AdminFuture<'a, TopicConfigFieldsState> {
        Box::pin(async move {
            let state = self
                .0
                .mutation_topic_config_state(target.broker_address.as_str().into(), topic.into())
                .await
                .map_err(|source| promote_to_admin_error("mutation_topic_config_state", source))?;
            Ok(TopicConfigFieldsState {
                version: match state.state {
                    ClientExpectedState::Absent => None,
                    ClientExpectedState::Present { version } => Some(version),
                },
                configuration: state.config.map(map_client_topic_config),
            })
        })
    }

    fn replace<'a>(
        &'a mut self,
        topic: &'a str,
        target: &'a TopicConfigFieldsTarget,
        replacement: &'a TopicReplacement,
    ) -> AdminFuture<'a, TopicFieldsWriteResult> {
        Box::pin(async move {
            let result = self
                .0
                .replace_topic_config_if_state(
                    target.broker_address.as_str().into(),
                    topic.into(),
                    ClientExpectedState::Present {
                        version: target.expected_version,
                    },
                    map_topic_replacement_to_client(replacement),
                )
                .await
                .map_err(|source| promote_to_admin_error("replace_topic_config_if_state", source))?;
            Ok(TopicFieldsWriteResult {
                applied: result.applied,
                version: match result.state {
                    ClientExpectedState::Absent => None,
                    ClientExpectedState::Present { version } => Some(version),
                },
                persistence: map_client_persistence(result.persistence),
            })
        })
    }

    fn order_config<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, Option<String>> {
        Box::pin(async move {
            self.0
                .mutation_order_topic_config(topic.into())
                .await
                .map(|value| value.map(|value| value.to_string()))
                .map_err(|source| promote_to_admin_error("mutation_order_topic_config", source))
        })
    }

    fn write_order_config<'a>(&'a mut self, topic: &'a str, value: Option<String>) -> AdminFuture<'a, ()> {
        Box::pin(async move {
            let result = match value {
                Some(value) => self.0.upsert_order_topic_config(topic.into(), value.into(), true).await,
                None => self.0.delete_order_topic_config(topic.into()).await,
            };
            result.map_err(|source| promote_to_admin_error("reconcile_order_topic_config", source))
        })
    }
}

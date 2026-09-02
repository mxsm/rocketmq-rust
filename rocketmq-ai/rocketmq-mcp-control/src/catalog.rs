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

use std::collections::BTreeSet;

use rmcp::model::ListToolsResult;
use rmcp::model::Tool;
use rmcp::model::ToolAnnotations;

use crate::config::MutationPolicyConfig;
use crate::model::ControlOperation;
use crate::tools::BrokerConfigMutationToolResponse;
use crate::tools::ConsumerGroupMutationToolResponse;
use crate::tools::OffsetMutationToolResponse;
use crate::tools::PatchBrokerConfigArgs;
use crate::tools::RequestModeMutationToolResponse;
use crate::tools::ResetConsumerOffsetArgs;
use crate::tools::SetConsumerRequestModeArgs;
use crate::tools::TopicMutationToolResponse;
use crate::tools::UpsertConsumerGroupArgs;
use crate::tools::UpsertTopicArgs;
use crate::tools::PATCH_BROKER_CONFIG_TOOL;
use crate::tools::RESET_CONSUMER_OFFSET_TOOL;
use crate::tools::SET_CONSUMER_REQUEST_MODE_TOOL;
use crate::tools::UPSERT_CONSUMER_GROUP_TOOL;
use crate::tools::UPSERT_TOPIC_TOOL;

#[derive(Debug, Clone, Default)]
pub struct OperationCatalog {
    operations: BTreeSet<ControlOperation>,
}

impl OperationCatalog {
    pub fn from_policy(policy: &MutationPolicyConfig) -> Self {
        #[cfg(feature = "write-tools")]
        let mut operations = BTreeSet::new();
        #[cfg(not(feature = "write-tools"))]
        let operations = BTreeSet::new();
        #[cfg(feature = "write-tools")]
        if policy.mutations_enabled {
            for operation in &policy.allowed_operations {
                operations.insert(*operation);
            }
        }
        #[cfg(not(feature = "write-tools"))]
        let _ = policy;
        Self { operations }
    }

    pub fn registered_operations(&self) -> u32 {
        u32::try_from(self.operations.len()).unwrap_or(u32::MAX)
    }

    pub fn list_tools(&self) -> ListToolsResult {
        ListToolsResult::with_all_items(self.operations.iter().copied().filter_map(tool_definition).collect())
    }

    pub fn list_tools_for(&self, allowed: impl Fn(ControlOperation) -> bool) -> ListToolsResult {
        ListToolsResult::with_all_items(
            self.operations
                .iter()
                .copied()
                .filter(|operation| allowed(*operation))
                .filter_map(tool_definition)
                .collect(),
        )
    }

    pub fn is_registered(&self, operation: ControlOperation) -> bool {
        self.operations.contains(&operation)
    }
}

fn tool_definition(operation: ControlOperation) -> Option<Tool> {
    let annotations = ToolAnnotations::with_title(match operation {
        ControlOperation::TopicUpsert => "Upsert RocketMQ topic",
        ControlOperation::ConsumerGroupUpsert => "Upsert RocketMQ consumer group",
        ControlOperation::ConsumerOffsetReset => "Reset RocketMQ consumer offset",
        ControlOperation::BrokerConfigPatch => "Patch RocketMQ Broker configuration",
        ControlOperation::ConsumerRequestMode => "Set RocketMQ consumer request mode",
    })
    .read_only(false)
    .destructive(true)
    .idempotent(true)
    .open_world(true);
    match operation {
        ControlOperation::TopicUpsert => Some(
            Tool::new(
                UPSERT_TOPIC_TOOL,
                "Dry-run or conditionally replace a complete Topic configuration on selected cluster masters.",
                std::sync::Arc::new(Default::default()),
            )
            .with_title("Upsert RocketMQ topic")
            .with_input_schema::<UpsertTopicArgs>()
            .with_output_schema::<TopicMutationToolResponse>()
            .with_annotations(annotations),
        ),
        ControlOperation::ConsumerGroupUpsert => Some(
            Tool::new(
                UPSERT_CONSUMER_GROUP_TOOL,
                "Dry-run or conditionally replace a complete Consumer Group configuration on selected cluster masters.",
                std::sync::Arc::new(Default::default()),
            )
            .with_title("Upsert RocketMQ consumer group")
            .with_input_schema::<UpsertConsumerGroupArgs>()
            .with_output_schema::<ConsumerGroupMutationToolResponse>()
            .with_annotations(annotations),
        ),
        ControlOperation::ConsumerOffsetReset => Some(
            Tool::new(
                RESET_CONSUMER_OFFSET_TOOL,
                "Dry-run or conditionally reset exact consumer queue offsets from a sealed RFC3339 preview.",
                std::sync::Arc::new(Default::default()),
            )
            .with_title("Reset RocketMQ consumer offset")
            .with_input_schema::<ResetConsumerOffsetArgs>()
            .with_output_schema::<OffsetMutationToolResponse>()
            .with_annotations(annotations),
        ),
        ControlOperation::BrokerConfigPatch => Some(
            Tool::new(
                PATCH_BROKER_CONFIG_TOOL,
                "Dry-run or generation-conditionally patch six allowlisted settings on one logical Broker.",
                std::sync::Arc::new(Default::default()),
            )
            .with_title("Patch RocketMQ Broker configuration")
            .with_input_schema::<PatchBrokerConfigArgs>()
            .with_output_schema::<BrokerConfigMutationToolResponse>()
            .with_annotations(annotations),
        ),
        ControlOperation::ConsumerRequestMode => Some(
            Tool::new(
                SET_CONSUMER_REQUEST_MODE_TOOL,
                "Dry-run or conditionally set pull/pop request mode on validated cluster masters.",
                std::sync::Arc::new(Default::default()),
            )
            .with_title("Set RocketMQ consumer request mode")
            .with_input_schema::<SetConsumerRequestModeArgs>()
            .with_output_schema::<RequestModeMutationToolResponse>()
            .with_annotations(annotations),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(enabled: bool, operations: Vec<ControlOperation>) -> MutationPolicyConfig {
        MutationPolicyConfig {
            mutations_enabled: enabled,
            dry_run: true,
            allowed_operations: operations,
            allowed_clusters: Vec::new(),
            operation_timeout_seconds: 12,
        }
    }

    #[test]
    fn catalog_is_policy_derived_and_registers_all_reviewed_operations() {
        assert_eq!(
            OperationCatalog::from_policy(&policy(false, vec![ControlOperation::TopicUpsert])).registered_operations(),
            0
        );
        let stage_c = OperationCatalog::from_policy(&policy(
            true,
            vec![
                ControlOperation::ConsumerOffsetReset,
                ControlOperation::BrokerConfigPatch,
                ControlOperation::ConsumerRequestMode,
            ],
        ));
        #[cfg(feature = "write-tools")]
        assert_eq!(stage_c.registered_operations(), 3);
        #[cfg(not(feature = "write-tools"))]
        assert_eq!(stage_c.registered_operations(), 0);

        #[cfg(feature = "write-tools")]
        {
            let one = OperationCatalog::from_policy(&policy(true, vec![ControlOperation::TopicUpsert]));
            assert_eq!(one.registered_operations(), 1);
            let two = OperationCatalog::from_policy(&policy(
                true,
                vec![ControlOperation::TopicUpsert, ControlOperation::ConsumerGroupUpsert],
            ));
            assert_eq!(two.registered_operations(), 2);
            for tool in two.list_tools().tools {
                assert_eq!(
                    tool.input_schema.get("additionalProperties"),
                    Some(&serde_json::json!(false))
                );
                let annotations = tool.annotations.unwrap();
                assert_eq!(annotations.read_only_hint, Some(false));
                assert_eq!(annotations.destructive_hint, Some(true));
                assert_eq!(annotations.idempotent_hint, Some(true));
                assert_eq!(annotations.open_world_hint, Some(true));
            }
            let all = OperationCatalog::from_policy(&policy(
                true,
                vec![
                    ControlOperation::TopicUpsert,
                    ControlOperation::ConsumerGroupUpsert,
                    ControlOperation::ConsumerOffsetReset,
                    ControlOperation::BrokerConfigPatch,
                    ControlOperation::ConsumerRequestMode,
                ],
            ));
            assert_eq!(all.registered_operations(), 5);
        }
    }

    #[cfg(feature = "write-tools")]
    #[test]
    fn reviewed_tool_schema_snapshot() {
        let catalog = OperationCatalog::from_policy(&policy(
            true,
            vec![
                ControlOperation::TopicUpsert,
                ControlOperation::ConsumerGroupUpsert,
                ControlOperation::ConsumerOffsetReset,
                ControlOperation::BrokerConfigPatch,
                ControlOperation::ConsumerRequestMode,
            ],
        ));
        insta::assert_json_snapshot!("control_reviewed_tool_schemas", catalog.list_tools().tools);
    }
}

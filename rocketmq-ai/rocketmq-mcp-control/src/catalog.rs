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
use std::sync::Arc;

use rmcp::model::JsonObject;
use rmcp::model::ListToolsResult;
use rmcp::model::Tool;
use rmcp::model::ToolAnnotations;

use crate::config::MutationPolicyConfig;
use crate::model::ControlOperation;
use crate::tools::BrokerConfigMutationToolResponse;
use crate::tools::BrokerLogFilterOperation;
use crate::tools::BrokerLogFilterToolResponse;
use crate::tools::ConsumerGroupMutationToolResponse;
use crate::tools::GetMutationResultArgs;
use crate::tools::MutationResultToolResponse;
use crate::tools::OffsetMutationToolResponse;
use crate::tools::PatchBrokerConfigArgs;
use crate::tools::ProxyDrainArgs;
use crate::tools::ProxyDrainOperation;
use crate::tools::ProxyDrainToolResponse;
use crate::tools::RequestModeMutationToolResponse;
use crate::tools::ResetConsumerOffsetArgs;
use crate::tools::RestoreBrokerLogFilterArgs;
use crate::tools::SetBrokerLogFilterTtlArgs;
use crate::tools::SetConsumerRequestModeArgs;
use crate::tools::TopicMutationToolResponse;
use crate::tools::UpsertConsumerGroupArgs;
use crate::tools::UpsertTopicArgs;
use crate::tools::BEGIN_PROXY_DRAIN_TOOL;
use crate::tools::CANCEL_PROXY_DRAIN_TOOL;
use crate::tools::GET_MUTATION_RESULT_TOOL;
use crate::tools::INVOCATION_ID_MEMBER;
use crate::tools::PATCH_BROKER_CONFIG_TOOL;
use crate::tools::RESET_CONSUMER_OFFSET_TOOL;
use crate::tools::RESTORE_BROKER_LOG_FILTER_TOOL;
use crate::tools::SET_BROKER_LOG_FILTER_TTL_TOOL;
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

/// Whether an operation replaces or withdraws something that is in use.
///
/// Ending a drain and setting or ending a log filter only add or give back: the filter is bounded
/// and the Broker ends it by itself. Beginning a drain takes a Proxy out of service, so it is
/// marked like the operations that replace state.
const fn is_destructive(operation: ControlOperation) -> bool {
    match operation {
        ControlOperation::TopicUpsert
        | ControlOperation::ConsumerGroupUpsert
        | ControlOperation::ConsumerOffsetReset
        | ControlOperation::BrokerConfigPatch
        | ControlOperation::ConsumerRequestMode
        | ControlOperation::ProxyDrainBegin => true,
        ControlOperation::ProxyDrainCancel
        | ControlOperation::BrokerLogFilterSet
        | ControlOperation::BrokerLogFilterRestore => false,
    }
}

fn tool_definition(operation: ControlOperation) -> Option<Tool> {
    let title = match operation {
        ControlOperation::TopicUpsert => "Upsert RocketMQ topic",
        ControlOperation::ConsumerGroupUpsert => "Upsert RocketMQ consumer group",
        ControlOperation::ConsumerOffsetReset => "Reset RocketMQ consumer offset",
        ControlOperation::BrokerConfigPatch => "Patch RocketMQ Broker configuration",
        ControlOperation::ConsumerRequestMode => "Set RocketMQ consumer request mode",
        ControlOperation::ProxyDrainBegin => "Begin RocketMQ Proxy drain",
        ControlOperation::ProxyDrainCancel => "Cancel RocketMQ Proxy drain",
        ControlOperation::BrokerLogFilterSet => "Set temporary RocketMQ Broker log level",
        ControlOperation::BrokerLogFilterRestore => "Restore RocketMQ Broker log level",
    };
    let annotations = ToolAnnotations::with_title(title)
        .read_only(false)
        .destructive(is_destructive(operation))
        .idempotent(true)
        .open_world(true);
    let tool = match operation {
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
        ControlOperation::ProxyDrainBegin => Some(
            Tool::new(
                BEGIN_PROXY_DRAIN_TOOL,
                "Dry-run or begin a drain of one configured Proxy: it stops admitting new work so that it can be \
                 restarted once nothing is pending. The drain lasts until rocketmq_cancel_proxy_drain is called \
                 with the same operation_id.",
                std::sync::Arc::new(Default::default()),
            )
            .with_title(title)
            .with_input_schema::<ProxyDrainArgs>()
            .with_output_schema::<ProxyDrainToolResponse>()
            .with_annotations(annotations),
        ),
        ControlOperation::ProxyDrainCancel => Some(
            Tool::new(
                CANCEL_PROXY_DRAIN_TOOL,
                "Dry-run or cancel the drain of one configured Proxy that rocketmq_begin_proxy_drain began with \
                 the same operation_id, so that the Proxy admits work again. A drain that another operation \
                 holds is left alone.",
                std::sync::Arc::new(Default::default()),
            )
            .with_title(title)
            .with_input_schema::<ProxyDrainArgs>()
            .with_output_schema::<ProxyDrainToolResponse>()
            .with_annotations(annotations),
        ),
        ControlOperation::BrokerLogFilterSet => Some(
            Tool::new(
                SET_BROKER_LOG_FILTER_TTL_TOOL,
                "Dry-run or set one rocketmq_broker:: logger of one Broker master to INFO or DEBUG for 60 to 900 \
                 seconds. The Broker returns to its startup log filter by itself when the time is up; \
                 rocketmq_restore_broker_log_filter ends the override earlier.",
                std::sync::Arc::new(Default::default()),
            )
            .with_title(title)
            .with_input_schema::<SetBrokerLogFilterTtlArgs>()
            .with_output_schema::<BrokerLogFilterToolResponse>()
            .with_annotations(annotations),
        ),
        ControlOperation::BrokerLogFilterRestore => Some(
            Tool::new(
                RESTORE_BROKER_LOG_FILTER_TOOL,
                "Dry-run or end the temporary log level that rocketmq_set_broker_log_filter_ttl set on one \
                 Broker master with the same operation_id, returning the Broker to its startup log filter. An \
                 override that another operation holds is left alone.",
                std::sync::Arc::new(Default::default()),
            )
            .with_title(title)
            .with_input_schema::<RestoreBrokerLogFilterArgs>()
            .with_output_schema::<BrokerLogFilterToolResponse>()
            .with_annotations(annotations),
        ),
    };
    tool.map(|mut tool| {
        unwrap_descriptions(Arc::make_mut(&mut tool.input_schema));
        if let Some(output_schema) = &mut tool.output_schema {
            let output_schema = Arc::make_mut(output_schema);
            declare_invocation_id(output_schema);
            pin_shared_operation(output_schema, operation);
            unwrap_descriptions(output_schema);
        }
        tool
    })
}

/// Narrows the operation of a response type that two Tools share to the operation of this Tool.
///
/// A Tool with a response type of its own fixes its operation in that type. The two drain Tools
/// share one response type, and so do the two log-filter Tools; the schema that each of them
/// publishes still allows only its own operation.
fn pin_shared_operation(output_schema: &mut JsonObject, operation: ControlOperation) {
    use schemars::JsonSchema;

    let shared = match operation {
        ControlOperation::ProxyDrainBegin | ControlOperation::ProxyDrainCancel => ProxyDrainOperation::schema_name(),
        ControlOperation::BrokerLogFilterSet | ControlOperation::BrokerLogFilterRestore => {
            BrokerLogFilterOperation::schema_name()
        }
        ControlOperation::TopicUpsert
        | ControlOperation::ConsumerGroupUpsert
        | ControlOperation::ConsumerOffsetReset
        | ControlOperation::BrokerConfigPatch
        | ControlOperation::ConsumerRequestMode => return,
    };
    if let Some(definition) = output_schema
        .get_mut("$defs")
        .and_then(|definitions| definitions.get_mut(shared.as_ref()))
    {
        *definition = serde_json::json!({"type": "string", "const": operation.as_str()});
    }
}

/// Declares the `invocation_id` that every mutation response carries.
///
/// The server adds the member when it sends a response, so the response types do not hold it and
/// the published schema declares it here, once for all mutation Tools.
fn declare_invocation_id(output_schema: &mut JsonObject) {
    let description = "Identifies this call in the durable audit log. Pass it to rocketmq_get_mutation_result to \
                       read how the call ended.";
    if let Some(properties) = output_schema
        .get_mut("properties")
        .and_then(serde_json::Value::as_object_mut)
    {
        properties.insert(
            INVOCATION_ID_MEMBER.to_owned(),
            serde_json::json!({
                "description": description,
                "type": "integer",
                "format": "uint64",
                "minimum": 1,
            }),
        );
    }
    if let Some(required) = output_schema
        .get_mut("required")
        .and_then(serde_json::Value::as_array_mut)
    {
        required.push(serde_json::Value::from(INVOCATION_ID_MEMBER));
    }
}

/// Returns the definition of the one read-only Tool of this server.
///
/// It is not a [`ControlOperation`]: it changes nothing, is not part of `allowed_operations`, and
/// stays available when mutations are switched off. Its annotations say so, unlike those of the
/// mutation Tools.
pub fn result_tool_definition() -> Tool {
    let title = "Get RocketMQ mutation result";
    let mut tool = Tool::new(
        GET_MUTATION_RESULT_TOOL,
        "Reads how one of your own mutation calls on a cluster ended: finished with its status, still in \
         progress, interrupted by a server restart, or not found. Use it when a mutation response was lost or \
         timed out, selecting the call by the invocation_id of a response or by the request_key you sent. It \
         changes nothing and never returns who made a call or why.",
        std::sync::Arc::new(Default::default()),
    )
    .with_title(title)
    .with_input_schema::<GetMutationResultArgs>()
    .with_output_schema::<MutationResultToolResponse>()
    .with_annotations(
        ToolAnnotations::with_title(title)
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            // It reads this server's own record of its calls, not the cluster.
            .open_world(false),
    );
    unwrap_descriptions(Arc::make_mut(&mut tool.input_schema));
    if let Some(output_schema) = &mut tool.output_schema {
        unwrap_descriptions(Arc::make_mut(output_schema));
    }
    tool
}

/// Joins the wrapped lines of every `description` in a schema, keeping paragraph breaks.
///
/// schemars publishes a doc comment with its source line breaks. Left in, re-wrapping a comment
/// would change the published Tool schema.
fn unwrap_descriptions(schema: &mut JsonObject) {
    for (keyword, value) in schema.iter_mut() {
        match value {
            serde_json::Value::String(text) if keyword == "description" && text.contains('\n') => {
                *text = unwrap_lines(text);
            }
            // A value under these keywords is data, not a schema.
            _ if matches!(keyword.as_str(), "default" | "const" | "enum" | "examples") => {}
            serde_json::Value::Object(nested) => unwrap_descriptions(nested),
            serde_json::Value::Array(items) => items
                .iter_mut()
                .filter_map(serde_json::Value::as_object_mut)
                .for_each(unwrap_descriptions),
            _ => {}
        }
    }
}

fn unwrap_lines(text: &str) -> String {
    let paragraphs = text.split("\n\n").map(|paragraph| {
        let lines = paragraph.lines().map(str::trim).filter(|line| !line.is_empty());
        lines.collect::<Vec<_>>().join(" ")
    });
    paragraphs.collect::<Vec<_>>().join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_OPERATIONS: [ControlOperation; 9] = [
        ControlOperation::TopicUpsert,
        ControlOperation::ConsumerGroupUpsert,
        ControlOperation::ConsumerOffsetReset,
        ControlOperation::BrokerConfigPatch,
        ControlOperation::ConsumerRequestMode,
        ControlOperation::ProxyDrainBegin,
        ControlOperation::ProxyDrainCancel,
        ControlOperation::BrokerLogFilterSet,
        ControlOperation::BrokerLogFilterRestore,
    ];

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
            let all = OperationCatalog::from_policy(&policy(true, ALL_OPERATIONS.to_vec()));
            assert_eq!(all.registered_operations(), 9);
            // Each operation is allowed by itself: a caller may end a drain or a log filter
            // without being able to begin one.
            for operation in ALL_OPERATIONS {
                let only = OperationCatalog::from_policy(&policy(true, vec![operation]));
                assert_eq!(only.registered_operations(), 1);
                assert!(only.is_registered(operation));
                assert_eq!(only.list_tools().tools.len(), 1);
            }
        }
    }

    fn all_operations() -> OperationCatalog {
        OperationCatalog {
            operations: BTreeSet::from(ALL_OPERATIONS),
        }
    }

    #[test]
    fn only_an_operation_that_takes_something_out_of_service_is_marked_destructive() {
        for tool in all_operations().list_tools().tools {
            let annotations = tool.annotations.as_ref().expect("annotations");
            let restoring = [
                CANCEL_PROXY_DRAIN_TOOL,
                SET_BROKER_LOG_FILTER_TTL_TOOL,
                RESTORE_BROKER_LOG_FILTER_TOOL,
            ]
            .contains(&tool.name.as_ref());
            assert_eq!(annotations.destructive_hint, Some(!restoring), "{}", tool.name);
            assert_eq!(annotations.read_only_hint, Some(false), "{}", tool.name);
            assert_eq!(annotations.idempotent_hint, Some(true), "{}", tool.name);
            assert_eq!(annotations.open_world_hint, Some(true), "{}", tool.name);
            assert_eq!(annotations.title.as_deref(), tool.title.as_deref(), "{}", tool.name);
        }
    }

    /// Calls `visit` with the pointer and the schema of every input property, nested ones included.
    fn for_each_input_property(schema: &JsonObject, pointer: &str, visit: &mut impl FnMut(&str, &JsonObject)) {
        for keyword in ["properties", "$defs"] {
            let Some(named) = schema.get(keyword).and_then(serde_json::Value::as_object) else {
                continue;
            };
            for (name, nested) in named {
                let pointer = format!("{pointer}/{name}");
                let nested = nested.as_object().expect("a schema object");
                if keyword == "properties" {
                    visit(&pointer, nested);
                }
                for_each_input_property(nested, &pointer, visit);
            }
        }
    }

    #[test]
    fn every_input_property_has_a_description() {
        let tools = all_operations().list_tools().tools;
        assert_eq!(tools.len(), 9);
        let mut described = 0;
        for tool in tools {
            for_each_input_property(&tool.input_schema, "", &mut |pointer, property| {
                let description = property.get("description").and_then(serde_json::Value::as_str);
                assert!(
                    description.is_some_and(|text| !text.trim().is_empty()),
                    "{}: input {pointer} has no description",
                    tool.name
                );
                described += 1;
            });
        }
        // 96 arguments over the nine Tools, plus the six settings of a Broker configuration patch.
        assert_eq!(described, 102);
    }

    #[test]
    fn published_descriptions_do_not_keep_source_line_breaks() {
        fn assert_unwrapped(tool: &str, value: &serde_json::Value) {
            match value {
                serde_json::Value::Object(members) => {
                    for (keyword, nested) in members {
                        match nested.as_str() {
                            Some(text) if keyword == "description" => {
                                let single_break = text.replace("\n\n", "").contains('\n');
                                assert!(!single_break, "{tool}: wrapped description {text:?}");
                            }
                            _ => assert_unwrapped(tool, nested),
                        }
                    }
                }
                serde_json::Value::Array(items) => items.iter().for_each(|item| assert_unwrapped(tool, item)),
                _ => {}
            }
        }

        for tool in all_operations().list_tools().tools {
            assert_unwrapped(&tool.name, &serde_json::to_value(&tool).unwrap());
        }
        assert_eq!(
            unwrap_lines("first line\n  second line\n\nnext paragraph\nends here"),
            "first line second line\n\nnext paragraph ends here"
        );
        let mut schema = serde_json::json!({
            "description": "wrapped\ntext",
            "default": {"description": "data\nstays"},
            "properties": {"description": {"description": "nested\ntext", "enum": ["a\nb"]}}
        });
        unwrap_descriptions(schema.as_object_mut().unwrap());
        assert_eq!(
            schema,
            serde_json::json!({
                "description": "wrapped text",
                "default": {"description": "data\nstays"},
                "properties": {"description": {"description": "nested text", "enum": ["a\nb"]}}
            })
        );
    }

    #[cfg(feature = "write-tools")]
    #[test]
    fn reviewed_tool_schema_snapshot() {
        let catalog = OperationCatalog::from_policy(&policy(true, ALL_OPERATIONS.to_vec()));
        // The mutation Tools in catalog order, then the one read-only Tool.
        let mut tools = catalog.list_tools().tools;
        tools.push(result_tool_definition());
        insta::assert_json_snapshot!("control_reviewed_tool_schemas", tools);
    }

    #[test]
    fn every_mutation_output_schema_allows_only_the_operation_of_its_tool() {
        for operation in ALL_OPERATIONS {
            let tool = tool_definition(operation).expect("a reviewed Tool");
            let schema = tool.output_schema.as_deref().expect("an output schema");
            let definition = schema["properties"]["operation"]["$ref"]
                .as_str()
                .and_then(|reference| reference.strip_prefix("#/$defs/"))
                .expect("a definition of the operation");
            assert_eq!(
                schema["$defs"][definition],
                serde_json::json!({"type": "string", "const": operation.as_str()}),
                "{}",
                tool.name
            );
        }
    }

    #[test]
    fn every_mutation_output_schema_declares_the_invocation_id() {
        for tool in all_operations().list_tools().tools {
            let schema = tool.output_schema.as_deref().expect("an output schema");
            let declared = &schema["properties"][INVOCATION_ID_MEMBER];
            assert_eq!(declared["type"], "integer", "{}", tool.name);
            assert_eq!(declared["minimum"], 1, "{}", tool.name);
            assert!(declared["description"]
                .as_str()
                .is_some_and(|text| text.contains(GET_MUTATION_RESULT_TOOL) && !text.contains('\n')));
            let required = schema["required"].as_array().expect("required members");
            assert_eq!(
                required
                    .iter()
                    .filter(|member| member.as_str() == Some(INVOCATION_ID_MEMBER))
                    .count(),
                1,
                "{}",
                tool.name
            );
        }
    }

    /// Collects the property names a schema declares, at any depth.
    fn property_names(schema: &serde_json::Value, names: &mut BTreeSet<String>) {
        match schema {
            serde_json::Value::Object(keywords) => {
                for (keyword, value) in keywords {
                    if let ("properties", serde_json::Value::Object(properties)) = (keyword.as_str(), value) {
                        names.extend(properties.keys().cloned());
                    }
                    property_names(value, names);
                }
            }
            serde_json::Value::Array(values) => values.iter().for_each(|value| property_names(value, names)),
            _ => {}
        }
    }

    #[test]
    fn the_result_tool_is_read_only_closed_and_names_no_caller() {
        let tool = result_tool_definition();
        assert_eq!(tool.name, GET_MUTATION_RESULT_TOOL);
        let annotations = tool.annotations.clone().expect("annotations");
        assert_eq!(annotations.read_only_hint, Some(true));
        assert_eq!(annotations.destructive_hint, Some(false));
        assert_eq!(annotations.idempotent_hint, Some(true));
        assert_eq!(annotations.open_world_hint, Some(false));

        assert_eq!(
            tool.input_schema.get("additionalProperties"),
            Some(&serde_json::json!(false))
        );
        assert_eq!(
            tool.input_schema.get("required"),
            Some(&serde_json::json!(["schema_version", "cluster"]))
        );
        let mut described = 0;
        for_each_input_property(&tool.input_schema, "", &mut |pointer, property| {
            let description = property.get("description").and_then(serde_json::Value::as_str);
            assert!(
                description.is_some_and(|text| !text.trim().is_empty()),
                "input {pointer} has no description"
            );
            described += 1;
        });
        assert_eq!(described, 4);
        let mut inputs = BTreeSet::new();
        property_names(
            &serde_json::Value::Object(tool.input_schema.as_ref().clone()),
            &mut inputs,
        );
        assert_eq!(
            inputs.into_iter().collect::<Vec<_>>(),
            ["cluster", "invocation_id", "request_key", "schema_version"]
        );

        let output = serde_json::Value::Object(tool.output_schema.as_deref().expect("an output schema").clone());
        assert_eq!(output["additionalProperties"], false);
        let mut outputs = BTreeSet::new();
        property_names(&output, &mut outputs);
        for private in [
            "operator",
            "reason",
            "request_key",
            "subject",
            "digest",
            "requested_digest",
        ] {
            assert!(!outputs.contains(private), "the answer declares {private}");
        }
        assert!(outputs
            .iter()
            .all(|name| !name.contains("addr") && !name.contains("endpoint")));
        // Everything but the envelope is null in some state, and the schema has to allow that.
        for nullable in [
            "invocation_id",
            "operation",
            "mode",
            "target",
            "status",
            "error_code",
            "duration_millis",
            "changed",
            "target_results",
        ] {
            let alternatives = output["properties"][nullable]["anyOf"]
                .as_array()
                .unwrap_or_else(|| panic!("{nullable} has no alternatives"));
            assert!(
                alternatives.contains(&serde_json::json!({"type": "null"})),
                "{nullable} cannot be null"
            );
        }
        let states = &output["$defs"]["MutationResultState"];
        let mut declared_states = BTreeSet::new();
        collect_strings(states, &mut declared_states);
        for state in ["finished", "in_progress", "interrupted", "not_found"] {
            assert!(declared_states.contains(state), "state {state} is not declared");
        }
        // The description sends a caller here for a lost response and stays within a Tool listing.
        let description = tool.description.as_deref().expect("a description");
        assert!(
            description.contains("request_key") && description.len() <= 400,
            "{description}"
        );
    }

    fn collect_strings(value: &serde_json::Value, strings: &mut BTreeSet<String>) {
        match value {
            serde_json::Value::String(text) => {
                strings.insert(text.clone());
            }
            serde_json::Value::Array(values) => values.iter().for_each(|value| collect_strings(value, strings)),
            serde_json::Value::Object(members) => members.values().for_each(|value| collect_strings(value, strings)),
            _ => {}
        }
    }
}

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

use schemars::JsonSchema;

use rmcp::model::ListToolsResult;
use rmcp::model::Tool;
use rmcp::model::ToolAnnotations;

use crate::guard::RiskLevel;
use crate::tools::broker_tools;
#[cfg(feature = "dangerous-tools")]
use crate::tools::change_tools;
use crate::tools::cluster_tools;
use crate::tools::consumer_tools;
use crate::tools::diagnosis_tools;
use crate::tools::topic_tools;

pub fn list_tools() -> ListToolsResult {
    ListToolsResult::with_all_items(tool_definitions())
}

pub fn get_tool(name: &str) -> Option<Tool> {
    tool_definitions().into_iter().find(|tool| tool.name.as_ref() == name)
}

pub fn tool_risk_level(name: &str) -> Option<RiskLevel> {
    match name {
        cluster_tools::CLUSTER_OVERVIEW_TOOL
        | topic_tools::LIST_TOPICS_TOOL
        | topic_tools::DESCRIBE_TOPIC_TOOL
        | topic_tools::QUERY_TOPIC_ROUTE_TOOL
        | consumer_tools::LIST_CONSUMER_GROUPS_TOOL
        | consumer_tools::QUERY_CONSUMER_LAG_TOOL
        | broker_tools::DESCRIBE_BROKER_TOOL => Some(RiskLevel::ReadOnly),
        diagnosis_tools::DIAGNOSE_CONSUMER_LAG_TOOL => Some(RiskLevel::Diagnose),
        #[cfg(feature = "dangerous-tools")]
        change_tools::CREATE_TOPIC_TOOL
        | change_tools::UPDATE_TOPIC_CONFIG_TOOL
        | change_tools::UPDATE_TOPIC_PERM_TOOL
        | change_tools::UPDATE_BROKER_CONFIG_TOOL
        | change_tools::RESET_CONSUMER_OFFSET_TOOL => Some(RiskLevel::Change),
        _ => None,
    }
}

pub fn tool_definitions() -> Vec<Tool> {
    let tools = vec![
        read_only_tool::<cluster_tools::ClusterOverviewArgs, cluster_tools::ClusterOverviewOutput>(
            cluster_tools::CLUSTER_OVERVIEW_TOOL,
            "RocketMQ cluster overview",
            "Summarize configured cluster brokers, topic count, and consumer group count.",
        ),
        read_only_tool::<topic_tools::ListTopicsArgs, topic_tools::ListTopicsOutput>(
            topic_tools::LIST_TOPICS_TOOL,
            "RocketMQ topic list",
            "List topics visible from the selected RocketMQ cluster.",
        ),
        read_only_tool::<topic_tools::DescribeTopicArgs, topic_tools::DescribeTopicOutput>(
            topic_tools::DESCRIBE_TOPIC_TOOL,
            "RocketMQ topic description",
            "Describe a topic with broker and queue route information.",
        ),
        read_only_tool::<topic_tools::QueryTopicRouteArgs, topic_tools::QueryTopicRouteOutput>(
            topic_tools::QUERY_TOPIC_ROUTE_TOOL,
            "RocketMQ topic route",
            "Query topic route data including broker addresses and queue distribution.",
        ),
        read_only_tool::<consumer_tools::ListConsumerGroupsArgs, consumer_tools::ListConsumerGroupsOutput>(
            consumer_tools::LIST_CONSUMER_GROUPS_TOOL,
            "RocketMQ consumer groups",
            "List consumer groups and current consumption summary.",
        ),
        read_only_tool::<consumer_tools::QueryConsumerLagArgs, consumer_tools::QueryConsumerLagOutput>(
            consumer_tools::QUERY_CONSUMER_LAG_TOOL,
            "RocketMQ consumer lag",
            "Query per-queue consumer lag for a topic and consumer group.",
        ),
        read_only_tool::<broker_tools::DescribeBrokerArgs, broker_tools::DescribeBrokerOutput>(
            broker_tools::DESCRIBE_BROKER_TOOL,
            "RocketMQ broker description",
            "Describe broker rows for a broker name in the selected cluster.",
        ),
        read_only_tool::<diagnosis_tools::DiagnoseConsumerLagArgs, crate::model::diagnosis::DiagnosisReport>(
            diagnosis_tools::DIAGNOSE_CONSUMER_LAG_TOOL,
            "RocketMQ consumer lag diagnosis",
            "Diagnose consumer lag from read-only lag, topic route, and broker evidence.",
        ),
    ];

    #[cfg(feature = "dangerous-tools")]
    {
        let mut tools = tools;
        tools.extend([
            change_tool::<change_tools::CreateTopicArgs, change_tools::ChangePlan>(
                change_tools::CREATE_TOPIC_TOOL,
                "RocketMQ create topic plan",
                "Generate a dry-run change plan for future topic creation support.",
            ),
            change_tool::<change_tools::UpdateTopicConfigArgs, change_tools::ChangePlan>(
                change_tools::UPDATE_TOPIC_CONFIG_TOOL,
                "RocketMQ update topic config plan",
                "Generate a dry-run change plan for future topic config updates.",
            ),
            change_tool::<change_tools::UpdateTopicPermArgs, change_tools::ChangePlan>(
                change_tools::UPDATE_TOPIC_PERM_TOOL,
                "RocketMQ update topic permission plan",
                "Generate a dry-run change plan for future topic permission updates.",
            ),
            change_tool::<change_tools::UpdateBrokerConfigArgs, change_tools::ChangePlan>(
                change_tools::UPDATE_BROKER_CONFIG_TOOL,
                "RocketMQ update broker config plan",
                "Generate a dry-run change plan for future broker config updates.",
            ),
            change_tool::<change_tools::ResetConsumerOffsetArgs, change_tools::ChangePlan>(
                change_tools::RESET_CONSUMER_OFFSET_TOOL,
                "RocketMQ reset consumer offset plan",
                "Generate a dry-run impact plan for future consumer offset resets.",
            ),
        ]);
        tools
    }

    #[cfg(not(feature = "dangerous-tools"))]
    {
        tools
    }
}

fn read_only_tool<I, O>(name: &'static str, title: &'static str, description: &'static str) -> Tool
where
    I: JsonSchema + 'static,
    O: JsonSchema + 'static,
{
    Tool::new(name, description, std::sync::Arc::new(Default::default()))
        .with_title(title)
        .with_input_schema::<I>()
        .with_output_schema::<O>()
        .with_annotations(
            ToolAnnotations::with_title(title)
                .read_only(true)
                .destructive(false)
                .idempotent(true)
                .open_world(true),
        )
}

#[cfg(feature = "dangerous-tools")]
fn change_tool<I, O>(name: &'static str, title: &'static str, description: &'static str) -> Tool
where
    I: JsonSchema + 'static,
    O: JsonSchema + 'static,
{
    Tool::new(name, description, std::sync::Arc::new(Default::default()))
        .with_title(title)
        .with_input_schema::<I>()
        .with_output_schema::<O>()
        .with_annotations(
            ToolAnnotations::with_title(title)
                .read_only(false)
                .destructive(false)
                .idempotent(false)
                .open_world(true),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_tools_returns_all_read_only_mvp_tools() {
        let result = list_tools();
        let names = result.tools.iter().map(|tool| tool.name.as_ref()).collect::<Vec<_>>();

        assert!(names.starts_with(&mvp_tool_names()));
        #[cfg(not(feature = "dangerous-tools"))]
        assert_eq!(names, mvp_tool_names());
        #[cfg(feature = "dangerous-tools")]
        assert_eq!(names, all_tool_names());
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn each_tool_has_input_schema_and_expected_annotation() {
        for tool in tool_definitions() {
            assert_eq!(
                tool.input_schema.get("type").and_then(|value| value.as_str()),
                Some("object")
            );
            assert!(tool.output_schema.is_some());
            let annotations = tool.annotations.as_ref().expect("tool annotations");
            assert_eq!(annotations.destructive_hint, Some(false));
            if is_mvp_tool(tool.name.as_ref()) {
                assert_eq!(annotations.read_only_hint, Some(true));
                assert_eq!(annotations.idempotent_hint, Some(true));
            } else {
                assert_eq!(annotations.read_only_hint, Some(false));
                assert_eq!(annotations.idempotent_hint, Some(false));
            }
        }
    }

    #[test]
    fn each_registered_tool_has_a_guard_risk_level() {
        for tool in tool_definitions() {
            assert!(tool_risk_level(tool.name.as_ref()).is_some());
        }
        assert_eq!(
            tool_risk_level(diagnosis_tools::DIAGNOSE_CONSUMER_LAG_TOOL),
            Some(RiskLevel::Diagnose)
        );
    }

    #[test]
    fn tool_contract_schema_metadata_snapshot() {
        let contracts = tool_definitions()
            .into_iter()
            .map(|tool| serde_json::to_value(tool).expect("tool contract serializes"))
            .collect::<Vec<_>>();

        #[cfg(not(feature = "dangerous-tools"))]
        insta::assert_json_snapshot!("tool_contract_schema_metadata", contracts);

        #[cfg(feature = "dangerous-tools")]
        insta::assert_json_snapshot!("tool_contract_schema_metadata_with_dangerous_tools", contracts);
    }

    fn mvp_tool_names() -> Vec<&'static str> {
        vec![
            "mq_cluster_overview",
            "mq_list_topics",
            "mq_describe_topic",
            "mq_query_topic_route",
            "mq_list_consumer_groups",
            "mq_query_consumer_lag",
            "mq_describe_broker",
            "mq_diagnose_consumer_lag",
        ]
    }

    #[cfg(feature = "dangerous-tools")]
    fn all_tool_names() -> Vec<&'static str> {
        let mut names = mvp_tool_names();
        names.extend([
            "mq_create_topic",
            "mq_update_topic_config",
            "mq_update_topic_perm",
            "mq_update_broker_config",
            "mq_reset_consumer_offset",
        ]);
        names
    }

    fn is_mvp_tool(name: &str) -> bool {
        mvp_tool_names().contains(&name)
    }
}

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

use rmcp::model::ListToolsResult;
use rmcp::model::TaskSupport;
use rmcp::model::Tool;
use rmcp::model::ToolAnnotations;
use rmcp::model::ToolExecution;
use schemars::JsonSchema;

use crate::guard::RiskLevel;
use crate::model::contract::ToolResponse;
use crate::tools::broker_tools;
#[cfg(feature = "change-planning")]
use crate::tools::change_tools;
use crate::tools::cluster_tools;
use crate::tools::consumer_tools;
use crate::tools::diagnosis_tools;
use crate::tools::topic_tools;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolId {
    GetClusterOverview,
    ListTopics,
    DescribeTopic,
    GetTopicRoute,
    ListConsumerGroups,
    GetConsumerLag,
    DescribeBroker,
    DiagnoseConsumerLag,
    #[cfg(feature = "change-planning")]
    PlanCreateTopic,
    #[cfg(feature = "change-planning")]
    PlanUpdateTopicConfig,
    #[cfg(feature = "change-planning")]
    PlanUpdateTopicPermissions,
    #[cfg(feature = "change-planning")]
    PlanUpdateBrokerConfig,
    #[cfg(feature = "change-planning")]
    PlanResetConsumerOffset,
}

impl ToolId {
    pub const ALL: &'static [Self] = &[
        Self::GetClusterOverview,
        Self::ListTopics,
        Self::DescribeTopic,
        Self::GetTopicRoute,
        Self::ListConsumerGroups,
        Self::GetConsumerLag,
        Self::DescribeBroker,
        Self::DiagnoseConsumerLag,
        #[cfg(feature = "change-planning")]
        Self::PlanCreateTopic,
        #[cfg(feature = "change-planning")]
        Self::PlanUpdateTopicConfig,
        #[cfg(feature = "change-planning")]
        Self::PlanUpdateTopicPermissions,
        #[cfg(feature = "change-planning")]
        Self::PlanUpdateBrokerConfig,
        #[cfg(feature = "change-planning")]
        Self::PlanResetConsumerOffset,
    ];

    pub fn resolve(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|tool_id| tool_id.descriptor().name == name)
    }

    pub fn descriptor(self) -> ToolDescriptor {
        match self {
            Self::GetClusterOverview => ToolDescriptor::read_only(
                self,
                "rocketmq_get_cluster_overview",
                "RocketMQ cluster overview",
                "Summarize brokers, topic count, and consumer group count for one RocketMQ cluster.",
                RiskLevel::ReadOnly,
            ),
            Self::ListTopics => ToolDescriptor::read_only(
                self,
                "rocketmq_list_topics",
                "RocketMQ topic list",
                "List a bounded page of topics visible from one RocketMQ cluster.",
                RiskLevel::ReadOnly,
            ),
            Self::DescribeTopic => ToolDescriptor::read_only(
                self,
                "rocketmq_describe_topic",
                "RocketMQ topic description",
                "Describe a topic with bounded queue route information.",
                RiskLevel::ReadOnly,
            ),
            Self::GetTopicRoute => ToolDescriptor::read_only(
                self,
                "rocketmq_get_topic_route",
                "RocketMQ topic route",
                "Get bounded topic route data without exposing internal addresses by default.",
                RiskLevel::ReadOnly,
            ),
            Self::ListConsumerGroups => ToolDescriptor::read_only(
                self,
                "rocketmq_list_consumer_groups",
                "RocketMQ consumer groups",
                "List a bounded page of consumer groups and consumption summaries.",
                RiskLevel::ReadOnly,
            ),
            Self::GetConsumerLag => ToolDescriptor::read_only(
                self,
                "rocketmq_get_consumer_lag",
                "RocketMQ consumer lag",
                "Get bounded per-queue lag for a topic and consumer group.",
                RiskLevel::ReadOnly,
            ),
            Self::DescribeBroker => ToolDescriptor::read_only(
                self,
                "rocketmq_describe_broker",
                "RocketMQ broker description",
                "Describe broker state without exposing internal addresses by default.",
                RiskLevel::ReadOnly,
            ),
            Self::DiagnoseConsumerLag => ToolDescriptor::read_only(
                self,
                "rocketmq_diagnose_consumer_lag",
                "RocketMQ consumer lag diagnosis",
                "Diagnose consumer lag from read-only lag, topic route, and broker evidence.",
                RiskLevel::Diagnose,
            ),
            #[cfg(feature = "change-planning")]
            Self::PlanCreateTopic => ToolDescriptor::read_only(
                self,
                "rocketmq_plan_create_topic",
                "RocketMQ create topic plan",
                "Generate a non-mutating topic creation plan.",
                RiskLevel::Plan,
            ),
            #[cfg(feature = "change-planning")]
            Self::PlanUpdateTopicConfig => ToolDescriptor::read_only(
                self,
                "rocketmq_plan_update_topic_config",
                "RocketMQ topic configuration plan",
                "Generate a non-mutating topic configuration update plan.",
                RiskLevel::Plan,
            ),
            #[cfg(feature = "change-planning")]
            Self::PlanUpdateTopicPermissions => ToolDescriptor::read_only(
                self,
                "rocketmq_plan_update_topic_permissions",
                "RocketMQ topic permission plan",
                "Generate a non-mutating topic permission update plan.",
                RiskLevel::Plan,
            ),
            #[cfg(feature = "change-planning")]
            Self::PlanUpdateBrokerConfig => ToolDescriptor::read_only(
                self,
                "rocketmq_plan_update_broker_config",
                "RocketMQ broker configuration plan",
                "Generate a non-mutating broker configuration update plan.",
                RiskLevel::Plan,
            ),
            #[cfg(feature = "change-planning")]
            Self::PlanResetConsumerOffset => ToolDescriptor::read_only(
                self,
                "rocketmq_plan_reset_consumer_offset",
                "RocketMQ consumer offset reset plan",
                "Generate a non-mutating consumer offset reset plan.",
                RiskLevel::Plan,
            ),
        }
    }

    pub fn definition(self) -> Tool {
        let descriptor = self.descriptor();
        match self {
            Self::GetClusterOverview => {
                descriptor.build::<cluster_tools::ClusterOverviewArgs, cluster_tools::ClusterOverviewOutput>()
            }
            Self::ListTopics => descriptor.build::<topic_tools::ListTopicsArgs, topic_tools::ListTopicsOutput>(),
            Self::DescribeTopic => {
                descriptor.build::<topic_tools::DescribeTopicArgs, topic_tools::DescribeTopicOutput>()
            }
            Self::GetTopicRoute => {
                descriptor.build::<topic_tools::QueryTopicRouteArgs, topic_tools::QueryTopicRouteOutput>()
            }
            Self::ListConsumerGroups => {
                descriptor.build::<consumer_tools::ListConsumerGroupsArgs, consumer_tools::ListConsumerGroupsOutput>()
            }
            Self::GetConsumerLag => {
                descriptor.build::<consumer_tools::QueryConsumerLagArgs, consumer_tools::QueryConsumerLagOutput>()
            }
            Self::DescribeBroker => {
                descriptor.build::<broker_tools::DescribeBrokerArgs, broker_tools::DescribeBrokerOutput>()
            }
            Self::DiagnoseConsumerLag => {
                descriptor.build::<diagnosis_tools::DiagnoseConsumerLagArgs, crate::model::diagnosis::DiagnosisReport>()
            }
            #[cfg(feature = "change-planning")]
            Self::PlanCreateTopic => descriptor.build::<change_tools::CreateTopicArgs, change_tools::ChangePlan>(),
            #[cfg(feature = "change-planning")]
            Self::PlanUpdateTopicConfig => {
                descriptor.build::<change_tools::UpdateTopicConfigArgs, change_tools::ChangePlan>()
            }
            #[cfg(feature = "change-planning")]
            Self::PlanUpdateTopicPermissions => {
                descriptor.build::<change_tools::UpdateTopicPermArgs, change_tools::ChangePlan>()
            }
            #[cfg(feature = "change-planning")]
            Self::PlanUpdateBrokerConfig => {
                descriptor.build::<change_tools::UpdateBrokerConfigArgs, change_tools::ChangePlan>()
            }
            #[cfg(feature = "change-planning")]
            Self::PlanResetConsumerOffset => {
                descriptor.build::<change_tools::ResetConsumerOffsetArgs, change_tools::ChangePlan>()
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ToolDescriptor {
    pub id: ToolId,
    pub name: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    pub risk_level: RiskLevel,
    pub annotations: ToolAnnotationsPolicy,
}

impl ToolDescriptor {
    const fn read_only(
        id: ToolId,
        name: &'static str,
        title: &'static str,
        description: &'static str,
        risk_level: RiskLevel,
    ) -> Self {
        Self {
            id,
            name,
            title,
            description,
            risk_level,
            annotations: ToolAnnotationsPolicy {
                read_only: true,
                destructive: false,
                idempotent: true,
                open_world: true,
            },
        }
    }

    fn build<I, O>(self) -> Tool
    where
        I: JsonSchema + 'static,
        O: JsonSchema + 'static,
    {
        Tool::new(self.name, self.description, std::sync::Arc::new(Default::default()))
            .with_title(self.title)
            .with_input_schema::<I>()
            .with_output_schema::<ToolResponse<O>>()
            .with_annotations(
                ToolAnnotations::with_title(self.title)
                    .read_only(self.annotations.read_only)
                    .destructive(self.annotations.destructive)
                    .idempotent(self.annotations.idempotent)
                    .open_world(self.annotations.open_world),
            )
            .with_execution(ToolExecution::new().with_task_support(TaskSupport::Forbidden))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ToolAnnotationsPolicy {
    pub read_only: bool,
    pub destructive: bool,
    pub idempotent: bool,
    pub open_world: bool,
}

pub fn list_tools() -> ListToolsResult {
    ListToolsResult::with_all_items(ToolId::ALL.iter().map(|tool_id| tool_id.definition()).collect())
}

pub fn list_tools_for(mut allows: impl FnMut(&ToolDescriptor) -> bool) -> ListToolsResult {
    ListToolsResult::with_all_items(
        ToolId::ALL
            .iter()
            .filter(|tool_id| allows(&tool_id.descriptor()))
            .map(|tool_id| tool_id.definition())
            .collect(),
    )
}

pub fn get_tool(name: &str) -> Option<Tool> {
    ToolId::resolve(name).map(ToolId::definition)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_is_the_single_source_for_discovery_and_risk() {
        let definitions = list_tools().tools;
        assert_eq!(definitions.len(), ToolId::ALL.len());
        for tool_id in ToolId::ALL {
            let descriptor = tool_id.descriptor();
            let tool = get_tool(descriptor.name).expect("catalog tool");
            assert_eq!(tool.name, descriptor.name);
            assert!(tool.output_schema.is_some());
            assert!(matches!(
                descriptor.risk_level,
                RiskLevel::ReadOnly | RiskLevel::Diagnose | RiskLevel::Plan
            ));
        }
    }

    #[test]
    fn default_catalog_contains_only_frozen_query_and_diagnosis_names() {
        let names = ToolId::ALL
            .iter()
            .map(|tool_id| tool_id.descriptor().name)
            .collect::<Vec<_>>();
        assert_eq!(
            &names[..8],
            &[
                "rocketmq_get_cluster_overview",
                "rocketmq_list_topics",
                "rocketmq_describe_topic",
                "rocketmq_get_topic_route",
                "rocketmq_list_consumer_groups",
                "rocketmq_get_consumer_lag",
                "rocketmq_describe_broker",
                "rocketmq_diagnose_consumer_lag",
            ]
        );
        #[cfg(not(feature = "change-planning"))]
        assert_eq!(names.len(), 8);
    }

    #[test]
    fn complete_tool_contract_snapshot() {
        let contracts = ToolId::ALL
            .iter()
            .map(|tool_id| serde_json::to_value(tool_id.definition()).expect("tool contract serializes"))
            .collect::<Vec<_>>();

        #[cfg(not(feature = "change-planning"))]
        insta::assert_json_snapshot!("tool_contract_schema_metadata", contracts);

        #[cfg(feature = "change-planning")]
        insta::assert_json_snapshot!("tool_contract_schema_metadata_with_change_planning", contracts);
    }
}

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
use rmcp::model::Tool;
use rmcp::model::ToolAnnotations;
use schemars::JsonSchema;

use crate::guard::RiskLevel;
use crate::model::contract::ToolResponse;
use crate::tools::broker_tools;
#[cfg(feature = "change-planning")]
use crate::tools::change_tools;
use crate::tools::cluster_tools;
use crate::tools::config_tools;
use crate::tools::connection_tools;
use crate::tools::consumer_tools;
use crate::tools::diagnosis_tools;
use crate::tools::message_tools;
use crate::tools::proxy_tools;
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
    GetBrokerDiagnostics,
    GetBrokerConfigSummary,
    GetBrokerLogFilterState,
    GetProxyDrainState,
    DiagnoseConsumerLag,
    ListConsumerConnections,
    ListProducerConnections,
    GetMessageMetadata,
    GetTopicConfigState,
    GetConsumerGroupConfigState,
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
        Self::GetBrokerDiagnostics,
        Self::GetBrokerConfigSummary,
        Self::GetBrokerLogFilterState,
        Self::GetProxyDrainState,
        Self::DiagnoseConsumerLag,
        Self::ListConsumerConnections,
        Self::ListProducerConnections,
        Self::GetMessageMetadata,
        Self::GetTopicConfigState,
        Self::GetConsumerGroupConfigState,
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
            Self::GetBrokerDiagnostics => ToolDescriptor::read_only(
                self,
                "rocketmq_get_broker_diagnostics",
                "RocketMQ broker diagnostics",
                "Get bounded readiness, store, recovery, and security diagnostics for one logical Broker.",
                RiskLevel::Diagnose,
            ),
            Self::GetBrokerConfigSummary => ToolDescriptor::read_only(
                self,
                "rocketmq_get_broker_config_summary",
                "RocketMQ broker configuration summary",
                "Get the fixed allowlisted configuration summary for one logical Broker.",
                RiskLevel::ReadOnly,
            ),
            Self::GetBrokerLogFilterState => ToolDescriptor::read_only(
                self,
                "rocketmq_get_broker_log_filter_state",
                "RocketMQ broker log-filter state",
                "Get the temporary state of one allowlisted rocketmq_broker logger target.",
                RiskLevel::Diagnose,
            ),
            Self::GetProxyDrainState => ToolDescriptor::read_only(
                self,
                "rocketmq_get_proxy_drain_state",
                "RocketMQ Proxy drain state",
                "Get bounded drain progress for one configured logical Proxy alias.",
                RiskLevel::Diagnose,
            ),
            Self::DiagnoseConsumerLag => ToolDescriptor::read_only(
                self,
                "rocketmq_diagnose_consumer_lag",
                "RocketMQ consumer lag diagnosis",
                "Diagnose consumer lag from read-only lag, topic route, and broker evidence.",
                RiskLevel::Diagnose,
            ),
            Self::ListConsumerConnections => ToolDescriptor::read_only(
                self,
                "rocketmq_list_consumer_connections",
                "RocketMQ consumer connections",
                "List a bounded page of pseudonymous consumer connections for one exact group.",
                RiskLevel::ReadOnly,
            ),
            Self::ListProducerConnections => ToolDescriptor::read_only(
                self,
                "rocketmq_list_producer_connections",
                "RocketMQ producer connections",
                "List a bounded page of pseudonymous producer connections for one exact Topic and Producer group.",
                RiskLevel::ReadOnly,
            ),
            Self::GetMessageMetadata => ToolDescriptor::read_only(
                self,
                "rocketmq_get_message_metadata",
                "RocketMQ message metadata",
                "Get fixed body-free metadata for one message as process-lifetime aliases.",
                RiskLevel::ReadOnly,
            ),
            Self::GetTopicConfigState => ToolDescriptor::read_only(
                self,
                "rocketmq_get_topic_config_state",
                "RocketMQ Topic configuration state",
                "Get version-CAS observations for one Topic at bounded logical Brokers.",
                RiskLevel::ReadOnly,
            ),
            Self::GetConsumerGroupConfigState => ToolDescriptor::read_only(
                self,
                "rocketmq_get_consumer_group_config_state",
                "RocketMQ consumer group configuration state",
                "Get version-CAS observations for one Consumer Group at bounded logical Brokers.",
                RiskLevel::ReadOnly,
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
            Self::GetBrokerDiagnostics => {
                descriptor.build::<broker_tools::BrokerDiagnosticsArgs, broker_tools::BrokerDiagnosticsOutput>()
            }
            Self::GetBrokerConfigSummary => {
                descriptor.build::<config_tools::BrokerConfigSummaryArgs, config_tools::BrokerConfigSummaryOutput>()
            }
            Self::GetBrokerLogFilterState => {
                descriptor.build::<config_tools::BrokerLogFilterStateArgs, config_tools::BrokerLogFilterStateOutput>()
            }
            Self::GetProxyDrainState => {
                descriptor.build::<proxy_tools::ProxyDrainStateArgs, proxy_tools::ProxyDrainStateOutput>()
            }
            Self::DiagnoseConsumerLag => {
                descriptor.build::<diagnosis_tools::DiagnoseConsumerLagArgs, crate::model::diagnosis::DiagnosisReport>()
            }
            Self::ListConsumerConnections => descriptor.build::<
                connection_tools::ListConsumerConnectionsArgs,
                connection_tools::ListConsumerConnectionsOutput,
            >(),
            Self::ListProducerConnections => descriptor.build::<
                connection_tools::ListProducerConnectionsArgs,
                connection_tools::ListProducerConnectionsOutput,
            >(),
            Self::GetMessageMetadata => {
                descriptor.build::<message_tools::MessageMetadataArgs, message_tools::MessageMetadataOutput>()
            }
            Self::GetTopicConfigState => {
                descriptor.build::<config_tools::TopicConfigStateArgs, config_tools::TopicConfigStateOutput>()
            }
            Self::GetConsumerGroupConfigState => descriptor.build::<
                config_tools::ConsumerGroupConfigStateArgs,
                config_tools::ConsumerGroupConfigStateOutput,
            >(),
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
            let wire = serde_json::to_value(&tool).expect("tool serializes");
            assert!(
                wire.get("execution").is_none(),
                "rmcp 3.1 tools must not expose a task-capable execution surface"
            );
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
            &names[..12],
            &[
                "rocketmq_get_cluster_overview",
                "rocketmq_list_topics",
                "rocketmq_describe_topic",
                "rocketmq_get_topic_route",
                "rocketmq_list_consumer_groups",
                "rocketmq_get_consumer_lag",
                "rocketmq_describe_broker",
                "rocketmq_get_broker_diagnostics",
                "rocketmq_get_broker_config_summary",
                "rocketmq_get_broker_log_filter_state",
                "rocketmq_get_proxy_drain_state",
                "rocketmq_diagnose_consumer_lag",
            ]
        );
        #[cfg(not(feature = "change-planning"))]
        assert_eq!(names.len(), 17);
        #[cfg(feature = "change-planning")]
        assert_eq!(names.len(), 22);
    }

    #[test]
    fn connection_message_and_config_state_contracts_are_read_only_and_closed() {
        let expected = [
            ToolId::ListConsumerConnections,
            ToolId::ListProducerConnections,
            ToolId::GetMessageMetadata,
            ToolId::GetTopicConfigState,
            ToolId::GetConsumerGroupConfigState,
        ];
        for tool_id in expected {
            let descriptor = tool_id.descriptor();
            assert_eq!(descriptor.risk_level, RiskLevel::ReadOnly);
            assert!(descriptor.annotations.read_only);
            assert!(!descriptor.annotations.destructive);
            assert!(descriptor.annotations.idempotent);
            assert_eq!(
                tool_id.definition().input_schema.get("additionalProperties"),
                Some(&serde_json::Value::Bool(false))
            );
        }
    }

    #[test]
    fn broker_and_proxy_contracts_have_exact_risk_and_read_only_annotations() {
        let expected = [
            (ToolId::GetBrokerDiagnostics, RiskLevel::Diagnose),
            (ToolId::GetBrokerConfigSummary, RiskLevel::ReadOnly),
            (ToolId::GetBrokerLogFilterState, RiskLevel::Diagnose),
            (ToolId::GetProxyDrainState, RiskLevel::Diagnose),
        ];
        for (tool_id, risk) in expected {
            let descriptor = tool_id.descriptor();
            assert_eq!(descriptor.risk_level, risk);
            assert!(descriptor.annotations.read_only);
            assert!(!descriptor.annotations.destructive);
            assert!(descriptor.annotations.idempotent);
            let definition = tool_id.definition();
            assert!(definition.output_schema.is_some());
            assert_eq!(
                definition.input_schema.get("additionalProperties"),
                Some(&serde_json::Value::Bool(false)),
                "{} must reject unknown arguments",
                descriptor.name
            );
        }
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

    #[cfg(feature = "change-planning")]
    #[test]
    fn change_planning_catalog_is_read_only_and_uses_only_canonical_names() {
        let planning = ToolId::ALL
            .iter()
            .map(|tool_id| tool_id.descriptor())
            .filter(|descriptor| descriptor.risk_level == RiskLevel::Plan)
            .collect::<Vec<_>>();

        assert_eq!(
            planning.iter().map(|descriptor| descriptor.name).collect::<Vec<_>>(),
            vec![
                "rocketmq_plan_create_topic",
                "rocketmq_plan_update_topic_config",
                "rocketmq_plan_update_topic_permissions",
                "rocketmq_plan_update_broker_config",
                "rocketmq_plan_reset_consumer_offset",
            ]
        );
        assert!(planning.iter().all(|descriptor| {
            descriptor.annotations.read_only
                && !descriptor.annotations.destructive
                && descriptor.annotations.idempotent
                && !descriptor.name.starts_with("mq_")
        }));
    }
}

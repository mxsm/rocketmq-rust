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

use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;

use rmcp::model::JsonObject;
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
use crate::tools::infrastructure_tools;
use crate::tools::message_tools;
use crate::tools::proxy_tools;
use crate::tools::topic_tools;

/// How a Tool binds a request to one configured logical cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterArg {
    /// `cluster` must name a configured logical cluster.
    Required,
    /// `cluster` may be omitted; the configured default cluster is then authorized and queried.
    OptionalDefault,
}

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
    GetTopicStats,
    GetTopicConfig,
    GetConsumerGroupDetails,
    GetConsumerProgress,
    GetHaStatus,
    GetControllerMetadata,
    GetNameserverConfigSummary,
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
        Self::GetTopicStats,
        Self::GetTopicConfig,
        Self::GetConsumerGroupDetails,
        Self::GetConsumerProgress,
        Self::GetHaStatus,
        Self::GetControllerMetadata,
        Self::GetNameserverConfigSummary,
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

    /// Returns how this Tool binds a request to one logical cluster.
    ///
    /// The Guard resolves the effective cluster from this shape before authorization, so it
    /// must match the input schema: `OptionalDefault` exactly when `cluster` is not required.
    pub const fn cluster_arg(self) -> ClusterArg {
        match self {
            Self::ListTopics | Self::ListConsumerGroups => ClusterArg::OptionalDefault,
            Self::GetClusterOverview
            | Self::DescribeTopic
            | Self::GetTopicRoute
            | Self::GetConsumerLag
            | Self::DescribeBroker
            | Self::GetBrokerDiagnostics
            | Self::GetBrokerConfigSummary
            | Self::GetBrokerLogFilterState
            | Self::GetProxyDrainState
            | Self::DiagnoseConsumerLag
            | Self::ListConsumerConnections
            | Self::ListProducerConnections
            | Self::GetMessageMetadata
            | Self::GetTopicConfigState
            | Self::GetConsumerGroupConfigState
            | Self::GetTopicStats
            | Self::GetTopicConfig
            | Self::GetConsumerGroupDetails
            | Self::GetConsumerProgress
            | Self::GetHaStatus
            | Self::GetControllerMetadata
            | Self::GetNameserverConfigSummary => ClusterArg::Required,
            #[cfg(feature = "change-planning")]
            Self::PlanCreateTopic
            | Self::PlanUpdateTopicConfig
            | Self::PlanUpdateTopicPermissions
            | Self::PlanUpdateBrokerConfig
            | Self::PlanResetConsumerOffset => ClusterArg::Required,
        }
    }

    pub fn descriptor(self) -> ToolDescriptor {
        match self {
            Self::GetClusterOverview => ToolDescriptor::read_only(
                self,
                "rocketmq_get_cluster_overview",
                "RocketMQ cluster overview",
                "Summarizes one cluster: its Brokers with their state, plus the number of Topics and Consumer \
                 Groups. Use it first, to learn the Broker names and the size of a cluster. To see the names behind \
                 the counts, call rocketmq_list_topics or rocketmq_list_consumer_groups.",
                RiskLevel::ReadOnly,
            ),
            Self::ListTopics => ToolDescriptor::read_only(
                self,
                "rocketmq_list_topics",
                "RocketMQ topic list",
                "Lists the Topic names of one cluster, a page at a time, optionally keeping only names that contain \
                 a text. Use it to find the exact name of a Topic. A cluster with more than 10,000 Topics needs a \
                 filter. Next, pass a name to rocketmq_get_topic_route or rocketmq_get_topic_stats.",
                RiskLevel::ReadOnly,
            ),
            Self::DescribeTopic => ToolDescriptor::read_only(
                self,
                "rocketmq_describe_topic",
                "RocketMQ topic description",
                "Returns where one Topic is hosted: the Brokers that serve it and each Broker's queue counts. It \
                 returns the same route data as rocketmq_get_topic_route plus a list of Broker names; prefer \
                 rocketmq_get_topic_route in new integrations.",
                RiskLevel::ReadOnly,
            ),
            Self::GetTopicRoute => ToolDescriptor::read_only(
                self,
                "rocketmq_get_topic_route",
                "RocketMQ topic route",
                "Returns where one Topic is hosted: the Brokers that serve it and each Broker's read and write queue \
                 counts and permission. Use it to check that a Topic exists and how it is spread over Brokers. For \
                 message counts and offsets use rocketmq_get_topic_stats; for configuration differences use \
                 rocketmq_get_topic_config.",
                RiskLevel::ReadOnly,
            ),
            Self::ListConsumerGroups => ToolDescriptor::read_only(
                self,
                "rocketmq_list_consumer_groups",
                "RocketMQ consumer groups",
                "Lists the Consumer Groups of one cluster, a page at a time, with client count, consume type, TPS \
                 and total lag for each. Use it to find the exact name of a group or to spot the groups that are \
                 behind. Next, call rocketmq_get_consumer_lag or rocketmq_diagnose_consumer_lag for one group.",
                RiskLevel::ReadOnly,
            ),
            Self::GetConsumerLag => ToolDescriptor::read_only(
                self,
                "rocketmq_get_consumer_lag",
                "RocketMQ consumer lag",
                "Returns the lag of one Consumer Group on one Topic: total lag, the largest queue lag, consume TPS, \
                 and a page of per-queue offsets. Use it when you know both the Topic and the group. For an \
                 explanation of the lag use rocketmq_diagnose_consumer_lag; for every Topic the group consumes use \
                 rocketmq_get_consumer_progress.",
                RiskLevel::ReadOnly,
            ),
            Self::DescribeBroker => ToolDescriptor::read_only(
                self,
                "rocketmq_describe_broker",
                "RocketMQ broker description",
                "Returns the state of one Broker: its instances with version, inbound and outbound TPS, and whether \
                 each is active. Use it for a first look at a Broker named by rocketmq_get_cluster_overview. For \
                 readiness, store and recovery detail use rocketmq_get_broker_diagnostics.",
                RiskLevel::ReadOnly,
            ),
            Self::GetBrokerDiagnostics => ToolDescriptor::read_only(
                self,
                "rocketmq_get_broker_diagnostics",
                "RocketMQ broker diagnostics",
                "Returns readiness, store, recovery, HA and security diagnostics for one Broker. Use it when a \
                 Broker looks unhealthy, or after rocketmq_diagnose_consumer_lag points at it. For the plain state \
                 use rocketmq_describe_broker; for replication between master and slaves use rocketmq_get_ha_status.",
                RiskLevel::Diagnose,
            ),
            Self::GetBrokerConfigSummary => ToolDescriptor::read_only(
                self,
                "rocketmq_get_broker_config_summary",
                "RocketMQ broker configuration summary",
                "Returns the allowlisted configuration values of one Broker. Use it to check how a Broker is \
                 configured. Keys outside the fixed allowlist are not returned.",
                RiskLevel::ReadOnly,
            ),
            Self::GetBrokerLogFilterState => ToolDescriptor::read_only(
                self,
                "rocketmq_get_broker_log_filter_state",
                "RocketMQ broker log-filter state",
                "Returns the temporary log-filter state of one logger on one Broker. Use it to check whether a \
                 raised log level is still in effect for a rocketmq_broker:: module. The logger argument is a Rust \
                 module path.",
                RiskLevel::Diagnose,
            ),
            Self::GetProxyDrainState => ToolDescriptor::read_only(
                self,
                "rocketmq_get_proxy_drain_state",
                "RocketMQ Proxy drain state",
                "Returns the drain progress of one Proxy: its phase, whether admission and routing are still open, \
                 whether readiness is published, and how much work is pending. Use it while a Proxy is being drained \
                 for maintenance. The Proxy is named by its alias in the server configuration.",
                RiskLevel::Diagnose,
            ),
            Self::DiagnoseConsumerLag => ToolDescriptor::read_only(
                self,
                "rocketmq_diagnose_consumer_lag",
                "RocketMQ consumer lag diagnosis",
                "Explains the lag of one Consumer Group on one Topic. It reads the lag of every queue, the Topic \
                 route and the Broker that holds the most lag, then reports a severity, likely causes and \
                 recommendations. Use it when a group is behind and you need a cause. For the raw per-queue numbers \
                 use rocketmq_get_consumer_lag.",
                RiskLevel::Diagnose,
            ),
            Self::ListConsumerConnections => ToolDescriptor::read_only(
                self,
                "rocketmq_list_consumer_connections",
                "RocketMQ consumer connections",
                "Lists the clients connected for one Consumer Group, a page at a time: Broker, client pseudonym, \
                 language and version. Use it to check whether consumers are online and which versions they run. \
                 Client identifiers and addresses are replaced by pseudonyms.",
                RiskLevel::ReadOnly,
            ),
            Self::ListProducerConnections => ToolDescriptor::read_only(
                self,
                "rocketmq_list_producer_connections",
                "RocketMQ producer connections",
                "Lists the clients connected for one Producer Group on one Topic, a page at a time: Broker, client \
                 pseudonym, language and version. Use it to check whether producers are online. Client identifiers \
                 and addresses are replaced by pseudonyms.",
                RiskLevel::ReadOnly,
            ),
            Self::GetMessageMetadata => ToolDescriptor::read_only(
                self,
                "rocketmq_get_message_metadata",
                "RocketMQ message metadata",
                "Returns the metadata of one message by its identifier: Topic, queue and offset, size, born and \
                 stored times, and reconsume count. Use it to confirm that a message was stored, and where. The body \
                 is never returned and message identifiers are replaced by pseudonyms.",
                RiskLevel::ReadOnly,
            ),
            Self::GetTopicConfigState => ToolDescriptor::read_only(
                self,
                "rocketmq_get_topic_config_state",
                "RocketMQ Topic configuration state",
                "Returns one Topic's configuration version and queue settings on each selected Broker. Use it right \
                 before a change, to read the version a compare-and-set update must quote, and afterwards to confirm \
                 the change. To compare every Broker without naming them use rocketmq_get_topic_config.",
                RiskLevel::ReadOnly,
            ),
            Self::GetConsumerGroupConfigState => ToolDescriptor::read_only(
                self,
                "rocketmq_get_consumer_group_config_state",
                "RocketMQ consumer group configuration state",
                "Returns one Consumer Group's configuration version and settings on each selected Broker, such as \
                 retry limits and whether consuming is enabled. Use it right before a change, to read the version a \
                 compare-and-set update must quote, and afterwards to confirm the change. For connections use \
                 rocketmq_get_consumer_group_details.",
                RiskLevel::ReadOnly,
            ),
            Self::GetTopicStats => ToolDescriptor::read_only(
                self,
                "rocketmq_get_topic_stats",
                "RocketMQ Topic statistics",
                "Returns per-queue statistics of one Topic, a page at a time: minimum and maximum offset, message \
                 count and last update time, plus totals over all queues. Use it to see how much data a Topic holds \
                 and whether it is still written to. For where the Topic is hosted use rocketmq_get_topic_route.",
                RiskLevel::ReadOnly,
            ),
            Self::GetTopicConfig => ToolDescriptor::read_only(
                self,
                "rocketmq_get_topic_config",
                "RocketMQ Topic configuration",
                "Returns the configuration of one Topic on every Broker that hosts it: queue counts, permission, \
                 order and message type, and the fields on which the Brokers disagree. Use it to find inconsistent \
                 Topic configuration. For hosting and routing use rocketmq_get_topic_route.",
                RiskLevel::ReadOnly,
            ),
            Self::GetConsumerGroupDetails => ToolDescriptor::read_only(
                self,
                "rocketmq_get_consumer_group_details",
                "RocketMQ consumer group details",
                "Returns how one Consumer Group is set up on each Broker: whether it is configured there, whether \
                 clients are connected, its consume type, and the total connection count. Use it to check that a \
                 group exists and is online. For lag use rocketmq_get_consumer_lag; for the connected clients use \
                 rocketmq_list_consumer_connections.",
                RiskLevel::ReadOnly,
            ),
            Self::GetConsumerProgress => ToolDescriptor::read_only(
                self,
                "rocketmq_get_consumer_progress",
                "RocketMQ consumer progress",
                "Returns the progress of one Consumer Group over every Topic it consumes: totals for lag and \
                 in-flight messages, and a page of per-queue rows. Use it when you know the group but not the Topic, \
                 or need the lag across all of its Topics. For one Topic use rocketmq_get_consumer_lag.",
                RiskLevel::ReadOnly,
            ),
            Self::GetHaStatus => ToolDescriptor::read_only(
                self,
                "rocketmq_get_ha_status",
                "RocketMQ HA status",
                "Returns the replication state of master Brokers: commit log offset, in-sync slave count and each \
                 slave connection, optionally with the sync state the Controllers hold. Use it to check whether \
                 slaves keep up with their master. For Controller leadership use rocketmq_get_controller_metadata.",
                RiskLevel::Diagnose,
            ),
            Self::GetControllerMetadata => ToolDescriptor::read_only(
                self,
                "rocketmq_get_controller_metadata",
                "RocketMQ Controller metadata",
                "Returns metadata of the configured Controllers: group, leader, peer count and log indexes. Use it \
                 to check which Controller leads and whether the Controller group is healthy. For Broker replication \
                 use rocketmq_get_ha_status.",
                RiskLevel::Diagnose,
            ),
            Self::GetNameserverConfigSummary => ToolDescriptor::read_only(
                self,
                "rocketmq_get_nameserver_config_summary",
                "RocketMQ NameServer configuration summary",
                "Returns the allowlisted configuration values of the cluster's NameServers and the values on which \
                 they differ. Use it to check that the NameServers are configured alike. Keys outside the fixed \
                 allowlist are not returned.",
                RiskLevel::ReadOnly,
            ),
            #[cfg(feature = "change-planning")]
            Self::PlanCreateTopic => ToolDescriptor::read_only(
                self,
                "rocketmq_plan_create_topic",
                "RocketMQ create topic plan",
                "Builds a plan for creating a Topic without changing the cluster. The plan lists the intended \
                 change, its impact and rollback suggestions, and expires after five minutes. Use it to review a \
                 change before an operator applies it by other means.",
                RiskLevel::Plan,
            ),
            #[cfg(feature = "change-planning")]
            Self::PlanUpdateTopicConfig => ToolDescriptor::read_only(
                self,
                "rocketmq_plan_update_topic_config",
                "RocketMQ topic configuration plan",
                "Builds a plan for changing one configuration entry of a Topic without changing the cluster. The \
                 plan records the current Topic state, the intended change, its impact and rollback suggestions, and \
                 expires after five minutes. Use it to review a change before an operator applies it by other means.",
                RiskLevel::Plan,
            ),
            #[cfg(feature = "change-planning")]
            Self::PlanUpdateTopicPermissions => ToolDescriptor::read_only(
                self,
                "rocketmq_plan_update_topic_permissions",
                "RocketMQ topic permission plan",
                "Builds a plan for changing the permission of a Topic without changing the cluster. The plan records \
                 the current Topic state, the intended change, its impact and rollback suggestions, and expires \
                 after five minutes. Use it to review a change before an operator applies it by other means.",
                RiskLevel::Plan,
            ),
            #[cfg(feature = "change-planning")]
            Self::PlanUpdateBrokerConfig => ToolDescriptor::read_only(
                self,
                "rocketmq_plan_update_broker_config",
                "RocketMQ broker configuration plan",
                "Builds a plan for changing one configuration entry of a Broker without changing the cluster. The \
                 plan records the current Broker state, the intended change, its impact and rollback suggestions, \
                 and expires after five minutes. Use it to review a change before an operator applies it by other \
                 means.",
                RiskLevel::Plan,
            ),
            #[cfg(feature = "change-planning")]
            Self::PlanResetConsumerOffset => ToolDescriptor::read_only(
                self,
                "rocketmq_plan_reset_consumer_offset",
                "RocketMQ consumer offset reset plan",
                "Builds a plan for resetting the offsets of one Consumer Group on one Topic without changing the \
                 cluster. The plan records the current lag, the intended reset, its impact and rollback suggestions, \
                 and expires after five minutes. Use it to review a reset before an operator applies it by other \
                 means.",
                RiskLevel::Plan,
            ),
        }
    }

    /// Returns the published definition of this Tool.
    ///
    /// A definition is built on first use and then kept: generating two schemas and unwrapping
    /// their descriptions is too costly to repeat for every call that validates against them.
    pub fn definition(self) -> Tool {
        static DEFINITIONS: LazyLock<Vec<OnceLock<Tool>>> =
            LazyLock::new(|| ToolId::ALL.iter().map(|_| OnceLock::new()).collect());
        let cached = Self::ALL
            .iter()
            .position(|tool_id| *tool_id == self)
            .and_then(|index| DEFINITIONS.get(index));
        match cached {
            Some(definition) => definition.get_or_init(|| self.build_definition()).clone(),
            None => self.build_definition(),
        }
    }

    fn build_definition(self) -> Tool {
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
            Self::GetTopicStats => {
                descriptor.build::<topic_tools::GetTopicStatsArgs, topic_tools::GetTopicStatsOutput>()
            }
            Self::GetTopicConfig => {
                descriptor.build::<config_tools::GetTopicConfigArgs, config_tools::GetTopicConfigOutput>()
            }
            Self::GetConsumerGroupDetails => descriptor.build::<
                consumer_tools::GetConsumerGroupDetailsArgs,
                consumer_tools::GetConsumerGroupDetailsOutput,
            >(),
            Self::GetConsumerProgress => descriptor.build::<
                consumer_tools::GetConsumerProgressArgs,
                consumer_tools::GetConsumerProgressOutput,
            >(),
            Self::GetHaStatus => descriptor.build::<
                infrastructure_tools::GetHaStatusArgs,
                infrastructure_tools::GetHaStatusOutput,
            >(),
            Self::GetControllerMetadata => descriptor.build::<
                infrastructure_tools::GetControllerMetadataArgs,
                infrastructure_tools::GetControllerMetadataOutput,
            >(),
            Self::GetNameserverConfigSummary => descriptor.build::<
                infrastructure_tools::GetNameserverConfigSummaryArgs,
                infrastructure_tools::GetNameserverConfigSummaryOutput,
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
        let mut tool = Tool::new(self.name, self.description, Arc::new(Default::default()))
            .with_title(self.title)
            .with_input_schema::<I>()
            .with_output_schema::<ToolResponse<O>>()
            .with_annotations(
                ToolAnnotations::with_title(self.title)
                    .read_only(self.annotations.read_only)
                    .destructive(self.annotations.destructive)
                    .idempotent(self.annotations.idempotent)
                    .open_world(self.annotations.open_world),
            );
        unwrap_descriptions(Arc::make_mut(&mut tool.input_schema));
        if let Some(output_schema) = &mut tool.output_schema {
            unwrap_descriptions(Arc::make_mut(output_schema));
        }
        tool
    }
}

/// Joins the wrapped lines of every `description` in a schema, keeping paragraph breaks.
///
/// schemars publishes a doc comment with its source line breaks. Left in, re-wrapping a comment
/// would change the published schema and, with it, the Tool surface digest.
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

    /// Collects the properties, at any depth of an input schema, that a model cannot read about.
    fn undescribed_properties(schema: &serde_json::Value, undescribed: &mut Vec<String>) {
        match schema {
            serde_json::Value::Object(keywords) => {
                for (keyword, value) in keywords {
                    if let ("properties", serde_json::Value::Object(properties)) = (keyword.as_str(), value) {
                        undescribed.extend(
                            properties
                                .iter()
                                .filter(|(_, property)| {
                                    property["description"]
                                        .as_str()
                                        .is_none_or(|text| text.trim().is_empty())
                                })
                                .map(|(name, _)| name.clone()),
                        );
                    }
                    undescribed_properties(value, undescribed);
                }
            }
            serde_json::Value::Array(values) => values
                .iter()
                .for_each(|value| undescribed_properties(value, undescribed)),
            _ => {}
        }
    }

    #[test]
    fn every_input_property_has_a_description() {
        for tool_id in ToolId::ALL {
            let name = tool_id.descriptor().name;
            let schema = serde_json::Value::Object(tool_id.definition().input_schema.as_ref().clone());
            let mut undescribed = Vec::new();
            undescribed_properties(&schema, &mut undescribed);
            assert!(undescribed.is_empty(), "{name}: {undescribed:?}");
            // A blank cluster can never select one, and the schema says so.
            assert_eq!(schema["properties"]["cluster"]["minLength"], 1, "{name}");
        }
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

        for tool_id in ToolId::ALL {
            let definition = tool_id.definition();
            assert_unwrapped(&definition.name, &serde_json::to_value(&definition).unwrap());
            // The kept definition is the one a fresh build produces.
            assert_eq!(definition, tool_id.build_definition());
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

    #[test]
    fn tool_descriptions_fit_a_tool_listing() {
        for tool_id in ToolId::ALL {
            let descriptor = tool_id.descriptor();
            assert!(
                (1..=400).contains(&descriptor.description.len()),
                "{}: {} characters",
                descriptor.name,
                descriptor.description.len()
            );
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
        assert_eq!(names.len(), 24);
        #[cfg(feature = "change-planning")]
        assert_eq!(names.len(), 29);
    }

    #[test]
    fn cluster_arg_matches_the_input_schema() {
        for tool_id in ToolId::ALL {
            let definition = tool_id.definition();
            let schema = serde_json::to_value(definition.input_schema.as_ref()).expect("input schema serializes");
            assert!(
                schema["properties"].get("cluster").is_some(),
                "{} must take a cluster argument",
                definition.name
            );
            let cluster_is_required = schema["required"]
                .as_array()
                .is_some_and(|required| required.iter().any(|name| name == "cluster"));
            let expected = if cluster_is_required {
                ClusterArg::Required
            } else {
                ClusterArg::OptionalDefault
            };
            assert_eq!(tool_id.cluster_arg(), expected, "tool={}", definition.name);
        }
    }

    #[test]
    fn connection_message_and_config_state_contracts_are_read_only_and_closed() {
        let expected = [
            ToolId::ListConsumerConnections,
            ToolId::ListProducerConnections,
            ToolId::GetMessageMetadata,
            ToolId::GetTopicConfigState,
            ToolId::GetConsumerGroupConfigState,
            ToolId::GetTopicStats,
            ToolId::GetTopicConfig,
            ToolId::GetConsumerGroupDetails,
            ToolId::GetConsumerProgress,
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
    fn infrastructure_contracts_have_exact_risk_and_no_target_addresses() {
        for (tool_id, risk) in [
            (ToolId::GetHaStatus, RiskLevel::Diagnose),
            (ToolId::GetControllerMetadata, RiskLevel::Diagnose),
            (ToolId::GetNameserverConfigSummary, RiskLevel::ReadOnly),
        ] {
            let descriptor = tool_id.descriptor();
            assert_eq!(descriptor.risk_level, risk);
            assert!(descriptor.annotations.read_only);
            assert!(!descriptor.annotations.destructive);
            let definition = tool_id.definition();
            assert_eq!(
                definition.input_schema.get("additionalProperties"),
                Some(&serde_json::Value::Bool(false))
            );
            let properties = definition.input_schema["properties"].as_object().unwrap();
            for forbidden in ["address", "addr", "endpoint"] {
                assert!(properties
                    .keys()
                    .all(|key| !key.to_ascii_lowercase().contains(forbidden)));
            }
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

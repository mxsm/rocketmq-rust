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
use serde::Deserialize;
use serde::Serialize;

/// Closed set of Phase 3 actions that may enter supervised planning.
///
/// Execution remains gated by the exact descriptor's `plan_only` and
/// `execution_supported` flags. Permanently destructive R3 and unknown actions
/// are intentionally not representable.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ExecutionAction {
    #[serde(rename = "observability.logger_level_ttl.v1")]
    ObservabilityLoggerLevelTtl,
    #[serde(rename = "proxy.scale_out_one.v1")]
    ProxyScaleOutOne,
    #[serde(rename = "proxy.restart_one.v1")]
    ProxyRestartOne,
    #[serde(rename = "broker.config.patch_allowlisted.v1")]
    BrokerConfigPatchAllowlisted,
    #[serde(rename = "topic.config.patch_allowlisted.v1")]
    TopicConfigPatchAllowlisted,
    #[serde(rename = "subscription_group.patch_allowlisted.v1")]
    SubscriptionGroupPatchAllowlisted,
    #[serde(rename = "consumer.request_mode.patch_allowlisted.v1")]
    ConsumerRequestModePatchAllowlisted,
    #[serde(rename = "consumer.offset.reset_bounded.v1")]
    ConsumerOffsetResetBounded,
    #[serde(rename = "topic.queue.expand_only.v1")]
    TopicQueueExpandOnly,
    #[serde(rename = "namesrv.config.patch_allowlisted.v1")]
    NameSrvConfigPatchAllowlisted,
    #[serde(rename = "controller.config.patch_allowlisted.v1")]
    ControllerConfigPatchAllowlisted,
    #[serde(rename = "proxy.rollout_image_canary.v1")]
    ProxyRolloutImageCanary,
    #[serde(rename = "broker.restart_one.v1")]
    BrokerRestartOne,
    #[serde(rename = "static_topic.patch_non_remap.v1")]
    StaticTopicPatchNonRemap,
    #[serde(rename = "tiered.cold_data_flow.patch_allowlisted.v1")]
    TieredColdDataFlowPatchAllowlisted,
    #[serde(rename = "store.readahead.patch_allowlisted.v1")]
    StoreReadaheadPatchAllowlisted,
    #[serde(rename = "security.credential_rotate_overlap.v1")]
    SecurityCredentialRotateOverlap,
    #[serde(rename = "telemetry.collector.restart_one.v1")]
    TelemetryCollectorRestartOne,
    #[serde(rename = "consumer.offset.clone_or_reset_broad.v1")]
    ConsumerOffsetCloneOrResetBroad,
    #[serde(rename = "message.direct_consume.v1")]
    MessageDirectConsume,
    #[serde(rename = "message.dlq.resend.v1")]
    MessageDlqResend,
    #[serde(rename = "timer.switch.v1")]
    TimerSwitch,
    #[serde(rename = "controller.elect.v1")]
    ControllerElect,
    #[serde(rename = "static_topic.remap.v1")]
    StaticTopicRemap,
    #[serde(rename = "broker.container.add_remove.v1")]
    BrokerContainerAddRemove,
}

impl ExecutionAction {
    /// Every Phase 3 supervised planning action in stable catalog order.
    pub const ALL: [Self; 25] = [
        Self::ObservabilityLoggerLevelTtl,
        Self::ProxyScaleOutOne,
        Self::ProxyRestartOne,
        Self::BrokerConfigPatchAllowlisted,
        Self::TopicConfigPatchAllowlisted,
        Self::SubscriptionGroupPatchAllowlisted,
        Self::ConsumerRequestModePatchAllowlisted,
        Self::ConsumerOffsetResetBounded,
        Self::TopicQueueExpandOnly,
        Self::NameSrvConfigPatchAllowlisted,
        Self::ControllerConfigPatchAllowlisted,
        Self::ProxyRolloutImageCanary,
        Self::BrokerRestartOne,
        Self::StaticTopicPatchNonRemap,
        Self::TieredColdDataFlowPatchAllowlisted,
        Self::StoreReadaheadPatchAllowlisted,
        Self::SecurityCredentialRotateOverlap,
        Self::TelemetryCollectorRestartOne,
        Self::ConsumerOffsetCloneOrResetBroad,
        Self::MessageDirectConsume,
        Self::MessageDlqResend,
        Self::TimerSwitch,
        Self::ControllerElect,
        Self::StaticTopicRemap,
        Self::BrokerContainerAddRemove,
    ];

    /// Wave 3 actions that are deliberately planning-only in Phase 3.
    pub const WAVE3_PLAN_ONLY: [Self; 7] = [
        Self::ConsumerOffsetCloneOrResetBroad,
        Self::MessageDirectConsume,
        Self::MessageDlqResend,
        Self::TimerSwitch,
        Self::ControllerElect,
        Self::StaticTopicRemap,
        Self::BrokerContainerAddRemove,
    ];

    /// Returns the exact Action Catalog identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::ObservabilityLoggerLevelTtl => "observability.logger_level_ttl.v1",
            Self::ProxyScaleOutOne => "proxy.scale_out_one.v1",
            Self::ProxyRestartOne => "proxy.restart_one.v1",
            Self::BrokerConfigPatchAllowlisted => "broker.config.patch_allowlisted.v1",
            Self::TopicConfigPatchAllowlisted => "topic.config.patch_allowlisted.v1",
            Self::SubscriptionGroupPatchAllowlisted => "subscription_group.patch_allowlisted.v1",
            Self::ConsumerRequestModePatchAllowlisted => "consumer.request_mode.patch_allowlisted.v1",
            Self::ConsumerOffsetResetBounded => "consumer.offset.reset_bounded.v1",
            Self::TopicQueueExpandOnly => "topic.queue.expand_only.v1",
            Self::NameSrvConfigPatchAllowlisted => "namesrv.config.patch_allowlisted.v1",
            Self::ControllerConfigPatchAllowlisted => "controller.config.patch_allowlisted.v1",
            Self::ProxyRolloutImageCanary => "proxy.rollout_image_canary.v1",
            Self::BrokerRestartOne => "broker.restart_one.v1",
            Self::StaticTopicPatchNonRemap => "static_topic.patch_non_remap.v1",
            Self::TieredColdDataFlowPatchAllowlisted => "tiered.cold_data_flow.patch_allowlisted.v1",
            Self::StoreReadaheadPatchAllowlisted => "store.readahead.patch_allowlisted.v1",
            Self::SecurityCredentialRotateOverlap => "security.credential_rotate_overlap.v1",
            Self::TelemetryCollectorRestartOne => "telemetry.collector.restart_one.v1",
            Self::ConsumerOffsetCloneOrResetBroad => "consumer.offset.clone_or_reset_broad.v1",
            Self::MessageDirectConsume => "message.direct_consume.v1",
            Self::MessageDlqResend => "message.dlq.resend.v1",
            Self::TimerSwitch => "timer.switch.v1",
            Self::ControllerElect => "controller.elect.v1",
            Self::StaticTopicRemap => "static_topic.remap.v1",
            Self::BrokerContainerAddRemove => "broker.container.add_remove.v1",
        }
    }

    /// Resolves an exact catalog identifier without accepting aliases.
    #[must_use]
    pub fn from_id(value: &str) -> Option<Self> {
        match value {
            "observability.logger_level_ttl.v1" => Some(Self::ObservabilityLoggerLevelTtl),
            "proxy.scale_out_one.v1" => Some(Self::ProxyScaleOutOne),
            "proxy.restart_one.v1" => Some(Self::ProxyRestartOne),
            "broker.config.patch_allowlisted.v1" => Some(Self::BrokerConfigPatchAllowlisted),
            "topic.config.patch_allowlisted.v1" => Some(Self::TopicConfigPatchAllowlisted),
            "subscription_group.patch_allowlisted.v1" => Some(Self::SubscriptionGroupPatchAllowlisted),
            "consumer.request_mode.patch_allowlisted.v1" => Some(Self::ConsumerRequestModePatchAllowlisted),
            "consumer.offset.reset_bounded.v1" => Some(Self::ConsumerOffsetResetBounded),
            "topic.queue.expand_only.v1" => Some(Self::TopicQueueExpandOnly),
            "namesrv.config.patch_allowlisted.v1" => Some(Self::NameSrvConfigPatchAllowlisted),
            "controller.config.patch_allowlisted.v1" => Some(Self::ControllerConfigPatchAllowlisted),
            "proxy.rollout_image_canary.v1" => Some(Self::ProxyRolloutImageCanary),
            "broker.restart_one.v1" => Some(Self::BrokerRestartOne),
            "static_topic.patch_non_remap.v1" => Some(Self::StaticTopicPatchNonRemap),
            "tiered.cold_data_flow.patch_allowlisted.v1" => Some(Self::TieredColdDataFlowPatchAllowlisted),
            "store.readahead.patch_allowlisted.v1" => Some(Self::StoreReadaheadPatchAllowlisted),
            "security.credential_rotate_overlap.v1" => Some(Self::SecurityCredentialRotateOverlap),
            "telemetry.collector.restart_one.v1" => Some(Self::TelemetryCollectorRestartOne),
            "consumer.offset.clone_or_reset_broad.v1" => Some(Self::ConsumerOffsetCloneOrResetBroad),
            "message.direct_consume.v1" => Some(Self::MessageDirectConsume),
            "message.dlq.resend.v1" => Some(Self::MessageDlqResend),
            "timer.switch.v1" => Some(Self::TimerSwitch),
            "controller.elect.v1" => Some(Self::ControllerElect),
            "static_topic.remap.v1" => Some(Self::StaticTopicRemap),
            "broker.container.add_remove.v1" => Some(Self::BrokerContainerAddRemove),
            _ => None,
        }
    }
}

/// Maximum descriptor-authorized blast radius.
#[derive(Clone, Copy, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImpactScope {
    #[default]
    SingleResource,
    SingleInstance,
    OneReplica,
    AllowlistedFields,
}

/// Deterministic compensation mode.
#[derive(Clone, Copy, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompensationMode {
    #[default]
    Automatic,
    ManualTakeover,
    NotAvailable,
}

/// Verification policy frozen by an action descriptor.
#[derive(Clone, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationSpec {
    #[serde(default)]
    pub resource_conditions: Vec<String>,
    #[serde(default)]
    pub technical_slis: Vec<String>,
    pub stable_window_seconds: u64,
    pub max_wait_seconds: u64,
}

/// Compensation behavior frozen by an action descriptor.
#[derive(Clone, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompensationSpec {
    pub mode: CompensationMode,
    #[serde(default)]
    pub required_before_fields: Vec<String>,
    pub timeout_seconds: u64,
}

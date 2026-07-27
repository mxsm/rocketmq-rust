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

/// Closed set of Phase 3 actions that may enter supervised execution.
///
/// R3 and unknown actions are intentionally not representable.
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
}

impl ExecutionAction {
    /// Returns the exact Action Catalog identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::ObservabilityLoggerLevelTtl => "observability.logger_level_ttl.v1",
            Self::ProxyScaleOutOne => "proxy.scale_out_one.v1",
            Self::ProxyRestartOne => "proxy.restart_one.v1",
            Self::BrokerConfigPatchAllowlisted => "broker.config.patch_allowlisted.v1",
            Self::TopicConfigPatchAllowlisted => "topic.config.patch_allowlisted.v1",
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

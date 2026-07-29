// Copyright 2023 The RocketMQ Rust Authors
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

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::CompensationSpec;
use crate::ImpactScope;
use crate::SchemaVersion;
use crate::VerificationSpec;

/// Kind-safe namespace for extension identifiers.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescriptorKind {
    EvidenceSource,
    DiagnosticPack,
    Action,
    Provider,
    Integration,
}

/// Lifecycle status of one descriptor version.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescriptorStatus {
    Active,
    Disabled,
    Deprecated,
}

/// Explicit migration information for a deprecated extension.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Deprecation {
    pub since: String,
    pub replacement: Option<String>,
    pub message: String,
}

/// Risk taxonomy reserved for future action planning and approval.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionRisk {
    Read,
    Plan,
    R1,
    R2,
    R3,
}

/// Extension contract for a queryable evidence source.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct EvidenceSourceDescriptor {
    pub id: String,
    pub version: String,
    pub owner: String,
    pub supported_versions: Vec<SchemaVersion>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub required_capabilities: BTreeSet<String>,
    pub config_schema: Value,
    pub status: DescriptorStatus,
    pub deprecation: Option<Deprecation>,
    pub source_kind: String,
    pub query_schema: Value,
    pub result_schema: Value,
}

/// Extension contract for a diagnostic reasoning pack.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticPackDescriptor {
    pub id: String,
    pub version: String,
    pub owner: String,
    pub supported_versions: Vec<SchemaVersion>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub required_capabilities: BTreeSet<String>,
    pub config_schema: Value,
    pub status: DescriptorStatus,
    pub deprecation: Option<Deprecation>,
    pub required_sources: BTreeSet<String>,
    pub produced_hypotheses: BTreeSet<String>,
}

/// Versioned action contract shared by planning, policy, and typed handlers.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionDescriptor {
    pub id: String,
    pub version: String,
    pub owner: String,
    pub supported_versions: Vec<SchemaVersion>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub required_capabilities: BTreeSet<String>,
    pub config_schema: Value,
    pub status: DescriptorStatus,
    pub deprecation: Option<Deprecation>,
    pub risk: ActionRisk,
    pub execution_supported: bool,
    #[serde(default)]
    pub parameter_schema: Value,
    #[serde(default)]
    pub preconditions: Vec<String>,
    #[serde(default)]
    pub max_impact: ImpactScope,
    #[serde(default)]
    pub verification: VerificationSpec,
    #[serde(default)]
    pub timeout_seconds: u64,
    #[serde(default)]
    pub compensation: CompensationSpec,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub forbidden_fields: BTreeSet<String>,
    #[serde(default)]
    pub plan_only: bool,
}

/// Model provider capabilities without provider SDK coupling.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProviderDescriptor {
    pub id: String,
    pub version: String,
    pub owner: String,
    pub supported_versions: Vec<SchemaVersion>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub required_capabilities: BTreeSet<String>,
    pub config_schema: Value,
    pub status: DescriptorStatus,
    pub deprecation: Option<Deprecation>,
    pub protocols: BTreeSet<String>,
    pub supports_streaming: bool,
    pub supports_tools: bool,
    pub supports_structured_output: bool,
    pub supports_embeddings: bool,
}

/// Extension contract for an external system integration.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct IntegrationDescriptor {
    pub id: String,
    pub version: String,
    pub owner: String,
    pub supported_versions: Vec<SchemaVersion>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub required_capabilities: BTreeSet<String>,
    pub config_schema: Value,
    pub status: DescriptorStatus,
    pub deprecation: Option<Deprecation>,
    pub integration_kind: String,
    pub inbound: bool,
    pub outbound: bool,
    #[serde(default)]
    pub interfaces: BTreeSet<crate::IntegrationSpiCapability>,
    #[serde(default)]
    pub operational: crate::IntegrationOperationalPolicy,
}

/// All Phase 00 descriptor types in one versioned registry value.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "descriptor", rename_all = "snake_case")]
pub enum Descriptor {
    EvidenceSource(EvidenceSourceDescriptor),
    DiagnosticPack(DiagnosticPackDescriptor),
    Action(ActionDescriptor),
    Provider(ProviderDescriptor),
    Integration(IntegrationDescriptor),
}

macro_rules! descriptor_match {
    ($self:expr, $binding:ident => $value:expr) => {
        match $self {
            Descriptor::EvidenceSource($binding) => $value,
            Descriptor::DiagnosticPack($binding) => $value,
            Descriptor::Action($binding) => $value,
            Descriptor::Provider($binding) => $value,
            Descriptor::Integration($binding) => $value,
        }
    };
}

impl Descriptor {
    /// Returns the descriptor namespace.
    #[must_use]
    pub const fn kind(&self) -> DescriptorKind {
        match self {
            Self::EvidenceSource(_) => DescriptorKind::EvidenceSource,
            Self::DiagnosticPack(_) => DescriptorKind::DiagnosticPack,
            Self::Action(_) => DescriptorKind::Action,
            Self::Provider(_) => DescriptorKind::Provider,
            Self::Integration(_) => DescriptorKind::Integration,
        }
    }

    /// Returns the stable identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        descriptor_match!(self, descriptor => descriptor.id.as_str())
    }

    /// Returns the semantic descriptor version.
    #[must_use]
    pub fn version(&self) -> &str {
        descriptor_match!(self, descriptor => descriptor.version.as_str())
    }

    /// Returns contract families the extension understands.
    #[must_use]
    pub fn supported_versions(&self) -> &[SchemaVersion] {
        descriptor_match!(self, descriptor => descriptor.supported_versions.as_slice())
    }

    /// Returns capabilities required before activation.
    #[must_use]
    pub fn required_capabilities(&self) -> &BTreeSet<String> {
        descriptor_match!(self, descriptor => &descriptor.required_capabilities)
    }

    /// Returns the current lifecycle status.
    #[must_use]
    pub const fn status(&self) -> DescriptorStatus {
        descriptor_match!(self, descriptor => descriptor.status)
    }

    /// Updates lifecycle status inside the registry.
    pub fn set_status(&mut self, status: DescriptorStatus) {
        descriptor_match!(self, descriptor => descriptor.status = status);
    }

    /// Records explicit deprecation metadata.
    pub fn set_deprecation(&mut self, deprecation: Deprecation) {
        descriptor_match!(self, descriptor => descriptor.deprecation = Some(deprecation));
    }
}

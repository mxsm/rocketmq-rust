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

use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::Arc;

use rocketmq_sre_model_gateway::CanonicalModelRequest;
use rocketmq_sre_model_gateway::CanonicalModelResponse;
use rocketmq_sre_model_gateway::ChatModelProvider;
use rocketmq_sre_model_gateway::DataClass;
use rocketmq_sre_model_gateway::FinishReason;
use rocketmq_sre_model_gateway::InvocationContext;
use rocketmq_sre_model_gateway::InvocationMetadata;
use rocketmq_sre_model_gateway::InvocationPurpose;
use rocketmq_sre_model_gateway::ModelInvocationOutcome;
use rocketmq_sre_model_gateway::ProviderCapabilities;
use rocketmq_sre_model_gateway::ProviderError;
use rocketmq_sre_model_gateway::ProviderHealth;
use rocketmq_sre_model_gateway::ProviderRegistry;
use rocketmq_sre_model_gateway::ProviderRouter;
use rocketmq_sre_model_gateway::RoutingPolicy;
use rocketmq_sre_model_gateway::RoutingRequirements;
use rocketmq_sre_model_gateway::builtin_provider_profiles;
use serde::Deserialize;
use serde::Serialize;

use super::ShadowEvalError;

/// Provider behavior exercised by the offline shadow suite.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderMode {
    /// Deterministic provider response with no network call.
    Mock,
    /// No model profile is registered.
    RulesOnly,
    /// A registered provider returns service unavailable.
    Outage,
}

impl Display for ProviderMode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mock => formatter.write_str("mock"),
            Self::RulesOnly => formatter.write_str("rules_only"),
            Self::Outage => formatter.write_str("outage"),
        }
    }
}

impl FromStr for ProviderMode {
    type Err = ShadowEvalError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "mock" => Ok(Self::Mock),
            "rules-only" | "rules_only" => Ok(Self::RulesOnly),
            "outage" => Ok(Self::Outage),
            other => Err(ShadowEvalError::InvalidManifest(format!(
                "unknown provider mode `{other}`"
            ))),
        }
    }
}

#[derive(Clone)]
enum MockBehavior {
    Respond(String),
    Outage,
}

struct ShadowMockProvider {
    profile_id: String,
    capabilities: ProviderCapabilities,
    behavior: MockBehavior,
}

impl ChatModelProvider for ShadowMockProvider {
    fn profile_id(&self) -> &str {
        &self.profile_id
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.capabilities.clone()
    }

    fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }

    fn invoke(
        &self,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
    ) -> Result<CanonicalModelResponse, ProviderError> {
        context.ensure_active()?;
        self.capabilities.ensure_request_supported(request)?;
        match &self.behavior {
            MockBehavior::Respond(content) => Ok(CanonicalModelResponse::text(
                "phase1-shadow-mock",
                "deterministic-synthesis-v1",
                content,
                FinishReason::Stop,
            )),
            MockBehavior::Outage => Err(ProviderError::service_unavailable(
                "shadow provider fixture is unavailable",
            )),
        }
    }
}

pub(super) fn invoke_provider(
    mode: ProviderMode,
    request: &CanonicalModelRequest,
    response_content: String,
) -> Result<ModelInvocationOutcome, ShadowEvalError> {
    let mut registry = ProviderRegistry::new();
    if mode != ProviderMode::RulesOnly {
        let profile = builtin_provider_profiles()
            .into_iter()
            .find(|candidate| candidate.id == "vllm")
            .ok_or_else(|| {
                ShadowEvalError::InvalidManifest("built-in vllm profile is required by shadow evaluation".to_owned())
            })?;
        let behavior = match mode {
            ProviderMode::Mock => MockBehavior::Respond(response_content),
            ProviderMode::Outage => MockBehavior::Outage,
            ProviderMode::RulesOnly => {
                return Err(ShadowEvalError::InvalidManifest(
                    "rules-only mode cannot register a provider".to_owned(),
                ));
            }
        };
        let provider = Arc::new(ShadowMockProvider {
            profile_id: profile.id.clone(),
            capabilities: profile.capabilities.clone(),
            behavior,
        });
        registry.register(profile, provider)?;
    }

    let router = ProviderRouter::new(registry, RoutingPolicy { max_fallbacks: 1 });
    let requirements = RoutingRequirements::new(DataClass::Internal);
    let metadata = InvocationMetadata {
        purpose: InvocationPurpose::Evaluation,
        requested_profile_id: (mode != ProviderMode::RulesOnly).then(|| "vllm".to_owned()),
        prompt_version: "phase1-shadow-eval.v1".to_owned(),
        output_schema_version: "phase1-shadow-synthesis.v1".to_owned(),
        mark_primary: false,
        ..InvocationMetadata::default()
    };
    router
        .invoke(request, &requirements, &metadata)
        .map_err(ShadowEvalError::from)
}

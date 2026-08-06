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
use std::fmt::Debug;
use std::fmt::Formatter;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::error::ProviderError;
use crate::error::ProviderErrorCode;
use crate::ir::CanonicalModelRequest;
use crate::ir::ModelContentPart;
use crate::ir::ResponseFormat;
use crate::ir::ToolChoice;
use crate::secret::SecretReference;

/// Canonical provider families implemented by the gateway.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFamily {
    OpenAiCompatible,
    Anthropic,
    Gemini,
    Bedrock,
    ProviderSpi,
}

/// Explicit wire dialect selected by a provider profile.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderDialect {
    OpenAi,
    AzureOpenAi,
    Anthropic,
    Gemini,
    Bedrock,
    DeepSeekResponses,
    DeepSeekOpenAi,
    DeepSeekAnthropic,
    ZhipuGlm,
    Kimi,
    Vllm,
    Ollama,
    LlamaCpp,
    Sglang,
    EnterpriseProxy,
    ProprietarySpi,
}

/// Data sensitivity accepted by a provider profile.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataClass {
    Public,
    Internal,
    Confidential,
    Restricted,
}

/// Individually routable provider capability.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCapability {
    Chat,
    Text,
    JsonObject,
    JsonSchema,
    ToolCalling,
    ToolChoiceRequired,
    ToolChoiceSpecific,
    StrictTools,
    Vision,
    Reasoning,
    Streaming,
    Embeddings,
    Rerank,
    KimiMfjs,
}

/// Versioned capability declaration for one profile.
#[derive(Clone, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    #[serde(default)]
    pub supported: BTreeSet<ProviderCapability>,
    pub max_input_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
}

impl ProviderCapabilities {
    /// Baseline chat capabilities shared by conforming providers.
    #[must_use]
    pub fn chat_default() -> Self {
        Self {
            supported: BTreeSet::from([
                ProviderCapability::Chat,
                ProviderCapability::Text,
                ProviderCapability::JsonObject,
                ProviderCapability::ToolCalling,
                ProviderCapability::Streaming,
            ]),
            max_input_tokens: None,
            max_output_tokens: None,
        }
    }

    /// Adds declared capabilities.
    #[must_use]
    pub fn with(mut self, capabilities: impl IntoIterator<Item = ProviderCapability>) -> Self {
        self.supported.extend(capabilities);
        self
    }

    /// Whether all requested capabilities are declared.
    #[must_use]
    pub fn supports_all(&self, required: &BTreeSet<ProviderCapability>) -> bool {
        required.is_subset(&self.supported)
    }

    /// Returns the explicit capabilities required by a canonical request.
    #[must_use]
    pub fn required_for_request(request: &CanonicalModelRequest) -> BTreeSet<ProviderCapability> {
        let mut required = BTreeSet::from([ProviderCapability::Chat, ProviderCapability::Text]);
        match &request.response_format {
            ResponseFormat::Text => {}
            ResponseFormat::JsonObject => {
                required.insert(ProviderCapability::JsonObject);
            }
            ResponseFormat::JsonSchema { .. } => {
                required.insert(ProviderCapability::JsonSchema);
            }
        }
        if !request.tools.is_empty() {
            required.insert(ProviderCapability::ToolCalling);
        }
        if request.tools.iter().any(|tool| tool.strict) {
            required.insert(ProviderCapability::StrictTools);
        }
        match request.tool_choice {
            ToolChoice::Auto | ToolChoice::None => {}
            ToolChoice::Required => {
                required.insert(ProviderCapability::ToolChoiceRequired);
            }
            ToolChoice::Specific { .. } => {
                required.insert(ProviderCapability::ToolChoiceSpecific);
            }
        }
        if request.stream {
            required.insert(ProviderCapability::Streaming);
        }
        if request.reasoning
            || request
                .messages
                .iter()
                .any(|message| message.reasoning_content.is_some())
        {
            required.insert(ProviderCapability::Reasoning);
        }
        if request.messages.iter().any(|message| {
            message
                .parts
                .iter()
                .any(|part| matches!(part, ModelContentPart::Image { .. }))
        }) {
            required.insert(ProviderCapability::Vision);
        }
        if request.extensions.kimi_mfjs {
            required.insert(ProviderCapability::KimiMfjs);
        }
        required
    }

    /// Validates a request against the declared capability surface.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderErrorCode::CapabilityUnsupported`] without prompt
    /// simulation when any required capability is absent.
    pub fn ensure_request_supported(&self, request: &CanonicalModelRequest) -> Result<(), ProviderError> {
        if request.tools.iter().any(|tool| tool.mutates_cluster) {
            return Err(ProviderError::policy_denied(
                "model gateway accepts read-only tools only",
            ));
        }
        if let ToolChoice::Specific { name } = &request.tool_choice
            && !request.tools.iter().any(|tool| &tool.name == name)
        {
            return Err(ProviderError::new(
                ProviderErrorCode::InvalidRequest,
                "specific tool choice does not match a declared tool",
            ));
        }
        if let (Some(requested), Some(maximum)) = (request.max_output_tokens, self.max_output_tokens)
            && requested > maximum
        {
            return Err(ProviderError::capability_unsupported(
                "requested output token limit exceeds provider capability",
            ));
        }
        let required = Self::required_for_request(request);
        let missing: Vec<_> = required.difference(&self.supported).copied().collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(ProviderError::capability_unsupported(format!(
                "provider profile lacks required capabilities: {missing:?}"
            )))
        }
    }
}

/// Runtime health used by the registry and router.
#[derive(Clone, Copy, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHealth {
    #[default]
    Unknown,
    Healthy,
    Degraded,
    Unavailable,
    Quarantined,
}

impl ProviderHealth {
    /// Whether a profile may receive a new invocation.
    #[must_use]
    pub const fn routable(self) -> bool {
        matches!(self, Self::Unknown | Self::Healthy | Self::Degraded)
    }
}

/// Immutable, reference-only provider configuration.
#[derive(Clone, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProviderProfile {
    pub id: String,
    pub endpoint: String,
    pub provider_family: ProviderFamily,
    pub dialect: ProviderDialect,
    /// Normalized family used in invocation records and routing.
    pub model_family: String,
    pub model: String,
    pub model_revision: String,
    pub endpoint_instance: String,
    pub capabilities: ProviderCapabilities,
    pub region: String,
    #[serde(default)]
    pub allowed_data_classes: BTreeSet<DataClass>,
    pub priority: u16,
    pub health: ProviderHealth,
    /// Budget filter in micro-USD per 1K combined tokens.
    pub estimated_cost_microusd_per_1k_tokens: Option<u64>,
    /// A reference only. Serialized profiles never contain credential values.
    pub credential_ref: Option<SecretReference>,
    #[serde(default)]
    pub preserve_reasoning_content: bool,
    #[serde(default)]
    pub kimi_mfjs_enabled: bool,
}

impl Debug for ProviderProfile {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderProfile")
            .field("id", &self.id)
            .field("endpoint", &"[ENDPOINT REDACTED]")
            .field("provider_family", &self.provider_family)
            .field("dialect", &self.dialect)
            .field("model_family", &self.model_family)
            .field("model", &self.model)
            .field("model_revision", &self.model_revision)
            .field("endpoint_instance", &self.endpoint_instance)
            .field("capabilities", &self.capabilities)
            .field("region", &self.region)
            .field("allowed_data_classes", &self.allowed_data_classes)
            .field("priority", &self.priority)
            .field("health", &self.health)
            .field(
                "estimated_cost_microusd_per_1k_tokens",
                &self.estimated_cost_microusd_per_1k_tokens,
            )
            .field(
                "credential_ref",
                &self.credential_ref.as_ref().map(|_| "[REFERENCE REDACTED]"),
            )
            .field("preserve_reasoning_content", &self.preserve_reasoning_content)
            .field("kimi_mfjs_enabled", &self.kimi_mfjs_enabled)
            .finish()
    }
}

impl ProviderProfile {
    /// Validates security- and routing-relevant profile invariants.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderErrorCode::ProfileInvalid`] for invalid endpoints,
    /// missing identities, or a profile without chat capability.
    pub fn validate(&self) -> Result<(), ProviderError> {
        if self.id.trim().is_empty()
            || self.model_family.trim().is_empty()
            || self.model.trim().is_empty()
            || self.model_revision.trim().is_empty()
            || self.endpoint_instance.trim().is_empty()
        {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "provider profile identity fields must be non-empty",
            ));
        }
        if !(self.endpoint.starts_with("https://")
            || self.endpoint.starts_with("http://")
            || self.endpoint.starts_with("grpc://")
            || self.endpoint.starts_with("grpcs://"))
        {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "provider endpoint must use an approved transport scheme",
            ));
        }
        if !self.capabilities.supported.contains(&ProviderCapability::Chat) {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "chat provider profile must declare chat capability",
            ));
        }
        Ok(())
    }
}

fn credential(locator: &str) -> Option<SecretReference> {
    SecretReference::external(locator.to_owned()).ok()
}

fn common_capabilities() -> ProviderCapabilities {
    ProviderCapabilities::chat_default().with([
        ProviderCapability::JsonSchema,
        ProviderCapability::ToolChoiceRequired,
        ProviderCapability::ToolChoiceSpecific,
        ProviderCapability::StrictTools,
        ProviderCapability::Vision,
    ])
}

fn cloud_data_classes() -> BTreeSet<DataClass> {
    BTreeSet::from([DataClass::Public, DataClass::Internal, DataClass::Confidential])
}

fn local_data_classes() -> BTreeSet<DataClass> {
    BTreeSet::from([
        DataClass::Public,
        DataClass::Internal,
        DataClass::Confidential,
        DataClass::Restricted,
    ])
}

#[allow(
    clippy::too_many_arguments,
    reason = "centralizes immutable built-in profile fixtures"
)]
fn profile(
    id: &str,
    endpoint: &str,
    family: ProviderFamily,
    dialect: ProviderDialect,
    model_family: &str,
    model: &str,
    revision: &str,
    region: &str,
    priority: u16,
    capabilities: ProviderCapabilities,
    credential_ref: Option<SecretReference>,
) -> ProviderProfile {
    ProviderProfile {
        id: id.to_owned(),
        endpoint: endpoint.to_owned(),
        provider_family: family,
        dialect,
        model_family: model_family.to_owned(),
        model: model.to_owned(),
        model_revision: revision.to_owned(),
        endpoint_instance: format!("{id}:{region}"),
        capabilities,
        region: region.to_owned(),
        allowed_data_classes: cloud_data_classes(),
        priority,
        health: ProviderHealth::Healthy,
        estimated_cost_microusd_per_1k_tokens: None,
        credential_ref,
        preserve_reasoning_content: false,
        kimi_mfjs_enabled: false,
    }
}

/// Returns the built-in cloud, Chinese-provider, local, and enterprise profiles.
///
/// These are protocol fixtures and safe defaults, not live credentials.
#[must_use]
pub fn builtin_provider_profiles() -> Vec<ProviderProfile> {
    let full = common_capabilities();
    let reasoning = full
        .clone()
        .with([ProviderCapability::Reasoning, ProviderCapability::Embeddings]);
    let deepseek_responses =
        ProviderCapabilities::chat_default().with([ProviderCapability::JsonSchema, ProviderCapability::Reasoning]);
    let deepseek_openai = deepseek_responses.clone();
    let mut profiles = vec![
        profile(
            "openai",
            "https://api.openai.com/v1",
            ProviderFamily::OpenAiCompatible,
            ProviderDialect::OpenAi,
            "gpt",
            "gpt-configured",
            "configured",
            "global",
            10,
            reasoning.clone(),
            credential("rocketmq-sre/models/openai"),
        ),
        profile(
            "azure-openai",
            "https://example.openai.azure.com",
            ProviderFamily::OpenAiCompatible,
            ProviderDialect::AzureOpenAi,
            "gpt",
            "deployment-configured",
            "deployment",
            "configured",
            20,
            reasoning.clone(),
            credential("rocketmq-sre/models/azure-openai"),
        ),
        profile(
            "anthropic",
            "https://api.anthropic.com/v1",
            ProviderFamily::Anthropic,
            ProviderDialect::Anthropic,
            "claude",
            "claude-configured",
            "configured",
            "global",
            30,
            full.clone().with([ProviderCapability::Reasoning]),
            credential("rocketmq-sre/models/anthropic"),
        ),
        profile(
            "google-gemini",
            "https://generativelanguage.googleapis.com/v1beta",
            ProviderFamily::Gemini,
            ProviderDialect::Gemini,
            "gemini",
            "gemini-configured",
            "configured",
            "global",
            40,
            reasoning.clone(),
            credential("rocketmq-sre/models/gemini"),
        ),
        profile(
            "aws-bedrock",
            "https://bedrock-runtime.configured.amazonaws.com",
            ProviderFamily::Bedrock,
            ProviderDialect::Bedrock,
            "bedrock",
            "bedrock-configured",
            "configured",
            "configured",
            50,
            full.clone().with([ProviderCapability::Embeddings]),
            credential("rocketmq-sre/models/bedrock"),
        ),
        profile(
            "deepseek-responses",
            "https://api.deepseek.com",
            ProviderFamily::OpenAiCompatible,
            ProviderDialect::DeepSeekResponses,
            "deepseek",
            "deepseek-v4-flash",
            "v4-flash",
            "cn",
            14,
            deepseek_responses,
            credential("rocketmq-sre/models/deepseek"),
        ),
        profile(
            "deepseek",
            "https://api.deepseek.com",
            ProviderFamily::OpenAiCompatible,
            ProviderDialect::DeepSeekOpenAi,
            "deepseek",
            "deepseek-chat",
            "configured",
            "cn",
            15,
            deepseek_openai,
            credential("rocketmq-sre/models/deepseek"),
        ),
        profile(
            "deepseek-anthropic",
            "https://api.deepseek.com/anthropic",
            ProviderFamily::Anthropic,
            ProviderDialect::DeepSeekAnthropic,
            "deepseek",
            "deepseek-chat",
            "configured",
            "cn",
            16,
            full.clone().with([ProviderCapability::Reasoning]),
            credential("rocketmq-sre/models/deepseek-anthropic"),
        ),
        profile(
            "zhipu-glm",
            "https://open.bigmodel.cn/api/paas/v4",
            ProviderFamily::OpenAiCompatible,
            ProviderDialect::ZhipuGlm,
            "glm",
            "glm-configured",
            "configured",
            "cn",
            25,
            ProviderCapabilities::chat_default().with([ProviderCapability::JsonSchema, ProviderCapability::Embeddings]),
            credential("rocketmq-sre/models/zhipu-glm"),
        ),
        profile(
            "kimi-moonshot",
            "https://api.moonshot.cn/v1",
            ProviderFamily::OpenAiCompatible,
            ProviderDialect::Kimi,
            "kimi",
            "moonshot-configured",
            "configured",
            "cn",
            26,
            full.clone(),
            credential("rocketmq-sre/models/kimi"),
        ),
        profile(
            "kimi-moonshot-mfjs",
            "https://api.moonshot.cn/v1",
            ProviderFamily::OpenAiCompatible,
            ProviderDialect::Kimi,
            "kimi",
            "moonshot-configured",
            "configured-mfjs",
            "cn",
            27,
            full.clone().with([ProviderCapability::KimiMfjs]),
            credential("rocketmq-sre/models/kimi"),
        ),
        profile(
            "enterprise-openai-proxy",
            "https://models.internal.example/v1",
            ProviderFamily::OpenAiCompatible,
            ProviderDialect::EnterpriseProxy,
            "enterprise",
            "configured-model",
            "configured",
            "private",
            60,
            full.clone(),
            credential("rocketmq-sre/models/enterprise-proxy"),
        ),
    ];
    for (id, dialect, endpoint) in [
        ("vllm", ProviderDialect::Vllm, "http://vllm.models.svc/v1"),
        ("ollama", ProviderDialect::Ollama, "http://ollama.models.svc/v1"),
        ("llama-cpp", ProviderDialect::LlamaCpp, "http://llama-cpp.models.svc/v1"),
        ("sglang", ProviderDialect::Sglang, "http://sglang.models.svc/v1"),
    ] {
        let mut local = profile(
            id,
            endpoint,
            ProviderFamily::OpenAiCompatible,
            dialect,
            "local",
            "served-model",
            "served",
            "private",
            100,
            full.clone().with([ProviderCapability::Reasoning]),
            None,
        );
        local.allowed_data_classes = local_data_classes();
        profiles.push(local);
    }

    for candidate in &mut profiles {
        if matches!(
            candidate.dialect,
            ProviderDialect::DeepSeekOpenAi | ProviderDialect::DeepSeekAnthropic
        ) {
            candidate.preserve_reasoning_content = true;
        }
        if candidate.id == "kimi-moonshot-mfjs" {
            candidate.kimi_mfjs_enabled = true;
        }
        candidate.estimated_cost_microusd_per_1k_tokens =
            Some(if candidate.model_family == "local" { 0 } else { 10_000 });
    }
    profiles
}

/// One frozen provider-profile manifest entry.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProviderProfileManifestEntry {
    pub id: String,
    pub dialect: String,
    pub provider_family: String,
    pub model_family: String,
    pub model_revision: String,
    pub capabilities: BTreeSet<String>,
    pub fixture: String,
}

/// Versioned profile-to-contract-fixture manifest.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProviderProfileManifest {
    pub schema_version: String,
    pub profiles: Vec<ProviderProfileManifestEntry>,
}

impl ProviderProfileManifest {
    /// Parses the JSON-compatible YAML manifest without a YAML runtime
    /// dependency.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderErrorCode::ProfileInvalid`] for malformed data or an
    /// unsupported manifest version.
    pub fn parse(input: &str) -> Result<Self, ProviderError> {
        let parsed: Self = serde_json::from_str(input).map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "provider profile manifest is invalid",
            )
        })?;
        if parsed.schema_version != "rocketmq-sre.provider-profile-manifest.v1" {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "provider profile manifest version is unsupported",
            ));
        }
        let mut ids = BTreeSet::new();
        if parsed.profiles.iter().any(|entry| {
            entry.id.is_empty()
                || entry.fixture.is_empty()
                || entry.capabilities.is_empty()
                || entry.fixture.contains("..")
                || !ids.insert(entry.id.clone())
        }) {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "provider profile manifest contains an invalid or duplicate entry",
            ));
        }
        Ok(parsed)
    }
}

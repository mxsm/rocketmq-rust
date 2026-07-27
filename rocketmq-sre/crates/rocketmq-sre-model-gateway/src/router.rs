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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_sre_contracts::CorrelationId;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::error::ProviderError;
use crate::error::ProviderErrorCode;
use crate::ir::CanonicalModelRequest;
use crate::ir::CanonicalModelResponse;
use crate::ir::ResponseFormat;
use crate::profile::DataClass;
use crate::profile::ProviderCapability;
use crate::profile::ProviderFamily;
use crate::profile::ProviderHealth;
use crate::profile::ProviderProfile;
use crate::provider::ChatModelProvider;
use crate::provider::InvocationContext;

/// Stable model invocation identifier.
#[derive(Clone, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ModelInvocationId(String);

impl ModelInvocationId {
    fn new() -> Self {
        Self(format!("mi-{}", CorrelationId::new()))
    }
}

impl Display for ModelInvocationId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Why the SRE invoked a model.
#[derive(Clone, Copy, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationPurpose {
    #[default]
    Diagnosis,
    EvidenceQueryPlanning,
    KnowledgeRetrieval,
    InspectionSummary,
    OperatorQuestion,
    Evaluation,
}

/// Caller-supplied invocation lineage and version metadata.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct InvocationMetadata {
    pub incident_id: Option<String>,
    pub diagnosis_revision: Option<String>,
    pub parent_invocation_id: Option<ModelInvocationId>,
    pub purpose: InvocationPurpose,
    pub requested_profile_id: Option<String>,
    pub prompt_version: String,
    pub output_schema_version: String,
    pub deadline_unix_ms: Option<u64>,
    /// Explicitly binds this successful invocation as the diagnosis primary.
    pub mark_primary: bool,
}

impl Default for InvocationMetadata {
    fn default() -> Self {
        Self {
            incident_id: None,
            diagnosis_revision: None,
            parent_invocation_id: None,
            purpose: InvocationPurpose::Diagnosis,
            requested_profile_id: None,
            prompt_version: "unspecified".to_owned(),
            output_schema_version: "unspecified".to_owned(),
            deadline_unix_ms: None,
            mark_primary: false,
        }
    }
}

/// One failed attempt in a limited fallback chain.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct FallbackAttempt {
    pub profile_id: String,
    pub provider_family: ProviderFamily,
    pub model_family: String,
    pub model_revision: String,
    pub endpoint_instance: String,
    pub error_code: ProviderErrorCode,
    pub retryable: bool,
}

/// Auditable identity for the model that actually produced a result.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ModelInvocationRecord {
    pub invocation_id: ModelInvocationId,
    pub correlation_id: CorrelationId,
    pub incident_id: Option<String>,
    pub diagnosis_revision: Option<String>,
    pub parent_invocation_id: Option<ModelInvocationId>,
    pub purpose: InvocationPurpose,
    pub requested_profile_id: Option<String>,
    pub actual_profile_id: String,
    pub provider_family: ProviderFamily,
    pub actual_model_family: String,
    pub actual_model: String,
    pub actual_model_revision: String,
    pub endpoint_instance: String,
    pub fallback_chain: Vec<FallbackAttempt>,
    pub prompt_version: String,
    pub output_schema_version: String,
    pub started_at_unix_ms: u64,
    pub completed_at_unix_ms: u64,
}

/// Explicit primary-model selection for a diagnosis revision.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DiagnosisModelSelection {
    pub primary_model_invocation_id: Option<ModelInvocationId>,
    /// Phase 01 remains read-only regardless of model result.
    pub execution_eligible: bool,
}

/// Successful gateway invocation.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ModelInvocationResult {
    pub response: CanonicalModelResponse,
    pub record: ModelInvocationRecord,
    pub diagnosis_selection: DiagnosisModelSelection,
}

/// Rules-only degradation when no model endpoint is eligible or available.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct RulesOnlyResult {
    pub primary_model_invocation_id: Option<ModelInvocationId>,
    pub execution_eligible: bool,
    pub correlation_id: CorrelationId,
    pub fallback_chain: Vec<FallbackAttempt>,
    pub reason: String,
}

/// Model or deterministic degradation outcome.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ModelInvocationOutcome {
    Completed(Box<ModelInvocationResult>),
    RulesOnly(RulesOnlyResult),
}

struct RegisteredProvider {
    profile: ProviderProfile,
    provider: Arc<dyn ChatModelProvider>,
}

/// Provider registry keyed by stable profile ID.
#[derive(Default)]
pub struct ProviderRegistry {
    entries: BTreeMap<String, RegisteredProvider>,
}

impl ProviderRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one validated provider/profile pair.
    ///
    /// # Errors
    ///
    /// Fails closed on invalid profiles, profile/provider ID mismatch, or a
    /// duplicate profile ID.
    pub fn register(
        &mut self,
        profile: ProviderProfile,
        provider: Arc<dyn ChatModelProvider>,
    ) -> Result<(), ProviderError> {
        profile.validate()?;
        if profile.id != provider.profile_id() {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "provider instance does not match profile id",
            ));
        }
        if !provider.capabilities().supports_all(&profile.capabilities.supported) {
            return Err(ProviderError::new(
                ProviderErrorCode::CapabilityUnsupported,
                "provider instance does not satisfy its profile capabilities",
            ));
        }
        if self.entries.contains_key(&profile.id) {
            return Err(ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "provider profile id is already registered",
            ));
        }
        self.entries
            .insert(profile.id.clone(), RegisteredProvider { profile, provider });
        Ok(())
    }

    /// Updates the routing health for a registered profile.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderErrorCode::ProfileInvalid`] for an unknown profile.
    pub fn set_profile_health(&mut self, profile_id: &str, health: ProviderHealth) -> Result<(), ProviderError> {
        let entry = self.entries.get_mut(profile_id).ok_or_else(|| {
            ProviderError::new(ProviderErrorCode::ProfileInvalid, "provider profile is not registered")
        })?;
        entry.profile.health = health;
        Ok(())
    }

    /// Returns a registered profile without exposing its provider client.
    #[must_use]
    pub fn profile(&self, profile_id: &str) -> Option<&ProviderProfile> {
        self.entries.get(profile_id).map(|entry| &entry.profile)
    }

    /// Returns provider profiles in stable ID order.
    pub fn profiles(&self) -> impl Iterator<Item = &ProviderProfile> {
        self.entries.values().map(|entry| &entry.profile)
    }
}

/// Provider eligibility filters.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct RoutingRequirements {
    pub required_capabilities: BTreeSet<ProviderCapability>,
    pub data_class: DataClass,
    pub region: Option<String>,
    pub max_cost_microusd_per_1k_tokens: Option<u64>,
}

impl RoutingRequirements {
    /// Creates routing requirements for a data class.
    #[must_use]
    pub fn new(data_class: DataClass) -> Self {
        Self {
            required_capabilities: BTreeSet::new(),
            data_class,
            region: None,
            max_cost_microusd_per_1k_tokens: None,
        }
    }

    /// Requires one additional capability.
    #[must_use]
    pub fn requiring(mut self, capability: ProviderCapability) -> Self {
        self.required_capabilities.insert(capability);
        self
    }
}

/// Limited fallback policy.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct RoutingPolicy {
    /// Number of profiles tried after the initial selection.
    pub max_fallbacks: usize,
}

impl Default for RoutingPolicy {
    fn default() -> Self {
        Self { max_fallbacks: 1 }
    }
}

/// Capability-, residency-, budget-, and priority-aware provider router.
pub struct ProviderRouter {
    registry: ProviderRegistry,
    policy: RoutingPolicy,
}

impl ProviderRouter {
    /// Creates a router over an immutable registry snapshot.
    #[must_use]
    pub const fn new(registry: ProviderRegistry, policy: RoutingPolicy) -> Self {
        Self { registry, policy }
    }

    /// Invokes an eligible provider with tightly limited fallback.
    ///
    /// # Errors
    ///
    /// Returns the first non-fallback-safe provider error, including policy,
    /// safety, invalid-input, capability, and schema-validation failures.
    pub fn invoke(
        &self,
        request: &CanonicalModelRequest,
        requirements: &RoutingRequirements,
        metadata: &InvocationMetadata,
    ) -> Result<ModelInvocationOutcome, ProviderError> {
        let mut required = requirements.required_capabilities.clone();
        required.extend(ProviderCapabilitiesForRequest::from_request(request));
        self.ensure_static_eligibility(&required, requirements)?;
        let mut candidates: Vec<_> = self
            .registry
            .entries
            .values()
            .filter(|entry| {
                entry.profile.health.routable()
                    && entry.provider.health().routable()
                    && entry.profile.capabilities.supports_all(&required)
                    && entry.profile.allowed_data_classes.contains(&requirements.data_class)
                    && requirements
                        .region
                        .as_ref()
                        .is_none_or(|region| region == &entry.profile.region)
                    && budget_allows(&entry.profile, requirements)
            })
            .collect();
        candidates.sort_by(|left, right| {
            let left_requested = metadata.requested_profile_id.as_deref() == Some(&left.profile.id);
            let right_requested = metadata.requested_profile_id.as_deref() == Some(&right.profile.id);
            right_requested
                .cmp(&left_requested)
                .then_with(|| left.profile.priority.cmp(&right.profile.priority))
                .then_with(|| left.profile.id.cmp(&right.profile.id))
        });
        if candidates.is_empty() {
            return Ok(rules_only(
                request.correlation_id,
                Vec::new(),
                "eligible model profiles are unavailable",
            ));
        }

        let started_at = current_unix_ms();
        let invocation_id = ModelInvocationId::new();
        let mut fallback_chain = Vec::new();
        let max_attempts = self.policy.max_fallbacks.saturating_add(1);
        for (index, entry) in candidates.into_iter().take(max_attempts).enumerate() {
            let mut invocation_context = InvocationContext::new(request.correlation_id);
            invocation_context.deadline_unix_ms = metadata.deadline_unix_ms;
            match entry.provider.invoke(&invocation_context, request) {
                Ok(response) => {
                    validate_structured_output(&request.response_format, &response.content)?;
                    let record = ModelInvocationRecord {
                        invocation_id: invocation_id.clone(),
                        correlation_id: request.correlation_id,
                        incident_id: metadata.incident_id.clone(),
                        diagnosis_revision: metadata.diagnosis_revision.clone(),
                        parent_invocation_id: metadata.parent_invocation_id.clone(),
                        purpose: metadata.purpose,
                        requested_profile_id: metadata.requested_profile_id.clone(),
                        actual_profile_id: entry.profile.id.clone(),
                        provider_family: entry.profile.provider_family,
                        actual_model_family: entry.profile.model_family.clone(),
                        actual_model: response.model.clone(),
                        actual_model_revision: entry.profile.model_revision.clone(),
                        endpoint_instance: entry.profile.endpoint_instance.clone(),
                        fallback_chain,
                        prompt_version: metadata.prompt_version.clone(),
                        output_schema_version: metadata.output_schema_version.clone(),
                        started_at_unix_ms: started_at,
                        completed_at_unix_ms: current_unix_ms(),
                    };
                    return Ok(ModelInvocationOutcome::Completed(Box::new(ModelInvocationResult {
                        response,
                        diagnosis_selection: DiagnosisModelSelection {
                            primary_model_invocation_id: metadata.mark_primary.then(|| record.invocation_id.clone()),
                            execution_eligible: false,
                        },
                        record,
                    })));
                }
                Err(error) if error.fallback_allowed() => {
                    fallback_chain.push(FallbackAttempt {
                        profile_id: entry.profile.id.clone(),
                        provider_family: entry.profile.provider_family,
                        model_family: entry.profile.model_family.clone(),
                        model_revision: entry.profile.model_revision.clone(),
                        endpoint_instance: entry.profile.endpoint_instance.clone(),
                        error_code: error.code,
                        retryable: error.retryable,
                    });
                    if index + 1 >= max_attempts {
                        return Ok(rules_only(
                            request.correlation_id,
                            fallback_chain,
                            "eligible model profiles are unavailable",
                        ));
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(rules_only(
            request.correlation_id,
            fallback_chain,
            "eligible model profiles are unavailable",
        ))
    }

    fn ensure_static_eligibility(
        &self,
        required: &BTreeSet<ProviderCapability>,
        requirements: &RoutingRequirements,
    ) -> Result<(), ProviderError> {
        if self.registry.entries.is_empty() {
            return Ok(());
        }
        let capability_matches: Vec<_> = self
            .registry
            .entries
            .values()
            .filter(|entry| entry.profile.capabilities.supports_all(required))
            .collect();
        if capability_matches.is_empty() {
            return Err(ProviderError::capability_unsupported(
                "no registered provider satisfies the required capability set",
            ));
        }
        let residency_matches: Vec<_> = capability_matches
            .into_iter()
            .filter(|entry| {
                entry.profile.allowed_data_classes.contains(&requirements.data_class)
                    && requirements
                        .region
                        .as_ref()
                        .is_none_or(|region| region == &entry.profile.region)
            })
            .collect();
        if residency_matches.is_empty() {
            return Err(ProviderError::new(
                ProviderErrorCode::DataResidencyDenied,
                "no registered provider satisfies data residency policy",
            ));
        }
        if residency_matches
            .iter()
            .all(|entry| !budget_allows(&entry.profile, requirements))
        {
            return Err(ProviderError::policy_denied(
                "no registered provider satisfies the model budget",
            ));
        }
        Ok(())
    }
}

struct ProviderCapabilitiesForRequest;

impl ProviderCapabilitiesForRequest {
    fn from_request(request: &CanonicalModelRequest) -> BTreeSet<ProviderCapability> {
        crate::profile::ProviderCapabilities::required_for_request(request)
    }
}

fn budget_allows(profile: &ProviderProfile, requirements: &RoutingRequirements) -> bool {
    match requirements.max_cost_microusd_per_1k_tokens {
        None => true,
        Some(maximum) => profile
            .estimated_cost_microusd_per_1k_tokens
            .is_some_and(|cost| cost <= maximum),
    }
}

fn rules_only(
    correlation_id: CorrelationId,
    fallback_chain: Vec<FallbackAttempt>,
    reason: &str,
) -> ModelInvocationOutcome {
    ModelInvocationOutcome::RulesOnly(RulesOnlyResult {
        primary_model_invocation_id: None,
        execution_eligible: false,
        correlation_id,
        fallback_chain,
        reason: reason.to_owned(),
    })
}

fn validate_structured_output(format: &ResponseFormat, content: &str) -> Result<(), ProviderError> {
    match format {
        ResponseFormat::Text => Ok(()),
        ResponseFormat::JsonObject => {
            let value: Value = serde_json::from_str(content).map_err(|_| schema_error())?;
            if value.is_object() { Ok(()) } else { Err(schema_error()) }
        }
        ResponseFormat::JsonSchema { schema, .. } => {
            let value: Value = serde_json::from_str(content).map_err(|_| schema_error())?;
            validate_json_schema_node(schema, schema, &value)
        }
    }
}

fn validate_json_schema_node(root: &Value, schema: &Value, value: &Value) -> Result<(), ProviderError> {
    if let Some(boolean_schema) = schema.as_bool() {
        return if boolean_schema { Ok(()) } else { Err(schema_error()) };
    }
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let pointer = reference
            .strip_prefix('#')
            .filter(|pointer| pointer.starts_with('/'))
            .ok_or_else(schema_error)?;
        let target = root.pointer(pointer).ok_or_else(schema_error)?;
        validate_json_schema_node(root, target, value)?;
    }
    if let Some(all_of) = schema.get("allOf").and_then(Value::as_array) {
        for branch in all_of {
            validate_json_schema_node(root, branch, value)?;
        }
    }
    if let Some(any_of) = schema.get("anyOf").and_then(Value::as_array)
        && !any_of
            .iter()
            .any(|branch| validate_json_schema_node(root, branch, value).is_ok())
    {
        return Err(schema_error());
    }
    if let Some(one_of) = schema.get("oneOf").and_then(Value::as_array)
        && one_of
            .iter()
            .filter(|branch| validate_json_schema_node(root, branch, value).is_ok())
            .count()
            != 1
    {
        return Err(schema_error());
    }
    if let Some(not_schema) = schema.get("not")
        && validate_json_schema_node(root, not_schema, value).is_ok()
    {
        return Err(schema_error());
    }
    if let Some(expected_type) = schema.get("type") {
        let type_matches = match expected_type {
            Value::String(expected) => value_matches_type(value, expected),
            Value::Array(expected) => expected
                .iter()
                .filter_map(Value::as_str)
                .any(|expected| value_matches_type(value, expected)),
            _ => false,
        };
        if !type_matches {
            return Err(schema_error());
        }
    }
    if let Some(constant) = schema.get("const")
        && constant != value
    {
        return Err(schema_error());
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array)
        && !allowed.contains(value)
    {
        return Err(schema_error());
    }
    if let Some(object) = value.as_object() {
        enforce_count_bounds(
            object.len(),
            schema.get("minProperties").and_then(Value::as_u64),
            schema.get("maxProperties").and_then(Value::as_u64),
        )?;
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for name in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(name) {
                    return Err(schema_error());
                }
            }
        }
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (name, property_schema) in properties {
                if let Some(property_value) = object.get(name) {
                    validate_json_schema_node(root, property_schema, property_value)?;
                }
            }
            if schema.get("additionalProperties") == Some(&Value::Bool(false))
                && object.keys().any(|name| !properties.contains_key(name))
            {
                return Err(schema_error());
            }
            if let Some(additional_schema) = schema
                .get("additionalProperties")
                .filter(|additional| additional.is_object())
            {
                for (name, additional_value) in object {
                    if !properties.contains_key(name) {
                        validate_json_schema_node(root, additional_schema, additional_value)?;
                    }
                }
            }
        } else if schema.get("additionalProperties") == Some(&Value::Bool(false)) && !object.is_empty() {
            return Err(schema_error());
        }
    }
    if let Some(array) = value.as_array() {
        enforce_count_bounds(
            array.len(),
            schema.get("minItems").and_then(Value::as_u64),
            schema.get("maxItems").and_then(Value::as_u64),
        )?;
        if schema.get("uniqueItems") == Some(&Value::Bool(true)) {
            for (index, item) in array.iter().enumerate() {
                if array[index.saturating_add(1)..].contains(item) {
                    return Err(schema_error());
                }
            }
        }
        if let Some(item_schema) = schema.get("items") {
            for item in array {
                validate_json_schema_node(root, item_schema, item)?;
            }
        }
        if let Some(contains_schema) = schema.get("contains")
            && !array
                .iter()
                .any(|item| validate_json_schema_node(root, contains_schema, item).is_ok())
        {
            return Err(schema_error());
        }
    }
    if let Some(string) = value.as_str() {
        enforce_count_bounds(
            string.chars().count(),
            schema.get("minLength").and_then(Value::as_u64),
            schema.get("maxLength").and_then(Value::as_u64),
        )?;
        // Regex evaluation is intentionally not approximated. Pattern-bearing
        // schemas fail closed until a full validator backend is configured.
        if schema.get("pattern").is_some() {
            return Err(schema_error());
        }
    }
    if let Some(number) = value.as_f64()
        && (schema
            .get("minimum")
            .and_then(Value::as_f64)
            .is_some_and(|minimum| number < minimum)
            || schema
                .get("maximum")
                .and_then(Value::as_f64)
                .is_some_and(|maximum| number > maximum)
            || schema
                .get("exclusiveMinimum")
                .and_then(Value::as_f64)
                .is_some_and(|minimum| number <= minimum)
            || schema
                .get("exclusiveMaximum")
                .and_then(Value::as_f64)
                .is_some_and(|maximum| number >= maximum))
    {
        return Err(schema_error());
    }
    Ok(())
}

fn value_matches_type(value: &Value, expected: &str) -> bool {
    match expected {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => false,
    }
}

fn enforce_count_bounds(count: usize, minimum: Option<u64>, maximum: Option<u64>) -> Result<(), ProviderError> {
    let count = count as u64;
    if minimum.is_some_and(|minimum| count < minimum) || maximum.is_some_and(|maximum| count > maximum) {
        Err(schema_error())
    } else {
        Ok(())
    }
}

fn schema_error() -> ProviderError {
    ProviderError::new(
        ProviderErrorCode::SchemaValidationFailed,
        "model output failed local JSON schema validation",
    )
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_subset_validates_required_types_and_additional_properties() {
        let schema = serde_json::json!({
            "type":"object",
            "required":["status"],
            "properties":{"status":{"type":"string","enum":["healthy","degraded"]}},
            "additionalProperties":false
        });
        assert!(validate_json_schema_node(&schema, &schema, &serde_json::json!({"status":"healthy"})).is_ok());
        assert_eq!(
            validate_json_schema_node(
                &schema,
                &schema,
                &serde_json::json!({"status":"healthy","secret":"leak"})
            )
            .expect_err("additional property")
            .code,
            ProviderErrorCode::SchemaValidationFailed
        );
    }

    #[test]
    fn schema_validator_resolves_local_defs_and_combinators() {
        let schema = serde_json::json!({
            "$defs":{
                "status":{"type":"string","enum":["healthy","degraded"]}
            },
            "type":"object",
            "required":["status","samples"],
            "properties":{
                "status":{"$ref":"#/$defs/status"},
                "samples":{
                    "type":"array",
                    "minItems":1,
                    "items":{"oneOf":[{"type":"integer"},{"const":"unknown"}]}
                }
            }
        });
        assert!(
            validate_json_schema_node(
                &schema,
                &schema,
                &serde_json::json!({"status":"healthy","samples":[1,"unknown"]})
            )
            .is_ok()
        );
        assert_eq!(
            validate_json_schema_node(&schema, &schema, &serde_json::json!({"status":"invalid","samples":[]}))
                .expect_err("invalid schema output")
                .code,
            ProviderErrorCode::SchemaValidationFailed
        );
    }
}

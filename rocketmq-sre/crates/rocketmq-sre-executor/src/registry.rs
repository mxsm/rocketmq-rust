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

use std::collections::BTreeSet;

use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::PlanStep;
use rocketmq_sre_core::ActionCatalog;
use rocketmq_sre_core::EMBEDDED_ACTION_DESCRIPTOR_YAMLS;
use serde_json::Map;
use serde_json::Value;

use crate::ExecutorError;

/// Exact catalog snapshot revalidated inside Executor before every dispatch.
#[derive(Clone, Debug)]
pub struct ExecutorActionRegistry {
    catalog: ActionCatalog,
    executable: BTreeSet<ExecutionAction>,
}

impl ExecutorActionRegistry {
    /// Loads all embedded descriptor versions. A descriptor remains
    /// non-executable until its `execution_supported` flag is explicitly
    /// enabled by its action implementation milestone.
    ///
    /// # Errors
    ///
    /// Rejects malformed YAML, duplicate versions, and catalog invariants.
    pub fn embedded() -> Result<Self, ExecutorError> {
        let descriptors = EMBEDDED_ACTION_DESCRIPTOR_YAMLS
            .iter()
            .copied()
            .map(serde_yaml::from_str::<ActionDescriptor>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ExecutorError::Configuration)?;
        Self::from_descriptors(descriptors)
    }

    /// Creates an immutable registry from exact descriptors.
    ///
    /// Primarily useful for focused engine tests and future dynamic descriptor
    /// snapshots. Production uses [`Self::embedded`].
    ///
    /// # Errors
    ///
    /// Rejects any descriptor outside the closed action catalog.
    pub fn from_descriptors(descriptors: impl IntoIterator<Item = ActionDescriptor>) -> Result<Self, ExecutorError> {
        let mut catalog = ActionCatalog::default();
        let mut executable = BTreeSet::new();
        for descriptor in descriptors {
            let action = ExecutionAction::from_id(&descriptor.id).ok_or(ExecutorError::Configuration)?;
            if descriptor.execution_supported {
                executable.insert(action);
            }
            catalog.register(descriptor)?;
        }
        Ok(Self { catalog, executable })
    }

    /// Rechecks exact version, risk, immutable descriptor fields, and closed
    /// parameters immediately before an execution starts.
    ///
    /// # Errors
    ///
    /// Fails closed for disabled/unknown versions, R3, descriptor drift, and
    /// parameters outside the local allowlist.
    pub fn validate_step(&self, step: &PlanStep) -> Result<(), ExecutorError> {
        self.validate_step_authorization(step, false)
    }

    /// Rechecks a step for its exact human or autonomy authorization path.
    ///
    /// # Errors
    ///
    /// In addition to the normal descriptor checks, autonomous requests are
    /// rejected unless the exact descriptor version is R1.
    pub fn validate_step_authorization(&self, step: &PlanStep, autonomous: bool) -> Result<(), ExecutorError> {
        let descriptor = self
            .catalog
            .executable_descriptor(step.action, &step.descriptor_version)?;
        if !matches!(descriptor.risk, ActionRisk::R1 | ActionRisk::R2)
            || (autonomous && descriptor.risk != ActionRisk::R1)
            || descriptor.max_impact != step.max_impact
            || descriptor.verification != step.verification
            || descriptor.compensation != step.compensation
            || !self.executable.contains(&step.action)
        {
            return Err(ExecutorError::InvalidRequest);
        }
        validate_parameters(descriptor, &step.parameters)?;
        Ok(())
    }

    #[must_use]
    pub fn executable_actions(&self) -> Vec<ExecutionAction> {
        self.executable.iter().copied().collect()
    }
}

fn validate_parameters(descriptor: &ActionDescriptor, parameters: &Value) -> Result<(), ExecutorError> {
    let mut observed_fields = BTreeSet::new();
    collect_fields(parameters, &mut observed_fields);
    if observed_fields
        .iter()
        .any(|field| descriptor.forbidden_fields.contains(field))
    {
        return Err(ExecutorError::InvalidRequest);
    }
    validate_schema_value(parameters, &descriptor.parameter_schema)
}

fn collect_fields(value: &Value, fields: &mut BTreeSet<String>) {
    match value {
        Value::Object(values) => {
            for (name, value) in values {
                fields.insert(name.clone());
                collect_fields(value, fields);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_fields(value, fields);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn validate_schema_value(value: &Value, schema: &Value) -> Result<(), ExecutorError> {
    match schema.get("type").and_then(Value::as_str) {
        Some("object") => validate_object(value, schema),
        Some("string") => validate_string(value, schema),
        Some("integer") => validate_integer(value, schema),
        Some("boolean") if value.is_boolean() => Ok(()),
        Some("array") if value.is_array() => Ok(()),
        Some(_) => Err(ExecutorError::InvalidRequest),
        None => Ok(()),
    }
}

fn validate_object(value: &Value, schema: &Value) -> Result<(), ExecutorError> {
    let values = value.as_object().ok_or(ExecutorError::InvalidRequest)?;
    let empty = Map::new();
    let properties = schema.get("properties").and_then(Value::as_object).unwrap_or(&empty);
    if schema.get("additionalProperties") == Some(&Value::Bool(false))
        && values.keys().any(|name| !properties.contains_key(name))
    {
        return Err(ExecutorError::InvalidRequest);
    }
    if schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .any(|name| !values.contains_key(name))
    {
        return Err(ExecutorError::InvalidRequest);
    }
    validate_property_count(values, schema)?;
    for (name, value) in values {
        if let Some(property_schema) = properties.get(name) {
            validate_schema_value(value, property_schema)?;
        }
    }
    Ok(())
}

fn validate_property_count(values: &Map<String, Value>, schema: &Value) -> Result<(), ExecutorError> {
    let count = u64::try_from(values.len()).map_err(|_| ExecutorError::InvalidRequest)?;
    if schema
        .get("minProperties")
        .and_then(Value::as_u64)
        .is_some_and(|minimum| count < minimum)
        || schema
            .get("maxProperties")
            .and_then(Value::as_u64)
            .is_some_and(|maximum| count > maximum)
    {
        return Err(ExecutorError::InvalidRequest);
    }
    Ok(())
}

fn validate_string(value: &Value, schema: &Value) -> Result<(), ExecutorError> {
    let value = value.as_str().ok_or(ExecutorError::InvalidRequest)?;
    if schema
        .get("maxLength")
        .and_then(Value::as_u64)
        .is_some_and(|maximum| u64::try_from(value.chars().count()).map_or(true, |count| count > maximum))
        || schema
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|allowed| !allowed.iter().any(|candidate| candidate.as_str() == Some(value)))
    {
        return Err(ExecutorError::InvalidRequest);
    }
    Ok(())
}

fn validate_integer(value: &Value, schema: &Value) -> Result<(), ExecutorError> {
    let value = value.as_i64().ok_or(ExecutorError::InvalidRequest)?;
    if schema
        .get("minimum")
        .and_then(Value::as_i64)
        .is_some_and(|minimum| value < minimum)
        || schema
            .get("maximum")
            .and_then(Value::as_i64)
            .is_some_and(|maximum| value > maximum)
    {
        return Err(ExecutorError::InvalidRequest);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rocketmq_sre_contracts::PlanStepId;
    use serde_json::json;

    use super::*;

    #[test]
    fn embedded_registry_only_enables_actions_with_shipped_handlers() {
        let registry = ExecutorActionRegistry::embedded().expect("embedded catalog");
        assert_eq!(
            registry.executable_actions(),
            vec![
                ExecutionAction::ObservabilityLoggerLevelTtl,
                ExecutionAction::ProxyScaleOutOne,
                ExecutionAction::ProxyRestartOne,
                ExecutionAction::BrokerConfigPatchAllowlisted,
                ExecutionAction::TopicConfigPatchAllowlisted,
                ExecutionAction::TelemetryCollectorRestartOne,
            ]
        );
    }

    #[test]
    fn autonomous_authorization_accepts_r1_and_rejects_r2() {
        let mut r1 = descriptor(ExecutionAction::ObservabilityLoggerLevelTtl);
        r1.execution_supported = true;
        let r1_step = step(
            &r1,
            ExecutionAction::ObservabilityLoggerLevelTtl,
            json!({
                "component": "broker",
                "logger": "rocketmq",
                "level": "DEBUG",
                "ttl_seconds": 60
            }),
        );
        let r1_registry = ExecutorActionRegistry::from_descriptors([r1]).expect("R1 registry");
        assert!(r1_registry.validate_step_authorization(&r1_step, true).is_ok());

        let r2 = descriptor(ExecutionAction::BrokerConfigPatchAllowlisted);
        let r2_step = step(
            &r2,
            ExecutionAction::BrokerConfigPatchAllowlisted,
            json!({
                "broker": "broker-a",
                "expected_generation": 9,
                "patch": {"send_message_thread_pool_nums": 32}
            }),
        );
        let r2_registry = ExecutorActionRegistry::from_descriptors([r2]).expect("R2 registry");
        assert!(r2_registry.validate_step_authorization(&r2_step, false).is_ok());
        assert!(r2_registry.validate_step_authorization(&r2_step, true).is_err());
    }

    fn descriptor(action: ExecutionAction) -> ActionDescriptor {
        EMBEDDED_ACTION_DESCRIPTOR_YAMLS
            .iter()
            .find_map(|yaml| {
                let descriptor = serde_yaml::from_str::<ActionDescriptor>(yaml).ok()?;
                (descriptor.id == action.id()).then_some(descriptor)
            })
            .expect("embedded action descriptor")
    }

    fn step(descriptor: &ActionDescriptor, action: ExecutionAction, parameters: Value) -> PlanStep {
        PlanStep {
            id: PlanStepId::new(),
            sequence: 1,
            action,
            descriptor_version: descriptor.version.clone(),
            resource: format!("test/{}", action.id()),
            parameters,
            evidence_ids: Vec::new(),
            precondition_hash: format!("sha256:{}", "a".repeat(64)),
            max_impact: descriptor.max_impact,
            verification: descriptor.verification.clone(),
            compensation: descriptor.compensation.clone(),
        }
    }
}

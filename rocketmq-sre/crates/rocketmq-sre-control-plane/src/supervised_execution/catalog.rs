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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::DescriptorStatus;
use rocketmq_sre_contracts::ExecutionAction;
use serde::Deserialize;
use serde_json::Map;
use serde_json::Value;

use crate::ControlPlaneError;

const LOGGER_LEVEL: &str = include_str!("../../../../config/actions/observability.logger_level_ttl.v1.yaml");
const PROXY_SCALE: &str = include_str!("../../../../config/actions/proxy.scale_out_one.v1.yaml");
const PROXY_RESTART: &str = include_str!("../../../../config/actions/proxy.restart_one.v1.yaml");
const BROKER_CONFIG: &str = include_str!("../../../../config/actions/broker.config.patch_allowlisted.v1.yaml");
const TOPIC_CONFIG: &str = include_str!("../../../../config/actions/topic.config.patch_allowlisted.v1.yaml");
const CAPABILITY_CATALOG: &str = include_str!("../../../../config/capabilities/rocketmq-capability-catalog.v1.yaml");

#[derive(Clone)]
pub(super) struct ActionCatalog {
    executable: BTreeMap<ExecutionAction, ActionDescriptor>,
    manual_only: BTreeMap<String, ManualAction>,
}

#[derive(Clone, Debug)]
pub(super) struct ManualAction {
    pub(super) id: String,
    pub(super) title: String,
    pub(super) description: String,
}

pub(super) enum CatalogResolution<'a> {
    Executable(ExecutionAction, &'a ActionDescriptor),
    ManualOnly(&'a ManualAction),
}

#[derive(Deserialize)]
struct CapabilityCatalog {
    capabilities: Vec<CapabilityEntry>,
}

#[derive(Deserialize)]
struct CapabilityEntry {
    id: String,
    title: String,
    description: String,
    sre_class: String,
}

impl ActionCatalog {
    pub(super) fn embedded() -> Result<Self, ControlPlaneError> {
        let mut executable = BTreeMap::new();
        for yaml in [LOGGER_LEVEL, PROXY_SCALE, PROXY_RESTART, BROKER_CONFIG, TOPIC_CONFIG] {
            let descriptor: ActionDescriptor = serde_yaml::from_str(yaml).map_err(|error| {
                ControlPlaneError::configuration(format!("Phase 3 action descriptor is invalid: {error}"))
            })?;
            let action = ExecutionAction::from_id(&descriptor.id).ok_or_else(|| {
                ControlPlaneError::configuration("action descriptor is outside the closed execution catalog")
            })?;
            validate_descriptor(action, &descriptor)?;
            if executable.insert(action, descriptor).is_some() {
                return Err(ControlPlaneError::configuration(
                    "action descriptor catalog contains a duplicate action",
                ));
            }
        }
        let capability_catalog: CapabilityCatalog = serde_yaml::from_str(CAPABILITY_CATALOG)
            .map_err(|error| ControlPlaneError::configuration(format!("capability catalog is invalid: {error}")))?;
        let manual_only = capability_catalog
            .capabilities
            .into_iter()
            .filter(|entry| entry.sre_class.eq_ignore_ascii_case("r3"))
            .map(|entry| {
                (
                    entry.id.clone(),
                    ManualAction {
                        id: entry.id,
                        title: entry.title,
                        description: entry.description,
                    },
                )
            })
            .collect();
        Ok(Self {
            executable,
            manual_only,
        })
    }

    pub(super) fn resolve(&self, id: &str) -> Result<CatalogResolution<'_>, ControlPlaneError> {
        if let Some(action) = ExecutionAction::from_id(id) {
            let descriptor = self.executable.get(&action).ok_or_else(|| {
                ControlPlaneError::configuration("closed execution action has no embedded descriptor")
            })?;
            return Ok(CatalogResolution::Executable(action, descriptor));
        }
        self.manual_only
            .get(id)
            .map(CatalogResolution::ManualOnly)
            .ok_or_else(|| {
                ControlPlaneError::validation(
                    "unknown_action",
                    "action is not registered in the supervised or manual-only catalog",
                )
            })
    }

    pub(super) fn descriptor(&self, action: ExecutionAction) -> Result<&ActionDescriptor, ControlPlaneError> {
        self.executable
            .get(&action)
            .ok_or_else(|| ControlPlaneError::configuration("execution action has no embedded descriptor"))
    }
}

fn validate_descriptor(action: ExecutionAction, descriptor: &ActionDescriptor) -> Result<(), ControlPlaneError> {
    if descriptor.id != action.id()
        || descriptor.version.trim().is_empty()
        || descriptor.status != DescriptorStatus::Active
        || !matches!(descriptor.risk, ActionRisk::R1 | ActionRisk::R2)
        || descriptor.plan_only
        || !descriptor.parameter_schema.is_object()
        || descriptor.timeout_seconds == 0
    {
        return Err(ControlPlaneError::configuration(
            "action descriptor violates the supervised catalog invariants",
        ));
    }
    Ok(())
}

pub(super) fn validate_parameters(descriptor: &ActionDescriptor, parameters: &Value) -> Result<(), ControlPlaneError> {
    let mut observed_fields = BTreeSet::new();
    collect_fields(parameters, &mut observed_fields);
    if let Some(field) = observed_fields
        .iter()
        .find(|field| descriptor.forbidden_fields.contains(field.as_str()))
    {
        return Err(ControlPlaneError::validation(
            "forbidden_action_field",
            format!("action parameters contain forbidden field `{field}`"),
        ));
    }
    validate_schema_value(parameters, &descriptor.parameter_schema, "$")
}

fn collect_fields(value: &Value, fields: &mut BTreeSet<String>) {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                fields.insert(key.to_owned());
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

fn validate_schema_value(value: &Value, schema: &Value, path: &str) -> Result<(), ControlPlaneError> {
    match schema.get("type").and_then(Value::as_str) {
        Some("object") => validate_object(value, schema, path),
        Some("string") => validate_string(value, schema, path),
        Some("integer") => validate_integer(value, schema, path),
        Some("boolean") if value.is_boolean() => Ok(()),
        Some("array") if value.is_array() => Ok(()),
        Some(expected) => Err(parameter_error(format!("{path} must be a {expected}"))),
        None => Ok(()),
    }
}

fn validate_object(value: &Value, schema: &Value, path: &str) -> Result<(), ControlPlaneError> {
    let values = value
        .as_object()
        .ok_or_else(|| parameter_error(format!("{path} must be an object")))?;
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();
    for required in required {
        if !values.contains_key(required) {
            return Err(parameter_error(format!("{path}.{required} is required")));
        }
    }
    if schema.get("additionalProperties") == Some(&Value::Bool(false))
        && let Some(unknown) = values.keys().find(|key| !properties.contains_key(key.as_str()))
    {
        return Err(parameter_error(format!("{path}.{unknown} is not allowlisted")));
    }
    validate_property_count(values, schema, path)?;
    for (name, value) in values {
        if let Some(property_schema) = properties.get(name) {
            validate_schema_value(value, property_schema, &format!("{path}.{name}"))?;
        }
    }
    Ok(())
}

fn validate_property_count(values: &Map<String, Value>, schema: &Value, path: &str) -> Result<(), ControlPlaneError> {
    let count =
        u64::try_from(values.len()).map_err(|_| parameter_error(format!("{path} contains too many properties")))?;
    if schema
        .get("minProperties")
        .and_then(Value::as_u64)
        .is_some_and(|minimum| count < minimum)
    {
        return Err(parameter_error(format!("{path} has too few properties")));
    }
    if schema
        .get("maxProperties")
        .and_then(Value::as_u64)
        .is_some_and(|maximum| count > maximum)
    {
        return Err(parameter_error(format!("{path} has too many properties")));
    }
    Ok(())
}

fn validate_string(value: &Value, schema: &Value, path: &str) -> Result<(), ControlPlaneError> {
    let value = value
        .as_str()
        .ok_or_else(|| parameter_error(format!("{path} must be a string")))?;
    if schema
        .get("maxLength")
        .and_then(Value::as_u64)
        .is_some_and(|maximum| u64::try_from(value.chars().count()).map_or(true, |count| count > maximum))
    {
        return Err(parameter_error(format!("{path} is too long")));
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array)
        && !allowed.iter().any(|candidate| candidate.as_str() == Some(value))
    {
        return Err(parameter_error(format!("{path} is outside the allowlist")));
    }
    Ok(())
}

fn validate_integer(value: &Value, schema: &Value, path: &str) -> Result<(), ControlPlaneError> {
    let value = value
        .as_i64()
        .ok_or_else(|| parameter_error(format!("{path} must be an integer")))?;
    if schema
        .get("minimum")
        .and_then(Value::as_i64)
        .is_some_and(|minimum| value < minimum)
        || schema
            .get("maximum")
            .and_then(Value::as_i64)
            .is_some_and(|maximum| value > maximum)
    {
        return Err(parameter_error(format!("{path} is outside the allowed range")));
    }
    Ok(())
}

fn parameter_error(detail: String) -> ControlPlaneError {
    ControlPlaneError::validation("invalid_action_parameters", detail)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn embedded_catalog_has_every_closed_action_and_manual_r3() {
        let catalog = ActionCatalog::embedded().expect("catalog");
        for action in [
            ExecutionAction::ObservabilityLoggerLevelTtl,
            ExecutionAction::ProxyScaleOutOne,
            ExecutionAction::ProxyRestartOne,
            ExecutionAction::BrokerConfigPatchAllowlisted,
            ExecutionAction::TopicConfigPatchAllowlisted,
        ] {
            assert_eq!(catalog.descriptor(action).expect("descriptor").id, action.id());
        }
        assert!(matches!(
            catalog.resolve("broker.reset_master_flush_offset"),
            Ok(CatalogResolution::ManualOnly(_))
        ));
        assert!(catalog.resolve("unknown.arbitrary.request").is_err());
    }

    #[test]
    fn parameters_are_revalidated_against_server_descriptor() {
        let catalog = ActionCatalog::embedded().expect("catalog");
        let descriptor = catalog
            .descriptor(ExecutionAction::ProxyScaleOutOne)
            .expect("descriptor");
        validate_parameters(
            descriptor,
            &json!({"namespace":"default","workload":"proxy","expected_replicas":2}),
        )
        .expect("valid parameters");
        assert!(
            validate_parameters(
                descriptor,
                &json!({"namespace":"default","workload":"proxy","replicas_delta":1})
            )
            .is_err()
        );
        assert!(
            validate_parameters(
                descriptor,
                &json!({"namespace":"default","workload":"proxy","expected_replicas":1000})
            )
            .is_err()
        );
    }
}

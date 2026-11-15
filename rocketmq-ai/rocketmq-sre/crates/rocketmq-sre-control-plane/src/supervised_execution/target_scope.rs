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
use std::fmt;

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::canonical_sha256;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use super::catalog::ActionCatalog;
use super::catalog::CatalogResolution;
use crate::ControlPlaneError;
use crate::ControlPlaneRequestFailure;

const SCOPE_SCHEMA: &str = "rocketmq-sre.action-target-scope.v1";
const MAX_CONFIG_BYTES: usize = 128 * 1024;
const MAX_RULES: usize = 128;

mod lookup;
use lookup::ReadLookup;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeDocument {
    schema_version: String,
    rules: Vec<ScopeRule>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScopeRule {
    tenant_id: TenantId,
    cluster_id: ClusterId,
    action_id: String,
    descriptor_version: String,
    incident_resource: String,
    target: String,
    lookup: ReadLookup,
    security_revision: String,
}

/// Operator authorization declarations; they do not attest network or version identity.
#[derive(Clone, Default)]
pub(crate) struct ActionTargetScope {
    rules: Vec<ScopeRule>,
}

pub(super) struct TargetScopeCandidate<'a> {
    pub(super) tenant_id: TenantId,
    pub(super) cluster_id: ClusterId,
    pub(super) action_id: &'a str,
    pub(super) descriptor_version: &'a str,
    pub(super) incident_resource: Option<&'a str>,
    pub(super) target: &'a str,
    pub(super) parameters: &'a Value,
}

impl ActionTargetScope {
    pub(crate) fn parse(raw: Option<&str>) -> Result<Self, ControlPlaneError> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        if raw.len() > MAX_CONFIG_BYTES {
            return Err(invalid_config());
        }
        let document: ScopeDocument = serde_json::from_str(raw).map_err(|_| invalid_config())?;
        if document.schema_version != SCOPE_SCHEMA || document.rules.len() > MAX_RULES {
            return Err(invalid_config());
        }
        let catalog = ActionCatalog::embedded()?;
        let mut seen = BTreeSet::new();
        for rule in &document.rules {
            if rule.tenant_id.as_uuid().is_nil()
                || rule.cluster_id.as_uuid().is_nil()
                || !bounded_literal(&rule.action_id, 160)
                || !bounded_literal(&rule.descriptor_version, 64)
                || !bounded_literal(&rule.incident_resource, 512)
                || !bounded_literal(&rule.target, 512)
                || rule.target.contains("://")
                || rule.target.chars().any(char::is_whitespace)
            {
                return Err(invalid_config());
            }
            let revision = rule.security_revision.parse::<u64>().map_err(|_| invalid_config())?;
            if revision == 0 || revision.to_string() != rule.security_revision {
                return Err(invalid_config());
            }
            let CatalogResolution::Supervised(action, descriptor) =
                catalog.resolve(&rule.action_id).map_err(|_| invalid_config())?
            else {
                return Err(invalid_config());
            };
            if descriptor.version != rule.descriptor_version
                || !rule.lookup.valid_for(action)
                || !seen.insert((
                    rule.tenant_id,
                    rule.cluster_id,
                    rule.action_id.clone(),
                    rule.descriptor_version.clone(),
                    rule.incident_resource.clone(),
                    rule.target.clone(),
                    rule.lookup.clone(),
                ))
            {
                return Err(invalid_config());
            }
        }
        Ok(Self { rules: document.rules })
    }

    pub(super) fn matched_digest(
        &self,
        candidate: TargetScopeCandidate<'_>,
    ) -> Result<Option<String>, ControlPlaneRequestFailure> {
        let matched = self.rules.iter().find(|rule| {
            rule.tenant_id == candidate.tenant_id
                && rule.cluster_id == candidate.cluster_id
                && rule.action_id == candidate.action_id
                && rule.descriptor_version == candidate.descriptor_version
                && Some(rule.incident_resource.as_str()) == candidate.incident_resource
                && rule.target == candidate.target
                && rule.lookup.matches_parameters(candidate.parameters)
        });
        matched
            .map(|rule| canonical_sha256(&(SCOPE_SCHEMA, rule)))
            .transpose()
            .map_err(|source| ControlPlaneRequestFailure::from(ControlPlaneError::configuration_source(source)))
    }
}

impl fmt::Debug for ActionTargetScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActionTargetScope")
            .field("rule_count", &self.rules.len())
            .finish()
    }
}

fn bounded_literal(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value.trim() == value
        && !value
            .chars()
            .any(|value| value.is_control() || matches!(value, '*' | '?'))
}

fn invalid_config() -> ControlPlaneError {
    ControlPlaneError::configuration("action target scope configuration is invalid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rule() -> serde_json::Value {
        json!({"tenant_id": TenantId::new(), "cluster_id": ClusterId::new(),
            "action_id": "observability.logger_level_ttl.v1", "descriptor_version": "1.0.0",
            "incident_resource": "broker/broker-a", "target": "broker/127.0.0.1:10911", "security_revision": "1",
            "lookup":{"kind":"logger","component":"broker","logger":"rocketmq_broker::processor"}})
    }

    fn scope(rules: Vec<serde_json::Value>) -> Result<ActionTargetScope, ControlPlaneError> {
        ActionTargetScope::parse(Some(
            &json!({"schema_version": SCOPE_SCHEMA, "rules": rules}).to_string(),
        ))
    }

    fn matched(scope: &ActionTargetScope, value: &serde_json::Value) -> Option<String> {
        let parameters = Value::Object(
            value["lookup"]
                .as_object()
                .unwrap()
                .iter()
                .filter(|(key, _)| key.as_str() != "kind")
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        );
        scope
            .matched_digest(TargetScopeCandidate {
                tenant_id: serde_json::from_value(value["tenant_id"].clone()).unwrap(),
                cluster_id: serde_json::from_value(value["cluster_id"].clone()).unwrap(),
                action_id: value["action_id"].as_str().unwrap(),
                descriptor_version: value["descriptor_version"].as_str().unwrap(),
                incident_resource: value["incident_resource"].as_str(),
                target: value["target"].as_str().unwrap(),
                parameters: &parameters,
            })
            .unwrap()
    }

    #[test]
    fn exact_rule_is_stable_across_order_and_unrelated_changes_but_not_security_changes() {
        let original = rule();
        let other = rule();
        let digest = matched(&scope(vec![original.clone()]).unwrap(), &original).unwrap();
        assert_eq!(
            matched(&scope(vec![other.clone(), original.clone()]).unwrap(), &original),
            Some(digest.clone())
        );
        assert_eq!(
            matched(&scope(vec![original.clone(), other]).unwrap(), &original),
            Some(digest.clone())
        );
        for field in [
            "tenant_id",
            "cluster_id",
            "incident_resource",
            "target",
            "descriptor_version",
            "action_id",
        ] {
            let mut wrong = original.clone();
            wrong[field] = if field.ends_with("_id") && field != "action_id" {
                json!(uuid::Uuid::new_v4())
            } else {
                json!("different")
            };
            assert!(
                matched(&scope(vec![original.clone()]).unwrap(), &wrong).is_none(),
                "{field}"
            );
        }
        let mut changed = original.clone();
        changed["security_revision"] = json!("2");
        assert_ne!(matched(&scope(vec![changed]).unwrap(), &original), Some(digest));
        assert!(matched(&ActionTargetScope::default(), &original).is_none());
        let mut another_logger = original.clone();
        another_logger["lookup"]["logger"] = json!("rocketmq_broker::other");
        let rules = scope(vec![another_logger.clone(), original.clone()]).unwrap();
        assert_eq!(
            matched(&rules, &original),
            matched(&scope(vec![original.clone()]).unwrap(), &original)
        );
        assert_eq!(
            matched(&rules, &another_logger),
            matched(&scope(vec![another_logger.clone()]).unwrap(), &another_logger)
        );
        let mut duplicate_revision = original.clone();
        duplicate_revision["security_revision"] = json!("2");
        assert!(scope(vec![original, duplicate_revision]).is_err());
    }

    #[test]
    fn private_configuration_is_bounded_exact_and_does_not_echo_values() {
        let value = rule();
        assert!(scope(vec![value.clone(), value.clone()]).is_err());
        for lookup in [
            Value::Null,
            json!({"kind":"unknown"}),
            json!({"kind":"broker","broker":"private-broker-marker"}),
            json!({"kind":"logger","component":"broker","logger":"private-marker", "extra":true}),
            json!({"kind":"logger","component":"broker","logger":"private*marker"}),
            json!({"kind":"logger","component":"broker","logger":"x".repeat(513)}),
        ] {
            let mut bad = value.clone();
            bad["lookup"] = lookup;
            let error = scope(vec![bad]).unwrap_err().to_string();
            assert!(!error.contains("private"));
        }
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove("lookup");
        assert!(scope(vec![missing]).is_err());
        for (field, invalid) in [
            ("target", "broker/*"),
            ("target", "https://private.example"),
            ("security_revision", "01"),
            ("security_revision", "0"),
            ("descriptor_version", "unknown"),
        ] {
            let mut bad = value.clone();
            bad[field] = json!(invalid);
            let error = scope(vec![bad]).unwrap_err().to_string();
            assert!(!error.contains(invalid));
        }
        assert!(ActionTargetScope::parse(Some(&"x".repeat(MAX_CONFIG_BYTES + 1))).is_err());
        assert!(scope(vec![value.clone(); MAX_RULES + 1]).is_err());
        let debug = format!("{:?}", scope(vec![value]).unwrap());
        assert!(!debug.contains("10911"));
        assert!(debug.contains("rule_count"));
    }
}

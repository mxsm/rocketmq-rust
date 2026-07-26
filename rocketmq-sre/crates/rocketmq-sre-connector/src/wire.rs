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

use chrono::DateTime;
use chrono::Utc;
use rmcp::model::JsonObject;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;

use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::MCP_BUSINESS_SCHEMA;

const MAX_PAGE_LIMIT: u32 = 200;
const MAX_IDENTIFIER_BYTES: usize = 255;

/// Supported Phase 00 read operations. Every variant maps to a fixed,
/// non-mutating MCP tool.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EvidenceOperation {
    ClusterOverview,
    TopicList {
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        limit: Option<u32>,
        #[serde(default)]
        cursor: Option<String>,
    },
    TopicDescribe {
        topic: String,
        #[serde(default)]
        limit: Option<u32>,
        #[serde(default)]
        cursor: Option<String>,
    },
    BrokerDescribe {
        broker_name: String,
    },
    ConsumerLag {
        topic: String,
        consumer_group: String,
        #[serde(default)]
        limit: Option<u32>,
        #[serde(default)]
        cursor: Option<String>,
    },
}

impl EvidenceOperation {
    #[must_use]
    pub const fn tool_name(&self) -> &'static str {
        match self {
            Self::ClusterOverview => "rocketmq_get_cluster_overview",
            Self::TopicList { .. } => "rocketmq_list_topics",
            Self::TopicDescribe { .. } => "rocketmq_describe_topic",
            Self::BrokerDescribe { .. } => "rocketmq_describe_broker",
            Self::ConsumerLag { .. } => "rocketmq_get_consumer_lag",
        }
    }

    #[must_use]
    pub fn resource(&self) -> String {
        match self {
            Self::ClusterOverview => "cluster/overview".to_owned(),
            Self::TopicList { .. } => "topics".to_owned(),
            Self::TopicDescribe { topic, .. } => format!("topics/{topic}"),
            Self::BrokerDescribe { broker_name } => {
                format!("brokers/{broker_name}")
            }
            Self::ConsumerLag {
                topic, consumer_group, ..
            } => format!("consumer-groups/{consumer_group}/lag/{topic}"),
        }
    }

    /// Validates all caller-controlled identifiers and pagination bounds.
    ///
    /// # Errors
    ///
    /// Returns an invalid-query error for blank/oversized identifiers, invalid
    /// cursors, or page sizes outside the MCP contract.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        match self {
            Self::ClusterOverview => Ok(()),
            Self::TopicList { filter, limit, cursor } => {
                validate_optional_identifier("filter", filter.as_deref())?;
                validate_page(*limit, cursor.as_deref())
            }
            Self::TopicDescribe { topic, limit, cursor } => {
                validate_identifier("topic", topic)?;
                validate_page(*limit, cursor.as_deref())
            }
            Self::BrokerDescribe { broker_name } => validate_identifier("broker_name", broker_name),
            Self::ConsumerLag {
                topic,
                consumer_group,
                limit,
                cursor,
            } => {
                validate_identifier("topic", topic)?;
                validate_identifier("consumer_group", consumer_group)?;
                validate_page(*limit, cursor.as_deref())
            }
        }
    }

    /// Produces only the public JSON arguments of the fixed read-only tool.
    ///
    /// # Errors
    ///
    /// Returns an invalid-query error if the operation does not validate.
    pub fn arguments(&self, cluster: &str) -> Result<JsonObject, ConnectorError> {
        self.validate()?;
        let value = match self {
            Self::ClusterOverview => json!({"cluster": cluster}),
            Self::TopicList { filter, limit, cursor } => json!({
                "cluster": cluster,
                "filter": filter,
                "limit": limit,
                "cursor": cursor
            }),
            Self::TopicDescribe { topic, limit, cursor } => json!({
                "cluster": cluster,
                "topic": topic,
                "limit": limit,
                "cursor": cursor
            }),
            Self::BrokerDescribe { broker_name } => json!({
                "cluster": cluster,
                "broker_name": broker_name
            }),
            Self::ConsumerLag {
                topic,
                consumer_group,
                limit,
                cursor,
            } => json!({
                "cluster": cluster,
                "topic": topic,
                "consumer_group": consumer_group,
                "limit": limit,
                "cursor": cursor
            }),
        };
        let Value::Object(arguments) = value else {
            return Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "tool arguments did not encode as a JSON object",
            ));
        };
        Ok(arguments)
    }
}

/// Connector-owned decoder for the public `rocketmq-mcp.v2` tool envelope.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct WireEvidenceEnvelope {
    pub schema_version: String,
    pub request_id: String,
    pub cluster: String,
    pub observed_at: DateTime<Utc>,
    pub freshness_ms: u64,
    pub cache_status: String,
    pub partial: bool,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub data: Value,
}

/// Validates a structured tool result against the schema received during
/// `tools/list`, then decodes the business envelope.
///
/// # Errors
///
/// Fails closed for a missing output schema, JSON Schema validation error,
/// unexpected schema/cluster, or sensitive fields.
pub fn validate_wire_envelope(
    output_schema: &JsonObject,
    value: Value,
    expected_cluster: &str,
) -> Result<WireEvidenceEnvelope, ConnectorError> {
    let schema = Value::Object(output_schema.clone());
    let validator = jsonschema::validator_for(&schema).map_err(|_| {
        ConnectorError::capability(
            ConnectorErrorCode::SchemaDigestMismatch,
            "verified MCP output schema cannot be compiled",
        )
    })?;
    if validator.iter_errors(&value).next().is_some() {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::SchemaDigestMismatch,
            "MCP result does not conform to its verified output schema",
        ));
    }
    if contains_sensitive_field(&value) {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "MCP result contains a forbidden sensitive field",
        ));
    }
    let envelope: WireEvidenceEnvelope = serde_json::from_value(value).map_err(|_| {
        ConnectorError::capability(
            ConnectorErrorCode::UnsupportedSchemaMajor,
            "MCP result is not a rocketmq-mcp.v2 evidence envelope",
        )
    })?;
    if envelope.schema_version != MCP_BUSINESS_SCHEMA {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::UnsupportedSchemaMajor,
            format!(
                "wire schema `{}` does not equal `{MCP_BUSINESS_SCHEMA}`",
                envelope.schema_version
            ),
        ));
    }
    if envelope.cluster != expected_cluster {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::ClusterNotAllowed,
            "MCP result cluster differs from the requested cluster",
        ));
    }
    if envelope.request_id.trim().is_empty() {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "MCP result has an empty request identifier",
        ));
    }
    Ok(envelope)
}

fn validate_page(limit: Option<u32>, cursor: Option<&str>) -> Result<(), ConnectorError> {
    if limit.is_some_and(|value| !(1..=MAX_PAGE_LIMIT).contains(&value)) {
        return Err(invalid_query("page limit must be between 1 and 200"));
    }
    validate_optional_identifier("cursor", cursor)
}

fn validate_optional_identifier(name: &str, value: Option<&str>) -> Result<(), ConnectorError> {
    if let Some(value) = value {
        validate_identifier(name, value)?;
    }
    Ok(())
}

fn validate_identifier(name: &str, value: &str) -> Result<(), ConnectorError> {
    if value.trim().is_empty() || value.len() > MAX_IDENTIFIER_BYTES {
        return Err(invalid_query(format!(
            "{name} must contain between 1 and {MAX_IDENTIFIER_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid_query(format!("{name} must not contain control characters")));
    }
    Ok(())
}

fn invalid_query(detail: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorCode::InvalidEvidenceQuery, false, detail)
}

fn contains_sensitive_field(value: &Value) -> bool {
    match value {
        Value::Object(object) => object
            .iter()
            .any(|(key, value)| is_sensitive_key(key) || contains_sensitive_field(value)),
        Value::Array(values) => values.iter().any(contains_sensitive_field),
        _ => false,
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|character| !matches!(character, '_' | '-' | '.'))
        .flat_map(char::to_lowercase)
        .collect::<String>();
    normalized.contains("accesskey")
        || normalized.contains("secretkey")
        || normalized.contains("clientsecret")
        || normalized.contains("token")
        || normalized.contains("password")
        || normalized.contains("privatekey")
        || normalized.contains("tlsmaterial")
        || normalized.contains("aclconfig")
        || normalized.contains("plainaccessconfig")
        || normalized.contains("certificate")
        || normalized == "body"
        || normalized.ends_with("messagebody")
        || normalized.ends_with("rawbody")
        || normalized.ends_with("clientip")
        || matches!(
            normalized.as_str(),
            "namesrvaddr"
                | "namesrvaddrs"
                | "brokeraddr"
                | "brokeraddrs"
                | "clientaddr"
                | "remoteaddr"
                | "remoteaddress"
                | "localaddr"
                | "localaddress"
                | "storehost"
        )
}

#[cfg(test)]
mod tests {
    use serde_json::Map;

    use super::*;

    fn envelope_schema() -> JsonObject {
        let Value::Object(schema) = json!({
            "type": "object",
            "required": [
                "schema_version", "request_id", "cluster", "observed_at",
                "freshness_ms", "cache_status", "partial", "warnings", "data"
            ],
            "properties": {
                "schema_version": {"type": "string"},
                "request_id": {"type": "string"},
                "cluster": {"type": "string"},
                "observed_at": {"type": "string", "format": "date-time"},
                "freshness_ms": {"type": "integer", "minimum": 0},
                "cache_status": {"type": "string"},
                "partial": {"type": "boolean"},
                "warnings": {"type": "array", "items": {"type": "string"}},
                "data": {"type": "object"}
            }
        }) else {
            panic!("schema fixture must be an object");
        };
        schema
    }

    fn envelope(data: Value) -> Value {
        json!({
            "schema_version": "rocketmq-mcp.v2",
            "request_id": "request-1",
            "cluster": "local",
            "observed_at": "2026-07-26T08:00:00Z",
            "freshness_ms": 25,
            "cache_status": "miss",
            "partial": false,
            "warnings": [],
            "data": data
        })
    }

    #[test]
    fn validates_schema_before_decoding_envelope() {
        let decoded = validate_wire_envelope(&envelope_schema(), envelope(json!({"topic_count": 3})), "local")
            .expect("valid envelope");
        assert_eq!(decoded.freshness_ms, 25);

        let mut invalid = envelope(json!({}));
        invalid.as_object_mut().expect("object").remove("request_id");
        assert_eq!(
            validate_wire_envelope(&envelope_schema(), invalid, "local")
                .expect_err("schema mismatch")
                .code,
            ConnectorErrorCode::SchemaDigestMismatch
        );
    }

    #[test]
    fn rejects_sensitive_fields_after_schema_validation() {
        let result = validate_wire_envelope(
            &envelope_schema(),
            envelope(json!({"nested": {"access_token": "must-not-leak"}})),
            "local",
        );
        assert_eq!(
            result.expect_err("sensitive field").code,
            ConnectorErrorCode::CapabilityMismatch
        );
    }

    #[test]
    fn operations_are_fixed_read_only_calls() {
        let operation = EvidenceOperation::ConsumerLag {
            topic: "orders".to_owned(),
            consumer_group: "billing".to_owned(),
            limit: Some(50),
            cursor: None,
        };
        assert_eq!(operation.tool_name(), "rocketmq_get_consumer_lag");
        assert_eq!(operation.resource(), "consumer-groups/billing/lag/orders");
        let arguments = operation.arguments("local").expect("arguments");
        assert_eq!(arguments["cluster"], "local");

        let _schema_type_check: Map<String, Value> = envelope_schema();
    }
}

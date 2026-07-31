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
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceExposure;
use rocketmq_sre_contracts::Sensitivity;
use serde_json::Map;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use tokio::sync::Notify;

use crate::ConnectorError;
use crate::ConnectorErrorCode;

const MAX_SAFE_STRING_BYTES: usize = 4096;

/// Cancellation signal shared by the reverse channel and an in-flight source
/// query. It deliberately has no detached worker or runtime ownership.
#[derive(Clone, Debug, Default)]
pub(crate) struct CancelSignal {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl CancelSignal {
    pub(crate) fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::AcqRel) {
            self.notify.notify_waiters();
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        self.notify.notified().await;
    }
}

/// Sanitized, bounded output produced before canonical evidence capture.
#[derive(Clone, Debug)]
pub(crate) struct SourceOutput {
    pub observed_at: DateTime<Utc>,
    pub freshness_seconds: u64,
    pub partial: bool,
    pub warnings: Vec<String>,
    pub sensitivity: Sensitivity,
    pub coverage: CoverageStatus,
    pub exposure: EvidenceExposure,
    pub content: Value,
}

impl SourceOutput {
    pub(crate) fn available(content: Value, observed_at: DateTime<Utc>) -> Self {
        Self {
            observed_at,
            freshness_seconds: Utc::now().signed_duration_since(observed_at).num_seconds().max(0) as u64,
            partial: false,
            warnings: Vec::new(),
            sensitivity: Sensitivity::Internal,
            coverage: CoverageStatus::Available,
            exposure: EvidenceExposure::Unknown,
            content,
        }
    }

    pub(crate) fn with_exposure(mut self, exposure: EvidenceExposure) -> Self {
        self.exposure = exposure;
        self
    }

    pub(crate) fn missing(source: &str) -> Self {
        Self {
            observed_at: Utc::now(),
            freshness_seconds: 0,
            partial: true,
            warnings: vec!["source_unavailable".to_owned()],
            sensitivity: Sensitivity::Internal,
            coverage: CoverageStatus::Missing,
            exposure: EvidenceExposure::Unknown,
            content: serde_json::json!({
                "status": "missing",
                "source": source,
                "error_code": "source_unavailable"
            }),
        }
    }

    pub(crate) fn not_production_verified(source: &str, reason_code: &'static str) -> Self {
        Self {
            observed_at: Utc::now(),
            freshness_seconds: 0,
            partial: true,
            warnings: vec!["not_production_verified".to_owned()],
            sensitivity: Sensitivity::Internal,
            coverage: CoverageStatus::NotProductionVerified,
            exposure: EvidenceExposure::Unsupported,
            content: serde_json::json!({
                "status": "not_production_verified",
                "source": source,
                "reason_code": reason_code
            }),
        }
    }
}

/// Runs a source future under both an absolute deadline and explicit
/// cancellation.
pub(crate) async fn bounded_future<T>(
    deadline: DateTime<Utc>,
    cancel: &CancelSignal,
    future: impl Future<Output = Result<T, ConnectorError>>,
) -> Result<T, ConnectorError> {
    if cancel.is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorCode::QueryCancelled,
            false,
            "evidence query was cancelled before collection",
        ));
    }
    let remaining = deadline.signed_duration_since(Utc::now());
    let timeout = remaining
        .to_std()
        .map_err(|_| ConnectorError::new(ConnectorErrorCode::DeadlineExceeded, true, "query deadline elapsed"))?;
    if timeout.is_zero() {
        return Err(ConnectorError::new(
            ConnectorErrorCode::DeadlineExceeded,
            true,
            "query deadline elapsed",
        ));
    }

    tokio::select! {
        _ = cancel.cancelled() => Err(ConnectorError::new(
            ConnectorErrorCode::QueryCancelled,
            false,
            "evidence query was cancelled",
        )),
        result = tokio::time::timeout(timeout, future) => {
            result.map_err(|_| ConnectorError::new(
                ConnectorErrorCode::DeadlineExceeded,
                true,
                "evidence source exceeded the query deadline",
            ))?
        }
    }
}

/// Reads an HTTP response without permitting an unbounded allocation.
pub(crate) async fn bounded_response(
    mut response: reqwest::Response,
    max_bytes: usize,
    deadline: DateTime<Utc>,
    cancel: &CancelSignal,
) -> Result<Vec<u8>, ConnectorError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(ConnectorError::new(
            ConnectorErrorCode::OutputTooLarge,
            false,
            "source response exceeds the configured byte bound",
        ));
    }
    let mut body = Vec::new();
    loop {
        let chunk = bounded_future(deadline, cancel, async {
            response
                .chunk()
                .await
                .map_err(|_| ConnectorError::source("source response body is unavailable"))
        })
        .await?;
        let Some(chunk) = chunk else {
            break;
        };
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(ConnectorError::new(
                ConnectorErrorCode::OutputTooLarge,
                false,
                "source response exceeds the configured byte bound",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub(crate) fn parse_json(body: &[u8]) -> Result<Value, ConnectorError> {
    serde_json::from_slice(body).map_err(|_| ConnectorError::source("source returned invalid JSON"))
}

/// Removes sensitive fields, pseudonymizes message identifiers and bounds
/// arrays/strings. The function never propagates arbitrary warning text.
pub(crate) fn sanitize_and_bound(
    value: Value,
    max_rows: usize,
    max_bytes: usize,
    pseudonym_key: &[u8],
) -> Result<(Value, bool), ConnectorError> {
    if contains_message_body(&value) {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "evidence source returned forbidden message content",
        ));
    }
    let mut remaining_rows = max_rows;
    let mut truncated = false;
    let sanitized = sanitize_value(value, None, &mut remaining_rows, &mut truncated, pseudonym_key);
    let encoded = serde_json::to_vec(&sanitized)
        .map_err(|_| ConnectorError::source("sanitized source output cannot be encoded"))?;
    if encoded.len() > max_bytes {
        return Err(ConnectorError::new(
            ConnectorErrorCode::OutputTooLarge,
            false,
            "sanitized source output exceeds the configured byte bound",
        ));
    }
    Ok((sanitized, truncated))
}

fn sanitize_value(
    value: Value,
    key: Option<&str>,
    remaining_rows: &mut usize,
    truncated: &mut bool,
    pseudonym_key: &[u8],
) -> Value {
    match value {
        Value::Object(object) => {
            let mut sanitized = Map::new();
            for (field, value) in object {
                if is_forbidden_field(&field) {
                    *truncated = true;
                    continue;
                }
                let value = if is_pseudonymized_field(&field) {
                    pseudonymize_value(value, pseudonym_key)
                } else {
                    sanitize_value(value, Some(&field), remaining_rows, truncated, pseudonym_key)
                };
                sanitized.insert(field, value);
            }
            Value::Object(sanitized)
        }
        Value::Array(values) => {
            let take = values.len().min(*remaining_rows);
            if take < values.len() {
                *truncated = true;
            }
            *remaining_rows = remaining_rows.saturating_sub(take);
            Value::Array(
                values
                    .into_iter()
                    .take(take)
                    .map(|value| sanitize_value(value, key, remaining_rows, truncated, pseudonym_key))
                    .collect(),
            )
        }
        Value::String(value) => {
            if key.is_some_and(is_pseudonymized_field) {
                Value::String(pseudonymize_identifier(&value, pseudonym_key))
            } else if value.len() > MAX_SAFE_STRING_BYTES {
                *truncated = true;
                Value::String(truncate_utf8(&value, MAX_SAFE_STRING_BYTES))
            } else {
                Value::String(value)
            }
        }
        other => other,
    }
}

fn pseudonymize_value(value: Value, key: &[u8]) -> Value {
    match value {
        Value::String(value) => Value::String(pseudonymize_identifier(&value, key)),
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .filter_map(|value| {
                    value
                        .as_str()
                        .map(|value| Value::String(pseudonymize_identifier(value, key)))
                })
                .collect(),
        ),
        _ => Value::Null,
    }
}

pub(super) fn pseudonymize_identifier(value: &str, key: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(key);
    digest.update(b"\0");
    digest.update(value.as_bytes());
    format!("sha256:{:x}", digest.finalize())
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary = boundary.saturating_sub(1);
    }
    value[..boundary].to_owned()
}

fn normalized_key(key: &str) -> String {
    key.chars()
        .filter(|character| !matches!(character, '_' | '-' | '.'))
        .flat_map(char::to_lowercase)
        .collect()
}

fn contains_message_body(value: &Value) -> bool {
    match value {
        Value::Object(object) => object
            .iter()
            .any(|(key, value)| is_message_body_field(key) || contains_message_body(value)),
        Value::Array(values) => values.iter().any(contains_message_body),
        _ => false,
    }
}

fn is_message_body_field(key: &str) -> bool {
    let key = normalized_key(key);
    matches!(key.as_str(), "body" | "bodybase64" | "payload" | "payloadbase64")
        || key.ends_with("messagebody")
        || key.ends_with("messagebodybase64")
        || key.ends_with("rawbody")
        || key.ends_with("rawbodybase64")
        || key.ends_with("messagepayload")
        || key.ends_with("messagepayloadbase64")
        || key.ends_with("rawpayload")
        || key.ends_with("rawpayloadbase64")
}

fn is_forbidden_field(key: &str) -> bool {
    let key = normalized_key(key);
    key.contains("accesskey")
        || key.contains("secretkey")
        || key.contains("clientsecret")
        || key.contains("password")
        || key.contains("token")
        || key.contains("credential")
        || key.contains("privatekey")
        || key.contains("certificate")
        || key.contains("tlsmaterial")
        || key.contains("aclconfig")
        || is_message_body_field(&key)
        || key.ends_with("clientip")
        || key.ends_with("clientaddr")
        || key.ends_with("remoteaddr")
        || key.ends_with("remoteaddress")
        || key.ends_with("localaddr")
        || key.ends_with("localaddress")
        || key.ends_with("brokeraddr")
        || key.ends_with("brokeraddress")
        || key.ends_with("namesrvaddr")
        || key.ends_with("storehost")
}

fn is_pseudonymized_field(key: &str) -> bool {
    matches!(
        normalized_key(key).as_str(),
        "messageid" | "msgid" | "messagekey" | "messagekeys" | "keys" | "traceid"
    )
}

pub(crate) fn validate_identifier(value: &str, name: &str) -> Result<(), ConnectorError> {
    if value.is_empty()
        || value.len() > 255
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'%' | b':'))
    {
        return Err(ConnectorError::new(
            ConnectorErrorCode::InvalidEvidenceQuery,
            false,
            format!("{name} contains unsupported characters"),
        ));
    }
    Ok(())
}

pub(crate) fn require_label(labels: &BTreeSet<String>, label: &str) -> Result<(), ConnectorError> {
    if !labels.contains(label) {
        return Err(ConnectorError::new(
            ConnectorErrorCode::ClusterNotAllowed,
            false,
            "source query requires a label outside the configured allowlist",
        ));
    }
    Ok(())
}

pub(crate) fn max_duration(start: DateTime<Utc>, end: DateTime<Utc>) -> Duration {
    end.signed_duration_since(start).to_std().unwrap_or(Duration::ZERO)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn sanitizer_removes_secrets_and_pseudonymizes_message_metadata() {
        let (value, truncated) = sanitize_and_bound(
            json!({
                "access_token": "no",
                "client_ip": "10.0.0.1",
                "message_id": "message-a",
                "keys": ["order-1"],
                "rows": [1, 2, 3]
            }),
            2,
            4096,
            b"tenant-key",
        )
        .expect("sanitized");
        assert!(truncated);
        assert!(value.get("access_token").is_none());
        assert!(value.get("client_ip").is_none());
        assert!(
            value["message_id"]
                .as_str()
                .is_some_and(|value| value.starts_with("sha256:"))
        );
        assert_eq!(value["rows"].as_array().map(Vec::len), Some(2));
    }

    #[test]
    fn sanitizer_rejects_every_message_body_alias_before_bounding() {
        for key in [
            "body",
            "body_base64",
            "messageBody",
            "message_body_base64",
            "decoded_message_body",
            "compressed_message_body_base64",
            "raw-body",
            "payload",
            "payloadBase64",
            "message_payload",
            "raw_payload_base64",
        ] {
            let mut message = Map::new();
            message.insert(key.to_owned(), Value::String("must-never-enter-evidence".to_owned()));
            let value = json!({
                "rows": [
                    {
                        "metadata": Value::Object(message)
                    }
                ]
            });
            let error = sanitize_and_bound(value, 0, 4096, b"tenant-key").expect_err(key);
            assert_eq!(error.code, ConnectorErrorCode::CapabilityMismatch, "{key}");
        }
    }

    #[tokio::test]
    async fn cancellation_wins_over_a_pending_source() {
        let cancel = CancelSignal::default();
        cancel.cancel();
        let result = bounded_future(Utc::now() + chrono::Duration::seconds(1), &cancel, async {
            std::future::pending::<Result<(), ConnectorError>>().await
        })
        .await;
        assert_eq!(result.expect_err("cancelled").code, ConnectorErrorCode::QueryCancelled);
    }
}

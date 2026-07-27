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

use chrono::DateTime;
use chrono::Utc;
use reqwest::Client;
use url::Url;

use super::common::CancelSignal;
use super::common::SourceOutput;
use super::common::bounded_future;
use super::common::bounded_response;
use super::common::parse_json;
use super::common::require_label;
use super::common::validate_identifier;
use crate::ConnectorError;

pub(crate) struct PrometheusSource {
    client: Client,
    base_url: Option<Url>,
    label_allowlist: BTreeSet<String>,
}

impl PrometheusSource {
    pub(crate) fn new(client: Client, base_url: Option<Url>, label_allowlist: BTreeSet<String>) -> Self {
        Self {
            client,
            base_url,
            label_allowlist,
        }
    }

    pub(crate) fn configured(&self) -> bool {
        self.base_url.is_some()
    }

    pub(crate) async fn query(
        &self,
        cluster: &str,
        resource: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        max_bytes: usize,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        self.query_with_matchers(cluster, resource, &[], start, end, max_bytes, deadline, cancel)
            .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Prometheus query bounds and fixed label matchers remain explicit"
    )]
    pub(crate) async fn query_with_matchers(
        &self,
        cluster: &str,
        resource: &str,
        matchers: &[(String, String)],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        max_bytes: usize,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        let base_url = self
            .base_url
            .as_ref()
            .ok_or_else(|| ConnectorError::source("Prometheus source is not configured"))?;
        require_label(&self.label_allowlist, "cluster")?;
        let metric = resource
            .strip_prefix("metrics/")
            .or_else(|| resource.strip_prefix("prometheus/"))
            .unwrap_or(resource);
        validate_metric(metric)?;
        validate_identifier(cluster, "cluster")?;
        let mut selector = vec![format!(r#"cluster="{cluster}""#)];
        for (label, value) in matchers {
            validate_label(label)?;
            require_label(&self.label_allowlist, label)?;
            validate_identifier(value, "Prometheus label value")?;
            if label == "cluster" {
                return Err(ConnectorError::new(
                    crate::ConnectorErrorCode::InvalidEvidenceQuery,
                    false,
                    "canonical Prometheus query cannot override the cluster matcher",
                ));
            }
            selector.push(format!(r#"{label}="{value}""#));
        }
        let endpoint = base_url
            .join("api/v1/query_range")
            .map_err(|_| ConnectorError::configuration("Prometheus query URL cannot be constructed"))?;
        let expression = format!("{metric}{{{}}}", selector.join(","));
        let request = self.client.get(endpoint).query(&[
            ("query", expression),
            ("start", start.timestamp().to_string()),
            ("end", end.timestamp().to_string()),
            ("step", query_step(start, end).to_string()),
        ]);
        let response = bounded_future(deadline, cancel, async {
            request
                .send()
                .await
                .map_err(|_| ConnectorError::source("Prometheus query failed"))
        })
        .await?;
        if !response.status().is_success() {
            return Err(ConnectorError::source("Prometheus rejected the bounded query"));
        }
        let body = bounded_response(response, max_bytes, deadline, cancel).await?;
        let value = parse_json(&body)?;
        if value.get("status").and_then(serde_json::Value::as_str) != Some("success") {
            return Err(ConnectorError::source("Prometheus returned an unsuccessful response"));
        }
        Ok(SourceOutput::available(value, end))
    }
}

fn validate_metric(metric: &str) -> Result<(), ConnectorError> {
    if metric.is_empty()
        || metric.len() > 255
        || !metric
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'.'))
    {
        return Err(ConnectorError::new(
            crate::ConnectorErrorCode::InvalidEvidenceQuery,
            false,
            "Prometheus metric name is invalid",
        ));
    }
    Ok(())
}

fn validate_label(label: &str) -> Result<(), ConnectorError> {
    if label.is_empty()
        || label.len() > 255
        || !label
            .bytes()
            .enumerate()
            .all(|(index, byte)| byte.is_ascii_alphabetic() || byte == b'_' || (index > 0 && byte.is_ascii_digit()))
    {
        return Err(ConnectorError::new(
            crate::ConnectorErrorCode::InvalidEvidenceQuery,
            false,
            "Prometheus label name is invalid",
        ));
    }
    Ok(())
}

fn query_step(start: DateTime<Utc>, end: DateTime<Utc>) -> i64 {
    let seconds = end.signed_duration_since(start).num_seconds().max(1);
    (seconds / 240).clamp(1, 300)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_query_has_a_fixed_cluster_matcher() {
        assert!(validate_metric("rocketmq_broker_up").is_ok());
        assert!(validate_metric("up or vector(1)").is_err());
        assert!(validate_label("node_id").is_ok());
        assert!(validate_label("service.name").is_err());
        let start = Utc::now();
        assert_eq!(query_step(start, start + chrono::Duration::seconds(600)), 2);
    }
}

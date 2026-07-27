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

use chrono::DateTime;
use chrono::Utc;
use reqwest::Client;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceExposure;
use serde_json::Value;
use serde_json::json;
use url::Url;

use super::common::CancelSignal;
use super::common::SourceOutput;
use super::common::bounded_future;
use super::common::bounded_response;
use super::common::parse_json;
use super::common::pseudonymize_identifier;
use super::common::validate_identifier;
use crate::ConnectorError;
use crate::ConnectorErrorCode;

const ALERT_LABELS: [&str; 6] = [
    "alertname",
    "severity",
    "cluster",
    "namespace",
    "component",
    "resource_kind",
];

pub(crate) struct AlertmanagerSource {
    client: Client,
    base_url: Option<Url>,
    pseudonymization_key: Vec<u8>,
}

impl AlertmanagerSource {
    pub(crate) fn new(client: Client, base_url: Option<Url>, pseudonymization_key: &[u8]) -> Self {
        Self {
            client,
            base_url,
            pseudonymization_key: pseudonymization_key.to_vec(),
        }
    }

    pub(crate) fn configured(&self) -> bool {
        self.base_url.is_some()
    }

    pub(crate) async fn query(
        &self,
        cluster: &str,
        resource: &str,
        max_rows: usize,
        max_bytes: usize,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        validate_identifier(cluster, "cluster")?;
        if !matches!(resource, "alerts" | "alertmanager/alerts" | "active-alerts") {
            return Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "Alertmanager source supports only active alert evidence",
            ));
        }
        let base_url = self
            .base_url
            .as_ref()
            .ok_or_else(|| ConnectorError::source("Alertmanager source is not configured"))?;
        let endpoint = base_url
            .join("api/v2/alerts")
            .map_err(|_| ConnectorError::configuration("Alertmanager URL cannot be constructed"))?;
        let response = bounded_future(deadline, cancel, async {
            self.client
                .get(endpoint)
                .query(&[
                    ("active", "true"),
                    ("silenced", "true"),
                    ("inhibited", "true"),
                    ("unprocessed", "true"),
                ])
                .query(&[("filter", format!(r#"cluster="{cluster}""#))])
                .send()
                .await
                .map_err(|_| ConnectorError::source("Alertmanager query failed"))
        })
        .await?;
        if !response.status().is_success() {
            return Err(ConnectorError::source("Alertmanager rejected the bounded query"));
        }
        let body = bounded_response(response, max_bytes, deadline, cancel).await?;
        let raw = parse_json(&body)?;
        let alerts = raw
            .as_array()
            .ok_or_else(|| ConnectorError::source("Alertmanager response is invalid"))?;
        let mut items = Vec::with_capacity(alerts.len().min(max_rows));
        for alert in alerts.iter().take(max_rows) {
            let labels = filtered_labels(alert.get("labels"));
            if !matches_cluster(&labels, cluster) {
                continue;
            }
            let fingerprint = alert
                .get("fingerprint")
                .and_then(Value::as_str)
                .map(|value| pseudonymize_identifier(value, &self.pseudonymization_key));
            items.push(json!({
                "labels": labels,
                "fingerprint": fingerprint,
                "starts_at": alert.get("startsAt"),
                "ends_at": alert.get("endsAt"),
                "updated_at": alert.get("updatedAt"),
                "status": {
                    "state": alert.pointer("/status/state"),
                    "silenced_by_count": alert
                        .pointer("/status/silencedBy")
                        .and_then(Value::as_array)
                        .map(Vec::len),
                    "inhibited_by_count": alert
                        .pointer("/status/inhibitedBy")
                        .and_then(Value::as_array)
                        .map(Vec::len)
                }
            }));
        }
        let observed_at = Utc::now();
        let mut output = SourceOutput::available(
            json!({
                "schema_version": "rocketmq.alertmanager-evidence.v1",
                "observed_at": observed_at,
                "alerts": items
            }),
            observed_at,
        )
        .with_exposure(EvidenceExposure::AlertmanagerApi);
        if alerts.len() > max_rows {
            output.partial = true;
            output.coverage = CoverageStatus::Partial;
            output.warnings.push("alertmanager_rows_bounded".to_owned());
        }
        Ok(output)
    }
}

fn matches_cluster(labels: &BTreeMap<String, String>, cluster: &str) -> bool {
    labels.get("cluster").map(String::as_str) == Some(cluster)
}

fn filtered_labels(raw: Option<&Value>) -> BTreeMap<String, String> {
    let allowed = BTreeSet::from(ALERT_LABELS);
    raw.and_then(Value::as_object)
        .map(|labels| {
            labels
                .iter()
                .filter(|(key, _)| allowed.contains(key.as_str()))
                .filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_owned())))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_label_filter_drops_routing_and_secret_labels() {
        let labels = filtered_labels(Some(&json!({
            "alertname": "BrokerDown",
            "cluster": "local",
            "pagerduty_token": "secret",
            "client_ip": "10.0.0.2"
        })));
        assert_eq!(labels.get("alertname").map(String::as_str), Some("BrokerDown"));
        assert!(!labels.contains_key("pagerduty_token"));
        assert!(!labels.contains_key("client_ip"));
        assert!(matches_cluster(&labels, "local"));
        assert!(!matches_cluster(&labels, "other"));
        assert!(!matches_cluster(&BTreeMap::new(), "local"));
    }
}

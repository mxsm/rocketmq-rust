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

pub(crate) struct LokiSource {
    client: Client,
    base_url: Option<Url>,
    label_allowlist: BTreeSet<String>,
}

impl LokiSource {
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

    #[allow(
        clippy::too_many_arguments,
        reason = "the source boundary keeps every query bound explicit"
    )]
    pub(crate) async fn query(
        &self,
        cluster: &str,
        resource: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        max_rows: usize,
        max_bytes: usize,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        let base_url = self
            .base_url
            .as_ref()
            .ok_or_else(|| ConnectorError::source("Loki source is not configured"))?;
        require_label(&self.label_allowlist, "cluster")?;
        require_label(&self.label_allowlist, "component")?;
        validate_identifier(cluster, "cluster")?;
        let component = resource
            .strip_prefix("logs/")
            .or_else(|| resource.strip_prefix("loki/"))
            .unwrap_or(resource);
        validate_identifier(component, "component")?;
        let endpoint = base_url
            .join("loki/api/v1/query_range")
            .map_err(|_| ConnectorError::configuration("Loki query URL cannot be constructed"))?;
        let expression = format!(r#"{{cluster="{cluster}",component="{component}"}}"#);
        let request = self.client.get(endpoint).query(&[
            ("query", expression),
            (
                "start",
                start
                    .timestamp_nanos_opt()
                    .unwrap_or(start.timestamp_micros().saturating_mul(1_000))
                    .to_string(),
            ),
            (
                "end",
                end.timestamp_nanos_opt()
                    .unwrap_or(end.timestamp_micros().saturating_mul(1_000))
                    .to_string(),
            ),
            ("limit", max_rows.to_string()),
            ("direction", "backward".to_owned()),
        ]);
        let response = bounded_future(deadline, cancel, async {
            request
                .send()
                .await
                .map_err(|_| ConnectorError::source("Loki query failed"))
        })
        .await?;
        if !response.status().is_success() {
            return Err(ConnectorError::source("Loki rejected the bounded query"));
        }
        let body = bounded_response(response, max_bytes, deadline, cancel).await?;
        let value = parse_json(&body)?;
        if value.get("status").and_then(serde_json::Value::as_str) != Some("success") {
            return Err(ConnectorError::source("Loki returned an unsuccessful response"));
        }
        let mut output = SourceOutput::available(value, end);
        output.sensitivity = rocketmq_sre_contracts::Sensitivity::Confidential;
        Ok(output)
    }
}

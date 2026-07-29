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

pub(crate) struct TempoSource {
    client: Client,
    base_url: Option<Url>,
    label_allowlist: BTreeSet<String>,
}

impl TempoSource {
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
            .ok_or_else(|| ConnectorError::source("Tempo source is not configured"))?;
        require_label(&self.label_allowlist, "rocketmq.cluster")?;
        require_label(&self.label_allowlist, "service.name")?;
        validate_identifier(cluster, "cluster")?;
        let service = resource
            .strip_prefix("traces/service/")
            .or_else(|| resource.strip_prefix("tempo/service/"))
            .ok_or_else(|| {
                ConnectorError::new(
                    crate::ConnectorErrorCode::InvalidEvidenceQuery,
                    false,
                    "Tempo queries must use a bounded service name, not a raw trace identifier",
                )
            })?;
        validate_identifier(service, "service name")?;
        let endpoint = base_url
            .join("api/search")
            .map_err(|_| ConnectorError::configuration("Tempo query URL cannot be constructed"))?;
        let traceql = trace_selector(cluster, service);
        let request = self.client.get(endpoint).query(&[
            ("q", traceql),
            ("start", start.timestamp().to_string()),
            ("end", end.timestamp().to_string()),
            ("limit", max_rows.to_string()),
        ]);
        let response = bounded_future(deadline, cancel, async {
            request
                .send()
                .await
                .map_err(|_| ConnectorError::source("Tempo query failed"))
        })
        .await?;
        if !response.status().is_success() {
            return Err(ConnectorError::source("Tempo rejected the bounded query"));
        }
        let body = bounded_response(response, max_bytes, deadline, cancel).await?;
        let value = parse_json(&body)?;
        let mut output = SourceOutput::available(value, end);
        output.sensitivity = rocketmq_sre_contracts::Sensitivity::Confidential;
        Ok(output)
    }
}

fn trace_selector(cluster: &str, service: &str) -> String {
    format!(r#"{{ resource.service.name = "{service}" && resource.rocketmq.cluster = "{cluster}" }}"#)
}

#[cfg(test)]
mod tests {
    use super::trace_selector;

    #[test]
    fn selector_requires_cluster_and_fixed_service_identity() {
        assert_eq!(
            trace_selector("cluster-a", "rocketmq-controller"),
            r#"{ resource.service.name = "rocketmq-controller" && resource.rocketmq.cluster = "cluster-a" }"#
        );
    }
}

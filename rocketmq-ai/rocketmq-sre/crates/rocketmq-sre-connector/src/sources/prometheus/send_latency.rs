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

mod output;
mod proof;
#[cfg(test)]
mod tests;

use super::*;
use crate::config::SendLatencyPolicy;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_core::diagnostics::packs::{LATENCY_FAMILIES, assess_latency_histograms};

use proof::{Budget, RawSet, ReadIssue, select_histograms};

/// One admitted canonical read, with at most three physical HTTP requests.
pub(crate) struct SendLatencyRequest<'a> {
    pub(crate) external_cluster: &'a str,
    pub(crate) query: &'a EvidenceQuery,
    pub(crate) policy: Option<&'a SendLatencyPolicy>,
    pub(crate) max_rows: usize,
    pub(crate) max_bytes: usize,
    pub(crate) deadline: DateTime<Utc>,
    pub(crate) cancel: &'a CancelSignal,
}

impl PrometheusSource {
    pub(crate) async fn query_send_latency(
        &self,
        request: SendLatencyRequest<'_>,
    ) -> Result<SourceOutput, ConnectorError> {
        let started = Utc::now();
        let topic = request
            .query
            .resource
            .strip_prefix("send-latency/")
            .ok_or_else(|| ConnectorError::source("canonical send latency resource is invalid"))?;
        validate_identifier(topic, "topic")?;
        validate_identifier(request.external_cluster, "cluster")?;
        for label in [self.cluster_label.as_str(), "topic"] {
            require_label(&self.label_allowlist, label)?;
        }
        let base = self
            .base_url
            .as_ref()
            .ok_or_else(|| ConnectorError::source("Prometheus source is not configured"))?;
        let endpoint = base
            .join("api/v1/query")
            .map_err(ConnectorError::configuration_source)?;
        let range = &request.query.time_range;
        if range
            .end
            .signed_duration_since(range.start)
            .to_std()
            .map_or(true, |duration| duration > self.max_time_range)
        {
            return Err(ConnectorError::new(
                ConnectorFailure::InvalidEvidenceQuery,
                false,
                "send latency window exceeds the configured source bound",
            ));
        }
        let mut budget = Budget::new(request.max_rows, request.max_bytes);
        let result = if topic == "other" {
            Err(ReadIssue::AmbiguousTopic)
        } else if range.end > started {
            Err(ReadIssue::FutureWindow)
        } else if range.end.timestamp_millis() <= range.start.timestamp_millis() {
            Err(ReadIssue::UnsupportedWindow)
        } else {
            self.read_latency_proof(&request, &endpoint, topic, &mut budget).await?
        };
        if matches!(result, Err(ReadIssue::Scope)) {
            return Err(ConnectorError::capability(
                ConnectorFailure::CapabilityMismatch,
                "Prometheus histogram differs from the authorized cluster or topic",
            ));
        }
        Ok(output::project(&request, topic, started, Utc::now(), &budget, result))
    }

    async fn read_latency_proof(
        &self,
        request: &SendLatencyRequest<'_>,
        endpoint: &Url,
        topic: &str,
        budget: &mut Budget,
    ) -> Result<Result<output::WindowProof, ReadIssue>, ConnectorError> {
        let start = request.query.time_range.start.timestamp_millis();
        let end = request.query.time_range.end.timestamp_millis();
        let Some(width) = end.checked_sub(start).filter(|width| *width > 0) else {
            return Ok(Err(ReadIssue::UnsupportedWindow));
        };
        let selector = format!(
            r#"{}="{}",topic="{}""#,
            self.cluster_label, request.external_cluster, topic
        );
        let raw_expression = format!(
            r#"{{__name__=~"{}",{selector}}}[{width}ms]"#,
            LATENCY_FAMILIES.join("|")
        );
        let first = self
            .read_latency_http(request, endpoint, &raw_expression, budget)
            .await?;
        let raw = match RawSet::parse(&first, &self.cluster_label, request.external_cluster, topic, budget) {
            Ok(raw) => raw,
            Err(issue) => return Ok(Err(issue)),
        };
        let Some(family) = raw.selected_family() else {
            return Ok(Err(ReadIssue::MissingHistogram));
        };
        let expression = format!("increase({family}{{{selector}}}[{width}ms])");
        let increases = self.read_latency_http(request, endpoint, &expression, budget).await?;
        let increases = match proof::parse_increases(
            &increases,
            &self.cluster_label,
            request.external_cluster,
            topic,
            end,
            budget,
        ) {
            Ok(rows) => rows,
            Err(issue) => return Ok(Err(issue)),
        };
        let repeated = self
            .read_latency_http(request, endpoint, &raw_expression, budget)
            .await?;
        let repeated = match RawSet::parse(&repeated, &self.cluster_label, request.external_cluster, topic, budget) {
            Ok(raw) => raw,
            Err(issue) => return Ok(Err(issue)),
        };
        if repeated != raw {
            return Ok(Err(ReadIssue::IncoherentReads));
        }
        let histograms = match select_histograms(&raw, family, increases) {
            Ok(rows) => rows,
            Err(issue) => return Ok(Err(issue)),
        };
        let assessment = match assess_latency_histograms(start, end, &histograms) {
            Ok(assessment) => assessment,
            Err(issue) => return Ok(Err(ReadIssue::Histogram(issue))),
        };
        Ok(Ok(output::WindowProof {
            family,
            histograms,
            assessment,
        }))
    }

    async fn read_latency_http(
        &self,
        request: &SendLatencyRequest<'_>,
        endpoint: &Url,
        expression: &str,
        budget: &mut Budget,
    ) -> Result<Value, ConnectorError> {
        // Each await observes the original absolute deadline and cancel signal.
        // No retry or per-read timeout can extend this canonical operation.
        let remaining_millis = request.deadline.signed_duration_since(Utc::now()).num_milliseconds();
        if remaining_millis <= 0 {
            return Err(ConnectorError::new(
                ConnectorFailure::DeadlineExceeded,
                true,
                "histogram query deadline elapsed",
            ));
        }
        let query = self.client.get(endpoint.clone()).query(&[
            ("query", expression.to_owned()),
            ("timeout", format!("{remaining_millis}ms")),
            ("limit", budget.series_limit().to_string()),
            (
                "time",
                request
                    .query
                    .time_range
                    .end
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            ),
        ]);
        budget.requests += 1;
        let response = bounded_future(request.deadline, request.cancel, async {
            query.send().await.map_err(ConnectorError::source_error)
        })
        .await?;
        if !response.status().is_success() {
            return Err(ConnectorError::source("Prometheus rejected the closed histogram read"));
        }
        let body = bounded_response(response, budget.bytes_remaining, request.deadline, request.cancel).await?;
        budget.bytes_remaining -= body.len();
        budget.bytes_read += body.len();
        parse_json(&body)
    }
}

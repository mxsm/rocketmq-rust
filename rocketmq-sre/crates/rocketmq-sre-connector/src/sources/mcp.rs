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

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use serde_json::Value;

use super::common::CancelSignal;
use super::common::SourceOutput;
use super::common::bounded_future;
use crate::ConnectorError;
use crate::EvidenceOperation;
use crate::mcp::McpGateway;

pub(crate) struct McpSource<G> {
    gateway: Arc<G>,
}

impl<G> McpSource<G>
where
    G: McpGateway,
{
    pub(crate) fn new(gateway: Arc<G>) -> Self {
        Self { gateway }
    }

    pub(crate) async fn query(
        &self,
        cluster: &str,
        operation: &EvidenceOperation,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        let wire = bounded_future(deadline, cancel, self.gateway.query(cluster, operation)).await?;
        let mut output = SourceOutput::available(wire.data, wire.observed_at);
        output.freshness_seconds = wire.freshness_ms.saturating_add(999) / 1000;
        output.partial = wire.partial;
        if !wire.warnings.is_empty() {
            output.warnings.push("rocketmq_mcp_source_warning".to_owned());
        }
        if wire.partial {
            output.warnings.push("rocketmq_mcp_partial_response".to_owned());
            output.coverage = rocketmq_sre_contracts::CoverageStatus::Partial;
        }
        Ok(output)
    }

    pub(crate) async fn system_resource(
        &self,
        uri: &str,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        let value = bounded_future(deadline, cancel, self.gateway.read_system_resource(uri)).await?;
        let observed_at = value
            .get("observed_at")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<DateTime<Utc>>().ok())
            .unwrap_or_else(Utc::now);
        Ok(SourceOutput::available(value, observed_at))
    }
}

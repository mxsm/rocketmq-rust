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

use super::common::CancelSignal;
use super::common::SourceOutput;
use super::mcp::McpSource;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::mcp::McpGateway;

const RUNTIME_RESOURCE_URI: &str = "rocketmq://system/runtime/v1";
const OBSERVABILITY_RESOURCE_URI: &str = "rocketmq://system/observability/v1";

pub(crate) struct RuntimeSource;

impl RuntimeSource {
    pub(crate) async fn query<G>(
        mcp: &McpSource<G>,
        resource: &str,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError>
    where
        G: McpGateway,
    {
        let uri = match resource {
            "runtime" | "runtime/diagnostics" | RUNTIME_RESOURCE_URI => RUNTIME_RESOURCE_URI,
            "runtime/observability" | "observability" | OBSERVABILITY_RESOURCE_URI => OBSERVABILITY_RESOURCE_URI,
            _ => {
                return Err(ConnectorError::new(
                    ConnectorErrorCode::InvalidEvidenceQuery,
                    false,
                    "runtime source supports only the Phase 00 diagnostics contracts",
                ));
            }
        };
        mcp.system_resource(uri, deadline, cancel).await
    }
}

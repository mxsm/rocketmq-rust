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
use serde_json::json;

use super::admin_query::AdminQuerySource;
use super::common::CancelSignal;
use super::common::SourceOutput;
use super::common::validate_identifier;
use super::mcp::McpSource;
use crate::ConnectorError;
use crate::EvidenceOperation;
use crate::mcp::McpGateway;

pub(crate) struct TopologySource;

impl TopologySource {
    pub(crate) async fn query<G>(
        mcp: &McpSource<G>,
        admin: &AdminQuerySource,
        cluster: &str,
        resource: &str,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError>
    where
        G: McpGateway,
    {
        let (operation, admin_resource) = if matches!(resource, "topology" | "topology/cluster" | "cluster/topology") {
            (EvidenceOperation::ClusterOverview, "admin/brokers".to_owned())
        } else if let Some(topic) = resource.strip_prefix("topology/topic/") {
            validate_identifier(topic, "topic")?;
            (
                EvidenceOperation::TopicDescribe {
                    topic: topic.to_owned(),
                    limit: Some(200),
                    cursor: None,
                },
                format!("admin/topic-route/{topic}"),
            )
        } else {
            return Err(ConnectorError::new(
                crate::ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "topology source supports cluster or topic topology",
            ));
        };

        let mut output = match mcp.query(cluster, &operation, deadline, cancel).await {
            Ok(output) => output,
            Err(error) if error.code == crate::ConnectorErrorCode::SourceUnavailable && admin.configured() => {
                admin.query(cluster, &admin_resource, deadline, cancel).await?
            }
            Err(error) => return Err(error),
        };
        output.content = json!({
            "topology_basis": output.content,
            "source_preference": "mcp_first"
        });
        Ok(output)
    }
}

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

use std::sync::Arc;

use super::CanonicalRead;
use super::ReadAdapter;
use super::ReadAdapterKind;
use super::ReadContext;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::mcp::McpGateway;
use crate::sources::McpSource;
use crate::sources::SourceOutput;

pub(crate) struct McpReadAdapter<G> {
    source: McpSource<G>,
}

impl<G> McpReadAdapter<G>
where
    G: McpGateway,
{
    pub(crate) fn new(gateway: Arc<G>) -> Self {
        Self {
            source: McpSource::new(gateway),
        }
    }
}

impl<G> ReadAdapter for McpReadAdapter<G>
where
    G: McpGateway,
{
    fn kind(&self) -> ReadAdapterKind {
        ReadAdapterKind::Mcp
    }

    async fn read(
        &self,
        context: &ReadContext<'_>,
        request: &CanonicalRead<'_>,
    ) -> Result<SourceOutput, ConnectorError> {
        match request {
            CanonicalRead::Mcp(operation) => {
                self.source
                    .query(context.external_cluster, operation, context.deadline, context.cancel)
                    .await
            }
            CanonicalRead::McpSystemResource(uri) => {
                self.source.system_resource(uri, context.deadline, context.cancel).await
            }
            CanonicalRead::Admin(_)
            | CanonicalRead::AdminProducerConnections { .. }
            | CanonicalRead::AdminConsumerConnections { .. } => Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "Admin request cannot be routed to the MCP read adapter",
            )
            .with_correlation_id(context.correlation_id)),
        }
    }
}

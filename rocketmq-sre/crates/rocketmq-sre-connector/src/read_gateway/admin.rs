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

use rocketmq_runtime::ChildServiceContext;

use super::CanonicalRead;
use super::ReadAdapter;
use super::ReadAdapterKind;
use super::ReadContext;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::config::AdminSourceConfig;
use crate::sources::AdminQuerySource;
use crate::sources::SourceOutput;

pub(crate) struct AdminReadAdapter {
    source: AdminQuerySource,
}

impl AdminReadAdapter {
    pub(crate) fn new(config: Option<AdminSourceConfig>) -> Self {
        Self {
            source: AdminQuerySource::new(config),
        }
    }

    pub(crate) fn configured(&self) -> bool {
        self.source.configured()
    }

    pub(crate) async fn initialize(&self, context: ChildServiceContext) -> Result<(), ConnectorError> {
        self.source.start(context).await
    }

    pub(crate) async fn shutdown(&self) {
        self.source.shutdown().await;
    }
}

impl ReadAdapter for AdminReadAdapter {
    fn kind(&self) -> ReadAdapterKind {
        ReadAdapterKind::Admin
    }

    async fn read(
        &self,
        context: &ReadContext<'_>,
        request: &CanonicalRead<'_>,
    ) -> Result<SourceOutput, ConnectorError> {
        match request {
            CanonicalRead::Admin(resource) => {
                self.source
                    .query(context.external_cluster, resource, context.deadline, context.cancel)
                    .await
            }
            CanonicalRead::AdminProducerConnections { max_rows } => {
                self.source
                    .query_producer_connections(context.external_cluster, *max_rows, context.deadline, context.cancel)
                    .await
            }
            CanonicalRead::AdminConsumerConnections {
                consumer_group,
                max_rows,
            } => {
                self.source
                    .query_consumer_connections(
                        context.external_cluster,
                        consumer_group,
                        *max_rows,
                        context.deadline,
                        context.cancel,
                    )
                    .await
            }
            CanonicalRead::Mcp(_) | CanonicalRead::McpSystemResource(_) => Err(ConnectorError::new(
                ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "MCP request cannot be routed to the Admin read adapter",
            )
            .with_correlation_id(context.correlation_id)),
        }
    }
}

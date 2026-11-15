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
use crate::ConnectorFailure;
use crate::config::{ConnectorConfig, NamesrvRouteConfig, SourceLimits};
use crate::sources::AdminQuerySource;
use crate::sources::SourceOutput;

pub(crate) struct AdminReadAdapter {
    source: AdminQuerySource,
    routes: std::collections::BTreeMap<rocketmq_sre_contracts::ClusterId, NamesrvRouteConfig>,
    limits: SourceLimits,
}

impl AdminReadAdapter {
    pub(crate) fn new(config: &ConnectorConfig) -> Self {
        Self {
            source: AdminQuerySource::new(config.admin_source.clone()),
            routes: config.namesrv_routes.clone(),
            limits: config.source_limits.clone(),
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
                if let Some(topic) = resource.strip_prefix("namesrv-route/") {
                    let Some(config) = self.routes.get(&context.cluster_id) else {
                        return Ok(SourceOutput::not_production_verified(
                            "admin-query",
                            "namesrv_route_peers_unconfigured",
                        ));
                    };
                    return self
                        .source
                        .query_namesrv_route(context, topic, config, &self.limits)
                        .await;
                }
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
                ConnectorFailure::InvalidEvidenceQuery,
                false,
                "MCP request cannot be routed to the Admin read adapter",
            )
            .with_correlation_id(context.correlation_id)),
        }
    }
}

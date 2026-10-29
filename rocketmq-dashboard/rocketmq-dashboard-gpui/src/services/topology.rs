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

use super::{AppServices, ServiceFuture};
use crate::{
    infrastructure::admin_provider::{GpuiAdminProvider, ProviderFailure, ProviderFailureCode},
    state::{UiError, UiErrorCode},
};
use rocketmq_dashboard_common::*;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

#[derive(Clone, Debug)]
pub(crate) struct TopologyReadRequest {
    pub context: TopologyContext,
    pub source: TopologySource,
    pub observed_at_ms: u64,
    pub nameservers: Vec<String>,
}
pub(crate) trait TopologyBackend: Send + Sync {
    fn layer(&self, request: TopologyReadRequest) -> ServiceFuture<'_, Result<TopologyLayer, UiError>>;
}
pub(crate) struct RealTopologyBackend {
    provider: Option<Arc<GpuiAdminProvider>>,
}
impl RealTopologyBackend {
    pub fn new(provider: Arc<GpuiAdminProvider>) -> Arc<Self> {
        Arc::new(Self {
            provider: Some(provider),
        })
    }
    pub fn unavailable() -> Arc<Self> {
        Arc::new(Self { provider: None })
    }
}
impl TopologyBackend for RealTopologyBackend {
    fn layer(&self, request: TopologyReadRequest) -> ServiceFuture<'_, Result<TopologyLayer, UiError>> {
        Box::pin(async move {
            let provider = self.provider.as_ref().ok_or_else(|| {
                UiError::new(
                    "Topology requires a configured connection.",
                    UiErrorCode::Configuration,
                    false,
                )
            })?;
            let result = if request.source == TopologySource::Nameservers {
                provider
                    .topology_nameservers(request.context.revision, request.nameservers, request.observed_at_ms)
                    .await
            } else {
                provider
                    .topology_layer(request.context.revision, request.source, request.observed_at_ms)
                    .await
            };
            result.map_err(provider_error)
        })
    }
}
fn provider_error(error: ProviderFailure) -> UiError {
    let code = match error.code() {
        ProviderFailureCode::NotConfigured => UiErrorCode::Configuration,
        ProviderFailureCode::Authentication => UiErrorCode::Authentication,
        ProviderFailureCode::Unavailable
        | ProviderFailureCode::Cancelled
        | ProviderFailureCode::StaleRevision
        | ProviderFailureCode::Runtime => UiErrorCode::Connection,
    };
    let retryable = error.is_retryable();
    match error.into_operational() {
        Some(source) => UiError::caused_by("Topology source is unavailable.", code, retryable, source),
        None => UiError::new("Topology source is unavailable.", code, retryable),
    }
}
fn failure(error: &UiError) -> TopologyFailureCode {
    match error.code() {
        UiErrorCode::Configuration => TopologyFailureCode::NotConfigured,
        UiErrorCode::Authentication => TopologyFailureCode::Authentication,
        UiErrorCode::Connection => TopologyFailureCode::Connection,
        UiErrorCode::Validation => TopologyFailureCode::InvalidData,
        UiErrorCode::CapabilityUnavailable => TopologyFailureCode::Unsupported,
        UiErrorCode::Unknown => TopologyFailureCode::Unavailable,
    }
}
pub(crate) fn now_ms() -> Result<u64, UiError> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|duration| duration.as_millis().try_into().ok())
        .ok_or_else(|| {
            UiError::new(
                "The topology observation time is unavailable.",
                UiErrorCode::Unknown,
                true,
            )
        })
}

impl AppServices {
    /// One backpressured loop belongs to the application TaskGroup. Dropping its page receiver
    /// cancels the bridge; process shutdown cancels and awaits this loop and its current read.
    pub(crate) async fn watch_topology(
        &self,
        context: TopologyContext,
        refresh: TopologyRefresh,
        sender: tokio::sync::mpsc::Sender<Result<TopologySnapshot, UiError>>,
    ) -> Result<(), UiError> {
        let services = self.clone();
        let work = topology_loop(services, context, refresh, sender);
        match &self.runtime_bridge {
            Some(runtime) => runtime.run("gpui-topology-loop", work).await,
            None if refresh == TopologyRefresh::Manual => work.await,
            None => Err(UiError::new(
                "Automatic topology refresh requires the desktop runtime.",
                UiErrorCode::CapabilityUnavailable,
                false,
            )),
        }
    }
    pub(crate) fn topology_context(&self) -> TopologyContext {
        let state = self.connection_state();
        TopologyContext {
            environment: state.environments.map(|catalog| catalog.active_id),
            revision: state.config.revision,
            scope: state.config.scope,
        }
    }
    pub(crate) async fn topology_snapshot(&self, context: TopologyContext) -> Result<TopologySnapshot, UiError> {
        if self.topology_context() != context {
            return Err(stale());
        }
        let now = now_ms()?;
        let config = self.connection_state().config;
        let mut local = TopologyLayer::empty(TopologySource::Config, now);
        local.nodes.push(TopologyNode::new(
            TopologyNodeId::Configuration,
            format!("{:?} · revision {}", context.scope, context.revision),
            local.source,
            now,
        ));
        local.total = config.nameservers.len();
        local.truncated = config.nameservers.len() > 16;
        local.inventory_complete = !local.truncated;
        for address in config.nameservers.iter().take(16) {
            let label = if config.current_nameserver.as_ref() == Some(address) {
                format!("{address} · selected")
            } else {
                address.clone()
            };
            local.nodes.push(TopologyNode::new(
                TopologyNodeId::NameServer(address.clone()),
                label,
                local.source,
                now,
            ));
            local.edges.push(TopologyEdge::new(
                TopologyNodeId::Configuration,
                TopologyNodeId::NameServer(address.clone()),
                TopologyEdgeKind::ConfiguredEndpoint,
                local.source,
                now,
            ));
        }
        let backend = Arc::clone(&self.topology);
        let captured = context.clone();
        let budget = self.runtime_bridge.as_ref().map(|_| Duration::from_secs(6));
        let work = async move {
            let read = |source| {
                read_layer(
                    Arc::clone(&backend),
                    TopologyReadRequest {
                        context: captured.clone(),
                        source,
                        observed_at_ms: now,
                        nameservers: config.nameservers.clone(),
                    },
                    budget,
                )
            };
            let (nameservers, brokers, topics, producers, consumers, replication) = tokio::join!(
                read(TopologySource::Nameservers),
                read(TopologySource::Brokers),
                read(TopologySource::Topics),
                read(TopologySource::Producers),
                read(TopologySource::Consumers),
                read(TopologySource::Replication)
            );
            let layers = [local, nameservers, brokers, topics, producers, consumers, replication]
                .into_iter()
                .map(|layer| (layer.source, layer))
                .collect();
            Ok(TopologySnapshot {
                context: captured,
                generated_at_ms: now_ms()?,
                layers,
            })
        };
        let result = match &self.runtime_bridge {
            Some(runtime) => runtime.run("gpui-topology-snapshot", work).await,
            None => work.await,
        }?;
        if self.topology_context() != context {
            return Err(stale());
        }
        Ok(result)
    }
    #[cfg(test)]
    pub(crate) fn with_topology_backend(mut self, backend: Arc<dyn TopologyBackend>) -> Self {
        self.topology = backend;
        self
    }
    #[cfg(test)]
    pub(crate) fn with_topology_runtime(mut self, context: rocketmq_runtime::ChildServiceContext) -> Self {
        self.runtime_bridge = Some(super::RuntimeBridge {
            context,
            completion: None,
        });
        self
    }
}
async fn topology_loop(
    services: AppServices,
    context: TopologyContext,
    refresh: TopologyRefresh,
    sender: tokio::sync::mpsc::Sender<Result<TopologySnapshot, UiError>>,
) -> Result<(), UiError> {
    loop {
        if services.topology_context() != context {
            return Err(stale());
        }
        let result = services.topology_snapshot(context.clone()).await;
        if services.topology_context() != context {
            return Err(stale());
        }
        if sender.send(result).await.is_err() {
            break;
        }
        let Some(seconds) = refresh.seconds() else {
            break;
        };
        tokio::time::sleep(Duration::from_secs(seconds)).await;
    }
    Ok(())
}
fn stale() -> UiError {
    UiError::new(
        "The topology connection changed. Refresh the current environment.",
        UiErrorCode::Configuration,
        false,
    )
}
async fn read_layer(
    backend: Arc<dyn TopologyBackend>,
    request: TopologyReadRequest,
    budget: Option<Duration>,
) -> TopologyLayer {
    let source = request.source;
    let now = request.observed_at_ms;
    if request.context.scope != ConnectionScope::NameServer {
        return TopologyLayer::failed(source, now, TopologyFailureCode::Unsupported);
    }
    let result = match budget {
        Some(budget) => match tokio::time::timeout(
            if source == TopologySource::Replication {
                Duration::from_secs(3)
            } else {
                budget
            },
            backend.layer(request),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => return TopologyLayer::failed(source, now, TopologyFailureCode::Timeout),
        },
        None => backend.layer(request).await,
    };
    result.unwrap_or_else(|error| TopologyLayer::failed(source, now, failure(&error)))
}

#[cfg(test)]
#[path = "topology_test_support.rs"]
pub(crate) mod test_support;
#[cfg(test)]
#[path = "topology_tests.rs"]
mod tests;

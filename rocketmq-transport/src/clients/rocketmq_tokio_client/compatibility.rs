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

use cheetah_string::CheetahString;
use tracing::debug;
use tracing::info;
use tracing::warn;

use super::TransportClient;

/// The result of reconciling one direct cached transport session.
///
/// This is a point-in-time observation. Existing clones of a retired session
/// remain valid Rust values, but the direct registry no longer selects them.
#[must_use]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CachedConnectionState {
    /// A direct cached session exists and is healthy.
    Healthy,
    /// An unhealthy direct cached session was removed.
    UnhealthyRetired,
    /// No direct cached session exists.
    Absent,
}

impl<PR: Send + Sync + Clone + 'static> TransportClient<PR> {
    pub(super) fn reconcile_cached_connection_inner(&self, addr: &CheetahString) -> CachedConnectionState {
        self.connection_registry.reconcile_direct_session(addr)
    }

    pub(super) fn is_address_reachable_inner(&self, addr: &CheetahString) {
        match self.reconcile_cached_connection_inner(addr) {
            CachedConnectionState::Healthy => {}
            CachedConnectionState::UnhealthyRetired => warn!("Removed unhealthy connection"),
            CachedConnectionState::Absent => debug!("No connection found"),
        }
    }

    pub(super) fn close_clients_inner(&self, addrs: Vec<String>) {
        for addr in &addrs {
            let key = CheetahString::from(addr.as_str());
            if !self.connection_registry.remove_sessions_by_identity(&key).is_empty() {
                info!("Closed client connection");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_runtime::RuntimeContext;
    use tokio::net::TcpListener;

    use super::*;
    use crate::request_processor::default_request_processor::DefaultRequestProcessor;
    use crate::runtime::config::client_config::TransportClientConfig;

    #[tokio::test]
    async fn reconciliation_reports_absent_healthy_and_retired_direct_sessions() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener address");
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client connection");
            let _socket = socket;
            let _ = release_rx.await;
        });
        let runtime = RuntimeContext::from_current("compatibility-reconciliation");
        let client = Arc::new(TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            runtime.service_context("client"),
        ));
        client.start().await.expect("start client");
        let target = CheetahString::from_string(addr.to_string());

        assert_eq!(
            client.reconcile_cached_connection_inner(&target),
            CachedConnectionState::Absent
        );
        let session = client
            .create_client(&target, Duration::from_secs(1))
            .await
            .expect("create cached direct session");
        assert_eq!(
            client.reconcile_cached_connection_inner(&target),
            CachedConnectionState::Healthy
        );

        session.retire_after_timeout().await;
        assert_eq!(
            client.reconcile_cached_connection_inner(&target),
            CachedConnectionState::UnhealthyRetired
        );
        assert_eq!(
            client.reconcile_cached_connection_inner(&target),
            CachedConnectionState::Absent
        );

        let _ = client.shutdown_now();
        let _ = release_tx.send(());
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn legacy_reachability_facade_keeps_unhealthy_cache_cleanup() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener address");
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client connection");
            let _socket = socket;
            let _ = release_rx.await;
        });
        let runtime = RuntimeContext::from_current("legacy-reachability-cleanup");
        let client = Arc::new(TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            runtime.service_context("client"),
        ));
        client.start().await.expect("start client");
        let target = CheetahString::from_string(addr.to_string());
        let session = client
            .create_client(&target, Duration::from_secs(1))
            .await
            .expect("create cached direct session");
        session.retire_after_timeout().await;

        // This validates the retained compatibility call site while keeping its deprecation scoped to this test.
        #[allow(deprecated)]
        client.is_address_reachable(&target);
        assert_eq!(
            client.reconcile_cached_connection_inner(&target),
            CachedConnectionState::Absent
        );

        let _ = client.shutdown_now();
        let _ = release_tx.send(());
        server.await.expect("server task");
    }
}

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

use rocketmq_observability::metrics::namesrv::NameServerConnectionEvent;
use rocketmq_transport::api::v2::SessionId;
use rocketmq_transport::api::v2::V2SessionEvent;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeHandle;

pub struct BrokerHousekeepingService {
    name_server_runtime_inner: NameServerRuntimeHandle,
    active_sessions: dashmap::DashSet<SessionId>,
}

impl BrokerHousekeepingService {
    pub(crate) fn new(name_server_runtime_inner: NameServerRuntimeHandle) -> Self {
        Self {
            name_server_runtime_inner,
            active_sessions: dashmap::DashSet::new(),
        }
    }

    /// Runs until the NameServer lifecycle requests shutdown or the registry is dropped.
    pub(crate) async fn run(
        self: Arc<Self>,
        mut events: broadcast::Receiver<V2SessionEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                event = events.recv() => match event {
                    Ok(event) => self.on_event(event),
                    Err(broadcast::error::RecvError::Closed) => return,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "NameServer broker session observer lagged; expiry reconciliation remains authoritative");
                    }
                },
            }
        }
    }

    fn on_event(&self, event: V2SessionEvent) {
        let Some(runtime) = self.name_server_runtime_inner.upgrade() else {
            return;
        };
        match event {
            V2SessionEvent::Connected(session) => {
                if self.active_sessions.insert(session.id()) {
                    runtime
                        .namesrv_metrics()
                        .record_connection_event(NameServerConnectionEvent::Admitted, self.active_sessions.len());
                }
            }
            V2SessionEvent::Disconnected(session_id) => {
                if self.active_sessions.remove(&session_id).is_some() {
                    runtime
                        .namesrv_metrics()
                        .record_connection_event(NameServerConnectionEvent::Closed, self.active_sessions.len());
                }
                runtime.route_info_manager().on_session_destroy(session_id);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn active_session_count(&self) -> usize {
        self.active_sessions.len()
    }
}

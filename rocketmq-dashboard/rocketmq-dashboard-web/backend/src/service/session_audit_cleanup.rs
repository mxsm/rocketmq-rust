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

use crate::config::AuthConfig;
use crate::error::DashboardError;
use crate::model::SessionAuditCleanupHealth;
use crate::persistence::DashboardPersistence;
use chrono::Utc;
use rocketmq_runtime::ChildServiceContext;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::MissedTickBehavior;

#[derive(Debug, Clone)]
pub struct SessionAuditCleanupRuntime {
    state: Arc<RwLock<SessionAuditCleanupHealth>>,
}

impl SessionAuditCleanupRuntime {
    pub fn new(persistence: &DashboardPersistence) -> Self {
        Self {
            state: Arc::new(RwLock::new(SessionAuditCleanupHealth {
                backend: persistence.storage_backend(),
                connectivity: "available".to_string(),
                role: "standby".to_string(),
                last_cleanup_at_ms: None,
                recent_error: None,
            })),
        }
    }

    pub async fn health(&self) -> SessionAuditCleanupHealth {
        self.state.read().await.clone()
    }

    async fn leader(&self) {
        let mut state = self.state.write().await;
        state.connectivity = "available".to_string();
        state.role = "leader".to_string();
        state.recent_error = None;
    }

    async fn standby(&self) {
        let mut state = self.state.write().await;
        state.connectivity = "available".to_string();
        state.role = "standby".to_string();
    }

    async fn success(&self) {
        let mut state = self.state.write().await;
        state.connectivity = "available".to_string();
        state.last_cleanup_at_ms = Some(Utc::now().timestamp_millis());
        state.recent_error = None;
    }

    async fn unavailable(&self) {
        let mut state = self.state.write().await;
        state.connectivity = "unavailable".to_string();
        state.recent_error = Some("session and audit cleanup is unavailable".to_string());
    }
}

/// Starts the owned bounded retention loop. MySQL and PostgreSQL acquire a
/// dedicated fenced task lease before a batch; File and mounted SQLite are
/// single-node stores and use their process/instance coordination instead.
pub fn start_session_audit_cleanup(
    service_context: ChildServiceContext,
    persistence: Arc<DashboardPersistence>,
    config: AuthConfig,
    runtime: SessionAuditCleanupRuntime,
) -> Result<(), DashboardError> {
    let cancellation = service_context.task_group().cancellation_token();
    service_context
        .spawn_service("dashboard-session-audit-cleanup", async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(config.cleanup_interval_secs));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let holder_id = uuid::Uuid::now_v7().to_string();
            let lease_ttl_ms = i64::try_from(config.cleanup_interval_secs)
                .ok()
                .and_then(|seconds| seconds.checked_mul(1_000))
                .map(|ttl| ttl.clamp(30_000, 3_600_000))
                .unwrap_or(30_000);
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        let lease = if persistence.session_audit_cleanup_uses_sql_lease() {
                            match persistence.acquire_session_audit_cleanup_lease(&holder_id, lease_ttl_ms).await {
                                Ok(Some(lease)) => Some(lease),
                                Ok(None) => {
                                    runtime.standby().await;
                                    continue;
                                }
                                Err(_) => {
                                    runtime.unavailable().await;
                                    continue;
                                }
                            }
                        } else {
                            None
                        };
                        runtime.leader().await;
                        let now = Utc::now().timestamp_millis();
                        let session_cutoff = now.saturating_sub(i64::from(config.session_retention_days) * 86_400_000);
                        let audit_cutoff = now.saturating_sub(i64::from(config.audit_retention_days) * 86_400_000);
                        let result = async {
                            persistence.delete_sessions_before(session_cutoff, config.cleanup_batch_size as usize).await?;
                            persistence.delete_audit_before(audit_cutoff, config.cleanup_batch_size as usize).await
                        }.await;
                        match result {
                            Ok(_) => runtime.success().await,
                            Err(_) => runtime.unavailable().await,
                        }
                        if let Some(lease) = lease {
                            let _ = persistence.release_history_lease(&lease).await;
                        }
                    }
                }
            }
            runtime.standby().await;
        })
        .map_err(|error| DashboardError::internal_source("Could not start session and audit cleanup", error))?;
    Ok(())
}

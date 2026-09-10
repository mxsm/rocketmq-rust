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

use super::db;
use super::types::*;
use crate::audit::AuditContext;
use crate::error::{DashboardError, DashboardResult};
use crate::nameserver::NameServerRuntimeState;
use crate::persistence::StorageManager;
use rocketmq_dashboard_common::NameServerRuntimeAdapter;
use rusqlite::TransactionBehavior;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

#[derive(Clone)]
pub(crate) struct ConnectionManager {
    storage: StorageManager,
    runtime: Arc<NameServerRuntimeState>,
    current: Arc<Mutex<ConnectionSettingsView>>,
    gate: Arc<RwLock<()>>,
}
impl ConnectionManager {
    pub(crate) async fn initialize(
        storage: StorageManager,
        runtime: Arc<NameServerRuntimeState>,
    ) -> DashboardResult<Self> {
        let view = storage
            .run("connection-initialize", |connection| {
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                db::ensure_identities(&transaction)?;
                let view = db::load(&transaction)?;
                transaction.commit()?;
                Ok(view)
            })
            .await?;
        Ok(Self {
            storage,
            runtime,
            current: Arc::new(Mutex::new(view)),
            gate: Arc::new(RwLock::new(())),
        })
    }

    pub(crate) fn snapshot(&self) -> DashboardResult<ConnectionSettingsView> {
        self.current
            .lock()
            .map(|view| view.clone())
            .map_err(|_| DashboardError::Internal("connection snapshot poisoned"))
    }

    pub(crate) fn check_revision(&self, revision: i64) -> DashboardResult<()> {
        if self.snapshot()?.revision != revision {
            return Err(DashboardError::ConfigurationConflict);
        }
        Ok(())
    }

    /// Remote mutations hold this read lease through their RPC. A switch waits for already accepted writes.
    pub(crate) async fn mutation_lease(
        &self,
        revision: i64,
        audit: &AuditContext,
    ) -> DashboardResult<OwnedRwLockReadGuard<()>> {
        let guard = self.gate.clone().read_owned().await;
        let view = self.snapshot()?;
        if view.revision != revision {
            return Err(DashboardError::ConfigurationConflict);
        }
        audit.set_environment(view.environment_id)?;
        Ok(guard)
    }

    pub(crate) async fn change(
        &self,
        expected_revision: i64,
        change: ConnectionChange,
        audit: AuditContext,
    ) -> DashboardResult<ConnectionMutationResult> {
        let _guard = self.gate.write().await;
        let runtime = self.runtime.clone();
        let current = self.current.clone();
        self.storage
            .run("connection-change", move |connection| {
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let mut view = db::load(&transaction)?;
                if view.revision != expected_revision {
                    return Err(DashboardError::ConfigurationConflict);
                }
                db::apply(&mut view, change)?;
                crate::nameserver::db::save_snapshot_to_transaction(&transaction, &view.nameserver)?;
                crate::proxy::db::save_snapshot_to_transaction(&transaction, &view.proxy)?;
                db::ensure_identities(&transaction)?;
                transaction.execute(
                    "UPDATE connection_metadata SET revision = revision + 1 WHERE id = 1",
                    [],
                )?;
                let view = db::load(&transaction)?;
                audit.set_environment(view.environment_id.clone())?;
                audit.record_local_success(&transaction)?;
                // Lock publication before committing; a poisoned state must not commit an invisible configuration.
                let mut published = current
                    .lock()
                    .map_err(|_| DashboardError::Internal("connection snapshot poisoned"))?;
                transaction.commit()?;
                runtime.apply_snapshot(&view.nameserver)?;
                *published = view.clone();
                Ok(ConnectionMutationResult {
                    message: "Connection settings saved",
                    settings: view,
                })
            })
            .await
    }
}

#[cfg(test)]
mod tests;

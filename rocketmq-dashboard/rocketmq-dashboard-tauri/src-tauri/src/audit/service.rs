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
use crate::error::{CommandError, CommandResult, DashboardError, DashboardResult};
use crate::persistence::StorageManager;
use chrono::Utc;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::service_context::ChildServiceContext;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

const AUDIT_WARNING: &str = "The operation completed, but its audit record could not be saved. Review the result before taking any further action.";

#[derive(Clone)]
pub(crate) struct AuditManager {
    storage: StorageManager,
    context: ChildServiceContext,
    accepting: Arc<Mutex<bool>>,
}

impl AuditManager {
    pub(crate) fn new(storage: StorageManager, context: ChildServiceContext) -> Self {
        Self {
            storage,
            context,
            accepting: Arc::new(Mutex::new(true)),
        }
    }

    /// Once admitted, an operation and its terminal audit record outlive the IPC waiter.
    pub(crate) async fn execute<T, F, Fut>(
        &self,
        access: AuditAccess,
        action: AuditAction,
        resource: Option<String>,
        operation: F,
    ) -> CommandResult<Audited<T>>
    where
        T: AuditReceipt + Send + 'static,
        F: FnOnce(AuditContext) -> Fut + Send + 'static,
        Fut: Future<Output = DashboardResult<T>> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        {
            let accepting = self.accepting.lock().map_err(|_| DashboardError::StorageClosed)?;
            if !*accepting {
                return Err(DashboardError::StorageClosed.into());
            }
            let manager = self.clone();
            self.context
                .spawn_service("audited-mutation", async move {
                    let result = manager.execute_accepted(access, action, resource, operation).await;
                    let _ = sender.send(result);
                })
                .map_err(DashboardError::Persistence)?;
        }
        receiver
            .await
            .map_err(|_| CommandError::from(DashboardError::StorageClosed))?
    }

    async fn execute_accepted<T, F, Fut>(
        &self,
        access: AuditAccess,
        action: AuditAction,
        resource: Option<String>,
        operation: F,
    ) -> CommandResult<Audited<T>>
    where
        T: AuditReceipt,
        F: FnOnce(AuditContext) -> Fut,
        Fut: Future<Output = DashboardResult<T>>,
    {
        let mut context = AuditContext {
            actor: None,
            environment: Arc::new(Mutex::new(None)),
            event_id: Uuid::new_v4().to_string(),
            request_id: Uuid::new_v4().to_string(),
            action,
            resource_name: resource.filter(|name| name.len() <= 255 && !name.chars().any(char::is_control)),
        };
        let mut actor = None;
        let identity = match access {
            AuditAccess::Login => Ok(()),
            AuditAccess::Account { sessions, token } => sessions.require_session(&token).await.map(|user| {
                actor = Some(user.username);
            }),
            AuditAccess::Dashboard { sessions, token } => match sessions.require_session(&token).await {
                Ok(user) => {
                    actor = Some(user.username);
                    if user.must_change_password {
                        Err(DashboardError::PasswordChangeRequired)
                    } else {
                        Ok(())
                    }
                }
                Err(error) => Err(error),
            },
        };
        context.actor = actor.clone();
        let result = match identity {
            Ok(_) => operation(context.clone()).await.map_err(CommandError::from),
            Err(error) => Err(CommandError::from(error)),
        };
        let summary = match &result {
            Ok(receipt) => {
                actor = actor.or_else(|| receipt.authenticated_actor());
                receipt.summary()
            }
            Err(error) => Summary::error(error, action.remote()),
        };
        let event = context.event(actor, summary);
        let saved = self
            .storage
            .run("audit-terminal", move |connection| db::insert(connection, &event))
            .await
            .is_ok();
        match result {
            Ok(result) => Ok(Audited {
                result,
                audit_warning: if saved { None } else { Some(AUDIT_WARNING) },
            }),
            Err(mut error) => {
                if !saved {
                    error.audit_warning =
                        Some("The audit record could not be saved. The reported operation outcome is unchanged.");
                }
                Err(error)
            }
        }
    }

    pub(crate) async fn query(&self, query: AuditQuery) -> DashboardResult<AuditPage> {
        self.storage
            .run("audit-query", move |connection| db::query(connection, query))
            .await
    }

    pub(crate) fn start_cleanup(&self) -> DashboardResult<()> {
        let storage = self.storage.clone();
        self.storage.start_background("audit-retention", async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let cutoff = Utc::now().timestamp_millis() - 30 * 24 * 60 * 60 * 1000;
                if storage.run("audit-cleanup", move |connection| {
                    connection.execute("DELETE FROM audit_events WHERE event_id IN (SELECT event_id FROM audit_events WHERE created_at_ms < ?1 ORDER BY created_at_ms LIMIT 1000)", [cutoff])?; Ok(())
                }).await.is_err() { log::warn!("Audit retention cleanup could not complete"); }
            }
        })
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) -> bool {
        match self.accepting.lock() {
            Ok(mut accepting) => *accepting = false,
            Err(_) => return false,
        }
        self.context
            .task_group()
            .shutdown_until(ShutdownDeadline::after(timeout))
            .await
            .is_healthy()
    }
}

#[cfg(test)]
mod tests;

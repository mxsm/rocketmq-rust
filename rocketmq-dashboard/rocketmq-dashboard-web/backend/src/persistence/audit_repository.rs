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

use crate::model::AuditAction;
use crate::model::AuditEvent;
use crate::model::AuditOutcome;
use crate::persistence::DashboardPersistence;
use crate::persistence::backend::PersistenceBackend;
use crate::persistence::error::PersistenceError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditCursor {
    pub created_at_ms: i64,
    pub event_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditQuery {
    pub start_ms: i64,
    pub end_ms: i64,
    pub actor: Option<String>,
    pub action: Option<AuditAction>,
    pub outcome: Option<AuditOutcome>,
    pub environment_id: Option<String>,
    pub cursor: Option<AuditCursor>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuditPage {
    pub events: Vec<AuditEvent>,
    pub next_cursor: Option<AuditCursor>,
}

/// Repository boundary for tamper-evident operational history. Events are
/// append-only from the dashboard API; no delete API is exposed.
#[allow(async_fn_in_trait)]
pub trait AuditRepository {
    async fn append_audit_event(&self, event: AuditEvent) -> Result<(), PersistenceError>;
    async fn query_audit_events(&self, query: AuditQuery) -> Result<AuditPage, PersistenceError>;
    async fn delete_audit_before(&self, cutoff_ms: i64, limit: usize) -> Result<u64, PersistenceError>;
}

impl DashboardPersistence {
    pub async fn append_audit_event(&self, event: AuditEvent) -> Result<(), PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(store) => store.append_audit_event(event).await,
            PersistenceBackend::Sql(store) => store.append_audit_event(event).await,
        }
    }

    pub async fn query_audit_events(&self, query: AuditQuery) -> Result<AuditPage, PersistenceError> {
        validate_query(&query)?;
        match &self.backend {
            PersistenceBackend::File(store) => store.query_audit_events(query).await,
            PersistenceBackend::Sql(store) => store.query_audit_events(query).await,
        }
    }

    pub async fn delete_audit_before(&self, cutoff_ms: i64, limit: usize) -> Result<u64, PersistenceError> {
        if limit == 0 || limit > 1_000 {
            return Err(PersistenceError::InvalidConfig(
                "audit cleanup batch must be between 1 and 1000".to_string(),
            ));
        }
        match &self.backend {
            PersistenceBackend::File(store) => store.delete_audit_before(cutoff_ms, limit).await,
            PersistenceBackend::Sql(store) => store.delete_audit_before(cutoff_ms, limit).await,
        }
    }
}

impl AuditRepository for DashboardPersistence {
    async fn append_audit_event(&self, event: AuditEvent) -> Result<(), PersistenceError> {
        Self::append_audit_event(self, event).await
    }

    async fn query_audit_events(&self, query: AuditQuery) -> Result<AuditPage, PersistenceError> {
        Self::query_audit_events(self, query).await
    }

    async fn delete_audit_before(&self, cutoff_ms: i64, limit: usize) -> Result<u64, PersistenceError> {
        Self::delete_audit_before(self, cutoff_ms, limit).await
    }
}

fn validate_query(query: &AuditQuery) -> Result<(), PersistenceError> {
    if query.start_ms < 0 || query.end_ms < query.start_ms || query.limit == 0 || query.limit > 200 {
        return Err(PersistenceError::InvalidConfig("audit query is invalid".to_string()));
    }
    if let Some(cursor) = &query.cursor
        && (cursor.event_id.is_empty() || cursor.event_id.len() > 36)
    {
        return Err(PersistenceError::InvalidConfig("audit cursor is invalid".to_string()));
    }
    Ok(())
}

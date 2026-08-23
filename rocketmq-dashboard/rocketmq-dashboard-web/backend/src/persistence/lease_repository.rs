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
use crate::model::EnvironmentId;
use crate::model::StorageBackend;
use crate::persistence::DashboardPersistence;
use crate::persistence::backend::PersistenceBackend;
use crate::persistence::error::PersistenceError;

/// Opaque authority granted by the SQL task lease. The holder identity is
/// never serialized or exposed through a dashboard API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryLease {
    pub(crate) environment_id: EnvironmentId,
    pub(crate) name: String,
    pub(crate) holder_id: String,
    pub(crate) fencing_token: i64,
    pub(crate) expires_at_ms: i64,
}

impl HistoryLease {
    pub(crate) fn new(
        environment_id: EnvironmentId,
        holder_id: String,
        fencing_token: i64,
        expires_at_ms: i64,
    ) -> Self {
        Self {
            name: Self::name_for(&environment_id),
            environment_id,
            holder_id,
            fencing_token,
            expires_at_ms,
        }
    }

    pub(crate) fn name_for(environment_id: &EnvironmentId) -> String {
        format!("dashboard-history:{}", environment_id.0)
    }

    pub fn environment_id(&self) -> &EnvironmentId {
        &self.environment_id
    }

    pub fn expires_at_ms(&self) -> i64 {
        self.expires_at_ms
    }
}

impl DashboardPersistence {
    pub async fn acquire_history_lease(
        &self,
        environment_id: &EnvironmentId,
        holder_id: &str,
        ttl_ms: i64,
    ) -> Result<Option<HistoryLease>, PersistenceError> {
        validate_lease_request(environment_id, holder_id, ttl_ms)?;
        match &self.backend {
            PersistenceBackend::File(_) => Ok(None),
            PersistenceBackend::Sql(store) => store.acquire_history_lease(environment_id, holder_id, ttl_ms).await,
        }
    }

    pub async fn renew_history_lease(
        &self,
        lease: &HistoryLease,
        ttl_ms: i64,
    ) -> Result<Option<HistoryLease>, PersistenceError> {
        validate_lease_request(&lease.environment_id, &lease.holder_id, ttl_ms)?;
        match &self.backend {
            PersistenceBackend::File(_) => Ok(Some(lease.clone())),
            PersistenceBackend::Sql(store) => store.renew_history_lease(lease, ttl_ms).await,
        }
    }

    pub async fn release_history_lease(&self, lease: &HistoryLease) -> Result<bool, PersistenceError> {
        match &self.backend {
            PersistenceBackend::File(_) => Ok(true),
            PersistenceBackend::Sql(store) => store.release_history_lease(lease).await,
        }
    }

    pub fn history_uses_sql_lease(&self) -> bool {
        matches!(
            self.storage_backend(),
            StorageBackend::Sqlite | StorageBackend::MySql | StorageBackend::Postgres
        )
    }
}

fn validate_lease_request(
    environment_id: &EnvironmentId,
    holder_id: &str,
    ttl_ms: i64,
) -> Result<(), PersistenceError> {
    if environment_id.0.is_empty()
        || environment_id.0.len() > 36
        || holder_id.is_empty()
        || holder_id.len() > 128
        || ttl_ms <= 0
    {
        return Err(PersistenceError::InvalidConfig(
            "history lease request is invalid".to_string(),
        ));
    }
    Ok(())
}

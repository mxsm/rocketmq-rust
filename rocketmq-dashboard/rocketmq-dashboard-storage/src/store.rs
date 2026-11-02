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

use std::{path::PathBuf, time::Duration};

use rocketmq_dashboard_common::{EnvironmentId, HistoryPoint, HistoryRetention};
use rocketmq_runtime::ChildServiceContext;

use crate::{
    file::FileStore, session::SessionDocument, DesktopDocument, HistoryAppendOutcome, HistoryDocument, HistoryLease,
    SessionId, SessionPage, SessionQuery, SessionRecord, StorageEngine, StorageError, StorageErrorKind, StorageOptions,
};

/// A CAS token for the complete canonical document, independent of config revision.
/// File tokens belong to the current single-writer lifetime; SQL tokens survive reopen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRevision(pub(crate) i64);

impl StorageRevision {
    pub fn value(self) -> i64 {
        self.0
    }

    pub(crate) fn next(self) -> Result<Self, StorageError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| StorageError::new(StorageErrorKind::RevisionExhausted))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesktopSnapshot {
    pub storage_revision: StorageRevision,
    pub document: DesktopDocument,
}

/// A completely decoded import candidate. Reading it does not rewrite source files.
#[derive(Clone)]
pub struct ImportSnapshot {
    pub document: DesktopDocument,
    pub history: HistoryDocument,
    pub(crate) sessions: SessionDocument,
}

impl ImportSnapshot {
    /// Reads all native sources under their directory writer before returning a candidate.
    /// Missing configuration is an error, even if an empty database is available.
    pub async fn read_files(config_path: PathBuf, context: ChildServiceContext) -> Result<Self, StorageError> {
        FileStore::read_import(config_path, context).await
    }

    pub fn sessions(&self) -> &[SessionRecord] {
        &self.sessions.sessions
    }
}

/// A fresh health query. Failed status reads return an error instead of cached healthy data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageStatus {
    pub engine: StorageEngine,
    pub schema_version: u32,
    pub document_schema_version: Option<u32>,
    pub initialized: bool,
    pub last_successful_write_epoch_ms: Option<u64>,
    pub pool_connections: Option<u32>,
    pub pool_idle_connections: Option<usize>,
}

enum Backend {
    File(FileStore),
    #[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
    Sql(crate::sql::SqlStore),
}

/// One selected authority for native configuration, History, Monitor, Session and Audit.
///
/// Opening SQL never imports file data or fabricates an initialized database. The application
/// must explicitly initialize a new database or import decoded file data. Closing this handle
/// closes all its clones' pools/writer; call it after owned users have finished.
pub struct DashboardStore {
    backend: Backend,
}

macro_rules! dispatch {
    ($self:expr, $method:ident $(, $argument:expr)* $(,)?) => {
        match &$self.backend {
            Backend::File(store) => store.$method($($argument),*).await,
            #[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
            Backend::Sql(store) => store.$method($($argument),*).await,
        }
    };
}

impl DashboardStore {
    pub async fn open(options: StorageOptions, context: ChildServiceContext) -> Result<Self, StorageError> {
        let backend = match options {
            StorageOptions::File { config_path } => Backend::File(FileStore::open(config_path, context).await?),
            StorageOptions::Database { engine, connection } => {
                if engine == StorageEngine::File {
                    return Err(StorageError::new(StorageErrorKind::Validation));
                }
                if !engine.is_compiled() {
                    return Err(StorageError::new(StorageErrorKind::DriverUnavailable { engine }));
                }
                #[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
                {
                    Backend::Sql(crate::sql::SqlStore::connect(engine, connection, context).await?)
                }
                #[cfg(not(any(feature = "sqlite", feature = "mysql", feature = "postgres")))]
                {
                    let _ = connection.as_str();
                    return Err(StorageError::new(StorageErrorKind::DriverUnavailable { engine }));
                }
            }
        };
        Ok(Self { backend })
    }

    /// Loads authoritative state for every call; peer writes are never hidden by a local cache.
    pub async fn load_desktop(&self) -> Result<Option<DesktopSnapshot>, StorageError> {
        dispatch!(self, load_desktop)
    }

    pub async fn initialize(&self, document: DesktopDocument) -> Result<DesktopSnapshot, StorageError> {
        document.validate()?;
        dispatch!(self, initialize, document)
    }

    /// Atomically replaces config/environment/Monitor/Audit after a whole-document CAS.
    /// Audit-only retries must reload and apply the journal transition without replaying business I/O.
    pub async fn compare_exchange_desktop(
        &self,
        expected: StorageRevision,
        document: DesktopDocument,
    ) -> Result<DesktopSnapshot, StorageError> {
        document.validate()?;
        dispatch!(self, compare_exchange_desktop, expected, document)
    }

    /// Atomically imports all domains into an empty SQL authority; never overwrites a destination.
    pub async fn import(&self, snapshot: ImportSnapshot) -> Result<DesktopSnapshot, StorageError> {
        snapshot.document.validate()?;
        snapshot.history.validate()?;
        snapshot.sessions.validate()?;
        dispatch!(self, import, snapshot)
    }

    pub async fn history(&self) -> Result<HistoryDocument, StorageError> {
        dispatch!(self, history)
    }

    /// Leases use the authority's clock; multiple clients do not rely on synchronized clocks.
    pub async fn acquire_history_lease(
        &self,
        environment_id: EnvironmentId,
        holder_id: String,
        ttl: Duration,
    ) -> Result<Option<HistoryLease>, StorageError> {
        let ttl_ms = lease_input(&holder_id, ttl)?;
        dispatch!(self, acquire_history_lease, environment_id, holder_id, ttl_ms)
    }

    pub async fn renew_history_lease(
        &self,
        lease: HistoryLease,
        ttl: Duration,
    ) -> Result<Option<HistoryLease>, StorageError> {
        let ttl_ms = lease_input(lease.holder_id(), ttl)?;
        dispatch!(self, renew_history_lease, lease, ttl_ms)
    }

    pub async fn release_history_lease(&self, lease: HistoryLease) -> Result<bool, StorageError> {
        dispatch!(self, release_history_lease, lease)
    }

    /// The lease and its fencing token are verified in the same transaction as bounded retention.
    /// Attribution comes from the lease captured before sampling, even if the active profile changes.
    pub async fn append_history(
        &self,
        lease: HistoryLease,
        sample: Vec<HistoryPoint>,
        retention: HistoryRetention,
    ) -> Result<HistoryAppendOutcome, StorageError> {
        dispatch!(self, append_history, lease, sample, retention)
    }

    pub async fn create_session(&self, session: SessionRecord, now_epoch_ms: u64) -> Result<(), StorageError> {
        dispatch!(self, create_session, session, now_epoch_ms)
    }

    /// Checks revocation/expiry against authoritative state and advances last access atomically.
    /// Storage errors must not be treated as a valid or anonymous authenticated session.
    pub async fn access_session(
        &self,
        id: SessionId,
        now_epoch_ms: u64,
    ) -> Result<Option<SessionRecord>, StorageError> {
        dispatch!(self, access_session, id, now_epoch_ms)
    }

    pub async fn sessions(&self, query: SessionQuery) -> Result<SessionPage, StorageError> {
        dispatch!(self, sessions, query)
    }

    pub async fn revoke_session(&self, id: SessionId, now_epoch_ms: u64) -> Result<bool, StorageError> {
        dispatch!(self, revoke_session, id, now_epoch_ms)
    }

    pub async fn revoke_all_sessions(&self, username: String, now_epoch_ms: u64) -> Result<usize, StorageError> {
        dispatch!(self, revoke_all_sessions, username, now_epoch_ms)
    }

    pub async fn status(&self) -> Result<StorageStatus, StorageError> {
        dispatch!(self, status)
    }

    /// Waits for SQL connections to close or releases the file directory writer.
    pub async fn close(&self) -> Result<(), StorageError> {
        dispatch!(self, close)
    }
}

fn lease_input(holder: &str, ttl: Duration) -> Result<i64, StorageError> {
    let ttl_ms = i64::try_from(ttl.as_millis()).map_err(StorageError::validation)?;
    if holder.is_empty()
        || holder.len() > 128
        || !holder
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        || ttl_ms == 0
    {
        return Err(StorageError::new(StorageErrorKind::Validation));
    }
    Ok(ttl_ms)
}

pub(crate) fn now_ms() -> Result<i64, StorageError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(StorageError::validation)?;
    i64::try_from(elapsed.as_millis()).map_err(StorageError::validation)
}

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

//! Native Dashboard storage, independent of GPUI, HTTP, and Web backend migrations.
//!
//! Configuration, environments, Monitor rules and the P11 audit journal remain one
//! canonical document. A separate storage revision covers every document change,
//! including changes that do not advance the configuration revision. A failed CAS
//! must be resolved by loading authoritative state and reapplying the domain operation.
//! The caller owns the runtime and must await [`DashboardStore::close`] before stopping it.

mod atomic_file;
mod audit;
mod config;
mod document;
mod error;
mod file;
mod history;
mod options;
mod session;
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
mod sql;
mod store;

#[cfg(test)]
mod contracts_tests;

pub use audit::{AuditJournal, MAX_AUDIT_EVENTS, MAX_PENDING_AUDIT_OPERATIONS};
pub use config::{
    AuthConfig, DesktopConfig, FoundationFlags, CONFIG_SCHEMA_VERSION, HISTORY_MAX_POINTS_CAP, HISTORY_MAX_SERIES_CAP,
    HISTORY_MAX_TOTAL_POINTS_CAP,
};
pub use document::{
    DesktopDocument, DesktopSettings, EnvironmentCatalog, EnvironmentChange, EnvironmentProfile, LocalOperationKind,
    LocalOperationRecord, MonitorDocument, MonitorMigrationState, MonitorRule, StoredMonitorRule,
};
pub use error::{StorageError, StorageErrorKind};
pub use history::{HistoryAppendOutcome, HistoryDocument, HistoryLease, LegacyHistoryRecord, HISTORY_SCHEMA_VERSION};
pub use options::{DatabaseConnection, StorageEngine, StorageOptions, StorageSelection};
pub use session::{SessionCursor, SessionId, SessionPage, SessionQuery, SessionRecord};
pub use store::{DashboardStore, DesktopSnapshot, ImportSnapshot, StorageRevision, StorageStatus};

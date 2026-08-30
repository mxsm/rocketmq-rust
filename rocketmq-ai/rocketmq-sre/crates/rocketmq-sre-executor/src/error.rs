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

use thiserror::Error;

use rocketmq_sre_core::ActionCatalogError;

/// Sanitized Change Executor service failure.
#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("change executor configuration is invalid")]
    Configuration,
    #[error("change executor request is unauthorized")]
    Unauthorized,
    #[error("execution request is invalid")]
    InvalidRequest,
    #[error("execution request signature or scope was rejected")]
    AuthorityRejected,
    #[error("Lease Authority is unavailable")]
    AuthorityUnavailable,
    #[error("Execution Agent rejected the request")]
    AgentRejected,
    #[error("Execution Agent is unavailable")]
    AgentUnavailable,
    #[error("execution verification source rejected the request")]
    VerificationRejected,
    #[error("execution verification source is unavailable")]
    VerificationUnavailable,
    #[error("approved preconditions changed before dispatch")]
    PreconditionChanged,
    #[error("active lease handoff is blocked by an unresolved effect")]
    ReconcileBlocked,
    #[error("the action descriptor is not executable")]
    Catalog(#[source] ActionCatalogError),
    #[error("durable execution state is unavailable")]
    Journal(#[from] JournalError),
    #[error("change executor HTTP client is unavailable")]
    Http(#[source] reqwest::Error),
    #[error("change executor listener is unavailable")]
    Io(#[source] std::io::Error),
}

impl From<ActionCatalogError> for ExecutorError {
    fn from(error: ActionCatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<reqwest::Error> for ExecutorError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error)
    }
}

impl From<std::io::Error> for ExecutorError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Fail-closed durable execution journal error.
#[derive(Debug, Error)]
pub enum JournalError {
    #[error("persistent execution state is unavailable")]
    Database(#[source] sqlx::Error),
    #[error("execution snapshot could not be encoded")]
    SnapshotEncoding(#[source] serde_json::Error),
    #[error("execution snapshot could not be decoded")]
    SnapshotDecoding(#[source] serde_json::Error),
    #[error("idempotency key is already bound to a different execution request")]
    IdempotencyConflict,
    #[error("the resource is already locked by another active execution")]
    ResourceLocked,
    #[error("the resource is quarantined")]
    ResourceQuarantined,
    #[error("the executor lease or fencing epoch is not active")]
    LeaseRejected,
    #[error("the requested persistent execution record was not found")]
    NotFound,
    #[error("the durable execution request is invalid: {0}")]
    InvalidInput(String),
}

impl From<sqlx::Error> for JournalError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

impl From<serde_json::Error> for JournalError {
    fn from(error: serde_json::Error) -> Self {
        Self::SnapshotEncoding(error)
    }
}

pub(crate) fn database_message(error: &sqlx::Error) -> Option<&str> {
    error.as_database_error().map(|database| database.message())
}

pub(crate) fn has_database_code(error: &sqlx::Error, expected: &str) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .is_some_and(|code| code == expected)
}

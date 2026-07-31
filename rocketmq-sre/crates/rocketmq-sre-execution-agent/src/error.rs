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

/// Sanitized Execution Agent service failure.
#[derive(Debug, Error)]
pub enum ExecutionAgentError {
    #[error("execution agent configuration is invalid")]
    Configuration,
    #[error("execution agent request is unauthorized")]
    Unauthorized,
    #[error("execution agent request is invalid")]
    InvalidRequest,
    #[error("action is not registered in the typed driver registry")]
    ActionNotRegistered,
    #[error("Lease Authority rejected the request")]
    AuthorityRejected,
    #[error("Lease Authority is unavailable")]
    AuthorityUnavailable,
    #[error("a previous effect remains non-terminal")]
    UnresolvedEffect,
    #[error("typed driver returned a sanitized failure")]
    DriverFailed,
    #[error("typed driver result is unknown")]
    DriverUnknown,
    #[error("dispatch barrier is unavailable")]
    DispatchBarrierUnavailable,
    #[error("execution agent persistence is unavailable")]
    Store(#[from] AgentStoreError),
    #[error("execution agent HTTP client is unavailable")]
    Http(#[source] reqwest::Error),
    #[error("execution agent listener is unavailable")]
    Io(#[source] std::io::Error),
}

impl ExecutionAgentError {
    #[must_use]
    pub(crate) const fn stable_code(&self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized_workload_identity",
            Self::InvalidRequest => "invalid_agent_request",
            Self::ActionNotRegistered => "action_not_registered",
            Self::AuthorityRejected => "stale_lease_epoch",
            Self::UnresolvedEffect => "unresolved_old_effects",
            Self::DriverFailed => "driver_failed",
            Self::DriverUnknown => "effect_unknown",
            Self::Configuration => "source_unavailable",
            Self::AuthorityUnavailable => "authority_unavailable",
            Self::DispatchBarrierUnavailable => "dispatch_barrier_unavailable",
            Self::Store(_) => "effect_store_unavailable",
            Self::Http(_) => "authority_transport_unavailable",
            Self::Io(_) => "service_io_unavailable",
        }
    }
}

impl From<reqwest::Error> for ExecutionAgentError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error)
    }
}

impl From<std::io::Error> for ExecutionAgentError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Fail-closed Agent fence/effect persistence error.
#[derive(Debug, Error)]
pub enum AgentStoreError {
    #[error("execution agent persistence is unavailable")]
    Database(#[source] sqlx::Error),
    #[error("execution agent snapshot encoding failed")]
    SnapshotEncoding(#[source] serde_json::Error),
    #[error("execution agent snapshot decoding failed")]
    SnapshotDecoding(#[source] serde_json::Error),
    #[error("fencing epoch was rejected")]
    FenceRejected,
    #[error("idempotency key is already bound to a different effect")]
    IdempotencyConflict,
    #[error("execution agent effect was not found")]
    NotFound,
    #[error("execution agent effect transition is invalid")]
    InvalidTransition,
    #[error("execution agent request is invalid: {0}")]
    InvalidInput(String),
}

impl From<sqlx::Error> for AgentStoreError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

pub(crate) fn database_message(error: &sqlx::Error) -> Option<&str> {
    error.as_database_error().map(|database| database.message())
}

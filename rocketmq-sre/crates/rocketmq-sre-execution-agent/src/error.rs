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

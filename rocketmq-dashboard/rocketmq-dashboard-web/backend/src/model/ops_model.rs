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

use crate::model::StorageBackend;
use crate::persistence::StorageMode;
use crate::persistence::StorageStatus;
use serde::Deserialize;
use serde::Serialize;

/// A bounded explanation for a non-healthy storage result.
///
/// This deliberately carries no backend error, endpoint, filesystem path, or
/// other deployment detail. Detailed diagnostics stay in operator-controlled
/// logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StorageStatusReason {
    CapacityBelowReserve,
    ProbeFailed,
}

/// Storage information intended for authenticated dashboard operators.
///
/// `observation_started_at` bounds the meaning of
/// `last_successful_write_at`: the latter is a process-local observation, not
/// a claim about writes made before this dashboard process started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageStatusView {
    pub backend: StorageBackend,
    pub mode: StorageMode,
    pub status: StorageStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<StorageStatusReason>,
    pub schema_or_format_version: Option<i64>,
    pub observation_started_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_successful_write_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safe_available_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_connections: Option<usize>,
}

/// The intentionally minimal shape used by unauthenticated health probes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MinimalHealthStatus {
    pub status: &'static str,
}

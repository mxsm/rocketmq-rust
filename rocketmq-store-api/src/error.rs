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

//! Canonical errors exposed by storage capabilities.

use std::error::Error as StdError;
use std::fmt;

/// Closed vocabulary for operations that may cross the capability boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreOperation {
    Load,
    Start,
    Shutdown,
    Append,
    Read,
    QueryOffset,
    Replicate,
    AppendDerived,
    Admin,
}

impl StoreOperation {
    /// Returns the stable machine-readable operation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Load => "load",
            Self::Start => "start",
            Self::Shutdown => "shutdown",
            Self::Append => "append",
            Self::Read => "read",
            Self::QueryOffset => "query_offset",
            Self::Replicate => "replicate",
            Self::AppendDerived => "append_derived",
            Self::Admin => "admin",
        }
    }
}

/// Stable, low-cardinality storage failure classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreErrorKind {
    NotStarted,
    Unavailable,
    InvalidRequest,
    NotFound,
    Capacity,
    Storage,
    Io,
    Corruption,
    Timeout,
    Unsupported,
    Internal,
}

impl StoreErrorKind {
    /// Returns the stable machine-readable classification name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Unavailable => "unavailable",
            Self::InvalidRequest => "invalid_request",
            Self::NotFound => "not_found",
            Self::Capacity => "capacity",
            Self::Storage => "storage",
            Self::Io => "io",
            Self::Corruption => "corruption",
            Self::Timeout => "timeout",
            Self::Unsupported => "unsupported",
            Self::Internal => "internal",
        }
    }
}

/// Storage error containing only closed, non-sensitive vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreError {
    kind: StoreErrorKind,
    operation: StoreOperation,
}

impl StoreError {
    /// Creates an error from closed operation and kind vocabularies.
    pub const fn new(kind: StoreErrorKind, operation: StoreOperation) -> Self {
        Self { kind, operation }
    }

    /// Returns the stable failure classification.
    pub const fn kind(&self) -> StoreErrorKind {
        self.kind
    }

    /// Returns the operation that failed.
    pub const fn operation(&self) -> StoreOperation {
        self.operation
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "store operation {} failed: {}",
            self.operation.as_str(),
            self.kind.as_str()
        )
    }
}

impl StdError for StoreError {}

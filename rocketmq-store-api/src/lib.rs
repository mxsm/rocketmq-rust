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

//! Storage capability contracts.

use std::error::Error as StdError;
use std::fmt;
use std::future::Future;

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

/// Storage lifecycle capability.
pub trait StoreLifecycle: Send + Sync {
    type Error: StdError + Send + Sync + 'static;

    /// Loads existing state.
    ///
    /// # Errors
    ///
    /// Returns a typed error when state cannot be loaded safely.
    fn load(&mut self) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Starts the store within its existing lifecycle owner.
    ///
    /// # Errors
    ///
    /// Returns a typed error when startup cannot establish a usable store.
    fn start(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Stops the store and completes its owned shutdown sequence.
    ///
    /// # Errors
    ///
    /// Returns a typed error when final progress or shutdown fails.
    fn shutdown(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Message append capability generic over a consumer-owned input.
pub trait MessageAppender<M: Send>: Send {
    type Receipt: Send;
    type Error: StdError + Send + Sync + 'static;

    /// Appends one input and returns the implementation-owned receipt projection.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the append cannot produce an outcome.
    fn append_message(&mut self, message: M) -> impl Future<Output = Result<Self::Receipt, Self::Error>> + Send;
}

/// Message read capability with implementation-owned request and output values.
pub trait MessageReader: Send + Sync {
    type Request: Send;
    type Output: Send;
    type Error: StdError + Send + Sync + 'static;

    /// Reads a bounded message window.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the requested data cannot be read safely.
    fn read(&self, request: Self::Request) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

/// Logical offset lookup capability.
pub trait OffsetIndex: Send + Sync {
    type Query: Send + Sync;
    type Output;
    type Error: StdError + Send + Sync + 'static;

    /// Queries the current logical offset projection.
    ///
    /// # Errors
    ///
    /// Returns a typed error when index state is unavailable or inconsistent.
    fn query_offset(&self, query: &Self::Query) -> Result<Self::Output, Self::Error>;
}

/// Health snapshot capability with an implementation-owned projection.
pub trait StoreHealth: Send + Sync {
    type Snapshot;

    /// Returns the current health projection.
    fn health_snapshot(&self) -> Self::Snapshot;
}

/// Replication control capability with implementation-owned command and state values.
pub trait ReplicationControl: Send + Sync {
    type Command: Send;
    type State: Send;
    type Error: StdError + Send + Sync + 'static;

    /// Returns current replication state.
    fn replication_state(&self) -> Self::State;

    /// Applies one replication command.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the command violates store invariants.
    fn apply_replication(
        &mut self,
        command: Self::Command,
    ) -> impl Future<Output = Result<Self::State, Self::Error>> + Send;
}

/// Derived-record append capability with implementation-owned values.
pub trait DerivedRecordSink: Send {
    type Record: Send;
    type Progress: Send;
    type Error: StdError + Send + Sync + 'static;

    /// Appends derived data independently from primary append acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns a typed error when derived progress cannot be persisted.
    fn append_derived(
        &mut self,
        record: Self::Record,
    ) -> impl Future<Output = Result<Self::Progress, Self::Error>> + Send;
}

/// Administrative storage capability with implementation-owned values.
pub trait AdminStore: Send {
    type Request: Send;
    type Response: Send;
    type Error: StdError + Send + Sync + 'static;

    /// Executes one bounded administrative operation.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the operation is rejected or fails.
    fn execute_admin(
        &mut self,
        request: Self::Request,
    ) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;
}

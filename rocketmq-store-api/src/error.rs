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

use rocketmq_error::DomainError;
use rocketmq_error::ErrorCode;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorKind;
use rocketmq_error::Sensitive;

type BoxError = Box<dyn StdError + Send + Sync>;

/// Closed vocabulary for operations that may cross the capability boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StoreOperation {
    Load,
    Start,
    Shutdown,
    Append,
    Flush,
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
            Self::Flush => "flush",
            Self::Read => "read",
            Self::QueryOffset => "query_offset",
            Self::Replicate => "replicate",
            Self::AppendDerived => "append_derived",
            Self::Admin => "admin",
        }
    }
}

/// Stable, low-cardinality storage failure classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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
    /// Every stable storage error kind.
    pub const ALL: &'static [Self] = &[
        Self::NotStarted,
        Self::Unavailable,
        Self::InvalidRequest,
        Self::NotFound,
        Self::Capacity,
        Self::Storage,
        Self::Io,
        Self::Corruption,
        Self::Timeout,
        Self::Unsupported,
        Self::Internal,
    ];

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

    /// Returns the stable storage-domain error code.
    pub const fn code(self) -> ErrorCode {
        ErrorCode::new(match self {
            Self::NotStarted => "STORE_NOT_STARTED",
            Self::Unavailable => "STORE_UNAVAILABLE",
            Self::InvalidRequest => "STORE_INVALID_REQUEST",
            Self::NotFound => "STORE_NOT_FOUND",
            Self::Capacity => "STORE_CAPACITY_EXHAUSTED",
            Self::Storage => "STORE_STORAGE_FAILED",
            Self::Io => "STORE_IO_FAILED",
            Self::Corruption => "STORE_CORRUPTED",
            Self::Timeout => "STORE_TIMEOUT",
            Self::Unsupported => "STORE_UNSUPPORTED",
            Self::Internal => "STORE_INTERNAL",
        })
    }
}

/// Closed vocabulary identifying the storage component that failed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum StoreComponent {
    #[default]
    Store,
    Configuration,
    CommitLog,
    MappedFile,
    RocksDb,
    TieredStore,
    HighAvailability,
    DLedger,
}

impl StoreComponent {
    /// Returns the stable machine-readable component name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Store => "store",
            Self::Configuration => "configuration",
            Self::CommitLog => "commit_log",
            Self::MappedFile => "mapped_file",
            Self::RocksDb => "rocksdb",
            Self::TieredStore => "tiered_store",
            Self::HighAvailability => "high_availability",
            Self::DLedger => "dledger",
        }
    }
}

/// Canonical storage failure shared by the capability and implementation layers.
///
/// Classification fields use closed vocabularies. Diagnostic detail is always
/// treated as sensitive at external boundaries, while typed causes remain
/// available through [`StdError::source`].
#[derive(Debug)]
pub struct StoreError {
    kind: StoreErrorKind,
    operation: StoreOperation,
    component: StoreComponent,
    detail: Option<String>,
    source: Option<BoxError>,
}

impl StoreError {
    /// Creates an error from closed operation and kind vocabularies.
    pub const fn new(kind: StoreErrorKind, operation: StoreOperation) -> Self {
        Self {
            kind,
            operation,
            component: StoreComponent::Store,
            detail: None,
            source: None,
        }
    }

    /// Adds the closed component classification.
    pub const fn in_component(mut self, component: StoreComponent) -> Self {
        self.component = component;
        self
    }

    /// Adds diagnostic-only detail. Boundary projections redact this value.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Preserves a typed cause in the standard source chain.
    pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// Preserves an already boxed typed cause in the standard source chain.
    pub fn with_boxed_source(mut self, source: BoxError) -> Self {
        self.source = Some(source);
        self
    }

    /// Returns the stable failure classification.
    pub const fn kind(&self) -> StoreErrorKind {
        self.kind
    }

    /// Returns the operation that failed.
    pub const fn operation(&self) -> StoreOperation {
        self.operation
    }

    /// Returns the component that failed.
    pub const fn component(&self) -> StoreComponent {
        self.component
    }

    /// Returns diagnostic detail without exposing it to external adapters.
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    /// Creates a configuration validation failure.
    pub fn config(operation: StoreOperation, detail: impl Into<String>) -> Self {
        Self::new(StoreErrorKind::InvalidRequest, operation)
            .in_component(StoreComponent::Configuration)
            .with_detail(detail)
    }

    /// Creates an unsupported-configuration failure.
    pub fn unsupported(operation: StoreOperation, detail: impl Into<String>) -> Self {
        Self::new(StoreErrorKind::Unsupported, operation)
            .in_component(StoreComponent::Configuration)
            .with_detail(detail)
    }

    /// Creates an invalid state-machine transition failure.
    pub fn invalid_state(operation: StoreOperation, detail: impl Into<String>) -> Self {
        Self::new(StoreErrorKind::Internal, operation).with_detail(detail)
    }

    /// Creates a storage failure when no lower-level typed source exists.
    pub fn storage(operation: StoreOperation, detail: impl Into<String>) -> Self {
        Self::new(StoreErrorKind::Storage, operation).with_detail(detail)
    }

    /// Creates a mapped-file failure while preserving its typed source.
    pub fn mapped_file(operation: StoreOperation, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(StoreErrorKind::Storage, operation)
            .in_component(StoreComponent::MappedFile)
            .with_source(source)
    }

    /// Creates a RocksDB failure while preserving its typed source.
    pub fn rocksdb(operation: StoreOperation, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(StoreErrorKind::Storage, operation)
            .in_component(StoreComponent::RocksDb)
            .with_source(source)
    }

    /// Creates a tiered-store failure while preserving its typed source.
    pub fn tiered_store(operation: StoreOperation, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(StoreErrorKind::Unavailable, operation)
            .in_component(StoreComponent::TieredStore)
            .with_source(source)
    }

    /// Creates a high-availability failure while preserving its typed source.
    pub fn high_availability(operation: StoreOperation, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::new(StoreErrorKind::Unavailable, operation)
            .in_component(StoreComponent::HighAvailability)
            .with_source(source)
    }

    /// Creates a DLedger configuration or lifecycle failure.
    pub fn dledger(operation: StoreOperation, detail: impl Into<String>) -> Self {
        Self::new(StoreErrorKind::Unavailable, operation)
            .in_component(StoreComponent::DLedger)
            .with_detail(detail)
    }

    /// Creates a mapped-file lookup failure.
    pub const fn mapped_file_not_found(operation: StoreOperation) -> Self {
        Self::new(StoreErrorKind::NotFound, operation).in_component(StoreComponent::MappedFile)
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.component == StoreComponent::Store {
            write!(
                formatter,
                "store operation {} failed: {}",
                self.operation.as_str(),
                self.kind.as_str()
            )?;
        } else {
            write!(
                formatter,
                "store {} operation {} failed: {}",
                self.component.as_str(),
                self.operation.as_str(),
                self.kind.as_str()
            )?;
        }
        if let Some(detail) = &self.detail {
            write!(formatter, ": {detail}")?;
        }
        Ok(())
    }
}

impl StdError for StoreError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as &(dyn StdError + 'static))
    }
}

impl DomainError for StoreError {
    fn kind(&self) -> ErrorKind {
        match self.kind {
            StoreErrorKind::NotStarted => ErrorKind::NotInitialized,
            StoreErrorKind::Unavailable => ErrorKind::Service,
            StoreErrorKind::InvalidRequest | StoreErrorKind::Unsupported => ErrorKind::IllegalArgument,
            StoreErrorKind::NotFound => ErrorKind::MessageLookupFailed,
            StoreErrorKind::Capacity => ErrorKind::StorageOutOfSpace,
            StoreErrorKind::Storage => match self.operation {
                StoreOperation::Read | StoreOperation::QueryOffset => ErrorKind::StorageReadFailed,
                _ => ErrorKind::StorageWriteFailed,
            },
            StoreErrorKind::Io => ErrorKind::Io,
            StoreErrorKind::Corruption => ErrorKind::StorageCorrupted,
            StoreErrorKind::Timeout => ErrorKind::Timeout,
            StoreErrorKind::Internal => ErrorKind::Internal,
        }
    }

    fn code(&self) -> ErrorCode {
        self.kind.code()
    }

    fn context(&self) -> ErrorContext {
        let mut context = ErrorContext::new()
            .with_field("store_operation", self.operation.as_str())
            .with_field("store_component", self.component.as_str());
        if let Some(detail) = &self.detail {
            context.push_sensitive("store_detail", Sensitive::new(detail.clone()));
        }
        context
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use rocketmq_error::DomainError;
    use rocketmq_error::RedactionPolicy;

    use super::*;

    #[test]
    fn storage_error_exposes_stable_metadata() {
        let error = StoreError::new(StoreErrorKind::Unavailable, StoreOperation::Append)
            .in_component(StoreComponent::HighAvailability);

        assert_eq!(StoreErrorKind::Unavailable, error.kind());
        assert_eq!(StoreOperation::Append, error.operation());
        assert_eq!(StoreComponent::HighAvailability, error.component());
        assert_eq!("STORE_UNAVAILABLE", DomainError::code(&error).as_str());
        assert_eq!(RedactionPolicy::RedactSensitive, error.redaction());
    }

    #[test]
    fn storage_error_preserves_typed_source_and_redacts_detail() {
        let error = StoreError::new(StoreErrorKind::Storage, StoreOperation::Flush)
            .in_component(StoreComponent::MappedFile)
            .with_detail("C:\\secret\\commitlog")
            .with_source(io::Error::other("disk failure"));

        assert_eq!(Some("disk failure"), error.source().map(ToString::to_string).as_deref());
        assert_eq!(
            "<redacted>",
            error
                .boundary_view()
                .context()
                .fields()
                .iter()
                .find(|field| field.key == "store_detail")
                .expect("detail field")
                .value
        );
    }
}

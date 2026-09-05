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

use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::fmt;
use std::panic::Location;

use rocketmq_error::fields;
use rocketmq_error::CanonicalCondition;
use rocketmq_error::DiagnosticView;
use rocketmq_error::Error as CanonicalError;
use rocketmq_error::ErrorCode;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::ErrorSeverity;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RecoveryHint;
use rocketmq_error::Sensitive;
use rocketmq_error::ViewContextViolation;

type BoxError = Box<dyn StdError + Send + Sync>;

/// Closed vocabulary for operations that may cross the capability boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StoreOperation {
    /// Represents the load case.
    Load,
    /// Represents the start case.
    Start,
    /// Represents the shutdown case.
    Shutdown,
    /// Represents the append case.
    Append,
    /// Represents the flush case.
    Flush,
    /// Represents the read case.
    Read,
    /// Represents the query offset case.
    QueryOffset,
    /// Represents the replicate case.
    Replicate,
    /// Represents the append derived case.
    AppendDerived,
    /// Represents the admin case.
    Admin,
}

impl StoreOperation {
    /// Every storage operation accepted by [`StoreError`].
    pub const ALL: &'static [Self] = &[
        Self::Load,
        Self::Start,
        Self::Shutdown,
        Self::Append,
        Self::Flush,
        Self::Read,
        Self::QueryOffset,
        Self::Replicate,
        Self::AppendDerived,
        Self::Admin,
    ];

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

/// Closed vocabulary identifying the storage component that failed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum StoreComponent {
    #[default]
    /// Represents the store case.
    Store,
    /// Represents the configuration case.
    Configuration,
    /// Represents the commit log case.
    CommitLog,
    /// Represents the mapped file case.
    MappedFile,
    /// Represents the rocks db case.
    RocksDb,
    /// Represents the tiered store case.
    TieredStore,
    /// Represents the high availability case.
    HighAvailability,
    /// Represents the dledger case.
    DLedger,
}

impl StoreComponent {
    /// Every storage component accepted by [`StoreError`].
    pub const ALL: &'static [Self] = &[
        Self::Store,
        Self::Configuration,
        Self::CommitLog,
        Self::MappedFile,
        Self::RocksDb,
        Self::TieredStore,
        Self::HighAvailability,
        Self::DLedger,
    ];

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
/// Stable identity and policy come exclusively from the catalog descriptor.
/// Operation and component are typed diagnostic context. Private detail and a
/// typed source are retained for local diagnosis, but their values are never
/// formatted by this facade or exposed through its public view.
pub struct StoreError {
    error: CanonicalError,
    operation: StoreOperation,
    component: StoreComponent,
    private_detail: Option<Sensitive<String>>,
}

impl StoreError {
    /// Creates a storage error with catalog-owned identity.
    ///
    /// Callers select one of the twelve `STORAGE_*` descriptors exported by
    /// `rocketmq-error`; runtime detail must never select or override identity.
    ///
    /// # Panics
    ///
    /// Panics when `descriptor` is not one of the twelve canonical storage
    /// descriptors. This is a programmer contract: runtime failures select a
    /// reviewed storage identity before constructing the facade.
    #[track_caller]
    pub fn new(descriptor: &'static ErrorDescriptor, operation: StoreOperation) -> Self {
        Self::try_new(descriptor, operation)
            .expect("StoreError requires one of the twelve canonical storage descriptors")
    }

    /// Attempts to create a storage error with a validated catalog identity.
    ///
    /// Returns `None` when `descriptor` belongs to another error domain. The
    /// facade never accepts caller-provided context, so successful construction
    /// also guarantees that only the four storage context fields can be emitted.
    #[track_caller]
    pub fn try_new(descriptor: &'static ErrorDescriptor, operation: StoreOperation) -> Option<Self> {
        if !is_storage_descriptor(descriptor) {
            return None;
        }
        let component = StoreComponent::Store;
        Some(Self {
            error: CanonicalError::new(descriptor).with_context(store_context(operation, component, false, false)),
            operation,
            component,
            private_detail: None,
        })
    }

    /// Adds the closed component classification as diagnostic context.
    pub fn in_component(mut self, component: StoreComponent) -> Self {
        self.component = component;
        self.rebuild_context()
    }

    /// Retains sensitive diagnostic detail without exposing its value.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.private_detail = Some(Sensitive::new(detail.into()));
        self.rebuild_context()
    }

    /// Preserves a typed cause in the standard source chain.
    pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.error = self.error.with_source(source);
        self.rebuild_context()
    }

    /// Preserves an already boxed typed cause in the standard source chain.
    pub fn with_boxed_source(mut self, source: BoxError) -> Self {
        self.error = self.error.with_boxed_source(source);
        self.rebuild_context()
    }

    /// Returns the immutable catalog descriptor that owns this error's identity.
    pub const fn descriptor(&self) -> &'static ErrorDescriptor {
        self.error.descriptor()
    }

    /// Returns the stable dotted catalog code.
    pub const fn code(&self) -> ErrorCode {
        self.error.code()
    }

    /// Returns the protocol-independent condition.
    pub const fn condition(&self) -> CanonicalCondition {
        self.error.condition()
    }

    /// Returns the catalog-owned severity.
    pub const fn severity(&self) -> ErrorSeverity {
        self.error.severity()
    }

    /// Returns the catalog-owned recovery hint.
    pub const fn recovery_hint(&self) -> RecoveryHint {
        self.error.recovery_hint()
    }

    /// Returns the operation that failed.
    pub const fn operation(&self) -> StoreOperation {
        self.operation
    }

    /// Returns the component that failed.
    pub const fn component(&self) -> StoreComponent {
        self.component
    }

    /// Returns the bounded context retained by the canonical error.
    pub const fn context(&self) -> &ErrorContext {
        self.error.context()
    }

    /// Returns the first-promotion caller location.
    pub const fn location(&self) -> &'static Location<'static> {
        self.error.location()
    }

    /// Returns the catalog-controlled captured backtrace, when enabled.
    pub const fn backtrace(&self) -> Option<&Backtrace> {
        self.error.backtrace()
    }

    /// Creates the descriptor-validated public projection.
    ///
    /// # Errors
    ///
    /// Returns a schema violation if the catalog and the internally generated
    /// storage context become inconsistent.
    pub fn public_view(&self) -> Result<PublicErrorView<'_>, ViewContextViolation> {
        self.error.public_view()
    }

    /// Creates the descriptor-validated controlled diagnostic projection.
    ///
    /// # Errors
    ///
    /// Returns a schema violation if the catalog and the internally generated
    /// storage context become inconsistent.
    pub fn diagnostic_view(&self) -> Result<DiagnosticView<'_>, ViewContextViolation> {
        self.error.diagnostic_view()
    }

    fn rebuild_context(self) -> Self {
        let context = store_context(
            self.operation,
            self.component,
            self.private_detail.is_some(),
            self.error.source().is_some(),
        );
        Self {
            error: self.error.with_context(context),
            ..self
        }
    }
}

fn is_storage_descriptor(descriptor: &ErrorDescriptor) -> bool {
    matches!(
        descriptor.code().as_str(),
        "storage.lifecycle.not_started"
            | "storage.backend.unavailable"
            | "storage.request.invalid"
            | "storage.mapped_file.not_found"
            | "storage.capacity.exhausted"
            | "storage.read.failed"
            | "storage.write.failed"
            | "storage.io.failed"
            | "storage.state.corrupted"
            | "storage.operation.timed_out"
            | "storage.operation.unsupported"
            | "storage.internal.failure"
    )
}

fn store_context(
    operation: StoreOperation,
    component: StoreComponent,
    detail_present: bool,
    source_present: bool,
) -> ErrorContext {
    let mut context = ErrorContext::new()
        .with_text(fields::STORE_OPERATION, operation.as_str())
        .with_text(fields::STORE_COMPONENT, component.as_str());
    if detail_present {
        context = context.with_secret_presence(fields::STORE_DETAIL_PRESENT);
    }
    if source_present {
        context = context.with_secret_presence(fields::SOURCE_PRESENT);
    }
    context
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, formatter)
    }
}

impl fmt::Debug for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreError")
            .field("code", &self.code())
            .field("message", &self.descriptor().public_message())
            .field("operation", &self.operation)
            .field("component", &self.component)
            .field("detail_present", &self.private_detail.is_some())
            .field("source_present", &self.error.source().is_some())
            .finish()
    }
}

impl StdError for StoreError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.error.source()
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use rocketmq_error::ContextVisibility;
    use rocketmq_error::ViewValueRef;
    use rocketmq_error::PROTOCOL_HEADER_INVALID;
    use rocketmq_error::STORAGE_BACKEND_UNAVAILABLE;
    use rocketmq_error::STORAGE_WRITE_FAILED;

    use super::*;

    #[derive(Debug)]
    struct StoreCause {
        source: io::Error,
    }

    impl fmt::Display for StoreCause {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("store cause")
        }
    }

    impl StdError for StoreCause {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(&self.source)
        }
    }

    #[test]
    fn storage_vocabularies_remain_exact() {
        assert_eq!(StoreOperation::ALL.len(), 10);
        assert_eq!(StoreComponent::ALL.len(), 8);
    }

    #[test]
    fn storage_error_exposes_catalog_metadata() {
        let error = StoreError::new(&STORAGE_BACKEND_UNAVAILABLE, StoreOperation::Append)
            .in_component(StoreComponent::HighAvailability);

        assert_eq!(&STORAGE_BACKEND_UNAVAILABLE, error.descriptor());
        assert_eq!("storage.backend.unavailable", error.code().as_str());
        assert_eq!(StoreOperation::Append, error.operation());
        assert_eq!(StoreComponent::HighAvailability, error.component());
    }

    #[test]
    fn storage_error_rejects_non_storage_catalog_identity() {
        assert!(StoreError::try_new(&PROTOCOL_HEADER_INVALID, StoreOperation::Read).is_none());
        assert!(
            std::panic::catch_unwind(|| { StoreError::new(&PROTOCOL_HEADER_INVALID, StoreOperation::Read) }).is_err()
        );
    }

    #[test]
    fn storage_error_preserves_typed_source_and_safe_views() {
        let error = StoreError::new(&STORAGE_WRITE_FAILED, StoreOperation::Flush)
            .in_component(StoreComponent::MappedFile)
            .with_detail("C:\\secret\\commitlog")
            .with_source(io::Error::other("disk-secret"));

        assert!(error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some());
        assert_eq!("storage.write.failed: Storage write failed", error.to_string());
        assert!(!error.to_string().contains("secret"));
        assert!(!format!("{error:?}").contains("disk-secret"));
        assert!(!format!("{error:?}").contains("commitlog"));

        let public = error.public_view().expect("valid public view");
        assert!(public.fields().next().is_none());

        let fields = error
            .diagnostic_view()
            .expect("valid diagnostic view")
            .fields()
            .map(|field| (field.name(), field.visibility(), field.value()))
            .collect::<Vec<_>>();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].2, ViewValueRef::Text("flush"));
        assert_eq!(fields[1].2, ViewValueRef::Text("mapped_file"));
        assert_eq!(fields[2].1, ContextVisibility::SecretPresenceOnly);
        assert_eq!(fields[2].2, ViewValueRef::Redacted);
        assert_eq!(fields[3].2, ViewValueRef::Redacted);
    }

    #[test]
    fn storage_error_preserves_causal_order_and_downcasts() {
        let error = StoreError::new(&STORAGE_WRITE_FAILED, StoreOperation::Flush).with_source(StoreCause {
            source: io::Error::other("typed leaf"),
        });

        let cause = error
            .source()
            .and_then(|source| source.downcast_ref::<StoreCause>())
            .expect("first cause remains typed");
        let leaf = cause
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .expect("leaf cause follows the wrapper");
        assert_eq!(leaf.to_string(), "typed leaf");
        assert!(leaf.source().is_none());
    }

    #[test]
    fn storage_error_preserves_an_already_boxed_source_as_the_direct_leaf() {
        let source: BoxError = Box::new(io::Error::other("boxed leaf"));
        let error = StoreError::new(&STORAGE_WRITE_FAILED, StoreOperation::Flush).with_boxed_source(source);

        let leaf = error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .expect("boxed source remains the direct typed leaf");
        assert_eq!(leaf.to_string(), "boxed leaf");
    }

    #[test]
    fn storage_error_builders_preserve_first_promotion_provenance() {
        let caller_line = line!() + 1;
        let error = StoreError::new(&STORAGE_WRITE_FAILED, StoreOperation::Flush)
            .in_component(StoreComponent::MappedFile)
            .with_detail("private detail")
            .with_source(io::Error::other("typed leaf"));

        assert_eq!(error.location().file(), file!());
        assert_eq!(error.location().line(), caller_line);
        assert_eq!(error.context().len(), 4);
    }
}

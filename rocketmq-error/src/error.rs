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

use std::backtrace::Backtrace;
use std::backtrace::BacktraceStatus;
use std::error::Error as StdError;
use std::fmt;
use std::panic::Location;
use std::sync::Arc;

use crate::BacktracePolicy;
use crate::CanonicalCondition;
use crate::ComponentId;
use crate::DiagnosticView;
use crate::ErrorClass;
use crate::ErrorCode;
use crate::ErrorContext;
use crate::ErrorDescriptor;
use crate::ErrorSeverity;
use crate::Exposure;
use crate::FaultAttribution;
use crate::ProjectionSpec;
use crate::PublicErrorView;
use crate::RecoveryHint;
use crate::ViewContextViolation;

type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// Opaque canonical RocketMQ error.
///
/// Identity and policy are owned by a reviewed catalog descriptor. Each value
/// retains typed context, at most one direct typed source, its first-promotion
/// caller location, and an optional catalog-controlled backtrace behind one
/// boxed pointer.
///
/// `Error` is intentionally not cloneable. Use [`SharedError`] when multiple
/// owners must observe the same error instance.
///
/// ```compile_fail
/// use rocketmq_error::{Error, CORE_INTERNAL_FAILURE};
///
/// let error = Error::new(&CORE_INTERNAL_FAILURE);
/// let _copy = error.clone();
/// ```
pub struct Error {
    inner: Box<ErrorInner>,
}

struct ErrorInner {
    descriptor: &'static ErrorDescriptor,
    context: ErrorContext,
    source: Option<BoxError>,
    location: &'static Location<'static>,
    backtrace: Option<Backtrace>,
}

/// Result whose error is the canonical [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// Shared ownership of one canonical [`Error`] instance.
pub type SharedError = Arc<Error>;

impl Error {
    /// Creates a source-free canonical error from a catalog descriptor.
    #[must_use]
    #[track_caller]
    pub fn new(descriptor: &'static ErrorDescriptor) -> Self {
        Self::from_parts(descriptor, None)
    }

    /// Creates a canonical error while retaining the direct typed source.
    #[must_use]
    #[track_caller]
    pub fn caused_by(descriptor: &'static ErrorDescriptor, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::from_parts(descriptor, Some(Box::new(source)))
    }

    #[track_caller]
    fn from_parts(descriptor: &'static ErrorDescriptor, source: Option<BoxError>) -> Self {
        Self {
            inner: Box::new(ErrorInner {
                descriptor,
                context: ErrorContext::new(),
                source,
                location: Location::caller(),
                backtrace: capture_backtrace(descriptor.backtrace_policy()),
            }),
        }
    }

    /// Replaces the typed context without changing source or provenance.
    #[must_use]
    pub fn with_context(mut self, context: ErrorContext) -> Self {
        self.inner.context = context;
        self
    }

    /// Replaces the direct typed source without changing context or provenance.
    #[must_use]
    pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.inner.source = Some(Box::new(source));
        self
    }

    /// Replaces the direct boxed source without adding another causal layer.
    #[must_use]
    pub fn with_boxed_source(mut self, source: Box<dyn StdError + Send + Sync + 'static>) -> Self {
        self.inner.source = Some(source);
        self
    }

    /// Returns the catalog descriptor that owns identity and policy.
    #[must_use]
    pub const fn descriptor(&self) -> &'static ErrorDescriptor {
        self.inner.descriptor
    }

    /// Returns the stable catalog code.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.descriptor().code()
    }

    /// Returns the broad catalog-owned class.
    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        self.descriptor().class()
    }

    /// Returns the protocol-independent canonical condition.
    #[must_use]
    pub const fn condition(&self) -> CanonicalCondition {
        self.descriptor().condition()
    }

    /// Returns the catalog-owned fault attribution.
    #[must_use]
    pub const fn fault(&self) -> FaultAttribution {
        self.descriptor().fault()
    }

    /// Returns the catalog component that owns this identity.
    #[must_use]
    pub const fn component(&self) -> ComponentId {
        self.descriptor().component()
    }

    /// Returns the catalog-owned severity.
    #[must_use]
    pub const fn severity(&self) -> ErrorSeverity {
        self.descriptor().severity()
    }

    /// Returns the catalog-owned recovery hint.
    #[must_use]
    pub const fn recovery_hint(&self) -> RecoveryHint {
        self.descriptor().recovery_hint()
    }

    /// Returns the public-context exposure policy.
    #[must_use]
    pub const fn exposure(&self) -> Exposure {
        self.descriptor().exposure()
    }

    /// Returns the catalog-controlled backtrace policy.
    #[must_use]
    pub const fn backtrace_policy(&self) -> BacktracePolicy {
        self.descriptor().backtrace_policy()
    }

    /// Returns descriptor-owned boundary projections.
    #[must_use]
    pub const fn projection(&self) -> ProjectionSpec {
        self.descriptor().projection()
    }

    /// Returns the typed context retained by this error.
    #[must_use]
    pub const fn context(&self) -> &ErrorContext {
        &self.inner.context
    }

    /// Creates a descriptor-validated public projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the retained context does not match the selected
    /// catalog descriptor.
    pub fn public_view(&self) -> std::result::Result<PublicErrorView<'_>, ViewContextViolation> {
        PublicErrorView::try_new(self.descriptor(), self.context())
    }

    /// Creates a descriptor-validated diagnostic projection.
    ///
    /// The view never includes the source, caller location, or backtrace.
    ///
    /// # Errors
    ///
    /// Returns an error when the retained context does not match the selected
    /// catalog descriptor.
    pub fn diagnostic_view(&self) -> std::result::Result<DiagnosticView<'_>, ViewContextViolation> {
        DiagnosticView::try_new(self.descriptor(), self.context())
    }

    /// Returns the first-promotion caller location.
    #[must_use]
    pub const fn location(&self) -> &'static Location<'static> {
        self.inner.location
    }

    /// Returns the captured backtrace, if standard-library capture was enabled.
    #[must_use]
    pub const fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace.as_ref()
    }
}

fn capture_backtrace(policy: BacktracePolicy) -> Option<Backtrace> {
    match policy {
        BacktracePolicy::Never => None,
        BacktracePolicy::OnDemand => {
            let backtrace = Backtrace::capture();
            matches!(backtrace.status(), BacktraceStatus::Captured).then_some(backtrace)
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.descriptor().public_message())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Error")
            .field("code", &self.code())
            .field("class", &self.class())
            .field("condition", &self.condition())
            .field("fault", &self.fault())
            .field("component", &self.component())
            .field("severity", &self.severity())
            .field("recovery_hint", &self.recovery_hint())
            .field("exposure", &self.exposure())
            .field("backtrace_policy", &self.backtrace_policy())
            .field("context", &self.context())
            .field("source_present", &self.inner.source.is_some())
            .field("backtrace_captured", &self.inner.backtrace.is_some())
            .finish()
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner
            .source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

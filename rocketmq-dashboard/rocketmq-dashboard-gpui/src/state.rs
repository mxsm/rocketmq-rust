// Copyright 2025 The RocketMQ Rust Authors
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

//! Pure, reusable UI state primitives.

use std::{error::Error as StdError, fmt, sync::Arc};

/// The read state for a screen resource.
///
/// A refresh and a failure can retain the previous value so a temporary request failure does not
/// unnecessarily clear useful data from a page.
#[derive(Clone, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "Feature entities begin consuming the shared read-state primitive after the foundation PR."
)]
pub enum Loadable<T> {
    /// The resource has not been requested yet.
    Idle,
    /// The first request is in flight.
    InitialLoading,
    /// A request completed with data.
    Ready(T),
    /// A subsequent request is in flight while the previous value remains visible.
    Refreshing(T),
    /// A completed request had no results.
    Empty,
    /// A request failed, optionally retaining the last successful value.
    Failed {
        /// The value that remains usable while the caller displays the failure.
        previous: Option<T>,
        /// A user-safe failure description.
        error: UiError,
    },
}

#[allow(
    dead_code,
    reason = "Feature entities begin consuming the shared read-state primitive after the foundation PR."
)]
impl<T> Loadable<T> {
    /// Starts a request, retaining a successful value for a non-blocking refresh.
    pub fn begin(self) -> Self {
        match self {
            Self::Ready(value) | Self::Refreshing(value) => Self::Refreshing(value),
            Self::Failed {
                previous: Some(value), ..
            } => Self::Refreshing(value),
            Self::Idle | Self::InitialLoading | Self::Empty | Self::Failed { previous: None, .. } => {
                Self::InitialLoading
            }
        }
    }

    /// Completes a request that returned data.
    pub fn ready(value: T) -> Self {
        Self::Ready(value)
    }

    /// Completes a request that returned no data.
    pub const fn empty() -> Self {
        Self::Empty
    }

    /// Records a failure and retains a value when one was available before the request.
    pub fn fail(self, error: UiError) -> Self {
        let previous = match self {
            Self::Ready(value) | Self::Refreshing(value) => Some(value),
            Self::Failed { previous, .. } => previous,
            Self::Idle | Self::InitialLoading | Self::Empty => None,
        };

        Self::Failed { previous, error }
    }

    /// Returns the currently displayable value, including one retained during refresh or failure.
    pub fn value(&self) -> Option<&T> {
        match self {
            Self::Ready(value) | Self::Refreshing(value) => Some(value),
            Self::Failed {
                previous: Some(value), ..
            } => Some(value),
            Self::Idle | Self::InitialLoading | Self::Empty | Self::Failed { previous: None, .. } => None,
        }
    }
}

impl<T> fmt::Debug for Loadable<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (variant, value_available) = match self {
            Self::Idle => ("Idle", false),
            Self::InitialLoading => ("InitialLoading", false),
            Self::Ready(_) => ("Ready", true),
            Self::Refreshing(_) => ("Refreshing", true),
            Self::Empty => ("Empty", false),
            Self::Failed { previous, .. } => ("Failed", previous.is_some()),
        };

        formatter
            .debug_struct("Loadable")
            .field("variant", &variant)
            .field("value_available", &value_available)
            .finish()
    }
}

/// The state of a write operation, independent from the queried page resource.
#[derive(Clone, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "Write dialogs introduced in later deliveries consume this shared mutation-state primitive."
)]
pub enum MutationState<R> {
    /// No mutation is in progress.
    Idle,
    /// The mutation request is in flight.
    Submitting,
    /// All requested targets completed successfully.
    Succeeded(R),
    /// At least one target completed while another did not.
    PartiallySucceeded(R),
    /// The mutation failed before it produced a usable result.
    Failed(UiError),
}

#[allow(
    dead_code,
    reason = "Write dialogs introduced in later deliveries consume this shared mutation-state primitive."
)]
impl<R> MutationState<R> {
    /// Marks a mutation as in progress.
    pub const fn submitting() -> Self {
        Self::Submitting
    }

    /// Stores a complete mutation result.
    pub fn succeeded(result: R) -> Self {
        Self::Succeeded(result)
    }

    /// Stores a target-level result when only part of an operation succeeded.
    pub fn partially_succeeded(result: R) -> Self {
        Self::PartiallySucceeded(result)
    }

    /// Stores a user-safe failure.
    pub fn failed(error: UiError) -> Self {
        Self::Failed(error)
    }
}

impl<R> fmt::Debug for MutationState<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (variant, result_available) = match self {
            Self::Idle => ("Idle", false),
            Self::Submitting => ("Submitting", false),
            Self::Succeeded(_) => ("Succeeded", true),
            Self::PartiallySucceeded(_) => ("PartiallySucceeded", true),
            Self::Failed(_) => ("Failed", false),
        };

        formatter
            .debug_struct("MutationState")
            .field("variant", &variant)
            .field("result_available", &result_available)
            .finish()
    }
}

/// A monotonically increasing identifier for a resource request.
///
/// A feature stores the latest value when it starts work and accepts a response only when its
/// captured epoch still matches. A configuration or scope change advances the epoch as well.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestEpoch(u64);

impl RequestEpoch {
    /// Creates the initial epoch for a resource.
    pub const fn initial() -> Self {
        Self(0)
    }

    /// Advances the resource epoch and returns the epoch captured by the new request.
    pub fn advance(&mut self) -> Result<Self, RequestEpochExhausted> {
        self.0 = self.0.checked_add(1).ok_or(RequestEpochExhausted)?;
        Ok(*self)
    }

    /// Returns whether a response captured for `request` still belongs to this resource state.
    pub const fn accepts(self, request: Self) -> bool {
        self.0 == request.0
    }

    /// Exposes the epoch for focused tests.
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// The process has issued every representable request epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestEpochExhausted;

/// A stable category for user-facing errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UiErrorCode {
    /// A local configuration could not be read or validated.
    #[allow(
        dead_code,
        reason = "The connection delivery adds concrete configuration failures to this stable UI category."
    )]
    Configuration,
    /// Authentication or session establishment failed.
    Authentication,
    /// The dashboard could not reach the selected RocketMQ endpoint.
    #[allow(
        dead_code,
        reason = "The connection delivery adds concrete endpoint failures to this stable UI category."
    )]
    Connection,
    /// User-provided input was invalid.
    #[allow(
        dead_code,
        reason = "Feature forms introduce validation failures after the foundation shell."
    )]
    Validation,
    /// A later delivery has not supplied the requested capability yet.
    CapabilityUnavailable,
    /// A failure does not fit one of the stable categories above.
    Unknown,
}

/// A user-facing error that deliberately avoids retaining sensitive diagnostic data in `Debug`.
#[derive(Clone)]
pub(crate) struct UiError {
    inner: Arc<UiErrorInner>,
}

struct UiErrorInner {
    summary: &'static str,
    code: UiErrorCode,
    retryable: bool,
    source: Option<Arc<dyn StdError + Send + Sync>>,
}

impl UiError {
    /// Creates an error with a user-readable summary and a stable category.
    pub(crate) fn new(summary: &'static str, code: UiErrorCode, retryable: bool) -> Self {
        Self {
            inner: Arc::new(UiErrorInner {
                summary,
                code,
                retryable,
                source: None,
            }),
        }
    }

    /// Creates a fixed user-facing projection while retaining the typed operational cause.
    pub(crate) fn caused_by(
        summary: &'static str,
        code: UiErrorCode,
        retryable: bool,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(UiErrorInner {
                summary,
                code,
                retryable,
                source: Some(Arc::new(source)),
            }),
        }
    }

    /// Returns the summary intended for the current screen.
    pub fn summary(&self) -> &str {
        self.inner.summary
    }

    /// Returns whether retry is appropriate without changing user input.
    pub fn is_retryable(&self) -> bool {
        self.inner.retryable
    }
}

impl PartialEq for UiError {
    fn eq(&self, other: &Self) -> bool {
        self.inner.summary == other.inner.summary
            && self.inner.code == other.inner.code
            && self.inner.retryable == other.inner.retryable
    }
}

impl Eq for UiError {}

impl fmt::Debug for UiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UiError")
            .field("code", &self.inner.code)
            .field("retryable", &self.inner.retryable)
            .field("source_available", &self.inner.source.is_some())
            .finish()
    }
}

impl fmt::Display for UiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.inner.summary)
    }
}

impl StdError for UiError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner.source.as_deref().map(|source| source as _)
    }
}

#[cfg(test)]
mod tests {
    use super::{Loadable, MutationState, RequestEpoch, UiError, UiErrorCode};

    fn connection_error() -> UiError {
        UiError::new(
            "Unable to reach the selected NameServer.",
            UiErrorCode::Connection,
            true,
        )
    }

    #[test]
    fn refresh_and_failure_keep_the_previous_value() {
        let state = Loadable::ready("broker-a".to_string()).begin();
        assert_eq!(state.value(), Some(&"broker-a".to_string()));

        let state = state.fail(connection_error());
        assert_eq!(state.value(), Some(&"broker-a".to_string()));
        assert!(matches!(state, Loadable::Failed { previous: Some(_), .. }));
    }

    #[test]
    fn request_epochs_reject_stale_responses() {
        let mut latest = RequestEpoch::initial();
        let first_request = latest.advance().expect("the first request epoch must be available");
        let second_request = latest.advance().expect("the second request epoch must be available");

        assert!(!latest.accepts(first_request));
        assert!(latest.accepts(second_request));
        assert_eq!(latest.value(), 2);
    }

    #[test]
    fn mutations_model_partial_success_separately() {
        let state = MutationState::partially_succeeded(vec!["broker-a"]);

        assert!(matches!(state, MutationState::PartiallySucceeded(_)));
    }

    #[test]
    fn error_debug_output_does_not_include_sources() {
        #[derive(Debug, thiserror::Error)]
        #[error("configuration revision 42 contains secret-value")]
        struct SensitiveSource;

        let error = UiError::caused_by(
            "Unable to save configuration.",
            UiErrorCode::Configuration,
            true,
            SensitiveSource,
        );

        let debug = format!("{error:?}");
        assert!(!debug.contains("configuration revision 42"));
        assert!(!debug.contains("secret-value"));
        assert!(debug.contains("source_available: true"));
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn state_debug_redacts_values_and_does_not_require_debug_bounds() {
        struct NotDebug;

        let secret = "super-secret-message-body";
        let loadable_debug = format!("{:?}", Loadable::ready(secret.to_owned()));
        let failed_loadable_debug = format!("{:?}", Loadable::ready(secret.to_owned()).fail(connection_error()));
        let mutation_debug = format!("{:?}", MutationState::succeeded(secret.to_owned()));
        let partial_mutation_debug = format!("{:?}", MutationState::partially_succeeded(secret.to_owned()));
        let opaque_loadable_debug = format!("{:?}", Loadable::ready(NotDebug));
        let opaque_mutation_debug = format!("{:?}", MutationState::succeeded(NotDebug));

        for debug in [
            &loadable_debug,
            &failed_loadable_debug,
            &mutation_debug,
            &partial_mutation_debug,
            &opaque_loadable_debug,
            &opaque_mutation_debug,
        ] {
            assert!(!debug.contains(secret));
        }
        assert!(loadable_debug.contains("value_available: true"));
        assert!(failed_loadable_debug.contains("variant: \"Failed\""));
        assert!(mutation_debug.contains("result_available: true"));
        assert!(partial_mutation_debug.contains("variant: \"PartiallySucceeded\""));
    }
}

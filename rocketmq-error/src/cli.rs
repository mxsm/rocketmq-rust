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

//! CLI-facing error projection.

use crate::CliExitCode;
use crate::DiagnosticView;
use crate::Error;
use crate::ErrorContext;
use crate::ErrorDescriptor;
use crate::PublicErrorView;
use crate::SharedError;
use crate::ViewValueRef;

/// Controls how much safe diagnostic information a CLI error line contains.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CliVerbosity {
    /// Emits only the stable code and fixed public message.
    #[default]
    Default,
    /// Appends descriptor-approved, redaction-aware diagnostic fields.
    Verbose,
}

/// Complete process-boundary projection for one CLI failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliOutput {
    exit_code: CliExitCode,
    stderr: String,
}

impl CliOutput {
    /// Returns the descriptor-owned process exit code.
    #[inline]
    pub const fn exit_code(&self) -> CliExitCode {
        self.exit_code
    }

    /// Returns the single-line, redaction-safe stderr payload.
    #[inline]
    pub fn stderr(&self) -> &str {
        &self.stderr
    }
}

/// Stable error view for command-line tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliErrorView {
    descriptor: &'static ErrorDescriptor,
    context: ErrorContext,
}

impl CliErrorView {
    /// Builds a CLI view from the canonical descriptor catalog.
    #[inline]
    pub fn from_error(error: &Error) -> Self {
        Self {
            descriptor: error.descriptor(),
            context: error.context().clone(),
        }
    }

    /// Builds a CLI view from a shared canonical error allocation.
    #[inline]
    pub fn from_shared_error(error: &SharedError) -> Self {
        Self::from_error(error.as_ref())
    }

    /// Produces the exit code and stderr line from one catalog projection.
    ///
    /// Default output contains only the stable code and fixed public message.
    /// Verbose output may append descriptor-approved, bounded diagnostic fields
    /// with secret-bearing values represented only as `<redacted>`. Neither
    /// mode renders source errors, locations, or backtraces.
    pub fn output(&self, verbosity: CliVerbosity) -> CliOutput {
        let public = PublicErrorView::try_new(self.descriptor, &self.context)
            .unwrap_or_else(|_| PublicErrorView::descriptor_only(self.descriptor));
        let mut stderr = format!("ERROR {}: {}", public.code(), public.message());

        if verbosity == CliVerbosity::Verbose {
            append_diagnostics(&mut stderr, self.descriptor, &self.context);
        }

        CliOutput {
            exit_code: public.projection().cli().exit_code,
            stderr,
        }
    }
}

fn append_diagnostics(stderr: &mut String, descriptor: &'static ErrorDescriptor, context: &ErrorContext) {
    let Ok(view) = DiagnosticView::try_new(descriptor, context) else {
        return;
    };
    let mut rendered = String::new();
    for field in view.fields() {
        if !rendered.is_empty() {
            rendered.push_str(", ");
        }
        rendered.push_str(field.name());
        rendered.push('=');
        let value = match field.value() {
            ViewValueRef::Text(value) => format!("{value:?}"),
            ViewValueRef::I64(value) => value.to_string(),
            ViewValueRef::U64(value) => value.to_string(),
            ViewValueRef::Bool(value) => value.to_string(),
            ViewValueRef::Redacted => "<redacted>".to_string(),
        };
        rendered.push_str(&value);
    }
    if !rendered.is_empty() {
        stderr.push_str("; details={");
        stderr.push_str(&rendered);
        stderr.push('}');
    }
    if view.is_truncated() {
        stderr.push_str("; truncated=true");
    }
}

impl From<&Error> for CliErrorView {
    #[inline]
    fn from(error: &Error) -> Self {
        Self::from_error(error)
    }
}

impl From<&SharedError> for CliErrorView {
    #[inline]
    fn from(error: &SharedError) -> Self {
        Self::from_shared_error(error)
    }
}

#[cfg(test)]
mod tests {
    use crate::CliErrorView;
    use crate::CliExitCode;
    use crate::CliVerbosity;
    use crate::Error;
    use crate::ErrorContext;

    #[test]
    fn cli_view_uses_descriptor_exit_code_and_stable_code() {
        let error = Error::new(&crate::CORE_ARGUMENT_INVALID)
            .with_context(ErrorContext::new().with_secret_presence(crate::fields::MESSAGE_PRESENT));
        let output = CliErrorView::from_error(&error).output(CliVerbosity::Default);

        assert_eq!(output.exit_code(), CliExitCode::USAGE);
        assert_eq!(output.stderr(), "ERROR core.argument.invalid: Argument is invalid");
    }

    #[test]
    fn default_stderr_never_contains_context_or_source_text() {
        let error = Error::new(&crate::STORAGE_READ_FAILED).with_context(
            ErrorContext::new()
                .with_secret_presence(crate::fields::PATH_PRESENT)
                .with_secret_presence(crate::fields::SOURCE_PRESENT),
        );
        let output = CliErrorView::from_error(&error).output(CliVerbosity::Default);
        let rendered = output.stderr();

        assert_eq!(rendered, "ERROR storage.read.failed: Storage read failed");
        assert!(!rendered.contains("secret/token"));
        assert!(!rendered.contains("permission denied"));

        let route_error = Error::new(&crate::ROUTE_TOPIC_NOT_FOUND)
            .with_context(ErrorContext::new().with_text(crate::fields::TOPIC, "TopicA"));
        let route = CliErrorView::from_error(&route_error).output(CliVerbosity::Default);
        assert_eq!(route.stderr(), "ERROR route.topic.not_found: Topic route was not found");
    }

    #[test]
    fn verbose_stderr_uses_only_controlled_diagnostic_fields() {
        let error = Error::new(&crate::CORE_ARGUMENT_INVALID)
            .with_context(ErrorContext::new().with_secret_presence(crate::fields::MESSAGE_PRESENT));
        let output = CliErrorView::from_error(&error).output(CliVerbosity::Verbose);
        let rendered = output.stderr();

        assert!(rendered.contains("message=<redacted>"));
        assert!(!rendered.contains("plain-text"));
        assert!(!rendered.contains("private"));
        assert!(!rendered.contains("xxxx"));
        assert!(!rendered.contains('\n'));
        assert!(!rendered.contains('\r'));
    }

    #[test]
    fn verbose_stderr_preserves_typed_scalar_fields() {
        let error = Error::new(&crate::BROKER_MESSAGE_TOO_LARGE).with_context(
            ErrorContext::new()
                .with_u64(crate::fields::ACTUAL_BYTES, 7)
                .with_u64(crate::fields::LIMIT_BYTES, 9),
        );
        let output = CliErrorView::from_error(&error).output(CliVerbosity::Verbose);
        let rendered = output.stderr();

        assert!(rendered.contains("actual_bytes=7"));
        assert!(rendered.contains("limit_bytes=9"));

        let error = Error::new(&crate::OBSERVABILITY_SUBSCRIBER_INSTALLATION_FAILED).with_context(
            ErrorContext::new()
                .with_bool(crate::fields::ATTEMPTED, true)
                .with_bool(crate::fields::INSTALLED, false),
        );
        let output = CliErrorView::from_error(&error).output(CliVerbosity::Verbose);
        assert!(output.stderr().contains("attempted=true"));
        assert!(output.stderr().contains("installed=false"));
    }

    #[test]
    fn verbosity_never_changes_the_descriptor_exit_code() {
        let error = Error::new(&crate::ROUTE_TOPIC_NOT_FOUND)
            .with_context(ErrorContext::new().with_text(crate::fields::TOPIC, "TopicA"));
        let view = CliErrorView::from_error(&error);

        assert_eq!(
            view.output(CliVerbosity::Default).exit_code(),
            view.output(CliVerbosity::Verbose).exit_code()
        );
    }

    #[test]
    fn every_supported_cli_exit_category_is_descriptor_owned() {
        let cases = [
            (&crate::CORE_ARGUMENT_INVALID, CliExitCode::USAGE),
            (&crate::CORE_LIFECYCLE_NOT_INITIALIZED, CliExitCode::DATA),
            (&crate::ROUTE_CLUSTER_NOT_FOUND, CliExitCode::NOT_FOUND),
            (&crate::TRANSPORT_CONNECTION_FAILED, CliExitCode::UNAVAILABLE),
            (&crate::CORE_INTERNAL_FAILURE, CliExitCode::SOFTWARE),
            (&crate::CORE_OPERATION_TIMED_OUT, CliExitCode::TEMPORARY_FAILURE),
            (&crate::PROXY_BROKER_PERMISSION_DENIED, CliExitCode::PERMISSION),
            (&crate::CORE_CONFIGURATION_INVALID, CliExitCode::CONFIG),
        ];

        for (descriptor, expected) in cases {
            let view = CliErrorView {
                descriptor,
                context: crate::ErrorContext::new(),
            };
            assert_eq!(view.output(CliVerbosity::Default).exit_code(), expected);
            assert_eq!(view.output(CliVerbosity::Verbose).exit_code(), expected);
        }
    }

    #[test]
    fn invalid_context_fails_closed_without_losing_descriptor_identity() {
        let view = CliErrorView {
            descriptor: &crate::ROUTE_TOPIC_NOT_FOUND,
            context: crate::ErrorContext::new().with_text(crate::fields::GROUP, "secret-group"),
        };

        let default = view.output(CliVerbosity::Default);
        let verbose = view.output(CliVerbosity::Verbose);
        assert_eq!(
            default.stderr(),
            "ERROR route.topic.not_found: Topic route was not found"
        );
        assert_eq!(verbose.stderr(), default.stderr());
        assert_eq!(verbose.exit_code(), default.exit_code());
        assert!(!verbose.stderr().contains("secret-group"));
    }
}

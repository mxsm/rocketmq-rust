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
use crate::ErrorCategory;
use crate::ErrorCode;
use crate::ErrorContext;
use crate::RocketMQError;

/// Stable error view for command-line tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliErrorView {
    exit_code: CliExitCode,
    code: ErrorCode,
    category: ErrorCategory,
    message: &'static str,
    context: ErrorContext,
}

impl CliErrorView {
    /// Build a CLI view from the central error spec registry.
    #[inline]
    pub fn from_error(error: &RocketMQError) -> Self {
        let view = error.boundary_view();
        Self {
            exit_code: view.cli().exit_code,
            code: view.code(),
            category: view.category(),
            message: view.message(),
            context: view.context().clone(),
        }
    }

    #[inline]
    pub const fn exit_code(&self) -> CliExitCode {
        self.exit_code
    }

    #[inline]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    #[inline]
    pub const fn category(&self) -> ErrorCategory {
        self.category
    }

    #[inline]
    pub const fn message(&self) -> &'static str {
        self.message
    }

    #[inline]
    pub const fn context(&self) -> &ErrorContext {
        &self.context
    }

    /// Render a one-line, redaction-aware stderr message.
    pub fn render_stderr(&self) -> String {
        let mut rendered = format!(
            "Error: code={}, category={}, exit_code={}, message={}",
            self.code,
            self.category,
            self.exit_code.as_i32(),
            self.message
        );
        if !self.context.is_empty() {
            rendered.push_str(", context={");
            rendered.push_str(&self.context.to_string());
            rendered.push('}');
        }
        rendered
    }
}

impl From<&RocketMQError> for CliErrorView {
    #[inline]
    fn from(error: &RocketMQError) -> Self {
        Self::from_error(error)
    }
}

#[cfg(test)]
mod tests {
    use crate::CliErrorView;
    use crate::CliExitCode;
    use crate::RocketMQError;

    #[test]
    fn cli_view_uses_spec_exit_code_and_stable_code() {
        let error = RocketMQError::validation_failed("topic", "topic must not be empty");
        let view = CliErrorView::from_error(&error);

        assert_eq!(view.exit_code(), CliExitCode::USAGE);
        assert_eq!(view.code().as_str(), "ILLEGAL_ARGUMENT");
        assert_eq!(view.category().as_str(), "system");
        assert_eq!(view.message(), "Argument is illegal");
        assert!(view.render_stderr().contains("code=ILLEGAL_ARGUMENT"));
    }

    #[test]
    fn cli_view_renders_redacted_context() {
        let error = RocketMQError::storage_read_failed("C:/secret/token/file", "permission denied");
        let rendered = CliErrorView::from_error(&error).render_stderr();

        assert!(rendered.contains("code=STORAGE_READ_FAILED"));
        assert!(rendered.contains("path=<redacted>"));
        assert!(!rendered.contains("secret/token"));
    }
}

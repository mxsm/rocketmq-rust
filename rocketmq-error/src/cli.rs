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

use crate::descriptor::ErrorCode;
use crate::CliExitCode;
use crate::ComponentId;
use crate::ErrorContext;
use crate::RocketMQError;

/// Stable error view for command-line tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliErrorView {
    exit_code: CliExitCode,
    code: ErrorCode,
    component: ComponentId,
    message: &'static str,
    context: ErrorContext,
}

impl CliErrorView {
    /// Builds a CLI view from the canonical descriptor catalog.
    #[inline]
    pub fn from_error(error: &RocketMQError) -> Self {
        let view = error.boundary_view();
        Self {
            exit_code: view.cli().exit_code,
            code: view.code(),
            component: view.component(),
            message: view.message(),
            context: view.context().clone(),
        }
    }

    #[inline]
    /// Returns the exit code.
    pub const fn exit_code(&self) -> CliExitCode {
        self.exit_code
    }

    #[inline]
    /// Returns the code.
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    #[inline]
    /// Returns the catalog component.
    pub const fn component(&self) -> ComponentId {
        self.component
    }

    #[inline]
    /// Returns the message.
    pub const fn message(&self) -> &'static str {
        self.message
    }

    #[inline]
    /// Returns the context.
    pub const fn context(&self) -> &ErrorContext {
        &self.context
    }

    /// Render a one-line, redaction-aware stderr message.
    pub fn render_stderr(&self) -> String {
        let mut rendered = format!(
            "Error: code={}, component={}, exit_code={}, message={}",
            self.code,
            self.component,
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
    fn cli_view_uses_descriptor_exit_code_and_stable_code() {
        let error = RocketMQError::validation_failed("topic", "topic must not be empty");
        let view = CliErrorView::from_error(&error);

        assert_eq!(view.exit_code(), CliExitCode::USAGE);
        assert_eq!(view.code().as_str(), "core.argument.invalid");
        assert_eq!(view.component().as_str(), "core");
        assert_eq!(view.message(), "Argument is invalid");
        assert!(view.render_stderr().contains("code=core.argument.invalid"));
    }

    #[test]
    fn cli_view_suppresses_generic_context_and_keeps_declared_public_fields() {
        let error = RocketMQError::storage_read_failed("C:/secret/token/file", "permission denied");
        let rendered = CliErrorView::from_error(&error).render_stderr();

        assert!(rendered.contains("code=storage.read.failed"));
        assert!(!rendered.contains("context={"));
        assert!(!rendered.contains("secret/token"));

        let public = CliErrorView::from_error(&RocketMQError::route_not_found("TopicA")).render_stderr();
        assert!(public.contains("context={topic=TopicA}"));
    }
}

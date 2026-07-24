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

//! Stable views for backend-neutral Admin errors.

use serde::Deserialize;
use serde::Serialize;

use crate::core::AdminError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminErrorView {
    pub code: String,
    pub message: String,
    pub context: Option<String>,
}

impl AdminErrorView {
    pub fn from_error(error: &AdminError) -> Self {
        match error {
            AdminError::InvalidArgument { field, reason } => Self {
                code: "ADMIN_INVALID_ARGUMENT".to_string(),
                message: "Invalid admin argument".to_string(),
                context: Some(format!("field={field}; reason={reason}")),
            },
            AdminError::NotFound { resource, name } => Self {
                code: "ADMIN_NOT_FOUND".to_string(),
                message: "Admin resource was not found".to_string(),
                context: Some(format!("resource={resource}; name={name}")),
            },
            AdminError::Backend {
                operation,
                reason,
                code,
                context,
                ..
            } => Self {
                code: code.clone().unwrap_or_else(|| "ADMIN_BACKEND".to_string()),
                message: format!("{operation} failed: {reason}"),
                context: context.clone(),
            },
            AdminError::SessionClosed => Self {
                code: "ADMIN_SESSION_CLOSED".to_string(),
                message: "Admin session is closed".to_string(),
                context: None,
            },
        }
    }

    pub fn stable_message(&self) -> String {
        match &self.context {
            Some(context) => format!("{}: {}", self.message, context),
            None => self.message.clone(),
        }
    }
}

pub fn stable_error_code(error: &AdminError) -> String {
    AdminErrorView::from_error(error).code
}

pub fn stable_error_message(error: &AdminError) -> String {
    AdminErrorView::from_error(error).stable_message()
}

#[cfg(test)]
mod tests {
    use super::AdminErrorView;
    use crate::core::AdminError;

    #[test]
    fn admin_error_view_preserves_stable_backend_metadata() {
        let error = AdminError::backend_view(
            "query_topic",
            "TOPIC_QUERY_FAILED",
            "Topic query failed",
            Some("broker=broker-a".to_string()),
            503,
            true,
        );
        let view = AdminErrorView::from_error(&error);

        assert_eq!(view.code, "TOPIC_QUERY_FAILED");
        assert_eq!(view.message, "query_topic failed: Topic query failed");
        assert_eq!(view.context.as_deref(), Some("broker=broker-a"));
        assert_eq!(
            view.stable_message(),
            "query_topic failed: Topic query failed: broker=broker-a"
        );
    }
}

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

//! Stable admin error view models.

use rocketmq_error::RocketMQError;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminErrorView {
    pub code: String,
    pub message: String,
    pub context: Option<String>,
}

impl AdminErrorView {
    pub fn from_error(error: &RocketMQError) -> Self {
        let context = error.context();
        Self {
            code: error.spec().code.as_str().to_string(),
            message: error.public_message().to_string(),
            context: (!context.is_empty()).then(|| context.to_string()),
        }
    }

    pub fn stable_message(&self) -> String {
        match &self.context {
            Some(context) => format!("{}: {}", self.message, context),
            None => self.message.clone(),
        }
    }
}

pub fn stable_error_code(error: &RocketMQError) -> String {
    AdminErrorView::from_error(error).code
}

pub fn stable_error_message(error: &RocketMQError) -> String {
    AdminErrorView::from_error(error).stable_message()
}

#[cfg(test)]
mod tests {
    use super::AdminErrorView;
    use rocketmq_error::RocketMQError;

    #[test]
    fn admin_error_view_uses_stable_code_and_redacted_context() {
        let error = RocketMQError::storage_read_failed("C:/secret/token/file", "permission denied");
        let view = AdminErrorView::from_error(&error);

        assert_eq!(view.code, "STORAGE_READ_FAILED");
        assert_eq!(view.message, "Storage read failed");
        let rendered = view.stable_message();
        assert!(rendered.contains("path=<redacted>"));
        assert!(!rendered.contains("secret/token"));
    }
}

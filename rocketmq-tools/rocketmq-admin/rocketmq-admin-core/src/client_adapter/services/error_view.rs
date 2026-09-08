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

//! Stable views for Client SDK errors returned by command services.

use rocketmq_error::Error as CanonicalError;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RecoveryHint;
use rocketmq_error::ViewValueRef;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminErrorView {
    pub code: String,
    pub message: String,
    pub context: Option<String>,
}

impl AdminErrorView {
    pub fn from_error(error: &CanonicalError) -> Self {
        let context = error.context();
        let public = PublicErrorView::try_new(error.descriptor(), &context)
            .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
        Self {
            code: public.code().as_str().to_string(),
            message: public.message().to_string(),
            context: render_public_context(&public),
        }
    }

    pub fn stable_message(&self) -> String {
        match &self.context {
            Some(context) => format!("{}: {}", self.message, context),
            None => self.message.clone(),
        }
    }
}

fn render_public_context(view: &PublicErrorView<'_>) -> Option<String> {
    let fields = view
        .fields()
        .filter_map(|field| {
            let value = match field.value() {
                ViewValueRef::Text(value) => value.to_owned(),
                ViewValueRef::I64(value) => value.to_string(),
                ViewValueRef::U64(value) => value.to_string(),
                ViewValueRef::Bool(value) => value.to_string(),
                ViewValueRef::Redacted => return None,
            };
            Some(format!("{}={value}", field.name()))
        })
        .collect::<Vec<_>>();
    (!fields.is_empty()).then(|| fields.join(", "))
}

pub(crate) fn rocketmq_http_status(error: &CanonicalError) -> u16 {
    error.descriptor().projection().http().status.as_u16()
}

pub(crate) fn rocketmq_is_retryable(error: &CanonicalError) -> bool {
    matches!(
        error.descriptor().recovery_hint(),
        RecoveryHint::Backoff | RecoveryHint::RefreshRoute | RecoveryHint::RefreshLeader | RecoveryHint::SwitchBroker
    )
}

pub fn stable_error_code(error: &CanonicalError) -> String {
    AdminErrorView::from_error(error).code
}

pub fn stable_error_message(error: &CanonicalError) -> String {
    AdminErrorView::from_error(error).stable_message()
}

#[cfg(test)]
mod tests {
    use super::AdminErrorView;

    #[test]
    fn admin_error_view_uses_stable_code_and_redacted_context() {
        let error = crate::client_adapter::services::errors::storage_read_failed("admin-test");
        let view = AdminErrorView::from_error(&error);

        assert_eq!(view.code, "storage.read.failed");
        assert_eq!(view.message, "Storage read failed");
        assert_eq!(view.context, None);
        assert_eq!(view.stable_message(), "Storage read failed");
    }
}

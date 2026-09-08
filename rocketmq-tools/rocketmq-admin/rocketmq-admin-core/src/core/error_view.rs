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

//! Serializable projections of catalog-backed Admin errors.

use rocketmq_error::PublicErrorView;
use rocketmq_error::ViewValueRef;
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
        let public = error
            .public_view()
            .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
        let context = render_context(&public);
        Self {
            code: public.code().as_str().to_string(),
            message: public.message().to_string(),
            context,
        }
    }

    pub fn stable_message(&self) -> String {
        match &self.context {
            Some(context) => format!("{}: {}", self.message, context),
            None => self.message.clone(),
        }
    }
}

fn render_context(view: &PublicErrorView<'_>) -> Option<String> {
    let mut rendered = String::new();
    for field in view.fields() {
        let value = match field.value() {
            ViewValueRef::Text(value) => value.to_string(),
            ViewValueRef::I64(value) => value.to_string(),
            ViewValueRef::U64(value) => value.to_string(),
            ViewValueRef::Bool(value) => value.to_string(),
            ViewValueRef::Redacted => continue,
        };
        if !rendered.is_empty() {
            rendered.push_str("; ");
        }
        rendered.push_str(field.name());
        rendered.push('=');
        rendered.push_str(&value);
    }
    (!rendered.is_empty()).then_some(rendered)
}

pub fn stable_error_code(error: &AdminError) -> String {
    error.code().as_str().to_string()
}

pub fn stable_error_message(error: &AdminError) -> String {
    AdminErrorView::from_error(error).stable_message()
}

#[cfg(test)]
mod tests {
    use super::AdminErrorView;
    use crate::core::AdminError;

    #[test]
    fn admin_error_view_uses_only_descriptor_owned_public_data() {
        let source = rocketmq_error::Error::new(&rocketmq_error::ROUTE_TOPIC_NOT_FOUND)
            .with_context(rocketmq_error::ErrorContext::new().with_text(rocketmq_error::fields::TOPIC, "orders"));
        let error = AdminError::from_error("query_topic", source);
        let view = AdminErrorView::from_error(&error);

        assert_eq!(view.code, "route.topic.not_found");
        assert_eq!(view.message, "Topic route was not found");
        assert_eq!(view.context.as_deref(), Some("topic=orders"));
        assert_eq!(view.stable_message(), "Topic route was not found: topic=orders");
    }

    #[test]
    fn private_admin_detail_never_reaches_the_serializable_view() {
        let error = AdminError::backend("query_topic", "password=plain-text\r\nC:\\private");
        let serialized = serde_json::to_string(&AdminErrorView::from_error(&error)).expect("serialize view");

        assert!(!serialized.contains("plain-text"));
        assert!(!serialized.contains("private"));
        assert!(!serialized.contains("query_topic"));
    }
}

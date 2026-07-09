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

use rmcp::model::GetPromptRequestParams;
use rmcp::model::GetPromptResult;
use rmcp::model::JsonObject;
use rmcp::model::PromptMessage;
use rmcp::model::Role;
use rmcp::ErrorData;
use serde_json::Value;

use crate::prompts::registry;
use crate::prompts::template::PromptTemplate;
use crate::prompts::template::PromptTemplateArgument;
use crate::prompts::template::PromptTemplateError;

pub fn get_prompt(request: GetPromptRequestParams) -> Result<GetPromptResult, ErrorData> {
    let template = registry::get_template(&request.name)
        .map_err(template_error)?
        .ok_or_else(|| ErrorData::invalid_params(format!("unknown prompt: {}", request.name), None))?;
    let arguments = request.arguments.unwrap_or_default();
    validate_required_arguments(&template, &arguments)?;
    let text = render_template(&template, &arguments);

    Ok(GetPromptResult::new(vec![PromptMessage::new_text(Role::User, text)])
        .with_description(template.front_matter.description))
}

fn validate_required_arguments(template: &PromptTemplate, arguments: &JsonObject) -> Result<(), ErrorData> {
    for argument in template
        .front_matter
        .arguments
        .iter()
        .filter(|argument| argument.required)
    {
        if !argument_present(argument, arguments) {
            return Err(ErrorData::invalid_params(
                format!("missing required prompt argument: {}", argument.name),
                None,
            ));
        }
    }
    Ok(())
}

fn argument_present(argument: &PromptTemplateArgument, arguments: &JsonObject) -> bool {
    arguments
        .get(&argument.name)
        .is_some_and(|value| !value.is_null() && value.as_str().is_none_or(|value| !value.trim().is_empty()))
}

fn render_template(template: &PromptTemplate, arguments: &JsonObject) -> String {
    let mut rendered = template.body.clone();
    for argument in &template.front_matter.arguments {
        let value = arguments.get(&argument.name).map(render_value).unwrap_or_default();
        rendered = rendered.replace(&format!("{{{{{}}}}}", argument.name), &value);
    }
    rendered
}

fn render_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn template_error(error: PromptTemplateError) -> ErrorData {
    ErrorData::internal_error(error.to_string(), None)
}

#[cfg(test)]
mod tests {
    use rmcp::model::ContentBlock;

    use super::*;

    #[test]
    fn renders_prompt_arguments() {
        let result = get_prompt(
            GetPromptRequestParams::new("diagnose_consumer_lag").with_arguments(
                serde_json::json!({
                    "cluster": "local-dev",
                    "topic": "orders",
                    "consumer_group": "order-service",
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .unwrap();
        let text = prompt_text(&result);

        assert!(text.contains("orders"));
        assert!(text.contains("order-service"));
        assert!(text.contains("mq_diagnose_consumer_lag"));
        assert!(text.contains("Do not call mutation tools"));
    }

    #[test]
    fn missing_required_argument_returns_protocol_error() {
        let err = get_prompt(
            GetPromptRequestParams::new("diagnose_consumer_lag").with_arguments(
                serde_json::json!({
                    "cluster": "local-dev",
                    "topic": "orders",
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .unwrap_err();

        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
        assert!(err.message.contains("consumer_group"));
    }

    fn prompt_text(result: &GetPromptResult) -> &str {
        match &result.messages[0].content {
            ContentBlock::Text(text) => &text.text,
            _ => panic!("prompt should render text"),
        }
    }
}

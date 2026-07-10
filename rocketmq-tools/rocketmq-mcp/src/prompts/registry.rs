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

use rmcp::model::ListPromptsResult;
use rmcp::model::Prompt;
use rmcp::model::PromptArgument;

use crate::prompts::template::PromptTemplate;
use crate::prompts::template::PromptTemplateError;

const PROMPT_SOURCES: &[&str] = &[
    include_str!("../../prompts/diagnose_consumer_lag.md"),
    include_str!("../../prompts/broker_health_check.md"),
];

pub fn list_prompts() -> Result<ListPromptsResult, PromptTemplateError> {
    Ok(ListPromptsResult::with_all_items(
        prompt_templates()?.into_iter().map(to_prompt).collect(),
    ))
}

pub fn get_template(name: &str) -> Result<Option<PromptTemplate>, PromptTemplateError> {
    Ok(prompt_templates()?
        .into_iter()
        .find(|template| template.front_matter.name == name))
}

pub fn prompt_templates() -> Result<Vec<PromptTemplate>, PromptTemplateError> {
    PROMPT_SOURCES
        .iter()
        .map(|source| PromptTemplate::parse(source))
        .collect()
}

fn to_prompt(template: PromptTemplate) -> Prompt {
    let arguments = template
        .front_matter
        .arguments
        .iter()
        .map(|argument| {
            let mut prompt_argument = PromptArgument::new(argument.name.clone()).with_required(argument.required);
            if let Some(description) = &argument.description {
                prompt_argument = prompt_argument.with_description(description.clone());
            }
            prompt_argument
        })
        .collect::<Vec<_>>();

    Prompt::new(
        template.front_matter.name,
        Some(template.front_matter.description),
        Some(arguments),
    )
    .with_title(template.front_matter.title)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_prompts_returns_mvp_runbooks() {
        let result = list_prompts().unwrap();
        let names = result
            .prompts
            .iter()
            .map(|prompt| prompt.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, ["diagnose_consumer_lag", "broker_health_check"]);
        assert!(result.prompts.iter().all(|prompt| prompt.arguments.is_some()));
    }

    #[test]
    fn get_template_finds_known_prompt() {
        let template = get_template("diagnose_consumer_lag").unwrap().unwrap();

        assert_eq!(template.front_matter.title, "Diagnose Consumer Lag");
        assert!(template.body.contains("rocketmq_diagnose_consumer_lag"));
    }
}

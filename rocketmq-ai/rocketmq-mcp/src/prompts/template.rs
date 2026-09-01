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

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptTemplate {
    pub front_matter: PromptFrontMatter,
    pub body: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PromptFrontMatter {
    pub name: String,
    pub title: String,
    pub description: String,
    #[serde(default)]
    pub arguments: Vec<PromptTemplateArgument>,
    #[serde(default)]
    pub required_tools: Vec<String>,
    #[serde(default)]
    pub conditional_tools: Vec<PromptConditionalTool>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PromptTemplateArgument {
    pub name: String,
    pub kind: PromptArgumentKind,
    #[serde(default)]
    pub required: bool,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptArgumentKind {
    Cluster,
    BrokerName,
    Topic,
    ConsumerGroup,
    MessageId,
    CheckLevel,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PromptConditionalTool {
    pub argument: String,
    pub tool: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PromptTemplateError {
    #[error("prompt template must start with YAML front matter")]
    MissingFrontMatter,

    #[error("prompt template front matter is not terminated")]
    UnterminatedFrontMatter,

    #[error("invalid prompt template front matter: {0}")]
    InvalidFrontMatter(#[from] serde_yaml::Error),
}

impl PromptTemplate {
    pub fn parse(source: &str) -> Result<Self, PromptTemplateError> {
        let source = source.strip_prefix('\u{feff}').unwrap_or(source);
        let source = source.replace("\r\n", "\n").replace('\r', "\n");
        let source = source
            .strip_prefix("---\n")
            .ok_or(PromptTemplateError::MissingFrontMatter)?;
        let (front_matter, body) = source
            .split_once("\n---\n")
            .ok_or(PromptTemplateError::UnterminatedFrontMatter)?;
        Ok(Self {
            front_matter: serde_yaml::from_str(front_matter)?,
            body: body.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_front_matter_and_body() {
        let template = PromptTemplate::parse(
            r#"---
name: test_prompt
title: Test Prompt
description: Test description.
arguments:
  - name: cluster
    kind: cluster
    required: true
    description: Cluster name.
required_tools:
  - rocketmq_get_cluster_overview
---
# Body

Use {{cluster}}.
"#,
        )
        .unwrap();

        assert_eq!(template.front_matter.name, "test_prompt");
        assert_eq!(template.front_matter.arguments[0].name, "cluster");
        assert!(template.front_matter.arguments[0].required);
        assert!(template.body.contains("{{cluster}}"));
    }

    #[test]
    fn parses_crlf_front_matter() {
        let template = PromptTemplate::parse(
            "---\r\nname: test_prompt\r\ntitle: Test Prompt\r\ndescription: Test description.\r\n---\r\nBody\r\n",
        )
        .unwrap();

        assert_eq!(template.front_matter.name, "test_prompt");
        assert_eq!(template.body, "Body\n");
    }

    #[test]
    fn front_matter_and_nested_schema_reject_unknown_or_missing_kinds() {
        let invalid = [
            "---\nname: test\ntitle: Test\ndescription: Test.\nunknown: true\n---\nBody\n",
            "---\nname: test\ntitle: Test\ndescription: Test.\narguments:\n  - name: cluster\n    kind: cluster\n    unexpected: true\n---\n{{cluster}}\n",
            "---\nname: test\ntitle: Test\ndescription: Test.\narguments:\n  - name: cluster\n---\n{{cluster}}\n",
            "---\nname: test\ntitle: Test\ndescription: Test.\nconditional_tools:\n  - argument: cluster\n    tool: rocketmq_get_cluster_overview\n    unexpected: true\n---\n{{cluster}}\n",
        ];
        for source in invalid {
            assert!(PromptTemplate::parse(source).is_err());
        }
    }
}

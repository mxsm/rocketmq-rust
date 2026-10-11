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
    /// Tools the Prompt starts with for a caller who may call every one of them. They do not
    /// decide whether the Prompt is available: the body names them only between the
    /// [`PREFERRED_TOOLS_OPEN`] and [`PREFERRED_TOOLS_CLOSE`] lines, and that part is left out for
    /// every other caller.
    #[serde(default)]
    pub preferred_tools: Vec<String>,
}

/// The line that starts the part of a Prompt body about its preferred Tools.
pub(crate) const PREFERRED_TOOLS_OPEN: &str = "<!-- preferred-tools -->";

/// The line that ends that part.
pub(crate) const PREFERRED_TOOLS_CLOSE: &str = "<!-- /preferred-tools -->";

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

impl PromptTemplate {
    pub fn parse(source: &str) -> crate::McpResult<Self> {
        let source = source.strip_prefix('\u{feff}').unwrap_or(source);
        let source = source.replace("\r\n", "\n").replace('\r', "\n");
        let source = source
            .strip_prefix("---\n")
            .ok_or_else(|| crate::McpError::invalid_config("missing prompt front matter".to_string()))?;
        let (front_matter, body) = source
            .split_once("\n---\n")
            .ok_or_else(|| crate::McpError::invalid_config("unterminated prompt front matter".to_string()))?;
        Ok(Self {
            front_matter: serde_yaml::from_str(front_matter).map_err(crate::McpError::from_source)?,
            body: body.to_string(),
        })
    }

    /// The body as one caller reads it: with the part about the preferred Tools when the caller
    /// may call them, and without it otherwise. The two marker lines are never part of it.
    pub(crate) fn body_for(&self, with_preferred_tools: bool) -> String {
        let mut body = String::with_capacity(self.body.len());
        let mut inside = false;
        for line in self.body.split_inclusive('\n') {
            match line.trim_end_matches('\n') {
                PREFERRED_TOOLS_OPEN => inside = true,
                PREFERRED_TOOLS_CLOSE => inside = false,
                _ if inside && !with_preferred_tools => {}
                _ => body.push_str(line),
            }
        }
        body
    }

    /// The part of the body about the preferred Tools, and the body without it.
    ///
    /// # Errors
    ///
    /// Returns an error when the two marker lines are not exactly one pair in order.
    pub(crate) fn preferred_tools_part(&self) -> crate::McpResult<Option<(String, String)>> {
        let markers = self
            .body
            .lines()
            .filter(|line| matches!(*line, PREFERRED_TOOLS_OPEN | PREFERRED_TOOLS_CLOSE))
            .collect::<Vec<_>>();
        match markers.as_slice() {
            [] => Ok(None),
            [PREFERRED_TOOLS_OPEN, PREFERRED_TOOLS_CLOSE] => {
                let part = self
                    .body
                    .split_inclusive('\n')
                    .skip_while(|line| line.trim_end_matches('\n') != PREFERRED_TOOLS_OPEN)
                    .skip(1)
                    .take_while(|line| line.trim_end_matches('\n') != PREFERRED_TOOLS_CLOSE)
                    .collect::<String>();
                Ok(Some((part, self.body_for(false))))
            }
            _ => Err(crate::McpError::invalid_config(
                "prompt preferred-tools markers must be one opening line followed by one closing line".to_string(),
            )),
        }
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

    fn with_preferred_part() -> PromptTemplate {
        PromptTemplate::parse(
            "---\nname: test\ntitle: Test\ndescription: Test.\npreferred_tools:\n  - rocketmq_get_cluster_health\n---\n\
             Before.\n\n<!-- preferred-tools -->\nCall the check first.\n\n<!-- /preferred-tools -->\nAfter.\n",
        )
        .unwrap()
    }

    #[test]
    fn the_part_about_preferred_tools_is_rendered_only_for_a_caller_who_may_call_them() {
        let template = with_preferred_part();

        assert_eq!(template.front_matter.preferred_tools, ["rocketmq_get_cluster_health"]);
        assert_eq!(template.body_for(true), "Before.\n\nCall the check first.\n\nAfter.\n");
        assert_eq!(template.body_for(false), "Before.\n\nAfter.\n");
        assert_eq!(
            template.preferred_tools_part().unwrap(),
            Some((
                "Call the check first.\n\n".to_string(),
                "Before.\n\nAfter.\n".to_string()
            ))
        );
    }

    #[test]
    fn a_body_without_markers_is_rendered_as_it_is() {
        let template = PromptTemplate::parse("---\nname: test\ntitle: Test\ndescription: Test.\n---\nBody\n").unwrap();

        assert_eq!(template.body_for(true), "Body\n");
        assert_eq!(template.body_for(false), "Body\n");
        assert_eq!(template.preferred_tools_part().unwrap(), None);
    }

    #[test]
    fn preferred_tools_markers_must_be_one_pair_in_order() {
        let body = |body: &str| PromptTemplate {
            body: body.to_string(),
            ..with_preferred_part()
        };
        for invalid in [
            "<!-- preferred-tools -->\nCall.\n",
            "Call.\n<!-- /preferred-tools -->\n",
            "<!-- /preferred-tools -->\nCall.\n<!-- preferred-tools -->\n",
            "<!-- preferred-tools -->\nA.\n<!-- /preferred-tools -->\n<!-- preferred-tools -->\nB.\n<!-- /preferred-tools -->\n",
        ] {
            assert!(body(invalid).preferred_tools_part().is_err(), "{invalid}");
        }
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

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

use std::collections::BTreeSet;

use rmcp::model::ListPromptsResult;
use rmcp::model::Prompt;
use rmcp::model::PromptArgument;

use crate::config::McpConfig;
use crate::guard::RiskLevel;
use crate::model::identifier;
use crate::prompts::template::PromptTemplate;
use crate::tools::catalog::ToolId;

const PROMPT_SOURCES: &[&str] = &[
    include_str!("../../prompts/diagnose_consumer_lag.md"),
    include_str!("../../prompts/broker_health_check.md"),
    include_str!("../../prompts/diagnose_broker_health.md"),
    include_str!("../../prompts/diagnose_message_delivery.md"),
    include_str!("../../prompts/analyze_consumer_connections.md"),
];

pub fn list_prompts() -> crate::McpResult<ListPromptsResult> {
    Ok(ListPromptsResult::with_all_items(
        prompt_templates()?.into_iter().map(to_prompt).collect(),
    ))
}

pub fn list_prompts_for(
    config: &McpConfig,
    mut allows_tool: impl FnMut(ToolId, &str) -> bool,
) -> crate::McpResult<ListPromptsResult> {
    let prompts = prompt_templates()?
        .into_iter()
        .filter(|template| {
            config.clusters.iter().any(|cluster| {
                identifier::is_logical_alias(&cluster.name)
                    && template
                        .front_matter
                        .required_tools
                        .iter()
                        .filter_map(|name| ToolId::resolve(name))
                        .all(|tool| allows_tool(tool, &cluster.name))
            })
        })
        .map(to_prompt)
        .collect();
    Ok(ListPromptsResult::with_all_items(prompts))
}

pub fn get_template(name: &str) -> crate::McpResult<Option<PromptTemplate>> {
    Ok(prompt_templates()?
        .into_iter()
        .find(|template| template.front_matter.name == name))
}

pub fn prompt_templates() -> crate::McpResult<Vec<PromptTemplate>> {
    let templates = PROMPT_SOURCES
        .iter()
        .map(|source| PromptTemplate::parse(source))
        .collect::<Result<Vec<_>, _>>()?;
    validate_registry(&templates)?;
    Ok(templates)
}

pub fn required_tools(
    template: &PromptTemplate,
    present_arguments: &BTreeSet<String>,
) -> crate::McpResult<Vec<ToolId>> {
    let mut names = template.front_matter.required_tools.clone();
    names.extend(
        template
            .front_matter
            .conditional_tools
            .iter()
            .filter(|requirement| present_arguments.contains(&requirement.argument))
            .map(|requirement| requirement.tool.clone()),
    );
    names
        .into_iter()
        .map(|name| {
            ToolId::resolve(&name)
                .ok_or_else(|| crate::McpError::invalid_config(format!("prompt references unknown Tool `{name}`")))
        })
        .collect()
}

/// The Tools a Prompt starts with for a caller who may call every one of them.
///
/// They never decide whether the Prompt is available. A caller who may not call them gets the
/// Prompt without the part of its body that names them.
pub fn preferred_tools(template: &PromptTemplate) -> crate::McpResult<Vec<ToolId>> {
    template
        .front_matter
        .preferred_tools
        .iter()
        .map(|name| {
            ToolId::resolve(name)
                .ok_or_else(|| crate::McpError::invalid_config(format!("prompt references unknown Tool `{name}`")))
        })
        .collect()
}

fn validate_registry(templates: &[PromptTemplate]) -> crate::McpResult<()> {
    let mut prompt_names = BTreeSet::new();
    for template in templates {
        let front = &template.front_matter;
        if front.name.trim().is_empty()
            || front.title.trim().is_empty()
            || front.description.trim().is_empty()
            || !prompt_names.insert(front.name.clone())
        {
            return Err(crate::McpError::invalid_config(
                "prompt names, titles, and descriptions must be unique and non-empty".to_string(),
            ));
        }

        let mut argument_names = BTreeSet::new();
        for argument in &front.arguments {
            if argument.name.trim().is_empty() || !argument_names.insert(argument.name.clone()) {
                return Err(crate::McpError::invalid_config(format!(
                    "prompt `{}` has duplicate or blank arguments",
                    front.name
                )));
            }
        }

        let placeholders = placeholders(&template.body)?;
        if placeholders.iter().any(|name| !argument_names.contains(name))
            || argument_names.iter().any(|name| !placeholders.contains(name))
        {
            return Err(crate::McpError::invalid_config(format!(
                "prompt `{}` placeholders do not match its argument registry",
                front.name
            )));
        }

        let mut required_tools = BTreeSet::new();
        for tool_name in &front.required_tools {
            validate_required_tool(&front.name, tool_name, &mut required_tools)?;
        }
        for requirement in &front.conditional_tools {
            if !argument_names.contains(&requirement.argument)
                || front
                    .arguments
                    .iter()
                    .find(|argument| argument.name == requirement.argument)
                    .is_some_and(|argument| argument.required)
            {
                return Err(crate::McpError::invalid_config(format!(
                    "prompt `{}` has an invalid conditional Tool argument",
                    front.name
                )));
            }
            validate_required_tool(&front.name, &requirement.tool, &mut required_tools)?;
        }
        validate_preferred_tools(template, &mut required_tools)?;
    }
    Ok(())
}

/// Checks that a Prompt names its preferred Tools in the marked part of its body and nowhere
/// else, so that a caller who may not call one of them is never told about it.
fn validate_preferred_tools(template: &PromptTemplate, seen: &mut BTreeSet<String>) -> crate::McpResult<()> {
    let front = &template.front_matter;
    let invalid = || {
        crate::McpError::invalid_config(format!(
            "prompt `{}` must name its preferred Tools in the marked part of its body, and only there",
            front.name
        ))
    };
    let Some((part, rest)) = template.preferred_tools_part()? else {
        return if front.preferred_tools.is_empty() {
            Ok(())
        } else {
            Err(invalid())
        };
    };
    // The arguments are rendered where every caller reads them, not in a part that may be left out.
    if front.preferred_tools.is_empty() || part.contains("{{") {
        return Err(invalid());
    }
    for tool_name in &front.preferred_tools {
        validate_required_tool(&front.name, tool_name, seen)?;
        if !part.contains(tool_name.as_str()) || rest.contains(tool_name.as_str()) {
            return Err(invalid());
        }
    }
    Ok(())
}

fn validate_required_tool(prompt_name: &str, tool_name: &str, seen: &mut BTreeSet<String>) -> crate::McpResult<()> {
    let tool = ToolId::resolve(tool_name).ok_or_else(|| {
        crate::McpError::invalid_config(format!("prompt `{prompt_name}` references unknown Tool `{tool_name}`"))
    })?;
    if !seen.insert(tool_name.to_string()) {
        return Err(crate::McpError::invalid_config(format!(
            "prompt `{prompt_name}` has duplicate required Tools"
        )));
    }
    if !matches!(tool.descriptor().risk_level, RiskLevel::ReadOnly | RiskLevel::Diagnose) {
        return Err(crate::McpError::invalid_config(format!(
            "prompt `{prompt_name}` references a non-query Tool"
        )));
    }
    Ok(())
}

fn placeholders(body: &str) -> crate::McpResult<BTreeSet<String>> {
    let mut placeholders = BTreeSet::new();
    let mut remainder = body;
    loop {
        let open = remainder.find("{{");
        let close = remainder.find("}}");
        let (Some(open), Some(close)) = (open, close) else {
            if open.is_some() || close.is_some() {
                return Err(crate::McpError::invalid_config(
                    "prompt contains an unmatched placeholder delimiter".to_string(),
                ));
            }
            break;
        };
        if close < open {
            return Err(crate::McpError::invalid_config(
                "prompt contains an unmatched placeholder delimiter".to_string(),
            ));
        }
        let after_open = &remainder[open + 2..];
        let close = close - open - 2;
        let name = &after_open[..close];
        if name.trim() != name || name.is_empty() || name.contains(['{', '}']) {
            return Err(crate::McpError::invalid_config(
                "prompt contains an invalid placeholder".to_string(),
            ));
        }
        placeholders.insert(name.to_string());
        remainder = &after_open[close + 2..];
    }
    Ok(placeholders)
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
    fn every_prompt_sends_a_needed_change_to_the_control_dry_run() {
        let templates = prompt_templates().unwrap();
        assert_eq!(templates.len(), 5);
        for template in templates {
            let name = &template.front_matter.name;
            // One planning entry: the dry run of the control server, for an operator to use.
            assert!(
                template.body.contains("dry run of the control MCP server"),
                "{name} does not say where a change is planned"
            );
            assert!(template.body.contains("Do not plan or apply it here."), "{name}");
            // The planning Tools of this server are deprecated and no Prompt leads to them.
            assert!(!template.body.contains("rocketmq_plan_"), "{name}");
            assert!(
                template
                    .front_matter
                    .required_tools
                    .iter()
                    .all(|tool| !tool.starts_with("rocketmq_plan_")),
                "{name}"
            );
        }
    }

    #[test]
    fn list_prompts_preserves_existing_and_adds_three_guides() {
        let result = list_prompts().unwrap();
        let names = result
            .prompts
            .iter()
            .map(|prompt| prompt.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "diagnose_consumer_lag",
                "broker_health_check",
                "diagnose_broker_health",
                "diagnose_message_delivery",
                "analyze_consumer_connections",
            ]
        );
    }

    #[test]
    fn registry_is_closed_and_resolves_all_required_tools() {
        let templates = prompt_templates().unwrap();
        assert_eq!(templates.len(), 5);
        for template in templates {
            let unconditional = required_tools(&template, &BTreeSet::new()).unwrap();
            assert_eq!(unconditional.len(), template.front_matter.required_tools.len());
        }
    }

    #[test]
    fn prompt_discovery_requires_all_unconditional_tools_on_one_configured_cluster() {
        let config = McpConfig::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("conf")
                .join("mcp.example.toml"),
        )
        .unwrap();
        let read_only = list_prompts_for(&config, |tool, cluster| {
            cluster == "local-dev" && tool.descriptor().risk_level == RiskLevel::ReadOnly
        })
        .unwrap();
        let read_only_names = read_only
            .prompts
            .iter()
            .map(|prompt| prompt.name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            read_only_names,
            BTreeSet::from([
                "broker_health_check",
                "diagnose_message_delivery",
                "analyze_consumer_connections",
            ])
        );

        let no_single_cluster = list_prompts_for(&config, |tool, cluster| {
            cluster == "local-dev" && tool != ToolId::GetConsumerProgress
        })
        .unwrap();
        let names = no_single_cluster
            .prompts
            .iter()
            .map(|prompt| prompt.name.as_str())
            .collect::<BTreeSet<_>>();
        assert!(!names.contains("diagnose_message_delivery"));
        assert!(!names.contains("analyze_consumer_connections"));
    }

    #[test]
    fn prompt_discovery_requires_at_least_one_representable_configured_cluster() {
        let mut config = McpConfig::load(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("conf")
                .join("mcp.example.toml"),
        )
        .unwrap();
        let safe_cluster = config.clusters[0].clone();
        config.clusters[0].name = "token=secret".to_string();
        assert!(list_prompts_for(&config, |_, _| true).unwrap().prompts.is_empty());

        config.clusters.push(safe_cluster);
        let mut observed_clusters = BTreeSet::new();
        let mixed = list_prompts_for(&config, |_, cluster| {
            observed_clusters.insert(cluster.to_string());
            true
        })
        .unwrap();
        assert_eq!(mixed.prompts.len(), 5);
        assert_eq!(observed_clusters, BTreeSet::from(["local-dev".to_string()]));
        let wire = serde_json::to_string(&mixed).unwrap();
        assert!(!wire.contains("token=secret"));
    }

    #[test]
    fn registry_rejects_duplicate_names_arguments_tools_and_placeholder_mismatches() {
        let valid = test_template(
            "test_prompt",
            "arguments:\n  - name: cluster\n    kind: cluster\n    required: true\nrequired_tools:\n  - rocketmq_get_cluster_overview",
            "Use {{cluster}}.",
        );
        assert!(validate_registry(std::slice::from_ref(&valid)).is_ok());
        assert!(validate_registry(&[valid.clone(), valid.clone()]).is_err());

        let duplicate_argument = test_template(
            "duplicate_argument",
            "arguments:\n  - name: cluster\n    kind: cluster\n  - name: cluster\n    kind: cluster\nrequired_tools:\n  - rocketmq_get_cluster_overview",
            "Use {{cluster}}.",
        );
        assert!(validate_registry(&[duplicate_argument]).is_err());

        let mismatched_placeholder = test_template(
            "mismatch",
            "arguments:\n  - name: cluster\n    kind: cluster\nrequired_tools:\n  - rocketmq_get_cluster_overview",
            "Use {{topic}}.",
        );
        assert!(validate_registry(&[mismatched_placeholder]).is_err());

        let duplicate_tool = test_template(
            "duplicate_tool",
            "arguments:\n  - name: cluster\n    kind: cluster\nrequired_tools:\n  - rocketmq_get_cluster_overview\n  - rocketmq_get_cluster_overview",
            "Use {{cluster}}.",
        );
        assert!(validate_registry(&[duplicate_tool]).is_err());

        let unknown_tool = test_template(
            "unknown_tool",
            "arguments:\n  - name: cluster\n    kind: cluster\nrequired_tools:\n  - rocketmq_private_tool",
            "Use {{cluster}}.",
        );
        assert!(validate_registry(&[unknown_tool]).is_err());

        let unmatched = test_template(
            "unmatched",
            "arguments:\n  - name: cluster\n    kind: cluster\nrequired_tools:\n  - rocketmq_get_cluster_overview",
            "Use }} then {{cluster}}.",
        );
        assert!(validate_registry(&[unmatched]).is_err());
    }

    #[test]
    fn preferred_tools_are_named_only_in_the_part_a_caller_without_them_never_reads() {
        let templates = prompt_templates().unwrap();
        let preferred = |name: &str| {
            templates
                .iter()
                .find(|template| template.front_matter.name == name)
                .map(|template| template.front_matter.preferred_tools.clone())
                .unwrap()
        };
        assert!(preferred("diagnose_consumer_lag").is_empty());
        assert_eq!(
            preferred("analyze_consumer_connections"),
            ["rocketmq_diagnose_consumer_group"]
        );
        assert_eq!(preferred("diagnose_broker_health"), ["rocketmq_diagnose_broker_health"]);
        assert_eq!(
            preferred("broker_health_check"),
            ["rocketmq_get_cluster_health", "rocketmq_diagnose_broker_health"]
        );
        assert_eq!(
            preferred("diagnose_message_delivery"),
            ["rocketmq_diagnose_message_delivery"]
        );
        for template in &templates {
            let name = &template.front_matter.name;
            let with = template.body_for(true);
            let without = template.body_for(false);
            assert!(!with.contains("preferred-tools"), "{name}");
            assert!(!without.contains("preferred-tools"), "{name}");
            for tool in &template.front_matter.preferred_tools {
                assert!(with.contains(tool.as_str()), "{name} does not name {tool}");
                assert!(!without.contains(tool.as_str()), "{name} names {tool} to every caller");
            }
        }
    }

    #[test]
    fn registry_rejects_preferred_tools_that_are_unmarked_undeclared_or_named_elsewhere() {
        let front =
            "arguments:\n  - name: cluster\n    kind: cluster\nrequired_tools:\n  - rocketmq_get_cluster_overview";
        let preferred = format!("{front}\npreferred_tools:\n  - rocketmq_get_cluster_health");
        let marked = "Use {{cluster}}.\n<!-- preferred-tools -->\nCall rocketmq_get_cluster_health first.\n<!-- \
                      /preferred-tools -->\nThen read the overview.";
        assert!(validate_registry(&[test_template("valid", &preferred, marked)]).is_ok());

        let invalid = [
            // Declared, and the body marks no part.
            (preferred.as_str(), "Use {{cluster}}."),
            // Marked, and nothing is declared.
            (front, marked),
            // The marked part does not name the Tool.
            (
                preferred.as_str(),
                "Use {{cluster}}.\n<!-- preferred-tools -->\nCall the check first.\n<!-- /preferred-tools -->",
            ),
            // The Tool is named where every caller reads it.
            (
                preferred.as_str(),
                "Use {{cluster}} and rocketmq_get_cluster_health.\n<!-- preferred-tools -->\nCall \
                 rocketmq_get_cluster_health first.\n<!-- /preferred-tools -->",
            ),
            // An argument is rendered in the part that may be left out.
            (
                preferred.as_str(),
                "<!-- preferred-tools -->\nCall rocketmq_get_cluster_health for {{cluster}}.\n<!-- /preferred-tools -->",
            ),
            // The markers are not a pair.
            (
                preferred.as_str(),
                "Use {{cluster}}.\n<!-- preferred-tools -->\nCall rocketmq_get_cluster_health first.",
            ),
        ];
        for (front_matter, body) in invalid {
            assert!(
                validate_registry(&[test_template("invalid", front_matter, body)]).is_err(),
                "{body}"
            );
        }

        // A preferred Tool is not a required Tool as well.
        let required_too = format!("{front}\npreferred_tools:\n  - rocketmq_get_cluster_overview");
        assert!(validate_registry(&[test_template(
            "required_too",
            &required_too,
            "Use {{cluster}}.\n<!-- preferred-tools -->\nCall rocketmq_get_cluster_overview first.\n<!-- \
             /preferred-tools -->",
        )])
        .is_err());
    }

    #[test]
    fn registered_prompts_keep_the_closed_tool_sets_and_read_only_safety_boundary() {
        let templates = prompt_templates().unwrap();
        let by_name = templates
            .iter()
            .map(|template| (template.front_matter.name.as_str(), template))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            by_name["diagnose_broker_health"].front_matter.required_tools,
            [
                "rocketmq_get_cluster_overview",
                "rocketmq_describe_broker",
                "rocketmq_get_broker_diagnostics",
                "rocketmq_get_broker_config_summary",
                "rocketmq_get_ha_status",
            ]
        );
        assert_eq!(
            by_name["diagnose_message_delivery"].front_matter.required_tools,
            [
                "rocketmq_get_topic_route",
                "rocketmq_get_topic_stats",
                "rocketmq_get_topic_config",
                "rocketmq_get_consumer_group_details",
                "rocketmq_get_consumer_progress",
            ]
        );
        assert_eq!(
            by_name["analyze_consumer_connections"].front_matter.required_tools,
            [
                "rocketmq_list_consumer_connections",
                "rocketmq_get_consumer_group_details",
                "rocketmq_get_consumer_progress",
            ]
        );
        for template in templates {
            let body = template.body.to_ascii_lowercase();
            assert!(body.contains("mutat"), "{}", template.front_matter.name);
            for forbidden_surface in [
                "control",
                "offset",
                "config",
                "cli",
                "shell",
                "free-form rpc",
                "message bod",
            ] {
                assert!(
                    body.contains(forbidden_surface),
                    "{} must forbid {forbidden_surface}",
                    template.front_matter.name
                );
            }
        }
    }

    fn test_template(name: &str, front_matter: &str, body: &str) -> PromptTemplate {
        PromptTemplate::parse(&format!(
            "---\nname: {name}\ntitle: Test\ndescription: Test.\n{front_matter}\n---\n{body}\n"
        ))
        .unwrap()
    }
}

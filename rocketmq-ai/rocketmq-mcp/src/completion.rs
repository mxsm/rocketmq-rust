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

//! Argument completion for Prompt arguments and Resource template variables.
//!
//! A completion names clusters, Topics, Consumer Groups, or Brokers, so it is an output surface
//! like a Tool result: the caller needs the permission of the listing Tool that would return the
//! same names, and every request is rate limited, audited, and sanitized. A request that may not
//! be answered gets the same empty result as one without candidates, so a completion never tells
//! a caller more than the listing Tool would.

use rmcp::model::CompleteRequestParams;
use rmcp::model::CompleteResult;
use rmcp::model::CompletionInfo;
use rmcp::model::JsonObject;
use rmcp::model::Reference;
use serde_json::Value;

use crate::adapter::query_facade::ReadOnlyQuery;
use crate::guard::context::RequestContext;
use crate::guard::Guard;
use crate::model::identifier;
use crate::prompts;
use crate::resources;
use crate::tools::catalog::ToolId;

/// What an argument names, which decides where its candidates come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionClass {
    Cluster,
    Topic,
    ConsumerGroup,
    Broker,
}

impl CompletionClass {
    /// The listing Tool that returns the same names; its permission admits the completion.
    pub(crate) const fn backing_tool(self) -> ToolId {
        match self {
            Self::Cluster => ToolId::ListClusters,
            Self::Topic => ToolId::ListTopics,
            Self::ConsumerGroup => ToolId::ListConsumerGroups,
            Self::Broker => ToolId::ListBrokers,
        }
    }

    const fn audit_operation(self) -> &'static str {
        match self {
            Self::Cluster => "completion:cluster",
            Self::Topic => "completion:topic",
            Self::ConsumerGroup => "completion:consumer_group",
            Self::Broker => "completion:broker",
        }
    }

    /// The longest value an argument of this class accepts, which bounds a useful prefix.
    const fn max_bytes(self) -> usize {
        match self {
            Self::Cluster | Self::Broker => identifier::LOGICAL_ALIAS_MAX_BYTES,
            Self::Topic => identifier::TOPIC_MAX_BYTES,
            Self::ConsumerGroup => identifier::CONSUMER_GROUP_MAX_BYTES,
        }
    }

    /// Whether `name` is a value the argument accepts. A name that is not, or that reads as
    /// sensitive, would be rejected or redacted when it is used, so it is not offered.
    fn accepts(self, name: &str) -> bool {
        let well_formed = match self {
            Self::Cluster | Self::Broker => identifier::is_logical_alias(name),
            Self::Topic => identifier::is_topic(name),
            Self::ConsumerGroup => identifier::is_consumer_group(name),
        };
        well_formed && !identifier::is_sensitive_resource_value(name)
    }

    /// Classifies a Prompt argument by its name. Arguments that take free values, such as a
    /// message identifier or a check level, have no candidates.
    fn of_prompt_argument(name: &str) -> Option<Self> {
        match name {
            "cluster" => Some(Self::Cluster),
            "topic" => Some(Self::Topic),
            "consumer_group" => Some(Self::ConsumerGroup),
            "broker_name" => Some(Self::Broker),
            _ => None,
        }
    }

    /// Classifies a Resource template variable by its name. `filter`, `limit`, and `cursor` take
    /// free values and have no candidates.
    fn of_template_variable(name: &str) -> Option<Self> {
        match name {
            "cluster" => Some(Self::Cluster),
            "topic" => Some(Self::Topic),
            "group" => Some(Self::ConsumerGroup),
            "broker" => Some(Self::Broker),
            _ => None,
        }
    }
}

/// Returns what `argument` of a registered Prompt or Resource template names.
///
/// `None` covers an unknown Prompt or template, an argument it does not declare, and an argument
/// that takes a free value.
fn classify(reference: &Reference, argument: &str) -> Option<CompletionClass> {
    match reference {
        Reference::Prompt(prompt) => {
            let template = prompts::registry::get_template(&prompt.name).ok().flatten()?;
            let declared = template
                .front_matter
                .arguments
                .iter()
                .any(|declared| declared.name == argument);
            declared
                .then(|| CompletionClass::of_prompt_argument(argument))
                .flatten()
        }
        Reference::Resource(resource) => {
            let variables = resources::registry::template_variables(&resource.uri)?;
            let declared = variables.iter().any(|variable| *variable == argument);
            declared
                .then(|| CompletionClass::of_template_variable(argument))
                .flatten()
        }
        // `Reference` is non-exhaustive: a kind this server does not publish has no candidates.
        _ => None,
    }
}

/// The logical cluster already chosen in the completion context, if any.
///
/// A Topic, Consumer Group, or Broker is completed inside this cluster only. Without it there are
/// no candidates: the default cluster is never assumed for a name the caller has not asked about.
fn context_cluster(request: &CompleteRequestParams) -> Option<&str> {
    request
        .context
        .as_ref()
        .and_then(|context| context.get_argument("cluster"))
        .map(|cluster| cluster.trim())
        .filter(|cluster| !cluster.is_empty())
}

/// What the audit record hashes: the reference, the argument, its partial value and the cluster.
fn audit_arguments(request: &CompleteRequestParams, cluster: Option<&str>) -> JsonObject {
    let target = match &request.r#ref {
        Reference::Prompt(prompt) => prompt.name.as_str(),
        Reference::Resource(resource) => resource.uri.as_str(),
        _ => "",
    };
    JsonObject::from_iter([
        ("ref".to_string(), Value::from(request.r#ref.reference_type())),
        ("target".to_string(), Value::from(target)),
        ("argument".to_string(), Value::from(request.argument.name.as_str())),
        ("value".to_string(), Value::from(request.argument.value.as_str())),
        ("cluster".to_string(), cluster.map_or(Value::Null, Value::from)),
    ])
}

fn starts_with_ignore_ascii_case(name: &str, prefix: &str) -> bool {
    name.len() >= prefix.len() && name.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

fn completion(matching: Vec<String>) -> CompleteResult {
    let total = matching.len();
    let has_more = total > CompletionInfo::MAX_VALUES;
    let mut values = matching;
    values.truncate(CompletionInfo::MAX_VALUES);
    // Truncated to the protocol maximum above, so the constructor cannot refuse the values.
    CompleteResult::new(
        CompletionInfo::with_pagination(values, Some(u32::try_from(total).unwrap_or(u32::MAX)), has_more)
            .unwrap_or_default(),
    )
}

/// Answers one `completion/complete` request.
///
/// Candidates are the names that start with the partial value, ignoring ASCII case, in sorted
/// order: at most 100 of them, with the number of matches and whether more exist. The result is
/// empty when the argument has no candidates, when the caller may not list the names, when the
/// request is rate limited, and when the names cannot be read; the audit log records the last
/// three as failures.
pub(crate) async fn complete<Q>(
    query: &Q,
    guard: &Guard,
    access: &RequestContext,
    request: &CompleteRequestParams,
) -> CompleteResult
where
    Q: ReadOnlyQuery,
{
    let Some(class) = classify(&request.r#ref, &request.argument.name) else {
        return CompleteResult::default();
    };
    let prefix = request.argument.value.as_str();
    let cluster = match class {
        CompletionClass::Cluster => None,
        CompletionClass::Topic | CompletionClass::ConsumerGroup | CompletionClass::Broker => context_cluster(request),
    };
    let Ok(guarded) = guard.begin_completion(
        access,
        class.backing_tool(),
        cluster,
        class.audit_operation(),
        &audit_arguments(request, cluster),
    ) else {
        return CompleteResult::default();
    };
    if prefix.len() > class.max_bytes() || prefix.chars().any(char::is_control) {
        return completion(guarded.finish(Vec::new()));
    }

    let names = match (class, cluster) {
        (CompletionClass::Cluster, _) => query
            .configured_clusters()
            .into_iter()
            .map(|entry| entry.name)
            .filter(|name| guard.allows_tool_on_cluster(access, ToolId::ListClusters, name))
            .collect(),
        (_, None) => Vec::new(),
        (_, Some(cluster)) => match query.completion_names(cluster, class, prefix).await {
            Ok(names) => names,
            Err(error) => {
                guarded.fail(error.code());
                return CompleteResult::default();
            }
        },
    };
    let matching = names
        .into_iter()
        .filter(|name| class.accepts(name) && starts_with_ignore_ascii_case(name, prefix))
        .collect();
    completion(guarded.finish(matching))
}

#[cfg(test)]
mod tests;

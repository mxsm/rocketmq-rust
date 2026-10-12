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

//! The `response_format` argument of the list Tools.
//!
//! A list Tool returns its items in one of two formats. `detailed`, the default, returns every
//! member of each item. `concise` returns only the members that name an item, for a caller that
//! wants to find a name and would pay for the rest in tokens and, for some lists, in reads.
//!
//! `ListItems` says for each list Tool where its items are and which members name one. The
//! published output schema and the `concise` result are both made from it, so they cannot
//! disagree: the schema requires exactly the members that `concise` keeps.

use rmcp::model::JsonObject;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::tools::catalog::ToolId;

/// How much of each item a list Tool returns.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[schemars(inline)]
pub enum ResponseFormat {
    Concise,
    #[default]
    Detailed,
}

impl ResponseFormat {
    /// The name of the argument in the input of a list Tool.
    pub(crate) const ARGUMENT: &'static str = "response_format";

    /// Takes the format out of the arguments of a call, leaving the arguments of the query.
    ///
    /// The input schema has already checked the value, and a Tool that is not a list Tool
    /// accepts no such argument. Anything but `concise` therefore reads as the default.
    pub(crate) fn take(arguments: &mut JsonObject) -> Self {
        match arguments.remove(Self::ARGUMENT) {
            Some(Value::String(format)) if format == "concise" => Self::Concise,
            _ => Self::Detailed,
        }
    }

    pub(crate) const fn is_concise(self) -> bool {
        matches!(self, Self::Concise)
    }

    /// Returns the result of `tool` in this format.
    pub(crate) fn shape(self, tool: ToolId, structured: &mut Value) {
        if let (Self::Concise, Some(items)) = (self, tool.list_items()) {
            items.keep_names(structured);
        }
    }
}

/// The published arguments of a list Tool: those of its query, and the format of the result.
///
/// It is only ever turned into a schema. A call is decoded by taking `response_format` out of
/// its arguments first, see [`ResponseFormat::take`].
#[derive(JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(
    dead_code,
    reason = "the type describes the arguments in the input schema and is never constructed"
)]
pub(crate) struct ListArguments<A> {
    #[serde(flatten)]
    query: A,
    /// How much of each item to return. `detailed`, the default, returns every field. `concise` returns only the
    /// fields that name an item, which is smaller and can be faster: use it to find a name, then ask for one item.
    #[serde(default)]
    response_format: ResponseFormat,
}

/// The items of a list Tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ListItems {
    /// The member of `data` that holds the items.
    pub(crate) array: &'static str,
    /// The definition of one item in the output schema of the Tool.
    pub(crate) definition: &'static str,
    /// The members that name an item. `concise` keeps these, and the schema requires no other.
    pub(crate) names: &'static [&'static str],
}

impl ListItems {
    /// Reduces every item of a result to the members that name it.
    fn keep_names(&self, structured: &mut Value) {
        let items = structured
            .get_mut("data")
            .and_then(|data| data.get_mut(self.array))
            .and_then(Value::as_array_mut);
        for item in items.into_iter().flatten() {
            if let Value::Object(members) = item {
                members.retain(|member, _| self.names.contains(&member.as_str()));
            }
        }
    }

    /// Lets an output schema accept an item in either format: of the members of an item, it
    /// then requires those that name it and no other.
    pub(crate) fn require_only_names(&self, output_schema: &mut JsonObject) {
        let required = output_schema
            .get_mut("$defs")
            .and_then(|definitions| definitions.get_mut(self.definition))
            .and_then(|definition| definition.get_mut("required"))
            .and_then(Value::as_array_mut);
        if let Some(required) = required {
            required.retain(|member| member.as_str().is_some_and(|member| self.names.contains(&member)));
        }
    }
}

impl ToolId {
    /// Returns the items of this Tool when it is a list Tool, one that takes `response_format`.
    pub(crate) const fn list_items(self) -> Option<ListItems> {
        match self {
            Self::ListTopics => Some(ListItems {
                array: "items",
                definition: "TopicListEntry",
                names: &["topic"],
            }),
            Self::ListConsumerGroups => Some(ListItems {
                array: "items",
                definition: "ConsumerGroupSummary",
                names: &["group"],
            }),
            Self::ListBrokers => Some(ListItems {
                array: "brokers",
                definition: "BrokerSummary",
                names: &["broker_name", "broker_id", "broker_active"],
            }),
            Self::ListConsumerConnections | Self::ListProducerConnections => Some(ListItems {
                array: "items",
                definition: "ConnectionRow",
                names: &["broker_name", "client_alias"],
            }),
            // `rocketmq_list_clusters` takes no argument at all, and its items are names already.
            Self::ListClusters
            | Self::GetClusterOverview
            | Self::DescribeTopic
            | Self::GetTopicRoute
            | Self::GetConsumerLag
            | Self::DescribeBroker
            | Self::GetBrokerDiagnostics
            | Self::GetBrokerConfigSummary
            | Self::GetBrokerLogFilterState
            | Self::GetProxyDrainState
            | Self::DiagnoseConsumerLag
            | Self::GetMessageMetadata
            | Self::GetTopicConfigState
            | Self::GetConsumerGroupConfigState
            | Self::GetTopicStats
            | Self::GetTopicConfig
            | Self::GetConsumerGroupDetails
            | Self::GetConsumerProgress
            | Self::GetHaStatus
            | Self::GetControllerMetadata
            | Self::GetNameserverConfigSummary
            | Self::DiagnoseConsumerGroup
            | Self::DiagnoseBrokerHealth
            | Self::GetClusterHealth
            | Self::DiagnoseMessageDelivery
            | Self::GetConsumerRunningInfo
            | Self::GetDlqSummary
            | Self::QueryMessageTrace
            | Self::FindMessagesByKey
            | Self::GetLiteBrokerInfo
            // Its items are names already, like those of `rocketmq_list_clusters`.
            | Self::ListTopicConsumers => None,
            #[cfg(feature = "change-planning")]
            Self::PlanCreateTopic
            | Self::PlanUpdateTopicConfig
            | Self::PlanUpdateTopicPermissions
            | Self::PlanUpdateBrokerConfig
            | Self::PlanResetConsumerOffset => None,
            #[cfg(feature = "security-admin")]
            Self::ListAclUsers => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list_tools() -> impl Iterator<Item = (ToolId, ListItems)> {
        ToolId::ALL
            .iter()
            .filter_map(|tool| tool.list_items().map(|items| (*tool, items)))
    }

    #[test]
    fn every_list_tool_publishes_the_argument_and_a_schema_that_accepts_both_formats() {
        let listed = list_tools().map(|(tool, _)| tool.descriptor().name).collect::<Vec<_>>();
        assert_eq!(
            listed,
            [
                "rocketmq_list_topics",
                "rocketmq_list_consumer_groups",
                "rocketmq_list_consumer_connections",
                "rocketmq_list_producer_connections",
                "rocketmq_list_brokers",
            ]
        );

        for (tool, items) in list_tools() {
            let name = tool.descriptor().name;
            let definition = tool.definition();
            let argument = &definition.input_schema["properties"][ResponseFormat::ARGUMENT];
            assert_eq!(argument["enum"], serde_json::json!(["concise", "detailed"]), "{name}");
            assert_eq!(argument["default"], "detailed", "{name}");
            assert!(
                argument["description"]
                    .as_str()
                    .is_some_and(|text| !text.contains('\n')),
                "{name}"
            );
            assert_eq!(definition.input_schema["additionalProperties"], false, "{name}");
            let required = definition.input_schema.get("required").cloned().unwrap_or_default();
            assert!(!required.to_string().contains(ResponseFormat::ARGUMENT), "{name}");

            // The table names real members of the item the Tool returns.
            let output = definition
                .output_schema
                .as_ref()
                .expect("a list Tool has an output schema");
            let data = output["properties"]["data"]["$ref"]
                .as_str()
                .expect("data is a definition");
            let data = &output["$defs"][data.trim_start_matches("#/$defs/")];
            assert_eq!(
                data["properties"][items.array]["items"]["$ref"],
                format!("#/$defs/{}", items.definition),
                "{name}"
            );
            let item = &output["$defs"][items.definition];
            for member in items.names {
                assert!(item["properties"].get(*member).is_some(), "{name}: {member}");
            }
            assert_eq!(item["required"], serde_json::json!(items.names), "{name}");
        }
    }

    #[test]
    fn a_tool_that_is_not_a_list_tool_keeps_its_schemas() {
        for tool in ToolId::ALL.iter().filter(|tool| tool.list_items().is_none()) {
            let definition = tool.definition();
            let properties = definition.input_schema.get("properties");
            assert!(
                properties.is_none_or(|properties| properties.get(ResponseFormat::ARGUMENT).is_none()),
                "{}",
                tool.descriptor().name
            );
        }
        // The Broker rows of the overview are the same type as those of the Broker list, and
        // only the list accepts them without their detail.
        let overview = ToolId::GetClusterOverview.definition();
        let required = &overview.output_schema.as_ref().unwrap()["$defs"]["BrokerSummary"]["required"];
        assert!(required.as_array().unwrap().len() > 3);
    }

    #[test]
    fn the_format_is_taken_out_of_the_arguments_and_only_concise_changes_a_list() {
        let arguments = |format: Value| {
            serde_json::json!({ "cluster": "local-dev", "response_format": format })
                .as_object()
                .cloned()
                .unwrap()
        };
        for (format, expected) in [
            (serde_json::json!("concise"), ResponseFormat::Concise),
            (serde_json::json!("detailed"), ResponseFormat::Detailed),
            (Value::Null, ResponseFormat::Detailed),
        ] {
            let mut arguments = arguments(format);
            assert_eq!(ResponseFormat::take(&mut arguments), expected);
            assert_eq!(Value::Object(arguments), serde_json::json!({ "cluster": "local-dev" }));
        }
        assert_eq!(ResponseFormat::take(&mut JsonObject::new()), ResponseFormat::Detailed);

        let result = || {
            serde_json::json!({
                "partial": false,
                "data": {
                    "cluster": "local-dev",
                    "total_count": 1,
                    "items": [{ "group": "orders", "client_count": 2, "diff_total": 7 }],
                },
            })
        };
        let mut concise = result();
        ResponseFormat::Concise.shape(ToolId::ListConsumerGroups, &mut concise);
        assert_eq!(concise["data"]["items"], serde_json::json!([{ "group": "orders" }]));
        assert_eq!(concise["data"]["total_count"], 1);

        let mut detailed = result();
        ResponseFormat::Detailed.shape(ToolId::ListConsumerGroups, &mut detailed);
        assert_eq!(detailed, result());
        let mut other = result();
        ResponseFormat::Concise.shape(ToolId::GetConsumerLag, &mut other);
        assert_eq!(other, result());
    }
}

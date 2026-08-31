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

use percent_encoding::percent_decode_str;
use percent_encoding::utf8_percent_encode;
use percent_encoding::AsciiSet;
use percent_encoding::CONTROLS;

use crate::model::contract::PageRequest;
use crate::model::contract::MAX_PAGE_LIMIT;

pub const JSON_MIME_TYPE: &str = "application/json";
const CLUSTER_URI_PREFIX: &str = "rocketmq://clusters/";
const SYSTEM_URI_PREFIX: &str = "rocketmq://system/";
const RESOURCE_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'&')
    .add(b'/')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'`');

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceKind {
    Capabilities,
    SystemRuntimeV1,
    SystemObservabilityV1,
    Overview,
    Topics,
    Topic(String),
    TopicRoute(String),
    Brokers,
    Broker(String),
    ConsumerGroups,
    ConsumerGroup(String),
    ConsumerLag { group: String, topic: String },
}

impl ResourceKind {
    pub const ROOTS: [Self; 5] = [
        Self::Capabilities,
        Self::Overview,
        Self::Topics,
        Self::Brokers,
        Self::ConsumerGroups,
    ];
    pub const SYSTEM_ROOTS: [Self; 2] = [Self::SystemRuntimeV1, Self::SystemObservabilityV1];

    pub(crate) const fn metric_operation(&self) -> &'static str {
        match self {
            Self::Capabilities => "cluster_capabilities",
            Self::SystemRuntimeV1 => "system_runtime_v1",
            Self::SystemObservabilityV1 => "system_observability_v1",
            Self::Overview => "cluster_overview",
            Self::Topics => "topic_list",
            Self::Topic(_) => "topic_describe",
            Self::TopicRoute(_) => "topic_route",
            Self::Brokers => "broker_list",
            Self::Broker(_) => "broker_describe",
            Self::ConsumerGroups => "consumer_group_list",
            Self::ConsumerGroup(_) => "consumer_group_describe",
            Self::ConsumerLag { .. } => "consumer_lag",
        }
    }

    fn path(&self) -> String {
        match self {
            Self::Capabilities => "capabilities".to_string(),
            Self::SystemRuntimeV1 => "runtime/v1".to_string(),
            Self::SystemObservabilityV1 => "observability/v1".to_string(),
            Self::Overview => "overview".to_string(),
            Self::Topics => "topics".to_string(),
            Self::Topic(topic) => format!("topics/{}", encode_segment(topic)),
            Self::TopicRoute(topic) => format!("topics/{}/route", encode_segment(topic)),
            Self::Brokers => "brokers".to_string(),
            Self::Broker(broker) => format!("brokers/{}", encode_segment(broker)),
            Self::ConsumerGroups => "consumer-groups".to_string(),
            Self::ConsumerGroup(group) => format!("consumer-groups/{}", encode_segment(group)),
            Self::ConsumerLag { group, .. } => format!("consumer-groups/{}/lag", encode_segment(group)),
        }
    }

    pub fn title(&self) -> &'static str {
        match self {
            Self::Capabilities => "RocketMQ MCP capabilities",
            Self::SystemRuntimeV1 => "RocketMQ MCP runtime diagnostics",
            Self::SystemObservabilityV1 => "RocketMQ MCP observability status",
            Self::Overview => "RocketMQ cluster overview",
            Self::Topics => "RocketMQ topics",
            Self::Topic(_) => "RocketMQ topic",
            Self::TopicRoute(_) => "RocketMQ topic route",
            Self::Brokers => "RocketMQ brokers",
            Self::Broker(_) => "RocketMQ broker",
            Self::ConsumerGroups => "RocketMQ consumer groups",
            Self::ConsumerGroup(_) => "RocketMQ consumer group",
            Self::ConsumerLag { .. } => "RocketMQ consumer lag",
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            Self::Capabilities => "Versioned read-only MCP capability and schema digest manifest.",
            Self::SystemRuntimeV1 => "Authenticated, bounded, and sanitized MCP runtime diagnostics.",
            Self::SystemObservabilityV1 => "Authenticated and sanitized MCP observability status.",
            Self::Overview => "Read-only overview for one configured RocketMQ cluster.",
            Self::Topics => "Read-only topic inventory for one configured RocketMQ cluster.",
            Self::Topic(_) => "Read-only details for one RocketMQ topic.",
            Self::TopicRoute(_) => "Read-only routing information for one RocketMQ topic.",
            Self::Brokers => "Read-only broker inventory for one configured RocketMQ cluster.",
            Self::Broker(_) => "Read-only details for one RocketMQ broker.",
            Self::ConsumerGroups => "Read-only consumer group inventory for one configured RocketMQ cluster.",
            Self::ConsumerGroup(_) => "Read-only details for one RocketMQ consumer group.",
            Self::ConsumerLag { .. } => "Read-only lag details for one RocketMQ consumer group and topic.",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RocketmqResourceUri {
    cluster: Option<String>,
    pub kind: ResourceKind,
    query: ResourceQuery,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceQuery {
    pub filter: Option<String>,
    pub page: PageRequest,
}

impl RocketmqResourceUri {
    pub fn new(cluster: impl Into<String>, kind: ResourceKind) -> Self {
        Self {
            cluster: Some(cluster.into()),
            kind,
            query: ResourceQuery::default(),
        }
    }

    pub fn system(kind: ResourceKind) -> Option<Self> {
        matches!(
            kind,
            ResourceKind::SystemRuntimeV1 | ResourceKind::SystemObservabilityV1
        )
        .then_some(Self {
            cluster: None,
            kind,
            query: ResourceQuery::default(),
        })
    }

    pub fn cluster(&self) -> Option<&str> {
        self.cluster.as_deref()
    }

    pub fn is_system(&self) -> bool {
        self.cluster.is_none()
    }

    pub fn query(&self) -> &ResourceQuery {
        &self.query
    }

    pub fn parse(uri: &str) -> Option<Self> {
        if let Some(path) = uri.strip_prefix(SYSTEM_URI_PREFIX) {
            let kind = match path {
                "runtime/v1" => ResourceKind::SystemRuntimeV1,
                "observability/v1" => ResourceKind::SystemObservabilityV1,
                _ => return None,
            };
            return Self::system(kind);
        }

        let remainder = uri.strip_prefix(CLUSTER_URI_PREFIX)?;
        let (cluster, resource) = remainder.split_once('/')?;
        if cluster.is_empty() || cluster.contains(['?', '#']) || resource.contains('#') {
            return None;
        }
        let cluster = decode_segment(cluster)?;
        let (path, query) = match resource.split_once('?') {
            Some((path, query)) if !query.is_empty() => (path, Some(query)),
            Some(_) => return None,
            None => (resource, None),
        };
        let segments = path.split('/').collect::<Vec<_>>();
        if segments.iter().any(|segment| segment.is_empty()) {
            return None;
        }
        let parameters = parse_query(query)?;
        let (kind, query) = match segments.as_slice() {
            ["capabilities"] if parameters.is_empty() => (ResourceKind::Capabilities, ResourceQuery::default()),
            ["overview"] if parameters.is_empty() => (ResourceKind::Overview, ResourceQuery::default()),
            ["topics"] => (
                ResourceKind::Topics,
                resource_query(&parameters, &["filter", "limit", "cursor"])?,
            ),
            ["topics", topic] => (
                ResourceKind::Topic(decode_segment(topic)?),
                resource_query(&parameters, &["limit", "cursor"])?,
            ),
            ["topics", topic, "route"] => (
                ResourceKind::TopicRoute(decode_segment(topic)?),
                resource_query(&parameters, &["limit", "cursor"])?,
            ),
            ["brokers"] if parameters.is_empty() => (ResourceKind::Brokers, ResourceQuery::default()),
            ["brokers", broker] if parameters.is_empty() => {
                (ResourceKind::Broker(decode_segment(broker)?), ResourceQuery::default())
            }
            ["consumer-groups"] => (
                ResourceKind::ConsumerGroups,
                resource_query(&parameters, &["filter", "limit", "cursor"])?,
            ),
            ["consumer-groups", group] if parameters.is_empty() => (
                ResourceKind::ConsumerGroup(decode_segment(group)?),
                ResourceQuery::default(),
            ),
            ["consumer-groups", group, "lag"] => {
                if parameters
                    .keys()
                    .any(|key| !["topic", "limit", "cursor"].contains(&key.as_str()))
                {
                    return None;
                }
                let topic = parameters.get("topic").filter(|topic| !topic.is_empty())?.clone();
                (
                    ResourceKind::ConsumerLag {
                        group: decode_segment(group)?,
                        topic,
                    },
                    resource_query(&parameters, &["topic", "limit", "cursor"])?,
                )
            }
            _ => return None,
        };
        Some(Self {
            cluster: Some(cluster),
            kind,
            query,
        })
    }

    pub fn as_string(&self) -> String {
        match &self.cluster {
            Some(cluster) => {
                let base = format!("{CLUSTER_URI_PREFIX}{}/{}", encode_segment(cluster), self.kind.path());
                append_query(base, &self.kind, &self.query)
            }
            None => format!("{SYSTEM_URI_PREFIX}{}", self.kind.path()),
        }
    }

    pub fn name(&self) -> String {
        let path = self.kind.path().replace(['/', '-', '?', '=', '&'], "_");
        match &self.cluster {
            Some(cluster) => format!("{cluster}_{path}"),
            None => format!("system_{path}"),
        }
    }
}

fn parse_query(query: Option<&str>) -> Option<std::collections::BTreeMap<String, String>> {
    let Some(query) = query else {
        return Some(std::collections::BTreeMap::new());
    };
    let mut parameters = std::collections::BTreeMap::new();
    for parameter in query.split('&') {
        let (key, value) = parameter.split_once('=')?;
        if key.is_empty() || value.is_empty() || parameters.contains_key(key) {
            return None;
        }
        parameters.insert(key.to_string(), decode_segment(value)?);
    }
    Some(parameters)
}

fn resource_query(parameters: &std::collections::BTreeMap<String, String>, allowed: &[&str]) -> Option<ResourceQuery> {
    if parameters.keys().any(|key| !allowed.contains(&key.as_str())) {
        return None;
    }
    let limit = match parameters.get("limit") {
        Some(limit) => {
            let limit = limit.parse::<u32>().ok()?;
            if !(1..=MAX_PAGE_LIMIT).contains(&limit) {
                return None;
            }
            Some(limit)
        }
        None => None,
    };
    Some(ResourceQuery {
        filter: parameters.get("filter").cloned(),
        page: PageRequest {
            limit,
            cursor: parameters.get("cursor").cloned(),
        },
    })
}

fn append_query(base: String, kind: &ResourceKind, query: &ResourceQuery) -> String {
    let mut parameters = Vec::new();
    if let ResourceKind::ConsumerLag { topic, .. } = kind {
        parameters.push(format!("topic={}", encode_segment(topic)));
    }
    if let Some(filter) = query.filter.as_deref() {
        parameters.push(format!("filter={}", encode_segment(filter)));
    }
    if let Some(limit) = query.page.limit {
        parameters.push(format!("limit={limit}"));
    }
    if let Some(cursor) = query.page.cursor.as_deref() {
        parameters.push(format!("cursor={}", encode_segment(cursor)));
    }
    if parameters.is_empty() {
        base
    } else {
        format!("{base}?{}", parameters.join("&"))
    }
}

fn encode_segment(segment: &str) -> String {
    utf8_percent_encode(segment, RESOURCE_SEGMENT_ENCODE_SET).to_string()
}

fn decode_segment(segment: &str) -> Option<String> {
    if segment.is_empty() || !has_valid_percent_encoding(segment) {
        return None;
    }
    percent_decode_str(segment)
        .decode_utf8()
        .ok()
        .map(|value| value.into_owned())
}

fn has_valid_percent_encoding(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return false;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_only_cluster_scoped_resource_uris() {
        let parsed = RocketmqResourceUri::parse("rocketmq://clusters/local-dev/overview").unwrap();
        assert_eq!(parsed.cluster(), Some("local-dev"));
        assert_eq!(parsed.kind, ResourceKind::Overview);
        assert_eq!(parsed.as_string(), "rocketmq://clusters/local-dev/overview");

        assert!(RocketmqResourceUri::parse("rocketmq://clusters/local-dev/unknown").is_none());
        let topic = RocketmqResourceUri::parse("rocketmq://clusters/local-dev/topics/orders").unwrap();
        assert_eq!(topic.kind, ResourceKind::Topic("orders".to_string()));

        let route = RocketmqResourceUri::parse("rocketmq://clusters/local-dev/topics/orders/route").unwrap();
        assert_eq!(route.kind, ResourceKind::TopicRoute("orders".to_string()));

        let lag =
            RocketmqResourceUri::parse("rocketmq://clusters/local-dev/consumer-groups/orders-service/lag?topic=orders")
                .unwrap();
        assert_eq!(
            lag.kind,
            ResourceKind::ConsumerLag {
                group: "orders-service".to_string(),
                topic: "orders".to_string(),
            }
        );

        assert!(
            RocketmqResourceUri::parse("rocketmq://clusters/local-dev/consumer-groups/orders-service/lag").is_none()
        );
        assert!(RocketmqResourceUri::parse("rocketmq://clusters/local-dev/topics/orders?legacy=true").is_none());
        assert!(RocketmqResourceUri::parse("rocketmq://clusters/local-dev/topics/%invalid").is_none());
        assert!(RocketmqResourceUri::parse("file:///etc/passwd").is_none());
    }

    #[test]
    fn system_resources_are_exact_and_never_cluster_scoped() {
        let runtime = RocketmqResourceUri::parse("rocketmq://system/runtime/v1").unwrap();
        assert!(runtime.is_system());
        assert_eq!(runtime.kind, ResourceKind::SystemRuntimeV1);
        assert_eq!(runtime.as_string(), "rocketmq://system/runtime/v1");

        let observability = RocketmqResourceUri::parse("rocketmq://system/observability/v1").unwrap();
        assert_eq!(observability.kind, ResourceKind::SystemObservabilityV1);
        assert!(RocketmqResourceUri::parse("rocketmq://system/runtime/v2").is_none());
        assert!(RocketmqResourceUri::parse("rocketmq://clusters/local-dev/runtime/v1").is_none());
    }

    #[test]
    fn resource_uri_percent_encoding_round_trips_rocketmq_names() {
        let uri = RocketmqResourceUri::new(
            "local/dev",
            ResourceKind::ConsumerLag {
                group: "%RETRY%order/service".to_string(),
                topic: "orders?priority=high".to_string(),
            },
        );

        let encoded = uri.as_string();
        let decoded = RocketmqResourceUri::parse(&encoded).unwrap();

        assert_eq!(
            encoded,
            "rocketmq://clusters/local%2Fdev/consumer-groups/%25RETRY%25order%2Fservice/lag?topic=orders%3Fpriority%\
             3Dhigh"
        );
        assert_eq!(decoded, uri);
    }

    #[test]
    fn query_bearing_resource_uris_round_trip_bounded_pagination() {
        let uri = RocketmqResourceUri::parse(
            "rocketmq://clusters/local-dev/topics?filter=order%2Fpriority&limit=2&cursor=rmq-s2-ab.cd",
        )
        .unwrap();
        assert_eq!(uri.query().filter.as_deref(), Some("order/priority"));
        assert_eq!(uri.query().page.limit, Some(2));
        assert_eq!(uri.query().page.cursor.as_deref(), Some("rmq-s2-ab.cd"));
        assert_eq!(
            uri.as_string(),
            "rocketmq://clusters/local-dev/topics?filter=order%2Fpriority&limit=2&cursor=rmq-s2-ab.cd"
        );

        assert!(RocketmqResourceUri::parse("rocketmq://clusters/local-dev/topics?limit=0").is_none());
        assert!(RocketmqResourceUri::parse("rocketmq://clusters/local-dev/topics?unknown=x").is_none());
        assert!(RocketmqResourceUri::parse(
            "rocketmq://clusters/local-dev/consumer-groups/group/lag?topic=orders&topic=payments"
        )
        .is_none());
    }
}

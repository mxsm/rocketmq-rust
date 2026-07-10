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

pub const JSON_MIME_TYPE: &str = "application/json";
const URI_PREFIX: &str = "rocketmq://clusters/";
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
    pub const ROOTS: [Self; 4] = [Self::Overview, Self::Topics, Self::Brokers, Self::ConsumerGroups];

    fn path(&self) -> String {
        match self {
            Self::Overview => "overview".to_string(),
            Self::Topics => "topics".to_string(),
            Self::Topic(topic) => format!("topics/{}", encode_segment(topic)),
            Self::TopicRoute(topic) => format!("topics/{}/route", encode_segment(topic)),
            Self::Brokers => "brokers".to_string(),
            Self::Broker(broker) => format!("brokers/{}", encode_segment(broker)),
            Self::ConsumerGroups => "consumer-groups".to_string(),
            Self::ConsumerGroup(group) => format!("consumer-groups/{}", encode_segment(group)),
            Self::ConsumerLag { group, topic } => format!(
                "consumer-groups/{}/lag?topic={}",
                encode_segment(group),
                encode_segment(topic)
            ),
        }
    }

    pub fn title(&self) -> &'static str {
        match self {
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
    pub cluster: String,
    pub kind: ResourceKind,
}

impl RocketmqResourceUri {
    pub fn new(cluster: impl Into<String>, kind: ResourceKind) -> Self {
        Self {
            cluster: cluster.into(),
            kind,
        }
    }

    pub fn parse(uri: &str) -> Option<Self> {
        let remainder = uri.strip_prefix(URI_PREFIX)?;
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
        let kind = match (segments.as_slice(), query) {
            (["overview"], None) => ResourceKind::Overview,
            (["topics"], None) => ResourceKind::Topics,
            (["topics", topic], None) => ResourceKind::Topic(decode_segment(topic)?),
            (["topics", topic, "route"], None) => ResourceKind::TopicRoute(decode_segment(topic)?),
            (["brokers"], None) => ResourceKind::Brokers,
            (["brokers", broker], None) => ResourceKind::Broker(decode_segment(broker)?),
            (["consumer-groups"], None) => ResourceKind::ConsumerGroups,
            (["consumer-groups", group], None) => ResourceKind::ConsumerGroup(decode_segment(group)?),
            (["consumer-groups", group, "lag"], Some(query)) => {
                let topic = query
                    .strip_prefix("topic=")
                    .filter(|topic| !topic.is_empty() && !topic.contains('&'))?;
                ResourceKind::ConsumerLag {
                    group: decode_segment(group)?,
                    topic: decode_segment(topic)?,
                }
            }
            _ => return None,
        };
        Some(Self::new(cluster, kind))
    }

    pub fn as_string(&self) -> String {
        format!("{URI_PREFIX}{}/{}", encode_segment(&self.cluster), self.kind.path())
    }

    pub fn name(&self) -> String {
        let path = self.kind.path().replace(['/', '-', '?', '=', '&'], "_");
        format!("{}_{path}", self.cluster)
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
        assert_eq!(parsed.cluster, "local-dev");
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
}

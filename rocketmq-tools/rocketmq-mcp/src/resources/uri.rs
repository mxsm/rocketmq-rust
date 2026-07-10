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

pub const JSON_MIME_TYPE: &str = "application/json";
const URI_PREFIX: &str = "rocketmq://clusters/";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    Overview,
    Topics,
    Brokers,
    ConsumerGroups,
}

impl ResourceKind {
    pub const ALL: [Self; 4] = [Self::Overview, Self::Topics, Self::Brokers, Self::ConsumerGroups];

    fn path(self) -> &'static str {
        match self {
            Self::Overview => "overview",
            Self::Topics => "topics",
            Self::Brokers => "brokers",
            Self::ConsumerGroups => "consumer-groups",
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Self::Overview => "RocketMQ cluster overview",
            Self::Topics => "RocketMQ topics",
            Self::Brokers => "RocketMQ brokers",
            Self::ConsumerGroups => "RocketMQ consumer groups",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::Overview => "Read-only overview for one configured RocketMQ cluster.",
            Self::Topics => "Read-only topic inventory for one configured RocketMQ cluster.",
            Self::Brokers => "Read-only broker inventory for one configured RocketMQ cluster.",
            Self::ConsumerGroups => "Read-only consumer group inventory for one configured RocketMQ cluster.",
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
        let (cluster, path) = remainder.split_once('/')?;
        if cluster.is_empty() || cluster.contains(['?', '#']) || path.contains(['?', '#', '/']) {
            return None;
        }
        let kind = match path {
            "overview" => ResourceKind::Overview,
            "topics" => ResourceKind::Topics,
            "brokers" => ResourceKind::Brokers,
            "consumer-groups" => ResourceKind::ConsumerGroups,
            _ => return None,
        };
        Some(Self::new(cluster, kind))
    }

    pub fn as_string(&self) -> String {
        format!("{URI_PREFIX}{}/{}", self.cluster, self.kind.path())
    }

    pub fn name(&self) -> String {
        format!("{}_{}", self.cluster, self.kind.path().replace('-', "_"))
    }
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
        assert!(RocketmqResourceUri::parse("rocketmq://clusters/local-dev/topics/orders").is_none());
        assert!(RocketmqResourceUri::parse("file:///etc/passwd").is_none());
    }
}

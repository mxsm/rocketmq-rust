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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RocketmqResourceUri {
    ClusterOverview,
    Topics,
    Brokers,
    ConsumerGroups,
}

impl RocketmqResourceUri {
    pub const ALL: [Self; 4] = [Self::ClusterOverview, Self::Topics, Self::Brokers, Self::ConsumerGroups];

    pub fn parse(uri: &str) -> Option<Self> {
        match uri {
            "rocketmq://cluster/overview" => Some(Self::ClusterOverview),
            "rocketmq://topics" => Some(Self::Topics),
            "rocketmq://brokers" => Some(Self::Brokers),
            "rocketmq://consumer-groups" => Some(Self::ConsumerGroups),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClusterOverview => "rocketmq://cluster/overview",
            Self::Topics => "rocketmq://topics",
            Self::Brokers => "rocketmq://brokers",
            Self::ConsumerGroups => "rocketmq://consumer-groups",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::ClusterOverview => "cluster_overview",
            Self::Topics => "topics",
            Self::Brokers => "brokers",
            Self::ConsumerGroups => "consumer_groups",
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Self::ClusterOverview => "RocketMQ cluster overview",
            Self::Topics => "RocketMQ topics",
            Self::Brokers => "RocketMQ brokers",
            Self::ConsumerGroups => "RocketMQ consumer groups",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::ClusterOverview => "Configured RocketMQ clusters and nameserver endpoints.",
            Self::Topics => "RocketMQ topic inventory resource.",
            Self::Brokers => "RocketMQ broker inventory resource.",
            Self::ConsumerGroups => "RocketMQ consumer group inventory resource.",
        }
    }
}

impl TryFrom<&str> for RocketmqResourceUri {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value).ok_or(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_resources_have_stable_uris() {
        let uris = RocketmqResourceUri::ALL.map(RocketmqResourceUri::as_str);

        assert_eq!(
            uris,
            [
                "rocketmq://cluster/overview",
                "rocketmq://topics",
                "rocketmq://brokers",
                "rocketmq://consumer-groups",
            ]
        );
    }

    #[test]
    fn parse_accepts_only_mvp_resource_uris() {
        assert_eq!(
            RocketmqResourceUri::parse("rocketmq://cluster/overview"),
            Some(RocketmqResourceUri::ClusterOverview)
        );
        assert_eq!(
            RocketmqResourceUri::parse("rocketmq://consumer-groups"),
            Some(RocketmqResourceUri::ConsumerGroups)
        );
        assert_eq!(RocketmqResourceUri::parse("rocketmq://topics/foo"), None);
        assert_eq!(RocketmqResourceUri::parse("file:///etc/passwd"), None);
    }
}

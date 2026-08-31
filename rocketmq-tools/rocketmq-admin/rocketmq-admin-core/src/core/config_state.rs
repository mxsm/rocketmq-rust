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

//! Logical, address-free configuration state queries.

use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::query::AdminQueryResult;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

/// Maximum logical Broker targets accepted by one configuration state query.
pub const MAX_CONFIG_STATE_BROKERS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfigStateRequest {
    pub cluster: String,
    pub topic: String,
    pub broker_names: Vec<String>,
}

impl TopicConfigStateRequest {
    pub fn try_new<Names, Name>(
        cluster: impl Into<String>,
        topic: impl Into<String>,
        broker_names: Names,
    ) -> AdminResult<Self>
    where
        Names: IntoIterator<Item = Name>,
        Name: Into<String>,
    {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            topic: required("topic", topic)?,
            broker_names: canonical_broker_names(broker_names)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfigStateRow {
    pub broker_name: String,
    pub version: u64,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub order: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfigStateResult {
    pub topic: String,
    pub states: Vec<TopicConfigStateRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroupConfigStateRequest {
    pub cluster: String,
    pub group: String,
    pub broker_names: Vec<String>,
}

impl ConsumerGroupConfigStateRequest {
    pub fn try_new<Names, Name>(
        cluster: impl Into<String>,
        group: impl Into<String>,
        broker_names: Names,
    ) -> AdminResult<Self>
    where
        Names: IntoIterator<Item = Name>,
        Name: Into<String>,
    {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            group: required("group", group)?,
            broker_names: canonical_broker_names(broker_names)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroupConfigStateRow {
    pub broker_name: String,
    pub version: u64,
    pub retry_max_times: u32,
    pub retry_queue_nums: u32,
    pub consume_timeout_minutes: u32,
    pub consume_enable: bool,
    pub consume_from_min_enable: bool,
    pub consume_broadcast_enable: bool,
    pub consume_message_orderly: bool,
    pub broker_id: u64,
    pub which_broker_when_consume_slowly: u64,
    pub notify_consumer_ids_changed_enable: bool,
    pub group_sys_flag: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroupConfigStateResult {
    pub group: String,
    pub states: Vec<ConsumerGroupConfigStateRow>,
}

/// Read-only logical configuration state queries. Version values are opaque
/// CAS observations and do not confer mutation capability.
pub trait ConfigStateQueryAdmin: Send {
    fn query_topic_config_state<'a>(
        &'a mut self,
        request: &'a TopicConfigStateRequest,
    ) -> AdminFuture<'a, AdminQueryResult<TopicConfigStateResult>>;

    fn query_consumer_group_config_state<'a>(
        &'a mut self,
        request: &'a ConsumerGroupConfigStateRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ConsumerGroupConfigStateResult>>;
}

fn canonical_broker_names<Names, Name>(broker_names: Names) -> AdminResult<Vec<String>>
where
    Names: IntoIterator<Item = Name>,
    Name: Into<String>,
{
    let names = broker_names
        .into_iter()
        .map(Into::into)
        .map(|name: String| name.trim().to_string())
        .map(|name| {
            if name.is_empty()
                || name.len() > 128
                || !name.is_ascii()
                || !name
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
            {
                Err(AdminError::invalid_argument(
                    "broker_names",
                    "must contain only bounded logical Broker names",
                ))
            } else {
                Ok(name)
            }
        })
        .collect::<AdminResult<BTreeSet<_>>>()?;
    if names.is_empty() {
        return Err(AdminError::invalid_argument("broker_names", "must not be empty"));
    }
    if names.len() > MAX_CONFIG_STATE_BROKERS {
        return Err(AdminError::invalid_argument(
            "broker_names",
            format!("must contain at most {MAX_CONFIG_STATE_BROKERS} unique names"),
        ));
    }
    Ok(names.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_normalize_sort_deduplicate_and_bound_broker_names() {
        let topic = TopicConfigStateRequest::try_new(" cluster-a ", " orders ", [" broker-b ", "broker-a", "broker-b"])
            .unwrap();
        assert_eq!(topic.cluster, "cluster-a");
        assert_eq!(topic.topic, "orders");
        assert_eq!(topic.broker_names, ["broker-a", "broker-b"]);

        let group = ConsumerGroupConfigStateRequest::try_new("cluster-a", "group-a", ["broker-a"]).unwrap();
        assert_eq!(group.group, "group-a");
        assert!(TopicConfigStateRequest::try_new("cluster-a", "orders", Vec::<String>::new()).is_err());
        assert!(ConsumerGroupConfigStateRequest::try_new("cluster-a", "group-a", ["127.0.0.1:10911"]).is_err());
        assert!(TopicConfigStateRequest::try_new(
            "cluster-a",
            "orders",
            (0..=MAX_CONFIG_STATE_BROKERS).map(|index| format!("broker-{index}")),
        )
        .is_err());
    }
}

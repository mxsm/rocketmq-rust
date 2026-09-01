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

use serde::de::Error as _;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;

/// Closed state condition shared by supervised Topic and Subscription Group mutations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ExpectedState {
    Absent,
    Present { version: u64 },
}

impl<'de> Deserialize<'de> for ExpectedState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawExpectedState {
            kind: String,
            version: Option<u64>,
        }
        let raw = RawExpectedState::deserialize(deserializer)?;
        match (raw.kind.as_str(), raw.version) {
            ("absent", None) => Ok(Self::Absent),
            ("present", Some(version)) => Ok(Self::Present { version }),
            ("absent" | "present", _) => Err(D::Error::custom("expected state has invalid version presence")),
            _ => Err(D::Error::custom("unknown expected state kind")),
        }
    }
}

/// Complete allowlisted Topic state accepted by supervised replacement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SupervisedTopicConfig {
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
    pub order: bool,
    pub message_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SupervisedTopicConfigCasRequestBody {
    pub expected_state: ExpectedState,
    pub replacement: SupervisedTopicConfig,
}

/// Complete public Subscription Group state accepted by supervised replacement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SupervisedSubscriptionGroupConfig {
    pub consume_enable: bool,
    pub consume_from_min_enable: bool,
    pub consume_broadcast_enable: bool,
    pub consume_message_orderly: bool,
    pub retry_queue_nums: i32,
    pub retry_max_times: i32,
    pub broker_id: u64,
    pub which_broker_when_consume_slowly: u64,
    pub notify_consumer_ids_changed_enable: bool,
    pub group_sys_flag: i32,
    pub consume_timeout_minute: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SupervisedSubscriptionGroupConfigCasRequestBody {
    pub expected_state: ExpectedState,
    pub replacement: SupervisedSubscriptionGroupConfig,
}

/// Broker mutation preflight contains no arbitrary property map.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BrokerMutationConfigState {
    pub generation: u64,
    pub auto_create_topic_enable: bool,
    pub auto_create_subscription_group: bool,
    pub broker_permission: u32,
    pub default_topic_queue_nums: u32,
    pub message_index_enable: bool,
    pub trace_topic_enable: bool,
}

/// Exact request-mode value used for conditional inspection and replacement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SupervisedMessageRequestMode {
    pub mode: String,
    pub pop_share_queue_num: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ExpectedMessageRequestMode {
    Absent,
    Present { mode: String, pop_share_queue_num: i32 },
}

impl<'de> Deserialize<'de> for ExpectedMessageRequestMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields, rename_all = "snake_case")]
        struct RawExpectedMode {
            kind: String,
            mode: Option<String>,
            pop_share_queue_num: Option<i32>,
        }
        let raw = RawExpectedMode::deserialize(deserializer)?;
        match (raw.kind.as_str(), raw.mode, raw.pop_share_queue_num) {
            ("absent", None, None) => Ok(Self::Absent),
            ("present", Some(mode), Some(pop_share_queue_num)) => Ok(Self::Present {
                mode,
                pop_share_queue_num,
            }),
            ("absent" | "present", _, _) => Err(D::Error::custom("expected request mode has invalid value presence")),
            _ => Err(D::Error::custom("unknown expected request mode kind")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMessageRequestModeRequestBody {
    pub topic: String,
    pub consumer_group: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MessageRequestModeStateBody {
    pub current: Option<SupervisedMessageRequestMode>,
}

/// Durable state of an accepted supervised mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationPersistenceState {
    NotRequired,
    Persisted,
    Failed,
}

/// Conditional request-mode outcome, including applied and durability truth.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MessageRequestModeMutationResultBody {
    pub applied: bool,
    pub changed: bool,
    pub current: Option<SupervisedMessageRequestMode>,
    pub persistence: MutationPersistenceState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetMessageRequestModeCasRequestBody {
    pub topic: String,
    pub consumer_group: String,
    pub expected_state: ExpectedMessageRequestMode,
    pub replacement: SupervisedMessageRequestMode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StateCasResultBody {
    pub applied: bool,
    pub changed: bool,
    pub state: ExpectedState,
    pub persistence: MutationPersistenceState,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_state_rejects_unknown_variants_and_fields() {
        assert!(serde_json::from_str::<ExpectedState>(r#"{"kind":"missing"}"#).is_err());
        assert!(serde_json::from_str::<ExpectedState>(r#"{"kind":"absent","version":1}"#).is_err());
        assert_eq!(
            serde_json::from_str::<ExpectedState>(r#"{"kind":"present","version":7}"#)
                .expect("present state should decode"),
            ExpectedState::Present { version: 7 }
        );
    }

    #[test]
    fn expected_message_request_mode_is_closed_and_requires_complete_present_state() {
        for invalid in [
            r#"{"kind":"absent","mode":"PULL","pop_share_queue_num":0}"#,
            r#"{"kind":"present","mode":"PULL"}"#,
            r#"{"kind":"present","pop_share_queue_num":0}"#,
            r#"{"kind":"present","mode":"PULL","pop_share_queue_num":0,"extra":true}"#,
            r#"{"kind":"unknown"}"#,
        ] {
            assert!(
                serde_json::from_str::<ExpectedMessageRequestMode>(invalid).is_err(),
                "{invalid} should be rejected"
            );
        }
        assert_eq!(
            serde_json::from_str::<ExpectedMessageRequestMode>(
                r#"{"kind":"present","mode":"POP","pop_share_queue_num":4}"#
            )
            .expect("complete present state should decode"),
            ExpectedMessageRequestMode::Present {
                mode: "POP".to_owned(),
                pop_share_queue_num: 4,
            }
        );
    }
}

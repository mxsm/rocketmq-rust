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

use std::borrow::Cow;
use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::error::ControlError;
use crate::model::MutationArguments;

pub const MUTATION_RESULT_SCHEMA_VERSION: &str = "rocketmq-mcp-mutation.v1";
pub const UPSERT_TOPIC_TOOL: &str = "rocketmq_upsert_topic";
pub const UPSERT_CONSUMER_GROUP_TOOL: &str = "rocketmq_upsert_consumer_group";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MutationResultSchemaVersion {
    #[serde(rename = "rocketmq-mcp-mutation.v1")]
    V1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TopicUpsertOperation {
    #[serde(rename = "topic_upsert")]
    TopicUpsert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ConsumerGroupUpsertOperation {
    #[serde(rename = "consumer_group_upsert")]
    ConsumerGroupUpsert,
}

macro_rules! const_string_schema {
    ($type:ty, $name:literal, $value:literal) => {
        impl JsonSchema for $type {
            fn schema_name() -> Cow<'static, str> {
                $name.into()
            }

            fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
                schemars::json_schema!({"type": "string", "const": $value})
            }
        }
    };
}

const_string_schema!(
    MutationResultSchemaVersion,
    "MutationResultSchemaVersion",
    "rocketmq-mcp-mutation.v1"
);
const_string_schema!(TopicUpsertOperation, "TopicUpsertOperation", "topic_upsert");
const_string_schema!(
    ConsumerGroupUpsertOperation,
    "ConsumerGroupUpsertOperation",
    "consumer_group_upsert"
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum TopicMessageType {
    Normal,
    Fifo,
    Delay,
    Transaction,
    Unspecified,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TopicReplacement {
    #[schemars(range(min = 1, max = 127))]
    pub read_queue_nums: u32,
    #[schemars(range(min = 1, max = 127))]
    pub write_queue_nums: u32,
    #[schemars(range(min = 1, max = 7))]
    pub perm: u32,
    pub order: bool,
    pub message_type: TopicMessageType,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConsumerGroupReplacement {
    pub consume_enable: bool,
    pub consume_from_min_enable: bool,
    pub consume_broadcast_enable: bool,
    pub consume_message_orderly: bool,
    #[schemars(range(min = 0, max = 127))]
    pub retry_queue_nums: i32,
    #[schemars(range(min = -1, max = 10_000))]
    pub retry_max_times: i32,
    pub broker_id: u64,
    pub which_broker_when_consume_slowly: u64,
    pub notify_consumer_ids_changed_enable: bool,
    pub group_sys_flag: i32,
    #[schemars(range(min = 1, max = 10_080))]
    pub consume_timeout_minute: i32,
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpsertTopicArgs {
    #[schemars(regex(pattern = "^rocketmq-mcp-control\\.arguments\\.v1$"))]
    pub schema_version: String,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub cluster: String,
    #[schemars(length(min = 1, max = 127), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))]
    pub topic: String,
    #[schemars(
        length(min = 1, max = 64),
        inner(length(min = 1, max = 127), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))
    )]
    pub broker_names: Vec<String>,
    #[serde(flatten)]
    pub replacement: TopicReplacement,
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
    #[serde(default)]
    pub confirm: bool,
    #[serde(default)]
    #[schemars(length(min = 5, max = 256))]
    pub reason: Option<String>,
    #[serde(default)]
    #[schemars(length(min = 8, max = 64), regex(pattern = "^[a-zA-Z0-9._:-]+$"))]
    pub request_key: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpsertConsumerGroupArgs {
    #[schemars(regex(pattern = "^rocketmq-mcp-control\\.arguments\\.v1$"))]
    pub schema_version: String,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub cluster: String,
    #[schemars(length(min = 1, max = 255), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))]
    pub consumer_group: String,
    #[schemars(
        length(min = 1, max = 64),
        inner(length(min = 1, max = 127), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))
    )]
    pub broker_names: Vec<String>,
    #[serde(flatten)]
    pub replacement: ConsumerGroupReplacement,
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
    #[serde(default)]
    pub confirm: bool,
    #[serde(default)]
    #[schemars(length(min = 5, max = 256))]
    pub reason: Option<String>,
    #[serde(default)]
    #[schemars(length(min = 8, max = 64), regex(pattern = "^[a-zA-Z0-9._:-]+$"))]
    pub request_key: Option<String>,
}

impl std::fmt::Debug for UpsertTopicArgs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UpsertTopicArgs")
            .field("schema_version", &self.schema_version)
            .field("broker_count", &self.broker_names.len())
            .field("dry_run", &self.dry_run)
            .field("confirm", &self.confirm)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for UpsertConsumerGroupArgs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UpsertConsumerGroupArgs")
            .field("schema_version", &self.schema_version)
            .field("broker_count", &self.broker_names.len())
            .field("dry_run", &self.dry_run)
            .field("confirm", &self.confirm)
            .finish_non_exhaustive()
    }
}

impl UpsertTopicArgs {
    pub fn validate(&self, configured_default_dry_run: bool, dry_run_omitted: bool) -> Result<(), ControlError> {
        validate_common(
            &self.schema_version,
            if dry_run_omitted {
                configured_default_dry_run
            } else {
                self.dry_run
            },
            self.confirm,
            self.reason.as_deref(),
            self.request_key.as_deref(),
        )?;
        validate_target_set(&self.broker_names)?;
        validate_user_name(&self.topic, NameKind::Topic)?;
        if !(1..=127).contains(&self.replacement.read_queue_nums)
            || !(1..=127).contains(&self.replacement.write_queue_nums)
            || !(1..=7).contains(&self.replacement.perm)
            || self.replacement.perm & 0b110 == 0
        {
            return Err(ControlError::invalid_arguments());
        }
        Ok(())
    }

    pub fn effective_dry_run(&self, configured_default: bool, omitted: bool) -> bool {
        if omitted {
            configured_default
        } else {
            self.dry_run
        }
    }
}

impl UpsertConsumerGroupArgs {
    pub fn validate(&self, configured_default_dry_run: bool, dry_run_omitted: bool) -> Result<(), ControlError> {
        validate_common(
            &self.schema_version,
            if dry_run_omitted {
                configured_default_dry_run
            } else {
                self.dry_run
            },
            self.confirm,
            self.reason.as_deref(),
            self.request_key.as_deref(),
        )?;
        validate_target_set(&self.broker_names)?;
        validate_user_name(&self.consumer_group, NameKind::ConsumerGroup)?;
        #[cfg(feature = "write-tools")]
        if rocketmq_admin_core::core::consumer::is_protected_consumer_group(&self.consumer_group) {
            return Err(ControlError::invalid_arguments());
        }
        if self.replacement.retry_queue_nums < 0
            || self.replacement.retry_queue_nums > 127
            || self.replacement.retry_max_times < -1
            || self.replacement.retry_max_times > 10_000
            || self.replacement.consume_timeout_minute <= 0
            || self.replacement.consume_timeout_minute > 10_080
        {
            return Err(ControlError::invalid_arguments());
        }
        Ok(())
    }

    pub fn effective_dry_run(&self, configured_default: bool, omitted: bool) -> bool {
        if omitted {
            configured_default
        } else {
            self.dry_run
        }
    }
}

fn validate_common(
    schema_version: &str,
    dry_run: bool,
    confirm: bool,
    reason: Option<&str>,
    request_key: Option<&str>,
) -> Result<(), ControlError> {
    MutationArguments {
        schema_version: schema_version.to_owned(),
        dry_run,
        confirm,
        reason: reason.map(ToOwned::to_owned),
        request_key: request_key.map(ToOwned::to_owned),
    }
    .validate()
}

fn validate_target_set(broker_names: &[String]) -> Result<(), ControlError> {
    if !(1..=64).contains(&broker_names.len()) {
        return Err(ControlError::invalid_arguments());
    }
    let mut canonical = broker_names.to_vec();
    canonical.sort();
    if canonical.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ControlError::invalid_arguments());
    }
    for broker in broker_names {
        validate_user_name(broker, NameKind::Broker)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum NameKind {
    Topic,
    ConsumerGroup,
    Broker,
}

fn validate_user_name(value: &str, kind: NameKind) -> Result<(), ControlError> {
    let max = if matches!(kind, NameKind::ConsumerGroup) {
        255
    } else {
        127
    };
    let valid = !value.is_empty()
        && value.len() <= max
        && !contains_encoded_octet(value)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'%' | b'|' | b'-' | b'_'));
    let system = match kind {
        NameKind::Topic => {
            value.starts_with("%RETRY%")
                || value.starts_with("%DLQ%")
                || value.starts_with("rmq_sys_")
                || matches!(
                    value,
                    "TBW102"
                        | "SCHEDULE_TOPIC_XXXX"
                        | "BenchmarkTest"
                        | "RMQ_SYS_TRANS_HALF_TOPIC"
                        | "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC"
                        | "RMQ_SYS_TRACE_TOPIC"
                        | "RMQ_SYS_TRANS_OP_HALF_TOPIC"
                        | "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC"
                        | "TRANS_CHECK_MAX_TIME_TOPIC"
                        | "SELF_TEST_TOPIC"
                        | "OFFSET_MOVED_EVENT"
                        | "CHECKPOINT_TOPIC"
                )
        }
        NameKind::ConsumerGroup => false,
        NameKind::Broker => false,
    };
    if !valid || system {
        return Err(ControlError::invalid_arguments());
    }
    Ok(())
}

fn contains_encoded_octet(value: &str) -> bool {
    value
        .as_bytes()
        .windows(3)
        .any(|window| window[0] == b'%' && window[1].is_ascii_hexdigit() && window[2].is_ascii_hexdigit())
}

const fn default_dry_run() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MutationMode {
    DryRun,
    Execute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MutationStatus {
    Planned,
    Applied,
    Partial,
    Conflict,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceState {
    NotRequired,
    Persisted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VerificationState {
    NotPerformed,
    Verified,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    Conflict,
    InvalidData,
    Unavailable,
    PersistenceFailed,
    VerificationFailed,
    OrderReconciliationFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum VisibleState<T> {
    Unknown,
    Absent,
    Present { version: u64, value: T },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LogicalMutationTarget {
    pub broker_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TopicMutationResource {
    pub topic: String,
    pub brokers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConsumerGroupMutationResource {
    pub consumer_group: String,
    pub brokers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MutationTarget<T> {
    pub target: LogicalMutationTarget,
    pub before: VisibleState<T>,
    pub requested: T,
    pub after: Option<VisibleState<T>>,
    pub applied: bool,
    pub changed: bool,
    pub persistence: PersistenceState,
    pub verification: VerificationState,
    pub failure: Option<FailureCode>,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MutationToolResponse<O, R, T> {
    pub schema_version: MutationResultSchemaVersion,
    pub operation: O,
    pub cluster: String,
    pub mode: MutationMode,
    pub status: MutationStatus,
    pub target: R,
    pub before: BTreeMap<String, VisibleState<T>>,
    pub requested: T,
    #[schemars(required)]
    pub after: Option<BTreeMap<String, VisibleState<T>>>,
    pub targets: Vec<MutationTarget<T>>,
    pub warnings: Vec<String>,
}

pub type TopicMutationToolResponse =
    MutationToolResponse<TopicUpsertOperation, TopicMutationResource, TopicReplacement>;
pub type ConsumerGroupMutationToolResponse =
    MutationToolResponse<ConsumerGroupUpsertOperation, ConsumerGroupMutationResource, ConsumerGroupReplacement>;

impl<O, R, T> MutationToolResponse<O, R, T> {
    pub fn is_error(&self) -> bool {
        matches!(
            self.status,
            MutationStatus::Partial | MutationStatus::Conflict | MutationStatus::Failed
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION;

    #[test]
    fn topic_schema_and_names_are_closed() {
        let valid = serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "topic": "orders_v1",
            "broker_names": ["broker-b", "broker-a"],
            "read_queue_nums": 8,
            "write_queue_nums": 8,
            "perm": 6,
            "order": false,
            "message_type": "NORMAL"
        });
        let args: UpsertTopicArgs = serde_json::from_value(valid.clone()).unwrap();
        args.validate(true, true).unwrap();
        for topic in ["a".repeat(127), "orders|v1".to_owned()] {
            let mut case = valid.clone();
            case["topic"] = serde_json::json!(topic);
            serde_json::from_value::<UpsertTopicArgs>(case)
                .unwrap()
                .validate(true, true)
                .unwrap();
        }
        for (field, value) in [
            ("topic", serde_json::json!("10.0.0.1")),
            ("topic", serde_json::json!("%RETRY%orders")),
            ("topic", serde_json::json!("token=secret")),
            ("topic", serde_json::json!("a".repeat(128))),
            ("topic", serde_json::json!("10%2e0%2e0%2e1")),
            ("topic", serde_json::json!("token%3dsecret")),
            ("broker_names", serde_json::json!([])),
            ("broker_names", serde_json::json!(["broker-a", "broker-a"])),
            ("read_queue_nums", serde_json::json!(0)),
            ("message_type", serde_json::json!("OTHER")),
        ] {
            let mut case = valid.clone();
            case[field] = value;
            let rejected = serde_json::from_value::<UpsertTopicArgs>(case)
                .map_err(|_| ControlError::invalid_arguments())
                .and_then(|args| args.validate(true, true));
            assert!(rejected.is_err(), "accepted {field}");
        }
        for system in [
            "TBW102",
            "SCHEDULE_TOPIC_XXXX",
            "BenchmarkTest",
            "RMQ_SYS_TRANS_HALF_TOPIC",
            "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC",
            "RMQ_SYS_TRACE_TOPIC",
            "RMQ_SYS_TRANS_OP_HALF_TOPIC",
            "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC",
            "TRANS_CHECK_MAX_TIME_TOPIC",
            "SELF_TEST_TOPIC",
            "OFFSET_MOVED_EVENT",
            "CHECKPOINT_TOPIC",
            "rmq_sys_internal",
            "%DLQ%orders",
        ] {
            let mut case = valid.clone();
            case["topic"] = serde_json::json!(system);
            assert!(serde_json::from_value::<UpsertTopicArgs>(case)
                .unwrap()
                .validate(true, true)
                .is_err());
        }
        let mut sixty_four = valid.clone();
        sixty_four["broker_names"] =
            serde_json::json!((0..64).map(|index| format!("broker-{index:02}")).collect::<Vec<_>>());
        serde_json::from_value::<UpsertTopicArgs>(sixty_four.clone())
            .unwrap()
            .validate(true, true)
            .unwrap();
        sixty_four["broker_names"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!("broker-64"));
        assert!(serde_json::from_value::<UpsertTopicArgs>(sixty_four)
            .unwrap()
            .validate(true, true)
            .is_err());
        let mut unknown = valid;
        unknown["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<UpsertTopicArgs>(unknown).is_err());
    }

    #[test]
    fn consumer_group_schema_and_execution_arguments_are_closed() {
        let valid = serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "consumer_group": "orders_consumers",
            "broker_names": ["broker-b", "broker-a"],
            "consume_enable": true,
            "consume_from_min_enable": false,
            "consume_broadcast_enable": false,
            "consume_message_orderly": false,
            "retry_queue_nums": 1,
            "retry_max_times": 16,
            "broker_id": 0,
            "which_broker_when_consume_slowly": 1,
            "notify_consumer_ids_changed_enable": true,
            "group_sys_flag": 0,
            "consume_timeout_minute": 15
        });
        let args: UpsertConsumerGroupArgs = serde_json::from_value(valid.clone()).unwrap();
        args.validate(true, true).unwrap();

        #[cfg(feature = "write-tools")]
        for protected in [
            "DEFAULT_CONSUMER",
            "TOOLS_CONSUMER",
            "SCHEDULE_CONSUMER",
            "FILTERSRV_CONSUMER",
            "__MONITOR_CONSUMER",
            "SELF_TEST_C_GROUP",
            "CID_ONS-HTTP-PROXY",
            "CID_ONSAPI_PULL",
            "CID_ONSAPI_PERMISSION",
            "CID_ONSAPI_OWNER",
            "CID_RMQ_SYS_TRANS",
            "CID_RMQ_SYS_INTERNAL",
            "CID_DefaultHeartBeatSyncerTopic",
            "%SYS%INTERNAL",
        ] {
            let mut case = valid.clone();
            case["consumer_group"] = serde_json::json!(protected);
            assert_eq!(
                serde_json::from_value::<UpsertConsumerGroupArgs>(case)
                    .unwrap()
                    .validate(true, true)
                    .unwrap_err()
                    .code(),
                crate::error::ControlErrorCode::InvalidArguments
            );
        }

        for (field, value) in [
            ("consumer_group", serde_json::json!("broker.example.test:10911")),
            ("broker_names", serde_json::json!(["broker-a", "broker-a"])),
            ("retry_queue_nums", serde_json::json!(-1)),
            ("retry_max_times", serde_json::json!(-2)),
            ("consume_timeout_minute", serde_json::json!(0)),
        ] {
            let mut case = valid.clone();
            case[field] = value;
            let rejected = serde_json::from_value::<UpsertConsumerGroupArgs>(case)
                .map_err(|_| ControlError::invalid_arguments())
                .and_then(|args| args.validate(true, true));
            assert!(rejected.is_err(), "accepted {field}");
        }

        let mut execute = valid.clone();
        execute["dry_run"] = serde_json::json!(false);
        let args: UpsertConsumerGroupArgs = serde_json::from_value(execute.clone()).unwrap();
        assert!(args.validate(true, false).is_err());
        execute["confirm"] = serde_json::json!(true);
        execute["reason"] = serde_json::json!("approved group replacement");
        serde_json::from_value::<UpsertConsumerGroupArgs>(execute)
            .unwrap()
            .validate(true, false)
            .unwrap();
    }

    #[test]
    fn mutation_argument_debug_omits_identifiers_reason_and_request_key() {
        let topic: UpsertTopicArgs = serde_json::from_value(serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-sensitive",
            "topic": "secret-topic",
            "broker_names": ["secret-broker"],
            "read_queue_nums": 8,
            "write_queue_nums": 8,
            "perm": 6,
            "order": false,
            "message_type": "NORMAL",
            "dry_run": false,
            "confirm": true,
            "reason": "sensitive approval reason",
            "request_key": "sensitive-key"
        }))
        .unwrap();
        let rendered = format!("{topic:?}");
        for sensitive in [
            "cluster-sensitive",
            "secret-topic",
            "secret-broker",
            "sensitive approval reason",
            "sensitive-key",
        ] {
            assert!(!rendered.contains(sensitive));
        }
        assert!(rendered.contains("broker_count: 1"));

        let group: UpsertConsumerGroupArgs = serde_json::from_value(serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-private",
            "consumer_group": "private-consumers",
            "broker_names": ["private-broker"],
            "consume_enable": true,
            "consume_from_min_enable": false,
            "consume_broadcast_enable": false,
            "consume_message_orderly": false,
            "retry_queue_nums": 1,
            "retry_max_times": 16,
            "broker_id": 0,
            "which_broker_when_consume_slowly": 1,
            "notify_consumer_ids_changed_enable": true,
            "group_sys_flag": 0,
            "consume_timeout_minute": 15,
            "dry_run": false,
            "confirm": true,
            "reason": "private approval reason",
            "request_key": "private-key"
        }))
        .unwrap();
        let rendered = format!("{group:?}");
        for sensitive in [
            "cluster-private",
            "private-consumers",
            "private-broker",
            "private approval reason",
            "private-key",
        ] {
            assert!(!rendered.contains(sensitive));
        }
        assert!(rendered.contains("broker_count: 1"));
    }

    #[test]
    fn stable_response_snapshot_exposes_only_logical_mutation_state() {
        let response = MutationToolResponse {
            schema_version: MutationResultSchemaVersion::V1,
            operation: TopicUpsertOperation::TopicUpsert,
            cluster: "cluster-a".to_owned(),
            mode: MutationMode::Execute,
            status: MutationStatus::Partial,
            target: TopicMutationResource {
                topic: "orders".to_owned(),
                brokers: vec!["broker-a".to_owned()],
            },
            before: BTreeMap::from([("broker-a".to_owned(), VisibleState::Absent)]),
            requested: TopicReplacement {
                read_queue_nums: 8,
                write_queue_nums: 8,
                perm: 6,
                order: false,
                message_type: TopicMessageType::Normal,
            },
            after: Some(BTreeMap::from([(
                "broker-a".to_owned(),
                VisibleState::Present {
                    version: 1,
                    value: TopicReplacement {
                        read_queue_nums: 8,
                        write_queue_nums: 8,
                        perm: 6,
                        order: false,
                        message_type: TopicMessageType::Normal,
                    },
                },
            )])),
            targets: vec![MutationTarget {
                target: LogicalMutationTarget {
                    broker_name: "broker-a".to_owned(),
                },
                before: VisibleState::Absent,
                requested: TopicReplacement {
                    read_queue_nums: 8,
                    write_queue_nums: 8,
                    perm: 6,
                    order: false,
                    message_type: TopicMessageType::Normal,
                },
                after: Some(VisibleState::Present {
                    version: 1,
                    value: TopicReplacement {
                        read_queue_nums: 8,
                        write_queue_nums: 8,
                        perm: 6,
                        order: false,
                        message_type: TopicMessageType::Normal,
                    },
                }),
                applied: true,
                changed: true,
                persistence: PersistenceState::Persisted,
                verification: VerificationState::Verified,
                failure: Some(FailureCode::OrderReconciliationFailed),
                retryable: false,
            }],
            warnings: vec!["topic order configuration was not reconciled".to_owned()],
        };
        insta::assert_json_snapshot!("control_mutation_response", response);
    }
}

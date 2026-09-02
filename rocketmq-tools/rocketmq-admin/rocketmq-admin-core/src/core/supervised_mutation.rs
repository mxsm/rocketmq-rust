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

//! Closed, presentation-independent contracts for supervised mutations.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::de::Error as _;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;

use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

pub const MAX_OFFSET_RESET_TARGETS: usize = 1_000;
pub const MAX_METADATA_MUTATION_TARGETS: usize = 64;

/// Private session identity carried by plans so they cannot cross an admin-session boundary.
pub(crate) struct MutationPlanSeal;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicMessageType {
    Normal,
    Fifo,
    Delay,
    Transaction,
    Unspecified,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicReplacement {
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
    pub order: bool,
    pub message_type: TopicMessageType,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionGroupReplacement {
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
#[serde(deny_unknown_fields)]
pub struct TopicMutationPreflightRequest {
    pub cluster: String,
    pub topic: String,
    pub replacement: TopicReplacement,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionGroupMutationPreflightRequest {
    pub cluster: String,
    pub consumer_group: String,
    pub replacement: SubscriptionGroupReplacement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationFailureCode {
    Conflict,
    InvalidData,
    Unavailable,
    PersistenceFailed,
    VerificationFailed,
    OrderReconciliationFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationPersistenceState {
    NotRequired,
    Persisted,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationVerificationState {
    NotPerformed,
    Verified,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MutationTargetFailure {
    pub broker_name: String,
    pub queue_id: Option<i32>,
    pub code: MutationFailureCode,
    pub retryable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataPreflightTarget<T> {
    pub broker_name: String,
    pub state: ExpectedState,
    pub current: Option<T>,
}

#[derive(Clone)]
pub(crate) struct ResolvedMetadataTarget<T> {
    pub(crate) broker_name: String,
    #[allow(
        dead_code,
        reason = "resolved addresses are consumed only by the optional mutation adapter"
    )]
    pub(crate) broker_addr: String,
    pub(crate) state: ExpectedState,
    pub(crate) current: Option<T>,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct TargetedTopicOrderGuard {
    pub(crate) expected: Option<BTreeMap<String, u32>>,
}

#[derive(Clone)]
pub struct TopicMutationPlan {
    pub(crate) seal: Arc<MutationPlanSeal>,
    pub(crate) cluster: String,
    pub(crate) topic: String,
    pub(crate) replacement: TopicReplacement,
    pub(crate) targets: Vec<ResolvedMetadataTarget<TopicReplacement>>,
    pub(crate) failures: Vec<MutationTargetFailure>,
    pub(crate) targeted_order_guard: Option<TargetedTopicOrderGuard>,
}

impl std::fmt::Debug for TopicMutationPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _session_bound = &self.seal;
        formatter
            .debug_struct("TopicMutationPlan")
            .field("cluster", &self.cluster)
            .field("topic", &self.topic)
            .field("replacement", &self.replacement)
            .field("targets", &self.preflight_targets())
            .field("failures", &self.failures)
            .field("targeted_order_guarded", &self.targeted_order_guard.is_some())
            .finish()
    }
}

impl TopicMutationPlan {
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub const fn replacement(&self) -> &TopicReplacement {
        &self.replacement
    }

    pub fn preflight_targets(&self) -> Vec<MetadataPreflightTarget<TopicReplacement>> {
        self.targets
            .iter()
            .map(|target| MetadataPreflightTarget {
                broker_name: target.broker_name.clone(),
                state: target.state,
                current: target.current.clone(),
            })
            .collect()
    }

    pub fn failures(&self) -> &[MutationTargetFailure] {
        &self.failures
    }
}

#[derive(Clone)]
pub struct SubscriptionGroupMutationPlan {
    pub(crate) seal: Arc<MutationPlanSeal>,
    pub(crate) cluster: String,
    pub(crate) consumer_group: String,
    pub(crate) replacement: SubscriptionGroupReplacement,
    pub(crate) targets: Vec<ResolvedMetadataTarget<SubscriptionGroupReplacement>>,
    pub(crate) failures: Vec<MutationTargetFailure>,
}

impl std::fmt::Debug for SubscriptionGroupMutationPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _session_bound = &self.seal;
        formatter
            .debug_struct("SubscriptionGroupMutationPlan")
            .field("cluster", &self.cluster)
            .field("consumer_group", &self.consumer_group)
            .field("replacement", &self.replacement)
            .field("targets", &self.preflight_targets())
            .field("failures", &self.failures)
            .finish()
    }
}

impl SubscriptionGroupMutationPlan {
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    pub fn consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub const fn replacement(&self) -> &SubscriptionGroupReplacement {
        &self.replacement
    }

    pub fn preflight_targets(&self) -> Vec<MetadataPreflightTarget<SubscriptionGroupReplacement>> {
        self.targets
            .iter()
            .map(|target| MetadataPreflightTarget {
                broker_name: target.broker_name.clone(),
                state: target.state,
                current: target.current.clone(),
            })
            .collect()
    }

    pub fn failures(&self) -> &[MutationTargetFailure] {
        &self.failures
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataMutationTargetOutcome {
    pub broker_name: String,
    pub expected_state: ExpectedState,
    pub resulting_state: Option<ExpectedState>,
    pub applied: bool,
    pub changed: bool,
    pub persistence: MutationPersistenceState,
    pub verification: MutationVerificationState,
    pub failure: Option<MutationFailureCode>,
    pub retryable: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataMutationOutcome {
    pub targets: Vec<MetadataMutationTargetOutcome>,
    pub failures: Vec<MutationTargetFailure>,
    pub order_reconciled: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OffsetResetPreviewRequest {
    pub cluster: String,
    pub topic: String,
    pub consumer_group: String,
    pub timestamp: i64,
    pub force: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OffsetResetPreviewRow {
    pub broker_name: String,
    pub queue_id: i32,
    pub current_offset: i64,
    pub planned_offset: i64,
    pub delta: i64,
    pub changed: bool,
}

#[derive(Clone)]
pub(crate) struct ResolvedOffsetResetTarget {
    #[allow(
        dead_code,
        reason = "resolved addresses are consumed only by the optional mutation adapter"
    )]
    pub(crate) broker_addr: String,
    pub(crate) row: OffsetResetPreviewRow,
}

#[derive(Clone)]
pub struct OffsetResetPlan {
    pub(crate) seal: Arc<MutationPlanSeal>,
    pub(crate) cluster: String,
    pub(crate) topic: String,
    pub(crate) consumer_group: String,
    pub(crate) timestamp: i64,
    pub(crate) force: bool,
    pub(crate) targets: Vec<ResolvedOffsetResetTarget>,
    failures: Vec<MutationTargetFailure>,
}

impl std::fmt::Debug for OffsetResetPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _session_bound = &self.seal;
        formatter
            .debug_struct("OffsetResetPlan")
            .field("cluster", &self.cluster)
            .field("topic", &self.topic)
            .field("consumer_group", &self.consumer_group)
            .field("timestamp", &self.timestamp)
            .field("force", &self.force)
            .field("rows", &self.rows())
            .field("failures", &self.failures)
            .finish()
    }
}

impl OffsetResetPlan {
    pub fn rows(&self) -> Vec<OffsetResetPreviewRow> {
        self.targets.iter().map(|target| target.row.clone()).collect()
    }

    pub fn failures(&self) -> &[MutationTargetFailure] {
        &self.failures
    }

    pub fn target_count(&self) -> usize {
        self.targets.len()
    }

    #[allow(dead_code, reason = "plan construction is owned by the optional mutation adapter")]
    pub(crate) fn try_new(
        seal: Arc<MutationPlanSeal>,
        cluster: String,
        topic: String,
        consumer_group: String,
        timestamp: i64,
        force: bool,
        mut targets: Vec<ResolvedOffsetResetTarget>,
        failures: Vec<MutationTargetFailure>,
    ) -> AdminResult<Self> {
        targets.sort_by(|left, right| {
            (&left.row.broker_name, left.row.queue_id).cmp(&(&right.row.broker_name, right.row.queue_id))
        });
        if targets.len() > MAX_OFFSET_RESET_TARGETS {
            return Err(AdminError::invalid_argument(
                "offsetTargets",
                format!("must contain at most {MAX_OFFSET_RESET_TARGETS} unique queue targets"),
            ));
        }
        if targets.windows(2).any(|pair| {
            pair[0].row.broker_name == pair[1].row.broker_name && pair[0].row.queue_id == pair[1].row.queue_id
        }) {
            return Err(AdminError::invalid_argument(
                "offsetTargets",
                "duplicate broker/queue target",
            ));
        }
        Ok(Self {
            seal,
            cluster,
            topic,
            consumer_group,
            timestamp,
            force,
            targets,
            failures,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OffsetResetTargetOutcome {
    pub broker_name: String,
    pub queue_id: i32,
    pub expected_offset: i64,
    pub planned_offset: i64,
    pub observed_offset: Option<i64>,
    pub applied: bool,
    pub changed: bool,
    pub failure: Option<MutationFailureCode>,
    pub retryable: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct OffsetResetOutcome {
    pub targets: Vec<OffsetResetTargetOutcome>,
    pub failures: Vec<MutationTargetFailure>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BrokerMutationConfigState {
    pub generation: u64,
    pub auto_create_topic_enable: bool,
    pub auto_create_subscription_group: bool,
    pub broker_permission: u32,
    pub default_topic_queue_nums: u32,
    pub message_index_enable: bool,
    pub trace_topic_enable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BrokerMutationConfigTarget {
    pub broker_name: String,
    pub state: BrokerMutationConfigState,
}

#[derive(Clone)]
pub struct BrokerMutationConfigPlan {
    pub(crate) seal: Arc<MutationPlanSeal>,
    pub(crate) cluster: String,
    pub(crate) targets: Vec<(String, String, BrokerMutationConfigState)>,
    pub(crate) failures: Vec<MutationTargetFailure>,
}

impl BrokerMutationConfigPlan {
    pub fn targets(&self) -> Vec<BrokerMutationConfigTarget> {
        self.targets
            .iter()
            .map(|(broker_name, _, state)| BrokerMutationConfigTarget {
                broker_name: broker_name.clone(),
                state: *state,
            })
            .collect()
    }

    pub fn failures(&self) -> &[MutationTargetFailure] {
        &self.failures
    }
}

impl std::fmt::Debug for BrokerMutationConfigPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _session_bound = &self.seal;
        formatter
            .debug_struct("BrokerMutationConfigPlan")
            .field("cluster", &self.cluster)
            .field("targets", &self.targets())
            .field("failures", &self.failures)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerMutationConfigPatch {
    pub auto_create_topic_enable: Option<bool>,
    pub auto_create_subscription_group: Option<bool>,
    pub broker_permission: Option<u32>,
    pub default_topic_queue_nums: Option<u32>,
    pub message_index_enable: Option<bool>,
    pub trace_topic_enable: Option<bool>,
}

impl BrokerMutationConfigPatch {
    pub const fn is_empty(self) -> bool {
        self.auto_create_topic_enable.is_none()
            && self.auto_create_subscription_group.is_none()
            && self.broker_permission.is_none()
            && self.default_topic_queue_nums.is_none()
            && self.message_index_enable.is_none()
            && self.trace_topic_enable.is_none()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestMode {
    Pull,
    Pop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestModeValue {
    pub mode: RequestMode,
    pub pop_share_queue_num: i32,
}

#[derive(Clone)]
pub struct RequestModeMutationPlan {
    pub(crate) seal: Arc<MutationPlanSeal>,
    pub(crate) cluster: String,
    pub(crate) topic: String,
    pub(crate) consumer_group: String,
    pub(crate) replacement: RequestModeValue,
    pub(crate) targets: Vec<(String, String, Option<RequestModeValue>)>,
    pub(crate) failures: Vec<MutationTargetFailure>,
}

impl RequestModeMutationPlan {
    pub fn targets(&self) -> Vec<(String, Option<RequestModeValue>)> {
        self.targets
            .iter()
            .map(|(broker, _, current)| (broker.clone(), *current))
            .collect()
    }

    pub fn failures(&self) -> &[MutationTargetFailure] {
        &self.failures
    }
}

impl std::fmt::Debug for RequestModeMutationPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _session_bound = &self.seal;
        formatter
            .debug_struct("RequestModeMutationPlan")
            .field("cluster", &self.cluster)
            .field("topic", &self.topic)
            .field("consumer_group", &self.consumer_group)
            .field("replacement", &self.replacement)
            .field("targets", &self.targets())
            .field("failures", &self.failures)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestModePreflightRequest {
    pub cluster: String,
    pub topic: String,
    pub consumer_group: String,
    pub replacement: RequestModeValue,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestModeTargetOutcome {
    pub broker_name: String,
    pub expected: Option<RequestModeValue>,
    pub current: Option<RequestModeValue>,
    pub applied: bool,
    pub changed: bool,
    pub persistence: MutationPersistenceState,
    pub verification: MutationVerificationState,
    pub failure: Option<MutationFailureCode>,
    pub retryable: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestModeMutationOutcome {
    pub targets: Vec<RequestModeTargetOutcome>,
    pub failures: Vec<MutationTargetFailure>,
}

#[allow(async_fn_in_trait)]
pub trait SupervisedMutationAdmin: Send {
    fn preflight_topic<'a>(
        &'a mut self,
        request: &'a TopicMutationPreflightRequest,
    ) -> AdminFuture<'a, TopicMutationPlan>;

    /// Preflights a topic replacement against only the named brokers in the
    /// selected cluster. Implementations must validate the complete selected
    /// cluster topology before issuing any target state request.
    fn preflight_topic_targets<'a>(
        &'a mut self,
        request: &'a TopicMutationPreflightRequest,
        broker_names: &'a [String],
    ) -> AdminFuture<'a, TopicMutationPlan> {
        let _ = (request, broker_names);
        Box::pin(async move {
            Err(AdminError::backend(
                "preflight_topic_targets",
                "targeted supervised topic preflight is unavailable",
            ))
        })
    }

    fn execute_topic<'a>(&'a mut self, plan: &'a TopicMutationPlan) -> AdminFuture<'a, MetadataMutationOutcome>;

    fn preflight_subscription_group<'a>(
        &'a mut self,
        request: &'a SubscriptionGroupMutationPreflightRequest,
    ) -> AdminFuture<'a, SubscriptionGroupMutationPlan>;

    /// Preflights a subscription-group replacement against only the named
    /// brokers in the selected cluster. Implementations must validate the
    /// complete selected cluster topology before issuing any target state
    /// request.
    fn preflight_subscription_group_targets<'a>(
        &'a mut self,
        request: &'a SubscriptionGroupMutationPreflightRequest,
        broker_names: &'a [String],
    ) -> AdminFuture<'a, SubscriptionGroupMutationPlan> {
        let _ = (request, broker_names);
        Box::pin(async move {
            Err(AdminError::backend(
                "preflight_subscription_group_targets",
                "targeted supervised subscription-group preflight is unavailable",
            ))
        })
    }

    fn execute_subscription_group<'a>(
        &'a mut self,
        plan: &'a SubscriptionGroupMutationPlan,
    ) -> AdminFuture<'a, MetadataMutationOutcome>;

    fn preview_offset_reset<'a>(
        &'a mut self,
        request: &'a OffsetResetPreviewRequest,
    ) -> AdminFuture<'a, OffsetResetPlan>;

    fn execute_offset_reset<'a>(&'a mut self, plan: &'a OffsetResetPlan) -> AdminFuture<'a, OffsetResetOutcome>;

    fn preflight_broker_config<'a>(&'a mut self, cluster: &'a str) -> AdminFuture<'a, BrokerMutationConfigPlan>;

    fn execute_broker_config_patch<'a>(
        &'a mut self,
        plan: &'a BrokerMutationConfigPlan,
        patch: BrokerMutationConfigPatch,
    ) -> AdminFuture<'a, MetadataMutationOutcome>;

    fn preflight_request_mode<'a>(
        &'a mut self,
        request: &'a RequestModePreflightRequest,
    ) -> AdminFuture<'a, RequestModeMutationPlan>;

    fn execute_request_mode<'a>(
        &'a mut self,
        plan: &'a RequestModeMutationPlan,
    ) -> AdminFuture<'a, RequestModeMutationOutcome>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_state_requires_exact_variant_fields() {
        assert_eq!(
            serde_json::from_str::<ExpectedState>(r#"{"kind":"absent"}"#).expect("absent"),
            ExpectedState::Absent
        );
        assert_eq!(
            serde_json::from_str::<ExpectedState>(r#"{"kind":"present","version":7}"#).expect("present"),
            ExpectedState::Present { version: 7 }
        );
        for invalid in [
            r#"{"kind":"absent","version":7}"#,
            r#"{"kind":"present"}"#,
            r#"{"kind":"present","version":7,"extra":true}"#,
            r#"{"kind":"future"}"#,
        ] {
            assert!(serde_json::from_str::<ExpectedState>(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn request_mode_preflight_rejects_unknown_top_level_and_nested_fields() {
        let valid = r#"{
            "cluster":"cluster-a",
            "topic":"orders",
            "consumer_group":"orders-consumer",
            "replacement":{"mode":"pull","pop_share_queue_num":0}
        }"#;
        assert!(serde_json::from_str::<RequestModePreflightRequest>(valid).is_ok());
        assert!(serde_json::from_str::<RequestModePreflightRequest>(
            r#"{
                "cluster":"cluster-a",
                "topic":"orders",
                "consumer_group":"orders-consumer",
                "replacement":{"mode":"pull","pop_share_queue_num":0},
                "unknown":true
            }"#,
        )
        .is_err());
        assert!(serde_json::from_str::<RequestModePreflightRequest>(
            r#"{
                "cluster":"cluster-a",
                "topic":"orders",
                "consumer_group":"orders-consumer",
                "replacement":{"mode":"pull","pop_share_queue_num":0,"unknown":true}
            }"#,
        )
        .is_err());
    }

    #[test]
    fn offset_plan_rejects_duplicates_and_more_than_one_thousand_targets() {
        let target = |queue_id| ResolvedOffsetResetTarget {
            broker_addr: "hidden".to_owned(),
            row: OffsetResetPreviewRow {
                broker_name: "broker-a".to_owned(),
                queue_id,
                current_offset: 10,
                planned_offset: 4,
                delta: -6,
                changed: true,
            },
        };
        assert!(OffsetResetPlan::try_new(
            Arc::new(MutationPlanSeal),
            "cluster-a".to_owned(),
            "topic-a".to_owned(),
            "group-a".to_owned(),
            1,
            false,
            vec![target(1), target(1)],
            vec![],
        )
        .is_err());
        let maximum = (0..MAX_OFFSET_RESET_TARGETS as i32).map(target).collect();
        assert!(OffsetResetPlan::try_new(
            Arc::new(MutationPlanSeal),
            "cluster-a".to_owned(),
            "topic-a".to_owned(),
            "group-a".to_owned(),
            1,
            false,
            maximum,
            vec![],
        )
        .is_ok());
        let overflow = (0..=MAX_OFFSET_RESET_TARGETS as i32).map(target).collect();
        assert!(OffsetResetPlan::try_new(
            Arc::new(MutationPlanSeal),
            "cluster-a".to_owned(),
            "topic-a".to_owned(),
            "group-a".to_owned(),
            1,
            false,
            overflow,
            vec![],
        )
        .is_err());
    }

    #[test]
    fn plans_never_expose_resolved_addresses_through_debug() {
        let plan = OffsetResetPlan::try_new(
            Arc::new(MutationPlanSeal),
            "cluster-a".to_owned(),
            "topic-a".to_owned(),
            "group-a".to_owned(),
            1,
            false,
            vec![ResolvedOffsetResetTarget {
                broker_addr: "192.0.2.1:10911".to_owned(),
                row: OffsetResetPreviewRow {
                    broker_name: "broker-a".to_owned(),
                    queue_id: 0,
                    current_offset: 2,
                    planned_offset: 1,
                    delta: -1,
                    changed: true,
                },
            }],
            vec![],
        )
        .expect("valid plan");
        assert!(!format!("{plan:?}").contains("192.0.2.1"));
    }
}

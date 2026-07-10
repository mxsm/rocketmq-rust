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

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanRequest<T> {
    pub cluster: String,
    pub reason: String,
    pub desired: T,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CreateTopicDesiredState {
    pub topic: String,
    #[serde(default)]
    pub read_queue_nums: Option<u32>,
    #[serde(default)]
    pub write_queue_nums: Option<u32>,
    #[serde(default)]
    pub perm: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpdateTopicConfigDesiredState {
    pub topic: String,
    pub config_key: String,
    pub config_value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpdateTopicPermissionsDesiredState {
    pub topic: String,
    pub perm: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpdateBrokerConfigDesiredState {
    pub broker_name: String,
    pub config_key: String,
    pub config_value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResetConsumerOffsetDesiredState {
    pub topic: String,
    pub consumer_group: String,
    #[serde(default)]
    pub target_offset: Option<i64>,
    #[serde(default)]
    pub timestamp_millis: Option<i64>,
}

pub type CreateTopicArgs = PlanRequest<CreateTopicDesiredState>;
pub type UpdateTopicConfigArgs = PlanRequest<UpdateTopicConfigDesiredState>;
pub type UpdateTopicPermArgs = PlanRequest<UpdateTopicPermissionsDesiredState>;
pub type UpdateBrokerConfigArgs = PlanRequest<UpdateBrokerConfigDesiredState>;
pub type ResetConsumerOffsetArgs = PlanRequest<ResetConsumerOffsetDesiredState>;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangePlanType {
    CreateTopic,
    UpdateTopicConfig,
    UpdateTopicPermissions,
    UpdateBrokerConfig,
    ResetConsumerOffset,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ChangePlan {
    pub plan_type: ChangePlanType,
    pub cluster: String,
    pub reason: String,
    pub summary: String,
    pub planned_changes: Vec<String>,
    pub impact_analysis: Vec<String>,
    pub rollback_suggestions: Vec<String>,
    pub mutates_cluster: bool,
}

pub fn plan_create_topic(request: CreateTopicArgs) -> ChangePlan {
    planned_change(
        ChangePlanType::CreateTopic,
        request,
        |desired| {
            vec![format!(
                "Create topic `{}` with read_queue_nums={:?}, write_queue_nums={:?}, perm={:?}.",
                desired.topic, desired.read_queue_nums, desired.write_queue_nums, desired.perm
            )]
        },
        |desired| {
            vec![format!(
                "Namesrv and brokers would expose a new topic named `{}` if an operator applies a later plan.",
                desired.topic
            )]
        },
        |desired| {
            vec![format!(
                "Record an operator-reviewed removal or disable procedure for topic `{}` before any future apply.",
                desired.topic
            )]
        },
    )
}

pub fn plan_update_topic_config(request: UpdateTopicConfigArgs) -> ChangePlan {
    planned_change(
        ChangePlanType::UpdateTopicConfig,
        request,
        |desired| {
            vec![format!(
                "Update topic `{}` config `{}` to `{}`.",
                desired.topic, desired.config_key, desired.config_value
            )]
        },
        |desired| {
            vec![format!(
                "Producers and consumers using topic `{}` may observe the new `{}` value after a future apply.",
                desired.topic, desired.config_key
            )]
        },
        |desired| {
            vec![format!(
                "Record the previous `{}` value before apply and restore it if validation fails.",
                desired.config_key
            )]
        },
    )
}

pub fn plan_update_topic_perm(request: UpdateTopicPermArgs) -> ChangePlan {
    planned_change(
        ChangePlanType::UpdateTopicPermissions,
        request,
        |desired| {
            vec![format!(
                "Update topic `{}` permission to `{}`.",
                desired.topic, desired.perm
            )]
        },
        |desired| {
            vec![format!(
                "Topic `{}` clients may gain or lose read/write access after a future apply.",
                desired.topic
            )]
        },
        |desired| {
            vec![format!(
                "Record the current permission for topic `{}` and restore it if client validation fails.",
                desired.topic
            )]
        },
    )
}

pub fn plan_update_broker_config(request: UpdateBrokerConfigArgs) -> ChangePlan {
    planned_change(
        ChangePlanType::UpdateBrokerConfig,
        request,
        |desired| {
            vec![format!(
                "Update broker `{}` config `{}` to `{}`.",
                desired.broker_name, desired.config_key, desired.config_value
            )]
        },
        |desired| {
            vec![format!(
                "Broker `{}` behavior may change after `{}` is applied by an operator.",
                desired.broker_name, desired.config_key
            )]
        },
        |desired| {
            vec![format!(
                "Record the previous broker `{}` value and restore it if broker health checks fail.",
                desired.config_key
            )]
        },
    )
}

pub fn plan_reset_consumer_offset(request: ResetConsumerOffsetArgs) -> ChangePlan {
    planned_change(
        ChangePlanType::ResetConsumerOffset,
        request,
        |desired| {
            vec![format!(
                "Reset consumer group `{}` offset for topic `{}` to offset {:?} or timestamp {:?}.",
                desired.consumer_group, desired.topic, desired.target_offset, desired.timestamp_millis
            )]
        },
        |desired| {
            vec![format!(
                "Consumer group `{}` may reconsume or skip messages on topic `{}` after a future apply.",
                desired.consumer_group, desired.topic
            )]
        },
        |desired| {
            vec![format!(
                "Capture current offsets for `{}` on `{}` before apply so they can be restored.",
                desired.consumer_group, desired.topic
            )]
        },
    )
}

fn planned_change<T, P, I, R>(
    plan_type: ChangePlanType,
    request: PlanRequest<T>,
    planned_changes: P,
    impact_analysis: I,
    rollback_suggestions: R,
) -> ChangePlan
where
    P: FnOnce(&T) -> Vec<String>,
    I: FnOnce(&T) -> Vec<String>,
    R: FnOnce(&T) -> Vec<String>,
{
    ChangePlan {
        plan_type,
        cluster: request.cluster,
        reason: request.reason,
        summary: format!("Generated a non-mutating {:?} plan.", plan_type),
        planned_changes: planned_changes(&request.desired),
        impact_analysis: impact_analysis(&request.desired),
        rollback_suggestions: rollback_suggestions(&request.desired),
        mutates_cluster: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_topic_plan_is_non_mutating() {
        let plan = plan_create_topic(PlanRequest {
            cluster: "local-dev".to_string(),
            reason: "capacity preparation".to_string(),
            desired: CreateTopicDesiredState {
                topic: "orders".to_string(),
                read_queue_nums: Some(8),
                write_queue_nums: Some(8),
                perm: Some("read_write".to_string()),
            },
        });

        assert_eq!(plan.plan_type, ChangePlanType::CreateTopic);
        assert!(!plan.mutates_cluster);
        assert!(!plan.planned_changes.is_empty());
    }
}

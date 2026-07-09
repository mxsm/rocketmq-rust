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

pub const CREATE_TOPIC_TOOL: &str = "mq_create_topic";
pub const UPDATE_TOPIC_CONFIG_TOOL: &str = "mq_update_topic_config";
pub const UPDATE_TOPIC_PERM_TOOL: &str = "mq_update_topic_perm";
pub const UPDATE_BROKER_CONFIG_TOOL: &str = "mq_update_broker_config";
pub const RESET_CONSUMER_OFFSET_TOOL: &str = "mq_reset_consumer_offset";

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ChangeRequest<T> {
    pub cluster: String,
    pub mode: ChangeMode,
    pub operator: String,
    pub reason: String,
    #[serde(default)]
    pub confirm_token: Option<String>,
    pub payload: T,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeMode {
    #[serde(alias = "dry-run", alias = "dryrun")]
    DryRun,
    Apply,
}

impl ChangeMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "dry_run",
            Self::Apply => "apply",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct CreateTopicPayload {
    pub topic: String,
    #[serde(default)]
    pub read_queue_nums: Option<u32>,
    #[serde(default)]
    pub write_queue_nums: Option<u32>,
    #[serde(default)]
    pub perm: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct UpdateTopicConfigPayload {
    pub topic: String,
    pub config_key: String,
    pub config_value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct UpdateTopicPermPayload {
    pub topic: String,
    pub perm: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct UpdateBrokerConfigPayload {
    pub broker_name: String,
    pub config_key: String,
    pub config_value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ResetConsumerOffsetPayload {
    pub topic: String,
    pub consumer_group: String,
    #[serde(default)]
    pub target_offset: Option<i64>,
    #[serde(default)]
    pub timestamp_millis: Option<i64>,
}

pub type CreateTopicArgs = ChangeRequest<CreateTopicPayload>;
pub type UpdateTopicConfigArgs = ChangeRequest<UpdateTopicConfigPayload>;
pub type UpdateTopicPermArgs = ChangeRequest<UpdateTopicPermPayload>;
pub type UpdateBrokerConfigArgs = ChangeRequest<UpdateBrokerConfigPayload>;
pub type ResetConsumerOffsetArgs = ChangeRequest<ResetConsumerOffsetPayload>;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ChangePlan {
    pub tool: String,
    pub cluster: String,
    pub mode: ChangeMode,
    pub operator: String,
    pub reason: String,
    pub summary: String,
    pub planned_changes: Vec<String>,
    pub impact_analysis: Vec<String>,
    pub rollback_suggestions: Vec<String>,
    pub confirm_challenge: Option<ConfirmChallenge>,
    pub applied: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ConfirmChallenge {
    pub challenge_id: String,
    pub instruction: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ChangeToolError {
    #[error("{0} apply mode is reserved; no mutation was executed")]
    ApplyReserved(&'static str),
}

pub fn plan_create_topic(request: CreateTopicArgs) -> Result<ChangePlan, ChangeToolError> {
    planned_change(
        CREATE_TOPIC_TOOL,
        request,
        |payload| {
            vec![format!(
                "Create topic `{}` with read_queue_nums={:?}, write_queue_nums={:?}, perm={:?}.",
                payload.topic, payload.read_queue_nums, payload.write_queue_nums, payload.perm
            )]
        },
        |payload| {
            vec![
                format!(
                    "Namesrv and brokers would expose a new topic named `{}`.",
                    payload.topic
                ),
                "No messages are produced, consumed, or moved during dry-run.".to_string(),
            ]
        },
        |payload| {
            vec![format!(
                "If applied later and validation fails, delete or disable topic `{}` through reviewed operator \
                 tooling.",
                payload.topic
            )]
        },
    )
}

pub fn plan_update_topic_config(request: UpdateTopicConfigArgs) -> Result<ChangePlan, ChangeToolError> {
    planned_change(
        UPDATE_TOPIC_CONFIG_TOOL,
        request,
        |payload| {
            vec![format!(
                "Update topic `{}` config `{}` to `{}`.",
                payload.topic, payload.config_key, payload.config_value
            )]
        },
        |payload| {
            vec![format!(
                "Producers and consumers using topic `{}` may observe the new `{}` value after apply.",
                payload.topic, payload.config_key
            )]
        },
        |payload| {
            vec![format!(
                "Record the previous `{}` value before apply and restore it if validation fails.",
                payload.config_key
            )]
        },
    )
}

pub fn plan_update_topic_perm(request: UpdateTopicPermArgs) -> Result<ChangePlan, ChangeToolError> {
    planned_change(
        UPDATE_TOPIC_PERM_TOOL,
        request,
        |payload| {
            vec![format!(
                "Update topic `{}` permission to `{}`.",
                payload.topic, payload.perm
            )]
        },
        |payload| {
            vec![format!(
                "Topic `{}` clients may gain or lose read/write access after apply.",
                payload.topic
            )]
        },
        |payload| {
            vec![format!(
                "Record the current permission for topic `{}` and restore it if client validation fails.",
                payload.topic
            )]
        },
    )
}

pub fn plan_update_broker_config(request: UpdateBrokerConfigArgs) -> Result<ChangePlan, ChangeToolError> {
    planned_change(
        UPDATE_BROKER_CONFIG_TOOL,
        request,
        |payload| {
            vec![format!(
                "Update broker `{}` config `{}` to `{}`.",
                payload.broker_name, payload.config_key, payload.config_value
            )]
        },
        |payload| {
            vec![format!(
                "Broker `{}` behavior may change after `{}` is applied.",
                payload.broker_name, payload.config_key
            )]
        },
        |payload| {
            vec![format!(
                "Record the previous broker `{}` value and restore it if broker health checks fail.",
                payload.config_key
            )]
        },
    )
}

pub fn plan_reset_consumer_offset(request: ResetConsumerOffsetArgs) -> Result<ChangePlan, ChangeToolError> {
    planned_change(
        RESET_CONSUMER_OFFSET_TOOL,
        request,
        |payload| {
            vec![format!(
                "Reset consumer group `{}` offset for topic `{}` to offset {:?} or timestamp {:?}.",
                payload.consumer_group, payload.topic, payload.target_offset, payload.timestamp_millis
            )]
        },
        |payload| {
            vec![
                format!(
                    "Consumer group `{}` may reconsume or skip messages on topic `{}` after apply.",
                    payload.consumer_group, payload.topic
                ),
                "Run dry-run evidence review with lag, queue offsets, and consumer ownership before any future apply."
                    .to_string(),
            ]
        },
        |payload| {
            vec![format!(
                "Capture current offsets for `{}` on `{}` before apply so they can be restored.",
                payload.consumer_group, payload.topic
            )]
        },
    )
}

fn planned_change<T, P, I, R>(
    tool: &'static str,
    request: ChangeRequest<T>,
    planned_changes: P,
    impact_analysis: I,
    rollback_suggestions: R,
) -> Result<ChangePlan, ChangeToolError>
where
    P: FnOnce(&T) -> Vec<String>,
    I: FnOnce(&T) -> Vec<String>,
    R: FnOnce(&T) -> Vec<String>,
{
    if matches!(request.mode, ChangeMode::Apply) {
        return Err(ChangeToolError::ApplyReserved(tool));
    }

    Ok(ChangePlan {
        tool: tool.to_string(),
        cluster: request.cluster.clone(),
        mode: request.mode,
        operator: request.operator.clone(),
        reason: request.reason.clone(),
        summary: format!("{tool} {} generated a dry-run change plan.", request.mode.as_str()),
        planned_changes: planned_changes(&request.payload),
        impact_analysis: impact_analysis(&request.payload),
        rollback_suggestions: rollback_suggestions(&request.payload),
        confirm_challenge: Some(ConfirmChallenge {
            challenge_id: format!("dry-run:{tool}:{}:{}", request.cluster, request.operator),
            instruction: "Future apply support must submit mode=apply with a reviewed confirm_token.".to_string(),
        }),
        applied: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dry_run_create_topic_returns_change_plan() {
        let plan = plan_create_topic(ChangeRequest {
            cluster: "local-dev".to_string(),
            mode: ChangeMode::DryRun,
            operator: "sre".to_string(),
            reason: "capacity preparation".to_string(),
            confirm_token: None,
            payload: CreateTopicPayload {
                topic: "orders".to_string(),
                read_queue_nums: Some(8),
                write_queue_nums: Some(8),
                perm: Some("read_write".to_string()),
            },
        })
        .unwrap();

        assert_eq!(plan.tool, CREATE_TOPIC_TOOL);
        assert!(!plan.applied);
        assert!(plan.confirm_challenge.is_some());
        assert!(plan.planned_changes[0].contains("orders"));
    }

    #[test]
    fn apply_mode_is_reserved() {
        let err = plan_reset_consumer_offset(ChangeRequest {
            cluster: "local-dev".to_string(),
            mode: ChangeMode::Apply,
            operator: "sre".to_string(),
            reason: "lag mitigation".to_string(),
            confirm_token: Some("reviewed".to_string()),
            payload: ResetConsumerOffsetPayload {
                topic: "orders".to_string(),
                consumer_group: "order-service".to_string(),
                target_offset: Some(10),
                timestamp_millis: None,
            },
        })
        .unwrap_err();

        assert!(err.to_string().contains("reserved"));
    }
}

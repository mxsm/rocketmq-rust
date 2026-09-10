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

use rocketmq_admin_core::core::topic::{TopicBatchOrderConfigOutcome, TopicBatchTargetOutcome};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TopicBatchOperation {
    Create,
    Update,
    Delete,
    DeleteBroker,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TopicTargetKind {
    Broker,
    Cluster,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicTargetReceipt {
    pub(crate) kind: TopicTargetKind,
    pub(crate) name: String,
    pub(crate) success: bool,
    pub(crate) error_code: Option<String>,
    pub(crate) message: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicOrderReceipt {
    pub(crate) success: bool,
    pub(crate) error_code: Option<String>,
    pub(crate) message: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicBatchResult {
    pub(crate) operation: TopicBatchOperation,
    pub(crate) topic: String,
    pub(crate) target_count: usize,
    pub(crate) targets: Vec<TopicTargetReceipt>,
    pub(crate) order_config: Option<TopicOrderReceipt>,
    pub(crate) success: bool,
    pub(crate) message: String,
}

pub(crate) fn project_batch(
    operation: TopicBatchOperation,
    topic: String,
    kind: TopicTargetKind,
    targets: Vec<TopicBatchTargetOutcome>,
    order_config: Option<TopicBatchOrderConfigOutcome>,
) -> TopicBatchResult {
    let order_required = operation != TopicBatchOperation::DeleteBroker;
    let success = !targets.is_empty()
        && targets.iter().all(|target| target.success)
        && (!order_required || order_config.as_ref().is_some_and(|result| result.success));
    TopicBatchResult {
        operation,
        topic,
        target_count: targets.len(),
        success,
        message: if success {
            "All requested targets completed."
        } else {
            "Some targets did not complete. Review each result before retrying."
        }
        .into(),
        targets: targets
            .into_iter()
            .map(|target| TopicTargetReceipt {
                kind,
                name: target.broker_name,
                success: target.success,
                error_code: (!target.success).then(|| "dashboard.topic_target_failed".into()),
                message: if target.success {
                    "Target completed."
                } else {
                    "Target operation did not complete; verify its current state."
                }
                .into(),
            })
            .collect(),
        order_config: order_config.map(|result| TopicOrderReceipt {
            success: result.success,
            error_code: (!result.success).then(|| "dashboard.topic_order_config_failed".into()),
            message: if result.success {
                "Order configuration completed."
            } else {
                "Order configuration did not complete; verify it before retrying."
            }
            .into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_targets_and_order_failure_remain_visible_without_raw_error_details() {
        let target = |name: &str, success| TopicBatchTargetOutcome {
            broker_name: name.into(),
            success,
            message: "password=secret path=C:/private".into(),
        };
        let result = project_batch(
            TopicBatchOperation::Update,
            "orders".into(),
            TopicTargetKind::Broker,
            vec![target("broker-a", true), target("broker-b", false)],
            Some(TopicBatchOrderConfigOutcome {
                success: false,
                message: "credential=secret".into(),
            }),
        );
        assert!(!result.success);
        assert_eq!(result.target_count, 2);
        assert!(result.targets[0].success);
        assert!(!result.targets[1].success);
        assert!(!result.order_config.unwrap().success);
        let result = project_batch(
            TopicBatchOperation::Update,
            "orders".into(),
            TopicTargetKind::Broker,
            vec![target("broker-a", true)],
            Some(TopicBatchOrderConfigOutcome {
                success: true,
                message: String::new(),
            }),
        );
        assert!(result.success);
        assert!(!serde_json::to_string(&result).unwrap().contains("secret"));
        let result = project_batch(
            TopicBatchOperation::Update,
            "orders".into(),
            TopicTargetKind::Broker,
            vec![target("broker-a", true)],
            Some(TopicBatchOrderConfigOutcome {
                success: false,
                message: "secret".into(),
            }),
        );
        assert!(!result.success, "an order-only failure must not disappear");
        assert!(!serde_json::to_string(&result).unwrap().contains("secret"));
    }
}

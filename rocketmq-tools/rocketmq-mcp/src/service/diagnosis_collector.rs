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

use serde::Serialize;
use serde_json::json;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::model::contract::observed_at;
use crate::model::diagnosis::EvidenceItem;
use crate::model::diagnosis::EvidenceKind;
use crate::model::diagnosis::EvidencePayload;
use crate::model::diagnosis::EvidenceSnapshot;
use crate::model::diagnosis::EvidenceStatus;
use crate::tools::broker_tools::DescribeBrokerOutput;
use crate::tools::consumer_tools::QueryConsumerLagOutput;
use crate::tools::diagnosis_tools::DiagnoseConsumerLagArgs;
use crate::tools::executor::ToolExecutionError;
use crate::tools::topic_tools::DescribeTopicOutput;
use crate::tools::topic_tools::QueryTopicRouteOutput;

pub(crate) const CONSUMER_LAG_EVIDENCE_VERSION_V2: &str = "rocketmq-mcp.evidence.consumer-lag.v2";

pub(crate) struct ConsumerLagEvidence {
    pub lag: Result<QueryConsumerLagOutput, ToolExecutionError>,
    pub topic: Result<DescribeTopicOutput, ToolExecutionError>,
    pub route: Result<QueryTopicRouteOutput, ToolExecutionError>,
    pub broker: Option<Result<DescribeBrokerOutput, ToolExecutionError>>,
}

impl ConsumerLagEvidence {
    pub(crate) fn snapshot(&self, args: &DiagnoseConsumerLagArgs) -> EvidenceSnapshot {
        let query_hash = query_hash(args);
        let mut items = vec![
            item(
                "consumer_lag",
                "rocketmq_get_consumer_lag",
                EvidenceKind::ConsumerLag,
                &query_hash,
                &self.lag,
            ),
            item(
                "topic_description",
                "rocketmq_describe_topic",
                EvidenceKind::TopicRoute,
                &query_hash,
                &self.topic,
            ),
            item(
                "topic_route",
                "rocketmq_get_topic_route",
                EvidenceKind::TopicRoute,
                &query_hash,
                &self.route,
            ),
        ];
        items.push(match &self.broker {
            Some(result) => item(
                "broker_summary",
                "rocketmq_describe_broker",
                EvidenceKind::BrokerSummary,
                &query_hash,
                result,
            ),
            None => missing_broker_item(&query_hash),
        });
        EvidenceSnapshot {
            evidence_version: CONSUMER_LAG_EVIDENCE_VERSION_V2.to_string(),
            query_hash,
            cluster: args.cluster.clone(),
            target: json!({ "topic": args.topic, "consumer_group": args.consumer_group }),
            observed_at: observed_at(),
            items,
        }
    }
}

fn query_hash(args: &DiagnoseConsumerLagArgs) -> String {
    let mut digest = Sha256::new();
    digest.update(args.cluster.as_bytes());
    digest.update([0]);
    digest.update(args.topic.as_bytes());
    digest.update([0]);
    digest.update(args.consumer_group.as_bytes());
    digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect()
}

fn item<T>(
    id: &str,
    source_tool: &str,
    kind: EvidenceKind,
    query_hash: &str,
    result: &Result<T, ToolExecutionError>,
) -> EvidenceItem
where
    T: Serialize,
{
    match result {
        Ok(value) => EvidenceItem {
            id: id.to_string(),
            kind,
            source_tool: source_tool.to_string(),
            observed_at: observed_at(),
            freshness_ms: 0,
            status: EvidenceStatus::Present,
            query_hash: query_hash.to_string(),
            payload: Some(payload(kind, value)),
            error_code: None,
            summary: format!("{source_tool} returned evidence."),
        },
        Err(error) => EvidenceItem {
            id: id.to_string(),
            kind,
            source_tool: source_tool.to_string(),
            observed_at: observed_at(),
            freshness_ms: 0,
            status: error_status(error),
            query_hash: query_hash.to_string(),
            payload: None,
            error_code: Some(error.code().to_string()),
            summary: error.to_string(),
        },
    }
}

fn payload<T>(kind: EvidenceKind, value: &T) -> EvidencePayload
where
    T: Serialize,
{
    let value = serde_json::to_value(value).unwrap_or(Value::Null);
    match kind {
        EvidenceKind::ConsumerLag => EvidencePayload::ConsumerLag(value),
        EvidenceKind::TopicRoute => EvidencePayload::TopicRoute(value),
        EvidenceKind::BrokerSummary => EvidencePayload::BrokerSummary(value),
    }
}

fn error_status(error: &ToolExecutionError) -> EvidenceStatus {
    match error {
        ToolExecutionError::TimedOut { .. } => EvidenceStatus::Timeout,
        ToolExecutionError::PermissionDenied(_) => EvidenceStatus::Unauthorized,
        ToolExecutionError::InvalidArguments(_) => EvidenceStatus::Invalid,
        _ => EvidenceStatus::Unavailable,
    }
}

fn missing_broker_item(query_hash: &str) -> EvidenceItem {
    EvidenceItem {
        id: "broker_summary".to_string(),
        kind: EvidenceKind::BrokerSummary,
        source_tool: "rocketmq_describe_broker".to_string(),
        observed_at: observed_at(),
        freshness_ms: 0,
        status: EvidenceStatus::Missing,
        query_hash: query_hash.to_string(),
        payload: None,
        error_code: None,
        summary: "No broker was selected because consumer lag evidence was unavailable.".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_is_replayable_and_marks_missing_evidence() {
        let args = DiagnoseConsumerLagArgs {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            consumer_group: "order-service".to_string(),
        };
        let evidence = ConsumerLagEvidence {
            lag: Err(ToolExecutionError::backend("nameserver unavailable")),
            topic: Err(ToolExecutionError::backend("topic route unavailable")),
            route: Err(ToolExecutionError::TimedOut { timeout_ms: 5_000 }),
            broker: None,
        };

        let first = evidence.snapshot(&args);
        let second = evidence.snapshot(&args);

        assert_eq!(first.query_hash, second.query_hash);
        assert_eq!(first.evidence_version, CONSUMER_LAG_EVIDENCE_VERSION_V2);
        assert_eq!(first.items[0].status, EvidenceStatus::Unavailable);
        assert_eq!(first.items[2].status, EvidenceStatus::Timeout);
        assert_eq!(first.items[3].status, EvidenceStatus::Missing);
    }
}

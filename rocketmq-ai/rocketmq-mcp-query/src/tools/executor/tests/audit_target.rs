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

//! The audit record of a call says which Topic, Consumer Group or Broker the call asked about.

use super::*;

async fn call(guard: &Guard, tool: ToolId, arguments: serde_json::Value) -> CallToolResult {
    let adapter = FakeAdapter {
        fail: false,
        partial: false,
    };
    ToolExecutor::new(adapter, guard.clone())
        .call(CallToolRequestParams::new(tool.descriptor().name).with_arguments(arguments.as_object().unwrap().clone()))
        .await
        .unwrap()
}

fn error_code(result: &CallToolResult) -> String {
    assert_eq!(result.is_error, Some(true));
    serde_json::from_str::<serde_json::Value>(&content_text(result)).unwrap()["code"]
        .as_str()
        .unwrap()
        .to_string()
}

/// The targets of the records a Guard kept, oldest first.
fn targets(guard: &Guard) -> Vec<serde_json::Value> {
    guard
        .audit_log()
        .records()
        .iter()
        .map(|record| serde_json::to_value(&record.target).unwrap())
        .collect()
}

#[tokio::test]
async fn a_tool_call_is_audited_with_the_names_it_asks_about() {
    let guard = test_guard("read_only");

    let producers = call(
        &guard,
        ToolId::ListProducerConnections,
        serde_json::json!({"cluster": "local-dev", "topic": "orders", "producer_group": "checkout"}),
    )
    .await;
    assert_eq!(producers.is_error, Some(false));
    let state = call(
        &guard,
        ToolId::GetTopicConfigState,
        serde_json::json!({"cluster": "local-dev", "topic": "orders", "broker_names": ["broker-a", "broker-b"]}),
    )
    .await;
    assert_eq!(state.is_error, Some(false));
    // A call that names nothing is recorded as before.
    let overview = call(
        &guard,
        ToolId::GetClusterOverview,
        serde_json::json!({"cluster": "local-dev"}),
    )
    .await;
    assert_eq!(overview.is_error, Some(false));
    // A call the caller may not make is recorded with what it tried to read.
    let refused = call(
        &guard,
        ToolId::GetBrokerDiagnostics,
        serde_json::json!({"cluster": "local-dev", "broker_name": "broker-a"}),
    )
    .await;
    assert_eq!(error_code(&refused), "unauthorized_scope");
    // A value that is no Topic name is not written down, whatever becomes of the call.
    call(
        &guard,
        ToolId::ListProducerConnections,
        serde_json::json!({"cluster": "local-dev", "topic": "token=secret", "producer_group": "checkout"}),
    )
    .await;

    assert_eq!(
        targets(&guard),
        [
            serde_json::json!({"topic": "orders", "producer_group": "checkout"}),
            serde_json::json!({"topic": "orders", "broker_names": ["broker-a", "broker-b"]}),
            serde_json::Value::Null,
            serde_json::json!({"broker_names": ["broker-a"]}),
            serde_json::json!({"producer_group": "checkout"}),
        ]
    );
    let records = guard.audit_log().records();
    assert_eq!(records[0].status, AuditStatus::Success);
    assert_eq!(records[3].status, AuditStatus::Failure);
    assert!(!serde_json::to_string(&records).unwrap().contains("secret"));
}

#[tokio::test]
async fn a_message_lookup_is_audited_with_its_topic_and_never_with_the_message() {
    let guard = test_guard("diagnose");

    let answered = call(
        &guard,
        ToolId::DiagnoseMessageDelivery,
        serde_json::json!({
            "cluster": "local-dev",
            "topic": "orders",
            "consumer_group": "order-service",
            "message_id": "RAW-MESSAGE-ID-A",
        }),
    )
    .await;
    assert_eq!(answered.is_error, Some(false));

    assert_eq!(
        targets(&guard),
        [serde_json::json!({"topic": "orders", "consumer_group": "order-service"})]
    );
    assert!(!serde_json::to_string(&guard.audit_log().records())
        .unwrap()
        .contains("RAW-MESSAGE-ID-A"));
}

#[test]
fn a_resource_read_is_audited_with_the_names_in_its_address() {
    let guard = test_guard("read_only");
    let context = guard.local_request_context();

    // A read the role allows, here one that fails while it is answered.
    let lag = ResourceKind::ConsumerLag {
        group: "order-service".to_string(),
        topic: "orders".to_string(),
    };
    let read = guard.begin_resource_read(&context, "local-dev", &lag).unwrap();
    assert!(read
        .finish_result(Err(ErrorData::internal_error("the source failed", None)))
        .is_err());
    // A read the caller may not make.
    let diagnostics = ResourceKind::BrokerDiagnostics("broker-a".to_string());
    assert!(guard.begin_resource_read(&context, "local-dev", &diagnostics).is_err());
    // A list names nothing.
    drop(
        guard
            .begin_resource_read(&context, "local-dev", &ResourceKind::Topics)
            .unwrap()
            .finish_result(Err(ErrorData::internal_error("the source failed", None))),
    );

    assert_eq!(
        targets(&guard),
        [
            serde_json::json!({"topic": "orders", "consumer_group": "order-service"}),
            serde_json::json!({"broker_names": ["broker-a"]}),
            serde_json::Value::Null,
        ]
    );
}

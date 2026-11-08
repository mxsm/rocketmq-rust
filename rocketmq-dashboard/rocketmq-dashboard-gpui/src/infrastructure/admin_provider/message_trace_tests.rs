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

use super::*;
use rocketmq_admin_core::core::{
    message_index::MessageTraceStep,
    message_trace::{MessageTraceObservation, MessageTraceReadResult},
};
use rocketmq_dashboard_common::{DEFAULT_TRACE_TOPIC, MessageIdentity, MessageTraceQuery, TRACE_NODE_LIMIT};

#[test]
fn trace_provider_uses_query_session_bounds_observations_and_rejects_scope_revision_or_identity_drift() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    let query = MessageTraceQuery {
        identity: MessageIdentity {
            topic: "orders".into(),
            message_id: "physical-id".into(),
        },
        trace_topic: DEFAULT_TRACE_TOPIC.into(),
    };
    let node = MessageTraceObservation {
        step: MessageTraceStep::ConsumeStart,
        timestamp: None,
        group: None,
        success: None,
        cost_time_ms: None,
        client_host: Some("private-client".into()),
        store_host: None,
        offset_message_id: None,
        retry_times: Some(2),
        transaction_state: None,
    };
    let result = MessageTraceReadResult {
        topic: "orders".into(),
        message_id: "physical-id".into(),
        producer_message_id: Some("private-producer-id".into()),
        trace_topic: DEFAULT_TRACE_TOPIC.into(),
        trace_topic_found: true,
        nodes: vec![node; TRACE_NODE_LIMIT + 1],
        limit_reached: false,
        unanswered_brokers: vec!["broker-b".into()],
    };
    *controls.trace_result.lock().unwrap() = Some(result.clone());
    runtime.block_on(async {
        provider.switch(snapshot(5, ConnectionScope::NameServer)).await.unwrap();
        let trace = provider.message_trace(5, query.clone()).await.unwrap();
        assert_eq!(trace.identity, query.identity);
        assert_eq!(trace.nodes.len(), TRACE_NODE_LIMIT);
        assert!(trace.limit_reached);
        assert!(
            trace.nodes[0].timestamp.is_none()
                && trace.nodes[0].success.is_none()
                && trace.nodes[0].cost_time_ms.is_none()
        );
        assert_eq!(trace.unanswered_brokers[0].text, "broker-b");
        let actual = controls.trace_queries.lock().unwrap()[0].clone();
        assert_eq!(actual.topic, "orders");
        assert_eq!(actual.message_id, "physical-id");
        assert_eq!(actual.trace_topic, DEFAULT_TRACE_TOPIC);
        for private in ["private-client", "private-producer-id"] {
            assert!(!format!("{trace:?}").contains(private));
        }
        assert!(provider.message_trace(4, query.clone()).await.is_err());
        assert_eq!(controls.trace_queries.lock().unwrap().len(), 1);
        let mut wrong = result;
        wrong.message_id = "different-message".into();
        *controls.trace_result.lock().unwrap() = Some(wrong);
        assert!(provider.message_trace(5, query.clone()).await.is_err());
        provider.switch(snapshot(6, ConnectionScope::Proxy)).await.unwrap();
        assert!(provider.message_trace(6, query).await.is_err());
        assert_eq!(controls.trace_queries.lock().unwrap().len(), 2);
        provider.shutdown().await;
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}

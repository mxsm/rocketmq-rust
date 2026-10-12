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

// The emergency stop through the authenticated HTTP surface. Included into the transport tests,
// whose routers, tokens and fakes it uses.

use std::sync::atomic::Ordering;

const TOPIC_TOOL: &str = crate::tools::UPSERT_TOPIC_TOOL;

fn code(response: &serde_json::Value) -> &serde_json::Value {
    &response["result"]["structuredContent"]["code"]
}

/// A complete Topic upsert, as a dry run or as an execution.
fn upsert(dry_run: bool) -> serde_json::Value {
    let mut arguments = topic_arguments("orders", vec!["broker-b".to_owned(), "broker-a".to_owned()]);
    let members = arguments.as_object_mut().unwrap();
    members.insert("dry_run".to_owned(), serde_json::json!(dry_run));
    if !dry_run {
        members.insert("confirm".to_owned(), serde_json::json!(true));
        members.insert("reason".to_owned(), serde_json::json!("approved topic rollout"));
    }
    arguments
}

#[tokio::test]
async fn a_stop_file_refuses_authorized_mutation_calls_until_it_is_removed() {
    let directory = tempfile::tempdir().unwrap();
    let stop_file = directory.path().join("stop-writes");
    let configured = stop_file.to_string_lossy().into_owned();
    let sink = Arc::new(MemoryAuditSink::new(64, 4096));
    let (router, counters) = write_router_with(
        |policy| policy.emergency_stop_file = Some(configured),
        crate::audit::AuditTrail::new(sink.clone()),
    )
    .await;
    let operator = token_with_claims(REQUIRED_WRITE_SCOPE, vec!["topic_upsert"], vec!["cluster-a"]);

    // Without the file the configuration decides alone: the dry run is planned and recorded.
    let planned = authenticated_tool_call(&router, &operator, 700, TOPIC_TOOL, upsert(true)).await;
    assert_eq!(planned["result"]["isError"], false, "{planned}");
    let invocation = planned["result"]["structuredContent"]["invocation_id"].as_u64().unwrap();
    let recorded = sink.records().await.unwrap().len();
    let opened = counters.opens.load(Ordering::SeqCst);
    assert!(recorded > 0 && opened > 0);

    tokio::fs::write(&stop_file, b"").await.unwrap();

    // The stop acts like `mutations_enabled = false`: a dry run is refused like an execution,
    // and the arguments are not read.
    let unparsed = serde_json::json!({"cluster": "cluster-a", "reason": "token=must-not-be-parsed"});
    for (id, arguments) in [(701, upsert(true)), (702, upsert(false)), (703, unparsed)] {
        let refused = authenticated_tool_call(&router, &operator, id, TOPIC_TOOL, arguments).await;
        assert_eq!(refused["result"]["isError"], true, "{refused}");
        assert_eq!(code(&refused), "mutation_disabled", "{refused}");
        assert!(!refused.to_string().contains("must-not-be-parsed"));
    }

    // Authorization still comes first, so a caller that may not mutate does not learn of the stop.
    let elsewhere = token_with_claims(REQUIRED_WRITE_SCOPE, vec!["topic_upsert"], vec!["cluster-b"]);
    let denied = authenticated_tool_call(&router, &elsewhere, 704, TOPIC_TOOL, upsert(true)).await;
    assert_eq!(code(&denied), "cluster_not_allowed");
    let no_operation = token_with_claims(REQUIRED_WRITE_SCOPE, Vec::new(), vec!["cluster-a"]);
    let denied = authenticated_tool_call(&router, &no_operation, 705, TOPIC_TOOL, upsert(true)).await;
    assert_eq!(code(&denied), "operation_not_allowed");

    // A refused call leaves no durable record and reaches no session.
    assert_eq!(sink.records().await.unwrap().len(), recorded);
    assert_eq!(counters.opens.load(Ordering::SeqCst), opened);

    // How the calls before the stop ended stays readable.
    let lookup = serde_json::json!({
        "schema_version": crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION,
        "cluster": "cluster-a",
        "invocation_id": invocation,
    });
    let found =
        authenticated_tool_call(&router, &operator, 706, crate::tools::GET_MUTATION_RESULT_TOOL, lookup).await;
    assert_eq!(found["result"]["isError"], false, "{found}");
    assert_eq!(found["result"]["structuredContent"]["state"], "finished");

    // Removing the file lifts the stop for the next call. Nothing was remembered.
    tokio::fs::remove_file(&stop_file).await.unwrap();
    let resumed = authenticated_tool_call(&router, &operator, 707, TOPIC_TOOL, upsert(true)).await;
    assert_eq!(resumed["result"]["isError"], false, "{resumed}");
    assert!(sink.records().await.unwrap().len() > recorded);
}

#[tokio::test]
async fn a_stop_file_never_enables_what_the_configuration_disables() {
    let directory = tempfile::tempdir().unwrap();
    let stop_file = directory.path().join("stop-writes");
    let configured = stop_file.to_string_lossy().into_owned();
    let sink = Arc::new(MemoryAuditSink::new(64, 4096));
    let (router, counters) = write_router_with(
        |policy| {
            policy.mutations_enabled = false;
            policy.emergency_stop_file = Some(configured);
        },
        crate::audit::AuditTrail::new(sink.clone()),
    )
    .await;
    let operator = token_with_claims(REQUIRED_WRITE_SCOPE, vec!["topic_upsert"], vec!["cluster-a"]);

    // The file is absent, and the configuration still refuses.
    let refused = authenticated_tool_call(&router, &operator, 710, TOPIC_TOOL, upsert(true)).await;
    assert_eq!(code(&refused), "mutation_disabled");
    assert!(sink.records().await.unwrap().is_empty());
    assert_eq!(counters.opens.load(Ordering::SeqCst), 0);
}

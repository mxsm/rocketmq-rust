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

// `rocketmq_get_mutation_result` through the authenticated HTTP surface. Included into the
// transport tests, whose routers, tokens and fakes it uses.

const LOOKUP_TOOL: &str = crate::tools::GET_MUTATION_RESULT_TOOL;

fn lookup_arguments(selector: serde_json::Value) -> serde_json::Value {
    let mut arguments = serde_json::json!({
        "schema_version": crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION,
        "cluster": "cluster-a",
    });
    for (name, value) in selector.as_object().unwrap() {
        arguments[name] = value.clone();
    }
    arguments
}

/// The structured answer of a successful lookup.
fn lookup_answer(response: &serde_json::Value) -> &serde_json::Value {
    assert_eq!(response["result"]["isError"], false, "{response}");
    let structured = &response["result"]["structuredContent"];
    assert_eq!(structured["schema_version"], crate::tools::MUTATION_LOOKUP_SCHEMA_VERSION);
    let text: serde_json::Value =
        serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(&text, structured);
    structured
}

/// Executes one Topic upsert as `token` and returns its response.
async fn execute_topic_upsert(router: &Router, token: &str, id: usize, request_key: &str) -> serde_json::Value {
    let mut arguments = topic_arguments("orders", vec!["broker-b".to_owned(), "broker-a".to_owned()]);
    let members = arguments.as_object_mut().unwrap();
    members.insert("dry_run".to_owned(), serde_json::json!(false));
    members.insert("confirm".to_owned(), serde_json::json!(true));
    members.insert("reason".to_owned(), serde_json::json!("approved topic rollout"));
    members.insert("request_key".to_owned(), serde_json::json!(request_key));
    let response = authenticated_tool_call(router, token, id, crate::tools::UPSERT_TOPIC_TOOL, arguments).await;
    assert_eq!(response["result"]["isError"], false, "{response}");
    response["result"]["structuredContent"].clone()
}

#[tokio::test]
async fn mutation_result_is_authorized_before_its_arguments_and_reads_only_the_callers_calls() {
    let (router, counters, sink) = write_router().await;
    let alice = token_with_subject_claims("alice", REQUIRED_WRITE_SCOPE, vec!["topic_upsert"], vec!["cluster-a"]);
    let unparsed = serde_json::json!({"cluster": "cluster-a", "request_key": "token=must-not-be-parsed"});

    // The write scope and the cluster come first; the arguments are not read before them.
    let read_only = token("rocketmq:read");
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 400,
        "method": "tools/call",
        "params": {"name": LOOKUP_TOOL, "arguments": unparsed}
    });
    let response = router
        .clone()
        .oneshot(request("/mcp", Body::from(body.to_string()), Some(&read_only)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let bytes = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
    let denied: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(denied["code"], "permission_denied");
    let elsewhere = token_with_subject_claims("alice", REQUIRED_WRITE_SCOPE, vec!["topic_upsert"], vec!["cluster-b"]);
    let denied = authenticated_tool_call(&router, &elsewhere, 401, LOOKUP_TOOL, unparsed.clone()).await;
    assert_eq!(denied["result"]["structuredContent"]["code"], "cluster_not_allowed");
    assert!(!denied.to_string().contains("must-not-be-parsed"));

    // Then the arguments: closed, and located without repeating what was sent.
    let unknown = authenticated_tool_call(
        &router,
        &alice,
        402,
        LOOKUP_TOOL,
        lookup_arguments(serde_json::json!({"invocation_id": 1, "operator": "alice"})),
    )
    .await;
    assert_eq!(unknown["result"]["isError"], true);
    assert_eq!(unknown["result"]["structuredContent"]["code"], "invalid_argument");
    assert_eq!(
        unknown["result"]["structuredContent"]["violations"],
        serde_json::json!([{"path": "/*", "constraint": "additional_property"}])
    );
    for arguments in [
        lookup_arguments(serde_json::json!({})),
        lookup_arguments(serde_json::json!({"invocation_id": 0})),
        lookup_arguments(serde_json::json!({"request_key": "short"})),
        unparsed,
    ] {
        let invalid = authenticated_tool_call(&router, &alice, 403, LOOKUP_TOOL, arguments.clone()).await;
        assert_eq!(
            invalid["result"]["structuredContent"]["code"], "invalid_argument",
            "{arguments}"
        );
        assert!(!invalid.to_string().contains("must-not-be-parsed"));
    }

    // A call that never happened is not found, and asking writes nothing.
    let missing = authenticated_tool_call(
        &router,
        &alice,
        404,
        LOOKUP_TOOL,
        lookup_arguments(serde_json::json!({"invocation_id": 1})),
    )
    .await;
    assert_eq!(lookup_answer(&missing)["state"], "not_found");
    assert!(sink.records().await.unwrap().is_empty());

    // Alice changes a Topic; the response names its invocation.
    let applied = execute_topic_upsert(&router, &alice, 405, "topic-request-0001").await;
    assert_eq!(applied["status"], "applied");
    let invocation = applied["invocation_id"].as_u64().unwrap();
    assert_eq!(invocation, sink.records().await.unwrap()[0].invocation_id.get());
    let records = sink.records().await.unwrap().len();
    let opens = counters.opens.load(std::sync::atomic::Ordering::SeqCst);

    for selector in [
        serde_json::json!({"invocation_id": invocation}),
        serde_json::json!({"request_key": "topic-request-0001"}),
        serde_json::json!({"invocation_id": invocation, "request_key": "topic-request-0001"}),
    ] {
        let found = authenticated_tool_call(&router, &alice, 406, LOOKUP_TOOL, lookup_arguments(selector.clone())).await;
        let answer = lookup_answer(&found);
        assert_eq!(answer["state"], "finished", "{selector}");
        assert_eq!(answer["invocation_id"], invocation);
        assert_eq!(answer["operation"], "topic_upsert");
        assert_eq!(answer["mode"], "execute");
        assert_eq!(answer["status"], "applied");
        assert_eq!(answer["error_code"], serde_json::Value::Null);
        assert_eq!(answer["changed"], true);
        assert_eq!(
            answer["target"],
            serde_json::json!({"topic": "orders", "broker_count": 2, "broker_names": ["broker-a", "broker-b"]})
        );
        assert_eq!(
            answer["target_results"],
            serde_json::json!({"applied": 2, "unchanged": 0, "conflict": 0, "failed": 0})
        );
        // Who asked for the change, why, and with which key stay in the durable record.
        let text = found.to_string();
        for private in ["alice", "approved topic rollout", "topic-request-0001", "operator", "reason"] {
            assert!(!text.contains(private), "the answer holds {private}");
        }
    }

    // Another caller with the same rights sees nothing of it, by identifier or by key. It needs
    // no operation of its own to ask.
    let bob = token_with_subject_claims("bob", REQUIRED_WRITE_SCOPE, Vec::new(), vec!["cluster-a"]);
    for selector in [
        serde_json::json!({"invocation_id": invocation}),
        serde_json::json!({"request_key": "topic-request-0001"}),
    ] {
        let hidden = authenticated_tool_call(&router, &bob, 407, LOOKUP_TOOL, lookup_arguments(selector)).await;
        let answer = lookup_answer(&hidden);
        assert_eq!(answer["state"], "not_found");
        assert_eq!(answer["invocation_id"], serde_json::Value::Null);
        assert_eq!(answer["target"], serde_json::Value::Null);
    }
    let listed = router
        .clone()
        .oneshot(request(
            "/mcp",
            Body::from(r#"{"jsonrpc":"2.0","id":408,"method":"tools/list","params":{}}"#),
            Some(&bob),
        ))
        .await
        .unwrap();
    let bytes = to_bytes(listed.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
    let listed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let names = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, [LOOKUP_TOOL]);

    // Every lookup left the audit trail and the cluster alone.
    assert_eq!(sink.records().await.unwrap().len(), records);
    assert_eq!(counters.opens.load(std::sync::atomic::Ordering::SeqCst), opens);
}

#[tokio::test]
async fn mutation_result_stays_readable_after_mutations_are_switched_off() {
    let (router, _, sink) = write_router().await;
    let alice = token_with_subject_claims("alice", REQUIRED_WRITE_SCOPE, vec!["topic_upsert"], vec!["cluster-a"]);
    let applied = execute_topic_upsert(&router, &alice, 420, "topic-request-0001").await;
    let invocation = applied["invocation_id"].as_u64().unwrap();

    // The server is restarted over the same audit records with mutations switched off.
    let trail = crate::audit::AuditTrail::resume(sink.clone()).await.unwrap();
    let (stopped, counters) = write_router_over(false, trail).await;
    let refused = authenticated_tool_call(
        &stopped,
        &alice,
        421,
        crate::tools::UPSERT_TOPIC_TOOL,
        topic_arguments("orders", vec!["broker-a".to_owned()]),
    )
    .await;
    assert_eq!(refused["result"]["structuredContent"]["code"], "mutation_disabled");

    // No mutation is listed any more, and the result of the earlier one can still be read.
    let listed = stopped
        .clone()
        .oneshot(request(
            "/mcp",
            Body::from(r#"{"jsonrpc":"2.0","id":422,"method":"tools/list","params":{}}"#),
            Some(&alice),
        ))
        .await
        .unwrap();
    let bytes = to_bytes(listed.into_body(), MAX_HTTP_BODY_BYTES).await.unwrap();
    let listed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let tools = listed["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], LOOKUP_TOOL);
    assert_eq!(tools[0]["annotations"]["readOnlyHint"], true);
    assert_eq!(tools[0]["annotations"]["destructiveHint"], false);

    let found = authenticated_tool_call(
        &stopped,
        &alice,
        423,
        LOOKUP_TOOL,
        lookup_arguments(serde_json::json!({"invocation_id": invocation})),
    )
    .await;
    let answer = lookup_answer(&found);
    assert_eq!(answer["state"], "finished");
    assert_eq!(answer["status"], "applied");
    assert_eq!(answer["target"]["topic"], "orders");
    assert_eq!(counters.opens.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(sink.records().await.unwrap().len(), 2);
}

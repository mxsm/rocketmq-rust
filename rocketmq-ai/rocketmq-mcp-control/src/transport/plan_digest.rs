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

// `plan_digest` and `expected_plan_digest` through the authenticated HTTP surface. Included into
// the transport tests, whose routers, tokens and fakes it uses.

use std::sync::atomic::Ordering;

const UPSERT: &str = crate::tools::UPSERT_TOPIC_TOOL;

fn writer() -> String {
    token_with_claims(REQUIRED_WRITE_SCOPE, vec!["topic_upsert"], vec!["cluster-a"])
}

/// The arguments of a dry run that replaces the Topic `orders` on two Brokers.
fn plan_arguments() -> serde_json::Value {
    topic_arguments("orders", vec!["broker-b".to_owned(), "broker-a".to_owned()])
}

/// The arguments that execute the plan of `plan`, naming `expected` when there is one.
fn execution_of(mut plan: serde_json::Value, request_key: &str, expected: Option<&str>) -> serde_json::Value {
    let members = plan.as_object_mut().unwrap();
    members.insert("dry_run".to_owned(), serde_json::json!(false));
    members.insert("confirm".to_owned(), serde_json::json!(true));
    members.insert("reason".to_owned(), serde_json::json!("approved topic rollout"));
    members.insert("request_key".to_owned(), serde_json::json!(request_key));
    if let Some(expected) = expected {
        members.insert("expected_plan_digest".to_owned(), serde_json::json!(expected));
    }
    plan
}

/// Runs a dry run and returns the digest of the plan it reports.
async fn plan_digest_of(router: &Router, token: &str, id: usize, arguments: serde_json::Value) -> String {
    let planned = authenticated_tool_call(router, token, id, UPSERT, arguments).await;
    let structured = &planned["result"]["structuredContent"];
    assert_eq!(planned["result"]["isError"], false, "{planned}");
    assert_eq!(structured["mode"], "dry_run");
    assert!(is_plan_digest(&structured["plan_digest"]), "{structured}");
    // The text block mirrors the structured response, the digest included.
    let text: serde_json::Value =
        serde_json::from_str(planned["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(&text, structured);
    structured["plan_digest"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn a_dry_run_reports_the_plan_that_an_execution_may_name() {
    let (router, counters, _) = write_router().await;
    let token = writer();
    let digest = plan_digest_of(&router, &token, 500, plan_arguments()).await;

    // An unchanged state is the same plan. How a call is made is not part of it: neither the
    // order of the Brokers, nor the reason or the key it carries.
    let mut restated = topic_arguments("orders", vec!["broker-a".to_owned(), "broker-b".to_owned()]);
    restated["reason"] = serde_json::json!("planning a topic rollout");
    restated["request_key"] = serde_json::json!("topic-plan-0001");
    assert_eq!(plan_digest_of(&router, &token, 501, restated).await, digest);

    // What is asked for is part of it, and so is the object it is asked of.
    let mut wider = plan_arguments();
    wider["write_queue_nums"] = serde_json::json!(16);
    assert_ne!(plan_digest_of(&router, &token, 502, wider).await, digest);
    let elsewhere = topic_arguments("payments", vec!["broker-b".to_owned(), "broker-a".to_owned()]);
    assert_ne!(plan_digest_of(&router, &token, 503, elsewhere).await, digest);
    assert_eq!(counters.executes.load(Ordering::SeqCst), 0);

    // The execution that names the plan writes it. Its own response reports no plan.
    let executed = authenticated_tool_call(
        &router,
        &token,
        504,
        UPSERT,
        execution_of(plan_arguments(), "topic-request-0001", Some(&digest)),
    )
    .await;
    let structured = &executed["result"]["structuredContent"];
    assert_eq!(executed["result"]["isError"], false, "{executed}");
    assert_eq!(structured["mode"], "execute");
    assert_eq!(structured["status"], "applied");
    assert_eq!(structured["plan_digest"], serde_json::Value::Null);
    assert_eq!(counters.executes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn execute_with_a_stale_plan_digest_is_a_conflict() {
    let (router, counters, sink) = write_router().await;
    let token = writer();
    let reviewed = plan_digest_of(&router, &token, 510, plan_arguments()).await;

    // Another writer creates the Topic between the plan and its execution.
    counters.topic_version.store(7, Ordering::SeqCst);
    let stale = execution_of(plan_arguments(), "topic-request-0001", Some(&reviewed));
    let refused = authenticated_tool_call(&router, &token, 511, UPSERT, stale.clone()).await;
    assert_eq!(refused["result"]["isError"], true);
    assert_eq!(
        refused["result"]["structuredContent"],
        serde_json::json!({
            "schema_version": crate::error::ERROR_SCHEMA_VERSION,
            "code": "precondition_conflict",
            "message": "mutation precondition conflict",
            "retryable": false
        })
    );
    // The state was read, nothing was written, and the session was closed.
    assert_eq!(counters.opens.load(Ordering::SeqCst), 2);
    assert_eq!(counters.executes.load(Ordering::SeqCst), 0);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 2);
    // The refusal has an audit pair of its own, and says nothing about the targets.
    let records = sink.records().await.unwrap();
    assert_eq!(records.len(), 4);
    let terminal = &records[3];
    assert_eq!(terminal.result, crate::audit::AuditResult::Conflict);
    assert_eq!(
        terminal.error_code,
        Some(crate::error::ControlErrorCode::PreconditionConflict)
    );
    assert_eq!(terminal.requested_digest, records[1].requested_digest);
    assert_eq!(terminal.before_digest, None);
    assert_eq!(terminal.changed, None);
    assert_eq!(terminal.target_results, None);
    // No record keeps the digest that the caller sent.
    let recorded = serde_json::to_string(&records).unwrap();
    assert!(!recorded.contains(&reviewed));

    // The refusal is the outcome of its call: the same key and arguments replay it.
    let replayed = authenticated_tool_call(&router, &token, 512, UPSERT, stale).await;
    assert_eq!(replayed["result"]["structuredContent"]["code"], "precondition_conflict");
    assert_eq!(counters.opens.load(Ordering::SeqCst), 2);

    // The plan read now is another one, and its execution under a key of its own writes it.
    let current = plan_digest_of(&router, &token, 513, plan_arguments()).await;
    assert_ne!(current, reviewed);
    let executed = authenticated_tool_call(
        &router,
        &token,
        514,
        UPSERT,
        execution_of(plan_arguments(), "topic-request-0002", Some(&current)),
    )
    .await;
    assert_eq!(executed["result"]["structuredContent"]["status"], "applied", "{executed}");
    assert_eq!(counters.executes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_malformed_plan_digest_is_an_invalid_argument_before_any_session() {
    let (router, counters, sink) = write_router().await;
    let token = writer();
    for (malformed, constraint) in [
        (serde_json::json!("0".repeat(63)), "min_length"),
        (serde_json::json!("0".repeat(65)), "max_length"),
        (serde_json::json!("A".repeat(64)), "pattern"),
        (serde_json::json!(7), "type"),
    ] {
        // The form is checked for a dry run as well, which otherwise ignores the argument.
        for dry_run in [true, false] {
            let mut arguments = execution_of(plan_arguments(), "topic-request-0001", None);
            arguments["dry_run"] = serde_json::json!(dry_run);
            arguments["expected_plan_digest"] = malformed.clone();
            let rejected = authenticated_tool_call(&router, &token, 520, UPSERT, arguments).await;
            let envelope = &rejected["result"]["structuredContent"];
            assert_eq!(envelope["code"], "invalid_argument", "{malformed}");
            assert_eq!(
                envelope["violations"],
                serde_json::json!([{"path": "/expected_plan_digest", "constraint": constraint}]),
                "{malformed}"
            );
            assert!(!rejected.to_string().contains(&"A".repeat(64)));
        }
    }
    assert_eq!(counters.opens.load(Ordering::SeqCst), 0);
    assert!(sink.records().await.unwrap().is_empty());

    // A well-formed digest that a dry run carries is ignored: a dry run reports, it does not compare.
    let mut arguments = plan_arguments();
    arguments["expected_plan_digest"] = serde_json::json!("0".repeat(64));
    let digest = plan_digest_of(&router, &token, 521, arguments).await;
    assert_eq!(plan_digest_of(&router, &token, 522, plan_arguments()).await, digest);
}

#[tokio::test]
async fn execute_without_a_digest_is_rejected_when_policy_requires_it() {
    let sink = Arc::new(MemoryAuditSink::new(64, 4096));
    let trail = crate::audit::AuditTrail::new(sink.clone());
    let (router, counters) = write_router_with(|policy| policy.require_plan_digest = true, trail).await;
    let token = writer();

    // A dry run names no plan: it is how a caller gets one.
    let digest = plan_digest_of(&router, &token, 530, plan_arguments()).await;
    assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
    assert_eq!(sink.records().await.unwrap().len(), 2);

    // An execution without one is an invalid call. It is located, and neither admitted nor
    // recorded, and it reaches no session.
    let mut absent = execution_of(plan_arguments(), "topic-request-0001", None);
    for _ in 0..2 {
        let refused = authenticated_tool_call(&router, &token, 531, UPSERT, absent.clone()).await;
        assert_eq!(refused["result"]["isError"], true);
        let envelope = &refused["result"]["structuredContent"];
        assert_eq!(envelope["code"], "invalid_argument");
        assert_eq!(
            envelope["violations"],
            serde_json::json!([{"path": "/expected_plan_digest", "constraint": "required"}])
        );
        // An explicit null is no plan either.
        absent["expected_plan_digest"] = serde_json::Value::Null;
    }
    assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
    assert_eq!(counters.executes.load(Ordering::SeqCst), 0);
    assert_eq!(sink.records().await.unwrap().len(), 2);

    // The other checks of an execution still come first: one that is not confirmed is told so.
    let mut unconfirmed = execution_of(plan_arguments(), "topic-request-0001", None);
    unconfirmed["confirm"] = serde_json::json!(false);
    let refused = authenticated_tool_call(&router, &token, 532, UPSERT, unconfirmed).await;
    assert_eq!(refused["result"]["structuredContent"]["code"], "confirmation_required");

    // With the plan of the dry run, the execution runs.
    let executed = authenticated_tool_call(
        &router,
        &token,
        533,
        UPSERT,
        execution_of(plan_arguments(), "topic-request-0001", Some(&digest)),
    )
    .await;
    assert_eq!(executed["result"]["structuredContent"]["status"], "applied", "{executed}");
    assert_eq!(counters.executes.load(Ordering::SeqCst), 1);

    // Where the policy does not require a plan, the same execution runs without one.
    let (lenient, lenient_counters, _) = write_router().await;
    let executed = authenticated_tool_call(
        &lenient,
        &token,
        534,
        UPSERT,
        execution_of(plan_arguments(), "topic-request-0001", None),
    )
    .await;
    assert_eq!(executed["result"]["structuredContent"]["status"], "applied", "{executed}");
    assert_eq!(lenient_counters.executes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_consumer_group_execution_compares_its_plan_before_it_writes() {
    const UPSERT_GROUP: &str = crate::tools::UPSERT_CONSUMER_GROUP_TOOL;
    let (router, counters, _) = write_router().await;
    let token = token_with_claims(REQUIRED_WRITE_SCOPE, vec!["consumer_group_upsert"], vec!["cluster-a"]);
    let plan = || consumer_group_arguments("orders_consumers", vec!["broker-b".to_owned(), "broker-a".to_owned()]);
    let digest_of = |planned: &serde_json::Value| {
        let digest = &planned["result"]["structuredContent"]["plan_digest"];
        assert!(is_plan_digest(digest), "{planned}");
        digest.as_str().unwrap().to_owned()
    };
    let reviewed = digest_of(&authenticated_tool_call(&router, &token, 540, UPSERT_GROUP, plan()).await);

    // The plan of other arguments is not the plan of this execution.
    let mut patient = plan();
    patient["retry_max_times"] = serde_json::json!(32);
    let other = digest_of(&authenticated_tool_call(&router, &token, 541, UPSERT_GROUP, patient).await);
    assert_ne!(other, reviewed);
    let refused = authenticated_tool_call(
        &router,
        &token,
        542,
        UPSERT_GROUP,
        execution_of(plan(), "group-request-0001", Some(&other)),
    )
    .await;
    assert_eq!(refused["result"]["structuredContent"]["code"], "precondition_conflict");
    assert_eq!(counters.executes.load(Ordering::SeqCst), 0);

    let executed = authenticated_tool_call(
        &router,
        &token,
        543,
        UPSERT_GROUP,
        execution_of(plan(), "group-request-0002", Some(&reviewed)),
    )
    .await;
    assert_eq!(executed["result"]["structuredContent"]["status"], "applied", "{executed}");
    assert_eq!(counters.executes.load(Ordering::SeqCst), 1);
}

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

use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Mutex as StdMutex;

use futures_util::future::join_all;

use super::*;

fn audit_context() -> AuditContext {
    AuditContext::try_new("operator@example.test", Some("approved maintenance change")).unwrap()
}

fn started_record() -> AuditRecord {
    AuditRecord {
        schema_version: AuditSchemaVersion::V2,
        sequence: 1,
        invocation_id: AuditInvocationId(1),
        timestamp_unix_millis: 1,
        event: AuditEvent::Started,
        operation: ControlOperation::TopicUpsert,
        cluster: ClusterName::try_new("cluster-a").unwrap(),
        operator: Some("operator@example.test".to_owned()),
        reason: Some("approved maintenance change".to_owned()),
        mode: AuditMode::DryRun,
        result: AuditResult::Started,
        error_code: None,
        duration_millis: None,
    }
}

#[derive(Clone, Copy)]
enum HostileSinkBehavior {
    Ok,
    InvalidArgument,
    ExecutionFailed,
    Hang,
}

struct HostileAuditSink {
    append: HostileSinkBehavior,
    read: HostileSinkBehavior,
}

impl ReliableAuditSink for HostileAuditSink {
    fn append<'a>(&'a self, _record: &'a AuditRecord) -> AuditFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            match self.append {
                HostileSinkBehavior::Ok => Ok(()),
                HostileSinkBehavior::InvalidArgument => Err(ControlError::invalid_argument()),
                HostileSinkBehavior::ExecutionFailed => Err(ControlError::execution_failed()),
                HostileSinkBehavior::Hang => std::future::pending().await,
            }
        })
    }

    fn records(&self) -> AuditFuture<'_, Result<Vec<AuditRecord>, ControlError>> {
        Box::pin(async move {
            match self.read {
                HostileSinkBehavior::Ok => Ok(Vec::new()),
                HostileSinkBehavior::InvalidArgument => Err(ControlError::invalid_argument()),
                HostileSinkBehavior::ExecutionFailed => Err(ControlError::execution_failed()),
                HostileSinkBehavior::Hang => std::future::pending().await,
            }
        })
    }
}

fn assert_audit_unavailable(error: ControlError) {
    assert_eq!(error, ControlError::audit_unavailable());
    assert_eq!(error.to_string(), "reliable audit storage is unavailable");
}

#[tokio::test(start_paused = true)]
async fn trail_normalizes_hostile_sink_errors_and_timeouts() {
    for behavior in [
        HostileSinkBehavior::InvalidArgument,
        HostileSinkBehavior::ExecutionFailed,
        HostileSinkBehavior::Hang,
    ] {
        let sink = Arc::new(HostileAuditSink {
            append: HostileSinkBehavior::Ok,
            read: behavior,
        });
        let resume_error = match AuditTrail::resume(sink.clone()).await {
            Ok(_) => panic!("hostile recovery read was accepted"),
            Err(error) => error,
        };
        assert_audit_unavailable(resume_error);
        assert_audit_unavailable(AuditTrail::new(sink).records().await.unwrap_err());
    }

    for behavior in [
        HostileSinkBehavior::InvalidArgument,
        HostileSinkBehavior::ExecutionFailed,
        HostileSinkBehavior::Hang,
    ] {
        let audit = AuditTrail::new(Arc::new(HostileAuditSink {
            append: behavior,
            read: HostileSinkBehavior::Ok,
        }));
        assert_audit_unavailable(audit.append_record(&started_record()).await.unwrap_err());
        assert_audit_unavailable(audit.records().await.unwrap_err());
    }
}

#[tokio::test]
async fn jsonl_sink_persists_queryable_ordered_bounded_records() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("control-audit.jsonl");
    let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let audit = AuditTrail::new(sink.clone());
    let cluster = ClusterName::try_new("cluster-a").unwrap();
    let invocation = audit
        .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, true)
        .await
        .unwrap();
    audit.terminal(&invocation, AuditResult::Planned, None).await.unwrap();

    let records = sink.records().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].sequence, 1);
    assert_eq!(records[1].sequence, 2);
    assert_eq!(records[0].invocation_id, records[1].invocation_id);
    let disk = tokio::fs::read_to_string(&path).await.unwrap();
    assert_eq!(disk.lines().count(), 2);
    for forbidden in [
        "Bearer",
        "access_key",
        "secret_key",
        "127.0.0.1",
        "request-1234",
        "raw backend",
    ] {
        assert!(!disk.contains(forbidden));
    }
    drop(audit);
    drop(sink);

    let resumed_sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let resumed = AuditTrail::resume(resumed_sink.clone()).await.unwrap();
    let invocation = resumed
        .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, true)
        .await
        .unwrap();
    resumed
        .terminal(
            &invocation,
            AuditResult::Conflict,
            Some(ControlErrorCode::PreconditionConflict),
        )
        .await
        .unwrap();
    let resumed_records = resumed_sink.records().await.unwrap();
    assert_eq!(resumed_records[2].sequence, 3);
    assert_eq!(resumed_records[3].sequence, 4);
    assert_eq!(resumed_records[2].invocation_id, resumed_records[3].invocation_id);
}

#[test]
fn v2_wire_shape_is_closed_and_redacts_debug_output() {
    let record = started_record();
    let value = serde_json::to_value(&record).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "schema_version": AUDIT_SCHEMA_VERSION,
            "sequence": 1,
            "invocation_id": 1,
            "timestamp_unix_millis": 1,
            "event": "started",
            "operation": "topic_upsert",
            "cluster": "cluster-a",
            "operator": "operator@example.test",
            "reason": "approved maintenance change",
            "mode": "dry_run",
            "result": "started",
            "error_code": null,
            "duration_millis": null,
        })
    );
    let debug = format!("{record:?} {:?}", audit_context());
    assert!(!debug.contains("operator@example.test"));
    assert!(!debug.contains("approved maintenance change"));
}

#[tokio::test(start_paused = true)]
async fn v2_terminal_records_use_monotonic_duration_and_exact_result_code_pairs() {
    let sink = Arc::new(MemoryAuditSink::new(16, 4096));
    let audit = AuditTrail::new(sink.clone());
    let cluster = ClusterName::try_new("cluster-a").unwrap();
    let invocation = audit
        .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, false)
        .await
        .unwrap();
    tokio::time::advance(Duration::from_millis(42)).await;
    audit
        .terminal(&invocation, AuditResult::Partial, Some(ControlErrorCode::PartialApply))
        .await
        .unwrap();
    let records = sink.records().await.unwrap();
    assert_eq!(records[1].event, AuditEvent::Failed);
    assert_eq!(records[1].result, AuditResult::Partial);
    assert_eq!(records[1].error_code, Some(ControlErrorCode::PartialApply));
    assert_eq!(records[1].duration_millis, Some(42));

    for (result, code) in [
        (AuditResult::Started, None),
        (AuditResult::Partial, Some(ControlErrorCode::ExecutionFailed)),
        (AuditResult::Conflict, Some(ControlErrorCode::ExecutionFailed)),
        (AuditResult::Failed, None),
    ] {
        assert!(validate_terminal(result, code).is_err());
    }
}

#[tokio::test]
async fn mixed_v1_v2_restart_maps_legacy_codes_without_rewriting_history() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mixed.jsonl");
    let prefix = [
        serde_json::json!({
            "schema_version": "rocketmq-mcp-control.audit.v1",
            "sequence": 1,
            "invocation_id": 1,
            "timestamp_unix_millis": 1,
            "event": "started",
            "operation": "topic_upsert",
            "cluster": "cluster-a",
            "dry_run": false,
            "error_code": null,
        }),
        serde_json::json!({
            "schema_version": "rocketmq-mcp-control.audit.v1",
            "sequence": 2,
            "invocation_id": 1,
            "timestamp_unix_millis": 2,
            "event": "failed",
            "operation": "topic_upsert",
            "cluster": "cluster-a",
            "dry_run": false,
            "error_code": "conflict",
        }),
        serde_json::json!({
            "schema_version": "rocketmq-mcp-control.audit.v1",
            "sequence": 3,
            "invocation_id": 3,
            "timestamp_unix_millis": 3,
            "event": "started",
            "operation": "consumer_group_upsert",
            "cluster": "cluster-a",
            "dry_run": false,
            "error_code": null,
        }),
        serde_json::json!({
            "schema_version": "rocketmq-mcp-control.audit.v1",
            "sequence": 4,
            "invocation_id": 3,
            "timestamp_unix_millis": 4,
            "event": "failed",
            "operation": "consumer_group_upsert",
            "cluster": "cluster-a",
            "dry_run": false,
            "error_code": "invalid_arguments",
        }),
    ]
    .into_iter()
    .map(|value| serde_json::to_string(&value).unwrap())
    .collect::<Vec<_>>()
    .join("\n")
        + "\n";
    tokio::fs::write(&path, prefix.as_bytes()).await.unwrap();

    let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let recovered = sink.records().await.unwrap();
    assert_eq!(recovered[1].error_code, Some(ControlErrorCode::PreconditionConflict));
    assert_eq!(recovered[1].result, AuditResult::Conflict);
    assert_eq!(recovered[3].error_code, Some(ControlErrorCode::InvalidArgument));
    let audit = AuditTrail::resume(sink.clone()).await.unwrap();
    let cluster = ClusterName::try_new("cluster-a").unwrap();
    let invocation = audit
        .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, false)
        .await
        .unwrap();
    audit.terminal(&invocation, AuditResult::Applied, None).await.unwrap();
    drop(audit);
    drop(sink);

    let appended = tokio::fs::read_to_string(&path).await.unwrap();
    assert!(appended.starts_with(&prefix));
    let appended_lines = appended.lines().collect::<Vec<_>>();
    assert_eq!(appended_lines.len(), 6);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(appended_lines[4]).unwrap()["schema_version"],
        AUDIT_SCHEMA_VERSION
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(appended_lines[5]).unwrap()["result"],
        "applied"
    );
    let reopened = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    assert_eq!(
        AuditTrail::resume(reopened).await.unwrap().state.lock().await.sequence,
        6
    );
}

#[tokio::test]
async fn v2_recovery_rejects_schema_drift_unsafe_evidence_and_cross_version_pairs() {
    let valid = serde_json::to_value(started_record()).unwrap();
    let mut cases = Vec::new();

    for operator in [
        "operator@example.test",
        "operator@sub.example.test",
        "operator@team.example.com",
        "first.middle.last@example.test",
        "operator@mail.example.co.uk",
        "123e4567-e89b-12d3-a456-426614174000",
        "12345678-1234-4234-8234-123456789012",
        "svc-control_01",
        "service-2026",
        "svc_1024",
        "svc_2130706433_ops",
    ] {
        let mut value = valid.clone();
        value["operator"] = serde_json::json!(operator);
        let record = serde_json::from_value::<AuditRecord>(value).unwrap();
        assert_eq!(recover_audit_state(&[record]).unwrap().sequence, 1);
    }

    for reason in [
        "CHG-1234 increase queue count",
        "ticket INC_42, increase queue count",
        "issue #42 release 1.2 approved",
        "version 2.10.3 approved",
    ] {
        let mut value = valid.clone();
        value["reason"] = serde_json::json!(reason);
        let record = serde_json::from_value::<AuditRecord>(value).unwrap();
        assert_eq!(recover_audit_state(&[record]).unwrap().sequence, 1);
    }

    let mut unknown_version = valid.clone();
    unknown_version["schema_version"] = serde_json::json!("rocketmq-mcp-control.audit.v3");
    cases.push(vec![unknown_version]);

    let mut unknown_field = valid.clone();
    unknown_field["endpoint"] = serde_json::json!("broker.internal:10911");
    cases.push(vec![unknown_field]);

    for field in ["reason", "error_code", "duration_millis"] {
        let mut missing_nullable = valid.clone();
        missing_nullable.as_object_mut().unwrap().remove(field);
        cases.push(vec![missing_nullable]);
    }

    for operator in [
        "",
        " operator",
        "operator name",
        "https://identity.invalid/operator",
        "token=top-secret",
        "token",
        "svc-secret",
        "Bearer abc.def.ghi",
        "a.b._",
        "a.b.",
        "a.b._@example.test",
        "eyJhbGciOiJSUzI1NiJ9.e30.x@example.test",
        "eyJhbGciOiJub25lIn0.e30.x@example.test",
        "eyJhbGciOiJSUzk5OSJ9.e30.x@example.test",
        "eyJ0eXAiOiJKV1QifQ.e30.x@example.test",
        "eyJhbGciOm51bGx9.e30.x@example.test",
        "10.0.0.1:10911",
        "127.1",
        "127.0.1",
        "127.000.000.001",
        "2130706433",
        "0x7f000001",
        "017700000001",
        "0x7f.0.0.1",
        "0177.0.0.1",
        "svc_10.0.0.1_ops",
        "svc_127.1_ops",
        "svc_0x7f000001_ops",
        "svc_017700000001_ops",
        "10.0.0.1@example.test",
        "2130706433@example.test",
        "svc_127.1@example.test",
        "operator@10.0.0.1.",
        "operator@127.0x1",
        "operator@127.0.0x1",
        "operator@0X7F.0X1",
        "operator@broker.internal",
        "operator@broker.internal.",
        "operator@example.123",
        "operator%25admin",
        "operator\u{202e}admin",
        "operator\u{2028}admin",
        "operator：admin",
    ] {
        let mut invalid_operator = valid.clone();
        invalid_operator["operator"] = serde_json::json!(operator);
        cases.push(vec![invalid_operator]);
    }

    for reason in [
        "token=top-secret",
        "token%3dtop-secret",
        "token%25253dtop-secret",
        "\"token\" = top-secret",
        "[secret_key]: top-secret",
        "Bearer abc.def.ghi",
        "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJvcGVyYXRvciJ9.signature-value",
        "compact a.b._ material",
        "unsigned a.b. material",
        "token=a.b._",
        "https://control.invalid/change",
        "//control.invalid/change",
        "custom:opaque-location",
        "broker.internal:10911",
        "broker.internal.",
        "10.0.0.1",
        "127.1",
        "127.0.1",
        "127.000.000.001",
        "2130706433",
        "0x7f000001",
        "017700000001",
        "0x7f.0.0.1",
        "0177.0.0.1",
        "[fe80::1%eth0]:10911",
        "endpoint=broker.internal:10911",
        "endpoint='10.0.0.1:10911'",
        "endpoint=[fe80::1%eth0]:10911",
        "target=[broker.internal:10911]",
        "user@broker.internal:10911",
        "ops@10.0.0.1",
        "host=(broker.internal.)",
        "target=/broker.internal/",
        "target=\\broker.internal\\",
        "|broker.internal|",
        ":broker.internal:",
        "-broker.internal-",
        "[broker.internal]/",
        "owner@broker.internal",
        "http:broker.internal",
        "{10.0.0.1}",
        "(a.b._)",
        "route,broker.internal,now",
        "route 10.0.0.1,next",
        "route#broker.internal#now",
        "route_10.0.0.1_now",
        "route..10.0.0.1..now",
        "route..broker.internal..now",
        "note..a.b.c..now",
        "route,127.1,now",
        "route_127.000.000.001_now",
        "route#0x7f000001#now",
        "route 0177.0.0.1 now",
        "approved fullwidth colon：secret",
        "approved bidi \u{202e} text",
        "approved format \u{200b} text",
        "approved separator \u{2028} text",
    ] {
        let mut unsafe_reason = valid.clone();
        unsafe_reason["reason"] = serde_json::json!(reason);
        cases.push(vec![unsafe_reason]);
    }

    let mut legacy_alias = valid.clone();
    legacy_alias["sequence"] = serde_json::json!(2);
    legacy_alias["event"] = serde_json::json!("failed");
    legacy_alias["mode"] = serde_json::json!("execute");
    legacy_alias["result"] = serde_json::json!("conflict");
    legacy_alias["error_code"] = serde_json::json!("conflict");
    legacy_alias["duration_millis"] = serde_json::json!(1);
    cases.push(vec![valid.clone(), legacy_alias]);

    let legacy_started = serde_json::json!({
        "schema_version": "rocketmq-mcp-control.audit.v1",
        "sequence": 1,
        "invocation_id": 1,
        "timestamp_unix_millis": 1,
        "event": "started",
        "operation": "topic_upsert",
        "cluster": "cluster-a",
        "dry_run": false,
        "error_code": null,
    });
    let mut v2_terminal = valid;
    v2_terminal["sequence"] = serde_json::json!(2);
    v2_terminal["event"] = serde_json::json!("completed");
    v2_terminal["mode"] = serde_json::json!("execute");
    v2_terminal["result"] = serde_json::json!("applied");
    v2_terminal["duration_millis"] = serde_json::json!(1);
    cases.push(vec![legacy_started, v2_terminal]);

    for (index, records) in cases.into_iter().enumerate() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(format!("invalid-{index}.jsonl"));
        let contents = records
            .into_iter()
            .map(|value| serde_json::to_string(&value).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        tokio::fs::write(&path, contents).await.unwrap();
        let error = match JsonlAuditSink::open(&path, 16, 4096).await {
            Ok(_) => panic!("unsafe v2 recovery case {index} was accepted"),
            Err(error) => error,
        };
        assert_eq!(error, ControlError::audit_unavailable());
        assert_eq!(error.to_string(), "reliable audit storage is unavailable");
    }
}

#[tokio::test]
async fn bounded_sinks_fail_instead_of_dropping_records() {
    let sink = MemoryAuditSink::new(1, 4096);
    let record = started_record();
    sink.append(&record).await.unwrap();
    assert_eq!(
        sink.append(&record).await.unwrap_err().code(),
        ControlErrorCode::AuditUnavailable
    );
}

#[tokio::test]
async fn sinks_reject_new_v1_records() {
    let mut record = started_record();
    record.schema_version = AuditSchemaVersion::V1;
    record.operator = None;
    record.reason = None;
    let memory = MemoryAuditSink::new(2, 4096);
    assert_eq!(
        memory.append(&record).await.unwrap_err().code(),
        ControlErrorCode::AuditUnavailable
    );

    let directory = tempfile::tempdir().unwrap();
    let sink = JsonlAuditSink::open(directory.path().join("audit.jsonl"), 2, 4096)
        .await
        .unwrap();
    assert_eq!(
        sink.append(&record).await.unwrap_err().code(),
        ControlErrorCode::AuditUnavailable
    );
}

#[tokio::test]
async fn concurrent_invocations_keep_global_order_and_stable_links() {
    let sink = Arc::new(MemoryAuditSink::new(64, 4096));
    let audit = AuditTrail::new(sink.clone());
    let cluster = ClusterName::try_new("cluster-a").unwrap();
    join_all((0..16).map(|_| {
        let audit = audit.clone();
        let cluster = cluster.clone();
        async move {
            let invocation = audit
                .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, true)
                .await
                .unwrap();
            audit.terminal(&invocation, AuditResult::Planned, None).await.unwrap();
        }
    }))
    .await;
    let records = sink.records().await.unwrap();
    assert_eq!(records.len(), 32);
    assert!(records.windows(2).all(|pair| pair[0].sequence < pair[1].sequence));
    for invocation_id in records
        .iter()
        .map(|record| record.invocation_id)
        .collect::<std::collections::BTreeSet<_>>()
    {
        let linked = records
            .iter()
            .filter(|record| record.invocation_id == invocation_id)
            .collect::<Vec<_>>();
        assert_eq!(linked.len(), 2);
        assert_eq!(linked[0].event, AuditEvent::Started);
        assert_eq!(linked[1].event, AuditEvent::Completed);
    }
}

#[tokio::test]
async fn terminal_state_rejects_duplicate_unknown_and_cross_trail_tokens() {
    let cluster = ClusterName::try_new("cluster-a").unwrap();
    let sink = Arc::new(MemoryAuditSink::new(32, 4096));
    let audit = AuditTrail::new(sink.clone());

    let sequential = audit
        .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, true)
        .await
        .unwrap();
    audit.terminal(&sequential, AuditResult::Planned, None).await.unwrap();
    assert!(audit.terminal(&sequential, AuditResult::Planned, None).await.is_err());

    let concurrent = audit
        .start(&audit_context(), ControlOperation::ConsumerGroupUpsert, &cluster, true)
        .await
        .unwrap();
    let (first, second) = tokio::join!(
        audit.terminal(&concurrent, AuditResult::Planned, None),
        audit.terminal(
            &concurrent,
            AuditResult::Conflict,
            Some(ControlErrorCode::PreconditionConflict)
        )
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);

    let other_sink = Arc::new(MemoryAuditSink::new(8, 4096));
    let other = AuditTrail::new(other_sink);
    let other_invocation = other
        .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, true)
        .await
        .unwrap();
    assert!(other.terminal(&concurrent, AuditResult::Planned, None).await.is_err());
    other
        .terminal(&other_invocation, AuditResult::Planned, None)
        .await
        .unwrap();

    let unknown = AuditInvocation {
        id: AuditInvocationId(u64::MAX),
        operation: ControlOperation::TopicUpsert,
        cluster,
        context: audit_context(),
        mode: AuditMode::DryRun,
        started_at: tokio::time::Instant::now(),
        trail_identity: audit.identity.clone(),
    };
    assert!(audit.terminal(&unknown, AuditResult::Planned, None).await.is_err());
    assert_eq!(sink.records().await.unwrap().len(), 4);
}

#[tokio::test]
async fn restart_preserves_dangling_start_and_allocates_a_new_invocation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dangling.jsonl");
    let cluster = ClusterName::try_new("cluster-a").unwrap();
    let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let audit = AuditTrail::new(sink.clone());
    let completed = audit
        .start(&audit_context(), ControlOperation::ConsumerGroupUpsert, &cluster, true)
        .await
        .unwrap();
    audit.terminal(&completed, AuditResult::Planned, None).await.unwrap();
    let dangling = audit
        .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, true)
        .await
        .unwrap();
    drop(audit);
    drop(sink);

    let resumed_sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let resumed = AuditTrail::resume(resumed_sink.clone()).await.unwrap();
    {
        let recovered = resumed.state.lock().await;
        assert!(recovered.invocations[&completed.id()].terminal);
        assert!(!recovered.invocations[&dangling.id()].terminal);
    }
    let next = resumed
        .start(&audit_context(), ControlOperation::ConsumerGroupUpsert, &cluster, true)
        .await
        .unwrap();
    resumed.terminal(&next, AuditResult::Planned, None).await.unwrap();
    assert!(next.id() > dangling.id());
    let records = resumed_sink.records().await.unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| record.invocation_id == dangling.id())
            .count(),
        1
    );
}

#[tokio::test]
async fn metadata_cap_tail_and_corruption_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let limit = audit_file_limit(16, 512).unwrap();
    assert_eq!(limit, 16 * 513);

    let sparse = directory.path().join("sparse.jsonl");
    let file = tokio::fs::File::create(&sparse).await.unwrap();
    file.set_len(limit + 1).await.unwrap();
    drop(file);
    assert!(JsonlAuditSink::open(&sparse, 16, 512).await.is_err());

    let record = started_record();
    let tail = directory.path().join("tail.jsonl");
    let encoded = serde_json::to_vec(&record).unwrap();
    tokio::fs::write(&tail, &encoded).await.unwrap();
    assert!(JsonlAuditSink::open(&tail, 16, 4096).await.is_err());

    let exact = directory.path().join("exact.jsonl");
    let mut exact_line = encoded.clone();
    exact_line.push(b'\n');
    tokio::fs::write(&exact, exact_line).await.unwrap();
    assert!(JsonlAuditSink::open(&exact, 16, encoded.len()).await.is_ok());

    let plus_one = directory.path().join("plus-one.jsonl");
    let mut oversized_line = encoded.clone();
    oversized_line.extend_from_slice(b" \n");
    tokio::fs::write(&plus_one, oversized_line).await.unwrap();
    assert!(JsonlAuditSink::open(&plus_one, 16, encoded.len()).await.is_err());

    let corrupt = directory.path().join("corrupt.jsonl");
    tokio::fs::write(&corrupt, b"{not-json}\n").await.unwrap();
    assert!(JsonlAuditSink::open(&corrupt, 16, 4096).await.is_err());
}

#[tokio::test]
async fn file_and_query_budgets_clamp_and_overflow_fail_closed() {
    assert_eq!(audit_file_limit(65_536, 16_384).unwrap(), MAX_AUDIT_FILE_BYTES);
    assert!(audit_file_limit(usize::MAX, 1).is_err());
    assert!(audit_file_limit(1, usize::MAX).is_err());

    let directory = tempfile::tempdir().unwrap();
    let oversized = directory.path().join("oversized.jsonl");
    let file = tokio::fs::File::create(&oversized).await.unwrap();
    file.set_len(MAX_AUDIT_FILE_BYTES + 1).await.unwrap();
    drop(file);
    assert!(JsonlAuditSink::open(&oversized, 65_536, 16_384).await.is_err());

    let record = started_record();
    let encoded_len = u64::try_from(encode_record(&record, 4096).unwrap().len()).unwrap();
    let sink = MemoryAuditSink {
        state: Mutex::new(MemoryAuditState {
            records: Vec::new(),
            bytes_used: 0,
        }),
        capacity: 2,
        max_record_bytes: 4096,
        max_file_bytes: Some(encoded_len),
        reject_writes: false,
    };
    sink.append(&record).await.unwrap();
    assert!(sink.append(&record).await.is_err());
    let state = sink.state.lock().await;
    assert_eq!(state.records.len(), 1);
    assert!(state.bytes_used <= sink.max_file_bytes.unwrap());
}

#[derive(Clone, Copy)]
enum FailureStage {
    Append,
    Flush,
    Sync,
}

struct StageFailWriter {
    stage: FailureStage,
    append_calls: AtomicUsize,
    flush_calls: AtomicUsize,
    sync_calls: AtomicUsize,
}

struct SwitchableHangWriter {
    stage: AtomicU8,
    buffer: StdMutex<Vec<u8>>,
    entered: AtomicUsize,
}

impl SwitchableHangWriter {
    fn new() -> Self {
        Self {
            stage: AtomicU8::new(0),
            buffer: StdMutex::new(Vec::new()),
            entered: AtomicUsize::new(0),
        }
    }

    fn hang_at(&self, stage: FailureStage) {
        self.stage.store(
            match stage {
                FailureStage::Append => 1,
                FailureStage::Flush => 2,
                FailureStage::Sync => 3,
            },
            AtomicOrdering::SeqCst,
        );
    }

    fn stage(&self) -> u8 {
        self.stage.load(AtomicOrdering::SeqCst)
    }
}

impl DurableAuditWriter for SwitchableHangWriter {
    fn append<'a>(&'a self, encoded: &'a [u8]) -> AuditFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            if self.stage() == 1 {
                let prefix = encoded.len().min(8);
                self.buffer.lock().unwrap().extend_from_slice(&encoded[..prefix]);
                self.entered.fetch_add(1, AtomicOrdering::SeqCst);
                std::future::pending().await
            } else {
                self.buffer.lock().unwrap().extend_from_slice(encoded);
                Ok(())
            }
        })
    }

    fn flush(&self) -> AuditFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            if self.stage() == 2 {
                self.entered.fetch_add(1, AtomicOrdering::SeqCst);
                std::future::pending().await
            } else {
                Ok(())
            }
        })
    }

    fn sync(&self) -> AuditFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            if self.stage() == 3 {
                self.entered.fetch_add(1, AtomicOrdering::SeqCst);
                std::future::pending().await
            } else {
                Ok(())
            }
        })
    }
}

impl DurableAuditWriter for StageFailWriter {
    fn append<'a>(&'a self, _encoded: &'a [u8]) -> AuditFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            self.append_calls.fetch_add(1, AtomicOrdering::SeqCst);
            if matches!(self.stage, FailureStage::Append) {
                Err(ControlError::audit_unavailable())
            } else {
                Ok(())
            }
        })
    }

    fn flush(&self) -> AuditFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            self.flush_calls.fetch_add(1, AtomicOrdering::SeqCst);
            if matches!(self.stage, FailureStage::Flush) {
                Err(ControlError::audit_unavailable())
            } else {
                Ok(())
            }
        })
    }

    fn sync(&self) -> AuditFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            self.sync_calls.fetch_add(1, AtomicOrdering::SeqCst);
            if matches!(self.stage, FailureStage::Sync) {
                Err(ControlError::audit_unavailable())
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn append_flush_and_sync_failures_poison_queries() {
    let cluster = ClusterName::try_new("cluster-a").unwrap();
    for stage in [FailureStage::Append, FailureStage::Flush, FailureStage::Sync] {
        let writer = Arc::new(StageFailWriter {
            stage,
            append_calls: AtomicUsize::new(0),
            flush_calls: AtomicUsize::new(0),
            sync_calls: AtomicUsize::new(0),
        });
        let sink = Arc::new(JsonlAuditSink::with_writer(writer.clone(), 16, 4096).unwrap());
        let audit = AuditTrail::new(sink.clone());
        assert!(audit
            .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, true)
            .await
            .is_err());
        assert_eq!(writer.append_calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            writer.flush_calls.load(AtomicOrdering::SeqCst),
            usize::from(!matches!(stage, FailureStage::Append))
        );
        assert_eq!(
            writer.sync_calls.load(AtomicOrdering::SeqCst),
            usize::from(matches!(stage, FailureStage::Sync))
        );
        assert!(sink.records().await.is_err());
    }
}

#[tokio::test(start_paused = true)]
async fn hanging_terminal_transactions_poison_and_leave_no_recoverable_partial_record() {
    let cluster = ClusterName::try_new("cluster-a").unwrap();
    for stage in [FailureStage::Append, FailureStage::Flush, FailureStage::Sync] {
        let writer = Arc::new(SwitchableHangWriter::new());
        let sink = Arc::new(JsonlAuditSink::with_writer(writer.clone(), 16, 4096).unwrap());
        let audit = AuditTrail::new(sink.clone());
        let invocation = audit
            .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, true)
            .await
            .unwrap();
        writer.hang_at(stage);
        assert_eq!(
            audit
                .terminal(&invocation, AuditResult::Planned, None)
                .await
                .unwrap_err()
                .code(),
            ControlErrorCode::AuditUnavailable
        );
        assert!(!audit.state.lock().await.invocations[&invocation.id()].terminal);
        assert!(audit.records().await.is_err());
        assert!(audit
            .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, true)
            .await
            .is_err());

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("partial.jsonl");
        let bytes = writer.buffer.lock().unwrap().clone();
        tokio::fs::write(&path, bytes).await.unwrap();
        assert!(JsonlAuditSink::open(&path, 16, 4096).await.is_err());
    }
}

#[tokio::test]
async fn dropping_a_hanging_audit_caller_permanently_poisoned_the_transaction() {
    let cluster = ClusterName::try_new("cluster-a").unwrap();
    let writer = Arc::new(SwitchableHangWriter::new());
    let sink = Arc::new(JsonlAuditSink::with_writer(writer.clone(), 16, 4096).unwrap());
    let audit = AuditTrail::new(sink);
    let invocation = audit
        .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, true)
        .await
        .unwrap();
    writer.hang_at(FailureStage::Append);
    let task = tokio::spawn({
        let audit = audit.clone();
        async move { audit.terminal(&invocation, AuditResult::Planned, None).await }
    });
    while writer.entered.load(AtomicOrdering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(audit.records().await.is_err());
    assert!(audit
        .start(&audit_context(), ControlOperation::TopicUpsert, &cluster, true)
        .await
        .is_err());
}

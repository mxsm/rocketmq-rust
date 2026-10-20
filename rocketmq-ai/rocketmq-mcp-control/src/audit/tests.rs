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

use super::jsonl::sealed_segment_path;
use super::jsonl::DurableAuditWriter;
use super::jsonl::MAX_AUDIT_FILE_BYTES;
use super::*;

fn audit_context() -> AuditContext {
    AuditContext::try_new("operator@example.test", Some("approved maintenance change")).unwrap()
}

fn started_record() -> AuditRecord {
    let subject = AuditSubject::sample(ControlOperation::TopicUpsert);
    AuditRecord {
        schema_version: AuditSchemaVersion::V3,
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
        target: Some(subject.target),
        requested_digest: Some(subject.requested_digest),
        request_key_digest: None,
        before_digest: None,
        changed: None,
        target_results: None,
    }
}

/// The same record as version 2 wrote it, which knows nothing about the object of the mutation.
fn v2_started_record() -> AuditRecord {
    AuditRecord {
        schema_version: AuditSchemaVersion::V2,
        target: None,
        requested_digest: None,
        ..started_record()
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
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &cluster,
            true,
            &AuditSubject::sample(ControlOperation::TopicUpsert),
        )
        .await
        .unwrap();
    audit
        .terminal(&invocation, AuditResult::Planned, None, &AuditOutcome::default())
        .await
        .unwrap();

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
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &cluster,
            true,
            &AuditSubject::sample(ControlOperation::TopicUpsert),
        )
        .await
        .unwrap();
    resumed
        .terminal(
            &invocation,
            AuditResult::Conflict,
            Some(ControlErrorCode::PreconditionConflict),
            &AuditOutcome::default(),
        )
        .await
        .unwrap();
    let resumed_records = resumed_sink.records().await.unwrap();
    assert_eq!(resumed_records[2].sequence, 3);
    assert_eq!(resumed_records[3].sequence, 4);
    assert_eq!(resumed_records[2].invocation_id, resumed_records[3].invocation_id);
}

#[test]
fn v3_wire_shape_is_closed_and_redacts_debug_output() {
    let record = started_record();
    let value = serde_json::to_value(&record).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "schema_version": "rocketmq-mcp-control.audit.v3",
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
            "target": {
                "topic": "orders",
                "brokers": {
                    "count": 1,
                    "digest": sha256_hex(br#"["broker-a"]"#),
                    "names": ["broker-a"],
                },
            },
            "requested_digest": sha256_hex(b"requested state"),
            "request_key_digest": null,
            "before_digest": null,
            "changed": null,
            "target_results": null,
        })
    );
    assert_eq!(serde_json::from_value::<AuditRecord>(value).unwrap(), record);
    let debug = format!("{record:?} {:?}", audit_context());
    for durable_only in [
        "operator@example.test",
        "approved maintenance change",
        "orders",
        "broker-a",
    ] {
        assert!(!debug.contains(durable_only));
    }

    // Version-2 records keep their own closed shape, which knows no target.
    let legacy = v2_started_record();
    let legacy_value = serde_json::to_value(&legacy).unwrap();
    assert_eq!(legacy_value["schema_version"], "rocketmq-mcp-control.audit.v2");
    assert_eq!(legacy_value.as_object().unwrap().len(), 13);
    assert_eq!(serde_json::from_value::<AuditRecord>(legacy_value).unwrap(), legacy);
}

#[tokio::test(start_paused = true)]
async fn terminal_records_use_monotonic_duration_and_exact_result_code_pairs() {
    let sink = Arc::new(MemoryAuditSink::new(16, 4096));
    let audit = AuditTrail::new(sink.clone());
    let cluster = ClusterName::try_new("cluster-a").unwrap();
    let invocation = audit
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &cluster,
            false,
            &AuditSubject::sample(ControlOperation::TopicUpsert),
        )
        .await
        .unwrap();
    tokio::time::advance(Duration::from_millis(42)).await;
    audit
        .terminal(
            &invocation,
            AuditResult::Partial,
            Some(ControlErrorCode::PartialApply),
            &AuditOutcome::default(),
        )
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
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &cluster,
            false,
            &AuditSubject::sample(ControlOperation::TopicUpsert),
        )
        .await
        .unwrap();
    audit
        .terminal(&invocation, AuditResult::Applied, None, &AuditOutcome::default())
        .await
        .unwrap();
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
async fn recovery_rejects_schema_drift_unsafe_evidence_and_cross_version_pairs() {
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
    unknown_version["schema_version"] = serde_json::json!("rocketmq-mcp-control.audit.v4");
    cases.push(vec![unknown_version]);

    // A version-2 record has no place for the object of the mutation.
    let mut v2_with_target = valid.clone();
    v2_with_target["schema_version"] = serde_json::json!("rocketmq-mcp-control.audit.v2");
    cases.push(vec![v2_with_target]);

    let mut unknown_field = valid.clone();
    unknown_field["endpoint"] = serde_json::json!("broker.internal:10911");
    cases.push(vec![unknown_field]);

    for field in [
        "reason",
        "error_code",
        "duration_millis",
        "target",
        "requested_digest",
        "request_key_digest",
        "before_digest",
        "changed",
        "target_results",
    ] {
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
            Ok(_) => panic!("unsafe recovery case {index} was accepted"),
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
async fn sinks_reject_new_v1_and_v2_records() {
    let mut v1 = v2_started_record();
    v1.schema_version = AuditSchemaVersion::V1;
    v1.operator = None;
    v1.reason = None;
    let directory = tempfile::tempdir().unwrap();
    for (index, record) in [v1, v2_started_record()].into_iter().enumerate() {
        let memory = MemoryAuditSink::new(2, 4096);
        assert_eq!(
            memory.append(&record).await.unwrap_err().code(),
            ControlErrorCode::AuditUnavailable
        );
        let sink = JsonlAuditSink::open(directory.path().join(format!("audit-{index}.jsonl")), 2, 4096)
            .await
            .unwrap();
        assert_eq!(
            sink.append(&record).await.unwrap_err().code(),
            ControlErrorCode::AuditUnavailable
        );
    }
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
                .start(
                    &audit_context(),
                    ControlOperation::TopicUpsert,
                    &cluster,
                    true,
                    &AuditSubject::sample(ControlOperation::TopicUpsert),
                )
                .await
                .unwrap();
            audit
                .terminal(&invocation, AuditResult::Planned, None, &AuditOutcome::default())
                .await
                .unwrap();
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
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &cluster,
            true,
            &AuditSubject::sample(ControlOperation::TopicUpsert),
        )
        .await
        .unwrap();
    audit
        .terminal(&sequential, AuditResult::Planned, None, &AuditOutcome::default())
        .await
        .unwrap();
    assert!(audit
        .terminal(&sequential, AuditResult::Planned, None, &AuditOutcome::default())
        .await
        .is_err());

    let concurrent = audit
        .start(
            &audit_context(),
            ControlOperation::ConsumerGroupUpsert,
            &cluster,
            true,
            &AuditSubject::sample(ControlOperation::ConsumerGroupUpsert),
        )
        .await
        .unwrap();
    let outcome = AuditOutcome::default();
    let (first, second) = tokio::join!(
        audit.terminal(&concurrent, AuditResult::Planned, None, &outcome),
        audit.terminal(
            &concurrent,
            AuditResult::Conflict,
            Some(ControlErrorCode::PreconditionConflict),
            &outcome
        )
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);

    let other_sink = Arc::new(MemoryAuditSink::new(8, 4096));
    let other = AuditTrail::new(other_sink);
    let other_invocation = other
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &cluster,
            true,
            &AuditSubject::sample(ControlOperation::TopicUpsert),
        )
        .await
        .unwrap();
    assert!(other
        .terminal(&concurrent, AuditResult::Planned, None, &AuditOutcome::default())
        .await
        .is_err());
    other
        .terminal(&other_invocation, AuditResult::Planned, None, &AuditOutcome::default())
        .await
        .unwrap();

    let unknown = AuditInvocation {
        id: AuditInvocationId(u64::MAX),
        operation: ControlOperation::TopicUpsert,
        cluster,
        context: audit_context(),
        subject: AuditSubject::sample(ControlOperation::TopicUpsert),
        mode: AuditMode::DryRun,
        started_at: tokio::time::Instant::now(),
        trail_identity: audit.identity.clone(),
    };
    assert!(audit
        .terminal(&unknown, AuditResult::Planned, None, &AuditOutcome::default())
        .await
        .is_err());
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
        .start(
            &audit_context(),
            ControlOperation::ConsumerGroupUpsert,
            &cluster,
            true,
            &AuditSubject::sample(ControlOperation::ConsumerGroupUpsert),
        )
        .await
        .unwrap();
    audit
        .terminal(&completed, AuditResult::Planned, None, &AuditOutcome::default())
        .await
        .unwrap();
    let dangling = audit
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &cluster,
            true,
            &AuditSubject::sample(ControlOperation::TopicUpsert),
        )
        .await
        .unwrap();
    drop(audit);
    drop(sink);

    let resumed_sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let resumed = AuditTrail::resume(resumed_sink.clone()).await.unwrap();
    // The dangling invocation stays on record without a terminal record, and nothing can finish
    // it now: its token belonged to the trail of the process that is gone.
    assert!(resumed.state.lock().await.invocations.is_empty());
    assert!(completed.id() < dangling.id());
    assert!(resumed
        .terminal(&dangling, AuditResult::Planned, None, &AuditOutcome::default())
        .await
        .is_err());
    let next = resumed
        .start(
            &audit_context(),
            ControlOperation::ConsumerGroupUpsert,
            &cluster,
            true,
            &AuditSubject::sample(ControlOperation::ConsumerGroupUpsert),
        )
        .await
        .unwrap();
    resumed
        .terminal(&next, AuditResult::Planned, None, &AuditOutcome::default())
        .await
        .unwrap();
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
            .start(
                &audit_context(),
                ControlOperation::TopicUpsert,
                &cluster,
                true,
                &AuditSubject::sample(ControlOperation::TopicUpsert)
            )
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
            .start(
                &audit_context(),
                ControlOperation::TopicUpsert,
                &cluster,
                true,
                &AuditSubject::sample(ControlOperation::TopicUpsert),
            )
            .await
            .unwrap();
        writer.hang_at(stage);
        assert_eq!(
            audit
                .terminal(&invocation, AuditResult::Planned, None, &AuditOutcome::default())
                .await
                .unwrap_err()
                .code(),
            ControlErrorCode::AuditUnavailable
        );
        assert!(!audit.state.lock().await.invocations[&invocation.id()].terminal);
        assert!(audit.records().await.is_err());
        assert!(audit
            .start(
                &audit_context(),
                ControlOperation::TopicUpsert,
                &cluster,
                true,
                &AuditSubject::sample(ControlOperation::TopicUpsert)
            )
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
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &cluster,
            true,
            &AuditSubject::sample(ControlOperation::TopicUpsert),
        )
        .await
        .unwrap();
    writer.hang_at(FailureStage::Append);
    let task = tokio::spawn({
        let audit = audit.clone();
        async move {
            audit
                .terminal(&invocation, AuditResult::Planned, None, &AuditOutcome::default())
                .await
        }
    });
    while writer.entered.load(AtomicOrdering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(audit.records().await.is_err());
    assert!(audit
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &cluster,
            true,
            &AuditSubject::sample(ControlOperation::TopicUpsert)
        )
        .await
        .is_err());
}

fn test_cluster() -> ClusterName {
    ClusterName::try_new("cluster-a").unwrap()
}

async fn start_topic(audit: &AuditTrail, dry_run: bool) -> AuditInvocation {
    audit
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &test_cluster(),
            dry_run,
            &AuditSubject::sample(ControlOperation::TopicUpsert),
        )
        .await
        .unwrap()
}

async fn finish_planned(audit: &AuditTrail, invocation: &AuditInvocation) {
    audit
        .terminal(invocation, AuditResult::Planned, None, &AuditOutcome::default())
        .await
        .unwrap();
}

/// Reads one segment file as JSON lines.
async fn read_lines(path: &std::path::Path) -> Vec<serde_json::Value> {
    tokio::fs::read_to_string(path)
        .await
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn is_segment_header(line: &serde_json::Value) -> bool {
    line["schema_version"] == "rocketmq-mcp-control.audit-segment.v1"
}

async fn write_lines(path: &std::path::Path, lines: &[serde_json::Value]) {
    let mut contents = String::new();
    for line in lines {
        contents.push_str(&serde_json::to_string(line).unwrap());
        contents.push('\n');
    }
    tokio::fs::write(path, contents).await.unwrap();
}

#[tokio::test]
async fn v3_records_carry_target_and_digests() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("v3.jsonl");
    let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let audit = AuditTrail::new(sink.clone());
    let subject = AuditSubject::try_new(
        ControlOperation::TopicUpsert,
        AuditTarget {
            topic: Some("orders".to_owned()),
            brokers: Some(AuditBrokerSet::from_names(&[
                "broker-b".to_owned(),
                "broker-a".to_owned(),
            ])),
            ..AuditTarget::default()
        },
        sha256_hex(b"requested"),
        Some(sha256_hex(b"change-0001")),
    )
    .unwrap();
    let invocation = audit
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &test_cluster(),
            false,
            &subject,
        )
        .await
        .unwrap();
    let outcome = AuditOutcome {
        before_digest: Some(sha256_hex(b"before")),
        changed: Some(true),
        target_results: Some(AuditTargetResults {
            applied: 1,
            unchanged: 1,
            conflict: 0,
            failed: 0,
        }),
    };
    audit
        .terminal(&invocation, AuditResult::Applied, None, &outcome)
        .await
        .unwrap();

    let lines = read_lines(&path).await;
    assert_eq!(lines.len(), 2);
    let target = serde_json::json!({
        "topic": "orders",
        "brokers": {
            "count": 2,
            "digest": sha256_hex(br#"["broker-a","broker-b"]"#),
            "names": ["broker-a", "broker-b"],
        },
    });
    for line in &lines {
        assert_eq!(line["schema_version"], AUDIT_SCHEMA_VERSION);
        assert_eq!(line["target"], target);
        assert_eq!(line["requested_digest"], sha256_hex(b"requested"));
        assert_eq!(line["request_key_digest"], sha256_hex(b"change-0001"));
    }
    for unknown_before_the_attempt in ["before_digest", "changed", "target_results"] {
        assert!(lines[0][unknown_before_the_attempt].is_null());
    }
    assert_eq!(lines[1]["before_digest"], sha256_hex(b"before"));
    assert_eq!(lines[1]["changed"], true);
    assert_eq!(
        lines[1]["target_results"],
        serde_json::json!({"applied": 1, "unchanged": 1, "conflict": 0, "failed": 0})
    );
    // The record proves which key was used without holding the key.
    assert!(!tokio::fs::read_to_string(&path).await.unwrap().contains("change-0001"));

    // A subject built for another operation is refused before anything is written.
    let other = AuditSubject::sample(ControlOperation::BrokerConfigPatch);
    let refused = audit
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &test_cluster(),
            false,
            &other,
        )
        .await
        .err()
        .unwrap();
    assert_audit_unavailable(refused);
    let pending = start_topic(&audit, false).await;
    let malformed = AuditOutcome {
        before_digest: Some("not-a-digest".to_owned()),
        ..AuditOutcome::default()
    };
    assert!(audit
        .terminal(&pending, AuditResult::Applied, None, &malformed)
        .await
        .is_err());
    assert_eq!(sink.records().await.unwrap().len(), 3);
}

#[tokio::test]
async fn mixed_v1_v2_v3_file_recovers() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mixed.jsonl");
    let v1 = |sequence: u64, event: &str| {
        serde_json::json!({
            "schema_version": "rocketmq-mcp-control.audit.v1",
            "sequence": sequence,
            "invocation_id": 1,
            "timestamp_unix_millis": sequence,
            "event": event,
            "operation": "topic_upsert",
            "cluster": "cluster-a",
            "dry_run": true,
            "error_code": null,
        })
    };
    let mut v2_started = v2_started_record();
    v2_started.sequence = 3;
    v2_started.invocation_id = AuditInvocationId(3);
    let mut v2_completed = v2_started.clone();
    v2_completed.sequence = 4;
    v2_completed.event = AuditEvent::Completed;
    v2_completed.result = AuditResult::Planned;
    v2_completed.duration_millis = Some(7);
    let prefix = [
        v1(1, "started"),
        v1(2, "completed"),
        serde_json::to_value(&v2_started).unwrap(),
        serde_json::to_value(&v2_completed).unwrap(),
    ];
    write_lines(&path, &prefix).await;
    let legacy_bytes = tokio::fs::read(&path).await.unwrap();

    let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let audit = AuditTrail::resume(sink.clone()).await.unwrap();
    let invocation = start_topic(&audit, true).await;
    finish_planned(&audit, &invocation).await;
    drop(audit);
    drop(sink);

    // Old records are read as they are and never rewritten; new ones are version 3.
    assert!(tokio::fs::read(&path).await.unwrap().starts_with(&legacy_bytes));
    let reopened = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let versions = reopened
        .records()
        .await
        .unwrap()
        .iter()
        .map(|record| record.schema_version)
        .collect::<Vec<_>>();
    assert_eq!(
        versions,
        [
            AuditSchemaVersion::V1,
            AuditSchemaVersion::V1,
            AuditSchemaVersion::V2,
            AuditSchemaVersion::V2,
            AuditSchemaVersion::V3,
            AuditSchemaVersion::V3,
        ]
    );
    assert_eq!(
        AuditTrail::resume(reopened).await.unwrap().state.lock().await.sequence,
        6
    );
}

#[tokio::test]
async fn terminal_record_must_match_the_started_target() {
    let started = serde_json::to_value(started_record()).unwrap();
    let mut terminal = started.clone();
    terminal["sequence"] = serde_json::json!(2);
    terminal["event"] = serde_json::json!("completed");
    terminal["result"] = serde_json::json!("planned");
    terminal["duration_millis"] = serde_json::json!(5);
    let directory = tempfile::tempdir().unwrap();

    let consistent = directory.path().join("consistent.jsonl");
    write_lines(&consistent, &[started.clone(), terminal.clone()]).await;
    assert!(JsonlAuditSink::open(&consistent, 16, 4096).await.is_ok());

    let other_brokers = AuditBrokerSet::from_names(&["broker-b".to_owned()]);
    let changes: [(&str, serde_json::Value); 4] = [
        ("/target/topic", serde_json::json!("payments")),
        ("/target/brokers", serde_json::to_value(other_brokers).unwrap()),
        ("/requested_digest", serde_json::json!(sha256_hex(b"another request"))),
        ("/request_key_digest", serde_json::json!(sha256_hex(b"another key"))),
    ];
    for (index, (pointer, value)) in changes.into_iter().enumerate() {
        let mut drifted = terminal.clone();
        *drifted.pointer_mut(pointer).unwrap() = value;
        let path = directory.path().join(format!("drifted-{index}.jsonl"));
        write_lines(&path, &[started.clone(), drifted]).await;
        let error = JsonlAuditSink::open(&path, 16, 4096).await.err().unwrap();
        assert_audit_unavailable(error);
    }

    // Evidence that does not belong on a record is refused on its own, too.
    let mut invalid_cases = Vec::new();
    for (pointer, value) in [
        ("/target/topic", serde_json::json!("10.0.0.1")),
        ("/target/topic", serde_json::json!("broker.example.test:10911")),
        ("/target/brokers/names", serde_json::json!(["broker-z"])),
        ("/target/brokers/digest", serde_json::json!("abc")),
        ("/requested_digest", serde_json::json!("ABC")),
        ("/before_digest", serde_json::json!(sha256_hex(b"known too early"))),
        ("/changed", serde_json::json!(true)),
    ] {
        let mut invalid = started.clone();
        *invalid.pointer_mut(pointer).unwrap() = value;
        invalid_cases.push(invalid);
    }
    // A Topic upsert names a Topic and its Brokers, not a Consumer Group or a single Broker.
    let mut wrong_shape = started.clone();
    wrong_shape["target"] = serde_json::json!({"broker": "broker-a"});
    invalid_cases.push(wrong_shape);
    let mut extra_member = started.clone();
    extra_member["target"]["endpoint"] = serde_json::json!("broker-a");
    invalid_cases.push(extra_member);
    for (index, invalid) in invalid_cases.into_iter().enumerate() {
        let path = directory.path().join(format!("invalid-{index}.jsonl"));
        write_lines(&path, &[invalid]).await;
        assert!(
            JsonlAuditSink::open(&path, 16, 4096).await.is_err(),
            "accepted invalid evidence case {index}"
        );
    }
}

#[tokio::test]
async fn sixty_four_broker_targets_fit_within_the_record_bound() {
    let operator = format!("{}@example.test", "o".repeat(115));
    assert_eq!(operator.len(), 128);
    let reason = format!("{}done", "change ".repeat(36));
    assert_eq!(reason.len(), 256);
    let context = AuditContext::try_new(&operator, Some(&reason)).unwrap();
    let long_names = (0..64)
        .map(|index| format!("{index:02}{}", "b".repeat(125)))
        .collect::<Vec<_>>();
    let inline_names = (0..63).map(|index| format!("broker-{index:06}")).collect::<Vec<_>>();
    let outcome = AuditOutcome {
        before_digest: Some(sha256_hex(b"before")),
        changed: Some(true),
        target_results: Some(AuditTargetResults {
            applied: u32::MAX,
            unchanged: u32::MAX,
            conflict: u32::MAX,
            failed: u32::MAX,
        }),
    };
    let group = Some("g".repeat(255));
    let subjects = [
        // 64 Brokers with the longest names are recorded by count and digest.
        (
            ControlOperation::ConsumerGroupUpsert,
            AuditTarget {
                consumer_group: group.clone(),
                brokers: Some(AuditBrokerSet::from_names(&long_names)),
                ..AuditTarget::default()
            },
        ),
        // The largest list that is still carried inline.
        (
            ControlOperation::ConsumerGroupUpsert,
            AuditTarget {
                consumer_group: group.clone(),
                brokers: Some(AuditBrokerSet::from_names(&inline_names)),
                ..AuditTarget::default()
            },
        ),
        (
            ControlOperation::ConsumerOffsetReset,
            AuditTarget {
                topic: Some("t".repeat(127)),
                consumer_group: group,
                ..AuditTarget::default()
            },
        ),
    ];
    assert!(subjects[0].1.brokers.as_ref().unwrap().names.is_none());
    assert_eq!(
        subjects[1].1.brokers.as_ref().unwrap().names.as_ref().unwrap().len(),
        63
    );

    let sink = Arc::new(MemoryAuditSink::new(16, MIN_AUDIT_RECORD_BYTES));
    let audit = AuditTrail::new(sink.clone());
    for (operation, target) in subjects {
        let subject =
            AuditSubject::try_new(operation, target, sha256_hex(b"requested"), Some(sha256_hex(b"key"))).unwrap();
        let invocation = audit
            .start(&context, operation, &test_cluster(), false, &subject)
            .await
            .unwrap();
        audit
            .terminal(
                &invocation,
                AuditResult::Partial,
                Some(ControlErrorCode::PartialApply),
                &outcome,
            )
            .await
            .unwrap();
    }
    // Sequence, invocation, timestamp and duration can each grow to twenty digits.
    let largest = sink
        .records()
        .await
        .unwrap()
        .iter()
        .map(|record| encode_record(record, MIN_AUDIT_RECORD_BYTES).unwrap().len())
        .max()
        .unwrap();
    assert!(
        largest + 80 <= MIN_AUDIT_RECORD_BYTES,
        "largest record is {largest} bytes"
    );
}

#[tokio::test]
async fn full_segment_rotates_without_failing_the_invocation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rotating.jsonl");
    let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let audit = AuditTrail::new(sink.clone());
    for _ in 0..20 {
        let invocation = start_topic(&audit, true).await;
        finish_planned(&audit, &invocation).await;
    }
    assert_eq!(sink.last_sequence().await.unwrap(), 40);
    // The sink holds the active segment only.
    assert!(sink.records().await.unwrap().len() < 16);

    // Every record is in exactly one place: two sealed segments and the active one.
    let mut sequences = Vec::new();
    for segment in [
        sealed_segment_path(&path, 1),
        sealed_segment_path(&path, 2),
        path.clone(),
    ] {
        let lines = read_lines(&segment).await;
        assert!(lines.len() <= 16);
        let carried = lines
            .first()
            .filter(|line| is_segment_header(line))
            .map_or(0, |header| header["open_invocations"].as_array().unwrap().len());
        let own = lines
            .iter()
            .filter(|line| !is_segment_header(line))
            .skip(carried)
            .map(|line| line["sequence"].as_u64().unwrap());
        sequences.extend(own);
    }
    assert_eq!(sequences, (1..=40).collect::<Vec<_>>());
    assert!(!tokio::fs::try_exists(sealed_segment_path(&path, 3)).await.unwrap());

    // A restart reads the active segment alone and continues the sequence.
    drop(audit);
    drop(sink);
    let reopened = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let resumed = AuditTrail::resume(reopened).await.unwrap();
    assert_eq!(start_topic(&resumed, true).await.id().get(), 41);
}

#[tokio::test]
async fn segment_header_links_to_the_previous_segment() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("linked.jsonl");
    let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let audit = AuditTrail::new(sink.clone());
    for _ in 0..16 {
        let invocation = start_topic(&audit, true).await;
        finish_planned(&audit, &invocation).await;
    }

    // Segment 1 is the plain file an older version would have written: no header.
    let first = sealed_segment_path(&path, 1);
    assert!(!is_segment_header(&read_lines(&first).await[0]));
    // Segment 2 filled up while invocation 31 was in flight, so segment 3 carries its start.
    let second = sealed_segment_path(&path, 2);
    let second_lines = read_lines(&second).await;
    assert_eq!(
        second_lines[0],
        serde_json::json!({
            "schema_version": "rocketmq-mcp-control.audit-segment.v1",
            "segment": 2,
            "previous_last_sequence": 16,
            "previous_segment_digest": sha256_hex(&tokio::fs::read(&first).await.unwrap()),
            "open_invocations": [],
        })
    );
    let active_lines = read_lines(&path).await;
    assert_eq!(
        active_lines[0],
        serde_json::json!({
            "schema_version": "rocketmq-mcp-control.audit-segment.v1",
            "segment": 3,
            "previous_last_sequence": 31,
            "previous_segment_digest": sha256_hex(&tokio::fs::read(&second).await.unwrap()),
            "open_invocations": [31],
        })
    );
    // The carried copy is the `started` record as it was first written.
    assert_eq!(&active_lines[1], second_lines.last().unwrap());
    assert_eq!(active_lines[2]["sequence"], 32);
    assert_eq!(active_lines[2]["invocation_id"], 31);
}

#[tokio::test]
async fn unfinished_invocations_survive_rotation_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("carried.jsonl");
    let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let audit = AuditTrail::new(sink.clone());
    let long_running = start_topic(&audit, false).await;
    for _ in 0..8 {
        let invocation = start_topic(&audit, true).await;
        finish_planned(&audit, &invocation).await;
    }
    // The segment was sealed while the first invocation and the last pair were open.
    let header = read_lines(&path).await.remove(0);
    assert_eq!(header["open_invocations"], serde_json::json!([1, 16]));
    audit
        .terminal(&long_running, AuditResult::Applied, None, &AuditOutcome::default())
        .await
        .unwrap();
    drop(audit);
    drop(sink);

    // The active segment alone recovers: the terminal record is checked against the carried start.
    let reopened = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let resumed = AuditTrail::resume(reopened.clone()).await.unwrap();
    let dangling = start_topic(&resumed, false).await;
    assert_eq!(dangling.id().get(), 19);
    drop(resumed);
    drop(reopened);

    // A start left behind by a process that is gone is not carried any further.
    let restarted = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let audit = AuditTrail::resume(restarted.clone()).await.unwrap();
    for _ in 0..8 {
        let invocation = start_topic(&audit, true).await;
        finish_planned(&audit, &invocation).await;
    }
    let lines = read_lines(&path).await;
    assert_eq!(lines[0]["segment"], 3);
    assert!(!lines[0]["open_invocations"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!(19)));
    let abandoned = read_lines(&sealed_segment_path(&path, 2)).await;
    assert_eq!(abandoned.iter().filter(|line| line["invocation_id"] == 19).count(), 1);

    // A carried copy that does not match the header is refused.
    let tampered = directory.path().join("tampered.jsonl");
    let mut lines = read_lines(&sealed_segment_path(&path, 2)).await;
    lines[1]["invocation_id"] = serde_json::json!(2);
    lines[1]["sequence"] = serde_json::json!(2);
    write_lines(&tampered, &lines).await;
    assert!(JsonlAuditSink::open(&tampered, 16, 4096).await.is_err());
}

#[tokio::test]
async fn rotation_failure_is_audit_unavailable_and_blocks_execution() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("blocked.jsonl");
    let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let audit = AuditTrail::new(sink.clone());
    for _ in 0..8 {
        let invocation = start_topic(&audit, true).await;
        finish_planned(&audit, &invocation).await;
    }
    // History already sits where the full segment would be sealed.
    let occupied = sealed_segment_path(&path, 1);
    tokio::fs::write(&occupied, b"archived by an operator\n").await.unwrap();
    let active_before = tokio::fs::read(&path).await.unwrap();

    let refused = audit
        .start(
            &audit_context(),
            ControlOperation::TopicUpsert,
            &test_cluster(),
            false,
            &AuditSubject::sample(ControlOperation::TopicUpsert),
        )
        .await
        .err()
        .unwrap();
    // No `started` record means no session and no RPC for that call.
    assert_audit_unavailable(refused);
    assert_audit_unavailable(audit.records().await.unwrap_err());
    assert_eq!(tokio::fs::read(&path).await.unwrap(), active_before);
    assert_eq!(tokio::fs::read(&occupied).await.unwrap(), b"archived by an operator\n");

    // The same collision is reported at startup instead of at the first full segment.
    drop(audit);
    drop(sink);
    assert_audit_unavailable(JsonlAuditSink::open(&path, 16, 4096).await.err().unwrap());
}

#[tokio::test]
async fn interrupted_rotation_is_finished_on_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("interrupted.jsonl");
    let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    let audit = AuditTrail::new(sink.clone());
    for _ in 0..9 {
        let invocation = start_topic(&audit, true).await;
        finish_planned(&audit, &invocation).await;
    }
    drop(audit);
    drop(sink);
    // A crash after the full segment was sealed and before the next one took its place leaves
    // the prepared segment under its temporary name.
    let prepared = directory.path().join("interrupted.jsonl.next");
    tokio::fs::rename(&path, &prepared).await.unwrap();

    let reopened = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
    assert!(!tokio::fs::try_exists(&prepared).await.unwrap());
    assert_eq!(reopened.last_sequence().await.unwrap(), 18);
    let resumed = AuditTrail::resume(reopened).await.unwrap();
    assert_eq!(start_topic(&resumed, true).await.id().get(), 19);
}

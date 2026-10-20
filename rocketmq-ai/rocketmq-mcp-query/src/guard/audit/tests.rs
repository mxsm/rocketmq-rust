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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;

use super::*;

#[test]
fn a_record_without_a_target_has_exactly_the_fields_of_schema_version_one() {
    let record = sanitize_record(sample_record("request-1", None).with_target(AuditTarget::default()));
    assert_eq!(record.target, None, "a target that names nothing is not kept");

    let written = serde_json::to_value(&record).unwrap();
    let mut fields = written
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    fields.sort_unstable();
    assert_eq!(
        fields,
        [
            "arguments_hash",
            "client",
            "cluster",
            "duration_ms",
            "error",
            "operator",
            "request_id",
            "risk_level",
            "schema_version",
            "status",
            "timestamp_unix_ms",
            "tool",
        ]
    );
    assert_eq!(written["schema_version"], 2);
}

#[test]
fn a_record_says_what_the_call_asked_about_and_writes_a_name_like_its_other_fields() {
    let record = sanitize_record(sample_record("request-1", None).with_target(target(serde_json::json!({
        "topic": "orders",
        "consumer_group": "billing",
        // A logical name, and one that carries a host name.
        "broker_names": ["broker-a", "broker-a.internal"],
    }))));

    let written = serde_json::to_value(&record).unwrap();
    assert_eq!(
        written["target"],
        serde_json::json!({
            "topic": "orders",
            "consumer_group": "billing",
            "broker_names": ["broker-a", sanitize_text("broker-a.internal")],
        })
    );
    assert!(!written.to_string().contains("broker-a.internal"));
}

#[test]
fn the_names_of_a_record_are_given_up_before_the_record_is() {
    let log = AuditLog::default();
    let mut config = test_config("memory");
    let named = |request_id: &str| {
        sample_record(request_id, None).with_target(target(serde_json::json!({
            "topic": "orders",
            "consumer_group": "billing",
        })))
    };

    // A limit that the record meets without its names: version 1 kept this record.
    config.max_record_bytes = log
        .prepare_record(&config, sample_record("request-1", None))
        .unwrap()
        .1
        .len();
    log.record(&config, named("request-1"));
    // With room for them, the names are kept.
    config.max_record_bytes = 16 * 1024;
    log.record(&config, named("request-2"));

    let records = log.records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].target, None);
    assert_eq!(
        serde_json::to_value(&records[1].target).unwrap(),
        serde_json::json!({ "topic": "orders", "consumer_group": "billing" })
    );
    assert_eq!(log.metrics().oversized, 0);
}

struct TestSinkState {
    attempts: Mutex<Vec<String>>,
    started: Notify,
    release: Semaphore,
    stall_first: AtomicBool,
    failures_remaining: AtomicU64,
    fail_flush: AtomicBool,
}

impl Default for TestSinkState {
    fn default() -> Self {
        Self {
            attempts: Mutex::new(Vec::new()),
            started: Notify::new(),
            release: Semaphore::new(0),
            stall_first: AtomicBool::new(false),
            failures_remaining: AtomicU64::new(0),
            fail_flush: AtomicBool::new(false),
        }
    }
}

struct TestAuditSink {
    state: Arc<TestSinkState>,
}

impl AuditSink for TestAuditSink {
    async fn write(&mut self, record: &AuditRecord, _encoded: &[u8]) -> crate::McpResult<()> {
        self.state
            .attempts
            .lock()
            .expect("test sink attempts lock")
            .push(record.request_id.clone());
        self.state.started.notify_one();
        if self.state.stall_first.swap(false, Ordering::AcqRel) {
            let permit = self.state.release.acquire().await.map_err(McpError::from_source)?;
            drop(permit);
        }
        if self
            .state
            .failures_remaining
            .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(McpError::from_source(std::io::Error::other("audit test sink failed")));
        }
        Ok(())
    }

    async fn flush(&mut self) -> crate::McpResult<()> {
        if self.state.fail_flush.load(Ordering::Acquire) {
            Err(McpError::from_source(std::io::Error::other("audit test sink failed")))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn memory_records_are_versioned_redacted_bounded_and_oversized_records_are_dropped() {
    let runtime = rocketmq_runtime::RuntimeContext::try_from_current("audit-memory-test").unwrap();
    let service = runtime.service_context("audit-writer");
    let log = AuditLog::default();
    let mut config = test_config("memory");
    log.start(&config, &service).unwrap();
    log.record(
        &config,
        sample_record("request-1", Some("token=secret\nrequest failed".to_string())),
    );

    let records = log.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].schema_version, AUDIT_SCHEMA_VERSION);
    let error = records[0].error.as_deref().expect("sanitized error");
    assert!(!error.contains("secret"));
    assert!(!error.contains('\n'));
    assert_eq!(error, "MCP request failed");

    config.max_record_bytes = 64;
    log.record(&config, sample_record("request-2", None));
    let metrics = log.metrics();
    assert_eq!(metrics.accepted, 1);
    assert_eq!(metrics.written, 1);
    assert_eq!(metrics.dropped, 1);
    assert_eq!(metrics.oversized, 1);

    config.max_record_bytes = 16 * 1024;
    let report = log
        .close_and_drain(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert_eq!(report.status, AuditDrainStatus::Drained);
    log.record(&config, sample_record("request-after-close", None));
    assert_eq!(log.metrics().closed_drops, 1);
    runtime
        .shutdown_tasks_until(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
}

#[tokio::test]
async fn count_capacity_is_non_blocking_and_preserves_fifo_for_accepted_records() {
    let runtime = rocketmq_runtime::RuntimeContext::try_from_current("audit-count-test").unwrap();
    let service = runtime.service_context("audit-writer");
    let log = AuditLog::default();
    let mut config = test_config("tracing");
    config.queue_capacity = 1;
    let state = Arc::new(TestSinkState::default());
    state.stall_first.store(true, Ordering::Release);
    log.start_with_sink(&config, &service, TestAuditSink { state: state.clone() })
        .unwrap();

    log.record(&config, sample_record("request-1", None));
    tokio::time::timeout(Duration::from_secs(1), state.started.notified())
        .await
        .expect("writer started");
    log.record(&config, sample_record("request-2", None));
    log.record(&config, sample_record("request-3", None));

    let metrics = log.metrics();
    assert_eq!(metrics.accepted, 2);
    assert_eq!(metrics.count_capacity_drops, 1);
    state.release.add_permits(1);
    let report = log
        .close_and_drain(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
    assert_eq!(report.status, AuditDrainStatus::Drained);
    assert_eq!(report.written, 2);
    assert_eq!(
        *state.attempts.lock().unwrap(),
        vec!["request-1".to_string(), "request-2".to_string()]
    );
    runtime
        .shutdown_tasks_until(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
}

#[tokio::test]
async fn byte_capacity_is_non_blocking_independent_of_record_capacity() {
    let runtime = rocketmq_runtime::RuntimeContext::try_from_current("audit-byte-test").unwrap();
    let service = runtime.service_context("audit-writer");
    let log = AuditLog::default();
    let mut config = test_config("tracing");
    let encoded_len = log
        .prepare_record(&config, sample_record("request-1", None))
        .unwrap()
        .1
        .len();
    config.max_record_bytes = encoded_len * 2;
    config.queue_max_bytes = encoded_len * 2;
    config.queue_capacity = 8;
    let state = Arc::new(TestSinkState::default());
    state.stall_first.store(true, Ordering::Release);
    log.start_with_sink(&config, &service, TestAuditSink { state: state.clone() })
        .unwrap();

    log.record(&config, sample_record("request-1", None));
    tokio::time::timeout(Duration::from_secs(1), state.started.notified())
        .await
        .expect("writer started");
    log.record(&config, sample_record("request-2", None));
    log.record(&config, sample_record("request-3", None));

    assert_eq!(log.metrics().accepted, 2);
    assert_eq!(log.metrics().byte_capacity_drops, 1);
    state.release.add_permits(1);
    let report = log
        .close_and_drain(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
    assert_eq!(report.status, AuditDrainStatus::Drained);
    assert_eq!(report.pending_bytes, 0);
    runtime
        .shutdown_tasks_until(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
}

#[tokio::test]
async fn sink_failure_does_not_stop_fifo_writer_and_flush_failure_is_reported() {
    let runtime = rocketmq_runtime::RuntimeContext::try_from_current("audit-failure-test").unwrap();
    let service = runtime.service_context("audit-writer");
    let log = AuditLog::default();
    let config = test_config("tracing");
    let state = Arc::new(TestSinkState::default());
    state.failures_remaining.store(1, Ordering::Release);
    state.fail_flush.store(true, Ordering::Release);
    log.start_with_sink(&config, &service, TestAuditSink { state: state.clone() })
        .unwrap();

    for request_id in ["request-1", "request-2", "request-3"] {
        log.record(&config, sample_record(request_id, None));
    }
    let report = log
        .close_and_drain(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;

    assert_eq!(report.status, AuditDrainStatus::Drained);
    assert_eq!(report.accepted, 3);
    assert_eq!(report.written, 2);
    assert_eq!(report.sink_failures, 1);
    assert_eq!(report.flush_failures, 1);
    assert_eq!(
        *state.attempts.lock().unwrap(),
        vec![
            "request-1".to_string(),
            "request-2".to_string(),
            "request-3".to_string()
        ]
    );
    runtime
        .shutdown_tasks_until(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
}

#[tokio::test]
async fn one_absolute_deadline_reports_stall_then_allows_a_later_drain() {
    let runtime = rocketmq_runtime::RuntimeContext::try_from_current("audit-deadline-test").unwrap();
    let service = runtime.service_context("audit-writer");
    let log = AuditLog::default();
    let config = test_config("tracing");
    let state = Arc::new(TestSinkState::default());
    state.stall_first.store(true, Ordering::Release);
    log.start_with_sink(&config, &service, TestAuditSink { state: state.clone() })
        .unwrap();
    log.record(&config, sample_record("request-1", None));
    tokio::time::timeout(Duration::from_secs(1), state.started.notified())
        .await
        .expect("writer started");

    let timed_out = log
        .close_and_drain(rocketmq_runtime::ShutdownDeadline::after(Duration::from_millis(10)))
        .await;
    assert_eq!(timed_out.status, AuditDrainStatus::TimedOut);
    assert_eq!(timed_out.pending_records, 1);
    log.record(&config, sample_record("request-after-close", None));
    assert_eq!(log.metrics().closed_drops, 1);

    state.release.add_permits(1);
    let drained = log
        .close_and_drain(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
    assert_eq!(drained.status, AuditDrainStatus::Drained);
    assert_eq!(drained.pending_records, 0);
    runtime
        .shutdown_tasks_until(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
}

#[tokio::test]
async fn runtime_cancellation_releases_pending_capacity_without_claiming_a_drain() {
    let runtime = rocketmq_runtime::RuntimeContext::try_from_current("audit-cancellation-test").unwrap();
    let service = runtime.service_context("audit-writer");
    let log = AuditLog::default();
    let config = test_config("tracing");
    let state = Arc::new(TestSinkState::default());
    state.stall_first.store(true, Ordering::Release);
    log.start_with_sink(&config, &service, TestAuditSink { state: state.clone() })
        .unwrap();
    log.record(&config, sample_record("request-cancelled", None));
    tokio::time::timeout(Duration::from_secs(1), state.started.notified())
        .await
        .expect("writer started");

    let deadline = rocketmq_runtime::ShutdownDeadline::after(Duration::from_millis(10));
    let audit = log.close_and_drain(deadline).await;
    assert_eq!(audit.status, AuditDrainStatus::TimedOut);
    let runtime_report = runtime.shutdown_tasks_until(deadline).await;
    assert!(!runtime_report.is_healthy());

    let after_cancellation = log
        .close_and_drain(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert_eq!(after_cancellation.status, AuditDrainStatus::TimedOut);
    assert_eq!(after_cancellation.pending_records, 0);
    assert_eq!(after_cancellation.pending_bytes, 0);
}

#[tokio::test]
async fn file_sink_flushes_ndjson_through_the_blocking_boundary() {
    let runtime = rocketmq_runtime::RuntimeContext::try_from_current("audit-file-test").unwrap();
    let service = runtime.service_context("audit-writer");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("audit.jsonl");
    let log = AuditLog::default();
    let mut config = test_config("file");
    config.path = path.to_string_lossy().into_owned();
    log.start(&config, &service).unwrap();
    log.record(&config, sample_record("request-file", None));

    let report = log
        .close_and_drain(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
    assert_eq!(report.status, AuditDrainStatus::Drained);
    assert_eq!(report.accepted, 1);
    assert_eq!(report.written, 1);
    let contents = std::fs::read_to_string(path).unwrap();
    let record: serde_json::Value = serde_json::from_str(contents.trim()).unwrap();
    assert_eq!(record["schema_version"], AUDIT_SCHEMA_VERSION);
    assert_eq!(record["request_id"], "request-file");
    runtime
        .shutdown_tasks_until(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
}

#[tokio::test]
async fn the_file_sink_seals_a_full_file_and_starts_the_next_one() {
    let runtime = rocketmq_runtime::RuntimeContext::try_from_current("audit-file-seal-test").unwrap();
    let service = runtime.service_context("audit-writer");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("audit.jsonl");
    let mut config = test_config("file");
    config.path = path.to_string_lossy().into_owned();
    config.max_record_bytes = AuditLog::default()
        .prepare_record(&config, sample_record("request-1", None))
        .unwrap()
        .1
        .len();

    // A file that is sealed has to hold one record at least.
    config.max_file_bytes = config.max_record_bytes as u64 - 1;
    assert!(AuditLog::default().start(&config, &service).is_err());

    // Two records fit a file, a third starts the next one.
    config.max_file_bytes = 2 * config.max_record_bytes as u64;
    let log = AuditLog::default();
    log.start(&config, &service).unwrap();
    for request_id in ["request-1", "request-2", "request-3"] {
        log.record(&config, sample_record(request_id, None));
    }
    let report = log
        .close_and_drain(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
    assert_eq!(report.status, AuditDrainStatus::Drained);
    assert_eq!(report.written, 3);
    assert_eq!(report.sink_failures, 0);

    let request_ids = |path: &std::path::Path| {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()["request_id"].clone())
            .collect::<Vec<_>>()
    };
    let sealed = directory.path().join("audit.jsonl.000001");
    assert_eq!(request_ids(&sealed), ["request-1", "request-2"]);
    assert_eq!(request_ids(&path), ["request-3"]);
    runtime
        .shutdown_tasks_until(rocketmq_runtime::ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
}

fn target(arguments: serde_json::Value) -> AuditTarget {
    AuditTarget::of_tool_arguments(arguments.as_object().expect("an object of arguments"))
}

fn test_config(sink: &str) -> AuditConfig {
    AuditConfig {
        enabled: true,
        sink: sink.to_string(),
        path: String::new(),
        queue_capacity: 16,
        max_record_bytes: 16 * 1024,
        queue_max_bytes: 1024 * 1024,
        max_file_bytes: 0,
    }
}

fn sample_record(request_id: &str, error: Option<String>) -> AuditRecord {
    AuditRecord::new(
        request_id.to_string(),
        "operator-a".to_string(),
        Some("client-a".to_string()),
        Some("cluster-a".to_string()),
        "rocketmq_get_cluster_overview".to_string(),
        "0123456789abcdef".to_string(),
        RiskLevel::ReadOnly,
        AuditStatus::Success,
        7,
        error,
    )
}

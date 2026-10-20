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

//! The consumer lag diagnosis workflow: what it reads, and what it does when a read fails.

use super::*;
use crate::model::diagnosis::EvidenceStatus;
use crate::model::diagnosis::Severity;

fn rule_ids(report: &DiagnosisReport) -> Vec<&str> {
    report.root_causes.iter().map(|cause| cause.rule_id.as_str()).collect()
}

#[tokio::test]
async fn diagnosis_names_offline_consumers_from_the_group_evidence() {
    let facade = QueryFacade::with_factory(
        example_config(),
        FakeSessionFactory {
            offline_consumers: true,
            ..Default::default()
        },
    );

    let result = facade.diagnose_consumer_lag(diagnosis_request()).await.unwrap();
    let report = &result.data;

    assert!(!result.partial);
    // The group consumes half a message per second, which would otherwise read as a slow consumer.
    assert_eq!(rule_ids(report), ["no_online_consumer"]);
    assert_eq!(report.root_causes[0].evidence_refs, ["consumer_group"]);
    let group = evidence_data(report, "consumer_group");
    assert_eq!(group["online_consumer_count"], 0);
    assert_eq!(group["offline_brokers"], serde_json::json!(["broker-a"]));
    assert_eq!(group["connections_complete"], true);
}

#[tokio::test]
async fn diagnosis_reads_a_missing_retry_or_dead_letter_topic_as_an_observation() {
    let facade = QueryFacade::with_factory(example_config(), FakeSessionFactory::default());

    let result = facade.diagnose_consumer_lag(diagnosis_request()).await.unwrap();
    let report = &result.data;

    assert!(!result.partial);
    assert!(result.source_failures.is_empty());
    for (id, topic) in [
        ("retry_topic_stats", "%RETRY%order-service"),
        ("dead_letter_topic_stats", "%DLQ%order-service"),
    ] {
        assert_eq!(evidence_item(report, id).status, EvidenceStatus::Present, "item={id}");
        let activity = evidence_data(report, id);
        assert_eq!(activity["topic"], topic);
        assert_eq!(activity["exists"], false);
        assert_eq!(activity["message_count"], 0);
    }
    assert_eq!(rule_ids(report), ["consume_throughput_low"]);
}

#[tokio::test]
async fn diagnosis_reports_a_retry_topic_that_is_receiving_messages() {
    let facade = QueryFacade::with_factory(
        example_config(),
        FakeSessionFactory {
            retried_messages: true,
            ..Default::default()
        },
    );

    let result = facade.diagnose_consumer_lag(diagnosis_request()).await.unwrap();
    let report = &result.data;

    assert!(!result.partial);
    assert_eq!(rule_ids(report), ["consume_throughput_low", "retry_queue_growing"]);
    assert_eq!(report.root_causes[1].evidence_refs, ["retry_topic_stats"]);
    let retry = evidence_data(report, "retry_topic_stats");
    assert_eq!(retry["exists"], true);
    assert_eq!(retry["message_count"], 12);
    assert_eq!(evidence_data(report, "dead_letter_topic_stats")["exists"], false);
}

#[tokio::test]
async fn diagnosis_survives_a_failed_supporting_source() {
    let factory = FakeSessionFactory {
        fail_topic_config: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade.diagnose_consumer_lag(diagnosis_request()).await.unwrap();
    let report = &result.data;

    assert!(result.partial);
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning == "diagnosis_evidence_incomplete"));
    assert_eq!(result.source_failures.len(), 1);
    assert_eq!(result.source_failures[0].source, QuerySource::TopicConfig);
    assert_eq!(result.source_failures[0].logical_target, "orders");
    assert_eq!(report.missing_evidence, ["topic_config"]);
    assert_eq!(
        evidence_item(report, "topic_config").status,
        EvidenceStatus::Unavailable
    );
    // The backlog is graded and explained from the evidence that was collected.
    assert_eq!(report.severity, Severity::High);
    assert_eq!(rule_ids(report), ["consume_throughput_low"]);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    // The reads after the failed one were still made.
    assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.topic_stats_queries.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn diagnosis_keeps_its_conclusion_when_supporting_evidence_runs_out_of_time() {
    let factory = FakeSessionFactory {
        hang_supporting_evidence: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let control = WorkflowControl::new(Duration::from_secs(10), CancellationToken::new());
    let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
    let started = tokio::time::Instant::now();

    let result = facade.diagnose_consumer_lag(diagnosis_request()).await.unwrap();
    let report = &result.data;

    // The supporting evidence gives up a tenth of the budget before the request would time out.
    assert_eq!(started.elapsed(), Duration::from_secs(9));
    assert_eq!(report.severity, Severity::High);
    assert_eq!(rule_ids(report), ["consume_throughput_low"]);
    let missing = [
        "broker_summary",
        "consumer_group",
        "topic_config",
        "retry_topic_stats",
        "dead_letter_topic_stats",
    ];
    assert_eq!(report.missing_evidence, missing);
    for id in missing {
        assert_eq!(evidence_item(report, id).status, EvidenceStatus::Timeout, "item={id}");
    }
    assert!(result.partial);
    assert_eq!(result.source_failures.len(), missing.len());
    assert!(result
        .source_failures
        .iter()
        .all(|failure| failure.code == SourceFailureCode::Timeout && failure.retryable));
    // The read that ran out of time was dropped where it stood, so the session was asked for
    // nothing else and then closed.
    assert_eq!(counters.consumer_group_details_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.topic_config_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.topic_stats_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn diagnosis_still_times_out_when_the_backlog_itself_cannot_be_read() {
    let factory = FakeSessionFactory {
        hang_consumer_lag: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let control = WorkflowControl::new(Duration::from_secs(10), CancellationToken::new());
    let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);

    let error = facade.diagnose_consumer_lag(diagnosis_request()).await.unwrap_err();

    assert!(matches!(
        error,
        ToolFailure::Rejected(crate::tools::executor::ToolRejection::TimedOut { timeout_ms: 10_000 })
    ));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn diagnosis_closes_its_session_when_the_request_is_cancelled() {
    let factory = FakeSessionFactory {
        hang_supporting_evidence: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let cancellation = CancellationToken::new();
    let control = WorkflowControl::new(Duration::from_secs(60), cancellation.clone());
    let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
    let waiting = counters.clone();
    tokio::spawn(async move {
        while waiting.consumer_group_details_queries.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        cancellation.cancel();
    });

    let error = facade.diagnose_consumer_lag(diagnosis_request()).await.unwrap_err();

    assert!(matches!(
        error,
        ToolFailure::Rejected(crate::tools::executor::ToolRejection::Cancelled)
    ));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn diagnosis_applies_the_thresholds_configured_for_the_topic() {
    let mut config = example_config();
    config.diagnosis.overrides = vec![crate::config::DiagnosisOverride {
        topic: Some("orders".to_string()),
        consumer_lag_threshold: Some(50_000),
        ..Default::default()
    }];
    let facade = QueryFacade::with_factory(config, FakeSessionFactory::default());

    let report = facade.diagnose_consumer_lag(diagnosis_request()).await.unwrap().data;

    // The backlog of 10,000 is high against the default threshold and healthy against this one.
    assert_eq!(report.severity, Severity::Healthy);
    assert_eq!(report.target["lag_threshold"], 50_000);
    assert_eq!(rule_ids(&report), ["backlog_within_threshold"]);
}

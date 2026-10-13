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

//! The Consumer Group diagnosis workflow: one session, and a report whatever a read does.

use super::*;
use crate::model::diagnosis::EvidenceStatus;
use crate::model::diagnosis::Severity;
use crate::tools::diagnosis_tools::DiagnoseConsumerGroupArgs;

fn request() -> DiagnoseConsumerGroupArgs {
    DiagnoseConsumerGroupArgs {
        cluster: "local-dev".to_string(),
        consumer_group: "order-service".to_string(),
    }
}

fn rule_ids(report: &DiagnosisReport) -> Vec<&str> {
    report.root_causes.iter().map(|cause| cause.rule_id.as_str()).collect()
}

#[tokio::test]
async fn consumer_group_diagnosis_reads_every_source_in_one_session() {
    let factory = FakeSessionFactory::default();
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade.diagnose_consumer_group(request()).await.unwrap();
    let report = &result.data;

    assert!(!result.partial);
    assert_eq!(report.report_type, "consumer_group");
    assert_eq!(report.severity, Severity::High);
    // Half a message per second, nine tenths of the backlog on one Topic, which has one queue.
    assert_eq!(
        rule_ids(report),
        [
            "consume_throughput_low",
            "lag_concentrated_on_queue",
            "lag_concentrated_on_topic"
        ]
    );
    let progress = evidence_data(report, "consumer_lag");
    assert_eq!(progress["total_lag"], 10_000);
    assert_eq!(progress["topic_count"], 2);
    assert_eq!(progress["topics"][0]["topic"], "orders");
    assert_eq!(evidence_data(report, "consumer_group")["online_consumer_count"], 1);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(counters.consumer_group_details_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.consumer_progress_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.topic_stats_queries.load(Ordering::SeqCst), 2);
    // Nothing of one Topic is read: that is the consumer lag diagnosis.
    assert_eq!(counters.consumer_lag_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.route_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 0);

    // An identical request is answered from the cache.
    let replay = facade.diagnose_consumer_group(request()).await.unwrap();
    assert_eq!(replay.cache_status, CacheStatus::Hit);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn consumer_group_diagnosis_survives_a_failed_progress_read() {
    let factory = FakeSessionFactory {
        fail_exact_read: true,
        offline_consumers: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade.diagnose_consumer_group(request()).await.unwrap();
    let report = &result.data;

    assert!(result.partial);
    assert_eq!(result.source_failures.len(), 1);
    assert_eq!(result.source_failures[0].source, QuerySource::ConsumerStatistics);
    assert_eq!(report.severity, Severity::Unknown);
    assert_eq!(report.missing_evidence, ["consumer_lag"]);
    assert_eq!(
        evidence_item(report, "consumer_lag").status,
        EvidenceStatus::Unavailable
    );
    // What the other evidence establishes is still concluded.
    assert_eq!(rule_ids(report), ["no_online_consumer"]);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn consumer_group_diagnosis_reports_a_group_that_does_not_exist() {
    let factory = FakeSessionFactory {
        missing_consumer_group: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade.diagnose_consumer_group(request()).await.unwrap();
    let report = &result.data;

    assert_eq!(rule_ids(report), ["target_not_found"]);
    assert!(report.summary.contains("the consumer group does not exist"));
    assert_eq!(evidence_item(report, "consumer_group").status, EvidenceStatus::Missing);
    assert_eq!(evidence_item(report, "consumer_lag").status, EvidenceStatus::Missing);
    // A group no Broker knows has no progress to ask for.
    assert_eq!(counters.consumer_progress_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn consumer_group_diagnosis_times_out_within_the_request_budget_and_closes_its_session() {
    let factory = FakeSessionFactory {
        hang_consumer_lag: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let control = WorkflowControl::new(Duration::from_secs(10), CancellationToken::new());
    let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
    let started = tokio::time::Instant::now();

    let error = facade.diagnose_consumer_group(request()).await.unwrap_err();

    assert!(matches!(
        error,
        ToolFailure::Rejected(crate::tools::executor::ToolRejection::TimedOut { timeout_ms: 10_000 })
    ));
    assert_eq!(started.elapsed(), Duration::from_secs(10));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn consumer_group_diagnosis_closes_its_session_when_the_request_is_cancelled() {
    let factory = FakeSessionFactory {
        hang_consumer_lag: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let cancellation = CancellationToken::new();
    let control = WorkflowControl::new(Duration::from_secs(60), cancellation.clone());
    let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
    let waiting = counters.clone();
    tokio::spawn(async move {
        while waiting.consumer_progress_queries.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        cancellation.cancel();
    });

    let error = facade.diagnose_consumer_group(request()).await.unwrap_err();

    assert!(matches!(
        error,
        ToolFailure::Rejected(crate::tools::executor::ToolRejection::Cancelled)
    ));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

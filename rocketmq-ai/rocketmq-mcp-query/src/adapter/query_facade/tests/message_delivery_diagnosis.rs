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

//! The message delivery diagnosis workflow: one session, a report whatever a read does, and no
//! message identifier in anything it returns.

use super::*;
use crate::model::diagnosis::EvidenceStatus;
use crate::model::diagnosis::Severity;
use crate::tools::diagnosis_tools::DiagnoseMessageDeliveryArgs;

/// The identifier the caller asks about, and the two the Broker reports for the message it finds.
const REQUESTED_ID: &str = "REQUESTED-MESSAGE-ID";
const STORED_ID: &str = "STORED-OFFSET-MESSAGE-ID";
const STORED_UNIQUE_ID: &str = "STORED-UNIQUE-MESSAGE-ID";

fn request(message_id: Option<&str>) -> DiagnoseMessageDeliveryArgs {
    DiagnoseMessageDeliveryArgs {
        cluster: "local-dev".to_string(),
        topic: "orders".to_string(),
        consumer_group: "order-service".to_string(),
        message_id: message_id.map(str::to_string),
    }
}

/// A message at offset 2 of queue 0, the queue the group reports lag for, stored a minute ago.
fn stored_message(topic: &str) -> SessionMessageMetadata {
    let stored = crate::model::contract::now_millis() - 60_000;
    SessionMessageMetadata {
        message_id: STORED_ID.to_string(),
        unique_message_id: Some(STORED_UNIQUE_ID.to_string()),
        topic: topic.to_string(),
        born_timestamp: stored - 5,
        store_timestamp: stored,
        queue_id: 0,
        queue_offset: 2,
        store_size: 128,
        reconsume_times: 0,
        sys_flag: 0,
        flag: 0,
        prepared_transaction_offset: 0,
    }
}

fn rule_ids(report: &DiagnosisReport) -> Vec<&str> {
    report.root_causes.iter().map(|cause| cause.rule_id.as_str()).collect()
}

fn evidence_ids(report: &DiagnosisReport) -> Vec<&str> {
    report
        .evidence_snapshot
        .items
        .iter()
        .map(|item| item.id.as_str())
        .collect()
}

#[tokio::test]
async fn message_delivery_diagnosis_reads_every_source_in_one_session() {
    let factory = FakeSessionFactory {
        message: Some(stored_message("orders")),
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade
        .diagnose_message_delivery(request(Some(REQUESTED_ID)))
        .await
        .unwrap();
    let report = &result.data;

    assert!(!result.partial);
    assert!(result.source_failures.is_empty());
    assert_eq!(report.report_type, "message_delivery");
    assert_eq!(
        evidence_ids(report),
        [
            "topic_route",
            "consumer_group",
            "consumer_lag",
            "topic_stats",
            "message_metadata"
        ]
    );
    // The group is 10,000 messages behind and has not reached offset 2 of the queue.
    assert_eq!(report.severity, Severity::High);
    assert_eq!(rule_ids(report), ["consumer_backlog", "message_waiting"]);
    let message = evidence_data(report, "message_metadata");
    assert_eq!(message["position"], "waiting");
    assert_eq!(message["topic"], "orders");
    assert_eq!(message["queue_offset"], 2);
    assert_eq!(evidence_data(report, "topic_stats")["message_count"], 12);
    assert_eq!(evidence_data(report, "consumer_group")["online_consumer_count"], 1);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(counters.route_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.consumer_group_details_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.consumer_lag_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.message_metadata_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.topic_stats_queries.load(Ordering::SeqCst), 1);

    // An identical request is answered from the cache; another message is another diagnosis.
    let replay = facade
        .diagnose_message_delivery(request(Some(REQUESTED_ID)))
        .await
        .unwrap();
    assert_eq!(replay.cache_status, CacheStatus::Hit);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    facade
        .diagnose_message_delivery(request(Some("ANOTHER-MESSAGE-ID")))
        .await
        .unwrap();
    assert_eq!(counters.starts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn message_delivery_diagnosis_names_a_message_by_its_pseudonyms_only() {
    let facade = QueryFacade::with_factory(
        example_config(),
        FakeSessionFactory {
            message: Some(stored_message("orders")),
            ..Default::default()
        },
    );

    let result = facade
        .diagnose_message_delivery(request(Some(REQUESTED_ID)))
        .await
        .unwrap();

    let report = serde_json::to_string(&result.data).unwrap();
    for identifier in [REQUESTED_ID, STORED_ID, STORED_UNIQUE_ID] {
        assert!(!report.contains(identifier), "{identifier} is in the report");
    }
    let requested = result.data.target["message_alias"].as_str().unwrap();
    assert!(requested.starts_with("message-"), "{requested}");
    let message = evidence_data(&result.data, "message_metadata");
    assert!(message["message_alias"].as_str().unwrap().starts_with("message-"));
    assert!(message["unique_message_alias"]
        .as_str()
        .unwrap()
        .starts_with("unique-message-"));
}

#[tokio::test]
async fn message_delivery_diagnosis_looks_up_no_message_unless_one_is_named() {
    let factory = FakeSessionFactory::default();
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade.diagnose_message_delivery(request(None)).await.unwrap();
    let report = &result.data;

    assert!(!result.partial);
    assert!(!report.partial, "a message that was not asked about is not missing");
    assert_eq!(
        evidence_ids(report),
        ["topic_route", "consumer_group", "consumer_lag", "topic_stats"]
    );
    assert_eq!(report.target["message_alias"], serde_json::Value::Null);
    assert_eq!(rule_ids(report), ["consumer_backlog"]);
    assert_eq!(counters.message_metadata_queries.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn message_delivery_diagnosis_tells_a_consumed_message_from_one_it_cannot_place() {
    let diagnose = |factory: FakeSessionFactory| async move {
        QueryFacade::with_factory(example_config(), factory)
            .diagnose_message_delivery(request(Some(REQUESTED_ID)))
            .await
            .unwrap()
    };

    // The group read queue 0 up to offset 5, past the message at offset 2.
    let consumed = diagnose(FakeSessionFactory {
        message: Some(stored_message("orders")),
        consumer_offset: 5,
        ..Default::default()
    })
    .await;
    assert_eq!(rule_ids(&consumed.data), ["consumer_backlog", "message_delivered"]);
    assert_eq!(
        evidence_data(&consumed.data, "message_metadata")["position"],
        "consumed"
    );

    // A Broker did not report its offsets, and its queue 0 could be the one that holds the message.
    let incomplete = diagnose(FakeSessionFactory {
        message: Some(stored_message("orders")),
        consumer_offset: 5,
        partial_sources: true,
        ..Default::default()
    })
    .await;
    assert!(incomplete.partial);
    assert_eq!(incomplete.source_failures[0].source, QuerySource::ConsumerStatistics);
    assert_eq!(
        rule_ids(&incomplete.data),
        ["consumer_backlog", "message_position_undetermined"]
    );

    // The message is on queue 1, and the group reports no offset for a queue 1.
    let unplaced = diagnose(FakeSessionFactory::default()).await;
    assert_eq!(
        evidence_data(&unplaced.data, "message_metadata")["position"],
        "undetermined"
    );
}

#[tokio::test]
async fn message_delivery_diagnosis_reports_a_message_that_no_broker_holds() {
    let facade = QueryFacade::with_factory(
        example_config(),
        FakeSessionFactory {
            missing_message: true,
            ..Default::default()
        },
    );

    let result = facade
        .diagnose_message_delivery(request(Some(REQUESTED_ID)))
        .await
        .unwrap();
    let report = &result.data;

    assert_eq!(rule_ids(report), ["consumer_backlog", "message_not_found"]);
    assert_eq!(report.missing_evidence, ["message_metadata"]);
    assert_eq!(
        evidence_item(report, "message_metadata").status,
        EvidenceStatus::Missing
    );
    assert!(result.partial);
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning == "diagnosis_evidence_incomplete"));
}

#[tokio::test]
async fn message_delivery_diagnosis_survives_a_failed_evidence_source() {
    let factory = FakeSessionFactory {
        fail_exact_read: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade
        .diagnose_message_delivery(request(Some(REQUESTED_ID)))
        .await
        .unwrap();
    let report = &result.data;

    assert!(result.partial);
    assert_eq!(report.missing_evidence, ["message_metadata"]);
    assert_eq!(
        evidence_item(report, "message_metadata").status,
        EvidenceStatus::Unavailable
    );
    // The path is judged from the evidence that was collected, and nothing is said about the message.
    assert_eq!(report.severity, Severity::High);
    assert_eq!(rule_ids(report), ["consumer_backlog"]);
    // The read after the failed one was still made.
    assert_eq!(counters.topic_stats_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn message_delivery_diagnosis_asks_nothing_about_a_topic_that_does_not_exist() {
    let factory = FakeSessionFactory {
        missing_topic: true,
        message: Some(stored_message("payments")),
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade
        .diagnose_message_delivery(request(Some(REQUESTED_ID)))
        .await
        .unwrap();
    let report = &result.data;

    assert_eq!(report.severity, Severity::Unknown);
    // The topic name is wrong, and the message says which topic was meant.
    assert_eq!(rule_ids(report), ["target_not_found", "message_on_other_topic"]);
    assert_eq!(
        report.summary,
        "Delivery from topic orders to group order-service is Unknown because the topic does not exist in the \
         selected cluster."
    );
    assert_eq!(evidence_data(report, "message_metadata")["topic"], "payments");
    assert_eq!(counters.consumer_lag_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.topic_stats_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.consumer_group_details_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.message_metadata_queries.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn message_delivery_diagnosis_asks_for_no_progress_of_a_group_that_does_not_exist() {
    let factory = FakeSessionFactory {
        missing_consumer_group: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade.diagnose_message_delivery(request(None)).await.unwrap();
    let report = &result.data;

    assert_eq!(report.severity, Severity::Unknown);
    assert_eq!(rule_ids(report), ["target_not_found"]);
    assert_eq!(report.root_causes[0].evidence_refs, ["consumer_group"]);
    assert!(report.summary.contains("the consumer group does not exist"));
    assert_eq!(counters.consumer_lag_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.topic_stats_queries.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn message_delivery_diagnosis_writes_its_report_when_supporting_evidence_runs_out_of_time() {
    let factory = FakeSessionFactory {
        hang_supporting_evidence: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let control = WorkflowControl::new(Duration::from_secs(10), CancellationToken::new());
    let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
    let started = tokio::time::Instant::now();

    let result = facade
        .diagnose_message_delivery(request(Some(REQUESTED_ID)))
        .await
        .unwrap();
    let report = &result.data;

    // The supporting evidence gives up a tenth of the budget before the request would time out.
    assert_eq!(started.elapsed(), Duration::from_secs(9));
    let missing = ["consumer_group", "consumer_lag", "topic_stats", "message_metadata"];
    assert_eq!(report.missing_evidence, missing);
    for id in missing {
        assert_eq!(evidence_item(report, id).status, EvidenceStatus::Timeout, "item={id}");
    }
    // The topic is fine and nothing was learned about the group: that is not a healthy path.
    assert_eq!(report.severity, Severity::Unknown);
    assert_eq!(
        report.summary,
        "Delivery from topic orders to group order-service is Unknown because Consumer Group evidence is missing."
    );
    assert!(result.partial);
    assert!(result
        .source_failures
        .iter()
        .all(|failure| failure.code == SourceFailureCode::Timeout && failure.retryable));
    // The read that ran out of time was dropped where it stood, so the session was asked for
    // nothing else and then closed.
    assert_eq!(counters.consumer_group_details_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.consumer_lag_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.message_metadata_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.topic_stats_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn message_delivery_diagnosis_times_out_within_the_request_budget_and_closes_its_session() {
    let factory = FakeSessionFactory {
        hang_topic_route: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let control = WorkflowControl::new(Duration::from_secs(10), CancellationToken::new());
    let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
    let started = tokio::time::Instant::now();

    let error = facade.diagnose_message_delivery(request(None)).await.unwrap_err();

    assert!(matches!(
        error,
        ToolFailure::Rejected(crate::tools::executor::ToolRejection::TimedOut { timeout_ms: 10_000 })
    ));
    assert_eq!(started.elapsed(), Duration::from_secs(10));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn message_delivery_diagnosis_closes_its_session_when_the_request_is_cancelled() {
    let factory = FakeSessionFactory {
        hang_topic_route: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let cancellation = CancellationToken::new();
    let control = WorkflowControl::new(Duration::from_secs(60), cancellation.clone());
    let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
    let waiting = counters.clone();
    tokio::spawn(async move {
        while waiting.route_queries.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        cancellation.cancel();
    });

    let error = facade.diagnose_message_delivery(request(None)).await.unwrap_err();

    assert!(matches!(
        error,
        ToolFailure::Rejected(crate::tools::executor::ToolRejection::Cancelled)
    ));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn message_delivery_diagnosis_applies_the_thresholds_configured_for_the_topic() {
    let mut config = example_config();
    config.diagnosis.overrides = vec![crate::config::DiagnosisOverride {
        topic: Some("orders".to_string()),
        consumer_lag_threshold: Some(50_000),
        ..Default::default()
    }];
    let facade = QueryFacade::with_factory(config, FakeSessionFactory::default());

    let report = facade.diagnose_message_delivery(request(None)).await.unwrap().data;

    // The backlog of 10,000 is high against the default threshold and within this one.
    assert_eq!(report.severity, Severity::Healthy);
    assert_eq!(report.target["lag_threshold"], 50_000);
    assert!(report.root_causes.is_empty());
}

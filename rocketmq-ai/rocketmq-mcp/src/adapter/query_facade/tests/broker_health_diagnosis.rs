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

//! The Broker health diagnosis workflow: one session, and a report whatever a read does.

use super::*;
use crate::model::diagnosis::EvidenceStatus;
use crate::model::diagnosis::Severity;
use crate::tools::diagnosis_tools::DiagnoseBrokerHealthArgs;

fn request() -> DiagnoseBrokerHealthArgs {
    DiagnoseBrokerHealthArgs {
        cluster: "local-dev".to_string(),
        broker_name: "broker-a".to_string(),
    }
}

#[tokio::test]
async fn broker_health_diagnosis_reads_every_source_in_one_session() {
    let factory = FakeSessionFactory::default();
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade.diagnose_broker_health(request()).await.unwrap();
    let report = &result.data;

    assert!(!result.partial);
    assert_eq!(report.report_type, "broker_health");
    assert_eq!(report.severity, Severity::Healthy);
    assert_eq!(report.policy_profile, "production-default");
    assert_eq!(
        report.evidence_refs,
        ["broker_summary", "broker_diagnostics", "ha_status", "broker_config"]
    );
    assert_eq!(evidence_data(report, "broker_summary")["broker_name"], "broker-a");
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.broker_diagnostics_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.ha_status_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.broker_config_queries.load(Ordering::SeqCst), 1);

    // An identical request is answered from the cache.
    let replay = facade.diagnose_broker_health(request()).await.unwrap();
    assert_eq!(replay.cache_status, CacheStatus::Hit);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn broker_health_diagnosis_survives_failed_supporting_sources() {
    let factory = FakeSessionFactory {
        fail_exact_read: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade.diagnose_broker_health(request()).await.unwrap();
    let report = &result.data;

    assert!(result.partial);
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning == "diagnosis_evidence_incomplete"));
    assert_eq!(result.source_failures.len(), 2);
    assert_eq!(report.missing_evidence, ["broker_diagnostics", "broker_config"]);
    assert_eq!(
        evidence_item(report, "broker_diagnostics").status,
        EvidenceStatus::Unavailable
    );
    // The instance answers, and nothing that was collected says otherwise.
    assert_eq!(report.severity, Severity::Healthy);
    assert_eq!(counters.ha_status_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn broker_health_diagnosis_reports_a_broker_that_does_not_exist() {
    let factory = FakeSessionFactory {
        selected_broker_missing: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade.diagnose_broker_health(request()).await.unwrap();
    let report = &result.data;

    assert_eq!(report.severity, Severity::Unknown);
    assert_eq!(report.root_causes.len(), 1);
    assert_eq!(report.root_causes[0].rule_id, "target_not_found");
    assert!(report.recommendations[0].action.starts_with("Confirm the Broker name"));
    for id in ["broker_summary", "broker_diagnostics", "ha_status", "broker_config"] {
        let item = evidence_item(report, id);
        assert_eq!(item.status, EvidenceStatus::Missing, "item={id}");
        assert_eq!(item.error_code.as_deref(), Some("not_found"), "item={id}");
    }
    // A Broker that is not registered has no instance to ask anything of.
    assert_eq!(counters.broker_diagnostics_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.ha_status_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.broker_config_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn broker_health_diagnosis_times_out_within_the_request_budget_and_closes_its_session() {
    let factory = FakeSessionFactory {
        hang_broker_query: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let control = WorkflowControl::new(Duration::from_secs(10), CancellationToken::new());
    let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
    let started = tokio::time::Instant::now();

    let error = facade.diagnose_broker_health(request()).await.unwrap_err();

    assert!(matches!(
        error,
        ToolFailure::Rejected(crate::tools::executor::ToolRejection::TimedOut { timeout_ms: 10_000 })
    ));
    assert_eq!(started.elapsed(), Duration::from_secs(10));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn broker_health_diagnosis_closes_its_session_when_the_request_is_cancelled() {
    let factory = FakeSessionFactory {
        hang_broker_query: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let cancellation = CancellationToken::new();
    let control = WorkflowControl::new(Duration::from_secs(60), cancellation.clone());
    let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
    let waiting = counters.clone();
    tokio::spawn(async move {
        while waiting.broker_queries.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        cancellation.cancel();
    });

    let error = facade.diagnose_broker_health(request()).await.unwrap_err();

    assert!(matches!(
        error,
        ToolFailure::Rejected(crate::tools::executor::ToolRejection::Cancelled)
    ));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

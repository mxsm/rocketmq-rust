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

//! The cluster health check workflow: one session, and a report whatever a read does.

use super::*;
use crate::model::diagnosis::Severity;
use crate::tools::diagnosis_tools::GetClusterHealthArgs;

fn request() -> GetClusterHealthArgs {
    GetClusterHealthArgs {
        cluster: "local-dev".to_string(),
    }
}

#[tokio::test]
async fn cluster_health_reads_the_broker_rows_and_the_replication_in_one_session() {
    let factory = FakeSessionFactory::default();
    let counters = factory.counters.clone();
    let facade = QueryFacade::with_factory(example_config(), factory);

    let result = facade.cluster_health(request()).await.unwrap();
    let report = &result.data;

    assert!(!result.partial);
    assert_eq!(report.report_type, "cluster_health");
    // One master without a replica: fine for a development cluster, and worth saying.
    assert_eq!(report.severity, Severity::Low);
    assert_eq!(report.root_causes.len(), 1);
    assert_eq!(report.root_causes[0].rule_id, "master_without_replica");
    let inventory = evidence_data(report, "broker_inventory");
    assert_eq!(inventory["cluster"], "local-dev");
    assert_eq!(inventory["instance_count"], 1);
    assert_eq!(inventory["masters_without_replica"], serde_json::json!(["broker-a"]));
    assert_eq!(evidence_data(report, "ha_status")["master_count"], 1);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(counters.broker_queries.load(Ordering::SeqCst), 1);
    assert_eq!(counters.ha_status_queries.load(Ordering::SeqCst), 1);
    // The check looks at no Topic, no Consumer Group, and inside no Broker.
    assert_eq!(counters.topic_inventory_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.consumer_group_inventory_queries.load(Ordering::SeqCst), 0);
    assert_eq!(counters.broker_diagnostics_queries.load(Ordering::SeqCst), 0);

    // An identical request is answered from the cache.
    let replay = facade.cluster_health(request()).await.unwrap();
    assert_eq!(replay.cache_status, CacheStatus::Hit);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cluster_health_keeps_the_source_failures_of_the_broker_rows() {
    let facade = QueryFacade::with_factory(
        example_config(),
        FakeSessionFactory {
            partial_sources: true,
            ..Default::default()
        },
    );

    let result = facade.cluster_health(request()).await.unwrap();

    // One Broker did not answer the row read: the report is complete, the result is not.
    assert!(result.partial);
    assert_eq!(result.source_failures.len(), 1);
    assert_eq!(result.source_failures[0].logical_target, "broker-b");
    assert!(!result.data.partial);
}

#[tokio::test(start_paused = true)]
async fn cluster_health_times_out_within_the_request_budget_and_closes_its_session() {
    let factory = FakeSessionFactory {
        hang_broker_query: true,
        ..Default::default()
    };
    let counters = factory.counters.clone();
    let control = WorkflowControl::new(Duration::from_secs(10), CancellationToken::new());
    let facade = QueryFacade::with_factory_and_control(example_config(), factory, control);
    let started = tokio::time::Instant::now();

    let error = facade.cluster_health(request()).await.unwrap_err();

    assert!(matches!(
        error,
        ToolFailure::Rejected(crate::tools::executor::ToolRejection::TimedOut { timeout_ms: 10_000 })
    ));
    assert_eq!(started.elapsed(), Duration::from_secs(10));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cluster_health_closes_its_session_when_the_request_is_cancelled() {
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

    let error = facade.cluster_health(request()).await.unwrap_err();

    assert!(matches!(
        error,
        ToolFailure::Rejected(crate::tools::executor::ToolRejection::Cancelled)
    ));
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counters.shutdowns.load(Ordering::SeqCst), 1);
}

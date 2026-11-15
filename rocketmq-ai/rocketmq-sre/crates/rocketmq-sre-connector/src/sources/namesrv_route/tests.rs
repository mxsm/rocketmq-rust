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

use super::*;
use crate::sources::CancelSignal;
use rocketmq_sre_contracts::{
    ClusterId, CorrelationId, EvidenceContent, EvidenceQuery, EvidenceSnapshot, QueryId, TenantId, TimeRange,
    current_evidence_schema,
};
use std::collections::{BTreeSet, VecDeque};

struct Script {
    replies: VecDeque<NameserverObservationReply>,
    bounds: Vec<(u64, usize, usize)>,
}
impl NamesrvPeerReader for Script {
    async fn observe(
        &mut self,
        request: &NameserverObservationRequest,
    ) -> Result<NameserverObservationReply, ConnectorError> {
        self.bounds.push((
            request.bounds.timeout_millis,
            request.bounds.max_body_bytes,
            request.bounds.max_rows,
        ));
        Ok(self.replies.pop_front().expect("one reply per actual RPC"))
    }
}
fn view(cluster_test: bool) -> NameserverObservationReply {
    NameserverObservationReply {
        status: NameserverObservationStatus::Available,
        received_body_bytes: Some(100),
        rows: Some(4),
        view: Some(NameserverRouteView {
            cluster_test,
            order_message_enable: false,
            support_acting_master: false,
        }),
        route: None,
    }
}
fn route() -> NameserverObservationReply {
    let raw = json!({"brokerDatas":[{"cluster":"local","brokerName":"broker-a","brokerAddrs":{"0":"broker-a:10911"},"zoneName":null,"enableActingMaster":false}],
        "queueDatas":[{"brokerName":"broker-a","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicSysFlag":0}],"orderTopicConf":null,"filterServerTable":{},"topicQueueMappingByBroker":null});
    let route = rocketmq_admin_core::core::nameserver_route::decode_nameserver_route(
        &serde_json::to_vec(&raw).unwrap(),
        "local",
        "orders",
        1000,
    )
    .unwrap();
    NameserverObservationReply {
        status: NameserverObservationStatus::Available,
        received_body_bytes: Some(400),
        rows: Some(30),
        view: None,
        route: Some(route),
    }
}
fn limits() -> SourceLimits {
    SourceLimits {
        max_rows: 5000,
        max_bytes: 64 * 1024,
        max_time_range: std::time::Duration::from_secs(3600),
        max_deadline: std::time::Duration::from_secs(20),
        max_concurrency: 1,
        max_requests_per_minute: 100,
        cache_ttl: std::time::Duration::ZERO,
        label_allowlist: BTreeSet::new(),
        prometheus_cluster_label: "cluster".to_owned(),
    }
}
fn context(cancel: &CancelSignal) -> ReadContext<'_> {
    let now = Utc::now();
    ReadContext {
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        external_cluster: "local",
        subject: "test",
        correlation_id: CorrelationId::new(),
        time_range_start: now - chrono::Duration::minutes(10),
        time_range_end: now - chrono::Duration::seconds(1),
        deadline: now + chrono::Duration::seconds(20),
        cancel,
    }
}
fn script(replies: Vec<NameserverObservationReply>) -> Script {
    Script {
        replies: replies.into(),
        bounds: vec![],
    }
}

#[tokio::test]
async fn namesrv_two_rounds_share_byte_row_and_absolute_rpc_budget_without_private_bodies() {
    let cancel = CancelSignal::default();
    let mut context = context(&cancel);
    context.external_cluster = "visible-logical-alias";
    let config = NamesrvRouteConfig::fixture_for_tests(context.cluster_id);
    let limits = limits();
    let mut script = script(vec![
        view(false),
        view(false),
        route(),
        route(),
        view(false),
        view(false),
        route(),
        route(),
    ]);
    let output = collect(
        &mut script,
        &context,
        "orders",
        &config,
        &limits,
        std::time::Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(script.bounds.len(), 8);
    assert!(script.replies.is_empty());
    assert!(script.bounds.iter().all(|(millis, _, _)| *millis <= 5000));
    assert!(
        script
            .bounds
            .windows(2)
            .all(|pair| pair[1].1 < pair[0].1 && pair[1].2 < pair[0].2)
    );
    assert_eq!(output.content["comparison"]["state"], "compared_equal");
    assert_eq!(output.content["read_statistics"]["known_received_body_bytes"], 2000);
    assert_eq!(output.content["read_statistics"]["row_work"], 136);
    let encoded = serde_json::to_string(&output.content).unwrap();
    assert!(!encoded.contains("broker-a:10911"));
    assert!(!encoded.contains("ns-a:9876"));
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: context.correlation_id,
        tenant_id: context.tenant_id,
        cluster_id: context.cluster_id,
        source: "admin-query".to_owned(),
        resource: "namesrv-route/orders".to_owned(),
        time_range: TimeRange::new(context.time_range_start, context.time_range_end).unwrap(),
    };
    let mut snapshot = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        output.observed_at,
        EvidenceContent::Inline(output.content),
    )
    .unwrap();
    snapshot.exposure = output.exposure;
    snapshot.freshness_seconds = output.freshness_seconds;
    snapshot.partial = output.partial;
    snapshot.coverage = output.coverage;
    assert!(
        rocketmq_sre_core::diagnostics::packs::assess_namesrv_route_observation(
            &snapshot,
            Utc::now().timestamp_millis()
        )
        .is_ok()
    );
}

#[tokio::test]
async fn namesrv_cluster_test_or_invalid_configuration_never_enqueues_its_route() {
    let cancel = CancelSignal::default();
    let context = context(&cancel);
    let config = NamesrvRouteConfig::fixture_for_tests(context.cluster_id);
    let mut script = script(vec![view(true), view(false), route(), view(true), view(false), route()]);
    let output = collect(
        &mut script,
        &context,
        "orders",
        &config,
        &limits(),
        std::time::Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(script.bounds.len(), 6);
    assert_eq!(output.content["read_statistics"]["planned_route_rpcs"], 4);
    assert_eq!(output.content["read_statistics"]["started_route_rpcs"], 2);
    assert_eq!(
        output.content["rounds"][0]["peers"][0]["route"]["status"],
        "not_attempted"
    );
    assert_eq!(output.content["comparison"]["state"], "incomparable");
    assert!(
        output.content["comparison"]["differing_dimensions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn namesrv_whole_operation_bound_stops_further_reads_and_keeps_actual_counts() {
    let cancel = CancelSignal::default();
    let context = context(&cancel);
    let config = NamesrvRouteConfig::fixture_for_tests(context.cluster_id);
    let mut limits = limits();
    limits.max_rows = 10;
    let mut oversized = view(false);
    oversized.rows = Some(11);
    oversized.status = NameserverObservationStatus::RowLimit;
    let mut script = script(vec![oversized]);
    let output = collect(
        &mut script,
        &context,
        "orders",
        &config,
        &limits,
        std::time::Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(script.bounds.len(), 1);
    assert_eq!(output.content["read_statistics"]["row_work"], 11);
    assert_eq!(output.content["read_statistics"]["bounded"], true);
    assert_eq!(output.content["read_statistics"]["started_config_rpcs"], 1);
    assert_eq!(output.content["read_statistics"]["started_route_rpcs"], 0);
    assert_eq!(output.content["comparison"]["state"], "incomparable");
}

#[tokio::test]
async fn namesrv_cancelled_or_foreign_scope_has_no_rpc_and_no_fallback() {
    let cancel = CancelSignal::default();
    cancel.cancel();
    let context = context(&cancel);
    let mut script = script(vec![]);
    assert_eq!(
        collect(
            &mut script,
            &context,
            "orders",
            &NamesrvRouteConfig::fixture_for_tests(context.cluster_id),
            &limits(),
            std::time::Duration::from_secs(5)
        )
        .await
        .unwrap_err()
        .failure(),
        ConnectorFailure::QueryCancelled
    );
    assert!(script.bounds.is_empty());
    let config = NamesrvRouteConfig::fixture_for_tests(ClusterId::new());
    assert_eq!(
        collect(
            &mut script,
            &context,
            "orders",
            &config,
            &limits(),
            std::time::Duration::from_secs(5)
        )
        .await
        .unwrap_err()
        .failure(),
        ConnectorFailure::ClusterNotAllowed
    );
}

#[test]
fn namesrv_complete_response_envelope_respects_the_512_kib_boundary_without_truncation() {
    let now = Utc::now();
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        source: "admin-query".to_owned(),
        resource: "namesrv-route/orders".to_owned(),
        time_range: TimeRange::new(now, now).unwrap(),
    };
    let mut base = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        now,
        EvidenceContent::Inline(json!({"padding":""})),
    )
    .unwrap();
    base.exposure = EvidenceExposure::AdminRpc;
    let envelope = rocketmq_sre_contracts::ConnectorResponseEnvelope {
        schema: rocketmq_sre_contracts::SchemaVersion::new("rocketmq-sre.connector-channel", 1, 0),
        session_id: rocketmq_sre_contracts::ConnectorSessionId::from_uuid(base.evidence_id.as_uuid()),
        correlation_id: base.correlation_id,
        sequence: u64::MAX,
        evidence: Some(base.clone()),
        error_code: None,
        retryable: false,
    };
    let baseline = serde_json::to_vec(&envelope).unwrap().len();
    for extra in [0, 1] {
        let mut snapshot = base.clone();
        let EvidenceContent::Inline(content) = &mut snapshot.content else {
            panic!()
        };
        content["padding"] = json!("x".repeat(rocketmq_sre_contracts::CONNECTOR_RESPONSE_MAX_BYTES - baseline + extra));
        snapshot.content_hash = snapshot.compute_content_hash().unwrap();
        let result = bounded_capture(snapshot.clone());
        if extra == 0 {
            assert_eq!(result.unwrap(), snapshot);
        } else {
            assert_eq!(result.unwrap_err().failure(), ConnectorFailure::OutputTooLarge);
        }
    }
}

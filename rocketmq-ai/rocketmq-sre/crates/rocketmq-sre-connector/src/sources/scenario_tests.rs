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

//! Deterministic public-wire / read-adapter to Core regressions. These fixtures
//! exercise missing signal boundaries; they do not assert live qualification.

use chrono::{Duration as ChronoDuration, TimeZone};
use rocketmq_admin_core::core::broker::QueryBrokerDiagnosticsResult;
use rocketmq_sre_contracts::{CorrelationId, QueryId, TenantId, TimeRange};
use rocketmq_sre_core::diagnostics::{DiagnosticEngine, DiagnosticStatus, FindingOutcome, full_registry};
use serde_json::json;

use super::canonical::{BrokerDiagnosticProfile, CanonicalProjection, MetricDiagnosticProfile, RouteDiagnosticProfile};
use super::*;
use crate::{VerifiedCapability, WireEvidenceEnvelope};

struct WireFixture(Value);

impl McpGateway for WireFixture {
    async fn handshake(&self) -> Result<BTreeMap<String, VerifiedCapability>, ConnectorError> {
        Ok(BTreeMap::new())
    }
    async fn query(&self, cluster: &str, _: &EvidenceOperation) -> Result<WireEvidenceEnvelope, ConnectorError> {
        let schema = json!({"type":"object","required":["schema_version","request_id","cluster","observed_at","freshness_ms","cache_status","partial","warnings","data"],"properties":{"data":{"type":"object"}}});
        crate::wire::validate_wire_envelope(schema.as_object().unwrap(), self.0.clone(), cluster)
    }
    async fn close(&self) {}
}

fn at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 11, 10, 0, 0).single().unwrap()
}

async fn mcp(data: Value, operation: EvidenceOperation) -> SourceOutput {
    let gateway = Arc::new(WireFixture(json!({
        "schema_version":"rocketmq-mcp.v2","request_id":"scenario-request","cluster":"isolated",
        "observed_at":at(),"freshness_ms":0,"cache_status":"miss","partial":false,"warnings":[],"data":data
    })));
    McpSource::new(gateway)
        .query(
            "isolated",
            &operation,
            Utc::now() + ChronoDuration::seconds(5),
            &CancelSignal::default(),
        )
        .await
        .unwrap()
        .with_exposure(EvidenceExposure::McpTool)
}

fn broker() -> SourceOutput {
    // The read-only public Admin DTO preserves nullable HA and Store fields.
    let result: QueryBrokerDiagnosticsResult = serde_json::from_value(json!({
        "schema_version":"rocketmq.admin-broker-diagnostics.v1","observed_at_millis":at().timestamp_millis(),
        "brokers":[{"broker_name":"broker-a","broker_id":0,"coverage":"partial","store_health":null,
            "ha":{"supported":true,"role":"master","max_replica_lag_bytes":0,"required_ack_count":null},
            "rocksdb":{"supported":false},"tiered":{"configured":false},
            "auth":{"supported":false,"credential_rotation_supported":false}}],"unavailable_brokers":0,"partial":true
    }))
    .unwrap();
    let mut output = broker_store_diagnostics::project(result).unwrap();
    output.freshness_seconds = 0;
    output
}

fn evidence(source: &str, resource: &str, output: SourceOutput) -> EvidenceSnapshot {
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: "00000001-0000-0000-0000-000000000000".parse::<TenantId>().unwrap(),
        cluster_id: "00000002-0000-0000-0000-000000000000".parse::<ClusterId>().unwrap(),
        source: source.to_owned(),
        resource: resource.to_owned(),
        time_range: TimeRange::new(at() - ChronoDuration::minutes(5), at()).unwrap(),
    };
    match capture(query, output).unwrap() {
        ConnectorAdmissionOutcome::Accepted(snapshot) => snapshot,
        ConnectorAdmissionOutcome::Rejected(reason) => panic!("fixture rejected: {reason:?}"),
    }
}

fn inconclusive(pack: &str, samples: &[EvidenceSnapshot]) {
    let result = DiagnosticEngine::new(full_registry().unwrap())
        .evaluate(pack, samples)
        .unwrap();
    assert_eq!(result.status, DiagnosticStatus::Inconclusive, "{pack}: {result:?}");
    assert!(
        result
            .findings
            .iter()
            .all(|finding| finding.outcome == FindingOutcome::Inconclusive)
    );
}

#[test]
fn lag_rate_history_measures_source_intervals_and_leaves_late_observations_incomplete() {
    let previous = consumer_lag_sample(&json!({"total_lag":10,"consume_rate_per_sec":1}), at()).unwrap();
    let current = consumer_lag_sample(
        &json!({"total_lag":14,"consume_rate_per_sec":1}),
        at() + ChronoDuration::seconds(2),
    )
    .unwrap();
    let output = || {
        let mut output = SourceOutput::available(
            json!({"total_lag":14,"queue_skew_ratio":1,"consume_rate_per_sec":1}),
            at(),
        );
        output.partial = true;
        output.coverage = CoverageStatus::Partial;
        output.warnings = vec!["consumer_lag_rate_history_unavailable".to_owned()];
        output
    };
    let mut valid = output();
    augment_consumer_lag_rates(&mut valid, &previous, &current, false);
    assert_eq!(valid.content["lag_slope_per_min"], 120.0);
    assert_eq!(valid.content["produce_rate_per_sec"], 3.0);
    for late in [at(), at() - ChronoDuration::seconds(2)] {
        let late = consumer_lag_sample(&json!({"total_lag":14,"consume_rate_per_sec":1}), late).unwrap();
        let mut partial = output();
        augment_consumer_lag_rates(&mut partial, &previous, &late, false);
        assert!(partial.content.get("lag_slope_per_min").is_none());
        assert!(partial.content.get("produce_rate_per_sec").is_none());
        assert_eq!(partial.coverage, CoverageStatus::Partial);
    }
}

#[tokio::test]
async fn backlog_store_uses_public_lag_fields_without_inventing_rates_or_store_health() {
    let raw = mcp(
        json!({"topic":"orders","consumer_group":"billing","total_lag":40,"consume_tps":12.5,
        "items":[{"lag":10},{"lag":30}],"total_count":2,"has_more":false}),
        EvidenceOperation::ConsumerLag {
            topic: "orders".to_owned(),
            consumer_group: "billing".to_owned(),
            limit: None,
            cursor: None,
        },
    )
    .await;
    let lag = projection::apply(raw, CanonicalProjection::ConsumerLag).unwrap();
    assert_eq!(lag.content["queue_skew_ratio"], 1.5);
    assert!(lag.content.get("produce_rate_per_sec").is_none());
    let store = projection::apply(
        broker(),
        CanonicalProjection::BrokerDiagnostics(BrokerDiagnosticProfile::StorePressure),
    )
    .unwrap();
    assert!(store.content["brokers"][0]["store_health"].is_null());
    assert!(store.content.get("disk_pressure").is_none());
    let samples = [
        evidence("rocketmq-mcp", "consumer-lag/billing/orders", lag),
        evidence("admin-query", "store-pressure/broker-a", store),
    ];
    inconclusive("consumer-lag.v2", &samples);
    inconclusive("store-pressure.v1", &samples);
    let missing = evidence(
        "admin-query",
        "store-pressure/broker-a",
        SourceOutput::missing("admin-query"),
    );
    inconclusive("store-pressure.v1", &[missing]);
}

#[test]
fn latency_ha_keeps_histograms_and_nullable_ack_fields_partial_then_rejects_stale_inputs() {
    let mut metric = SourceOutput::available(json!({"schema_version":"rocketmq.prometheus-evidence.v1",
        "metric":"rocketmq_rpc_latency_count","series":[{"samples":[{"observed_at":at()-ChronoDuration::seconds(1),"value":2},{"observed_at":at(),"value":3}]}]}),at())
        .with_exposure(EvidenceExposure::PrometheusApi);
    metric.freshness_seconds = 0;
    let metric = projection::apply(
        metric,
        CanonicalProjection::MetricDiagnostics(MetricDiagnosticProfile::SendLatency),
    )
    .unwrap();
    assert_eq!(metric.content["summary"]["window_delta"], 1.0);
    assert!(metric.content.get("store_slow").is_none());
    let ha = projection::apply(
        broker(),
        CanonicalProjection::BrokerDiagnostics(BrokerDiagnosticProfile::BrokerHa),
    )
    .unwrap();
    assert_eq!(ha.content["brokers"][0]["ha"]["max_replica_lag_bytes"], 0);
    assert!(ha.content["brokers"][0]["ha"]["required_ack_count"].is_null());
    assert!(ha.content.get("sync_state_set_sufficient").is_none());
    let mut samples = [
        evidence("prometheus", "send-latency/orders", metric),
        evidence("admin-query", "broker-ha/broker-a", ha),
    ];
    inconclusive("send-latency.v1", &samples);
    inconclusive("broker-ha.v1", &samples);
    for sample in &mut samples {
        sample.freshness_seconds = 601;
    }
    let engine = DiagnosticEngine::new(full_registry().unwrap());
    for pack in ["send-latency.v1", "broker-ha.v1"] {
        let result = engine.evaluate(pack, &samples).unwrap();
        assert_eq!(result.status, DiagnosticStatus::Inconclusive);
        assert!(!result.missing_required_evidence.is_empty());
    }
}

#[tokio::test]
async fn route_and_failed_connection_observations_do_not_establish_disconnect_or_root_cause() {
    let raw = mcp(
        json!({"topic":"orders","read_queue_count":4,"write_queue_count":4,"broker_names":["broker-a"]}),
        EvidenceOperation::TopicDescribe {
            topic: "orders".to_owned(),
            limit: None,
            cursor: None,
        },
    )
    .await;
    let route = projection::apply(
        raw,
        CanonicalProjection::RouteDiagnostics(RouteDiagnosticProfile::NameServer),
    )
    .unwrap();
    assert_eq!(route.content["route_available"], true);
    assert!(route.content.get("route_age_seconds").is_none());
    let mut producer = SourceOutput::available(
        json!({"connections":[],"queried_broker_count":2,
        "failed_brokers":["broker-a","broker-b"],"truncated":false}),
        at(),
    )
    .with_exposure(EvidenceExposure::AdminRpc);
    producer.freshness_seconds = 0;
    let producer = projection::apply(producer, CanonicalProjection::ProducerConnectivity).unwrap();
    assert!(producer.content.get("connected").is_none());
    let samples = [
        evidence("rocketmq-mcp", "namesrv-route/orders", route),
        evidence("admin-query", "producer-connectivity/producer-a", producer),
    ];
    inconclusive("namesrv-route.v1", &samples);
    inconclusive("producer-connectivity.v1", &samples);
}

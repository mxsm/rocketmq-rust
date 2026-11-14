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
use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use rocketmq_sre_contracts::{
    ClusterId, CorrelationId, EvidenceContent, EvidenceSnapshot, QueryId, TenantId, TimeRange, current_evidence_schema,
};
use rocketmq_sre_core::diagnostics::packs::{LATENCY_GRID, assess_send_latency_observation};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Mutex, Notify};

#[derive(Clone)]
struct Fixture {
    raw: Value,
    increase: Value,
    mode: &'static str,
    requests: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
    calls: Arc<AtomicUsize>,
    waiting: Arc<Notify>,
}
fn frames(base: i64) -> (Value, Value) {
    let mut raw = vec![];
    let mut vector = vec![];
    for bound in LATENCY_GRID {
        let le = bound.map_or_else(|| "+Inf".to_owned(), |value| value.to_string());
        let positive = bound.is_none_or(|value| value >= 250.0);
        let metric = json!({"__name__":LATENCY_FAMILIES[0],"cluster":"physical","node_id":"broker-a","topic":"orders","le":le,"job":"actual-scrape","instance":"private:18080"});
        let values: Vec<_> = [1, 25, 50, 75, 99]
            .into_iter()
            .enumerate()
            .map(|(index, delta)| {
                json!([
                    (base + delta) as f64 / 1000.0,
                    if positive {
                        (index * 50).to_string()
                    } else {
                        "0".to_owned()
                    }
                ])
            })
            .collect();
        raw.push(json!({"metric":metric,"values":values}));
        let mut labels = metric.clone();
        labels.as_object_mut().unwrap().remove("__name__");
        vector.push(json!({"metric":labels,"value":[(base+100) as f64/1000.0,if positive {(200.0*99.0/98.0).to_string()}else{"0".to_owned()}]}));
    }
    (
        json!({"status":"success","data":{"resultType":"matrix","result":raw}}),
        json!({"status":"success","data":{"resultType":"vector","result":vector}}),
    )
}
async fn fixture(State(state): State<Fixture>, Query(parameters): Query<BTreeMap<String, String>>) -> Json<Value> {
    let call = state.calls.fetch_add(1, Ordering::SeqCst);
    state.requests.lock().await.push(parameters);
    if state.mode == "hold_second" && call == 1 {
        state.waiting.notify_one();
        std::future::pending::<()>().await;
    }
    let mut response = if call == 1 {
        state.increase.clone()
    } else {
        state.raw.clone()
    };
    if call == 2 {
        match state.mode {
            "changed" => response["data"]["result"][0]["values"][0][1] = json!("1"),
            "new_alias" => {
                let mut row = response["data"]["result"][0].clone();
                row["metric"]["__name__"] = json!(LATENCY_FAMILIES[1]);
                response["data"]["result"].as_array_mut().unwrap().push(row);
            }
            "warnings" => response["warnings"] = json!(["arbitrary private query text must not enter evidence"]),
            _ => response["data"]["result"].as_array_mut().unwrap().reverse(),
        }
    }
    Json(response)
}
async fn setup(mode: &'static str) -> (PrometheusSource, EvidenceQuery, Fixture, tokio::task::JoinHandle<()>) {
    let base = Utc::now().timestamp_millis() - 1000;
    let (raw, increase) = frames(base);
    let state = Fixture {
        raw,
        increase,
        mode,
        requests: Default::default(),
        calls: Default::default(),
        waiting: Default::default(),
    };
    let app = Router::new().fallback(get(fixture)).with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let source = PrometheusSource::new(
        Client::new(),
        Some(Url::parse(&format!("http://{address}/")).unwrap()),
        BTreeSet::from(["cluster".to_owned(), "topic".to_owned()]),
        std::time::Duration::from_secs(3600),
    )
    .with_cluster_label("cluster".to_owned())
    .unwrap();
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        source: "prometheus".to_owned(),
        resource: "send-latency/orders".to_owned(),
        time_range: TimeRange::new(
            DateTime::from_timestamp_millis(base).unwrap() + Duration::nanoseconds(500),
            DateTime::from_timestamp_millis(base + 100).unwrap() + Duration::nanoseconds(800),
        )
        .unwrap(),
    };
    (source, query, state, server)
}
fn request<'a>(
    query: &'a EvidenceQuery,
    cancel: &'a CancelSignal,
    policy: Option<&'a SendLatencyPolicy>,
) -> SendLatencyRequest<'a> {
    SendLatencyRequest {
        external_cluster: "physical",
        query,
        policy,
        max_rows: 2000,
        max_bytes: 256 * 1024,
        deadline: Utc::now() + Duration::seconds(5),
        cancel,
    }
}

#[tokio::test]
async fn closed_three_reads_preserve_actual_scrapes_ms_grid_scope_and_local_estimate() {
    let (source, query, state, server) = setup("ordered").await;
    let cancel = CancelSignal::default();
    let policy = SendLatencyPolicy {
        cluster_id: query.cluster_id,
        topic: "orders".to_owned(),
        p99_millis: 200,
    };
    let output = source
        .query_send_latency(request(&query, &cancel, Some(&policy)))
        .await
        .unwrap();
    assert_eq!(output.content["policy"]["status"], "exceeded");
    assert_eq!(output.content["segments"][4]["observation"]["estimate_millis"], 248.5);
    assert_eq!(output.content["http_statistics"]["physical_requests"], 3);
    assert!(output.partial);
    assert_eq!(output.coverage, CoverageStatus::Partial);
    assert!(!output.content.to_string().contains("private:18080"));
    let mut snapshot = EvidenceSnapshot::capture(
        query.clone(),
        current_evidence_schema(),
        output.observed_at,
        EvidenceContent::Inline(output.content),
    )
    .unwrap();
    snapshot.freshness_seconds = output.freshness_seconds;
    snapshot.partial = output.partial;
    snapshot.coverage = output.coverage;
    assert!(assess_send_latency_observation(&snapshot, snapshot.observed_at.timestamp_millis()).is_ok());
    let requests = state.requests.lock().await;
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0]["query"], requests[2]["query"]);
    assert_eq!(requests[0]["time"], requests[2]["time"]);
    let limits: Vec<usize> = requests.iter().map(|row| row["limit"].parse().unwrap()).collect();
    assert!(limits.windows(2).all(|pair| pair[1] < pair[0]));
    let timeouts: Vec<u64> = requests
        .iter()
        .map(|row| row["timeout"].strip_suffix("ms").unwrap().parse().unwrap())
        .collect();
    assert!(timeouts.windows(2).all(|pair| pair[1] <= pair[0]));
    assert!(timeouts.iter().all(|timeout| *timeout > 0 && *timeout <= 5000));
    assert!(requests[0]["query"].ends_with("[100ms]"));
    assert!(requests[1]["query"].starts_with("increase(rocketmq_send_message_latency_bucket{"));
    assert!(!requests[1]["query"].contains("sum("));
    assert!(
        requests
            .iter()
            .all(|row| row["query"].contains("cluster=\"physical\",topic=\"orders\""))
    );
    assert_eq!(
        requests[0]["time"],
        query
            .time_range
            .end
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn changing_reads_or_annotations_cannot_keep_a_previous_green_estimate() {
    for mode in ["changed", "new_alias", "warnings"] {
        let (source, query, _, server) = setup(mode).await;
        let output = source
            .query_send_latency(request(&query, &CancelSignal::default(), None))
            .await
            .unwrap();
        assert!(output.content["segments"][4]["observation"].is_null());
        assert_eq!(output.content["segments"][4]["availability"], "unknown");
        assert!(!output.content.to_string().contains("arbitrary private"));
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn all_three_responses_share_rows_bytes_and_cancellation_without_retry() {
    let (source, query, state, server) = setup("ordered").await;
    let cancel = CancelSignal::default();
    let mut input = request(&query, &cancel, None);
    input.max_rows = 100;
    let output = source.query_send_latency(input).await.unwrap();
    assert_eq!(output.content["segments"][4]["reason_code"], "source_response_bounded");
    assert!(output.content["segments"][4]["observation"].is_null());
    assert_eq!(state.calls.load(Ordering::SeqCst), 2);
    server.abort();
    let _ = server.await;

    let (source, query, state, server) = setup("ordered").await;
    let mut input = request(&query, &cancel, None);
    input.max_bytes = serde_json::to_vec(&state.raw).unwrap().len() * 2;
    assert_eq!(
        source.query_send_latency(input).await.unwrap_err().failure(),
        ConnectorFailure::OutputTooLarge
    );
    assert_eq!(state.calls.load(Ordering::SeqCst), 3);
    server.abort();
    let _ = server.await;

    let (source, query, state, server) = setup("hold_second").await;
    let (result, ()) = tokio::join!(source.query_send_latency(request(&query, &cancel, None)), async {
        state.waiting.notified().await;
        cancel.cancel();
    });
    assert_eq!(result.unwrap_err().failure(), ConnectorFailure::QueryCancelled);
    assert_eq!(state.calls.load(Ordering::SeqCst), 2);
    server.abort();
    let _ = server.await;
}

#[test]
fn raw_identity_proof_rejects_scope_missing_buckets_alias_extra_nodes_and_duplicate_scrapes() {
    let (raw, vector) = frames(1_700_000_000_000);
    for mode in [
        "wrong_cluster",
        "wrong_topic",
        "missing_bucket",
        "duplicate",
        "alias_extra",
        "missing_increase",
        "fractional_counter",
        "fractional_near_precision_limit",
        "counter_above_precision_limit",
    ] {
        let mut raw = raw.clone();
        let mut vector = vector.clone();
        match mode {
            "wrong_cluster" => raw["data"]["result"][0]["metric"]["cluster"] = json!("foreign"),
            "wrong_topic" => raw["data"]["result"][0]["metric"]["topic"] = json!("foreign"),
            "missing_bucket" => {
                raw["data"]["result"].as_array_mut().unwrap().remove(3);
                vector["data"]["result"].as_array_mut().unwrap().remove(3);
            }
            "duplicate" => {
                let row = raw["data"]["result"][0].clone();
                raw["data"]["result"].as_array_mut().unwrap().push(row);
            }
            "alias_extra" => {
                let mut row = raw["data"]["result"][0].clone();
                row["metric"]["__name__"] = json!(LATENCY_FAMILIES[1]);
                row["metric"]["node_id"] = json!("broker-b");
                raw["data"]["result"].as_array_mut().unwrap().push(row);
            }
            "missing_increase" => {
                vector["data"]["result"].as_array_mut().unwrap().pop();
            }
            "fractional_counter" => raw["data"]["result"][0]["values"][0][1] = json!("1.5"),
            "fractional_near_precision_limit" => raw["data"]["result"][0]["values"][0][1] = json!("9007199254740990.5"),
            "counter_above_precision_limit" => raw["data"]["result"][0]["values"][0][1] = json!("9007199254740993"),
            _ => unreachable!(),
        }
        let mut budget = Budget::new(5000, 1000000);
        let result = RawSet::parse(&raw, "cluster", "physical", "orders", &mut budget).and_then(|raw| {
            let increases =
                proof::parse_increases(&vector, "cluster", "physical", "orders", 1_700_000_000_100, &mut budget)?;
            let histograms = select_histograms(&raw, raw.selected_family().unwrap(), increases)?;
            assess_latency_histograms(1_700_000_000_000, 1_700_000_000_100, &histograms).map_err(ReadIssue::Histogram)
        });
        assert!(result.is_err(), "{mode}");
    }
}

#[test]
fn prometheus_decimal_timestamp_is_exact_at_ms_and_rejects_sub_ms_or_overflow() {
    for (value, expected) in [
        (json!(1700000000.123), 1_700_000_000_123),
        (json!(1700000000), 1_700_000_000_000),
        (json!(-0.001), -1),
    ] {
        assert_eq!(proof::timestamp_millis(&value).unwrap(), expected);
    }
    for value in [json!(1700000000.1234), json!(1e100), json!("1700000000"), Value::Null] {
        assert!(proof::timestamp_millis(&value).is_err());
    }
}

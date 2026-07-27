// Copyright 2023 The RocketMQ Rust Authors
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

const METRICS: &str = include_str!("../../../../config/observability/sre/metrics.v1.yaml");
const SPANS: &str = include_str!("../../../../config/observability/sre/spans.v1.yaml");
const HEALTH: &str = include_str!("../../../../config/observability/sre/health.v1.yaml");
const DASHBOARD: &str =
    include_str!("../../../../deploy/dev/observability/grafana/dashboards/rocketmq-sre-phase1.json");

#[test]
fn committed_observability_manifests_parse_and_match_runtime_names() {
    let metrics: serde_yaml::Value = serde_yaml::from_str(METRICS).expect("metrics manifest should parse");
    let spans: serde_yaml::Value = serde_yaml::from_str(SPANS).expect("span manifest should parse");
    let health: serde_yaml::Value = serde_yaml::from_str(HEALTH).expect("health manifest should parse");

    assert_eq!(
        metrics["schema_version"].as_str(),
        Some("rocketmq.sre.observability-metrics.v1")
    );
    assert_eq!(
        spans["schema_version"].as_str(),
        Some("rocketmq.sre.observability-spans.v1")
    );
    assert_eq!(
        health["response_schema"].as_str(),
        Some(SreHealthViewV1::SCHEMA_VERSION)
    );

    let rendered = SreMetrics::new().render_prometheus();
    for metric in metrics["metrics"].as_sequence().expect("metrics must be a sequence") {
        let name = metric["name"].as_str().expect("metric name must be text");
        assert!(rendered.contains(name), "runtime metric `{name}` is missing");
    }
    for name in [
        SPAN_INCIDENT_RUN,
        SPAN_EVIDENCE_COLLECT,
        SPAN_DIAGNOSTIC_EVALUATE,
        SPAN_MODEL_INVOKE,
    ] {
        assert!(SPANS.contains(name), "span manifest is missing `{name}`");
    }
}

#[test]
fn grafana_dashboard_is_valid_and_uses_only_canonical_metric_prefix() {
    let dashboard: serde_json::Value = serde_json::from_str(DASHBOARD).expect("Grafana dashboard should parse");
    let panels = dashboard["panels"]
        .as_array()
        .expect("dashboard panels should be an array");

    assert!(panels.len() >= 8);
    for panel in panels {
        for target in panel["targets"].as_array().into_iter().flatten() {
            let expression = target["expr"].as_str().expect("PromQL expression should be text");
            assert!(
                expression.contains("rocketmq_sre_"),
                "dashboard expression must use canonical SRE metrics"
            );
            for forbidden in [
                "tenant_id",
                "cluster_id",
                "incident_id",
                "evidence_id",
                "connector_id",
                "prompt",
                "tool_arguments",
                "token=",
                "secret",
            ] {
                assert!(
                    !expression.contains(forbidden),
                    "dashboard expression contains forbidden label `{forbidden}`"
                );
            }
        }
    }
}

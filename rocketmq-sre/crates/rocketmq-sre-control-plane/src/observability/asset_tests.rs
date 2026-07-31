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
const SELF_SLO: &str = include_str!("../../../../config/operations/ai-sre-self-slo.v1.yaml");
const SELF_ALERTS: &str = include_str!("../../../../config/operations/ai-sre-self-alerts.v1.yaml");
const SELF_DEGRADATION: &str = include_str!("../../../../config/operations/ai-sre-degradation-policy.v1.yaml");
const SELF_RUNBOOK: &str = include_str!("../../../../docs/ai-sre-self-operations.md");

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

#[test]
fn self_operations_assets_cover_slos_alerts_runbooks_and_fail_closed_degradation() {
    let slo: serde_yaml::Value = serde_yaml::from_str(SELF_SLO).expect("self SLO catalog should parse");
    let alerts: serde_yaml::Value = serde_yaml::from_str(SELF_ALERTS).expect("self alert rules should parse");
    let degradation: serde_yaml::Value =
        serde_yaml::from_str(SELF_DEGRADATION).expect("self degradation policy should parse");

    assert_eq!(slo["schema_version"].as_str(), Some("rocketmq.sre.self-slo.v1"));
    let services = slo["services"].as_sequence().expect("self SLO services");
    let service_ids = services
        .iter()
        .map(|service| service["id"].as_str().expect("service id"))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        service_ids,
        [
            "connector",
            "control-plane",
            "execution-agent",
            "executor",
            "outbox",
            "postgresql",
            "probe",
            "provider-adapter",
        ]
        .into_iter()
        .collect()
    );
    for service in services {
        let runbook = service["runbook"].as_str().expect("service runbook");
        assert!(
            SELF_RUNBOOK.contains(&format!("## {runbook}")),
            "runbook `{runbook}` is not documented"
        );
        assert!(
            service["failure_mode"].as_str().is_some(),
            "every service needs an explicit failure mode"
        );
    }

    let rendered_alerts = serde_yaml::to_string(&alerts).expect("alert rules should serialize");
    let required_signals = slo["required_operational_signals"]
        .as_sequence()
        .expect("required operational signals");
    assert_eq!(required_signals.len(), 10);
    for signal in required_signals {
        let signal = signal.as_str().expect("operational signal name");
        assert!(
            rendered_alerts.contains(signal),
            "required operational signal `{signal}` has no alert or recording rule"
        );
    }
    let groups = alerts["spec"]["groups"].as_sequence().expect("Prometheus rule groups");
    let mut alert_count = 0;
    for rule in groups
        .iter()
        .flat_map(|group| group["rules"].as_sequence().into_iter().flatten())
    {
        if rule["alert"].as_str().is_some() {
            alert_count += 1;
            let runbook = rule["annotations"]["runbook"].as_str().expect("alert runbook");
            assert!(SELF_RUNBOOK.contains(&format!("## {runbook}")));
            assert!(rule["annotations"]["safety_response"].as_str().is_some());
        }
    }
    assert!(alert_count >= 14);

    assert_eq!(degradation["default_decision"].as_str(), Some("deny_new_mutation"));
    assert_eq!(
        degradation["rocketmq_data_plane"]["dependency_on_ai_sre"].as_str(),
        Some("none")
    );
    let dependency_failures = degradation["dependency_failures"]
        .as_sequence()
        .expect("dependency failure matrix");
    assert_eq!(dependency_failures.len(), 9);
    let allowed_recovery = degradation["automatic_recovery_allowlist"]
        .as_sequence()
        .expect("automatic recovery allowlist");
    assert_eq!(allowed_recovery.len(), 3);
    let serialized_allowlist = serde_yaml::to_string(allowed_recovery).expect("allowlist should serialize");
    for forbidden in [
        "postgresql",
        "restart_executor",
        "restart_execution_agent",
        "clear_quarantine",
        "advance_or_override_fence",
        "mutate_rocketmq_resource",
        "execute_shell_or_raw_request",
    ] {
        assert!(
            !serialized_allowlist.contains(forbidden),
            "automatic recovery allowlist contains forbidden capability `{forbidden}`"
        );
    }
    let denylist =
        serde_yaml::to_string(&degradation["automatic_recovery_denylist"]).expect("denylist should serialize");
    for required in [
        "restart_or_failover_postgresql",
        "restart_executor",
        "restart_execution_agent",
        "clear_quarantine",
        "advance_or_override_fence",
        "mutate_rocketmq_resource",
        "execute_shell_or_raw_request",
    ] {
        assert!(
            denylist.contains(required),
            "automatic recovery denylist is missing `{required}`"
        );
    }
    assert_eq!(slo["monthly_report"]["period_kind"].as_str(), Some("month"));
}

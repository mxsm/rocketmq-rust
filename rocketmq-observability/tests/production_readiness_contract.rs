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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use rocketmq_observability::metrics::catalog;
use rocketmq_observability::semantic;
use rocketmq_observability::TelemetryDropReason;
use rocketmq_observability::TelemetryEnqueueOutcome;
use rocketmq_observability::TelemetryOutageQueue;
use rocketmq_observability::TelemetryQueueLimits;
use rocketmq_observability::DEFAULT_MAX_QUEUE_BYTES;
use rocketmq_observability::DEFAULT_MAX_QUEUE_ITEMS;
use rocketmq_observability::DEFAULT_MAX_RECORD_BYTES;
use serde_json::Value;

const POLICY_PATH: &str = "distribution/config/architecture-production-readiness-policy.json";
const REGISTRY_PATH: &str = "scripts/telemetry-semantic-registry.json";
const DASHBOARD_PATH: &str = "distribution/config/grafana-architecture-production-readiness.json";
const ALERTS_PATH: &str = "distribution/config/prometheus-architecture-production-readiness-alerts.yaml";
const RUNBOOK_PATH: &str = "rocketmq-doc/en/architecture-production-readiness-runbook.md";

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("observability crate must remain below the repository root")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    fs::read_to_string(repository_root().join(relative))
        .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

fn json(relative: &str) -> Value {
    serde_json::from_str(&read(relative)).unwrap_or_else(|error| panic!("failed to parse {relative}: {error}"))
}

fn alerts() -> Value {
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&read(ALERTS_PATH)).expect("production readiness alerts must be valid YAML");
    serde_json::to_value(yaml).expect("string-keyed alert YAML must convert to JSON")
}

fn registry_signals(registry: &Value) -> BTreeMap<&str, &Value> {
    registry["signals"]
        .as_array()
        .expect("registry signals")
        .iter()
        .map(|signal| (signal["id"].as_str().expect("registry signal id"), signal))
        .collect()
}

fn dashboard_panels(dashboard: &Value) -> BTreeMap<&str, &Value> {
    dashboard["panels"]
        .as_array()
        .expect("dashboard panels")
        .iter()
        .map(|panel| (panel["title"].as_str().expect("dashboard panel title"), panel))
        .collect()
}

fn alert_rules(alerts: &Value) -> BTreeMap<&str, &Value> {
    alerts["groups"]
        .as_array()
        .expect("alert groups")
        .iter()
        .flat_map(|group| group["rules"].as_array().expect("alert rules"))
        .map(|rule| (rule["alert"].as_str().expect("alert name"), rule))
        .collect()
}

fn compact_promql(query: &str) -> String {
    query.chars().filter(|character| !character.is_whitespace()).collect()
}

fn metric_names(query: &str) -> BTreeSet<String> {
    query
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|token| token.starts_with("rocketmq_"))
        .map(|token| {
            token
                .strip_suffix("_bucket")
                .or_else(|| token.strip_suffix("_count"))
                .or_else(|| token.strip_suffix("_sum"))
                .unwrap_or(token)
                .to_string()
        })
        .collect()
}

fn markdown_anchors(runbook: &str) -> BTreeSet<String> {
    runbook
        .lines()
        .filter_map(|line| line.strip_prefix("## "))
        .map(|heading| {
            let mut anchor = String::new();
            let mut previous_hyphen = false;
            for character in heading.chars().flat_map(char::to_lowercase) {
                if character.is_ascii_alphanumeric() {
                    anchor.push(character);
                    previous_hyphen = false;
                } else if !previous_hyphen {
                    anchor.push('-');
                    previous_hyphen = true;
                }
            }
            anchor.trim_matches('-').to_string()
        })
        .collect()
}

fn runbook_alert_section<'a>(runbook: &'a str, alert: &str) -> &'a str {
    let start = runbook
        .find(&format!("Alert: `{alert}`"))
        .unwrap_or_else(|| panic!("runbook route missing for {alert}"));
    let remainder = &runbook[start..];
    let end = remainder.find("\n## ").unwrap_or(remainder.len());
    &remainder[..end]
}

#[test]
fn objectives_dashboard_alerts_and_runbook_share_one_contract() {
    let policy = json(POLICY_PATH);
    let registry = json(REGISTRY_PATH);
    let dashboard = json(DASHBOARD_PATH);
    let alerts = alerts();
    let runbook = read(RUNBOOK_PATH);
    let signals = registry_signals(&registry);
    let panels = dashboard_panels(&dashboard);
    let rules = alert_rules(&alerts);
    let anchors = markdown_anchors(&runbook);

    assert_eq!(policy["contract_version"], "1.0.0");
    for objective in policy["objectives"].as_array().expect("readiness objectives") {
        let id = objective["id"].as_str().expect("objective id");
        let source = objective["source"].as_str().expect("objective source");
        let owner = objective["owner"].as_str().expect("objective owner");
        assert_eq!(
            objective["allowed_query_labels"].as_array().map(Vec::len),
            Some(0),
            "{id}: production objective queries must aggregate high-cardinality labels"
        );

        let panel_title = objective["dashboard_panel"].as_str().expect("dashboard panel");
        let panel = panels
            .get(panel_title)
            .unwrap_or_else(|| panic!("{id}: dashboard panel missing"));
        if let Some(unit) = objective["dashboard_unit"].as_str() {
            assert_eq!(
                panel["fieldConfig"]["defaults"]["unit"], unit,
                "{id}: dashboard unit drift"
            );
            let threshold_values = panel["fieldConfig"]["defaults"]["thresholds"]["steps"]
                .as_array()
                .expect("live panel thresholds")
                .iter()
                .map(|step| &step["value"])
                .collect::<Vec<_>>();
            assert!(
                threshold_values.contains(&&objective["target"]),
                "{id}: dashboard threshold drift"
            );
        }

        let Some(query) = objective["query"].as_str() else {
            assert_eq!(source, "fault_evidence");
            assert!(anchors.contains(objective["runbook_anchor"].as_str().expect("runbook anchor")));
            continue;
        };
        let query_metrics = metric_names(query);
        assert!(query_metrics.contains(source), "{id}: source is absent from its query");
        for metric in &query_metrics {
            assert!(
                signals.contains_key(metric.as_str()),
                "{id}: unregistered metric {metric}"
            );
        }
        let source_signal = signals.get(source).expect("objective source signal");
        assert_eq!(
            source_signal["unit"], objective["source_unit"],
            "{id}: source metric unit drift"
        );
        assert_eq!(source_signal["owner"], owner, "{id}: metric owner drift");

        let panel_query = panel["targets"]
            .as_array()
            .expect("panel targets")
            .iter()
            .filter_map(|target| target["expr"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            query_metrics.is_subset(&metric_names(&panel_query)),
            "{id}: dashboard query does not cover the policy metrics"
        );

        let alert = objective["alert"].as_str().expect("live objective alert");
        let rule = rules.get(alert).unwrap_or_else(|| panic!("{id}: alert rule missing"));
        let alert_query = compact_promql(rule["expr"].as_str().expect("alert expression"));
        assert!(
            alert_query.contains(&compact_promql(query)),
            "{id}: alert expression drifted from policy query"
        );
        let comparison = match objective["operator"].as_str().expect("comparison operator") {
            "gte" => "<",
            "lte" => ">",
            operator => panic!("{id}: unsupported operator {operator}"),
        };
        let target = objective["target"].to_string();
        assert!(
            alert_query.contains(&format!("{comparison}{target}")),
            "{id}: alert threshold drift"
        );
        assert_eq!(rule["labels"]["contract"], "production-readiness-v1");
        assert_eq!(rule["labels"]["owner"], owner, "{id}: alert owner drift");

        let anchor = objective["runbook_anchor"].as_str().expect("runbook anchor");
        assert!(anchors.contains(anchor), "{id}: runbook anchor missing");
        assert!(
            rule["annotations"]["runbook_url"]
                .as_str()
                .expect("runbook URL")
                .ends_with(&format!("#{anchor}")),
            "{id}: alert runbook route drift"
        );
        let section = runbook_alert_section(&runbook, alert);
        for action in ["Diagnose", "Contain", "Recover", "Escalate"] {
            assert!(section.contains(action), "{id}: runbook action missing: {action}");
        }
    }
}

#[test]
fn release_identity_is_low_cardinality_and_consistent() {
    let policy = json(POLICY_PATH);
    let registry = json(REGISTRY_PATH);
    let dashboard = json(DASHBOARD_PATH);
    let alerts = alerts();
    let runbook = read(RUNBOOK_PATH);
    let contract = &policy["release_identity_contract"];
    let metric = contract["metric"].as_str().expect("release identity metric");
    let signals = registry_signals(&registry);
    let signal = signals.get(metric).copied().expect("release identity registry signal");
    let allowed_labels = contract["allowed_labels"]
        .as_array()
        .expect("release identity labels")
        .iter()
        .map(|label| label.as_str().expect("release identity label"))
        .collect::<Vec<_>>();

    assert_eq!(metric, semantic::metrics::RELEASE_INFO);
    assert_eq!(allowed_labels, ["service", "release_commit", "release_nonce"]);
    assert_eq!(signal["kind"], contract["kind"]);
    assert_eq!(signal["unit"], contract["unit"]);
    assert_eq!(signal["owner"], contract["owner"]);
    assert_eq!(signal["family"], "release-identity");
    assert_eq!(signal["attributes"], contract["allowed_labels"]);
    assert_eq!(signal["cardinality_budget"], contract["cardinality_budget"]);
    assert_eq!(signal["privacy"], contract["privacy"]);
    for label in &allowed_labels {
        assert!(
            !["password", "credential", "secret", "token", "message_body"].contains(label),
            "sensitive release identity label: {label}"
        );
    }

    let descriptor = catalog::rust_metrics()
        .iter()
        .find(|descriptor| descriptor.name == metric)
        .expect("release identity catalog descriptor");
    assert_eq!(descriptor.unit, contract["unit"].as_str().expect("identity unit"));
    assert_eq!(descriptor.labels, allowed_labels);

    let panels = dashboard_panels(&dashboard);
    let panel = panels
        .get(contract["dashboard_panel"].as_str().expect("identity panel"))
        .copied()
        .expect("release identity dashboard panel");
    let panel_query = panel["targets"][0]["expr"].as_str().expect("identity panel query");
    assert!(metric_names(panel_query).contains(metric));
    assert!(compact_promql(panel_query).contains("by(service,release_commit,release_nonce)"));

    let alert = contract["alert"].as_str().expect("identity alert");
    let rules = alert_rules(&alerts);
    let rule = rules.get(alert).copied().expect("release identity alert rule");
    assert!(metric_names(rule["expr"].as_str().expect("identity alert query")).contains(metric));
    assert_eq!(rule["labels"]["owner"], contract["owner"]);
    let section = runbook_alert_section(&runbook, alert);
    for action in ["Diagnose", "Contain", "Recover", "Escalate"] {
        assert!(
            section.contains(action),
            "release identity runbook action missing: {action}"
        );
    }
}

#[test]
fn five_services_publish_readiness_from_complete_evidence() {
    let policy = json(POLICY_PATH);
    let contract = &policy["readiness_contract"];
    assert_eq!(contract["probe_path"], "/readyz");
    assert_eq!(contract["ready_status"], 200);
    assert_eq!(contract["not_ready_status"], 503);
    assert_eq!(contract["identity_required_when_metrics_enabled"], true);

    let services = contract["services"].as_array().expect("readiness services");
    let service_names = services
        .iter()
        .map(|service| service["service"].as_str().expect("service name"))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        service_names,
        BTreeSet::from(["broker", "controller", "mcp", "namesrv", "proxy"])
    );

    for service in services {
        let name = service["service"].as_str().expect("readiness service");
        let requirements = service["requirements"]
            .as_array()
            .expect("readiness requirements")
            .iter()
            .map(|requirement| requirement.as_str().expect("readiness requirement"))
            .collect::<BTreeSet<_>>();
        assert!(
            requirements.contains("security_bootstrap"),
            "{name}: security readiness missing"
        );
        assert!(
            requirements.contains("release_identity"),
            "{name}: release identity readiness missing"
        );
        assert_eq!(
            service["identity_service"],
            format!("rocketmq-{name}"),
            "{name}: release identity service drift"
        );

        for source_contract in service["source_contracts"].as_array().expect("source contracts") {
            let relative = source_contract["path"].as_str().expect("source contract path");
            assert!(
                !Path::new(relative).is_absolute(),
                "{name}: source path must be repository-relative"
            );
            let source = read(relative);
            for marker in source_contract["required_markers"].as_array().expect("source markers") {
                let marker = marker.as_str().expect("source marker");
                assert!(
                    source.contains(marker),
                    "{name}: source marker missing from {relative}: {marker}"
                );
            }
        }
    }

    let lifecycle = read("rocketmq-runtime/src/service_lifecycle.rs");
    for marker in [
        r#""/readyz""#,
        "probe_response(200",
        "probe_response(503",
        "pub fn mark_ready",
    ] {
        assert!(
            lifecycle.contains(marker),
            "service lifecycle readiness marker missing: {marker}"
        );
    }
}

#[test]
fn collector_outage_is_bounded_measurable_and_non_blocking() {
    let policy = json(POLICY_PATH);
    let registry = json(REGISTRY_PATH);
    let contract = &policy["collector_outage_contract"];
    let outage = &registry["outage_policy"];

    assert_eq!(
        contract["max_queue_items"].as_u64(),
        Some(DEFAULT_MAX_QUEUE_ITEMS as u64)
    );
    assert_eq!(
        contract["max_queue_bytes"].as_u64(),
        Some(DEFAULT_MAX_QUEUE_BYTES as u64)
    );
    assert_eq!(
        contract["max_record_bytes"].as_u64(),
        Some(DEFAULT_MAX_RECORD_BYTES as u64)
    );
    for field in [
        "max_queue_items",
        "max_queue_bytes",
        "max_record_bytes",
        "enqueue",
        "overflow",
        "data_plane_blocking",
        "drop_signal",
        "shutdown_signal",
    ] {
        assert_eq!(contract[field], outage[field], "collector outage field drift: {field}");
    }
    assert_eq!(contract["recovery_objective_seconds"], 30);
    assert_eq!(contract["data_plane_blocking"], false);
    let measurements = outage["measurements"]
        .as_array()
        .expect("outage measurements")
        .iter()
        .map(|measurement| measurement.as_str().expect("outage measurement"))
        .collect::<BTreeSet<_>>();
    for required in contract["required_measurements"]
        .as_array()
        .expect("required outage measurements")
    {
        let required = required.as_str().expect("required outage measurement");
        assert!(
            measurements.contains(required),
            "missing collector outage measurement: {required}"
        );
    }
    let signals = registry_signals(&registry);
    assert!(signals.contains_key(contract["drop_signal"].as_str().expect("drop signal")));
    assert!(signals.contains_key(contract["shutdown_signal"].as_str().expect("shutdown signal")));

    let queue = TelemetryOutageQueue::new(TelemetryQueueLimits::new(1, 8, 8).expect("bounded test limits"));
    assert_eq!(queue.try_enqueue("first", 4), TelemetryEnqueueOutcome::Accepted);
    assert_eq!(
        queue.try_enqueue("second", 4),
        TelemetryEnqueueOutcome::Dropped(TelemetryDropReason::ItemLimit)
    );
    let snapshot = queue.snapshot();
    assert_eq!(snapshot.queued_items, 1);
    assert_eq!(snapshot.queued_bytes, 4);
    assert_eq!(snapshot.accepted_items, 1);
    assert_eq!(snapshot.dropped_items, 1);
}

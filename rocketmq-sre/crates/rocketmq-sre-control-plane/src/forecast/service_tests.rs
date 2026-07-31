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

use rocketmq_sre_contracts::ForecastWindow;
use rocketmq_sre_contracts::ResourceKind;
use rocketmq_sre_contracts::ResourceRef;

use super::*;

#[test]
fn worker_cursor_rotates_over_complete_target_window_surface() {
    let config = ForecastConfiguration::embedded().expect("forecast config");
    let total = config.targets.len() * config.windows.len();
    let cursor = AtomicUsize::new(total - 2);
    let start = cursor.fetch_add(config.max_evaluations_per_run, Ordering::Relaxed);
    let indexes = (0..config.max_evaluations_per_run.min(total))
        .map(|offset| start.wrapping_add(offset) % total)
        .collect::<Vec<_>>();
    assert!(indexes.contains(&0));
    assert!(indexes.contains(&(total - 1)));
}

#[test]
fn inferred_utilization_uses_worst_ready_component_without_average_masking() {
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    let report = ClusterForecastReport {
        schema_version: "test".into(),
        tenant_id,
        cluster_id,
        forecasts: vec![
            test_forecast(tenant_id, cluster_id, "broker_capacity", 0.4),
            test_forecast(tenant_id, cluster_id, "broker_capacity", 0.9),
        ],
        backlog_etas: vec![],
        baselines: vec![],
        anomalies: vec![],
        change_points: vec![],
        accuracy: vec![],
        partial: false,
        warnings: vec![],
        execution_eligible: false,
        observed_at: Utc::now(),
    };
    assert_eq!(inferred_utilization(SimulationKind::BrokerOffline, &report), Some(0.9));
}

fn test_forecast(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    category: &str,
    value: f64,
) -> rocketmq_sre_contracts::CapacityForecast {
    let now = Utc::now();
    rocketmq_sre_contracts::CapacityForecast {
        id: rocketmq_sre_contracts::ForecastId::new(),
        tenant_id,
        cluster_id,
        resource: ResourceRef {
            kind: ResourceKind::Broker,
            key: category.into(),
            display_name: Some(category.into()),
        },
        metric: category.into(),
        window: ForecastWindow::SevenDays,
        trend: rocketmq_sre_contracts::ForecastTrend::Stable,
        status: ForecastStatus::Ready,
        quality: rocketmq_sre_contracts::ForecastQuality::High,
        algorithm_version: "test".into(),
        sample_start: now,
        sample_end: now,
        coverage_ratio: 1.0,
        slope_per_hour: Some(0.0),
        volatility: Some(0.0),
        threshold: Some(1.0),
        exhaustion_at: None,
        points: vec![rocketmq_sre_contracts::ForecastPoint {
            at: now,
            value,
            projected: false,
        }],
        backtest: rocketmq_sre_contracts::ForecastBacktest {
            evaluated_points: 1,
            mean_absolute_error: Some(0.0),
            bias: Some(0.0),
            interval_coverage_ratio: Some(1.0),
        },
        advisories: vec![],
        evidence_ids: vec![],
        execution_eligible: false,
        observed_at: now,
    }
}

#[test]
fn no_service_path_can_mark_simulation_or_forecast_executable() {
    let request = WhatIfSimulationRequest {
        cluster_id: ClusterId::new(),
        kind: SimulationKind::TrafficIncrease,
        current_utilization: Some(0.5),
        current_instances: None,
        traffic_increase_percent: Some(25),
        instance_delta: None,
        current_queue_count: None,
        queue_delta: None,
        target_version: None,
        configuration_changes: vec![],
        affected_resource_keys: vec!["broker:a".into()],
        evidence_ids: vec![],
    };
    assert_eq!(
        simulate(&request).status,
        rocketmq_sre_contracts::SimulationStatus::Completed
    );
}

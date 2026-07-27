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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_observability::ObservabilityStatusViewV1;
use serde::Serialize;

use crate::observability::ProviderFamilyLabel;

/// Coarse health states ordered from least to most severe.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyStatus {
    Healthy,
    Unknown,
    Degraded,
    Unavailable,
}

impl DependencyStatus {
    const fn severity(self) -> u8 {
        match self {
            Self::Healthy => 0,
            Self::Unknown => 1,
            Self::Degraded => 2,
            Self::Unavailable => 3,
        }
    }

    const fn is_serving(self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }
}

/// Stable reason codes; free-form dependency errors are intentionally excluded
/// from the authenticated health response.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthReasonCode {
    AuthenticationFailed,
    CapabilityMismatch,
    ConnectionFailed,
    HeartbeatStale,
    NotConfigured,
    PoolExhausted,
    QueryFailed,
    RateLimited,
    Timeout,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
pub struct DatabaseHealthSample {
    pub status: DependencyStatus,
    pub latency_ms: Option<u64>,
    pub active_connections: u32,
    pub idle_connections: u32,
    pub max_connections: u32,
    pub reason: Option<HealthReasonCode>,
}

impl DatabaseHealthSample {
    #[must_use]
    pub const fn healthy(
        latency_ms: u64,
        active_connections: u32,
        idle_connections: u32,
        max_connections: u32,
    ) -> Self {
        Self {
            status: DependencyStatus::Healthy,
            latency_ms: Some(latency_ms),
            active_connections,
            idle_connections,
            max_connections,
            reason: None,
        }
    }

    #[must_use]
    pub const fn unavailable(reason: HealthReasonCode) -> Self {
        Self {
            status: DependencyStatus::Unavailable,
            latency_ms: None,
            active_connections: 0,
            idle_connections: 0,
            max_connections: 0,
            reason: Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProviderHealthSample {
    pub family: ProviderFamilyLabel,
    pub status: DependencyStatus,
    pub latency_ms: Option<u64>,
    pub reason: Option<HealthReasonCode>,
}

impl ProviderHealthSample {
    #[must_use]
    pub const fn new(
        family: ProviderFamilyLabel,
        status: DependencyStatus,
        latency_ms: Option<u64>,
        reason: Option<HealthReasonCode>,
    ) -> Self {
        Self {
            family,
            status,
            latency_ms,
            reason,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ConnectorHealthSample {
    pub status: DependencyStatus,
    pub heartbeat_age_seconds: Option<u64>,
    pub queryable_sources: u16,
    pub reason: Option<HealthReasonCode>,
}

impl ConnectorHealthSample {
    #[must_use]
    pub const fn new(
        status: DependencyStatus,
        heartbeat_age_seconds: Option<u64>,
        queryable_sources: u16,
        reason: Option<HealthReasonCode>,
    ) -> Self {
        Self {
            status,
            heartbeat_age_seconds,
            queryable_sources,
            reason,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DatabaseHealthView {
    status: DependencyStatus,
    latency_ms: Option<u64>,
    active_connections: u32,
    idle_connections: u32,
    max_connections: u32,
    reason: Option<HealthReasonCode>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProviderHealthSummary {
    family: &'static str,
    configured: u32,
    healthy: u32,
    degraded: u32,
    unavailable: u32,
    unknown: u32,
    status: DependencyStatus,
    max_latency_ms: Option<u64>,
    reasons: Vec<HealthReasonCode>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectorHealthSummary {
    configured: u32,
    healthy: u32,
    degraded: u32,
    unavailable: u32,
    unknown: u32,
    status: DependencyStatus,
    oldest_heartbeat_age_seconds: Option<u64>,
    queryable_sources: u64,
    reasons: Vec<HealthReasonCode>,
}

/// Sanitized details suitable for the authenticated `/readyz` response.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SreHealthViewV1 {
    schema_version: &'static str,
    observed_at: DateTime<Utc>,
    ready: bool,
    overall_status: DependencyStatus,
    rules_only_available: bool,
    evidence_collection_available: bool,
    database: DatabaseHealthView,
    providers: Vec<ProviderHealthSummary>,
    connectors: ConnectorHealthSummary,
    telemetry: ObservabilityStatusViewV1,
}

impl SreHealthViewV1 {
    pub const SCHEMA_VERSION: &'static str = "rocketmq.sre.health.v1";

    #[must_use]
    pub const fn ready(&self) -> bool {
        self.ready
    }
}

/// Pure health aggregator. It does no I/O and owns no background task; callers
/// inject probe results collected by their lifecycle-owned services.
pub struct HealthAggregator;

impl HealthAggregator {
    #[must_use]
    pub fn aggregate(
        database: DatabaseHealthSample,
        providers: impl IntoIterator<Item = ProviderHealthSample>,
        connectors: impl IntoIterator<Item = ConnectorHealthSample>,
        telemetry: ObservabilityStatusViewV1,
    ) -> SreHealthViewV1 {
        let provider_samples = providers.into_iter().collect::<Vec<_>>();
        let connector_samples = connectors.into_iter().collect::<Vec<_>>();
        let provider_views = ProviderFamilyLabel::ALL
            .iter()
            .filter_map(|family| provider_summary(*family, provider_samples.as_slice()))
            .collect::<Vec<_>>();
        let connector_view = connector_summary(connector_samples.as_slice());

        let mut overall_status = database.status;
        for provider in &provider_views {
            overall_status = worst(overall_status, provider.status);
        }
        overall_status = worst(overall_status, connector_view.status);

        let database_ready = database.status.is_serving();
        let evidence_collection_available = connector_view.healthy > 0 || connector_view.degraded > 0;
        SreHealthViewV1 {
            schema_version: SreHealthViewV1::SCHEMA_VERSION,
            observed_at: Utc::now(),
            ready: database_ready,
            overall_status,
            rules_only_available: database_ready,
            evidence_collection_available,
            database: database.into(),
            providers: provider_views,
            connectors: connector_view,
            telemetry,
        }
    }
}

impl From<DatabaseHealthSample> for DatabaseHealthView {
    fn from(value: DatabaseHealthSample) -> Self {
        Self {
            status: value.status,
            latency_ms: value.latency_ms,
            active_connections: value.active_connections,
            idle_connections: value.idle_connections,
            max_connections: value.max_connections,
            reason: value.reason,
        }
    }
}

fn provider_summary(family: ProviderFamilyLabel, samples: &[ProviderHealthSample]) -> Option<ProviderHealthSummary> {
    let matching = samples
        .iter()
        .filter(|sample| sample.family == family)
        .copied()
        .collect::<Vec<_>>();
    if matching.is_empty() {
        return None;
    }

    let mut summary = ProviderHealthSummary {
        family: family.as_str(),
        configured: matching.len() as u32,
        healthy: 0,
        degraded: 0,
        unavailable: 0,
        unknown: 0,
        status: DependencyStatus::Healthy,
        max_latency_ms: None,
        reasons: Vec::new(),
    };
    for sample in matching {
        increment_status(
            sample.status,
            &mut summary.healthy,
            &mut summary.degraded,
            &mut summary.unavailable,
            &mut summary.unknown,
        );
        summary.status = worst(summary.status, sample.status);
        summary.max_latency_ms = max_optional(summary.max_latency_ms, sample.latency_ms);
        push_reason(&mut summary.reasons, sample.reason);
    }
    summary.reasons.sort_unstable();
    Some(summary)
}

fn connector_summary(samples: &[ConnectorHealthSample]) -> ConnectorHealthSummary {
    if samples.is_empty() {
        return ConnectorHealthSummary {
            configured: 0,
            healthy: 0,
            degraded: 0,
            unavailable: 0,
            unknown: 0,
            status: DependencyStatus::Unknown,
            oldest_heartbeat_age_seconds: None,
            queryable_sources: 0,
            reasons: vec![HealthReasonCode::NotConfigured],
        };
    }

    let mut summary = ConnectorHealthSummary {
        configured: samples.len() as u32,
        healthy: 0,
        degraded: 0,
        unavailable: 0,
        unknown: 0,
        status: DependencyStatus::Healthy,
        oldest_heartbeat_age_seconds: None,
        queryable_sources: 0,
        reasons: Vec::new(),
    };
    for sample in samples {
        increment_status(
            sample.status,
            &mut summary.healthy,
            &mut summary.degraded,
            &mut summary.unavailable,
            &mut summary.unknown,
        );
        summary.status = worst(summary.status, sample.status);
        summary.oldest_heartbeat_age_seconds =
            max_optional(summary.oldest_heartbeat_age_seconds, sample.heartbeat_age_seconds);
        summary.queryable_sources = summary
            .queryable_sources
            .saturating_add(u64::from(sample.queryable_sources));
        push_reason(&mut summary.reasons, sample.reason);
    }
    summary.reasons.sort_unstable();
    summary
}

fn increment_status(
    status: DependencyStatus,
    healthy: &mut u32,
    degraded: &mut u32,
    unavailable: &mut u32,
    unknown: &mut u32,
) {
    match status {
        DependencyStatus::Healthy => *healthy = healthy.saturating_add(1),
        DependencyStatus::Degraded => *degraded = degraded.saturating_add(1),
        DependencyStatus::Unavailable => *unavailable = unavailable.saturating_add(1),
        DependencyStatus::Unknown => *unknown = unknown.saturating_add(1),
    }
}

fn worst(left: DependencyStatus, right: DependencyStatus) -> DependencyStatus {
    if left.severity() >= right.severity() {
        left
    } else {
        right
    }
}

fn max_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn push_reason(reasons: &mut Vec<HealthReasonCode>, reason: Option<HealthReasonCode>) {
    if let Some(reason) = reason
        && !reasons.contains(&reason)
    {
        reasons.push(reason);
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_observability::ObservabilityStatusHandle;

    use super::*;

    #[test]
    fn provider_failure_degrades_details_but_rules_only_readiness_remains() {
        let health = HealthAggregator::aggregate(
            DatabaseHealthSample::healthy(2, 2, 6, 10),
            [ProviderHealthSample::new(
                ProviderFamilyLabel::DeepSeek,
                DependencyStatus::Unavailable,
                None,
                Some(HealthReasonCode::Timeout),
            )],
            [ConnectorHealthSample::new(DependencyStatus::Healthy, Some(2), 7, None)],
            ObservabilityStatusHandle::default().view(),
        );

        assert!(health.ready());
        assert!(health.rules_only_available);
        assert_eq!(health.overall_status, DependencyStatus::Unavailable);
        assert_eq!(health.providers[0].family, "deepseek");
        assert_eq!(health.providers[0].unavailable, 1);
    }

    #[test]
    fn database_failure_is_not_ready() {
        let health = HealthAggregator::aggregate(
            DatabaseHealthSample::unavailable(HealthReasonCode::ConnectionFailed),
            [],
            [],
            ObservabilityStatusHandle::default().view(),
        );

        assert!(!health.ready());
        assert!(!health.rules_only_available);
        assert_eq!(health.overall_status, DependencyStatus::Unavailable);
    }

    #[test]
    fn serialized_health_view_has_no_sensitive_or_high_cardinality_fields() {
        let health = HealthAggregator::aggregate(
            DatabaseHealthSample::healthy(1, 1, 2, 3),
            [ProviderHealthSample::new(
                ProviderFamilyLabel::MoonshotKimi,
                DependencyStatus::Degraded,
                Some(90),
                Some(HealthReasonCode::RateLimited),
            )],
            [ConnectorHealthSample::new(
                DependencyStatus::Degraded,
                Some(61),
                4,
                Some(HealthReasonCode::HeartbeatStale),
            )],
            ObservabilityStatusHandle::default().view(),
        );
        let json = serde_json::to_string(&health).expect("health view should serialize");

        for forbidden in [
            "prompt",
            "evidence_content",
            "tool_arguments",
            "access_token",
            "secret",
            "password",
            "database_url",
            "endpoint_authority",
            "connector_id",
            "tenant_id",
            "cluster_id",
        ] {
            assert!(!json.contains(forbidden), "health view exposed `{forbidden}`");
        }
        assert!(json.contains("\"family\":\"moonshot_kimi\""));
        assert!(json.contains("\"reasons\":[\"rate_limited\"]"));
    }
}

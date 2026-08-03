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

use crate::EvidenceOperation;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BrokerDiagnosticProfile {
    StorePressure,
    StoreIntegrity,
    RocksDbHealth,
    TieredStore,
    BrokerHa,
    AuthFailure,
    ColdDataFlow,
    DrReadiness,
    SecurityPosture,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MetricDiagnosticProfile {
    CapacityRunway,
    ControllerHa,
    SendLatency,
    ProxyConnectivity,
    RetryDlq,
    TransactionMessage,
    PopRevive,
    TimerBacklog,
    QueueHotspot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RouteDiagnosticProfile {
    NameServer,
    StaticTopic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum KubernetesDiagnosticProfile {
    UpgradeReadiness,
    ChangeRegression,
}

/// Canonical projection applied after a source-owned wire response has passed
/// its native contract validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CanonicalProjection {
    BrokerRuntime,
    ConsumerLag,
    TopicRoute,
    ConsumerRuntime,
    ProducerConnectivity,
    ClientConnections,
    BrokerHealth,
    ConsumerRuntimeMetrics,
    CollectorMetrics,
    KubernetesWorkloads,
    DeploymentDriftBasis,
    CollectorWorkload,
    RuntimeObservability,
    ClusterTopology,
    BrokerDiagnostics(BrokerDiagnosticProfile),
    MetricDiagnostics(MetricDiagnosticProfile),
    RouteDiagnostics(RouteDiagnosticProfile),
    TopicSubscriptionConfig,
    MessageMetadata,
    RuntimeSaturation,
    KubernetesDiagnostics(KubernetesDiagnosticProfile),
}

/// A fixed read-only query that can collect bounded source material for a
/// canonical diagnostic resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum CanonicalQuery {
    Mcp(EvidenceOperation),
    Admin(String),
    Prometheus {
        resource: String,
        matchers: Vec<(String, String)>,
    },
    Kubernetes(String),
    Runtime(String),
}

/// Resolution of a Wave A canonical resource.
///
/// Query routes always carry an explicit projection. Routes without a safe
/// read-only source remain fail-closed instead of falling through to a
/// similarly named legacy operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum CanonicalResourceRoute {
    Query {
        query: CanonicalQuery,
        projection: CanonicalProjection,
    },
    NotProductionVerified {
        reason_code: &'static str,
    },
}

impl CanonicalResourceRoute {
    pub(super) const fn reason_code(&self) -> Option<&'static str> {
        match self {
            Self::Query { .. } => None,
            Self::NotProductionVerified { reason_code } => Some(reason_code),
        }
    }
}

/// Resolves the source/resource pairs declared by all eight Wave A packs.
///
/// Unknown resources return `None` so the existing Phase 00 query surface
/// remains backward compatible. Recognized resources never fall through to a
/// similarly named legacy operation.
pub(super) fn resolve(source: &str, resource: &str) -> Option<CanonicalResourceRoute> {
    match source {
        "rocketmq-mcp" => resolve_mcp(resource),
        "admin-query" => resolve_admin(resource),
        "prometheus" => resolve_prometheus(resource),
        "tempo" => resolve_tempo(resource),
        "kubernetes" => resolve_kubernetes(resource),
        "runtime" => resolve_runtime(resource),
        "topology" => resolve_topology(resource),
        _ => None,
    }
}

fn resolve_mcp(resource: &str) -> Option<CanonicalResourceRoute> {
    if let Some(broker) = resource.strip_prefix("broker-runtime/") {
        return Some(identifier(broker).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |broker_name| {
                query(
                    CanonicalQuery::Mcp(EvidenceOperation::BrokerDescribe { broker_name }),
                    CanonicalProjection::BrokerRuntime,
                )
            },
        ));
    }
    if let Some(value) = resource.strip_prefix("consumer-lag/") {
        return Some(match exact_pair(value) {
            Some((consumer_group, topic)) => query(
                CanonicalQuery::Mcp(EvidenceOperation::ConsumerLag {
                    topic,
                    consumer_group,
                    limit: Some(200),
                    cursor: None,
                }),
                CanonicalProjection::ConsumerLag,
            ),
            None => not_verified("consumer_lag_topic_required"),
        });
    }
    if let Some(topic) = resource.strip_prefix("topic-route/") {
        return Some(identifier(topic).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |topic| {
                query(
                    CanonicalQuery::Mcp(EvidenceOperation::TopicDescribe {
                        topic,
                        limit: Some(200),
                        cursor: None,
                    }),
                    CanonicalProjection::TopicRoute,
                )
            },
        ));
    }
    if let Some(topic) = resource.strip_prefix("namesrv-route/") {
        return Some(topic_route_diagnostics(topic, RouteDiagnosticProfile::NameServer));
    }
    if let Some(topic) = resource.strip_prefix("static-topic-route/") {
        return Some(topic_route_diagnostics(topic, RouteDiagnosticProfile::StaticTopic));
    }
    None
}

fn resolve_admin(resource: &str) -> Option<CanonicalResourceRoute> {
    if let Some(group) = resource.strip_prefix("consumer-runtime/") {
        return Some(identifier(group).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |group| {
                query(
                    CanonicalQuery::Admin(format!("connections/{group}")),
                    CanonicalProjection::ConsumerRuntime,
                )
            },
        ));
    }
    if let Some(broker) = resource.strip_prefix("broker-connections/") {
        return Some(identifier(broker).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |broker| {
                query(
                    CanonicalQuery::Admin(format!("broker-connections/{broker}")),
                    CanonicalProjection::ClientConnections,
                )
            },
        ));
    }
    if let Some(cluster) = resource.strip_prefix("client-connections/") {
        return Some(identifier(cluster).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |_| {
                query(
                    CanonicalQuery::Admin("client-connections".to_owned()),
                    CanonicalProjection::ClientConnections,
                )
            },
        ));
    }
    if let Some(value) = resource.strip_prefix("message-metadata/") {
        return Some(match exact_pair(value) {
            Some((topic, message_id)) => query(
                CanonicalQuery::Admin(format!("message-metadata/{topic}/{message_id}")),
                CanonicalProjection::MessageMetadata,
            ),
            None => not_verified("message_metadata_topic_and_id_required"),
        });
    }
    if let Some(producer_group) = resource.strip_prefix("producer-connectivity/") {
        return Some(identifier(producer_group).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |producer_group| {
                query(
                    CanonicalQuery::Admin(format!("producer-connections/{producer_group}")),
                    CanonicalProjection::ProducerConnectivity,
                )
            },
        ));
    }
    if let Some(value) = resource.strip_prefix("topic-subscription-config/") {
        return Some(match exact_pair(value) {
            Some((consumer_group, topic)) => query(
                CanonicalQuery::Admin(format!("topic-subscription-config/{consumer_group}/{topic}")),
                CanonicalProjection::TopicSubscriptionConfig,
            ),
            None => not_verified("topic_subscription_group_and_topic_required"),
        });
    }
    for (prefix, query_resource, profile) in [
        (
            "store-pressure/",
            "store/health",
            BrokerDiagnosticProfile::StorePressure,
        ),
        (
            "store-integrity/",
            "store/recovery",
            BrokerDiagnosticProfile::StoreIntegrity,
        ),
        (
            "rocksdb-health/",
            "store/rocksdb",
            BrokerDiagnosticProfile::RocksDbHealth,
        ),
        ("tiered-store/", "store/tiered", BrokerDiagnosticProfile::TieredStore),
        ("broker-ha/", "broker/diagnostics", BrokerDiagnosticProfile::BrokerHa),
        (
            "auth-failure/",
            "auth/diagnostics",
            BrokerDiagnosticProfile::AuthFailure,
        ),
        ("cold-data-flow/", "store/tiered", BrokerDiagnosticProfile::ColdDataFlow),
        (
            "dr-readiness/",
            "broker/diagnostics",
            BrokerDiagnosticProfile::DrReadiness,
        ),
        (
            "security-posture/",
            "auth/diagnostics",
            BrokerDiagnosticProfile::SecurityPosture,
        ),
    ] {
        if let Some(target) = resource.strip_prefix(prefix) {
            return Some(if safe_resource_path(target) {
                query(
                    CanonicalQuery::Admin(query_resource.to_owned()),
                    CanonicalProjection::BrokerDiagnostics(profile),
                )
            } else {
                not_verified("canonical_resource_parameters_unavailable")
            });
        }
    }
    None
}

fn resolve_prometheus(resource: &str) -> Option<CanonicalResourceRoute> {
    if let Some(broker) = resource.strip_prefix("broker-health/") {
        return Some(identifier(broker).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |broker| {
                query(
                    CanonicalQuery::Prometheus {
                        resource: "metrics/rocketmq_broker_up".to_owned(),
                        matchers: vec![("node_id".to_owned(), broker)],
                    },
                    CanonicalProjection::BrokerHealth,
                )
            },
        ));
    }
    if let Some(group) = resource.strip_prefix("consumer-runtime/") {
        return Some(identifier(group).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |group| {
                query(
                    CanonicalQuery::Prometheus {
                        resource: "metrics/rocketmq_consumer_connections".to_owned(),
                        matchers: vec![("consumer_group".to_owned(), group)],
                    },
                    CanonicalProjection::ConsumerRuntimeMetrics,
                )
            },
        ));
    }
    if let Some(collector) = resource.strip_prefix("telemetry-pipeline/") {
        return Some(identifier(collector).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |collector| {
                query(
                    CanonicalQuery::Prometheus {
                        resource: "metrics/otelcol_exporter_send_failed_metric_points_total".to_owned(),
                        matchers: vec![("service_name".to_owned(), collector)],
                    },
                    CanonicalProjection::CollectorMetrics,
                )
            },
        ));
    }
    for (prefix, query_resource, profile) in [
        (
            "capacity-runway/",
            "trend/30d/rocketmq_store_disk_usage",
            MetricDiagnosticProfile::CapacityRunway,
        ),
        (
            "controller-ha/",
            "metrics/rocketmq_controller_quorum_health_ratio",
            MetricDiagnosticProfile::ControllerHa,
        ),
        (
            "send-latency/",
            "metrics/rocketmq_send_message_latency_milliseconds_bucket",
            MetricDiagnosticProfile::SendLatency,
        ),
        (
            "proxy-connectivity/",
            "metrics/rocketmq_proxy_grpc_errors_total",
            MetricDiagnosticProfile::ProxyConnectivity,
        ),
        (
            "retry-dlq/",
            "metrics/rocketmq_send_to_dlq_messages_total",
            MetricDiagnosticProfile::RetryDlq,
        ),
        (
            "transaction-message/",
            "metrics/rocketmq_half_messages",
            MetricDiagnosticProfile::TransactionMessage,
        ),
        (
            "pop-revive/",
            "metrics/rocketmq_pop_revive_lag",
            MetricDiagnosticProfile::PopRevive,
        ),
        (
            "timer-backlog/",
            "metrics/rocketmq_timer_dequeue_lag",
            MetricDiagnosticProfile::TimerBacklog,
        ),
        (
            "queue-hotspot/",
            "metrics/rocketmq_consumer_lag_messages",
            MetricDiagnosticProfile::QueueHotspot,
        ),
    ] {
        if let Some(target) = resource.strip_prefix(prefix) {
            return Some(if safe_resource_path(target) {
                query(
                    CanonicalQuery::Prometheus {
                        resource: query_resource.to_owned(),
                        matchers: Vec::new(),
                    },
                    CanonicalProjection::MetricDiagnostics(profile),
                )
            } else {
                not_verified("canonical_resource_parameters_unavailable")
            });
        }
    }
    resolve_catalog_resource(
        resource,
        &[
            "store-trend/",
            "ha-network/",
            "namesrv-network/",
            "auth-telemetry/",
            "runtime-telemetry/",
            "prevention-trend/",
        ],
        "optional_metric_projection_not_production_verified",
    )
}

fn resolve_tempo(resource: &str) -> Option<CanonicalResourceRoute> {
    if resource.starts_with("message-trace/") {
        return Some(not_verified("pseudonymized_trace_identifier_is_not_queryable"));
    }
    resolve_catalog_resource(
        resource,
        &["routing-trace/"],
        "wave_b_trace_projection_not_production_verified",
    )
}

fn resolve_kubernetes(resource: &str) -> Option<CanonicalResourceRoute> {
    if let Some(namespace) = resource.strip_prefix("live-resources/") {
        return Some(identifier(namespace).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |_| {
                query(
                    CanonicalQuery::Kubernetes("pods".to_owned()),
                    CanonicalProjection::KubernetesWorkloads,
                )
            },
        ));
    }
    if let Some(component) = resource.strip_prefix("deployment-drift/") {
        return Some(identifier(component).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |_| {
                query(
                    CanonicalQuery::Kubernetes("pods".to_owned()),
                    CanonicalProjection::DeploymentDriftBasis,
                )
            },
        ));
    }
    if let Some(namespace) = resource.strip_prefix("otel-collector/") {
        return Some(identifier(namespace).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |_| {
                query(
                    CanonicalQuery::Kubernetes("pods".to_owned()),
                    CanonicalProjection::CollectorWorkload,
                )
            },
        ));
    }
    if let Some(namespace) = resource.strip_prefix("upgrade-readiness/") {
        return Some(if safe_resource_path(namespace) {
            query(
                CanonicalQuery::Kubernetes("statefulsets".to_owned()),
                CanonicalProjection::KubernetesDiagnostics(KubernetesDiagnosticProfile::UpgradeReadiness),
            )
        } else {
            not_verified("canonical_resource_parameters_unavailable")
        });
    }
    if let Some(namespace) = resource.strip_prefix("change-regression/") {
        return Some(if safe_resource_path(namespace) {
            query(
                CanonicalQuery::Kubernetes("change-timeline".to_owned()),
                CanonicalProjection::KubernetesDiagnostics(KubernetesDiagnosticProfile::ChangeRegression),
            )
        } else {
            not_verified("canonical_resource_parameters_unavailable")
        });
    }
    resolve_catalog_resource(
        resource,
        &["proxy-workload/"],
        "optional_kubernetes_projection_not_production_verified",
    )
}

fn resolve_runtime(resource: &str) -> Option<CanonicalResourceRoute> {
    if let Some(component) = resource.strip_prefix("observability/") {
        return Some(identifier(component).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |_| {
                query(
                    CanonicalQuery::Runtime("observability".to_owned()),
                    CanonicalProjection::RuntimeObservability,
                )
            },
        ));
    }
    if let Some(component) = resource.strip_prefix("build-info/") {
        return Some(identifier(component).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |_| not_verified("runtime_build_metadata_not_exposed"),
        ));
    }
    resource.strip_prefix("runtime-saturation/").map(|component| {
        identifier(component).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |component| {
                query(
                    CanonicalQuery::Runtime(format!("runtime/{component}")),
                    CanonicalProjection::RuntimeSaturation,
                )
            },
        )
    })
}

fn resolve_topology(resource: &str) -> Option<CanonicalResourceRoute> {
    resource.strip_prefix("asset-graph/").map(|cluster| {
        identifier(cluster).map_or_else(
            || not_verified("canonical_resource_parameters_unavailable"),
            |_| {
                query(
                    CanonicalQuery::Mcp(EvidenceOperation::ClusterOverview),
                    CanonicalProjection::ClusterTopology,
                )
            },
        )
    })
}

fn topic_route_diagnostics(topic: &str, profile: RouteDiagnosticProfile) -> CanonicalResourceRoute {
    identifier(topic).map_or_else(
        || not_verified("canonical_resource_parameters_unavailable"),
        |topic| {
            query(
                CanonicalQuery::Mcp(EvidenceOperation::TopicDescribe {
                    topic,
                    limit: Some(200),
                    cursor: None,
                }),
                CanonicalProjection::RouteDiagnostics(profile),
            )
        },
    )
}

const fn query(query: CanonicalQuery, projection: CanonicalProjection) -> CanonicalResourceRoute {
    CanonicalResourceRoute::Query { query, projection }
}

const fn not_verified(reason_code: &'static str) -> CanonicalResourceRoute {
    CanonicalResourceRoute::NotProductionVerified { reason_code }
}

fn resolve_catalog_resource(
    resource: &str,
    prefixes: &[&str],
    reason_code: &'static str,
) -> Option<CanonicalResourceRoute> {
    prefixes.iter().find_map(|prefix| {
        resource.strip_prefix(prefix).map(|suffix| {
            if safe_resource_path(suffix) {
                not_verified(reason_code)
            } else {
                not_verified("canonical_resource_parameters_unavailable")
            }
        })
    })
}

fn safe_resource_path(value: &str) -> bool {
    !value.is_empty() && value.len() <= 512 && value.split('/').all(|segment| identifier(segment).is_some())
}

fn exact_pair(value: &str) -> Option<(String, String)> {
    let mut parts = value.split('/');
    let first = identifier(parts.next()?)?;
    let second = identifier(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    Some((first, second))
}

fn identifier(value: &str) -> Option<String> {
    (!value.is_empty()
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'%' | b':')))
    .then(|| value.to_owned())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rocketmq_sre_core::diagnostics::EvidenceRequirement;
    use rocketmq_sre_core::diagnostics::full_pack_ids;
    use rocketmq_sre_core::diagnostics::full_registry;

    use super::super::normalize_source;
    use super::*;

    #[test]
    fn all_builtin_evidence_contracts_have_explicit_query_or_fail_closed_routes() {
        let registry = full_registry().expect("complete built-in registry");
        let mut pack_ids = BTreeSet::new();
        let mut requirement_count = 0;
        let mut required_count = 0;
        let mut optional_count = 0;
        let mut query_count = 0;
        let mut unsupported_count = 0;
        let mut required_query_count = 0;
        let mut required_unsupported_count = 0;
        let mut optional_query_count = 0;
        let mut optional_unsupported_count = 0;

        for pack in registry.active_packs() {
            pack_ids.insert(pack.qualified_id());
            for (requirement, required) in pack
                .required_evidence()
                .iter()
                .map(|requirement| (requirement, true))
                .chain(pack.optional_evidence().iter().map(|requirement| (requirement, false)))
            {
                requirement_count += 1;
                if required {
                    required_count += 1;
                } else {
                    optional_count += 1;
                }
                let resource = example_resource(requirement);
                let source = normalize_source(requirement.source).expect("registered pack source");
                let route = resolve(source, &resource).unwrap_or_else(|| {
                    panic!(
                        "missing canonical route for {} {}",
                        requirement.source, requirement.resource_prefix
                    )
                });
                match route {
                    CanonicalResourceRoute::Query { .. } => {
                        query_count += 1;
                        if required {
                            required_query_count += 1;
                        } else {
                            optional_query_count += 1;
                        }
                    }
                    CanonicalResourceRoute::NotProductionVerified { reason_code } => {
                        assert!(!reason_code.is_empty());
                        unsupported_count += 1;
                        if required {
                            required_unsupported_count += 1;
                        } else {
                            optional_unsupported_count += 1;
                        }
                    }
                }
            }
        }

        assert_eq!(pack_ids, full_pack_ids().into_iter().collect());
        assert_eq!(requirement_count, 69);
        assert_eq!(required_count, 32);
        assert_eq!(optional_count, 37);
        assert_eq!(query_count, 48);
        assert_eq!(unsupported_count, 21);
        assert_eq!(required_query_count, 32);
        assert_eq!(required_unsupported_count, 0);
        assert_eq!(optional_query_count, 16);
        assert_eq!(optional_unsupported_count, 21);
    }

    #[test]
    fn live_wave_a_routes_have_explicit_projections() {
        for (source, resource, projection) in [
            (
                "rocketmq-mcp",
                "consumer-lag/group-a/orders",
                CanonicalProjection::ConsumerLag,
            ),
            (
                "prometheus",
                "broker-health/broker-a",
                CanonicalProjection::BrokerHealth,
            ),
            (
                "topology",
                "asset-graph/cluster-a",
                CanonicalProjection::ClusterTopology,
            ),
            (
                "runtime",
                "observability/mcp",
                CanonicalProjection::RuntimeObservability,
            ),
            (
                "kubernetes",
                "deployment-drift/broker-a",
                CanonicalProjection::DeploymentDriftBasis,
            ),
            (
                "admin-query",
                "producer-connectivity/producer-a",
                CanonicalProjection::ProducerConnectivity,
            ),
            (
                "admin-query",
                "client-connections/cluster-a",
                CanonicalProjection::ClientConnections,
            ),
        ] {
            assert!(matches!(
                resolve(source, resource),
                Some(CanonicalResourceRoute::Query {
                    projection: actual,
                    ..
                }) if actual == projection
            ));
        }
    }

    #[test]
    fn required_diagnostic_profiles_use_fixed_read_only_queries() {
        for (resource, query_resource, profile) in [
            (
                "store-pressure/broker-a",
                "store/health",
                BrokerDiagnosticProfile::StorePressure,
            ),
            (
                "store-integrity/broker-a",
                "store/recovery",
                BrokerDiagnosticProfile::StoreIntegrity,
            ),
            (
                "rocksdb-health/broker-a",
                "store/rocksdb",
                BrokerDiagnosticProfile::RocksDbHealth,
            ),
            (
                "tiered-store/broker-a",
                "store/tiered",
                BrokerDiagnosticProfile::TieredStore,
            ),
            (
                "broker-ha/broker-a",
                "broker/diagnostics",
                BrokerDiagnosticProfile::BrokerHa,
            ),
            (
                "auth-failure/broker-a",
                "auth/diagnostics",
                BrokerDiagnosticProfile::AuthFailure,
            ),
            (
                "cold-data-flow/broker-a",
                "store/tiered",
                BrokerDiagnosticProfile::ColdDataFlow,
            ),
            (
                "dr-readiness/broker-a",
                "broker/diagnostics",
                BrokerDiagnosticProfile::DrReadiness,
            ),
            (
                "security-posture/broker-a",
                "auth/diagnostics",
                BrokerDiagnosticProfile::SecurityPosture,
            ),
        ] {
            assert_eq!(
                resolve("admin-query", resource),
                Some(query(
                    CanonicalQuery::Admin(query_resource.to_owned()),
                    CanonicalProjection::BrokerDiagnostics(profile),
                ))
            );
        }

        for (resource, query_resource, profile) in [
            (
                "capacity-runway/cluster-a",
                "trend/30d/rocketmq_store_disk_usage",
                MetricDiagnosticProfile::CapacityRunway,
            ),
            (
                "controller-ha/controller-a",
                "metrics/rocketmq_controller_quorum_health_ratio",
                MetricDiagnosticProfile::ControllerHa,
            ),
            (
                "send-latency/topic-a",
                "metrics/rocketmq_send_message_latency_milliseconds_bucket",
                MetricDiagnosticProfile::SendLatency,
            ),
            (
                "proxy-connectivity/proxy-a",
                "metrics/rocketmq_proxy_grpc_errors_total",
                MetricDiagnosticProfile::ProxyConnectivity,
            ),
            (
                "retry-dlq/group-a",
                "metrics/rocketmq_send_to_dlq_messages_total",
                MetricDiagnosticProfile::RetryDlq,
            ),
            (
                "transaction-message/topic-a",
                "metrics/rocketmq_half_messages",
                MetricDiagnosticProfile::TransactionMessage,
            ),
            (
                "pop-revive/group-a",
                "metrics/rocketmq_pop_revive_lag",
                MetricDiagnosticProfile::PopRevive,
            ),
            (
                "timer-backlog/topic-a",
                "metrics/rocketmq_timer_dequeue_lag",
                MetricDiagnosticProfile::TimerBacklog,
            ),
            (
                "queue-hotspot/topic-a",
                "metrics/rocketmq_consumer_lag_messages",
                MetricDiagnosticProfile::QueueHotspot,
            ),
        ] {
            assert_eq!(
                resolve("prometheus", resource),
                Some(query(
                    CanonicalQuery::Prometheus {
                        resource: query_resource.to_owned(),
                        matchers: Vec::new(),
                    },
                    CanonicalProjection::MetricDiagnostics(profile),
                ))
            );
        }

        assert!(matches!(
            resolve("admin-query", "message-metadata/topic-a/message-a"),
            Some(CanonicalResourceRoute::Query {
                query: CanonicalQuery::Admin(_),
                projection: CanonicalProjection::MessageMetadata,
            })
        ));
        assert!(matches!(
            resolve("admin-query", "topic-subscription-config/group-a/topic-a"),
            Some(CanonicalResourceRoute::Query {
                query: CanonicalQuery::Admin(_),
                projection: CanonicalProjection::TopicSubscriptionConfig,
            })
        ));
        assert!(matches!(
            resolve("rocketmq-mcp", "namesrv-route/topic-a"),
            Some(CanonicalResourceRoute::Query {
                query: CanonicalQuery::Mcp(EvidenceOperation::TopicDescribe { .. }),
                projection: CanonicalProjection::RouteDiagnostics(RouteDiagnosticProfile::NameServer),
            })
        ));
        assert!(matches!(
            resolve("rocketmq-mcp", "static-topic-route/topic-a"),
            Some(CanonicalResourceRoute::Query {
                query: CanonicalQuery::Mcp(EvidenceOperation::TopicDescribe { .. }),
                projection: CanonicalProjection::RouteDiagnostics(RouteDiagnosticProfile::StaticTopic),
            })
        ));
        assert!(matches!(
            resolve("runtime", "runtime-saturation/broker"),
            Some(CanonicalResourceRoute::Query {
                query: CanonicalQuery::Runtime(_),
                projection: CanonicalProjection::RuntimeSaturation,
            })
        ));
        assert!(matches!(
            resolve("kubernetes", "upgrade-readiness/rocketmq"),
            Some(CanonicalResourceRoute::Query {
                query: CanonicalQuery::Kubernetes(_),
                projection: CanonicalProjection::KubernetesDiagnostics(KubernetesDiagnosticProfile::UpgradeReadiness),
            })
        ));
        assert!(matches!(
            resolve("kubernetes", "change-regression/rocketmq"),
            Some(CanonicalResourceRoute::Query {
                query: CanonicalQuery::Kubernetes(_),
                projection: CanonicalProjection::KubernetesDiagnostics(KubernetesDiagnosticProfile::ChangeRegression),
            })
        ));
    }

    #[test]
    fn parameterized_routes_never_guess_missing_identifiers() {
        assert!(matches!(
            resolve("rocketmq-mcp", "consumer-lag/group-a"),
            Some(CanonicalResourceRoute::NotProductionVerified {
                reason_code: "consumer_lag_topic_required"
            })
        ));
        assert!(matches!(
            resolve("rocketmq-mcp", "consumer-lag/group-a/orders"),
            Some(CanonicalResourceRoute::Query {
                query: CanonicalQuery::Mcp(EvidenceOperation::ConsumerLag { .. }),
                projection: CanonicalProjection::ConsumerLag,
            })
        ));
        assert!(matches!(
            resolve("admin-query", "consumer-runtime/group-a"),
            Some(CanonicalResourceRoute::Query {
                query: CanonicalQuery::Admin(_),
                projection: CanonicalProjection::ConsumerRuntime,
            })
        ));
        assert!(matches!(
            resolve("admin-query", "message-metadata/topic-a/message-a"),
            Some(CanonicalResourceRoute::Query {
                query: CanonicalQuery::Admin(_),
                projection: CanonicalProjection::MessageMetadata
            })
        ));
        assert!(matches!(
            resolve("admin-query", "message-metadata/message-a"),
            Some(CanonicalResourceRoute::NotProductionVerified {
                reason_code: "message_metadata_topic_and_id_required"
            })
        ));
        assert!(resolve("rocketmq-mcp", "messages/raw-body").is_none());
    }

    fn example_resource(requirement: &EvidenceRequirement) -> String {
        let suffix = match requirement.resource_prefix {
            "broker-health/" => "broker-a",
            "broker-runtime/" => "broker-a",
            "broker-connections/" => "broker-a",
            "asset-graph/" => "cluster-a",
            "live-resources/" => "rocketmq",
            "client-connections/" => "cluster-a",
            "consumer-lag/" => "group-a/topic-a",
            "consumer-runtime/" => "group-a",
            "deployment-drift/" => "broker-a",
            "build-info/" => "broker-a",
            "message-metadata/" => "topic-a/message-a",
            "message-trace/" => "trace-hash-a",
            "topic-route/" => "topic-a",
            "topic-subscription-config/" => "group-a/topic-a",
            "producer-connectivity/" => "producer-a",
            "observability/" => "mcp",
            "runtime-saturation/" => "broker",
            "telemetry-pipeline/" => "collector-a",
            "otel-collector/" => "rocketmq",
            prefix if is_catalog_prefix(prefix) => "fixture",
            prefix => panic!("unexpected diagnostic resource prefix {prefix}"),
        };
        format!("{}{suffix}", requirement.resource_prefix)
    }

    fn is_catalog_prefix(prefix: &str) -> bool {
        [
            "store-pressure/",
            "store-integrity/",
            "rocksdb-health/",
            "tiered-store/",
            "store-trend/",
            "broker-ha/",
            "controller-ha/",
            "namesrv-route/",
            "ha-network/",
            "namesrv-network/",
            "send-latency/",
            "proxy-connectivity/",
            "proxy-workload/",
            "routing-trace/",
            "static-topic-route/",
            "retry-dlq/",
            "transaction-message/",
            "pop-revive/",
            "timer-backlog/",
            "queue-hotspot/",
            "auth-failure/",
            "auth-telemetry/",
            "runtime-telemetry/",
            "upgrade-readiness/",
            "capacity-runway/",
            "prevention-trend/",
            "cold-data-flow/",
            "dr-readiness/",
            "security-posture/",
            "change-regression/",
        ]
        .contains(&prefix)
    }
}

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

use std::collections::BTreeSet;

use chrono::Utc;
use rocketmq_sre_contracts::{InvestigationPackRef, InvestigationRequirementRef, InvestigationRun, ResourceKind};
use rocketmq_sre_core::diagnostics::{DiagnosticPack, DiagnosticPackRegistry, InvestigationReadBinding, PackVersion};
use sqlx::{PgConnection, Row};

use crate::ControlPlaneRequestFailure;

/// Only coherent, bounded inventory relations may supply a related target.
/// An ambiguous or absent target becomes missing evidence, never a guessed host.
#[derive(Default)]
struct Targets {
    topic: Option<String>,
    broker: Option<String>,
    consumer: Option<String>,
    producer: Option<String>,
    cluster: Option<String>,
}

pub(in crate::workflow) async fn resolve(
    connection: &mut PgConnection,
    registry: &DiagnosticPackRegistry,
    run: &InvestigationRun,
) -> Result<Vec<InvestigationReadBinding>, ControlPlaneRequestFailure> {
    let mut targets = Targets {
        cluster: Some(run.effective_scope.cluster_id.to_string()),
        ..Targets::default()
    };
    if let Some(subject) = &run.effective_scope.resource {
        match subject.kind {
            ResourceKind::Topic if identifier(&subject.key) => targets.topic = Some(subject.key.clone()),
            ResourceKind::Broker | ResourceKind::Store if identifier(&subject.key) => {
                targets.broker = Some(subject.key.clone())
            }
            ResourceKind::Store if subject.key.starts_with("broker/") && identifier(&subject.key[7..]) => {
                targets.broker = Some(subject.key[7..].to_owned())
            }
            ResourceKind::ConsumerGroup => {
                let mut pair = subject.key.split('/');
                if let (Some(group), Some(topic), None) = (pair.next(), pair.next(), pair.next()) {
                    if identifier(group) && identifier(topic) {
                        targets.consumer = Some(group.to_owned());
                        targets.topic = Some(topic.to_owned());
                    }
                } else if identifier(&subject.key) {
                    targets.consumer = Some(subject.key.clone());
                }
            }
            ResourceKind::ProducerGroup if identifier(&subject.key) => targets.producer = Some(subject.key.clone()),
            _ => {}
        }
    }
    let inventory = sqlx::query("SELECT id,observed_at,freshness_seconds,partial FROM asset_inventory_snapshots WHERE tenant_id=$1 AND cluster_id=$2 ORDER BY observed_at DESC,created_at DESC,id DESC LIMIT 1")
        .bind(run.initial_scope.tenant_id.as_uuid()).bind(run.initial_scope.cluster_id.as_uuid()).fetch_optional(&mut *connection).await?;
    if let Some(inventory) = inventory {
        let observed: chrono::DateTime<Utc> = inventory.try_get("observed_at")?;
        let reported: i64 = inventory.try_get("freshness_seconds")?;
        let elapsed = Utc::now().signed_duration_since(observed).num_seconds();
        if !inventory.try_get::<bool, _>("partial")?
            && (0..=300).contains(&elapsed)
            && reported >= 0
            && reported.saturating_add(elapsed) <= 300
        {
            let anchors = [
                targets.topic.as_ref().map(|key| format!("topic:{key}")),
                targets.consumer.as_ref().map(|key| format!("consumer_group:{key}")),
                targets.producer.as_ref().map(|key| format!("producer_group:{key}")),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            if !anchors.is_empty() {
                let edges = sqlx::query("SELECT from_key,to_key,relation FROM topology_edges WHERE tenant_id=$1 AND cluster_id=$2 AND inventory_snapshot_id=$3 AND NOT partial AND observed_at<=$4 AND observed_at>=$4-INTERVAL '300 seconds' AND freshness_seconds + EXTRACT(EPOCH FROM ($4-observed_at))<=300 AND (from_key=ANY($5) OR to_key=ANY($5)) ORDER BY from_key,to_key,relation LIMIT 129")
                    .bind(run.initial_scope.tenant_id.as_uuid()).bind(run.initial_scope.cluster_id.as_uuid()).bind(inventory.try_get::<uuid::Uuid,_>("id")?).bind(Utc::now()).bind(&anchors).fetch_all(&mut *connection).await?;
                if edges.len() <= 128 {
                    let mut topics = BTreeSet::new();
                    let mut brokers = BTreeSet::new();
                    let mut consumers = BTreeSet::new();
                    let mut producers = BTreeSet::new();
                    for edge in &edges {
                        let from: String = edge.try_get("from_key")?;
                        let to: String = edge.try_get("to_key")?;
                        let relation: String = edge.try_get("relation")?;
                        if relation == "routes_to"
                            && targets
                                .topic
                                .as_ref()
                                .is_some_and(|topic| from == format!("topic:{topic}"))
                        {
                            add_key(&mut brokers, &to, "broker:");
                        }
                        if relation == "consumes_from"
                            && targets
                                .consumer
                                .as_ref()
                                .is_some_and(|group| from == format!("consumer_group:{group}"))
                        {
                            add_key(&mut topics, &to, "topic:");
                        }
                        if relation == "consumes_from"
                            && targets
                                .topic
                                .as_ref()
                                .is_some_and(|topic| to == format!("topic:{topic}"))
                        {
                            add_key(&mut consumers, &from, "consumer_group:");
                        }
                        if relation == "produces_to"
                            && targets
                                .producer
                                .as_ref()
                                .is_some_and(|group| from == format!("producer_group:{group}"))
                        {
                            add_key(&mut topics, &to, "topic:");
                        }
                        if relation == "produces_to"
                            && targets
                                .topic
                                .as_ref()
                                .is_some_and(|topic| to == format!("topic:{topic}"))
                        {
                            add_key(&mut producers, &from, "producer_group:");
                        }
                    }
                    targets.topic = targets.topic.or_else(|| unique(topics));
                    targets.broker = targets.broker.or_else(|| unique(brokers));
                    targets.consumer = targets.consumer.or_else(|| unique(consumers));
                    targets.producer = targets.producer.or_else(|| unique(producers));
                }
            }
        }
    }
    let mut result = Vec::new();
    for reference in &run.effective_packs {
        let pack = exact_pack(registry, reference)?;
        for requirement in pack.required_evidence().iter().chain(pack.optional_evidence()) {
            if let Some(resource) = resource(requirement.source, requirement.resource_prefix, &targets) {
                result.push(InvestigationReadBinding {
                    requirement: InvestigationRequirementRef {
                        pack: InvestigationPackRef {
                            pack_id: pack.qualified_id(),
                            pack_version: pack.version().to_string(),
                        },
                        requirement_id: requirement.key.to_owned(),
                    },
                    source: requirement.source.to_owned(),
                    resource,
                    scope: run.effective_scope.clone(),
                });
            }
        }
    }
    Ok(result)
}

pub(in crate::workflow) fn exact_pack<'a>(
    registry: &'a DiagnosticPackRegistry,
    reference: &InvestigationPackRef,
) -> Result<&'a dyn DiagnosticPack, ControlPlaneRequestFailure> {
    let fail = || super::contract_failure(rocketmq_sre_contracts::InvestigationRunFailure::PackNotAllowed);
    let parts = reference
        .pack_version
        .split('.')
        .map(str::parse::<u16>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| fail())?;
    let [major, minor, patch] = parts.as_slice() else {
        return Err(fail());
    };
    let (base, _) = reference.pack_id.rsplit_once(".v").ok_or_else(fail)?;
    let pack = registry
        .get(base, PackVersion::new(*major, *minor, *patch))
        .ok_or_else(fail)?;
    if pack.qualified_id() != reference.pack_id || pack.version().to_string() != reference.pack_version {
        return Err(fail());
    }
    Ok(pack)
}

fn add_key(set: &mut BTreeSet<String>, value: &str, prefix: &str) {
    if let Some(key) = value.strip_prefix(prefix).filter(|key| identifier(key)) {
        set.insert(key.to_owned());
    }
}
fn unique(mut values: BTreeSet<String>) -> Option<String> {
    if values.len() == 1 { values.pop_first() } else { None }
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value
            .bytes()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, b'_' | b'-' | b'.' | b'%' | b'@'))
}

fn resource(source: &str, prefix: &str, targets: &Targets) -> Option<String> {
    let suffix = match (source, prefix) {
        ("rocketmq-mcp", "consumer-lag/") | ("admin-query", "topic-subscription-config/") => {
            Some(format!("{}/{}", targets.consumer.as_ref()?, targets.topic.as_ref()?))
        }
        ("rocketmq-mcp", "broker-runtime/")
        | (
            "admin-query",
            "broker-connections/"
            | "store-pressure/"
            | "store-integrity/"
            | "rocksdb-health/"
            | "tiered-store/"
            | "broker-ha/",
        )
        | ("prometheus", "broker-health/" | "store-trend/") => targets.broker.clone(),
        ("rocketmq-mcp", "topic-route/" | "namesrv-route/" | "static-topic-route/")
        | ("prometheus", "send-latency/") => targets.topic.clone(),
        ("admin-query", "consumer-runtime/") => targets.consumer.clone(),
        ("admin-query", "producer-connectivity/") => targets.producer.clone(),
        ("admin-query", "client-connections/") => targets.cluster.clone(),
        // New collector profiles require an explicit typed target adapter here.
        // Descriptor registration alone does not authorize arbitrary resource text.
        _ => None,
    }?;
    Some(format!("{prefix}{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_profiles_require_exact_typed_targets() {
        let targets = Targets {
            topic: Some("orders".into()),
            consumer: Some("billing".into()),
            broker: Some("broker-a".into()),
            ..Targets::default()
        };
        assert_eq!(
            resource("rocketmq-mcp", "consumer-lag/", &targets).as_deref(),
            Some("consumer-lag/billing/orders")
        );
        assert_eq!(
            resource("admin-query", "store-pressure/", &targets).as_deref(),
            Some("store-pressure/broker-a")
        );
        assert!(resource("admin-query", "http://target/", &targets).is_none());
        assert!(resource("rocketmq-mcp", "consumer-lag/", &Targets::default()).is_none());
        assert!(!identifier("broker-a/other"));
        assert!(!identifier("http://host"));
        assert!(unique(BTreeSet::from(["a".into(), "b".into()])).is_none());
    }
}

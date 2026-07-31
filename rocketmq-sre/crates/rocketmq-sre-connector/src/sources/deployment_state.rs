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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use super::kubernetes::filtered_labels;

pub(super) fn project_deployment(item: &Value, allowed_labels: &BTreeSet<String>) -> Value {
    project_workload(item, allowed_labels, WorkloadKind::Deployment)
}

pub(super) fn project_stateful_set(item: &Value, allowed_labels: &BTreeSet<String>) -> Value {
    project_workload(item, allowed_labels, WorkloadKind::StatefulSet)
}

pub(super) fn project_certificate(item: &Value, allowed_labels: &BTreeSet<String>) -> Value {
    let conditions = item
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .map(|conditions| {
            conditions
                .iter()
                .filter(|condition| condition.get("type").and_then(Value::as_str) == Some("Ready"))
                .map(|condition| {
                    json!({
                        "type": "Ready",
                        "status": condition.get("status"),
                        "reason": condition.get("reason"),
                        "last_transition_time": condition.get("lastTransitionTime")
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    json!({
        "schema_version": "rocketmq.kubernetes-certificate.v1",
        "name": item.pointer("/metadata/name"),
        "namespace": item.pointer("/metadata/namespace"),
        "labels": filtered_labels(item.pointer("/metadata/labels"), allowed_labels),
        "issuer_kind": item.pointer("/spec/issuerRef/kind"),
        "not_before": item.pointer("/status/notBefore"),
        "not_after": item.pointer("/status/notAfter"),
        "renewal_time": item.pointer("/status/renewalTime"),
        "revision": item.pointer("/status/revision"),
        "conditions": conditions
    })
}

#[derive(Clone, Copy)]
enum WorkloadKind {
    Deployment,
    StatefulSet,
}

impl WorkloadKind {
    const fn wire_name(self) -> &'static str {
        match self {
            Self::Deployment => "deployment",
            Self::StatefulSet => "stateful_set",
        }
    }
}

fn project_workload(item: &Value, allowed_labels: &BTreeSet<String>, kind: WorkloadKind) -> Value {
    let desired = item.pointer("/spec/replicas").and_then(Value::as_u64).unwrap_or(1);
    let updated = item
        .pointer("/status/updatedReplicas")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let ready = item
        .pointer("/status/readyReplicas")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let available = item
        .pointer("/status/availableReplicas")
        .and_then(Value::as_u64)
        .unwrap_or(ready);
    let generation = item.pointer("/metadata/generation").and_then(Value::as_u64);
    let observed_generation = item.pointer("/status/observedGeneration").and_then(Value::as_u64);
    let conditions = project_conditions(item.pointer("/status/conditions"));
    let image_digests = image_digests(item.pointer("/spec/template/spec/containers"));
    let rollout_state = if observed_generation < generation {
        "progressing"
    } else if ready >= desired && updated >= desired {
        "ready"
    } else if conditions
        .iter()
        .any(|condition| condition.get("state").and_then(Value::as_str) == Some("failed"))
    {
        "failed"
    } else {
        "progressing"
    };
    json!({
        "schema_version": "rocketmq.kubernetes-workload.v1",
        "kind": kind.wire_name(),
        "name": item.pointer("/metadata/name"),
        "namespace": item.pointer("/metadata/namespace"),
        "labels": filtered_labels(item.pointer("/metadata/labels"), allowed_labels),
        "config_summary": config_summary(item, kind),
        "feature_manifest": feature_manifest(item),
        "release": release_summary(item),
        "generation": generation,
        "observed_generation": observed_generation,
        "desired_replicas": desired,
        "updated_replicas": updated,
        "ready_replicas": ready,
        "available_replicas": available,
        "unavailable_replicas": item.pointer("/status/unavailableReplicas"),
        "current_revision": item.pointer("/status/currentRevision"),
        "update_revision": item.pointer("/status/updateRevision"),
        "rollout_state": rollout_state,
        "image_digests": image_digests,
        "conditions": conditions
    })
}

fn project_conditions(raw: Option<&Value>) -> Vec<Value> {
    raw.and_then(Value::as_array)
        .map(|conditions| {
            conditions
                .iter()
                .map(|condition| {
                    let status = condition.get("status").and_then(Value::as_str);
                    let reason = condition.get("reason").and_then(Value::as_str);
                    let state = if status == Some("False")
                        && reason.is_some_and(|reason| {
                            matches!(
                                reason,
                                "ProgressDeadlineExceeded" | "ReplicaFailure" | "FailedCreate" | "FailedDelete"
                            )
                        }) {
                        "failed"
                    } else {
                        "observed"
                    };
                    json!({
                        "type": condition.get("type"),
                        "status": status,
                        "reason": reason,
                        "state": state,
                        "last_transition_time": condition.get("lastTransitionTime")
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn config_summary(item: &Value, kind: WorkloadKind) -> Value {
    match kind {
        WorkloadKind::Deployment => json!({
            "strategy": item.pointer("/spec/strategy/type"),
            "min_ready_seconds": item.pointer("/spec/minReadySeconds"),
            "revision_history_limit": item.pointer("/spec/revisionHistoryLimit"),
            "progress_deadline_seconds": item.pointer("/spec/progressDeadlineSeconds")
        }),
        WorkloadKind::StatefulSet => json!({
            "pod_management_policy": item.pointer("/spec/podManagementPolicy"),
            "update_strategy": item.pointer("/spec/updateStrategy/type"),
            "min_ready_seconds": item.pointer("/spec/minReadySeconds"),
            "revision_history_limit": item.pointer("/spec/revisionHistoryLimit")
        }),
    }
}

fn feature_manifest(item: &Value) -> BTreeMap<String, bool> {
    let mut features = BTreeMap::new();
    if let Some(labels) = item.pointer("/metadata/labels").and_then(Value::as_object) {
        for (key, value) in labels {
            let Some(feature) = key.strip_prefix("rocketmq.apache.org/feature-") else {
                continue;
            };
            if !valid_token(feature) {
                continue;
            }
            match value.as_str() {
                Some("true" | "enabled") => {
                    features.insert(feature.to_owned(), true);
                }
                Some("false" | "disabled") => {
                    features.insert(feature.to_owned(), false);
                }
                _ => {}
            }
        }
    }
    if let Some(csv) = item
        .pointer("/metadata/annotations/rocketmq.apache.org~1features")
        .and_then(Value::as_str)
    {
        for feature in csv
            .split(',')
            .map(str::trim)
            .filter(|value| valid_token(value))
            .take(32)
        {
            features.insert(feature.to_owned(), true);
        }
    }
    features
}

fn release_summary(item: &Value) -> Map<String, Value> {
    let mut release = Map::new();
    let candidates = [
        ("version", "/metadata/labels/app.kubernetes.io~1version"),
        (
            "config_generation",
            "/metadata/annotations/rocketmq.apache.org~1config-generation",
        ),
        (
            "rollout_revision",
            "/metadata/annotations/deployment.kubernetes.io~1revision",
        ),
        ("release_id", "/metadata/annotations/rocketmq.apache.org~1release-id"),
    ];
    for (name, pointer) in candidates {
        if let Some(value) = item
            .pointer(pointer)
            .and_then(Value::as_str)
            .filter(|value| valid_token(value))
        {
            release.insert(name.to_owned(), Value::String(value.to_owned()));
        }
    }
    release
}

fn image_digests(raw: Option<&Value>) -> Vec<String> {
    let mut digests = BTreeSet::new();
    if let Some(containers) = raw.and_then(Value::as_array) {
        for image in containers
            .iter()
            .filter_map(|container| container.get("image"))
            .filter_map(Value::as_str)
        {
            if let Some((_, digest)) = image.rsplit_once('@')
                && valid_digest(digest)
            {
                digests.insert(digest.to_owned());
            }
        }
    }
    digests.into_iter().collect()
}

fn valid_digest(value: &str) -> bool {
    value.len() == 71 && value.starts_with("sha256:") && value[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_projection_keeps_digest_and_rollout_but_drops_image_repository_and_env() {
        let digest = format!("sha256:{}", "a".repeat(64));
        let raw = json!({
            "metadata": {
                "name": "broker",
                "namespace": "rocketmq",
                "generation": 4,
                "labels": {
                    "rocketmq.apache.org/cluster": "local",
                    "rocketmq.apache.org/feature-tiered-store": "enabled"
                },
                "annotations": {
                    "deployment.kubernetes.io/revision": "7",
                    "rocketmq.apache.org/features": "tls,controller"
                }
            },
            "spec": {
                "replicas": 3,
                "strategy": {"type": "RollingUpdate"},
                "template": {"spec": {"containers": [{
                    "image": format!("private.registry/rocketmq:secret@{digest}"),
                    "env": [{"name": "PASSWORD", "value": "secret"}]
                }]}}
            },
            "status": {
                "observedGeneration": 4,
                "updatedReplicas": 3,
                "readyReplicas": 3,
                "availableReplicas": 3
            }
        });
        let projected = project_deployment(&raw, &BTreeSet::from(["rocketmq.apache.org/cluster".to_owned()]));
        assert_eq!(projected["rollout_state"], "ready");
        assert_eq!(projected["image_digests"], json!([digest]));
        assert_eq!(projected["feature_manifest"]["tiered-store"], true);
        assert_eq!(projected["feature_manifest"]["tls"], true);
        let encoded = serde_json::to_string(&projected).expect("projection");
        assert!(!encoded.contains("private.registry"));
        assert!(!encoded.contains("PASSWORD"));
        assert!(!encoded.contains("secret"));
    }

    #[test]
    fn certificate_projection_never_exposes_secret_name_or_pem() {
        let raw = json!({
            "metadata": {"name": "broker-tls", "namespace": "rocketmq"},
            "spec": {
                "secretName": "broker-tls-material",
                "issuerRef": {"kind": "ClusterIssuer", "name": "private-ca"}
            },
            "status": {
                "notAfter": "2026-10-01T00:00:00Z",
                "renewalTime": "2026-09-01T00:00:00Z",
                "conditions": [{"type": "Ready", "status": "True"}],
                "privateKey": "-----BEGIN PRIVATE KEY-----"
            }
        });
        let projected = project_certificate(&raw, &BTreeSet::new());
        assert_eq!(projected["not_after"], "2026-10-01T00:00:00Z");
        let encoded = serde_json::to_string(&projected).expect("projection");
        assert!(!encoded.contains("broker-tls-material"));
        assert!(!encoded.contains("private-ca"));
        assert!(!encoded.contains("PRIVATE KEY"));
    }
}

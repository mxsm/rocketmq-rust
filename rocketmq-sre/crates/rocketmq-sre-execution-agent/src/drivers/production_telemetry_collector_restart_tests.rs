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

use k8s_openapi::api::apps::v1::DeploymentSpec;
use k8s_openapi::api::apps::v1::DeploymentStatus;
use k8s_openapi::api::core::v1::PodCondition;
use k8s_openapi::api::core::v1::PodSpec;
use k8s_openapi::api::core::v1::PodStatus;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use super::*;

#[test]
fn simple_exact_selector_and_ready_state_are_accepted() {
    let deployment = ready_deployment();
    let pod = ready_pod();

    assert_eq!(
        deployment_selector(&deployment).expect("exact selector"),
        "app.kubernetes.io/name=otel-collector"
    );
    assert!(pod_matches_deployment(&pod, &deployment).expect("pod selector"));
    assert!(deployment_ready(&deployment).expect("deployment status"));
    assert!(pod_ready(&pod));
}

#[test]
fn selector_expressions_fail_closed() {
    let mut deployment = ready_deployment();
    deployment
        .spec
        .as_mut()
        .expect("deployment spec")
        .selector
        .match_expressions = Some(vec![LabelSelectorRequirement {
        key: "environment".to_owned(),
        operator: "In".to_owned(),
        values: Some(vec!["test".to_owned()]),
    }]);

    assert!(deployment_selector(&deployment).is_err());
}

#[test]
fn stale_deployment_or_unready_pod_is_not_healthy() {
    let mut deployment = ready_deployment();
    deployment
        .status
        .as_mut()
        .expect("deployment status")
        .observed_generation = Some(1);
    deployment.metadata.generation = Some(2);
    let mut pod = ready_pod();
    pod.status
        .as_mut()
        .expect("pod status")
        .conditions
        .as_mut()
        .expect("pod conditions")[0]
        .status = "False".to_owned();

    assert!(!deployment_ready(&deployment).expect("stale deployment"));
    assert!(!pod_ready(&pod));
}

fn ready_deployment() -> Deployment {
    Deployment {
        metadata: ObjectMeta {
            name: Some("otel-collector".to_owned()),
            namespace: Some("observability".to_owned()),
            generation: Some(1),
            ..ObjectMeta::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([(
                    "app.kubernetes.io/name".to_owned(),
                    "otel-collector".to_owned(),
                )])),
                ..LabelSelector::default()
            },
            template: k8s_openapi::api::core::v1::PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([(
                        "app.kubernetes.io/name".to_owned(),
                        "otel-collector".to_owned(),
                    )])),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    containers: Vec::new(),
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        }),
        status: Some(DeploymentStatus {
            observed_generation: Some(1),
            ready_replicas: Some(1),
            unavailable_replicas: Some(0),
            ..DeploymentStatus::default()
        }),
    }
}

fn ready_pod() -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: Some("otel-collector-before".to_owned()),
            uid: Some("collector-uid-before".to_owned()),
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/name".to_owned(),
                "otel-collector".to_owned(),
            )])),
            ..ObjectMeta::default()
        },
        spec: None,
        status: Some(PodStatus {
            conditions: Some(vec![PodCondition {
                last_probe_time: None,
                last_transition_time: None,
                message: None,
                observed_generation: None,
                reason: None,
                status: "True".to_owned(),
                type_: "Ready".to_owned(),
            }]),
            ..PodStatus::default()
        }),
    }
}

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
use std::sync::Arc;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Node;
use k8s_openapi::api::core::v1::ResourceQuota;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::Api;
use kube::Client;
use kube::api::ListParams;
use kube::api::PostParams;

use super::DriverFuture;
use super::ProxyScaleClient;
use super::ProxyScaleOutOneWrite;
use super::ProxyScaleRestore;
use super::ProxyScaleState;
use crate::ExecutionAgentError;

const OPERATION_ANNOTATION: &str = "rocketmq.apache.org/sre-scale-operation";
const EXECUTION_ANNOTATION: &str = "rocketmq.apache.org/sre-scale-execution";
const PLAN_STEP_ANNOTATION: &str = "rocketmq.apache.org/sre-scale-plan-step";

/// Production typed Kubernetes client for exactly one-replica Proxy scaling.
#[derive(Clone)]
pub(crate) struct ProductionProxyScaleClient {
    client: Client,
    allowed_targets: Arc<BTreeSet<String>>,
}

impl ProductionProxyScaleClient {
    pub(crate) async fn start(allowed_targets: BTreeSet<String>) -> Result<Self, ExecutionAgentError> {
        if allowed_targets.is_empty() {
            return Err(ExecutionAgentError::Configuration);
        }
        let client = Client::try_default()
            .await
            .map_err(|_| ExecutionAgentError::Configuration)?;
        Ok(Self {
            client,
            allowed_targets: Arc::new(allowed_targets),
        })
    }

    fn require_target(&self, namespace: &str, workload: &str) -> Result<(), ExecutionAgentError> {
        let target = format!("{namespace}/{workload}");
        if self.allowed_targets.contains(&target) {
            Ok(())
        } else {
            Err(ExecutionAgentError::InvalidRequest)
        }
    }

    async fn deployment(&self, namespace: &str, workload: &str) -> Result<Deployment, ExecutionAgentError> {
        self.require_target(namespace, workload)?;
        Api::<Deployment>::namespaced(self.client.clone(), namespace)
            .get(workload)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn quota_available(&self, namespace: &str) -> Result<bool, ExecutionAgentError> {
        let quotas = Api::<ResourceQuota>::namespaced(self.client.clone(), namespace)
            .list(&ListParams::default())
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        Ok(quotas.items.iter().all(quota_has_room_for_one_pod))
    }

    async fn capacity_available(&self) -> Result<bool, ExecutionAgentError> {
        let nodes = Api::<Node>::all(self.client.clone())
            .list(&ListParams::default())
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        Ok(nodes.items.iter().any(node_accepts_new_pods))
    }

    async fn pdb_healthy(&self, namespace: &str, deployment: &Deployment) -> Result<bool, ExecutionAgentError> {
        let labels = deployment
            .spec
            .as_ref()
            .and_then(|spec| spec.template.metadata.as_ref())
            .and_then(|metadata| metadata.labels.as_ref())
            .ok_or(ExecutionAgentError::DriverFailed)?;
        let budgets = Api::<PodDisruptionBudget>::namespaced(self.client.clone(), namespace)
            .list(&ListParams::default())
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        let matching = budgets
            .items
            .iter()
            .filter(|budget| {
                budget
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.selector.as_ref())
                    .is_some_and(|selector| selector_matches(selector, labels))
            })
            .collect::<Vec<_>>();
        Ok(!matching.is_empty() && matching.into_iter().all(pdb_status_healthy))
    }

    async fn replace_replicas(
        &self,
        namespace: &str,
        workload: &str,
        expected_replicas: u32,
        target_replicas: u32,
        operation_id: &str,
        execution_id: &str,
        plan_step_id: &str,
        required_execution_id: Option<&str>,
        required_plan_step_id: Option<&str>,
    ) -> Result<(), ExecutionAgentError> {
        let mut deployment = self.deployment(namespace, workload).await?;
        let current = desired_replicas(&deployment)?;
        if current != expected_replicas {
            return Err(ExecutionAgentError::DriverFailed);
        }
        if let Some(required) = required_execution_id
            && deployment
                .metadata
                .annotations
                .as_ref()
                .and_then(|value| value.get(EXECUTION_ANNOTATION))
                .map(String::as_str)
                != Some(required)
        {
            return Err(ExecutionAgentError::DriverFailed);
        }
        if let Some(required) = required_plan_step_id
            && deployment
                .metadata
                .annotations
                .as_ref()
                .and_then(|value| value.get(PLAN_STEP_ANNOTATION))
                .map(String::as_str)
                != Some(required)
        {
            return Err(ExecutionAgentError::DriverFailed);
        }
        let target = i32::try_from(target_replicas).map_err(|_| ExecutionAgentError::InvalidRequest)?;
        deployment
            .spec
            .as_mut()
            .ok_or(ExecutionAgentError::DriverFailed)?
            .replicas = Some(target);
        let annotations = deployment.metadata.annotations.get_or_insert_with(BTreeMap::new);
        annotations.insert(OPERATION_ANNOTATION.to_owned(), operation_id.to_owned());
        annotations.insert(EXECUTION_ANNOTATION.to_owned(), execution_id.to_owned());
        annotations.insert(PLAN_STEP_ANNOTATION.to_owned(), plan_step_id.to_owned());
        let stored = Api::<Deployment>::namespaced(self.client.clone(), namespace)
            .replace(workload, &PostParams::default(), &deployment)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        if desired_replicas(&stored)? != target_replicas
            || annotation(&stored, OPERATION_ANNOTATION) != Some(operation_id)
            || annotation(&stored, EXECUTION_ANNOTATION) != Some(execution_id)
            || annotation(&stored, PLAN_STEP_ANNOTATION) != Some(plan_step_id)
        {
            return Err(ExecutionAgentError::DriverUnknown);
        }
        Ok(())
    }
}

impl ProxyScaleClient for ProductionProxyScaleClient {
    fn proxy_scale_state<'a>(&'a self, namespace: &'a str, workload: &'a str) -> DriverFuture<'a, ProxyScaleState> {
        Box::pin(async move {
            let deployment = self.deployment(namespace, workload).await?;
            let desired_replicas = desired_replicas(&deployment)?;
            let status = deployment.status.as_ref().ok_or(ExecutionAgentError::DriverFailed)?;
            Ok(ProxyScaleState {
                desired_replicas,
                ready_replicas: non_negative(status.ready_replicas)?,
                unavailable_replicas: non_negative(status.unavailable_replicas)?,
                quota_available: self.quota_available(namespace).await?,
                capacity_available: self.capacity_available().await?,
                pdb_healthy: self.pdb_healthy(namespace, &deployment).await?,
                last_operation_id: annotation(&deployment, OPERATION_ANNOTATION).map(str::to_owned),
            })
        })
    }

    fn scale_out_one<'a>(&'a self, request: &'a ProxyScaleOutOneWrite) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            if request.target_replicas != request.expected_replicas.saturating_add(1) {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            self.replace_replicas(
                &request.namespace,
                &request.workload,
                request.expected_replicas,
                request.target_replicas,
                &request.operation_id,
                &request.execution_id.to_string(),
                &request.plan_step_id.to_string(),
                None,
                None,
            )
            .await
        })
    }

    fn restore_proxy_replicas<'a>(&'a self, request: &'a ProxyScaleRestore) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let scaled_replicas = request
                .original_replicas
                .checked_add(1)
                .ok_or(ExecutionAgentError::InvalidRequest)?;
            let execution_id = request.execution_id.to_string();
            let plan_step_id = request.plan_step_id.to_string();
            self.replace_replicas(
                &request.namespace,
                &request.workload,
                scaled_replicas,
                request.original_replicas,
                &request.operation_id,
                &execution_id,
                &plan_step_id,
                Some(&execution_id),
                Some(&plan_step_id),
            )
            .await
        })
    }
}

fn desired_replicas(deployment: &Deployment) -> Result<u32, ExecutionAgentError> {
    deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.replicas)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(ExecutionAgentError::DriverFailed)
}

fn non_negative(value: Option<i32>) -> Result<u32, ExecutionAgentError> {
    value
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(ExecutionAgentError::DriverFailed)
}

fn annotation<'a>(deployment: &'a Deployment, key: &str) -> Option<&'a str> {
    deployment
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(key))
        .map(String::as_str)
}

fn quota_has_room_for_one_pod(quota: &ResourceQuota) -> bool {
    let Some(status) = quota.status.as_ref() else {
        return false;
    };
    let Some(hard) = status.hard.as_ref() else {
        return false;
    };
    let Some(used) = status.used.as_ref() else {
        return false;
    };
    hard.iter().all(|(resource, limit)| {
        if resource == "pods" {
            let Some(limit) = parse_integer_quantity(&limit.0) else {
                return false;
            };
            let Some(used) = used
                .get(resource)
                .and_then(|quantity| parse_integer_quantity(&quantity.0))
            else {
                return false;
            };
            used.checked_add(1).is_some_and(|next| next <= limit)
        } else {
            resource.starts_with("count/")
        }
    })
}

fn parse_integer_quantity(value: &str) -> Option<u64> {
    (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| value.parse().ok())
        .flatten()
}

fn node_accepts_new_pods(node: &Node) -> bool {
    if node.spec.as_ref().is_some_and(|spec| spec.unschedulable == Some(true)) {
        return false;
    }
    let Some(conditions) = node.status.as_ref().and_then(|status| status.conditions.as_ref()) else {
        return false;
    };
    conditions
        .iter()
        .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        && conditions.iter().all(|condition| {
            !matches!(
                condition.type_.as_str(),
                "DiskPressure" | "MemoryPressure" | "PIDPressure"
            ) || condition.status != "True"
        })
}

fn selector_matches(selector: &LabelSelector, labels: &BTreeMap<String, String>) -> bool {
    let labels_match = selector
        .match_labels
        .as_ref()
        .is_none_or(|required| required.iter().all(|(key, value)| labels.get(key) == Some(value)));
    let expressions_match = selector.match_expressions.as_ref().is_none_or(|requirements| {
        requirements.iter().all(|requirement| {
            let values = requirement.values.as_deref().unwrap_or_default();
            match requirement.operator.as_str() {
                "In" => labels
                    .get(&requirement.key)
                    .is_some_and(|value| values.iter().any(|candidate| candidate == value)),
                "NotIn" => labels
                    .get(&requirement.key)
                    .is_some_and(|value| values.iter().all(|candidate| candidate != value)),
                "Exists" => labels.contains_key(&requirement.key),
                "DoesNotExist" => !labels.contains_key(&requirement.key),
                _ => false,
            }
        })
    });
    labels_match && expressions_match
}

fn pdb_status_healthy(budget: &PodDisruptionBudget) -> bool {
    let Some(status) = budget.status.as_ref() else {
        return false;
    };
    let generation_current = budget
        .metadata
        .generation
        .zip(status.observed_generation)
        .is_some_and(|(generation, observed)| generation == observed);
    let health_current = status
        .current_healthy
        .zip(status.desired_healthy)
        .is_some_and(|(current, desired)| current >= desired);
    let no_pending_disruptions = status.disrupted_pods.as_ref().is_none_or(BTreeMap::is_empty);
    generation_current && health_current && no_pending_disruptions
}

#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::NodeCondition;
    use k8s_openapi::api::core::v1::NodeSpec;
    use k8s_openapi::api::core::v1::NodeStatus;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;

    use super::*;

    #[test]
    fn selectors_support_the_closed_kubernetes_operators() {
        let labels = BTreeMap::from([
            ("app".to_owned(), "proxy".to_owned()),
            ("tier".to_owned(), "messaging".to_owned()),
        ]);
        let selector = LabelSelector {
            match_labels: Some(BTreeMap::from([("app".to_owned(), "proxy".to_owned())])),
            match_expressions: Some(vec![
                LabelSelectorRequirement {
                    key: "tier".to_owned(),
                    operator: "In".to_owned(),
                    values: Some(vec!["messaging".to_owned()]),
                },
                LabelSelectorRequirement {
                    key: "debug".to_owned(),
                    operator: "DoesNotExist".to_owned(),
                    values: None,
                },
            ]),
        };
        assert!(selector_matches(&selector, &labels));
    }

    #[test]
    fn node_capacity_requires_ready_schedulable_and_no_pressure() {
        let mut node = Node {
            spec: Some(NodeSpec {
                unschedulable: Some(false),
                ..NodeSpec::default()
            }),
            status: Some(NodeStatus {
                conditions: Some(vec![
                    NodeCondition {
                        status: "True".to_owned(),
                        type_: "Ready".to_owned(),
                        ..NodeCondition::default()
                    },
                    NodeCondition {
                        status: "False".to_owned(),
                        type_: "MemoryPressure".to_owned(),
                        ..NodeCondition::default()
                    },
                ]),
                ..NodeStatus::default()
            }),
            ..Node::default()
        };
        assert!(node_accepts_new_pods(&node));
        node.status
            .as_mut()
            .expect("status")
            .conditions
            .as_mut()
            .expect("conditions")[1]
            .status = "True".to_owned();
        assert!(!node_accepts_new_pods(&node));
    }

    #[test]
    fn integer_quota_parser_rejects_ambiguous_units() {
        assert_eq!(parse_integer_quantity("12"), Some(12));
        assert_eq!(parse_integer_quantity("1200m"), None);
        assert_eq!(parse_integer_quantity("-1"), None);
    }
}

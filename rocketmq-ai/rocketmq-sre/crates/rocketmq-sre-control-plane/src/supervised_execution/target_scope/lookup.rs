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

use rocketmq_sre_contracts::ExecutionAction;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Actual request-controlled read selectors in the existing enabled drivers.
/// Target literals remain exact echoes; no namespace or address is inferred
/// from a display alias. Agent-private namespace/probe configuration is not an
/// independent CP observation and remains outside this authorization declaration.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum ReadLookup {
    Logger {
        component: String,
        logger: String,
    },
    Broker {
        broker: String,
    },
    Topic {
        topic: String,
    },
    SubscriptionGroup {
        group: String,
    },
    Workload {
        namespace: String,
        workload: String,
    },
    Pod {
        namespace: String,
        pod: String,
    },
    Canary {
        namespace: String,
        workload: String,
        container: String,
    },
    Telemetry {
        namespace: String,
        pod: String,
        pipeline: String,
    },
    CredentialSet {
        credential_set: String,
    },
}

impl ReadLookup {
    pub(super) fn valid_for(&self, action: ExecutionAction) -> bool {
        let compatible = match action {
            ExecutionAction::ObservabilityLoggerLevelTtl => {
                matches!(self, Self::Logger { component, .. } if component == "broker")
            }
            ExecutionAction::BrokerConfigPatchAllowlisted => matches!(self, Self::Broker { .. }),
            ExecutionAction::TopicConfigPatchAllowlisted => matches!(self, Self::Topic { .. }),
            ExecutionAction::SubscriptionGroupPatchAllowlisted => matches!(self, Self::SubscriptionGroup { .. }),
            ExecutionAction::ProxyScaleOutOne => matches!(self, Self::Workload { .. }),
            ExecutionAction::ProxyRestartOne => matches!(self, Self::Pod { .. }),
            ExecutionAction::ProxyRolloutImageCanary => matches!(self, Self::Canary { .. }),
            ExecutionAction::TelemetryCollectorRestartOne => matches!(self, Self::Telemetry { .. }),
            ExecutionAction::SecurityCredentialRotateOverlap => matches!(self, Self::CredentialSet { .. }),
            ExecutionAction::ConsumerRequestModePatchAllowlisted
            | ExecutionAction::ConsumerOffsetResetBounded
            | ExecutionAction::TopicQueueExpandOnly
            | ExecutionAction::NameSrvConfigPatchAllowlisted
            | ExecutionAction::ControllerConfigPatchAllowlisted
            | ExecutionAction::BrokerRestartOne
            | ExecutionAction::StaticTopicPatchNonRemap
            | ExecutionAction::TieredColdDataFlowPatchAllowlisted
            | ExecutionAction::StoreReadaheadPatchAllowlisted
            | ExecutionAction::ConsumerOffsetCloneOrResetBroad
            | ExecutionAction::MessageDirectConsume
            | ExecutionAction::MessageDlqResend
            | ExecutionAction::TimerSwitch
            | ExecutionAction::ControllerElect
            | ExecutionAction::StaticTopicRemap
            | ExecutionAction::BrokerContainerAddRemove => false,
        };
        compatible
            && self
                .fields()
                .iter()
                .all(|(_, value)| super::bounded_literal(value, 512))
    }

    pub(super) fn matches_parameters(&self, parameters: &Value) -> bool {
        self.fields()
            .iter()
            .all(|(key, expected)| parameters.get(*key).and_then(Value::as_str) == Some(*expected))
    }

    fn fields(&self) -> Vec<(&'static str, &str)> {
        match self {
            Self::Logger { component, logger } => vec![("component", component), ("logger", logger)],
            Self::Broker { broker } => vec![("broker", broker)],
            Self::Topic { topic } => vec![("topic", topic)],
            Self::SubscriptionGroup { group } => vec![("group", group)],
            Self::Workload { namespace, workload } => vec![("namespace", namespace), ("workload", workload)],
            Self::Pod { namespace, pod } => vec![("namespace", namespace), ("pod", pod)],
            Self::Canary {
                namespace,
                workload,
                container,
            } => vec![
                ("namespace", namespace),
                ("workload", workload),
                ("container", container),
            ],
            Self::Telemetry {
                namespace,
                pod,
                pipeline,
            } => vec![("namespace", namespace), ("pod", pod), ("pipeline", pipeline)],
            Self::CredentialSet { credential_set } => vec![("credential_set", credential_set)],
        }
    }
}

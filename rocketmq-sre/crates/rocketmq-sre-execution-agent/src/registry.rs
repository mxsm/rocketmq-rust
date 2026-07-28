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
use std::sync::Arc;

use rocketmq_sre_contracts::AgentStepRequest;
use rocketmq_sre_contracts::ExecutionAction;

use crate::AdminCoreDriver;
use crate::AgentActionHandler;
use crate::ConfigDriver;
use crate::ExecutionAgentError;
use crate::KubernetesDriver;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::ExecutionAgentCapabilities;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverFamily {
    AdminCore,
    Kubernetes,
    Config,
}

#[derive(Clone)]
struct RegisteredHandler {
    family: DriverFamily,
    handler: Arc<dyn AgentActionHandler>,
}

/// Closed action-to-driver registry. Plan-only, R3, and unknown action IDs
/// cannot be registered.
#[derive(Clone, Default)]
pub struct AgentDriverRegistry {
    handlers: BTreeMap<ExecutionAction, RegisteredHandler>,
}

impl AgentDriverRegistry {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Registers one exact Admin action.
    ///
    /// # Errors
    ///
    /// Rejects wrong driver families and duplicate registrations.
    pub fn register_admin<T>(&mut self, action: ExecutionAction, handler: T) -> Result<(), ExecutionAgentError>
    where
        T: AdminCoreDriver + 'static,
    {
        self.register(action, DriverFamily::AdminCore, Arc::new(handler))
    }

    /// Registers one exact Kubernetes action.
    ///
    /// # Errors
    ///
    /// Rejects wrong driver families and duplicate registrations.
    pub fn register_kubernetes<T>(&mut self, action: ExecutionAction, handler: T) -> Result<(), ExecutionAgentError>
    where
        T: KubernetesDriver + 'static,
    {
        self.register(action, DriverFamily::Kubernetes, Arc::new(handler))
    }

    /// Registers one exact configuration action.
    ///
    /// # Errors
    ///
    /// Rejects wrong driver families and duplicate registrations.
    pub fn register_config<T>(&mut self, action: ExecutionAction, handler: T) -> Result<(), ExecutionAgentError>
    where
        T: ConfigDriver + 'static,
    {
        self.register(action, DriverFamily::Config, Arc::new(handler))
    }

    fn register(
        &mut self,
        action: ExecutionAction,
        family: DriverFamily,
        handler: Arc<dyn AgentActionHandler>,
    ) -> Result<(), ExecutionAgentError> {
        if expected_family(action) != Some(family) || self.handlers.contains_key(&action) {
            return Err(ExecutionAgentError::InvalidRequest);
        }
        self.handlers.insert(action, RegisteredHandler { family, handler });
        Ok(())
    }

    pub(crate) fn handler(&self, action: ExecutionAction) -> Result<Arc<dyn AgentActionHandler>, ExecutionAgentError> {
        self.handlers
            .get(&action)
            .map(|registered| Arc::clone(&registered.handler))
            .ok_or(ExecutionAgentError::ActionNotRegistered)
    }

    pub(crate) fn validate_read(&self, request: &AgentReadRequest) -> Result<(), ExecutionAgentError> {
        if request.schema_version != EXECUTION_AGENT_SCHEMA_VERSION
            || request.descriptor_version.trim().is_empty()
            || request.target.trim().is_empty()
            || request.parameters.as_object().is_none()
        {
            return Err(ExecutionAgentError::InvalidRequest);
        }
        let registered = self
            .handlers
            .get(&request.action)
            .ok_or(ExecutionAgentError::ActionNotRegistered)?;
        if Some(registered.family) != expected_family(request.action) {
            return Err(ExecutionAgentError::InvalidRequest);
        }
        Ok(())
    }

    pub(crate) fn validate_dispatch(&self, request: &AgentStepRequest) -> Result<(), ExecutionAgentError> {
        if request.descriptor_version.trim().is_empty()
            || request.target.trim().is_empty()
            || request.parameters.as_object().is_none()
            || request.intent.fence_grant.execution_id != request.intent.execution_id
            || request.intent.fence_grant.step_id != request.intent.step_id
            || request.intent.fence_grant.plan_step_id != request.intent.step.id
            || request.intent.fence_grant.action != request.action
            || request.intent.fence_grant.resource != request.target
            || request.action != request.intent.step.action
            || request.target != request.intent.step.resource
            || request.parameters != request.intent.step.parameters
        {
            return Err(ExecutionAgentError::InvalidRequest);
        }
        self.handler(request.action).map(|_| ())
    }

    #[must_use]
    pub fn capabilities(&self) -> ExecutionAgentCapabilities {
        ExecutionAgentCapabilities {
            schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
            registered_actions: self.handlers.keys().copied().collect(),
            raw_admin_request_supported: false,
            arbitrary_json_patch_supported: false,
            shell_supported: false,
            durable_fencing: true,
        }
    }
}

const fn expected_family(action: ExecutionAction) -> Option<DriverFamily> {
    match action {
        ExecutionAction::ObservabilityLoggerLevelTtl | ExecutionAction::SecurityCredentialRotateOverlap => {
            Some(DriverFamily::Config)
        }
        ExecutionAction::ProxyScaleOutOne
        | ExecutionAction::ProxyRestartOne
        | ExecutionAction::ProxyRolloutImageCanary
        | ExecutionAction::BrokerRestartOne
        | ExecutionAction::TelemetryCollectorRestartOne => Some(DriverFamily::Kubernetes),
        ExecutionAction::BrokerConfigPatchAllowlisted
        | ExecutionAction::TopicConfigPatchAllowlisted
        | ExecutionAction::SubscriptionGroupPatchAllowlisted
        | ExecutionAction::ConsumerRequestModePatchAllowlisted
        | ExecutionAction::ConsumerOffsetResetBounded
        | ExecutionAction::TopicQueueExpandOnly
        | ExecutionAction::NameSrvConfigPatchAllowlisted
        | ExecutionAction::ControllerConfigPatchAllowlisted
        | ExecutionAction::StaticTopicPatchNonRemap
        | ExecutionAction::TieredColdDataFlowPatchAllowlisted
        | ExecutionAction::StoreReadaheadPatchAllowlisted => Some(DriverFamily::AdminCore),
        ExecutionAction::ConsumerOffsetCloneOrResetBroad
        | ExecutionAction::MessageDirectConsume
        | ExecutionAction::MessageDlqResend
        | ExecutionAction::TimerSwitch
        | ExecutionAction::ControllerElect
        | ExecutionAction::StaticTopicRemap
        | ExecutionAction::BrokerContainerAddRemove => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wave_three_has_no_agent_driver_family() {
        for action in ExecutionAction::WAVE3_PLAN_ONLY {
            assert_eq!(expected_family(action), None);
        }
    }

    #[test]
    fn representative_wave_two_actions_have_distinct_typed_families() {
        assert_eq!(
            expected_family(ExecutionAction::SubscriptionGroupPatchAllowlisted),
            Some(DriverFamily::AdminCore)
        );
        assert_eq!(
            expected_family(ExecutionAction::ProxyRolloutImageCanary),
            Some(DriverFamily::Kubernetes)
        );
        assert_eq!(
            expected_family(ExecutionAction::SecurityCredentialRotateOverlap),
            Some(DriverFamily::Config)
        );
    }
}

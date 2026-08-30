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

use std::sync::Arc;

use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::is_sha256_digest;

use crate::ExecutionAgentClient;
use crate::ExecutorActionRegistry;
use crate::ExecutorError;

/// Deterministic local and live-state validation performed before persistence
/// of a mutation intent.
#[derive(Clone)]
pub struct ExecutionPrechecker {
    registry: Arc<ExecutorActionRegistry>,
    agent: Arc<dyn ExecutionAgentClient>,
}

impl ExecutionPrechecker {
    #[must_use]
    pub fn new(registry: Arc<ExecutorActionRegistry>, agent: Arc<dyn ExecutionAgentClient>) -> Self {
        Self { registry, agent }
    }

    #[must_use]
    pub(crate) fn registry(&self) -> &ExecutorActionRegistry {
        &self.registry
    }

    /// Revalidates every plan step and compares its approved precondition hash
    /// to a fresh typed Agent read.
    ///
    /// # Errors
    ///
    /// Rejects descriptor drift, disabled actions, unsafe parameters, Agent
    /// readiness failures, malformed hashes, and any precondition change.
    pub async fn check(&self, request: &ExecutionRequest) -> Result<Vec<AgentReadResult>, ExecutorError> {
        let mut results = Vec::with_capacity(request.plan.steps.len());
        for step in &request.plan.steps {
            self.registry.validate_step(step)?;
            let result = self
                .agent
                .precheck(&AgentReadRequest {
                    schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                    tenant_id: request.tenant_id,
                    cluster_id: request.cluster_id,
                    execution_id: request.id,
                    plan_step_id: step.id,
                    action: step.action,
                    descriptor_version: step.descriptor_version.clone(),
                    target: step.resource.clone(),
                    parameters: step.parameters.clone(),
                })
                .await?;
            if !result.ready
                || !is_sha256_digest(&result.precondition_hash)
                || result.precondition_hash != step.precondition_hash
            {
                return Err(ExecutorError::PreconditionChanged);
            }
            results.push(result);
        }
        Ok(results)
    }
}

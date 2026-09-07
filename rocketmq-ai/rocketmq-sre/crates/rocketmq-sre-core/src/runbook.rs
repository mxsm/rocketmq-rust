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

use rocketmq_sre_contracts::SreContractError;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::ChangeSchedule;
use rocketmq_sre_contracts::ContractJsonValue;
use rocketmq_sre_contracts::DescriptorVersion;
use rocketmq_sre_contracts::RunbookDefinition;
use rocketmq_sre_contracts::RunbookStep;
use rocketmq_sre_contracts::RunbookStepBody;
use rocketmq_sre_contracts::RunbookStepId;
use rocketmq_sre_contracts::is_sha256_digest;

use crate::ActionCatalog;

/// Validates a composite runbook against the exact embedded Action Catalog.
pub struct RunbookValidator;

impl RunbookValidator {
    /// Validates identity, DAG ordering, manual gates, typed action versions,
    /// parameter boundaries, compensation edges, parallelism, and aggregate
    /// risk.
    ///
    /// # Errors
    ///
    /// Rejects unknown/plan-only actions, shell or raw mutation fields,
    /// dependency cycles, invalid gates, unsafe parallel groups, and risk
    /// understatement.
    pub fn validate(definition: &RunbookDefinition, catalog: &ActionCatalog) -> Result<(), SreContractError> {
        validate_header(definition)?;
        if !definition
            .steps
            .iter()
            .any(|step| matches!(step.body, RunbookStepBody::Action { .. }))
        {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
        let steps = index_steps(&definition.steps)?;
        let mut highest_risk = ActionRisk::R1;
        let mut parallel_groups: BTreeMap<&str, Vec<&RunbookStep>> = BTreeMap::new();
        let mut action_resources = BTreeSet::new();
        for (index, step) in definition.steps.iter().enumerate() {
            validate_step(index, step, &steps, catalog, &mut highest_risk)?;
            if let Some(group) = step.parallel_group.as_deref() {
                parallel_groups.entry(group).or_default().push(step);
            }
            if let RunbookStepBody::Action { resource, .. } = &step.body {
                action_resources.insert(resource.as_str());
            }
        }
        validate_parallel_groups(definition.max_parallelism, &parallel_groups)?;
        validate_compensation_edges(definition, &steps)?;
        if definition.max_parallelism > 1 || action_resources.len() > 1 {
            highest_risk = highest_risk.max(ActionRisk::R2);
        }
        if definition.risk < highest_risk {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
        Ok(())
    }

    /// Validates that every action step is bound exactly once to an approved
    /// plan identity while manual gates remain outside the execution surface.
    ///
    /// # Errors
    ///
    /// Rejects mismatched runbooks, missing/duplicate/extra bindings, invalid
    /// plan digests, or inconsistent scheduler projections.
    pub fn validate_schedule_bindings(
        definition: &RunbookDefinition,
        schedule: &ChangeSchedule,
    ) -> Result<(), SreContractError> {
        if schedule.runbook_id != definition.id
            || schedule.runbook_version != definition.version
            || schedule.next_step_sequence == 0
            || usize::from(schedule.next_step_sequence) > definition.steps.len() + 1
            || (schedule.active_execution_id.is_some() && schedule.waiting_manual_gate.is_some())
        {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
        let all_steps = definition.steps.iter().map(|step| step.id).collect::<BTreeSet<_>>();
        if !schedule.completed_steps.is_subset(&all_steps) {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
        if let Some(waiting) = schedule.waiting_manual_gate {
            let is_gate = definition
                .steps
                .iter()
                .any(|step| step.id == waiting && matches!(step.body, RunbookStepBody::ManualGate { .. }));
            if !is_gate {
                return Err(rocketmq_sre_contracts::SreContractError::new(
                    rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
                ));
            }
        }
        let action_steps = definition
            .steps
            .iter()
            .filter_map(|step| matches!(step.body, RunbookStepBody::Action { .. }).then_some(step.id))
            .collect::<BTreeSet<_>>();
        let mut bound_steps = BTreeSet::new();
        for binding in &schedule.plan_bindings {
            if binding.plan_id.as_uuid().is_nil()
                || !action_steps.contains(&binding.step_id)
                || !bound_steps.insert(binding.step_id)
                || !is_sha256_digest(&binding.plan_hash)
                || !is_sha256_digest(&binding.precondition_hash)
            {
                return Err(rocketmq_sre_contracts::SreContractError::new(
                    rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
                ));
            }
        }
        if action_steps != bound_steps {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
        Ok(())
    }
}

fn validate_header(definition: &RunbookDefinition) -> Result<(), SreContractError> {
    if definition.schema_version != RunbookDefinition::SCHEMA_VERSION
        || definition.id.as_uuid().is_nil()
        || definition.name.trim().is_empty()
        || definition.name.chars().count() > 128
        || definition.owner.trim().is_empty()
        || definition.owner.chars().count() > 128
        || definition.description.trim().is_empty()
        || definition.description.chars().count() > 2048
        || DescriptorVersion::parse(&definition.version).is_err()
        || !(1..=16).contains(&definition.max_parallelism)
        || definition.steps.is_empty()
        || definition.steps.len() > 64
        || !matches!(definition.risk, ActionRisk::R1 | ActionRisk::R2)
    {
        return Err(rocketmq_sre_contracts::SreContractError::new(
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
        ));
    }
    Ok(())
}

fn index_steps(steps: &[RunbookStep]) -> Result<BTreeMap<RunbookStepId, &RunbookStep>, SreContractError> {
    let mut indexed = BTreeMap::new();
    for (index, step) in steps.iter().enumerate() {
        let expected_sequence = u16::try_from(index + 1).map_err(|_| {
            rocketmq_sre_contracts::SreContractError::new(rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor)
        })?;
        if step.id.as_uuid().is_nil() || step.sequence != expected_sequence || indexed.insert(step.id, step).is_some() {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
    }
    Ok(indexed)
}

fn validate_step(
    index: usize,
    step: &RunbookStep,
    steps: &BTreeMap<RunbookStepId, &RunbookStep>,
    catalog: &ActionCatalog,
    highest_risk: &mut ActionRisk,
) -> Result<(), SreContractError> {
    if step.name.trim().is_empty()
        || step.name.chars().count() > 128
        || step.depends_on.contains(&step.id)
        || step.depends_on.iter().any(|dependency| {
            steps
                .get(dependency)
                .is_none_or(|candidate| candidate.sequence >= step.sequence)
        })
    {
        return Err(rocketmq_sre_contracts::SreContractError::new(
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
        ));
    }
    if index > 0 && step.parallel_group.is_none() {
        let previous = steps
            .values()
            .find(|candidate| candidate.sequence + 1 == step.sequence)
            .ok_or_else(|| {
                rocketmq_sre_contracts::SreContractError::new(
                    rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
                )
            })?;
        if !step.depends_on.contains(&previous.id) {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
    }
    match &step.body {
        RunbookStepBody::Action {
            action,
            descriptor_version,
            resource,
            parameters,
        } => {
            if resource.trim().is_empty()
                || resource.chars().count() > 512
                || resource.chars().any(char::is_control)
                || parameters.as_object().is_none()
            {
                return Err(rocketmq_sre_contracts::SreContractError::new(
                    rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
                ));
            }
            let descriptor = catalog.descriptor(*action, descriptor_version)?;
            if descriptor.plan_only {
                return Err(rocketmq_sre_contracts::SreContractError::new(
                    rocketmq_sre_contracts::PublicErrorCode::DescriptorNotFound,
                ));
            }
            *highest_risk = (*highest_risk).max(descriptor.risk);
            validate_parameter_fields(parameters, &descriptor.forbidden_fields)?;
        }
        RunbookStepBody::ManualGate { gate } => {
            if gate.gate_id.trim().is_empty()
                || gate.gate_id.chars().count() > 128
                || gate.title.trim().is_empty()
                || gate.title.chars().count() > 128
                || gate.instructions.trim().is_empty()
                || gate.instructions.chars().count() > 2048
                || gate.required_role.trim().is_empty()
                || gate.required_role.chars().count() > 128
                || gate.timeout_seconds == 0
                || gate.timeout_seconds > 86400
            {
                return Err(rocketmq_sre_contracts::SreContractError::new(
                    rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
                ));
            }
        }
    }
    if let Some(condition) = &step.condition
        && (condition.fact.trim().is_empty()
            || condition.fact.chars().count() > 128
            || condition.fact.chars().any(char::is_control))
    {
        return Err(rocketmq_sre_contracts::SreContractError::new(
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
        ));
    }
    Ok(())
}

fn validate_parameter_fields(
    value: &ContractJsonValue,
    descriptor_forbidden: &BTreeSet<String>,
) -> Result<(), SreContractError> {
    if let Some(values) = value.as_object() {
        for (field, value) in values {
            let normalized = field.to_ascii_lowercase();
            if descriptor_forbidden.contains(field)
                || [
                    "shell",
                    "command",
                    "args",
                    "raw_request_code",
                    "json_patch",
                    "arbitrary_patch",
                ]
                .contains(&normalized.as_str())
            {
                return Err(rocketmq_sre_contracts::SreContractError::new(
                    rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
                ));
            }
            validate_parameter_fields(value, descriptor_forbidden)?;
        }
    } else if let Some(values) = value.as_array() {
        for value in values {
            validate_parameter_fields(value, descriptor_forbidden)?;
        }
    }
    Ok(())
}

fn validate_parallel_groups(
    max_parallelism: u16,
    groups: &BTreeMap<&str, Vec<&RunbookStep>>,
) -> Result<(), SreContractError> {
    for (name, steps) in groups {
        if name.trim().is_empty()
            || name.chars().count() > 128
            || steps.len() > usize::from(max_parallelism)
            || steps.iter().any(|step| {
                steps
                    .iter()
                    .any(|peer| step.id != peer.id && step.depends_on.contains(&peer.id))
            })
        {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
        let resources = steps
            .iter()
            .filter_map(|step| match &step.body {
                RunbookStepBody::Action { resource, .. } => Some(resource),
                RunbookStepBody::ManualGate { .. } => None,
            })
            .collect::<BTreeSet<_>>();
        if resources.len() != steps.len() {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
    }
    Ok(())
}

fn validate_compensation_edges(
    definition: &RunbookDefinition,
    steps: &BTreeMap<RunbookStepId, &RunbookStep>,
) -> Result<(), SreContractError> {
    let mut edges = BTreeSet::new();
    for edge in &definition.compensation_edges {
        if edge.from_step == edge.compensation_step
            || !steps.contains_key(&edge.from_step)
            || !steps.contains_key(&edge.compensation_step)
            || !edges.insert((edge.from_step, edge.compensation_step, edge.trigger))
        {
            return Err(rocketmq_sre_contracts::SreContractError::new(
                rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
            ));
        }
    }
    Ok(())
}

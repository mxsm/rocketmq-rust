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
use std::error::Error;
use std::fmt;

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

/// Fail-closed composite runbook validation error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunbookError {
    InvalidDefinition(String),
    UnknownAction(String),
    RiskUnderstated,
    UnsafeParameter(String),
}

impl fmt::Display for RunbookError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDefinition(reason) => write!(formatter, "invalid runbook: {reason}"),
            Self::UnknownAction(action) => write!(formatter, "runbook action is not registered: {action}"),
            Self::RiskUnderstated => {
                formatter.write_str("runbook risk is below its action, resource-scope, or concurrency upper bound")
            }
            Self::UnsafeParameter(field) => write!(formatter, "runbook parameter is forbidden: {field}"),
        }
    }
}

impl Error for RunbookError {}

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
    pub fn validate(definition: &RunbookDefinition, catalog: &ActionCatalog) -> Result<(), RunbookError> {
        validate_header(definition)?;
        if !definition
            .steps
            .iter()
            .any(|step| matches!(step.body, RunbookStepBody::Action { .. }))
        {
            return Err(RunbookError::InvalidDefinition(
                "runbook must contain at least one typed action".to_owned(),
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
            return Err(RunbookError::RiskUnderstated);
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
    ) -> Result<(), RunbookError> {
        if schedule.runbook_id != definition.id
            || schedule.runbook_version != definition.version
            || schedule.next_step_sequence == 0
            || usize::from(schedule.next_step_sequence) > definition.steps.len() + 1
            || (schedule.active_execution_id.is_some() && schedule.waiting_manual_gate.is_some())
        {
            return Err(RunbookError::InvalidDefinition(
                "schedule projection does not match the runbook identity or step cursor".to_owned(),
            ));
        }
        let all_steps = definition.steps.iter().map(|step| step.id).collect::<BTreeSet<_>>();
        if !schedule.completed_steps.is_subset(&all_steps) {
            return Err(RunbookError::InvalidDefinition(
                "completed schedule steps are not present in the runbook".to_owned(),
            ));
        }
        if let Some(waiting) = schedule.waiting_manual_gate {
            let is_gate = definition
                .steps
                .iter()
                .any(|step| step.id == waiting && matches!(step.body, RunbookStepBody::ManualGate { .. }));
            if !is_gate {
                return Err(RunbookError::InvalidDefinition(
                    "waiting manual gate is not a runbook gate".to_owned(),
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
                return Err(RunbookError::InvalidDefinition(
                    "action-plan bindings are missing, duplicated, extra, or malformed".to_owned(),
                ));
            }
        }
        if action_steps != bound_steps {
            return Err(RunbookError::InvalidDefinition(
                "every runbook action step must have exactly one approved plan binding".to_owned(),
            ));
        }
        Ok(())
    }
}

fn validate_header(definition: &RunbookDefinition) -> Result<(), RunbookError> {
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
        return Err(RunbookError::InvalidDefinition(
            "header, version, risk, parallelism, or step count is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn index_steps(steps: &[RunbookStep]) -> Result<BTreeMap<RunbookStepId, &RunbookStep>, RunbookError> {
    let mut indexed = BTreeMap::new();
    for (index, step) in steps.iter().enumerate() {
        let expected_sequence = u16::try_from(index + 1)
            .map_err(|_| RunbookError::InvalidDefinition("step sequence exceeds u16".to_owned()))?;
        if step.id.as_uuid().is_nil() || step.sequence != expected_sequence || indexed.insert(step.id, step).is_some() {
            return Err(RunbookError::InvalidDefinition(
                "step identifiers must be unique and sequences contiguous".to_owned(),
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
) -> Result<(), RunbookError> {
    if step.name.trim().is_empty()
        || step.name.chars().count() > 128
        || step.depends_on.contains(&step.id)
        || step.depends_on.iter().any(|dependency| {
            steps
                .get(dependency)
                .is_none_or(|candidate| candidate.sequence >= step.sequence)
        })
    {
        return Err(RunbookError::InvalidDefinition(
            "step name or dependency ordering is invalid".to_owned(),
        ));
    }
    if index > 0 && step.parallel_group.is_none() {
        let previous = steps
            .values()
            .find(|candidate| candidate.sequence + 1 == step.sequence)
            .ok_or_else(|| RunbookError::InvalidDefinition("previous serial step is missing".to_owned()))?;
        if !step.depends_on.contains(&previous.id) {
            return Err(RunbookError::InvalidDefinition(
                "steps are serial by default and must depend on the preceding step".to_owned(),
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
                return Err(RunbookError::InvalidDefinition(
                    "action resource and parameters must be bounded".to_owned(),
                ));
            }
            let descriptor = catalog
                .descriptor(*action, descriptor_version)
                .map_err(|_| RunbookError::UnknownAction(action.id().to_owned()))?;
            if descriptor.plan_only {
                return Err(RunbookError::UnknownAction(action.id().to_owned()));
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
                return Err(RunbookError::InvalidDefinition("manual gate is invalid".to_owned()));
            }
        }
    }
    if let Some(condition) = &step.condition
        && (condition.fact.trim().is_empty()
            || condition.fact.chars().count() > 128
            || condition.fact.chars().any(char::is_control))
    {
        return Err(RunbookError::InvalidDefinition("step condition is invalid".to_owned()));
    }
    Ok(())
}

fn validate_parameter_fields(
    value: &ContractJsonValue,
    descriptor_forbidden: &BTreeSet<String>,
) -> Result<(), RunbookError> {
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
                return Err(RunbookError::UnsafeParameter(field.clone()));
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
) -> Result<(), RunbookError> {
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
            return Err(RunbookError::InvalidDefinition(
                "parallel group exceeds its bound or contains internal dependencies".to_owned(),
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
            return Err(RunbookError::InvalidDefinition(
                "parallel steps must be independent action resources".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_compensation_edges(
    definition: &RunbookDefinition,
    steps: &BTreeMap<RunbookStepId, &RunbookStep>,
) -> Result<(), RunbookError> {
    let mut edges = BTreeSet::new();
    for edge in &definition.compensation_edges {
        if edge.from_step == edge.compensation_step
            || !steps.contains_key(&edge.from_step)
            || !steps.contains_key(&edge.compensation_step)
            || !edges.insert((edge.from_step, edge.compensation_step, edge.trigger))
        {
            return Err(RunbookError::InvalidDefinition(
                "compensation edge is missing, self-referential, or duplicated".to_owned(),
            ));
        }
    }
    Ok(())
}

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

use std::error::Error;
use std::fmt;

use rocketmq_sre_contracts::ChangeConflict;
use rocketmq_sre_contracts::ChangeConflictCode;
use rocketmq_sre_contracts::ChangeSchedule;
use rocketmq_sre_contracts::ChangeScheduleStatus;
use rocketmq_sre_contracts::ChangeWindow;
use rocketmq_sre_contracts::ChangeWindowKind;
use rocketmq_sre_contracts::SreTimestamp;

/// Fail-closed calendar or scheduler transition error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChangeCalendarError {
    InvalidWindow,
    InvalidSchedule,
    InvalidTransition,
}

impl fmt::Display for ChangeCalendarError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWindow => formatter.write_str("change window is invalid"),
            Self::InvalidSchedule => formatter.write_str("change schedule is invalid"),
            Self::InvalidTransition => formatter.write_str("change schedule transition is invalid"),
        }
    }
}

impl Error for ChangeCalendarError {}

/// Deterministic maintenance-window and overlap evaluator.
pub struct ChangeCalendar;

impl ChangeCalendar {
    /// Validates a maintenance, freeze, or blackout window.
    ///
    /// # Errors
    ///
    /// Rejects invalid ranges, timezone identifiers, parallelism, names,
    /// resource keys, or reason metadata.
    pub fn validate_window(window: &ChangeWindow) -> Result<(), ChangeCalendarError> {
        if window.schema_version != ChangeWindow::SCHEMA_VERSION
            || window.name.trim().is_empty()
            || window.name.chars().count() > 128
            || !valid_timezone(&window.timezone)
            || window.starts_at >= window.ends_at
            || !(1..=16).contains(&window.max_parallelism)
            || window.reason.trim().is_empty()
            || window.reason.chars().count() > 2048
            || window.created_by.trim().is_empty()
            || window.resource_keys.iter().any(|resource| invalid_resource(resource))
        {
            return Err(ChangeCalendarError::InvalidWindow);
        }
        Ok(())
    }

    /// Returns every blocking window, resource, and parallelism conflict.
    ///
    /// Invalid inputs fail as errors; a valid request outside a maintenance
    /// window returns an explicit conflict.
    ///
    /// # Errors
    ///
    /// Rejects malformed schedule or window inputs.
    pub fn conflicts(
        schedule: &ChangeSchedule,
        runbook_max_parallelism: u16,
        windows: &[ChangeWindow],
        existing: &[ChangeSchedule],
    ) -> Result<Vec<ChangeConflict>, ChangeCalendarError> {
        validate_schedule(schedule)?;
        if !(1..=16).contains(&runbook_max_parallelism) {
            return Err(ChangeCalendarError::InvalidSchedule);
        }
        for window in windows {
            Self::validate_window(window)?;
        }
        let applicable = windows
            .iter()
            .filter(|window| window.tenant_id == schedule.tenant_id && window.cluster_id == schedule.cluster_id)
            .collect::<Vec<_>>();
        let maintenance = applicable.iter().any(|window| {
            window.kind == ChangeWindowKind::Maintenance
                && window.starts_at <= schedule.scheduled_start
                && window.ends_at >= schedule.scheduled_end
                && applies_to_all_resources(window, schedule)
        });
        let mut conflicts = Vec::new();
        if !maintenance {
            conflicts.push(conflict(
                ChangeConflictCode::OutsideMaintenanceWindow,
                "schedule is not fully contained by an applicable maintenance window",
                schedule,
                None,
                None,
                None,
            ));
        }
        for window in applicable {
            if !overlaps(
                schedule.scheduled_start,
                schedule.scheduled_end,
                window.starts_at,
                window.ends_at,
            ) || !resources_intersect(&window.resource_keys, &schedule.resource_keys)
            {
                continue;
            }
            let code = match window.kind {
                ChangeWindowKind::Maintenance => continue,
                ChangeWindowKind::Freeze => ChangeConflictCode::FreezeWindow,
                ChangeWindowKind::Blackout => ChangeConflictCode::BlackoutWindow,
            };
            conflicts.push(conflict(
                code,
                "schedule overlaps a blocking change calendar window",
                schedule,
                window.resource_keys.iter().next().cloned(),
                Some(window),
                None,
            ));
        }

        let mut overlapping_count = 0_u16;
        for candidate in existing {
            validate_schedule(candidate)?;
            if candidate.tenant_id != schedule.tenant_id
                || candidate.cluster_id != schedule.cluster_id
                || candidate.id == schedule.id
                || is_terminal(candidate.status)
                || !overlaps(
                    schedule.scheduled_start,
                    schedule.scheduled_end,
                    candidate.scheduled_start,
                    candidate.scheduled_end,
                )
            {
                continue;
            }
            overlapping_count = overlapping_count.saturating_add(1);
            if let Some(resource) = schedule
                .resource_keys
                .intersection(&candidate.resource_keys)
                .next()
                .cloned()
            {
                conflicts.push(conflict(
                    ChangeConflictCode::ResourceOverlap,
                    "another non-terminal schedule targets the same resource",
                    schedule,
                    Some(resource),
                    None,
                    Some(candidate),
                ));
            }
        }
        let window_parallelism = windows
            .iter()
            .filter(|window| {
                window.kind == ChangeWindowKind::Maintenance
                    && window.starts_at <= schedule.scheduled_start
                    && window.ends_at >= schedule.scheduled_end
                    && applies_to_all_resources(window, schedule)
            })
            .map(|window| window.max_parallelism)
            .min()
            .unwrap_or(1);
        let allowed = runbook_max_parallelism.min(window_parallelism);
        if overlapping_count >= allowed {
            conflicts.push(conflict(
                ChangeConflictCode::ParallelismExceeded,
                "overlapping schedules meet or exceed the runbook/window parallelism bound",
                schedule,
                None,
                None,
                None,
            ));
        }
        Ok(conflicts)
    }

    /// Pauses only future step dispatch.
    ///
    /// # Errors
    ///
    /// Rejects terminal or already stopping schedules.
    pub fn pause(schedule: &mut ChangeSchedule, now: SreTimestamp) -> Result<(), ChangeCalendarError> {
        if !matches!(
            schedule.status,
            ChangeScheduleStatus::Scheduled | ChangeScheduleStatus::Running
        ) {
            return Err(ChangeCalendarError::InvalidTransition);
        }
        schedule.status = ChangeScheduleStatus::Paused;
        schedule.pause_requested_at = Some(now);
        schedule.updated_at = now;
        Ok(())
    }

    /// Resumes future dispatch without clearing intent history.
    ///
    /// # Errors
    ///
    /// Rejects schedules that are not paused.
    pub fn resume(schedule: &mut ChangeSchedule, now: SreTimestamp) -> Result<(), ChangeCalendarError> {
        if schedule.status != ChangeScheduleStatus::Paused {
            return Err(ChangeCalendarError::InvalidTransition);
        }
        schedule.status = if schedule.intent_persisted {
            ChangeScheduleStatus::Running
        } else {
            ChangeScheduleStatus::Scheduled
        };
        schedule.updated_at = now;
        Ok(())
    }

    /// Cancels a not-yet-started schedule or enters safe stopping after an
    /// intent has been persisted.
    ///
    /// # Errors
    ///
    /// Rejects terminal and already stopping schedules.
    pub fn cancel(schedule: &mut ChangeSchedule, now: SreTimestamp) -> Result<(), ChangeCalendarError> {
        if is_terminal(schedule.status)
            || matches!(
                schedule.status,
                ChangeScheduleStatus::SafeStopping | ChangeScheduleStatus::Reconciling
            )
        {
            return Err(ChangeCalendarError::InvalidTransition);
        }
        schedule.cancel_requested_at = Some(now);
        schedule.status = if schedule.intent_persisted {
            ChangeScheduleStatus::SafeStopping
        } else {
            ChangeScheduleStatus::Cancelled
        };
        schedule.updated_at = now;
        Ok(())
    }

    /// Moves a safely stopped, post-intent schedule into reconciliation.
    ///
    /// # Errors
    ///
    /// Rejects every state except `safe_stopping`.
    pub fn begin_reconcile(schedule: &mut ChangeSchedule, now: SreTimestamp) -> Result<(), ChangeCalendarError> {
        if schedule.status != ChangeScheduleStatus::SafeStopping || !schedule.intent_persisted {
            return Err(ChangeCalendarError::InvalidTransition);
        }
        schedule.status = ChangeScheduleStatus::Reconciling;
        schedule.updated_at = now;
        Ok(())
    }
}

fn validate_schedule(schedule: &ChangeSchedule) -> Result<(), ChangeCalendarError> {
    if schedule.schema_version != ChangeSchedule::SCHEMA_VERSION
        || schedule.runbook_version.trim().is_empty()
        || schedule.scheduled_start >= schedule.scheduled_end
        || schedule.resource_keys.is_empty()
        || schedule.resource_keys.iter().any(|resource| invalid_resource(resource))
        || schedule.created_by.trim().is_empty()
    {
        return Err(ChangeCalendarError::InvalidSchedule);
    }
    Ok(())
}

fn valid_timezone(value: &str) -> bool {
    value == "UTC"
        || (value.contains('/')
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"/_+-".contains(&byte)))
}

fn invalid_resource(value: &str) -> bool {
    value.trim().is_empty() || value.chars().count() > 512 || value.chars().any(char::is_control)
}

fn overlaps(
    left_start: SreTimestamp,
    left_end: SreTimestamp,
    right_start: SreTimestamp,
    right_end: SreTimestamp,
) -> bool {
    left_start < right_end && right_start < left_end
}

fn resources_intersect(left: &std::collections::BTreeSet<String>, right: &std::collections::BTreeSet<String>) -> bool {
    left.is_empty() || right.is_empty() || left.intersection(right).next().is_some()
}

fn applies_to_all_resources(window: &ChangeWindow, schedule: &ChangeSchedule) -> bool {
    window.resource_keys.is_empty() || schedule.resource_keys.is_subset(&window.resource_keys)
}

const fn is_terminal(status: ChangeScheduleStatus) -> bool {
    matches!(
        status,
        ChangeScheduleStatus::Completed | ChangeScheduleStatus::Cancelled | ChangeScheduleStatus::Rejected
    )
}

fn conflict(
    code: ChangeConflictCode,
    message: &str,
    schedule: &ChangeSchedule,
    resource_key: Option<String>,
    window: Option<&ChangeWindow>,
    conflicting: Option<&ChangeSchedule>,
) -> ChangeConflict {
    ChangeConflict {
        code,
        message: message.to_owned(),
        resource_key,
        window_id: window.map(|value| value.id),
        conflicting_schedule_id: conflicting.map(|value| value.id),
        starts_at: schedule.scheduled_start,
        ends_at: schedule.scheduled_end,
        blocking: true,
    }
}

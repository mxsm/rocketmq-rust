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

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::SharedError;
use rocketmq_error::CORE_SERVICE_FAILED;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupLifecycleState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScheduleShutdownStage {
    GenerationDrain,
    ScheduleRegistry,
    PersistAdmission,
    PersistWrite,
}

/// The typed source stays private; logs contain only closed stages, kinds and counts.
#[derive(Debug, Clone)]
pub(super) struct ScheduleShutdownFailure {
    pub(super) stage: ScheduleShutdownStage,
    pub(super) generation: u64,
    pub(super) operation_tasks: Option<usize>,
    pub(super) group_tasks: usize,
    pub(super) scope_open: bool,
    pub(super) scope_cancelled: bool,
    source: RuntimeError,
}

impl fmt::Display for ScheduleShutdownFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("schedule shutdown stage failed")
    }
}

impl StdError for ScheduleShutdownFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}

pub(super) fn schedule_shutdown_failure(
    stage: ScheduleShutdownStage,
    generation: u64,
    group: &TaskGroup,
    operation_tasks: Option<usize>,
    source: RuntimeError,
) -> SharedError {
    let failure = ScheduleShutdownFailure {
        stage,
        generation,
        operation_tasks,
        group_tasks: group.task_count(),
        scope_open: group.lifecycle_state() == TaskGroupLifecycleState::Open,
        scope_cancelled: group.cancellation_token().is_cancelled(),
        source,
    };
    tracing::warn!(
        stage = ?failure.stage,
        generation = failure.generation,
        error_kind = ?failure.source.kind(),
        operation_tasks = ?failure.operation_tasks,
        group_tasks = failure.group_tasks,
        scope_open = failure.scope_open,
        scope_cancelled = failure.scope_cancelled,
        "ScheduleMessageService owned shutdown stage failed"
    );
    Arc::new(
        Error::caused_by(&CORE_SERVICE_FAILED, failure).with_context(
            ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, "schedule_message_service_shutdown"),
        ),
    )
}

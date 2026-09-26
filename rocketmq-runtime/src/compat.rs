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

//! Compatibility facade for the older runtime entry points.
//!
//! New code uses the ownership and capability API: [`RuntimeOwner`] owns the
//! runtime, a [`ChildServiceContext`] is injected into a component, work is
//! registered through [`TaskGroup`] or [`TaskSpawner`], bounded periodic work
//! goes through [`ScheduledTaskGroup`], short blocking work goes through
//! [`BlockingExecutor`], and shutdown returns a [`ShutdownReport`].
//! [`prelude`](crate::prelude) documents that path with compiling examples.
//!
//! The items below stay available, and keep their behavior, while call sites
//! migrate. This module groups retained helpers with their own responsibilities,
//! such as the executor services that adapt a pool or a dedicated client runtime.
//!
//! [`ActorRuntime`](crate::ActorRuntime) is deliberately absent. It owns a
//! dedicated thread instead of adapting the ownership API, so grouping it here
//! would imply a migration that does not apply.

pub use crate::executor_service::FuturesExecutorPlan;
pub use crate::executor_service::FuturesExecutorService;
pub use crate::executor_service::FuturesExecutorServiceBuilder;
pub use crate::executor_service::ScheduledExecutorService;
pub use crate::executor_service::ScheduledExecutorServicePlan;
pub use crate::executor_service::TokioExecutorService;
pub use crate::executor_service::TokioExecutorServicePlan;

pub use crate::schedule::executor::ExecutorConfig;
pub use crate::schedule::executor::ExecutorPool;
pub use crate::schedule::executor::TaskExecutor;
pub use crate::schedule::scheduler::SchedulerConfig;
pub use crate::schedule::scheduler::TaskScheduler;
pub use crate::schedule::task::Task;
pub use crate::schedule::task::TaskContext;
pub use crate::schedule::trigger::CronTrigger;
pub use crate::schedule::trigger::DelayTrigger;
pub use crate::schedule::trigger::DelayedIntervalTrigger;
pub use crate::schedule::trigger::IntervalTrigger;
pub use crate::schedule::trigger::Trigger;

/// The older scheduler's task result type.
///
/// Renamed on re-export so it cannot be confused with
/// [`ScheduleExecutionOutcome`](crate::ScheduleExecutionOutcome).
pub use crate::schedule::task::TaskResult as LegacyTaskResult;
/// The older scheduler's task status type.
pub use crate::schedule::task::TaskStatus as LegacyTaskStatus;

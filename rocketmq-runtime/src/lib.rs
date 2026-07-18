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

#![feature(async_fn_traits)]
#![feature(unboxed_closures)]

//! Runtime substrate for the RocketMQ Rust unified thread model.
//!
//! This crate standardizes how components own or borrow a Tokio runtime, how
//! they derive service-level task scopes through [`ServiceContext`], how
//! [`TaskGroup`] and [`ScheduledTaskGroup`] track long-running and periodic
//! tasks, how [`BlockingExecutor`] isolates short blocking work, and how
//! [`ShutdownReport`] records verifiable shutdown evidence.
//!
//! New production code should prefer [`RuntimeOwner`], [`RuntimeContext`], and
//! [`ServiceContext`]. [`RocketMQRuntime`] remains only as a deprecated
//! compatibility boundary.

pub mod actor;
pub mod blocking;
pub mod config;
pub mod context;
pub mod diagnostics;
pub mod error;
pub mod handle;
pub mod legacy;
pub mod owner;
pub mod schedule;
pub mod scheduled;
pub mod service_context;
pub mod service_lifecycle;
pub mod shutdown;
pub mod shutdown_deadline;
pub mod shutdown_report;
pub mod signal;
pub mod task;
pub mod task_group;

pub use actor::ActorRuntime;
pub use blocking::BlockingExecutor;
pub use blocking::BlockingExecutorSnapshot;
pub use blocking::BlockingKind;
pub use blocking::BlockingPoolPolicy;
pub use blocking::BlockingTaskSnapshot;
pub use config::RuntimeConfig;
pub use context::RuntimeContext;
pub use diagnostics::RuntimeDiagnostics;
pub use diagnostics::RuntimeDiagnosticsSnapshot;
pub use error::RuntimeError;
pub use error::RuntimeResult;
pub use handle::RuntimeHandle;
#[allow(deprecated)]
pub use legacy::RocketMQRuntime;
pub use owner::RuntimeOwner;
pub use schedule::executor::ExecutorConfig;
pub use schedule::executor::ExecutorPool;
pub use schedule::executor::TaskExecutor;
pub use schedule::scheduler::SchedulerConfig;
pub use schedule::scheduler::TaskScheduler;
pub use schedule::task::Task;
pub use schedule::task::TaskContext;
pub use schedule::task::TaskResult as LegacyTaskResult;
pub use schedule::task::TaskStatus;
pub use schedule::trigger::CronTrigger;
pub use schedule::trigger::DelayTrigger;
pub use schedule::trigger::DelayedIntervalTrigger;
pub use schedule::trigger::IntervalTrigger;
pub use schedule::trigger::Trigger;
pub use scheduled::ScheduleMode;
pub use scheduled::ScheduledTaskConfig;
pub use scheduled::ScheduledTaskControl;
pub use scheduled::ScheduledTaskGroup;
pub use scheduled::ScheduledTaskSnapshot;
pub use service_context::ServiceContext;
pub use service_lifecycle::ServiceLifecycle;
pub use service_lifecycle::ServiceLifecycleConfig;
pub use service_lifecycle::ServiceLifecycleState;
pub use service_lifecycle::ShutdownReason;
pub use service_lifecycle::ShutdownRequest;
pub use shutdown::Shutdown;
pub use shutdown_deadline::ShutdownDeadline;
pub use shutdown_report::ShutdownAnnotation;
pub use shutdown_report::ShutdownReport;
pub use shutdown_report::TaskSnapshot;
pub use signal::wait_for_signal;
pub use signal::wait_for_signal_result;
pub use task_group::DetachedTaskPolicy;
pub use task_group::TaskGroup;
pub use task_group::TaskGroupChildLease;
pub use task_group::TaskGroupChildStats;
pub use task_group::TaskGroupId;
pub use task_group::TaskGroupLifecycleState;
pub use task_group::TaskId;
pub use task_group::TaskKind;
pub use task_group::TaskResult;
